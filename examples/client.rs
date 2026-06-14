//! Example: a **client** that connects to a Race Result **Ubidium** and acts as
//! the gRPC client of its `TimingSystem` service — a Rust port of the Go SDK's
//! `cmd/client` example (fixed-IP mode).
//!
//! It mirrors what the Go client does for a single, known Ubidium:
//!
//! 1. connect to `<host>:443` over TLS,
//! 2. press a key on the device twice (`KEY_BACK`) via the unary `PressKey` RPC,
//! 3. open the **status** stream (continuous updates, pushed every 5s) and the
//!    **passing** stream (from the beginning of the current file, until stopped),
//! 4. read both concurrently and print what arrives, until Ctrl-C.
//!
//! The Go example can also auto-discover Ubidiums via UDP broadcast; that part
//! is intentionally omitted here — this example covers the fixed-IP path.
//!
//! ## Usage
//!
//! ```text
//! client --host <host[:port]> --device-id <id>
//!
//! # e.g.
//! client --host 192.168.1.112:443 --device-id U-40153
//! ```

use anyhow::{Context, Result};
use clap::Parser;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;

use ubidium::pb::{
    CmdGetPassings, CmdGetStatus, CmdPressKey, Key, PassingRequest, PassingResponse, StatusRequest,
    StatusResponse, cmd_get_passings, passing, passing_request, passing_response, status_request,
    status_response, timing_system_client::TimingSystemClient, transponder,
};

/// Connect to a Race Result Ubidium and stream its status and passings.
#[derive(Parser, Debug)]
#[command(about, long_about = None)]
struct Cli {
    /// Ubidium host or IP, optionally with `:port` (defaults to port 443).
    #[arg(long)]
    host: String,

    /// Device ID, e.g. `U-40153` (also used as the TLS server name).
    #[arg(long)]
    device_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let Cli { host, device_id } = Cli::parse();

    println!("Connecting to {host} (device id / TLS name: {device_id}) ...");

    let channel = ubidium::connect(&host, &device_id)
        .await
        .map_err(|e| anyhow::anyhow!("could not connect to {host}: {e}"))?;
    let mut client = TimingSystemClient::new(channel);

    // Control the Ubidium via its keypad, like the Go example: press BACK twice.
    client
        .press_key(CmdPressKey { key: Key::Back as i32 })
        .await
        .context("PressKey failed")?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    client
        .press_key(CmdPressKey { key: Key::Back as i32 })
        .await
        .context("PressKey failed")?;

    // --- Status stream: continuous updates, pushed at least every 5s. ---
    let (status_tx, status_rx) = mpsc::channel(1);
    status_tx
        .send(StatusRequest {
            cmd: Some(status_request::Cmd::Get(CmdGetStatus {
                r#continue: true,
                push_time: Some(prost_duration(5)),
            })),
        })
        .await
        .ok();
    let status_stream = client
        .open_status_stream(ReceiverStream::new(status_rx))
        .await
        .context("OpenStatusStream failed")?
        .into_inner();

    // --- Passing stream: from the beginning of the current file, until stopped. ---
    let (passing_tx, passing_rx) = mpsc::channel(1);
    passing_tx
        .send(PassingRequest {
            cmd: Some(passing_request::Cmd::Get(CmdGetPassings {
                start: Some(cmd_get_passings::Start::StartRef(
                    cmd_get_passings::StartReference::BeginningOfCurrentFile as i32,
                )),
                end: Some(cmd_get_passings::End::EndRef(
                    cmd_get_passings::EndReference::UntilStopped as i32,
                )),
            })),
        })
        .await
        .ok();
    let passing_stream = client
        .open_passing_stream(ReceiverStream::new(passing_rx))
        .await
        .context("OpenPassingStream failed")?
        .into_inner();

    // Read both streams concurrently in their own tasks.
    let status_task = tokio::spawn(read_status(status_stream));
    let passing_task = tokio::spawn(read_passings(passing_stream));

    println!("Streaming status and passings (press Ctrl-C to stop) ...\n");
    tokio::signal::ctrl_c().await.ok();
    println!("\nInterrupted, shutting down.");

    // Dropping the request senders closes the outbound halves, ending the
    // streams server-side; then stop the reader tasks.
    drop(status_tx);
    drop(passing_tx);
    status_task.abort();
    passing_task.abort();
    Ok(())
}

/// Build a `prost_types::Duration` of whole seconds.
fn prost_duration(secs: i64) -> prost_types::Duration {
    prost_types::Duration { seconds: secs, nanos: 0 }
}

async fn read_status(mut stream: Streaming<StatusResponse>) {
    loop {
        match stream.message().await {
            Ok(Some(msg)) => match msg.response {
                Some(status_response::Response::Status(s)) => {
                    let id = s.id.as_deref().unwrap_or("?");
                    let name = s.name.as_deref().unwrap_or("?");
                    let version = s.version.as_deref().unwrap_or("?");
                    let temp = s
                        .temperature
                        .map(|t| format!("{t:.1}°C"))
                        .unwrap_or_else(|| "-".into());
                    println!("Status: id={id} name={name} fw={version} temp={temp}");
                }
                Some(status_response::Response::Error(e)) => {
                    println!("Status error: {} ({})", e.message, e.code);
                }
                None => {}
            },
            Ok(None) => break,
            Err(status) => {
                eprintln!("status stream ended: {status}");
                break;
            }
        }
    }
}

async fn read_passings(mut stream: Streaming<PassingResponse>) {
    loop {
        match stream.message().await {
            Ok(Some(msg)) => match msg.response {
                Some(passing_response::Response::Passing(p)) => print_passing(&p),
                Some(passing_response::Response::Welcome(w)) => {
                    println!("Welcome: newest passing id {}, customer {}", w.current_id, w.cust_no);
                }
                Some(passing_response::Response::Error(e)) => {
                    println!("Passing error: {} ({})", e.message, e.code);
                }
                None => {}
            },
            Ok(None) => break,
            Err(status) => {
                eprintln!("passing stream ended: {status}");
                break;
            }
        }
    }
}

fn print_passing(p: &ubidium::pb::Passing) {
    let kind = match &p.data {
        Some(passing::Data::Active(_)) => "active",
        Some(passing::Data::Passive(_)) => "passive",
        Some(passing::Data::Marker(_)) => "marker",
        None => "unknown",
    };
    let transponder_id = p.transponder.as_ref().map(|t| t.id.as_str()).unwrap_or("?");
    println!(
        "[{kind}] passing id={} transponder={transponder_id} hits={} rssi={}",
        p.id, p.hits, p.rssi
    );
    if let Some(transponder::Data::Active(a)) =
        p.transponder.as_ref().and_then(|t| t.data.as_ref())
    {
        println!(
            "        active transponder: battery={} temperature={}°C wakeups={}",
            a.battery, a.temperature, a.wakeup_counter
        );
    }
}
