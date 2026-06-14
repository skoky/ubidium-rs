//! Example: connect to a Race Result **Ubidium** timing decoder over gRPC/TLS
//! and continuously listen for transponder passings, printing each one as it
//! arrives.
//!
//! The Ubidium runs a `TimingSystem` gRPC server on port **443**. Passings are
//! delivered through `OpenPassingStream`: we send a single `CmdGetPassings`
//! describing which passings we want (here: every new passing from now on,
//! until we stop), then read `PassingResponse` messages until interrupted with
//! Ctrl-C.
//!
//! ## TLS
//!
//! Every Ubidium presents a server certificate whose subject/SAN is the device
//! ID (e.g. `U-40153`), signed by the bundled `RACE RESULT TD proxy` CA. So we
//! pin that CA *and* override the expected domain name to the device ID, which
//! mirrors the official Python SDK (`grpc.ssl_target_name_override`).
//!
//! ## Usage
//!
//! ```text
//! passings --host <host[:port]> --device-id <id>
//!
//! # e.g.
//! passings --host 192.168.1.112:443 --device-id U-40153
//! ```

use anyhow::{Context, Result, bail};
use clap::Parser;
use tokio_stream::wrappers::ReceiverStream;

use ubidium::pb::{
    CmdGetPassings, PassingRequest, cmd_get_passings, passing, passing_request, passing_response,
    timing_system_client::TimingSystemClient, transponder,
};

/// Listen for live passings from a Race Result Ubidium until Ctrl-C.
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

    // OpenPassingStream is bi-directional: we stream PassingRequests out and
    // receive PassingResponses back. We only need to send one request.
    //
    // Keep `tx` alive for the lifetime of the stream so the outbound half stays
    // open; dropping it would signal "no more requests" to the Ubidium.
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(PassingRequest {
        cmd: Some(passing_request::Cmd::Get(CmdGetPassings {
            // Only new passings, recorded from the moment the request is processed.
            start: Some(cmd_get_passings::Start::StartRef(
                cmd_get_passings::StartReference::Now as i32,
            )),
            // Keep streaming until we explicitly stop (i.e. until Ctrl-C here).
            end: Some(cmd_get_passings::End::EndRef(
                cmd_get_passings::EndReference::UntilStopped as i32,
            )),
        })),
    })
    .await
    .ok();

    let response = client
        .open_passing_stream(ReceiverStream::new(rx))
        .await
        .context("OpenPassingStream RPC failed")?;

    if let Some(id) = response.metadata().get("device-id") {
        if let Ok(id) = id.to_str() {
            println!("Server reported device-id header: {id}");
        }
    }

    let mut stream = response.into_inner();

    println!("Listening for passings (press Ctrl-C to stop) ...\n");

    // Read passings until the stream ends or the user interrupts with Ctrl-C.
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\nInterrupted, shutting down.");
                break;
            }
            msg = stream.message() => {
                match msg.context("reading passing stream")? {
                    Some(resp) => handle_response(resp)?,
                    None => {
                        println!("Passing stream closed by the Ubidium.");
                        break;
                    }
                }
            }
        }
    }

    // Dropping `tx` closes the outbound stream and lets the server tear down.
    drop(tx);
    Ok(())
}

/// Handle one `PassingResponse` from the stream.
fn handle_response(resp: ubidium::pb::PassingResponse) -> Result<()> {
    match resp.response {
        Some(passing_response::Response::Passing(p)) => print_passing(&p),
        Some(passing_response::Response::Welcome(w)) => {
            println!(
                "Welcome: newest passing id {}, customer {}",
                w.current_id, w.cust_no
            );
        }
        Some(passing_response::Response::Error(err)) => {
            bail!("Ubidium returned an error: {} (code {})", err.message, err.code);
        }
        None => eprintln!("Received empty passing response, ignoring."),
    }
    Ok(())
}

/// Pretty-print a single passing.
fn print_passing(p: &ubidium::pb::Passing) {
    let kind = match &p.data {
        Some(passing::Data::Active(_)) => "active",
        Some(passing::Data::Passive(_)) => "passive",
        Some(passing::Data::Marker(_)) => "marker",
        None => "unknown",
    };

    let transponder_id = p
        .transponder
        .as_ref()
        .map(|t| t.id.as_str())
        .unwrap_or("?");

    let no = p
        .no
        .as_ref()
        .map(|n| format!("file {} / no {}", n.file, n.no))
        .unwrap_or_else(|| "-".to_string());

    let when = p
        .time
        .as_ref()
        .and_then(|t| t.utc.as_ref())
        .map(|utc| format!("{}.{:09} UTC", utc.seconds, utc.nanos))
        .unwrap_or_else(|| "-".to_string());

    let input = p
        .src
        .as_ref()
        .map(|s| s.input.as_str())
        .unwrap_or("-");

    println!(
        "[{kind}] id={} transponder={transponder_id} {no} hits={} rssi={} input={input} time={when}",
        p.id, p.hits, p.rssi
    );

    // Extra detail for active transponders (battery / temperature / wakeups).
    if let Some(transponder::Data::Active(a)) =
        p.transponder.as_ref().and_then(|t| t.data.as_ref())
    {
        println!(
            "        active transponder: battery={} temperature={}°C wakeups={}",
            a.battery, a.temperature, a.wakeup_counter
        );
    }
}
