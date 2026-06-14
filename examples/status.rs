//! Example: connect to a Race Result **Ubidium** timing decoder over gRPC/TLS
//! and print its **full status** — every field of the `Status` message,
//! including the nested active/passive equipment, GPS, batteries, power and
//! firmware-update sections.
//!
//! This is the verbose counterpart to the `systeminfo` example (which only
//! prints a hand-picked summary). Here we dump the entire decoded `Status`.
//!
//! The Ubidium delivers status through the **status stream**: we open
//! `OpenStatusStream`, send one `CmdGetStatus`, and print the first complete
//! `Status` snapshot we receive.
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
//! status --host <host[:port]> --device-id <id>
//!
//! # e.g.
//! status --host 192.168.1.112:443 --device-id U-40153
//! ```

use anyhow::{Context, Result, bail};
use clap::Parser;
use tokio_stream::wrappers::ReceiverStream;

use ubidium::pb::{
    CmdGetStatus, StatusRequest, status_request, status_response,
    timing_system_client::TimingSystemClient,
};

/// Dump the full status of a Race Result Ubidium (every field).
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

    // OpenStatusStream is bi-directional: we stream StatusRequests out and
    // receive StatusResponses back. We only need to send one request.
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(StatusRequest {
        cmd: Some(status_request::Cmd::Get(CmdGetStatus {
            // A single, complete snapshot is enough — no continuous updates.
            r#continue: false,
            push_time: None,
        })),
    })
    .await
    .ok();

    let response = client
        .open_status_stream(ReceiverStream::new(rx))
        .await
        .context("OpenStatusStream RPC failed")?;

    if let Some(id) = response.metadata().get("device-id") {
        if let Ok(id) = id.to_str() {
            println!("Server reported device-id header: {id}");
        }
    }

    let mut stream = response.into_inner();

    while let Some(msg) = stream.message().await.context("reading status stream")? {
        match msg.response {
            Some(status_response::Response::Status(status)) => {
                // Dump every field of the decoded Status message. The pretty
                // `Debug` output walks the whole nested structure (active /
                // passive equipment, GPS, batteries, power, update, ...).
                println!("\n=== Ubidium full status ===");
                println!("{status:#?}");
                println!("===========================");
                break; // got our snapshot, done
            }
            Some(status_response::Response::Error(err)) => {
                bail!("Ubidium returned an error: {} (code {})", err.message, err.code);
            }
            None => eprintln!("Received empty status response, waiting for the next one..."),
        }
    }

    // Dropping `tx` closes the outbound stream and lets the server tear down.
    drop(tx);
    Ok(())
}
