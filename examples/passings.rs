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
//! passings <host[:port]> <device-id>
//!
//! # e.g.
//! passings 192.168.1.112:443 U-40153
//! ```

use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};

use ubidium::CA_CERT_PEM;
use ubidium::pb::{
    CmdGetPassings, PassingRequest, cmd_get_passings, passing, passing_request, passing_response,
    timing_system_client::TimingSystemClient, transponder,
};

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let (host, device_id) = match (args.next(), args.next()) {
        (Some(host), Some(device_id)) => (host, device_id),
        _ => {
            eprintln!("usage: passings <host[:port]> <device-id>");
            eprintln!("   e.g. passings 192.168.1.112:443 U-40153");
            std::process::exit(2);
        }
    };

    // Default to the Ubidium gRPC port (443) if the caller didn't supply one.
    let authority = if host.contains(':') {
        host.clone()
    } else {
        format!("{host}:443")
    };
    let endpoint_url = format!("https://{authority}");

    println!("Connecting to {endpoint_url} (device id / TLS name: {device_id}) ...");

    let channel = connect(&endpoint_url, &device_id).await?;
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

/// Build a TLS gRPC channel to the Ubidium, pinning the bundled CA and
/// overriding the TLS server name to the device ID.
async fn connect(endpoint_url: &str, device_id: &str) -> Result<Channel> {
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(CA_CERT_PEM))
        // The server cert's CN/SAN is the device ID, not the IP we dial.
        .domain_name(device_id.to_string());

    let channel = Channel::from_shared(endpoint_url.to_string())
        .context("invalid endpoint URL")?
        .tls_config(tls)
        .context("TLS configuration failed")?
        // gRPC keepalive, matching the Python SDK's channel options.
        .keep_alive_while_idle(true)
        .http2_keep_alive_interval(Duration::from_secs(20))
        .keep_alive_timeout(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await
        .with_context(|| format!("could not connect to {endpoint_url}"))?;

    Ok(channel)
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
