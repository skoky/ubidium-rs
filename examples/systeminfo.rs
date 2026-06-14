//! Example: connect to a Race Result **Ubidium** timing decoder over gRPC/TLS
//! and print its system info (device ID, name, firmware version, customer
//! number, temperature, GPS, battery, ...).
//!
//! The Ubidium runs a `TimingSystem` gRPC server on port **443**. "System info"
//! is delivered through the status stream: we open `OpenStatusStream`, send a
//! single `CmdGetStatus`, and print the first `Status` message we get back.
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
//! ubidium-systeminfo <host[:port]> <device-id>
//!
//! # e.g.
//! ubidium-systeminfo 192.168.1.112:443 U-40153
//! ```

use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};

use ubidium::CA_CERT_PEM;
use ubidium::pb::{
    CmdGetStatus, StatusRequest, status_request, status_response,
    timing_system_client::TimingSystemClient,
};

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let (host, device_id) = match (args.next(), args.next()) {
        (Some(host), Some(device_id)) => (host, device_id),
        _ => {
            eprintln!("usage: ubidium-systeminfo <host[:port]> <device-id>");
            eprintln!("   e.g. ubidium-systeminfo 192.168.1.112:443 U-40153");
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

    // OpenStatusStream is bi-directional: we stream StatusRequests out and
    // receive StatusResponses back. We only need to send one request.
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(StatusRequest {
        cmd: Some(status_request::Cmd::Get(CmdGetStatus {
            // We only want a single snapshot, not continuous updates.
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

    // The Ubidium reports its device ID in the response trailers/headers too.
    if let Some(id) = response.metadata().get("device-id") {
        if let Ok(id) = id.to_str() {
            println!("Server reported device-id header: {id}");
        }
    }

    let mut stream = response.into_inner();

    // Read the first status message — that is the system info snapshot.
    while let Some(msg) = stream.message().await.context("reading status stream")? {
        match msg.response {
            Some(status_response::Response::Status(status)) => {
                print_system_info(&status);
                break; // got our snapshot, done
            }
            Some(status_response::Response::Error(err)) => {
                bail!("Ubidium returned an error: {} (code {})", err.message, err.code);
            }
            None => {
                eprintln!("Received empty status response, waiting for the next one...");
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

/// Pretty-print the interesting fields of a `Status` snapshot.
fn print_system_info(status: &ubidium::pb::Status) {
    println!("\n=== Ubidium system info ===");
    print_opt("Device ID", &status.id);
    print_opt("Name", &status.name);
    print_opt("Firmware version", &status.version);

    if let Some(cust_no) = status.cust_no {
        println!("Customer number  : {cust_no}");
    }
    if let Some(temp) = status.temperature {
        println!("Board temperature: {temp:.1} °C");
    }
    if let Some(passing_id) = status.passing_id {
        println!("Latest passing id: {passing_id}");
    }

    if let Some(time) = &status.time {
        if let Some(utc) = &time.utc {
            println!("Device time (UTC): {}s +{}ns, local offset {}s", utc.seconds, utc.nanos, time.offset);
        }
    }

    match status.gps.as_ref().and_then(|g| g.data.as_ref()) {
        Some(ubidium::pb::status::gps::Data::Location(loc)) => {
            println!("GPS              : fix at lat {:.6}, long {:.6}, alt {:.1}m", loc.lat, loc.long, loc.alt);
        }
        Some(ubidium::pb::status::gps::Data::NoFix(_)) => println!("GPS              : no fix"),
        None => {}
    }

    if let Some(update) = &status.update {
        if let Some(v) = &update.update_version {
            println!("Update available : {v} (installed: {})", update.installed.unwrap_or(false));
        }
    }
    println!("===========================");
}

fn print_opt(label: &str, value: &Option<String>) {
    if let Some(v) = value {
        println!("{label:<17}: {v}");
    }
}
