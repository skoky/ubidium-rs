//! Rust bindings for the Race Result **Ubidium** gRPC SDK.
//!
//! The protobuf messages and the `TimingSystem` / `TimingServer` gRPC clients
//! are generated at build time from the `.proto` files in `proto/` (see
//! `build.rs`). They all live in the `raceresult.ubidium` protobuf package,
//! re-exported here as the [`pb`] module.
//!
//! ```no_run
//! use ubidium::pb::timing_system_client::TimingSystemClient;
//! ```

/// Generated protobuf types and gRPC clients for the `raceresult.ubidium`
/// package.
pub mod pb {
    tonic::include_proto!("raceresult.ubidium");
}

/// CA certificate (`RACE RESULT TD proxy`) that signs the per-device server
/// certificates presented by Ubidiums. Bundled so the example is self
/// contained.
pub const CA_CERT_PEM: &[u8] = include_bytes!("../certs/cacert.pem");

/// Default gRPC port the Ubidium `TimingSystem` server listens on.
pub const DEFAULT_PORT: u16 = 443;

use std::time::Duration;

use tonic::transport::{Certificate, Channel, ClientTlsConfig};

/// Connect to a Ubidium's `TimingSystem` gRPC server over TLS.
///
/// `host` may be an IP or hostname, optionally with a `:port` suffix; when no
/// port is given, [`DEFAULT_PORT`] (443) is used.
///
/// `device_id` (e.g. `U-40153`) is used as the TLS server name: every Ubidium
/// presents a certificate whose subject/SAN is the device ID, signed by the
/// bundled [`CA_CERT_PEM`] CA. We pin that CA and override the expected domain
/// name to the device ID, mirroring the official Python SDK's
/// `grpc.ssl_target_name_override`.
///
/// ```no_run
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let channel = ubidium::connect("192.168.1.112:443", "U-40153").await?;
/// let client = ubidium::pb::timing_system_client::TimingSystemClient::new(channel);
/// # Ok(())
/// # }
/// ```
pub async fn connect(
    host: &str,
    device_id: &str,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    let authority = if host.contains(':') {
        host.to_string()
    } else {
        format!("{host}:{DEFAULT_PORT}")
    };
    let endpoint_url = format!("https://{authority}");

    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(CA_CERT_PEM))
        // The server cert's CN/SAN is the device ID, not the IP we dial.
        .domain_name(device_id.to_string());

    let channel = Channel::from_shared(endpoint_url)?
        .tls_config(tls)?
        // gRPC keepalive, matching the Python SDK's channel options.
        .keep_alive_while_idle(true)
        .http2_keep_alive_interval(Duration::from_secs(20))
        .keep_alive_timeout(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await?;

    Ok(channel)
}
