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
/// certificates presented by Ubidiums. Bundled so [`connect`] works out of
/// the box.
pub const CA_CERT_PEM: &[u8] = include_bytes!("../certs/cacert.pem");

/// Default gRPC port the Ubidium `TimingSystem` server listens on.
pub const DEFAULT_PORT: u16 = 443;

use std::time::Duration;

use tonic::transport::{Certificate, Channel, ClientTlsConfig};

/// Connect to a Ubidium's `TimingSystem` gRPC server over TLS.
///
/// `host` may be an IP or hostname, optionally with a `:port` suffix; when no
/// port is given, [`DEFAULT_PORT`] (443) is used. IPv6 addresses may be given
/// bare (`fe80::1`) or in brackets (`[fe80::1]:443`).
///
/// `device_id` (e.g. `U-40153`) is used as the TLS server name: every Ubidium
/// presents a certificate whose subject/SAN is the device ID, signed by the
/// bundled [`CA_CERT_PEM`] CA. We pin that CA and override the expected domain
/// name to the device ID, mirroring the official Python SDK's
/// `grpc.ssl_target_name_override`.
pub async fn connect(
    host: &str,
    device_id: &str,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    connect_with_ca(host, device_id, CA_CERT_PEM).await
}

/// Like [`connect`], but trusts the CA certificate(s) in `ca_pem` instead of
/// the bundled Race Result CA — e.g. for a test server or a TLS proxy.
pub async fn connect_with_ca(
    host: &str,
    device_id: &str,
    ca_pem: &[u8],
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_pem))
        // The server cert's CN/SAN is the device ID, not the IP we dial.
        .domain_name(device_id.to_string());

    let channel = Channel::from_shared(endpoint_url(host))?
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

/// Build the `https://host:port` endpoint URL, adding [`DEFAULT_PORT`] when
/// `host` has no port.
fn endpoint_url(host: &str) -> String {
    let authority = if host.starts_with('[') {
        // Bracketed IPv6, with or without a port.
        if host.contains("]:") {
            host.to_string()
        } else {
            format!("{host}:{DEFAULT_PORT}")
        }
    } else {
        match host.matches(':').count() {
            0 => format!("{host}:{DEFAULT_PORT}"),
            1 => host.to_string(),
            // Bare IPv6 address: brackets are required before adding a port.
            _ => format!("[{host}]:{DEFAULT_PORT}"),
        }
    };
    format!("https://{authority}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_url_adds_default_port() {
        assert_eq!(endpoint_url("192.168.1.112"), "https://192.168.1.112:443");
        assert_eq!(endpoint_url("ubidium.local"), "https://ubidium.local:443");
    }

    #[test]
    fn endpoint_url_keeps_explicit_port() {
        assert_eq!(
            endpoint_url("192.168.1.112:8443"),
            "https://192.168.1.112:8443"
        );
        assert_eq!(
            endpoint_url("ubidium.local:443"),
            "https://ubidium.local:443"
        );
    }

    #[test]
    fn endpoint_url_handles_ipv6() {
        assert_eq!(endpoint_url("fe80::1"), "https://[fe80::1]:443");
        assert_eq!(endpoint_url("::1"), "https://[::1]:443");
        assert_eq!(endpoint_url("[::1]"), "https://[::1]:443");
        assert_eq!(endpoint_url("[::1]:8443"), "https://[::1]:8443");
    }

    #[test]
    fn bundled_ca_is_a_single_pem_certificate() {
        let pem = std::str::from_utf8(CA_CERT_PEM).expect("CA PEM is UTF-8");
        assert!(pem.trim_start().starts_with("-----BEGIN CERTIFICATE-----"));
        assert!(pem.trim_end().ends_with("-----END CERTIFICATE-----"));
        assert_eq!(pem.matches("BEGIN CERTIFICATE").count(), 1);
    }

    #[tokio::test]
    async fn connect_fails_on_closed_port() {
        // Port 1 on localhost is not listening: the connection is refused.
        let err = connect("127.0.0.1:1", "U-1").await.unwrap_err();
        assert!(err.to_string().contains("transport error"), "{err}");
    }

    #[tokio::test]
    async fn connect_rejects_invalid_host() {
        assert!(connect("not a host", "U-1").await.is_err());
    }
}
