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
