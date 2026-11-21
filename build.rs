//! Compile the Ubidium `.proto` files into Rust types + gRPC client/server code
//! at build time.
//!
//! The protos are parsed by [`protox`](https://crates.io/crates/protox), a
//! pure-Rust protobuf compiler, and the resulting descriptors are turned into
//! Rust code by `tonic-prost-build`. No `protoc` binary is needed.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "proto";

    // All proto files that make up the SDK. `service.proto` imports the rest,
    // but listing every file keeps all of them in the descriptor set.
    let protos = [
        "common.proto",
        "status.proto",
        "transponder.proto",
        "passing.proto",
        "prewarn.proto",
        "service_status.proto",
        "service_passing.proto",
        "service_prewarn.proto",
        "service_command.proto",
        "service.proto",
    ];

    let proto_paths: Vec<String> = protos.iter().map(|p| format!("{proto_dir}/{p}")).collect();

    // Parse the protos in pure Rust (Google's well-known types such as
    // `google/protobuf/timestamp.proto` are built into protox).
    let fds = protox::compile(&proto_paths, [proto_dir])?;

    tonic_prost_build::configure()
        // Generate both the client side (to talk *to* a Ubidium's TimingSystem
        // server) and the server side (to act as a TimingServer that Ubidiums
        // connect to, as in `ubidium server`).
        .build_client(true)
        .build_server(true)
        // `compile_protos` adds this attribute itself; `compile_fds` does not.
        .type_attribute(".", "#[allow(clippy::all)]")
        .compile_fds(fds)?;

    // Rebuild if any proto changes.
    println!("cargo:rerun-if-changed={proto_dir}");
    Ok(())
}
