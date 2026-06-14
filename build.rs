//! Compile the Ubidium `.proto` files into Rust types + a gRPC client at build
//! time using `tonic-build` (which drives `prost` and `protoc`).
//!
//! Requires `protoc` to be available on the build host.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "proto";

    // All proto files that make up the SDK. `service.proto` imports the rest,
    // but tonic-build wants every file listed explicitly.
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

    let proto_paths: Vec<String> = protos
        .iter()
        .map(|p| format!("{proto_dir}/{p}"))
        .collect();

    tonic_build::configure()
        // We only act as a gRPC *client* (the Ubidium is the server), so the
        // server-side trait scaffolding is not needed.
        .build_server(false)
        .build_client(true)
        .compile_protos(&proto_paths, &[proto_dir])?;

    // Rebuild if any proto changes.
    println!("cargo:rerun-if-changed={proto_dir}");
    Ok(())
}
