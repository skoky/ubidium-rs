# ubidium-rs

Rust gRPC bindings for the [**Race Result**](https://www.raceresult.com/)
**Ubidium** timing decoder.

Generated from the protobuf definitions shipped with the official
**ubidium-sdk v0.9.10**. The `.proto` files in [`proto/`](proto/) are compiled
at build time with [`tonic-build`](https://crates.io/crates/tonic-build)
(driving `prost` + `protoc`), exposing the protobuf message types and the
`TimingSystem` / `TimingServer` gRPC clients under the [`ubidium::pb`] module.

> Race Result, Ubidium, and the SDK are trademarks/property of Race Result AG.
> This is an independent, unofficial Rust binding.

## Requirements

- A recent Rust toolchain (edition 2024, `rustc` ≥ 1.85).
- `protoc` (the Protocol Buffers compiler) on the build host, used by
  `tonic-build`:

  | Platform | Install |
  |----------|---------|
  | **Linux** (Debian/Ubuntu) | `sudo apt install protobuf-compiler` |
  | **Linux** (Fedora) | `sudo dnf install protobuf-compiler` |
  | **macOS** ([Homebrew](https://brew.sh/)) | `brew install protobuf` |
  | **Windows** ([winget](https://learn.microsoft.com/windows/package-manager/)) | `winget install protobuf` |
  | **Windows** ([Chocolatey](https://chocolatey.org/)) | `choco install protoc` |

  Or download a prebuilt binary from the
  [Protocol Buffers releases](https://github.com/protocolbuffers/protobuf/releases)
  and put `protoc` on your `PATH`. Verify with `protoc --version`.

## Install

```toml
[dependencies]
ubidium-rs = "0.1"
```

The crate's library name is `ubidium`, so you import it as:

```rust
use ubidium::pb::timing_system_client::TimingSystemClient;
```

## TLS

Every Ubidium presents a server certificate whose subject/SAN is the **device
ID** (e.g. `U-40153`), signed by the `RACE RESULT TD proxy` CA bundled in
[`certs/cacert.pem`](certs/cacert.pem) and re-exported as
[`ubidium::CA_CERT_PEM`]. To connect you therefore pin that CA *and* override
the expected TLS server name to the device ID, mirroring the official Python
SDK's `grpc.ssl_target_name_override`.

## Examples

The Ubidium runs a `TimingSystem` gRPC server on port **443**. In both examples
`<device-id>` (e.g. `U-40153`) is required because it is also used as the TLS
server name (see [TLS](#tls) above).

### Get Ubidium system info

There is no dedicated "system info" RPC — the device identity (ID, name,
firmware version, customer number, temperature, GPS, battery, ...) is delivered
through the **status stream**. The [`systeminfo`](examples/systeminfo.rs)
example opens `OpenStatusStream`, sends one `CmdGetStatus`, and prints the first
`Status` snapshot it receives.

```bash
cargo run --example systeminfo -- <host[:port]> <device-id>

# e.g.
cargo run --example systeminfo -- 192.168.1.112:443 U-40153
```

### Listen for passings

The [`passings`](examples/passings.rs) example opens `OpenPassingStream`, sends
one `CmdGetPassings` requesting every new passing from now on, and then prints
each transponder passing as it arrives until you stop it with Ctrl-C.

```bash
cargo run --example passings -- <host[:port]> <device-id>

# e.g.
cargo run --example passings -- 192.168.1.112:443 U-40153
```

## Layout

| Path | Purpose |
|------|---------|
| `proto/` | Ubidium `.proto` files (from ubidium-sdk v0.9.10) |
| `build.rs` | Compiles the protos with `tonic-build` |
| `src/lib.rs` | `ubidium::pb` generated types + clients, bundled CA cert |
| `examples/systeminfo.rs` | Example: connect and print system info |
| `examples/passings.rs` | Example: connect and listen for passings |
| `certs/cacert.pem` | `RACE RESULT TD proxy` CA certificate |

## License

MIT
