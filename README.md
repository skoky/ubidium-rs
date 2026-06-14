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

The Ubidium runs a `TimingSystem` gRPC server on port **443**. Every example
parses its CLI with [`clap`](https://crates.io/crates/clap), so `--help` lists
its options. `--device-id` (e.g. `U-40153`) is required by the client examples
because it is also used as the TLS server name (see [TLS](#tls) above).

### Get Ubidium system info

There is no dedicated "system info" RPC — the device identity (ID, name,
firmware version, customer number, temperature, GPS, battery, ...) is delivered
through the **status stream**. The [`systeminfo`](examples/systeminfo.rs)
example opens `OpenStatusStream`, sends one `CmdGetStatus`, and prints the first
`Status` snapshot it receives.

```bash
cargo run --example systeminfo -- --host <host[:port]> --device-id <id>

# e.g.
cargo run --example systeminfo -- --host 192.168.1.112:443 --device-id U-40153
```

### Listen for passings

The [`passings`](examples/passings.rs) example opens `OpenPassingStream`, sends
one `CmdGetPassings` requesting every new passing from now on, and then prints
each transponder passing as it arrives until you stop it with Ctrl-C.

```bash
cargo run --example passings -- --host <host[:port]> --device-id <id>

# e.g.
cargo run --example passings -- --host 192.168.1.112:443 --device-id U-40153
```

### Download stored passings (by ID range)

The [`stored_passings`](examples/stored_passings.rs) example downloads passings
using the **full** `CmdGetPassings` request, exposing every selector it offers.
Pick exactly one **start** and one **end** (enforced by `clap` argument groups):

- start: `--start-ref <now|current-file|first-file>`, `--start-id <ID>`, or
  `--start-no <FILE:NO>`
- end: `--end-ref <until-stopped|currently-existing|end-of-file>` or
  `--end-count <N>`

With `--end-ref until-stopped` the stream keeps delivering future passings until
you press Ctrl-C; the other end selectors terminate on their own. Run with
`--help` for the full list.

```bash
# all stored passings of the current file:
cargo run --example stored_passings -- --host 192.168.1.112:443 --device-id U-40153 \
    --start-ref current-file --end-ref currently-existing

# 50 passings starting at ID 1000:
cargo run --example stored_passings -- --host 192.168.1.112:443 --device-id U-40153 \
    --start-id 1000 --end-count 50

# from file 3 / passing 1 to the end of that file:
cargo run --example stored_passings -- --host 192.168.1.112:443 --device-id U-40153 \
    --start-no 3:1 --end-ref end-of-file
```

### Get full Ubidium status

The [`status`](examples/status.rs) example is the verbose counterpart to
`systeminfo`: it opens `OpenStatusStream`, sends one `CmdGetStatus`, and dumps
**every field** of the first `Status` snapshot (active/passive equipment, GPS,
batteries, power, firmware update, ...).

```bash
cargo run --example status -- --host <host[:port]> --device-id <id>

# e.g.
cargo run --example status -- --host 192.168.1.112:443 --device-id U-40153
```

### Full client (port of the Go `cmd/client`)

The [`client`](examples/client.rs) example is a Rust port of the Go SDK's client
(fixed-IP mode): it connects to a Ubidium, presses `KEY_BACK` twice via the
unary `PressKey` RPC, then opens **both** the status and passing streams and
prints updates from each concurrently until Ctrl-C.

```bash
cargo run --example client -- --host <host[:port]> --device-id <id>

# e.g.
cargo run --example client -- --host 192.168.1.112:443 --device-id U-40153
```

### TimingServer (port of the Go `cmd/server`)

The [`server`](examples/server.rs) example flips the roles: it is a Rust port of
the Go SDK's server, implementing the `TimingServer` gRPC service that **Ubidiums
connect out to**. Configure a Ubidium's "custom server" with this machine's
address/port. For each stream a Ubidium opens it subscribes to passings (and
acknowledges them), status updates, and sends one `PressKey` command. It serves
TLS using the bundled example server certificate
(`certs/example-server/`).

```bash
cargo run --example server -- --listen <addr>

# e.g. listen on all interfaces, port 8443
cargo run --example server -- --listen 0.0.0.0:8443
```

## Layout

| Path | Purpose |
|------|---------|
| `proto/` | Ubidium `.proto` files (from ubidium-sdk v0.9.10) |
| `build.rs` | Compiles the protos with `tonic-build` |
| `src/lib.rs` | `ubidium::pb` generated types + clients, bundled CA cert |
| `examples/systeminfo.rs` | Example: connect and print system info |
| `examples/passings.rs` | Example: connect and listen for passings |
| `examples/stored_passings.rs` | Example: download stored passings by ID range |
| `examples/status.rs` | Example: dump the full Ubidium status |
| `examples/client.rs` | Example: full client, port of the Go `cmd/client` |
| `examples/server.rs` | Example: `TimingServer`, port of the Go `cmd/server` |
| `certs/example-server/` | Example server certificate + key (for `server`) |
| `certs/cacert.pem` | `RACE RESULT TD proxy` CA certificate |

## License

MIT
