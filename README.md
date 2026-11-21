# ubidium-rs

Rust gRPC bindings for the [**Race Result**](https://www.raceresult.com/)
**Ubidium** timing decoder.

Generated from the protobuf definitions shipped with the official
**ubidium-sdk v0.9.12**. The `.proto` files in [`proto/`](proto/) are compiled
at build time with [`tonic-prost-build`](https://crates.io/crates/tonic-prost-build)
(driving `prost` + `protoc`), exposing the protobuf message types and the
`TimingSystem` / `TimingServer` gRPC clients under the [`ubidium::pb`] module.

The crate also ships the **`ubidium`** command-line tool, which can query a
Ubidium, stream or download passings, ping it, or act as a `TimingServer` —
see [Command-line tool](#command-line-tool).

> Race Result, Ubidium, and the SDK are trademarks/property of Race Result AG.
> This is an independent, unofficial Rust binding.

## Requirements

- A recent Rust toolchain (edition 2024, `rustc` ≥ 1.85).
- `protoc` (the Protocol Buffers compiler) on the build host, used by
  `tonic-prost-build`:

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

As a library:

```toml
[dependencies]
ubidium-rs = "0.3"
```

The `ubidium` binary and its dependencies (`tokio`, `clap`, `anyhow`, ...) sit
behind the default `cli` feature. If you only need the bindings, turn it off:

```toml
[dependencies]
ubidium-rs = { version = "0.3", default-features = false }
```

As a command-line tool:

```bash
cargo install ubidium-rs          # from crates.io
cargo install --path .            # from a checkout of this repository
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

## Command-line tool

The Ubidium runs a `TimingSystem` gRPC server on port **443**. The `ubidium`
binary talks to it through subcommands; `ubidium --help` lists them and
`ubidium <subcommand> --help` shows each one's options.

| Subcommand | What it does |
|------------|--------------|
| `systeminfo` (alias `info`) | Print a summary of the device's system info |
| `status` | Dump every field of the full `Status` message |
| `passings` | Listen for live passings until Ctrl-C |
| `stored-passings` | Download stored passings with any `CmdGetPassings` selector |
| `client` | Full client: press `KEY_BACK` twice, then stream status + passings |
| `ping` | Check reachability with the `Ping` RPC (SDK ≥ 0.9.11) |
| `server` | Run a `TimingServer` that Ubidiums connect out to |

Every subcommand that talks to a Ubidium needs `--host <host[:port]>` (the port
defaults to 443) and `--device-id <id>` (e.g. `U-40153`). The device ID is also
used as the TLS server name (see [TLS](#tls) above). Both can come from the
environment instead:

```bash
export UBIDIUM_HOST=192.168.1.112
export UBIDIUM_DEVICE_ID=U-40153
ubidium systeminfo
```

From a checkout you can run the tool without installing it, using
`cargo run --` in place of `ubidium`, e.g.
`cargo run -- systeminfo --host 192.168.1.112 --device-id U-40153`.

### Get Ubidium system info

There is no dedicated "system info" RPC — the device identity (ID, name,
firmware version, customer number, temperature, GPS, battery, ...) is delivered
through the **status stream**. `systeminfo` opens `OpenStatusStream`, sends one
`CmdGetStatus`, and prints the first `Status` snapshot it receives.

```bash
ubidium systeminfo --host 192.168.1.112:443 --device-id U-40153
```

### Get full Ubidium status

`status` is the verbose counterpart to `systeminfo`: it fetches the same
snapshot and dumps **every field** (active/passive equipment, GPS, batteries,
power, firmware update, ...).

```bash
ubidium status --host 192.168.1.112:443 --device-id U-40153
```

### Listen for passings

`passings` opens `OpenPassingStream`, sends one `CmdGetPassings` requesting
every new passing from now on, and prints each transponder passing as it
arrives until you stop it with Ctrl-C.

```bash
ubidium passings --host 192.168.1.112:443 --device-id U-40153
```

### Download stored passings

`stored-passings` exposes the **full** `CmdGetPassings` request. Pick exactly
one **start** and one **end**:

- start: `--start-ref <now|current-file|first-file>`, `--start-id <ID>`, or
  `--start-no <FILE:NO>`
- end: `--end-ref <until-stopped|currently-existing|end-of-file>` or
  `--end-count <N>`

With `--end-ref until-stopped` the stream keeps delivering future passings until
you press Ctrl-C; the other end selectors stop on their own.

```bash
# all stored passings of the current file:
ubidium stored-passings --host 192.168.1.112:443 --device-id U-40153 \
    --start-ref current-file --end-ref currently-existing

# 50 passings starting at ID 1000:
ubidium stored-passings --host 192.168.1.112:443 --device-id U-40153 \
    --start-id 1000 --end-count 50

# from file 3 / passing 1 to the end of that file:
ubidium stored-passings --host 192.168.1.112:443 --device-id U-40153 \
    --start-no 3:1 --end-ref end-of-file
```

### Full client (port of the Go `cmd/client`)

`client` is a port of the Go SDK's client (fixed-IP mode): it presses
`KEY_BACK` twice via the unary `PressKey` RPC, then opens **both** the status
and passing streams and prints updates from each until Ctrl-C.

```bash
ubidium client --host 192.168.1.112:443 --device-id U-40153
```

### Ping

`ping` calls the unary `Ping` RPC (added in SDK v0.9.11) and prints the
round-trip time of each call. `--count 0` pings until Ctrl-C.

```bash
ubidium ping --host 192.168.1.112:443 --device-id U-40153 --count 5 --interval 1
```

### TimingServer (port of the Go `cmd/server`)

`server` flips the roles: it implements the `TimingServer` gRPC service that
**Ubidiums connect out to**. Configure a Ubidium's "custom server" with this
machine's address and port. For each stream a Ubidium opens, the server
subscribes to passings (and acknowledges them) and to status updates, and sends
one `PressKey` command. It serves TLS using the bundled example server
certificate (`certs/example-server/`).

`--ping-interval <SECS>` also sends a `CmdPing` keep-alive on each command
stream every `SECS` seconds and prints the `CmdPong` replies (SDK ≥ 0.9.11
firmware; off by default).

```bash
# listen on all interfaces, port 8443 (the default)
ubidium server --listen 0.0.0.0:8443

# same, with a keep-alive ping every 30 s
ubidium server --listen 0.0.0.0:8443 --ping-interval 30
```

## Examples

The same functionality is also available as standalone, single-file examples
in [`examples/`](examples/), which are handy as a starting point for your own
code:

```bash
cargo run --example systeminfo -- --host 192.168.1.112:443 --device-id U-40153
```

Available examples: `systeminfo`, `status`, `passings`, `stored_passings`,
`client`, `server`.

## Layout

| Path | Purpose |
|------|---------|
| `proto/` | Ubidium `.proto` files (from ubidium-sdk v0.9.12) |
| `build.rs` | Compiles the protos with `tonic-prost-build` |
| `src/lib.rs` | `ubidium::pb` generated types + clients, bundled CA cert |
| `src/bin/ubidium/` | The `ubidium` command-line tool |
| `examples/systeminfo.rs` | Example: connect and print system info |
| `examples/passings.rs` | Example: connect and listen for passings |
| `examples/stored_passings.rs` | Example: download stored passings by ID range |
| `examples/status.rs` | Example: dump the full Ubidium status |
| `examples/client.rs` | Example: full client, port of the Go `cmd/client` |
| `examples/server.rs` | Example: `TimingServer`, port of the Go `cmd/server` |
| `certs/example-server/` | Example server certificate + key (for `ubidium server`) |
| `certs/cacert.pem` | `RACE RESULT TD proxy` CA certificate |

## License

Licensed under the [MIT License](LICENSE).

The MIT license covers the code in this repository. The `.proto` files in
[`proto/`](proto/) and the certificates in [`certs/`](certs/) come from Race
Result's ubidium-sdk and remain subject to Race Result's terms.
