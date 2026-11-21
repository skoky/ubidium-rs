//! `ubidium` — command-line tool for the Race Result **Ubidium** timing
//! decoder, built on the `ubidium` gRPC bindings.
//!
//! Subcommands:
//!
//! ```text
//! ubidium systeminfo      --host <host[:port]> --device-id <id>
//! ubidium status          --host <host[:port]> --device-id <id>
//! ubidium passings        --host <host[:port]> --device-id <id>
//! ubidium stored-passings --host <host[:port]> --device-id <id> --start-... --end-...
//! ubidium client          --host <host[:port]> --device-id <id>
//! ubidium ping            --host <host[:port]> --device-id <id> [--count N]
//! ubidium server          [--listen <addr>] [--ping-interval <secs>]
//! ```
//!
//! `--host`, `--device-id` and `--ca-cert` can also be given through the
//! `UBIDIUM_HOST`, `UBIDIUM_DEVICE_ID` and `UBIDIUM_CA_CERT` environment
//! variables.

mod client;
mod print;
mod server;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};

/// Command-line tool for the Race Result Ubidium timing decoder.
#[derive(Parser, Debug)]
#[command(
    name = "ubidium",
    version,
    author = "skokys@gmail.com",
    // clap's default help omits the author; this is the default plus a
    // "name version / author" header.
    help_template = "\
{before-help}{name} {version}
Author: {author}

{about-with-newline}
{usage-heading} {usage}

{all-args}{after-help}"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Which Ubidium to talk to (used by every client subcommand).
#[derive(Args, Debug, Clone)]
pub struct Target {
    /// Ubidium host or IP, optionally with `:port` (defaults to port 443).
    #[arg(long, env = "UBIDIUM_HOST")]
    pub host: String,

    /// Device ID, e.g. `U-40153` (also used as the TLS server name).
    #[arg(long, env = "UBIDIUM_DEVICE_ID")]
    pub device_id: String,

    /// PEM file with the CA certificate to trust instead of the bundled
    /// Race Result CA (e.g. for a test server or a TLS proxy).
    #[arg(long, env = "UBIDIUM_CA_CERT", value_name = "PEM")]
    pub ca_cert: Option<std::path::PathBuf>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Print a summary of the device's system info (one status snapshot).
    #[command(alias = "info")]
    Systeminfo(#[command(flatten)] Target),

    /// Dump every field of the device's full status.
    Status(#[command(flatten)] Target),

    /// Listen for live passings until Ctrl-C.
    Passings(#[command(flatten)] Target),

    /// Download stored passings using any `CmdGetPassings` start/end selector.
    #[command(alias = "stored_passings")]
    StoredPassings(client::StoredPassingsArgs),

    /// Full client: press BACK twice, then stream status and passings until Ctrl-C.
    Client(#[command(flatten)] Target),

    /// Check that the device is reachable using the `Ping` RPC (SDK >= 0.9.11).
    Ping(client::PingArgs),

    /// Run a `TimingServer` that Ubidiums connect out to.
    Server(server::ServerArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Systeminfo(t) => client::systeminfo(&t).await,
        Command::Status(t) => client::status(&t).await,
        Command::Passings(t) => client::passings(&t).await,
        Command::StoredPassings(args) => client::stored_passings(&args).await,
        Command::Client(t) => client::client(&t).await,
        Command::Ping(args) => client::ping(&args).await,
        Command::Server(args) => server::run(&args).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_is_well_formed() {
        Cli::command().debug_assert();
    }

    #[test]
    fn stored_passings_requires_one_start_and_one_end() {
        let base = [
            "ubidium",
            "stored-passings",
            "--host",
            "h",
            "--device-id",
            "U-1",
        ];
        let parse = |extra: &[&str]| Cli::try_parse_from(base.iter().chain(extra.iter()).copied());
        assert!(parse(&["--start-id", "1", "--end-count", "5"]).is_ok());
        assert!(parse(&["--start-no", "3:1", "--end-ref", "end-of-file"]).is_ok());
        assert!(parse(&["--end-count", "5"]).is_err(), "missing start");
        assert!(parse(&["--start-id", "1"]).is_err(), "missing end");
        assert!(
            parse(&["--start-id", "1", "--start-ref", "now", "--end-count", "5"]).is_err(),
            "two starts"
        );
    }
}
