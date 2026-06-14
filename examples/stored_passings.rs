//! Example: connect to a Race Result **Ubidium** timing decoder over gRPC/TLS
//! and download passings using the **full** `CmdGetPassings` request — every
//! `start` and `end` selector the protocol offers.
//!
//! `CmdGetPassings` selects *where to start* and *where to stop*:
//!
//! - **start** (exactly one of):
//!   - `--start-ref <now|current-file|first-file>` — a `StartReference`,
//!   - `--start-id <ID>` — begin at a specific passing ID,
//!   - `--start-no <FILE:NO>` — begin at a specific file + passing number;
//! - **end** (exactly one of):
//!   - `--end-ref <until-stopped|currently-existing|end-of-file>` — an
//!     `EndReference`,
//!   - `--end-count <N>` — stop after at most N passings.
//!
//! With `--end-ref until-stopped` the stream stays open for future passings;
//! press Ctrl-C to stop. The other end selectors terminate on their own.
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
//! # all stored passings of the current file:
//! stored_passings --host 192.168.1.112:443 --device-id U-40153 \
//!     --start-ref current-file --end-ref currently-existing
//!
//! # 50 passings starting at ID 1000:
//! stored_passings --host 192.168.1.112:443 --device-id U-40153 \
//!     --start-id 1000 --end-count 50
//!
//! # from file 3 / passing 1 to the end of that file:
//! stored_passings --host 192.168.1.112:443 --device-id U-40153 \
//!     --start-no 3:1 --end-ref end-of-file
//! ```

use std::str::FromStr;

use anyhow::{Context, Result, bail};
use clap::{ArgGroup, Parser, ValueEnum};
use tokio_stream::wrappers::ReceiverStream;

use ubidium::pb::{
    CmdGetPassings, PassingRequest, cmd_get_passings, passing, passing_request, passing_response,
    timing_system_client::TimingSystemClient, transponder,
};

/// `StartReference` choices for `--start-ref`.
#[derive(Clone, Copy, Debug, ValueEnum)]
enum StartRef {
    /// Only new passings from when the request is processed.
    Now,
    /// From the beginning of the current file.
    CurrentFile,
    /// From the beginning of the first existing file.
    FirstFile,
}

/// `EndReference` choices for `--end-ref`.
#[derive(Clone, Copy, Debug, ValueEnum)]
enum EndRef {
    /// Keep streaming future passings until stopped (Ctrl-C).
    UntilStopped,
    /// Stop at the newest passing that exists when the request is processed.
    CurrentlyExisting,
    /// Stop at the end of the file the start refers to.
    EndOfFile,
}

/// A `Passing.No` (file + passing number), parsed from `FILE:NO`.
#[derive(Clone, Copy, Debug)]
struct PassingNo {
    file: u32,
    no: u32,
}

impl FromStr for PassingNo {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (file, no) = s
            .split_once(':')
            .ok_or_else(|| format!("expected FILE:NO, got {s:?}"))?;
        Ok(PassingNo {
            file: file.parse().map_err(|_| format!("invalid file number: {file:?}"))?,
            no: no.parse().map_err(|_| format!("invalid passing number: {no:?}"))?,
        })
    }
}

/// Download passings from a Race Result Ubidium using any `CmdGetPassings` option.
#[derive(Parser, Debug)]
#[command(about, long_about = None)]
#[command(group(ArgGroup::new("start").required(true).args(["start_ref", "start_id", "start_no"])))]
#[command(group(ArgGroup::new("end").required(true).args(["end_ref", "end_count"])))]
struct Cli {
    /// Ubidium host or IP, optionally with `:port` (defaults to port 443).
    #[arg(long)]
    host: String,

    /// Device ID, e.g. `U-40153` (also used as the TLS server name).
    #[arg(long)]
    device_id: String,

    /// Start at a reference point (mutually exclusive with --start-id/--start-no).
    #[arg(long, value_enum)]
    start_ref: Option<StartRef>,

    /// Start at a specific passing ID.
    #[arg(long)]
    start_id: Option<u64>,

    /// Start at a specific file + passing number, given as FILE:NO.
    #[arg(long, value_name = "FILE:NO")]
    start_no: Option<PassingNo>,

    /// Stop at a reference point (mutually exclusive with --end-count).
    #[arg(long, value_enum)]
    end_ref: Option<EndRef>,

    /// Stop after at most N passings.
    #[arg(long, value_name = "N")]
    end_count: Option<u32>,
}

impl Cli {
    /// Build the `start` selector from whichever option was given. The clap
    /// `ArgGroup` guarantees exactly one is present.
    fn start(&self) -> cmd_get_passings::Start {
        if let Some(r) = self.start_ref {
            let r = match r {
                StartRef::Now => cmd_get_passings::StartReference::Now,
                StartRef::CurrentFile => cmd_get_passings::StartReference::BeginningOfCurrentFile,
                StartRef::FirstFile => cmd_get_passings::StartReference::BeginningOfFirstFile,
            };
            cmd_get_passings::Start::StartRef(r as i32)
        } else if let Some(id) = self.start_id {
            cmd_get_passings::Start::Id(id)
        } else {
            let n = self.start_no.expect("clap ArgGroup guarantees a start selector");
            cmd_get_passings::Start::No(passing::No { file: n.file, no: n.no })
        }
    }

    /// Build the `end` selector. The clap `ArgGroup` guarantees exactly one.
    fn end(&self) -> cmd_get_passings::End {
        if let Some(r) = self.end_ref {
            let r = match r {
                EndRef::UntilStopped => cmd_get_passings::EndReference::UntilStopped,
                EndRef::CurrentlyExisting => cmd_get_passings::EndReference::CurrentlyExisting,
                EndRef::EndOfFile => cmd_get_passings::EndReference::EndOfFile,
            };
            cmd_get_passings::End::EndRef(r as i32)
        } else {
            let count = self.end_count.expect("clap ArgGroup guarantees an end selector");
            cmd_get_passings::End::Count(count)
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let request = CmdGetPassings {
        start: Some(cli.start()),
        end: Some(cli.end()),
    };

    println!(
        "Connecting to {} (device id / TLS name: {}) ...",
        cli.host, cli.device_id
    );

    let channel = ubidium::connect(&cli.host, &cli.device_id)
        .await
        .map_err(|e| anyhow::anyhow!("could not connect to {}: {e}", cli.host))?;
    let mut client = TimingSystemClient::new(channel);

    // OpenPassingStream is bi-directional: we stream PassingRequests out and
    // receive PassingResponses back. We only need to send one request.
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(PassingRequest {
        cmd: Some(passing_request::Cmd::Get(request)),
    })
    .await
    .ok();

    let response = client
        .open_passing_stream(ReceiverStream::new(rx))
        .await
        .context("OpenPassingStream RPC failed")?;

    let mut stream = response.into_inner();

    println!("Downloading passings (press Ctrl-C to stop) ...\n");

    let mut count = 0u64;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\nInterrupted, shutting down.");
                break;
            }
            msg = stream.message() => {
                match msg.context("reading passing stream")? {
                    Some(resp) => match resp.response {
                        Some(passing_response::Response::Passing(p)) => {
                            print_passing(&p);
                            count += 1;
                        }
                        Some(passing_response::Response::Welcome(w)) => {
                            println!("Welcome: newest passing id {}, customer {}", w.current_id, w.cust_no);
                        }
                        Some(passing_response::Response::Error(err)) => {
                            bail!("Ubidium returned an error: {} (code {})", err.message, err.code);
                        }
                        None => eprintln!("Received empty passing response, ignoring."),
                    },
                    None => {
                        println!("Passing stream closed by the Ubidium.");
                        break;
                    }
                }
            }
        }
    }

    println!("\nDone. Received {count} passing(s).");

    // Dropping `tx` closes the outbound stream and lets the server tear down.
    drop(tx);
    Ok(())
}

/// Pretty-print a single passing.
fn print_passing(p: &ubidium::pb::Passing) {
    let kind = match &p.data {
        Some(passing::Data::Active(_)) => "active",
        Some(passing::Data::Passive(_)) => "passive",
        Some(passing::Data::Marker(_)) => "marker",
        None => "unknown",
    };

    let transponder_id = p.transponder.as_ref().map(|t| t.id.as_str()).unwrap_or("?");

    let no = p
        .no
        .as_ref()
        .map(|n| format!("file {} / no {}", n.file, n.no))
        .unwrap_or_else(|| "-".to_string());

    let when = p
        .time
        .as_ref()
        .and_then(|t| t.utc.as_ref())
        .map(|utc| format!("{}.{:09} UTC", utc.seconds, utc.nanos))
        .unwrap_or_else(|| "-".to_string());

    let input = p.src.as_ref().map(|s| s.input.as_str()).unwrap_or("-");

    println!(
        "[{kind}] id={} transponder={transponder_id} {no} hits={} rssi={} input={input} time={when}",
        p.id, p.hits, p.rssi
    );

    if let Some(transponder::Data::Active(a)) =
        p.transponder.as_ref().and_then(|t| t.data.as_ref())
    {
        println!(
            "        active transponder: battery={} temperature={}°C wakeups={}",
            a.battery, a.temperature, a.wakeup_counter
        );
    }
}
