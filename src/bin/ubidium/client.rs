//! Subcommands that connect *to* a Ubidium's `TimingSystem` gRPC server.

use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use clap::{ArgGroup, Args, ValueEnum};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;
use tonic::transport::Channel;

use ubidium::pb::{
    CmdGetPassings, CmdGetStatus, CmdPing, CmdPressKey, Key, PassingRequest, PassingResponse,
    Status, StatusRequest, StatusResponse, cmd_get_passings, passing, passing_request,
    passing_response, status_request, status_response, timing_system_client::TimingSystemClient,
};

use crate::{Target, print};

/// Connect to the target Ubidium and return a `TimingSystem` client.
async fn connect(t: &Target) -> Result<TimingSystemClient<Channel>> {
    println!(
        "Connecting to {} (device id / TLS name: {}) ...",
        t.host, t.device_id
    );
    let channel = ubidium::connect(&t.host, &t.device_id)
        .await
        .map_err(|e| anyhow!("could not connect to {}: {e}", t.host))?;
    Ok(TimingSystemClient::new(channel))
}

/// Print the `device-id` header the Ubidium sends with a stream response.
fn print_device_id_header<T>(response: &tonic::Response<T>) {
    if let Some(id) = response
        .metadata()
        .get("device-id")
        .and_then(|v| v.to_str().ok())
    {
        println!("Server reported device-id header: {id}");
    }
}

fn secs(secs: i64) -> prost_types::Duration {
    prost_types::Duration {
        seconds: secs,
        nanos: 0,
    }
}

// ---------------------------------------------------------------------------
// systeminfo / status
// ---------------------------------------------------------------------------

/// Open the status stream, request a single snapshot and return it.
async fn status_snapshot(t: &Target) -> Result<Status> {
    let mut client = connect(t).await?;

    // OpenStatusStream is bi-directional; we only need one request. Keep `tx`
    // alive until we have our answer so the outbound half stays open.
    let (tx, rx) = mpsc::channel(1);
    tx.send(StatusRequest {
        cmd: Some(status_request::Cmd::Get(CmdGetStatus {
            r#continue: false,
            push_time: None,
        })),
    })
    .await
    .ok();

    let response = client
        .open_status_stream(ReceiverStream::new(rx))
        .await
        .context("OpenStatusStream RPC failed")?;
    print_device_id_header(&response);
    let mut stream = response.into_inner();

    while let Some(msg) = stream.message().await.context("reading status stream")? {
        match msg.response {
            Some(status_response::Response::Status(status)) => return Ok(status),
            Some(status_response::Response::Error(err)) => {
                bail!(
                    "Ubidium returned an error: {} (code {})",
                    err.message,
                    err.code
                );
            }
            None => eprintln!("Received empty status response, waiting for the next one..."),
        }
    }
    drop(tx);
    bail!("status stream closed before a status snapshot arrived")
}

/// `ubidium systeminfo`: print a summary of the status snapshot.
pub async fn systeminfo(t: &Target) -> Result<()> {
    print::system_info(&status_snapshot(t).await?);
    Ok(())
}

/// `ubidium status`: dump every field of the status snapshot.
pub async fn status(t: &Target) -> Result<()> {
    let status = status_snapshot(t).await?;
    println!("\n=== Ubidium full status ===");
    println!("{status:#?}");
    println!("===========================");
    Ok(())
}

// ---------------------------------------------------------------------------
// passings / stored-passings
// ---------------------------------------------------------------------------

/// Open the passing stream with `request` and print passings until the stream
/// ends or Ctrl-C. Returns the number of passings received.
async fn stream_passings(t: &Target, request: CmdGetPassings) -> Result<u64> {
    let mut client = connect(t).await?;

    // Keep `tx` alive for the lifetime of the stream; dropping it would signal
    // "no more requests" to the Ubidium.
    let (tx, rx) = mpsc::channel(1);
    tx.send(PassingRequest {
        cmd: Some(passing_request::Cmd::Get(request)),
    })
    .await
    .ok();

    let response = client
        .open_passing_stream(ReceiverStream::new(rx))
        .await
        .context("OpenPassingStream RPC failed")?;
    print_device_id_header(&response);
    let mut stream = response.into_inner();

    println!("Receiving passings (press Ctrl-C to stop) ...\n");

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
                            print::passing_detailed(&p);
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

    drop(tx);
    Ok(count)
}

/// `ubidium passings`: every new passing from now on, until Ctrl-C.
pub async fn passings(t: &Target) -> Result<()> {
    stream_passings(
        t,
        CmdGetPassings {
            start: Some(cmd_get_passings::Start::StartRef(
                cmd_get_passings::StartReference::Now as i32,
            )),
            end: Some(cmd_get_passings::End::EndRef(
                cmd_get_passings::EndReference::UntilStopped as i32,
            )),
        },
    )
    .await?;
    Ok(())
}

/// `StartReference` choices for `--start-ref`.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum StartRef {
    /// Only new passings from when the request is processed.
    Now,
    /// From the beginning of the current file.
    CurrentFile,
    /// From the beginning of the first existing file.
    FirstFile,
}

/// `EndReference` choices for `--end-ref`.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum EndRef {
    /// Keep streaming future passings until stopped (Ctrl-C).
    UntilStopped,
    /// Stop at the newest passing that exists when the request is processed.
    CurrentlyExisting,
    /// Stop at the end of the file the start refers to.
    EndOfFile,
}

/// A `Passing.No` (file + passing number), parsed from `FILE:NO`.
#[derive(Clone, Copy, Debug)]
pub struct PassingNo {
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
            file: file
                .parse()
                .map_err(|_| format!("invalid file number: {file:?}"))?,
            no: no
                .parse()
                .map_err(|_| format!("invalid passing number: {no:?}"))?,
        })
    }
}

/// Arguments of `ubidium stored-passings`: exactly one start and one end.
#[derive(Args, Debug)]
#[command(group(ArgGroup::new("start").required(true).args(["start_ref", "start_id", "start_no"])))]
#[command(group(ArgGroup::new("end").required(true).args(["end_ref", "end_count"])))]
pub struct StoredPassingsArgs {
    #[command(flatten)]
    target: Target,

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

impl StoredPassingsArgs {
    /// The `start` selector; the clap `ArgGroup` guarantees exactly one is set.
    fn start(&self) -> cmd_get_passings::Start {
        use cmd_get_passings::{Start, StartReference};
        if let Some(r) = self.start_ref {
            let r = match r {
                StartRef::Now => StartReference::Now,
                StartRef::CurrentFile => StartReference::BeginningOfCurrentFile,
                StartRef::FirstFile => StartReference::BeginningOfFirstFile,
            };
            Start::StartRef(r as i32)
        } else if let Some(id) = self.start_id {
            Start::Id(id)
        } else {
            let n = self
                .start_no
                .expect("clap ArgGroup guarantees a start selector");
            Start::No(passing::No {
                file: n.file,
                no: n.no,
            })
        }
    }

    /// The `end` selector; the clap `ArgGroup` guarantees exactly one is set.
    fn end(&self) -> cmd_get_passings::End {
        use cmd_get_passings::{End, EndReference};
        if let Some(r) = self.end_ref {
            let r = match r {
                EndRef::UntilStopped => EndReference::UntilStopped,
                EndRef::CurrentlyExisting => EndReference::CurrentlyExisting,
                EndRef::EndOfFile => EndReference::EndOfFile,
            };
            End::EndRef(r as i32)
        } else {
            let count = self
                .end_count
                .expect("clap ArgGroup guarantees an end selector");
            End::Count(count)
        }
    }
}

/// `ubidium stored-passings`: download passings with any start/end selector.
pub async fn stored_passings(args: &StoredPassingsArgs) -> Result<()> {
    let request = CmdGetPassings {
        start: Some(args.start()),
        end: Some(args.end()),
    };
    let count = stream_passings(&args.target, request).await?;
    println!("\nDone. Received {count} passing(s).");
    Ok(())
}

// ---------------------------------------------------------------------------
// client
// ---------------------------------------------------------------------------

/// `ubidium client`: port of the Go SDK's `cmd/client` (fixed-IP mode).
pub async fn client(t: &Target) -> Result<()> {
    let mut client = connect(t).await?;

    // Control the Ubidium via its keypad, like the Go example: press BACK twice.
    for i in 0..2 {
        if i > 0 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        client
            .press_key(CmdPressKey {
                key: Key::Back as i32,
            })
            .await
            .context("PressKey failed")?;
    }

    // Status stream: continuous updates, pushed at least every 5s.
    let (status_tx, status_rx) = mpsc::channel(1);
    status_tx
        .send(StatusRequest {
            cmd: Some(status_request::Cmd::Get(CmdGetStatus {
                r#continue: true,
                push_time: Some(secs(5)),
            })),
        })
        .await
        .ok();
    let status_stream = client
        .open_status_stream(ReceiverStream::new(status_rx))
        .await
        .context("OpenStatusStream failed")?
        .into_inner();

    // Passing stream: from the beginning of the current file, until stopped.
    let (passing_tx, passing_rx) = mpsc::channel(1);
    passing_tx
        .send(PassingRequest {
            cmd: Some(passing_request::Cmd::Get(CmdGetPassings {
                start: Some(cmd_get_passings::Start::StartRef(
                    cmd_get_passings::StartReference::BeginningOfCurrentFile as i32,
                )),
                end: Some(cmd_get_passings::End::EndRef(
                    cmd_get_passings::EndReference::UntilStopped as i32,
                )),
            })),
        })
        .await
        .ok();
    let passing_stream = client
        .open_passing_stream(ReceiverStream::new(passing_rx))
        .await
        .context("OpenPassingStream failed")?
        .into_inner();

    let status_task = tokio::spawn(read_status(status_stream));
    let passing_task = tokio::spawn(read_passings(passing_stream));

    println!("Streaming status and passings (press Ctrl-C to stop) ...\n");
    tokio::signal::ctrl_c().await.ok();
    println!("\nInterrupted, shutting down.");

    // Dropping the request senders closes the outbound halves, ending the
    // streams server-side; then stop the reader tasks.
    drop(status_tx);
    drop(passing_tx);
    status_task.abort();
    passing_task.abort();
    Ok(())
}

async fn read_status(mut stream: Streaming<StatusResponse>) {
    loop {
        match stream.message().await {
            Ok(Some(msg)) => match msg.response {
                Some(status_response::Response::Status(s)) => print::status_line(&s),
                Some(status_response::Response::Error(e)) => {
                    println!("Status error: {} ({})", e.message, e.code);
                }
                None => {}
            },
            Ok(None) => break,
            Err(status) => {
                eprintln!("status stream ended: {status}");
                break;
            }
        }
    }
}

async fn read_passings(mut stream: Streaming<PassingResponse>) {
    loop {
        match stream.message().await {
            Ok(Some(msg)) => match msg.response {
                Some(passing_response::Response::Passing(p)) => print::passing_short("", &p),
                Some(passing_response::Response::Welcome(w)) => {
                    println!(
                        "Welcome: newest passing id {}, customer {}",
                        w.current_id, w.cust_no
                    );
                }
                Some(passing_response::Response::Error(e)) => {
                    println!("Passing error: {} ({})", e.message, e.code);
                }
                None => {}
            },
            Ok(None) => break,
            Err(status) => {
                eprintln!("passing stream ended: {status}");
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ping
// ---------------------------------------------------------------------------

/// Arguments of `ubidium ping`.
#[derive(Args, Debug)]
pub struct PingArgs {
    #[command(flatten)]
    target: Target,

    /// Number of pings to send (0 = until Ctrl-C).
    #[arg(long, short = 'c', default_value_t = 4)]
    count: u32,

    /// Seconds to wait between pings.
    #[arg(long, short = 'i', default_value_t = 1.0)]
    interval: f64,
}

/// `ubidium ping`: call the unary `Ping` RPC (added in SDK v0.9.11) and print
/// the round-trip time of each call.
pub async fn ping(args: &PingArgs) -> Result<()> {
    if !(args.interval.is_finite() && args.interval >= 0.0) {
        bail!("--interval must be a non-negative number of seconds");
    }
    let interval = Duration::from_secs_f64(args.interval);
    let mut client = connect(&args.target).await?;

    let (mut sent, mut ok) = (0u32, 0u32);
    loop {
        if args.count != 0 && sent >= args.count {
            break;
        }
        if sent > 0 {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => { println!(); break; }
                _ = tokio::time::sleep(interval) => {}
            }
        }
        sent += 1;
        let started = Instant::now();
        match client.ping(CmdPing {}).await {
            Ok(_) => {
                ok += 1;
                println!(
                    "pong from {}: seq={sent} time={:.1} ms",
                    args.target.device_id,
                    started.elapsed().as_secs_f64() * 1000.0
                );
            }
            Err(status) => println!("ping seq={sent} failed: {status}"),
        }
    }

    println!("{sent} ping(s) sent, {ok} pong(s) received");
    if ok == 0 && sent > 0 {
        bail!("no pong received from {}", args.target.device_id);
    }
    Ok(())
}
