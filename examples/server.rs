//! Example: a **TimingServer** that Ubidiums connect to — a Rust port of the Go
//! SDK's `cmd/server` example.
//!
//! Here the roles are reversed compared to the other examples: the Ubidium acts
//! as the gRPC *client* and connects out to this server (configure the Ubidium's
//! "custom server" with this machine's address and port). We implement the
//! `TimingServer` service and, for each stream a Ubidium opens, request data
//! and print what arrives:
//!
//! - **passing stream**: subscribe from the beginning of the current file until
//!   stopped, print each passing and acknowledge it,
//! - **status stream**: subscribe to continuous updates (pushed every 10s),
//! - **command stream**: send one `PressKey(KEY_BACK)` command and print the
//!   response.
//!
//! ## TLS
//!
//! The server presents the bundled example server certificate
//! (`certs/example-server/sdk_server.pem` + `.key`), matching the Go example.
//!
//! ## Usage
//!
//! ```text
//! server --listen <addr>
//!
//! # e.g. listen on all interfaces, port 8443
//! server --listen 0.0.0.0:8443
//! ```

use std::net::SocketAddr;
use std::pin::Pin;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

use ubidium::pb::{
    CmdAckPassing, CmdGetPassings, CmdGetStatus, CmdPressKey, CommandRequest, CommandResponse, Key,
    PassingRequest, PassingResponse, StatusRequest, StatusResponse, cmd_ack_passing,
    cmd_get_passings, command_request, command_response, passing, passing_request, passing_response,
    status_request, status_response,
    timing_server_server::{TimingServer, TimingServerServer},
    transponder,
};

/// Outbound stream type the server sends back to a Ubidium (the "requests").
type OutStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[derive(Default)]
struct UbidiumTimingServer;

#[tonic::async_trait]
impl TimingServer for UbidiumTimingServer {
    type OpenPassingStreamStream = OutStream<PassingRequest>;
    type OpenStatusStreamStream = OutStream<StatusRequest>;
    type OpenCommandStreamStream = OutStream<CommandRequest>;

    // Called by a Ubidium that connects to us.
    async fn open_passing_stream(
        &self,
        request: Request<Streaming<PassingResponse>>,
    ) -> Result<Response<Self::OpenPassingStreamStream>, Status> {
        let device_id = device_id_of(&request);
        let mut incoming = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<PassingRequest, Status>>(16);

        // Subscribe to all passings from the beginning of the current file.
        tx.send(Ok(PassingRequest {
            cmd: Some(passing_request::Cmd::Get(CmdGetPassings {
                start: Some(cmd_get_passings::Start::StartRef(
                    cmd_get_passings::StartReference::BeginningOfCurrentFile as i32,
                )),
                end: Some(cmd_get_passings::End::EndRef(
                    cmd_get_passings::EndReference::UntilStopped as i32,
                )),
            })),
        }))
        .await
        .ok();

        tokio::spawn(async move {
            let mut device_id = device_id;
            println!("Passing stream opened from {device_id}...");
            loop {
                match incoming.message().await {
                    Ok(Some(resp)) => match resp.response {
                        Some(passing_response::Response::Passing(p)) => {
                            // If the device-id header was missing, learn it from
                            // the passing's source and announce it.
                            if device_id == UNKNOWN {
                                if let Some(id) = p.src.as_ref().map(|s| &s.device_id) {
                                    if !id.is_empty() {
                                        device_id = id.clone();
                                        println!("Ubidium identified on passing stream: {device_id}");
                                    }
                                }
                            }
                            print_passing(&device_id, &p);
                            // Acknowledge the passing (drives the Ubidium's
                            // progress bar), like the Go server.
                            tx.send(Ok(PassingRequest {
                                cmd: Some(passing_request::Cmd::Ack(CmdAckPassing {
                                    latest: Some(cmd_ack_passing::Latest::Id(p.id)),
                                })),
                            }))
                            .await
                            .ok();
                        }
                        Some(passing_response::Response::Welcome(w)) => {
                            println!(
                                "Welcome from {device_id}: newest id {}, customer {}",
                                w.current_id, w.cust_no
                            );
                        }
                        Some(passing_response::Response::Error(e)) => {
                            println!("Passing error from {device_id}: {} ({})", e.message, e.code);
                        }
                        None => {}
                    },
                    Ok(None) => break,
                    Err(status) => {
                        eprintln!("passing stream error from {device_id}: {status}");
                        break;
                    }
                }
            }
            println!("Passing stream from {device_id} stopped.");
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn open_status_stream(
        &self,
        request: Request<Streaming<StatusResponse>>,
    ) -> Result<Response<Self::OpenStatusStreamStream>, Status> {
        let device_id = device_id_of(&request);
        let mut incoming = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<StatusRequest, Status>>(16);

        // Subscribe to continuous status updates, pushed at least every 10s.
        tx.send(Ok(StatusRequest {
            cmd: Some(status_request::Cmd::Get(CmdGetStatus {
                r#continue: true,
                push_time: Some(prost_types::Duration { seconds: 10, nanos: 0 }),
            })),
        }))
        .await
        .ok();

        tokio::spawn(async move {
            let mut device_id = device_id;
            println!("Status stream opened from {device_id}...");
            loop {
                match incoming.message().await {
                    Ok(Some(resp)) => match resp.response {
                        Some(status_response::Response::Status(s)) => {
                            // If the device-id header was missing, learn it from
                            // the status message and announce it.
                            if device_id == UNKNOWN {
                                if let Some(id) = s.id.as_ref() {
                                    if !id.is_empty() {
                                        device_id = id.clone();
                                        println!("Ubidium identified on status stream: {device_id}");
                                    }
                                }
                            }
                            let name = s.name.as_deref().unwrap_or("?");
                            let version = s.version.as_deref().unwrap_or("?");
                            println!("Status (update) from {device_id}: name={name} fw={version}");
                        }
                        Some(status_response::Response::Error(e)) => {
                            println!("Status error from {device_id}: {} ({})", e.message, e.code);
                        }
                        None => {}
                    },
                    Ok(None) => break,
                    Err(status) => {
                        eprintln!("status stream error from {device_id}: {status}");
                        break;
                    }
                }
            }
            println!("Status stream from {device_id} stopped.");
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn open_command_stream(
        &self,
        request: Request<Streaming<CommandResponse>>,
    ) -> Result<Response<Self::OpenCommandStreamStream>, Status> {
        let device_id = device_id_of(&request);
        let mut incoming = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<CommandRequest, Status>>(16);

        // Send a single command: press the BACK key on the Ubidium.
        tx.send(Ok(CommandRequest {
            id: 1,
            cmd: Some(command_request::Cmd::PressKey(CmdPressKey { key: Key::Back as i32 })),
        }))
        .await
        .ok();

        tokio::spawn(async move {
            println!("Command stream opened from {device_id}...");
            loop {
                match incoming.message().await {
                    Ok(Some(resp)) => {
                        let request_id = resp.request_id;
                        match resp.response {
                            Some(command_response::Response::PressKeyResponse(_)) => {
                                println!(
                                    "Received press-key response (request {request_id}) from {device_id}"
                                );
                            }
                            Some(command_response::Response::Error(e)) => {
                                println!("Command error from {device_id}: {} ({})", e.message, e.code);
                            }
                            other => {
                                println!("Received command response from {device_id}: {other:?}");
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(status) => {
                        eprintln!("command stream error from {device_id}: {status}");
                        break;
                    }
                }
            }
            println!("Command stream from {device_id} stopped.");
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// Placeholder used until a connecting Ubidium's device ID is known.
const UNKNOWN: &str = "unknown";

/// Read the `device-id` a Ubidium sends in the request metadata.
fn device_id_of<T>(request: &Request<T>) -> String {
    request
        .metadata()
        .get("device-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or(UNKNOWN)
        .to_string()
}

fn print_passing(device_id: &str, p: &ubidium::pb::Passing) {
    let kind = match &p.data {
        Some(passing::Data::Active(_)) => "active",
        Some(passing::Data::Passive(_)) => "passive",
        Some(passing::Data::Marker(_)) => "marker",
        None => "unknown",
    };
    let transponder_id = p.transponder.as_ref().map(|t| t.id.as_str()).unwrap_or("?");
    println!(
        "Got {kind} passing from {device_id}: id={} transponder={transponder_id} hits={} rssi={}",
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

/// Listen address to bind the TimingServer to.
#[derive(Parser, Debug)]
#[command(about, long_about = None)]
struct Cli {
    /// Address to listen on, e.g. `0.0.0.0:8443` or `[::]:443`.
    #[arg(long, default_value = "0.0.0.0:8443")]
    listen: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let addr: SocketAddr = cli.listen.parse().context("invalid --listen address")?;

    // Server identity: the bundled example server certificate + key.
    let identity = Identity::from_pem(
        include_bytes!("../certs/example-server/sdk_server.pem"),
        include_bytes!("../certs/example-server/sdk_server.key"),
    );

    println!("TimingServer listening on {addr} (TLS) — point a Ubidium's custom server here.");

    Server::builder()
        .tls_config(ServerTlsConfig::new().identity(identity))
        .context("TLS configuration failed")?
        .add_service(TimingServerServer::new(UbidiumTimingServer))
        .serve(addr)
        .await
        .context("server error")?;

    Ok(())
}
