//! `ubidium server`: a `TimingServer` that Ubidiums connect out to — a Rust
//! port of the Go SDK's `cmd/server` example.
//!
//! The Ubidium acts as the gRPC *client* here (configure its "custom server"
//! with this machine's address and port). For each stream a Ubidium opens:
//!
//! - **passing stream**: subscribe from the beginning of the current file until
//!   stopped, print each passing and acknowledge it,
//! - **status stream**: subscribe to continuous updates (pushed every 10s),
//! - **command stream**: send one `PressKey(KEY_BACK)` command, optionally
//!   followed by periodic `CmdPing` keep-alives (SDK >= 0.9.11), and print the
//!   responses.

use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Args;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

use ubidium::pb::{
    CmdAckPassing, CmdGetPassings, CmdGetStatus, CmdPing, CmdPressKey, CommandRequest,
    CommandResponse, Key, PassingRequest, PassingResponse, StatusRequest, StatusResponse,
    cmd_ack_passing, cmd_get_passings, command_request, command_response, passing_request,
    passing_response, status_request, status_response,
    timing_server_server::{TimingServer, TimingServerServer},
};

use crate::print;

/// Arguments of `ubidium server`.
#[derive(Args, Debug)]
pub struct ServerArgs {
    /// Address to listen on, e.g. `0.0.0.0:8443` or `[::]:443`.
    #[arg(long, default_value = "0.0.0.0:8443")]
    listen: String,

    /// Send a `CmdPing` keep-alive on each command stream every N seconds
    /// (0 = disabled). Requires Ubidium firmware with SDK >= 0.9.11.
    #[arg(long, value_name = "SECS", default_value_t = 0)]
    ping_interval: u64,
}

/// Outbound stream type the server sends back to a Ubidium (the "requests").
type OutStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// Placeholder used until a connecting Ubidium's device ID is known.
const UNKNOWN: &str = "unknown";

struct UbidiumTimingServer {
    ping_interval: Option<Duration>,
}

/// Read the `device-id` a Ubidium sends in the request metadata.
fn device_id_of<T>(request: &Request<T>) -> String {
    request
        .metadata()
        .get("device-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or(UNKNOWN)
        .to_string()
}

/// If the device ID is still unknown, learn it from `candidate`.
fn learn_device_id(device_id: &mut String, candidate: Option<&String>, stream: &str) {
    if device_id == UNKNOWN {
        if let Some(id) = candidate.filter(|id| !id.is_empty()) {
            *device_id = id.clone();
            println!("Ubidium identified on {stream} stream: {device_id}");
        }
    }
}

#[tonic::async_trait]
impl TimingServer for UbidiumTimingServer {
    type OpenPassingStreamStream = OutStream<PassingRequest>;
    type OpenStatusStreamStream = OutStream<StatusRequest>;
    type OpenCommandStreamStream = OutStream<CommandRequest>;

    async fn open_passing_stream(
        &self,
        request: Request<Streaming<PassingResponse>>,
    ) -> Result<Response<Self::OpenPassingStreamStream>, Status> {
        let mut device_id = device_id_of(&request);
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
            println!("Passing stream opened from {device_id}...");
            loop {
                match incoming.message().await {
                    Ok(Some(resp)) => match resp.response {
                        Some(passing_response::Response::Passing(p)) => {
                            learn_device_id(
                                &mut device_id,
                                p.src.as_ref().map(|s| &s.device_id),
                                "passing",
                            );
                            println!(
                                "{}",
                                print::passing_short(
                                    &format!("Got passing from {device_id}: "),
                                    &p
                                )
                            );
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
                        Some(passing_response::Response::Welcome(w)) => println!(
                            "Welcome from {device_id}: newest id {}, customer {}",
                            w.current_id, w.cust_no
                        ),
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
        let mut device_id = device_id_of(&request);
        let mut incoming = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<StatusRequest, Status>>(16);

        // Subscribe to continuous status updates, pushed at least every 10s.
        tx.send(Ok(StatusRequest {
            cmd: Some(status_request::Cmd::Get(CmdGetStatus {
                r#continue: true,
                push_time: Some(prost_types::Duration {
                    seconds: 10,
                    nanos: 0,
                }),
            })),
        }))
        .await
        .ok();

        tokio::spawn(async move {
            // Keep `tx` alive with the reader so the outbound stream stays open.
            let _tx = tx;
            println!("Status stream opened from {device_id}...");
            loop {
                match incoming.message().await {
                    Ok(Some(resp)) => match resp.response {
                        Some(status_response::Response::Status(s)) => {
                            learn_device_id(&mut device_id, s.id.as_ref(), "status");
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
            cmd: Some(command_request::Cmd::PressKey(CmdPressKey {
                key: Key::Back as i32,
            })),
        }))
        .await
        .ok();

        // Optional keep-alive: periodic CmdPing on the command stream. The task
        // ends once the stream is gone (sending fails).
        if let Some(every) = self.ping_interval {
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut id = 1u32;
                loop {
                    tokio::time::sleep(every).await;
                    id = id.wrapping_add(1).max(2);
                    let ping = CommandRequest {
                        id,
                        cmd: Some(command_request::Cmd::Ping(CmdPing {})),
                    };
                    if tx.send(Ok(ping)).await.is_err() {
                        break;
                    }
                }
            });
        }

        tokio::spawn(async move {
            let _tx = tx;
            println!("Command stream opened from {device_id}...");
            loop {
                match incoming.message().await {
                    Ok(Some(resp)) => {
                        let request_id = resp.request_id;
                        match resp.response {
                            Some(command_response::Response::PressKeyResponse(_)) => println!(
                                "Received press-key response (request {request_id}) from {device_id}"
                            ),
                            Some(command_response::Response::Pong(_)) => {
                                println!("Received pong (request {request_id}) from {device_id}")
                            }
                            Some(command_response::Response::Error(e)) => {
                                println!(
                                    "Command error from {device_id}: {} ({})",
                                    e.message, e.code
                                );
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

/// Run the TimingServer until the process is stopped (Ctrl-C).
pub async fn run(args: &ServerArgs) -> Result<()> {
    let addr: SocketAddr = args.listen.parse().context("invalid --listen address")?;

    // Server identity: the bundled example server certificate + key.
    let identity = Identity::from_pem(
        include_bytes!("../../../certs/example-server/sdk_server.pem"),
        include_bytes!("../../../certs/example-server/sdk_server.key"),
    );

    let ping_interval = (args.ping_interval > 0).then(|| Duration::from_secs(args.ping_interval));

    println!("TimingServer listening on {addr} (TLS) — point a Ubidium's custom server here.");
    if let Some(every) = ping_interval {
        println!(
            "Sending command-stream keep-alive pings every {}s.",
            every.as_secs()
        );
    }

    Server::builder()
        .tls_config(ServerTlsConfig::new().identity(identity))
        .context("TLS configuration failed")?
        .add_service(TimingServerServer::new(UbidiumTimingServer {
            ping_interval,
        }))
        // Plain `serve`: a graceful shutdown would wait forever on the
        // long-lived Ubidium streams, so Ctrl-C just terminates the process.
        .serve(addr)
        .await
        .context("server error")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Channel;
    use ubidium::pb::{Passing, timing_server_client::TimingServerClient};

    /// Serve `UbidiumTimingServer` over plain TCP (no TLS) on a random port.
    async fn start(ping_interval: Option<Duration>) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(
            Server::builder()
                .add_service(TimingServerServer::new(UbidiumTimingServer {
                    ping_interval,
                }))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        addr
    }

    /// Connect like a Ubidium does (the Ubidium is the gRPC client here).
    async fn fake_ubidium(addr: SocketAddr) -> TimingServerClient<Channel> {
        let channel = Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        TimingServerClient::new(channel)
    }

    /// A request carrying the Ubidium's outbound stream and `device-id` header.
    fn outbound<T>(rx: mpsc::Receiver<T>) -> Request<ReceiverStream<T>> {
        let mut req = Request::new(ReceiverStream::new(rx));
        req.metadata_mut()
            .insert("device-id", "U-TEST".parse().unwrap());
        req
    }

    /// Next message from the server, failing the test after 5s.
    async fn next<T>(stream: &mut Streaming<T>) -> T {
        tokio::time::timeout(Duration::from_secs(5), stream.message())
            .await
            .expect("timed out waiting for the server")
            .unwrap()
            .expect("stream closed")
    }

    #[tokio::test]
    async fn passing_stream_subscribes_then_acknowledges() {
        let mut ubidium = fake_ubidium(start(None).await).await;
        let (tx, rx) = mpsc::channel(4);
        let mut from_server = ubidium
            .open_passing_stream(outbound(rx))
            .await
            .unwrap()
            .into_inner();

        // First the server subscribes to everything from the current file on.
        assert_eq!(
            next(&mut from_server).await,
            PassingRequest {
                cmd: Some(passing_request::Cmd::Get(CmdGetPassings {
                    start: Some(cmd_get_passings::Start::StartRef(
                        cmd_get_passings::StartReference::BeginningOfCurrentFile as i32
                    )),
                    end: Some(cmd_get_passings::End::EndRef(
                        cmd_get_passings::EndReference::UntilStopped as i32
                    )),
                })),
            }
        );

        // Then it acknowledges every passing it receives.
        for id in [7, 8] {
            tx.send(PassingResponse {
                response: Some(passing_response::Response::Passing(Passing {
                    id,
                    ..Default::default()
                })),
            })
            .await
            .unwrap();
            assert_eq!(
                next(&mut from_server).await,
                PassingRequest {
                    cmd: Some(passing_request::Cmd::Ack(CmdAckPassing {
                        latest: Some(cmd_ack_passing::Latest::Id(id)),
                    })),
                }
            );
        }
    }

    #[tokio::test]
    async fn status_stream_requests_updates_every_10s() {
        let mut ubidium = fake_ubidium(start(None).await).await;
        let (_tx, rx) = mpsc::channel::<StatusResponse>(1);
        let mut from_server = ubidium
            .open_status_stream(outbound(rx))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            next(&mut from_server).await,
            StatusRequest {
                cmd: Some(status_request::Cmd::Get(CmdGetStatus {
                    r#continue: true,
                    push_time: Some(prost_types::Duration {
                        seconds: 10,
                        nanos: 0
                    }),
                })),
            }
        );
    }

    #[tokio::test]
    async fn command_stream_presses_back_once_without_pings() {
        let mut ubidium = fake_ubidium(start(None).await).await;
        let (_tx, rx) = mpsc::channel::<CommandResponse>(1);
        let mut from_server = ubidium
            .open_command_stream(outbound(rx))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            next(&mut from_server).await,
            CommandRequest {
                id: 1,
                cmd: Some(command_request::Cmd::PressKey(CmdPressKey {
                    key: Key::Back as i32
                })),
            }
        );
        // Keep-alive pings are off by default: nothing else arrives.
        let more = tokio::time::timeout(Duration::from_millis(200), from_server.message()).await;
        assert!(more.is_err(), "unexpected command: {more:?}");
    }

    #[tokio::test]
    async fn command_stream_sends_keepalive_pings() {
        let addr = start(Some(Duration::from_millis(20))).await;
        let mut ubidium = fake_ubidium(addr).await;
        let (_tx, rx) = mpsc::channel::<CommandResponse>(1);
        let mut from_server = ubidium
            .open_command_stream(outbound(rx))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(next(&mut from_server).await.id, 1, "PressKey first");
        for id in [2, 3] {
            assert_eq!(
                next(&mut from_server).await,
                CommandRequest {
                    id,
                    cmd: Some(command_request::Cmd::Ping(CmdPing {})),
                }
            );
        }
    }

    #[test]
    fn device_id_comes_from_metadata() {
        assert_eq!(device_id_of(&Request::new(())), UNKNOWN);
        let mut req = Request::new(());
        req.metadata_mut()
            .insert("device-id", "U-40153".parse().unwrap());
        assert_eq!(device_id_of(&req), "U-40153");
    }

    #[test]
    fn device_id_is_learned_only_while_unknown() {
        let mut id = UNKNOWN.to_string();
        learn_device_id(&mut id, Some(&String::new()), "test");
        assert_eq!(id, UNKNOWN, "empty candidates are ignored");
        learn_device_id(&mut id, None, "test");
        assert_eq!(id, UNKNOWN);
        learn_device_id(&mut id, Some(&"U-1".to_string()), "test");
        assert_eq!(id, "U-1");
        learn_device_id(&mut id, Some(&"U-2".to_string()), "test");
        assert_eq!(id, "U-1", "a known ID is kept");
    }
}
