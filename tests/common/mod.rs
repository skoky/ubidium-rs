//! A mock Ubidium: an in-process `TimingSystem` gRPC server over TLS, using the
//! SDK's example server certificate (`certs/example-server/`).
//!
//! Clients reach it with `ubidium::connect_with_ca(addr, SERVER_NAME, CA_PEM)`
//! (or `ubidium --ca-cert CA_PATH --device-id SERVER_NAME`).

#![allow(dead_code)] // not every test binary uses every helper

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

use ubidium::pb::{
    CmdGetPassings, CmdGetScreen, CmdGetScreenResponse, CmdGetSettings, CmdGetSettingsResponse,
    CmdNewFile, CmdNewFileResponse, CmdPing, CmdPong, CmdPressKey, CmdPressKeyResponse, CmdReboot,
    CmdRebootResponse, CmdSetSettings, CmdSetSettingsResponse, CmdSetTime, CmdSetTimeResponse,
    CommandRequest, CommandResponse, Passing, PassingRequest, PassingResponse, PrewarnRequest,
    PrewarnResponse, StatusRequest, StatusResponse, Time, Transponder, Welcome, cmd_get_passings,
    passing, passing_request, passing_response, status, status_request, status_response,
    timing_system_server::{TimingSystem, TimingSystemServer},
    transponder,
};

/// TLS name in the example server certificate (pass it as the device ID).
pub const SERVER_NAME: &str = "server.example.sdk.ubidium.raceresult.com";
/// CA that signed the example server certificate.
pub const CA_PEM: &[u8] = include_bytes!("../../certs/example-server/sdk_cacert.pem");
pub const CA_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/certs/example-server/sdk_cacert.pem"
);
/// Device ID the mock reports in its status.
pub const MOCK_DEVICE_ID: &str = "U-MOCK";

/// What the mock received from clients.
#[derive(Debug, Default)]
pub struct Seen {
    pub status_requests: Vec<StatusRequest>,
    pub passing_requests: Vec<PassingRequest>,
    pub keys: Vec<i32>,
    pub pings: usize,
}

#[derive(Clone, Default)]
pub struct MockUbidium {
    pub seen: Arc<Mutex<Seen>>,
    /// Passings served on the passing stream (filtered by the request).
    pub passings: Vec<Passing>,
}

impl MockUbidium {
    pub fn with_passings(ids: impl IntoIterator<Item = u64>) -> Self {
        Self {
            passings: ids.into_iter().map(sample_passing).collect(),
            ..Self::default()
        }
    }
}

/// A fully populated active passing with the given ID.
pub fn sample_passing(id: u64) -> Passing {
    Passing {
        id,
        no: Some(passing::No {
            file: 3,
            no: id as u32,
        }),
        src: Some(passing::Src {
            device_id: MOCK_DEVICE_ID.into(),
            input: "Loop1".into(),
            ..Default::default()
        }),
        transponder: Some(Transponder {
            id: format!("T{id}"),
            data: Some(transponder::Data::Active(transponder::ActiveData {
                wakeup_counter: 7,
                battery: 90,
                temperature: 21,
            })),
        }),
        time: Some(Time {
            utc: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 5,
            }),
            offset: 3600,
        }),
        hits: 9,
        rssi: -61,
        data: Some(passing::Data::Active(passing::ActiveData::default())),
        ..Default::default()
    }
}

/// The status snapshot the mock reports.
pub fn sample_status() -> ubidium::pb::Status {
    ubidium::pb::Status {
        id: Some(MOCK_DEVICE_ID.into()),
        name: Some("Mock Ubidium".into()),
        version: Some("9.9.9".into()),
        cust_no: Some(42),
        temperature: Some(41.5),
        passing_id: Some(1001),
        gps: Some(status::Gps {
            data: Some(status::gps::Data::NoFix(true)),
        }),
        ..Default::default()
    }
}

/// Start the mock on a random localhost port, over TLS.
pub async fn start(mock: MockUbidium) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let identity = Identity::from_pem(
        include_bytes!("../../certs/example-server/sdk_server.pem"),
        include_bytes!("../../certs/example-server/sdk_server.key"),
    );
    let server = Server::builder()
        .tls_config(ServerTlsConfig::new().identity(identity))
        .unwrap()
        .add_service(TimingSystemServer::new(mock))
        .serve_with_incoming(TcpListenerStream::new(listener));
    tokio::spawn(server);
    addr
}

type OutStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

fn unimplemented<T>() -> Result<Response<T>, Status> {
    Err(Status::unimplemented("not mocked"))
}

#[tonic::async_trait]
impl TimingSystem for MockUbidium {
    type OpenPassingStreamStream = OutStream<PassingResponse>;
    type OpenPrewarnStreamStream = OutStream<PrewarnResponse>;
    type OpenStatusStreamStream = OutStream<StatusResponse>;
    type OpenCommandStreamStream = OutStream<CommandResponse>;

    /// Answer the first `CmdGetPassings` with a welcome and the matching
    /// passings, then close the stream (as a device does once an end selector
    /// is reached).
    async fn open_passing_stream(
        &self,
        request: Request<Streaming<PassingRequest>>,
    ) -> Result<Response<Self::OpenPassingStreamStream>, Status> {
        let mut incoming = request.into_inner();
        let (tx, rx) = mpsc::channel(16);
        let seen = self.seen.clone();
        let passings = self.passings.clone();
        tokio::spawn(async move {
            let Ok(Some(req)) = incoming.message().await else {
                return;
            };
            seen.lock().unwrap().passing_requests.push(req);
            let Some(passing_request::Cmd::Get(CmdGetPassings { start, end })) = req.cmd else {
                return;
            };
            let welcome = Welcome {
                current_id: passings.last().map_or(0, |p| p.id),
                cust_no: 42,
                ..Default::default()
            };
            let _ = tx
                .send(Ok(PassingResponse {
                    response: Some(passing_response::Response::Welcome(welcome)),
                }))
                .await;
            let first = match start {
                Some(cmd_get_passings::Start::Id(id)) => id,
                _ => 0,
            };
            let max = match end {
                Some(cmd_get_passings::End::Count(n)) => n as usize,
                _ => usize::MAX,
            };
            for p in passings.into_iter().filter(|p| p.id >= first).take(max) {
                let _ = tx
                    .send(Ok(PassingResponse {
                        response: Some(passing_response::Response::Passing(p)),
                    }))
                    .await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn open_prewarn_stream(
        &self,
        _: Request<PrewarnRequest>,
    ) -> Result<Response<Self::OpenPrewarnStreamStream>, Status> {
        unimplemented()
    }

    /// Answer every `CmdGetStatus` with [`sample_status`].
    async fn open_status_stream(
        &self,
        request: Request<Streaming<StatusRequest>>,
    ) -> Result<Response<Self::OpenStatusStreamStream>, Status> {
        let mut incoming = request.into_inner();
        let (tx, rx) = mpsc::channel(16);
        let seen = self.seen.clone();
        tokio::spawn(async move {
            while let Ok(Some(req)) = incoming.message().await {
                let is_get = matches!(req.cmd, Some(status_request::Cmd::Get(_)));
                seen.lock().unwrap().status_requests.push(req);
                if is_get {
                    let resp = StatusResponse {
                        response: Some(status_response::Response::Status(sample_status())),
                    };
                    if tx.send(Ok(resp)).await.is_err() {
                        break;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn open_command_stream(
        &self,
        _: Request<Streaming<CommandRequest>>,
    ) -> Result<Response<Self::OpenCommandStreamStream>, Status> {
        unimplemented()
    }

    async fn new_file(
        &self,
        _: Request<CmdNewFile>,
    ) -> Result<Response<CmdNewFileResponse>, Status> {
        unimplemented()
    }

    async fn set_time(
        &self,
        _: Request<CmdSetTime>,
    ) -> Result<Response<CmdSetTimeResponse>, Status> {
        unimplemented()
    }

    async fn get_screen(
        &self,
        _: Request<CmdGetScreen>,
    ) -> Result<Response<CmdGetScreenResponse>, Status> {
        unimplemented()
    }

    async fn press_key(
        &self,
        request: Request<CmdPressKey>,
    ) -> Result<Response<CmdPressKeyResponse>, Status> {
        self.seen
            .lock()
            .unwrap()
            .keys
            .push(request.into_inner().key);
        Ok(Response::new(CmdPressKeyResponse {}))
    }

    async fn set_settings(
        &self,
        _: Request<CmdSetSettings>,
    ) -> Result<Response<CmdSetSettingsResponse>, Status> {
        unimplemented()
    }

    async fn get_settings(
        &self,
        _: Request<CmdGetSettings>,
    ) -> Result<Response<CmdGetSettingsResponse>, Status> {
        unimplemented()
    }

    async fn reboot(&self, _: Request<CmdReboot>) -> Result<Response<CmdRebootResponse>, Status> {
        unimplemented()
    }

    async fn ping(&self, _: Request<CmdPing>) -> Result<Response<CmdPong>, Status> {
        self.seen.lock().unwrap().pings += 1;
        Ok(Response::new(CmdPong {}))
    }
}
