//! Library tests against a mock Ubidium over TLS: CA pinning, the TLS name
//! override, and the status / passing / unary RPCs.

mod common;

use common::{CA_PEM, MockUbidium, SERVER_NAME};
use tokio_stream::wrappers::ReceiverStream;
use ubidium::pb::{
    CmdGetPassings, CmdGetStatus, CmdPing, CmdPressKey, Key, PassingRequest, StatusRequest,
    cmd_get_passings, passing_request, passing_response, status_request, status_response,
    timing_system_client::TimingSystemClient,
};

async fn client(addr: std::net::SocketAddr) -> TimingSystemClient<tonic::transport::Channel> {
    let channel = ubidium::connect_with_ca(&addr.to_string(), SERVER_NAME, CA_PEM)
        .await
        .expect("connect to mock");
    TimingSystemClient::new(channel)
}

#[tokio::test]
async fn ping_with_custom_ca_and_name_override() {
    let mock = MockUbidium::default();
    let addr = common::start(mock.clone()).await;
    client(addr).await.ping(CmdPing {}).await.expect("ping");
    assert_eq!(mock.seen.lock().unwrap().pings, 1);
}

#[tokio::test]
async fn wrong_device_id_is_rejected() {
    let addr = common::start(MockUbidium::default()).await;
    // The certificate is valid, but not for this name.
    let err = ubidium::connect_with_ca(&addr.to_string(), "U-40153", CA_PEM)
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("NotValidForName"), "{err:?}");
}

#[tokio::test]
async fn bundled_race_result_ca_rejects_foreign_certificate() {
    let addr = common::start(MockUbidium::default()).await;
    // `connect` pins the Race Result CA, which did not sign the mock's cert.
    let err = ubidium::connect(&addr.to_string(), SERVER_NAME)
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("UnknownIssuer"), "{err:?}");
}

#[tokio::test]
async fn status_stream_returns_snapshot() {
    let mock = MockUbidium::default();
    let addr = common::start(mock.clone()).await;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(StatusRequest {
        cmd: Some(status_request::Cmd::Get(CmdGetStatus {
            r#continue: false,
            push_time: None,
        })),
    })
    .await
    .unwrap();

    let mut stream = client(addr)
        .await
        .open_status_stream(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let msg = stream.message().await.unwrap().expect("one response");
    let Some(status_response::Response::Status(status)) = msg.response else {
        panic!("expected a status, got {msg:?}");
    };
    assert_eq!(status, common::sample_status());

    let seen = mock.seen.lock().unwrap();
    assert_eq!(seen.status_requests.len(), 1);
    let Some(status_request::Cmd::Get(get)) = &seen.status_requests[0].cmd else {
        panic!("expected CmdGetStatus");
    };
    assert!(!get.r#continue);
}

#[tokio::test]
async fn passing_stream_honours_start_id_and_count() {
    let mock = MockUbidium::with_passings(998..1005);
    let addr = common::start(mock.clone()).await;
    let request = PassingRequest {
        cmd: Some(passing_request::Cmd::Get(CmdGetPassings {
            start: Some(cmd_get_passings::Start::Id(1000)),
            end: Some(cmd_get_passings::End::Count(3)),
        })),
    };
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(request).await.unwrap();

    let mut stream = client(addr)
        .await
        .open_passing_stream(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();

    let mut welcome = None;
    let mut ids = Vec::new();
    while let Some(msg) = stream.message().await.unwrap() {
        match msg.response {
            Some(passing_response::Response::Welcome(w)) => welcome = Some(w),
            Some(passing_response::Response::Passing(p)) => {
                assert_eq!(p, common::sample_passing(p.id));
                ids.push(p.id);
            }
            other => panic!("unexpected response {other:?}"),
        }
    }
    assert_eq!(welcome.expect("welcome first").current_id, 1004);
    assert_eq!(ids, [1000, 1001, 1002]);
    assert_eq!(mock.seen.lock().unwrap().passing_requests, [request]);
}

#[tokio::test]
async fn press_key_is_forwarded() {
    let mock = MockUbidium::default();
    let addr = common::start(mock.clone()).await;
    client(addr)
        .await
        .press_key(CmdPressKey {
            key: Key::Back as i32,
        })
        .await
        .unwrap();
    assert_eq!(mock.seen.lock().unwrap().keys, [Key::Back as i32]);
}

#[tokio::test]
async fn unmocked_rpc_reports_unimplemented() {
    let addr = common::start(MockUbidium::default()).await;
    let err = client(addr)
        .await
        .reboot(ubidium::pb::CmdReboot::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unimplemented);
}
