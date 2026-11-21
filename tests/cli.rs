//! End-to-end tests of the `ubidium` binary against a mock Ubidium over TLS.

mod common;

use std::process::Output;
use std::time::Duration;

use common::{CA_PATH, MockUbidium, SERVER_NAME};
use tokio::process::Command;
use ubidium::pb::{
    Key, PassingRequest, cmd_get_passings, passing, passing_request, status_request,
};

const BIN: &str = env!("CARGO_BIN_EXE_ubidium");

/// `ubidium <args>` with no `UBIDIUM_*` variables inherited.
fn command(args: &[&str]) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.args(args)
        .env_remove("UBIDIUM_HOST")
        .env_remove("UBIDIUM_DEVICE_ID")
        .env_remove("UBIDIUM_CA_CERT")
        .kill_on_drop(true);
    cmd
}

async fn run(cmd: &mut Command) -> Output {
    tokio::time::timeout(Duration::from_secs(30), cmd.output())
        .await
        .expect("ubidium timed out")
        .expect("failed to run ubidium")
}

/// Run a client subcommand against the mock at `addr`; panics on failure.
async fn run_ok(addr: std::net::SocketAddr, subcommand: &[&str]) -> String {
    let host = addr.to_string();
    let target = [
        "--host",
        &host,
        "--device-id",
        SERVER_NAME,
        "--ca-cert",
        CA_PATH,
    ];
    let out = run(&mut command(&[subcommand, &target].concat())).await;
    let stdout = String::from_utf8(out.stdout).unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "{subcommand:?} failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    stdout
}

fn get_passings(req: &PassingRequest) -> (cmd_get_passings::Start, cmd_get_passings::End) {
    match &req.cmd {
        Some(passing_request::Cmd::Get(get)) => (get.start.unwrap(), get.end.unwrap()),
        other => panic!("expected CmdGetPassings, got {other:?}"),
    }
}

#[tokio::test]
async fn help_lists_every_subcommand() {
    let out = run(&mut command(&["--help"])).await;
    assert!(out.status.success());
    let help = String::from_utf8(out.stdout).unwrap();
    for sub in [
        "systeminfo",
        "status",
        "passings",
        "stored-passings",
        "client",
        "ping",
        "server",
    ] {
        assert!(help.contains(sub), "--help is missing {sub}:\n{help}");
    }
}

#[tokio::test]
async fn version_matches_crate() {
    let out = run(&mut command(&["--version"])).await;
    let version = String::from_utf8(out.stdout).unwrap();
    assert_eq!(
        version.trim(),
        format!("ubidium {}", env!("CARGO_PKG_VERSION"))
    );
}

#[tokio::test]
async fn help_shows_version_and_author() {
    let out = run(&mut command(&["--help"])).await;
    let help = String::from_utf8(out.stdout).unwrap();
    let mut lines = help.lines();
    assert_eq!(
        lines.next(),
        Some(format!("ubidium {}", env!("CARGO_PKG_VERSION")).as_str())
    );
    assert_eq!(lines.next(), Some("Author: skokys@gmail.com"));
}

#[tokio::test]
async fn usage_errors_exit_with_code_2() {
    for args in [
        &[
            "stored-passings",
            "--host",
            "h",
            "--device-id",
            "U-1",
            "--end-count",
            "1",
        ][..],
        &["ping", "--device-id", "U-1"],
        &["no-such-command"],
    ] {
        let out = run(&mut command(args)).await;
        assert_eq!(out.status.code(), Some(2), "{args:?}");
    }
}

#[tokio::test]
async fn systeminfo_prints_summary() {
    let mock = MockUbidium::default();
    let addr = common::start(mock.clone()).await;
    let out = run_ok(addr, &["systeminfo"]).await;
    for line in [
        "Device ID        : U-MOCK",
        "Name             : Mock Ubidium",
        "Firmware version : 9.9.9",
        "Customer number  : 42",
        "Board temperature: 41.5 °C",
        "Latest passing id: 1001",
        "GPS              : no fix",
    ] {
        assert!(out.contains(line), "missing {line:?} in:\n{out}");
    }
    let seen = mock.seen.lock().unwrap();
    let Some(status_request::Cmd::Get(get)) = &seen.status_requests[0].cmd else {
        panic!("expected CmdGetStatus");
    };
    assert!(!get.r#continue, "a single snapshot is requested");
}

#[tokio::test]
async fn info_alias_reads_target_from_environment() {
    let addr = common::start(MockUbidium::default()).await;
    let mut cmd = command(&["info"]);
    cmd.env("UBIDIUM_HOST", addr.to_string())
        .env("UBIDIUM_DEVICE_ID", SERVER_NAME)
        .env("UBIDIUM_CA_CERT", CA_PATH);
    let out = run(&mut cmd).await;
    assert!(out.status.success(), "{:?}", out);
    assert!(String::from_utf8(out.stdout).unwrap().contains("U-MOCK"));
}

#[tokio::test]
async fn status_dumps_every_field() {
    let addr = common::start(MockUbidium::default()).await;
    let out = run_ok(addr, &["status"]).await;
    assert!(out.contains("=== Ubidium full status ==="), "{out}");
    assert!(out.contains("\"Mock Ubidium\""), "{out}");
    assert!(out.contains("NoFix("), "{out}");
}

#[tokio::test]
async fn stored_passings_by_id_and_count() {
    let mock = MockUbidium::with_passings(998..1005);
    let addr = common::start(mock.clone()).await;
    let out = run_ok(
        addr,
        &["stored-passings", "--start-id", "1000", "--end-count", "2"],
    )
    .await;

    let passings: Vec<&str> = out.lines().filter(|l| l.starts_with('[')).collect();
    assert_eq!(
        passings,
        [
            "[active] id=1000 transponder=T1000 file 3 / no 1000 hits=9 rssi=-61 input=Loop1 time=1700000000.000000005 UTC",
            "[active] id=1001 transponder=T1001 file 3 / no 1001 hits=9 rssi=-61 input=Loop1 time=1700000000.000000005 UTC",
        ]
    );
    assert!(out.contains("Done. Received 2 passing(s)."), "{out}");

    let seen = mock.seen.lock().unwrap();
    assert_eq!(
        get_passings(&seen.passing_requests[0]),
        (
            cmd_get_passings::Start::Id(1000),
            cmd_get_passings::End::Count(2)
        )
    );
}

#[tokio::test]
async fn stored_passings_sends_file_number_and_end_reference() {
    let mock = MockUbidium::with_passings([1]);
    let addr = common::start(mock.clone()).await;
    run_ok(
        addr,
        &[
            "stored-passings",
            "--start-no",
            "3:1",
            "--end-ref",
            "end-of-file",
        ],
    )
    .await;
    let seen = mock.seen.lock().unwrap();
    assert_eq!(
        get_passings(&seen.passing_requests[0]),
        (
            cmd_get_passings::Start::No(passing::No { file: 3, no: 1 }),
            cmd_get_passings::End::EndRef(cmd_get_passings::EndReference::EndOfFile as i32)
        )
    );
}

#[tokio::test]
async fn passings_requests_new_passings_until_stopped() {
    let mock = MockUbidium::with_passings([5, 6]);
    let addr = common::start(mock.clone()).await;
    // The mock closes the stream after sending, which ends the command.
    let out = run_ok(addr, &["passings"]).await;
    assert_eq!(out.lines().filter(|l| l.starts_with("[active]")).count(), 2);
    assert!(
        out.contains("Passing stream closed by the Ubidium."),
        "{out}"
    );
    let seen = mock.seen.lock().unwrap();
    assert_eq!(
        get_passings(&seen.passing_requests[0]),
        (
            cmd_get_passings::Start::StartRef(cmd_get_passings::StartReference::Now as i32),
            cmd_get_passings::End::EndRef(cmd_get_passings::EndReference::UntilStopped as i32)
        )
    );
}

#[tokio::test]
async fn ping_counts_pongs() {
    let mock = MockUbidium::default();
    let addr = common::start(mock.clone()).await;
    let out = run_ok(addr, &["ping", "--count", "3", "--interval", "0"]).await;
    assert_eq!(
        out.lines().filter(|l| l.starts_with("pong from")).count(),
        3
    );
    assert!(out.contains("3 ping(s) sent, 3 pong(s) received"), "{out}");
    assert_eq!(mock.seen.lock().unwrap().pings, 3);
}

#[tokio::test]
async fn client_presses_back_twice_and_opens_both_streams() {
    let mock = MockUbidium::with_passings([1]);
    let addr = common::start(mock.clone()).await;
    let host = addr.to_string();
    let mut child = command(&[
        "client",
        "--host",
        &host,
        "--device-id",
        SERVER_NAME,
        "--ca-cert",
        CA_PATH,
    ])
    .stdout(std::process::Stdio::null())
    .spawn()
    .unwrap();

    // `client` runs until Ctrl-C; wait until it has done its setup, then stop it.
    let done = || {
        let seen = mock.seen.lock().unwrap();
        seen.keys.len() == 2 && seen.status_requests.len() == 1 && seen.passing_requests.len() == 1
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !done() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "client did not finish setup"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    child.kill().await.unwrap();

    let seen = mock.seen.lock().unwrap();
    assert_eq!(seen.keys, [Key::Back as i32, Key::Back as i32]);
    let Some(status_request::Cmd::Get(get)) = &seen.status_requests[0].cmd else {
        panic!("expected CmdGetStatus");
    };
    assert!(get.r#continue, "continuous status updates");
    assert_eq!(get.push_time.map(|d| d.seconds), Some(5));
    assert_eq!(
        get_passings(&seen.passing_requests[0]),
        (
            cmd_get_passings::Start::StartRef(
                cmd_get_passings::StartReference::BeginningOfCurrentFile as i32
            ),
            cmd_get_passings::End::EndRef(cmd_get_passings::EndReference::UntilStopped as i32)
        )
    );
}

#[tokio::test]
async fn untrusted_server_is_rejected_with_reason() {
    let addr = common::start(MockUbidium::default()).await;
    let host = addr.to_string();
    // No --ca-cert: the bundled Race Result CA does not trust the mock.
    let out = run(&mut command(&[
        "ping",
        "--host",
        &host,
        "--device-id",
        SERVER_NAME,
    ]))
    .await;
    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(
        stderr.contains(&format!("could not connect to {host}")),
        "{stderr}"
    );
    assert!(stderr.contains("UnknownIssuer"), "{stderr}");
}
