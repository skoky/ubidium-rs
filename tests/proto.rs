//! Tests of the generated protobuf types: enum values, wire encoding, and the
//! messages added in ubidium-sdk v0.9.11 / v0.9.12.

use prost::Message;
use ubidium::pb::{
    CmdGetPassings, CmdGetStatus, CmdPing, CmdPong, CmdStopClient, CmdStopClientResponse,
    CommandRequest, CommandResponse, Key, Passing, PassingRequest, cmd_get_passings,
    command_request, command_response, passing, passing_request,
};

fn roundtrip<M: Message + Default + PartialEq + std::fmt::Debug>(msg: &M) {
    let bytes = msg.encode_to_vec();
    assert_eq!(&M::decode(bytes.as_slice()).unwrap(), msg);
}

#[test]
fn passing_reference_enums_match_proto_numbers() {
    use cmd_get_passings::{EndReference as E, StartReference as S};
    assert_eq!(S::Unspecified as i32, 0);
    assert_eq!(S::Now as i32, 1);
    assert_eq!(S::BeginningOfCurrentFile as i32, 2);
    assert_eq!(S::BeginningOfFirstFile as i32, 3);
    assert_eq!(E::Unspecified as i32, 0);
    assert_eq!(E::UntilStopped as i32, 1);
    assert_eq!(E::CurrentlyExisting as i32, 2);
    assert_eq!(E::EndOfFile as i32, 3);

    assert_eq!(S::Now.as_str_name(), "START_REFERENCE_NOW");
    assert_eq!(
        E::from_str_name("END_REFERENCE_END_OF_FILE"),
        Some(E::EndOfFile)
    );
    assert_eq!(S::try_from(42).ok(), None);
}

#[test]
fn key_enum_uses_linux_key_codes() {
    assert_eq!(Key::Back as i32, 14);
    assert_eq!(Key::Ok as i32, 28);
    assert_eq!(Key::Up as i32, 103);
    assert_eq!(Key::Down as i32, 108);
    assert_eq!(Key::try_from(14), Ok(Key::Back));
}

#[test]
fn get_passings_oneofs_roundtrip() {
    for (start, end) in [
        (
            cmd_get_passings::Start::StartRef(cmd_get_passings::StartReference::Now as i32),
            cmd_get_passings::End::EndRef(cmd_get_passings::EndReference::UntilStopped as i32),
        ),
        (
            cmd_get_passings::Start::Id(1000),
            cmd_get_passings::End::Count(50),
        ),
        (
            cmd_get_passings::Start::No(passing::No { file: 3, no: 1 }),
            cmd_get_passings::End::EndRef(cmd_get_passings::EndReference::EndOfFile as i32),
        ),
    ] {
        roundtrip(&PassingRequest {
            cmd: Some(passing_request::Cmd::Get(CmdGetPassings {
                start: Some(start),
                end: Some(end),
            })),
        });
    }
}

#[test]
fn get_passings_wire_format() {
    // start.id = 1000 (field 3, varint), end.count = 50 (field 5, varint).
    let msg = CmdGetPassings {
        start: Some(cmd_get_passings::Start::Id(1000)),
        end: Some(cmd_get_passings::End::Count(50)),
    };
    assert_eq!(msg.encode_to_vec(), [0x18, 0xe8, 0x07, 0x28, 0x32]);
}

#[test]
fn get_status_continue_field_is_field_1() {
    let msg = CmdGetStatus {
        r#continue: true,
        push_time: None,
    };
    assert_eq!(msg.encode_to_vec(), [0x08, 0x01]);
    // proto3 default (false) is not encoded at all.
    assert!(CmdGetStatus::default().encode_to_vec().is_empty());
}

#[test]
fn empty_bytes_decode_to_default_passing() {
    let p = Passing::decode(&[][..]).unwrap();
    assert_eq!(p, Passing::default());
    assert!(p.transponder.is_none() && p.data.is_none() && p.location.is_none());
}

#[test]
fn unknown_fields_are_skipped() {
    // A future firmware might add fields: field 99 (varint 1) must be ignored.
    let mut bytes = Passing {
        id: 7,
        ..Default::default()
    }
    .encode_to_vec();
    bytes.extend_from_slice(&[0x98, 0x06, 0x01]);
    assert_eq!(Passing::decode(bytes.as_slice()).unwrap().id, 7);
}

#[test]
fn sdk_0_9_11_ping_pong_commands() {
    roundtrip(&CommandRequest {
        id: 2,
        cmd: Some(command_request::Cmd::Ping(CmdPing {})),
    });
    roundtrip(&CommandResponse {
        request_id: 2,
        response: Some(command_response::Response::Pong(CmdPong {})),
    });
}

#[test]
fn sdk_0_9_12_stop_client_commands() {
    let stop = CommandRequest {
        id: 3,
        cmd: Some(command_request::Cmd::StopClient(CmdStopClient {
            disable_auto_connect: Some(true),
        })),
    };
    roundtrip(&stop);
    // `optional bool`: explicit `false` is encoded and distinguishable from unset.
    let explicit_false = CmdStopClient {
        disable_auto_connect: Some(false),
    };
    assert_eq!(explicit_false.encode_to_vec(), [0x08, 0x00]);
    assert_eq!(
        CmdStopClient::decode(&[][..]).unwrap().disable_auto_connect,
        None
    );
    roundtrip(&CommandResponse {
        request_id: 3,
        response: Some(command_response::Response::StopClientResponse(
            CmdStopClientResponse {},
        )),
    });
}
