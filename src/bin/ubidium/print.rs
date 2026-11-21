//! Human-readable formatting of Ubidium messages, shared by the subcommands.
//!
//! Every function returns the text (possibly several lines, no trailing
//! newline) so callers `println!` it and tests can check it.

use std::fmt::Write;

use ubidium::pb::{Passing, Status, passing, status, transponder};

/// Short label for the kind of a passing.
fn kind(p: &Passing) -> &'static str {
    match &p.data {
        Some(passing::Data::Active(_)) => "active",
        Some(passing::Data::Passive(_)) => "passive",
        Some(passing::Data::Marker(_)) => "marker",
        None => "unknown",
    }
}

fn transponder_id(p: &Passing) -> &str {
    p.transponder.as_ref().map(|t| t.id.as_str()).unwrap_or("?")
}

/// Extra line for active transponders (battery / temperature / wakeups).
fn active_transponder(out: &mut String, p: &Passing) {
    if let Some(transponder::Data::Active(a)) = p.transponder.as_ref().and_then(|t| t.data.as_ref())
    {
        let _ = write!(
            out,
            "\n        active transponder: battery={} temperature={}°C wakeups={}",
            a.battery, a.temperature, a.wakeup_counter
        );
    }
}

/// Detailed passing: id, transponder, file/no, hits, RSSI, input, time.
///
/// The first line has a stable shape that scripts may parse:
/// `[kind] id=N transponder=T (file F / no N|-) hits=N rssi=N input=I time=(S.NANOS UTC|-)`
pub fn passing_detailed(p: &Passing) -> String {
    let no =
        p.no.as_ref()
            .map(|n| format!("file {} / no {}", n.file, n.no))
            .unwrap_or_else(|| "-".to_string());
    let when = p
        .time
        .as_ref()
        .and_then(|t| t.utc.as_ref())
        .map(|utc| format!("{}.{:09} UTC", utc.seconds, utc.nanos))
        .unwrap_or_else(|| "-".to_string());
    let input = p.src.as_ref().map(|s| s.input.as_str()).unwrap_or("-");

    let mut out = format!(
        "[{}] id={} transponder={} {no} hits={} rssi={} input={input} time={when}",
        kind(p),
        p.id,
        transponder_id(p),
        p.hits,
        p.rssi
    );
    active_transponder(&mut out, p);
    out
}

/// Compact passing, prefixed with `prefix` (e.g. a device ID).
pub fn passing_short(prefix: &str, p: &Passing) -> String {
    let mut out = format!(
        "{prefix}[{}] passing id={} transponder={} hits={} rssi={}",
        kind(p),
        p.id,
        transponder_id(p),
        p.hits,
        p.rssi
    );
    active_transponder(&mut out, p);
    out
}

/// The interesting fields of a `Status` snapshot, as a small table.
pub fn system_info(status: &Status) -> String {
    let mut out = String::from("\n=== Ubidium system info ===\n");
    let mut line = |label: &str, value: &dyn std::fmt::Display| {
        let _ = writeln!(out, "{label:<17}: {value}");
    };

    if let Some(v) = &status.id {
        line("Device ID", v);
    }
    if let Some(v) = &status.name {
        line("Name", v);
    }
    if let Some(v) = &status.version {
        line("Firmware version", v);
    }
    if let Some(cust_no) = status.cust_no {
        line("Customer number", &cust_no);
    }
    if let Some(temp) = status.temperature {
        line("Board temperature", &format!("{temp:.1} °C"));
    }
    if let Some(passing_id) = status.passing_id {
        line("Latest passing id", &passing_id);
    }
    if let Some(time) = &status.time {
        if let Some(utc) = &time.utc {
            line(
                "Device time (UTC)",
                &format!(
                    "{}s +{}ns, local offset {}s",
                    utc.seconds, utc.nanos, time.offset
                ),
            );
        }
    }
    match status.gps.as_ref().and_then(|g| g.data.as_ref()) {
        Some(status::gps::Data::Location(loc)) => line(
            "GPS",
            &format!(
                "fix at lat {:.6}, long {:.6}, alt {:.1}m",
                loc.lat, loc.long, loc.alt
            ),
        ),
        Some(status::gps::Data::NoFix(_)) => line("GPS", &"no fix"),
        None => {}
    }
    if let Some(update) = &status.update {
        if let Some(v) = &update.update_version {
            line(
                "Update available",
                &format!("{v} (installed: {})", update.installed.unwrap_or(false)),
            );
        }
    }
    out.push_str("===========================");
    out
}

/// One-line status summary used by the streaming `client` subcommand.
pub fn status_line(s: &Status) -> String {
    let id = s.id.as_deref().unwrap_or("?");
    let name = s.name.as_deref().unwrap_or("?");
    let version = s.version.as_deref().unwrap_or("?");
    let temp = s
        .temperature
        .map(|t| format!("{t:.1}°C"))
        .unwrap_or_else(|| "-".into());
    format!("Status: id={id} name={name} fw={version} temp={temp}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use ubidium::pb::{Location, Time, Transponder};

    fn full_passing() -> Passing {
        Passing {
            id: 4242,
            no: Some(passing::No { file: 3, no: 17 }),
            src: Some(passing::Src {
                input: "Loop1".into(),
                ..Default::default()
            }),
            transponder: Some(Transponder {
                id: "ABC123".into(),
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
            data: Some(passing::Data::Active(Default::default())),
            ..Default::default()
        }
    }

    #[test]
    fn detailed_passing_with_all_fields() {
        assert_eq!(
            passing_detailed(&full_passing()),
            "[active] id=4242 transponder=ABC123 file 3 / no 17 hits=9 rssi=-61 \
             input=Loop1 time=1700000000.000000005 UTC\n        \
             active transponder: battery=90 temperature=21°C wakeups=7"
        );
    }

    #[test]
    fn detailed_passing_with_missing_fields() {
        let p = Passing {
            id: 5,
            data: Some(passing::Data::Marker(Default::default())),
            ..Default::default()
        };
        assert_eq!(
            passing_detailed(&p),
            "[marker] id=5 transponder=? - hits=0 rssi=0 input=- time=-"
        );
    }

    #[test]
    fn passing_kinds() {
        let with = |data| Passing {
            data,
            ..Default::default()
        };
        assert_eq!(kind(&with(None)), "unknown");
        assert_eq!(
            kind(&with(Some(passing::Data::Passive(Default::default())))),
            "passive"
        );
        assert_eq!(
            kind(&with(Some(passing::Data::Active(Default::default())))),
            "active"
        );
        assert_eq!(
            kind(&with(Some(passing::Data::Marker(Default::default())))),
            "marker"
        );
    }

    #[test]
    fn passive_transponder_has_no_extra_line() {
        let mut p = full_passing();
        p.transponder.as_mut().unwrap().data = Some(transponder::Data::Passive(Default::default()));
        assert!(!passing_detailed(&p).contains('\n'));
        assert!(!passing_short("", &p).contains('\n'));
    }

    #[test]
    fn short_passing_with_prefix() {
        let s = passing_short("Got passing from U-1: ", &full_passing());
        assert_eq!(
            s.lines().next().unwrap(),
            "Got passing from U-1: [active] passing id=4242 transponder=ABC123 hits=9 rssi=-61"
        );
        assert_eq!(s.lines().count(), 2);
    }

    #[test]
    fn system_info_lists_known_fields() {
        let status = Status {
            id: Some("U-40153".into()),
            name: Some("Finish".into()),
            version: Some("1.2.3".into()),
            cust_no: Some(42),
            temperature: Some(41.25),
            passing_id: Some(1001),
            time: Some(Time {
                utc: Some(prost_types::Timestamp {
                    seconds: 10,
                    nanos: 20,
                }),
                offset: 3600,
            }),
            gps: Some(status::Gps {
                data: Some(status::gps::Data::Location(Location {
                    lat: 48.1,
                    long: 17.2,
                    alt: 140.0,
                })),
            }),
            update: Some(status::Update {
                update_version: Some("1.3.0".into()),
                installed: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            system_info(&status),
            "\n=== Ubidium system info ===\n\
             Device ID        : U-40153\n\
             Name             : Finish\n\
             Firmware version : 1.2.3\n\
             Customer number  : 42\n\
             Board temperature: 41.2 °C\n\
             Latest passing id: 1001\n\
             Device time (UTC): 10s +20ns, local offset 3600s\n\
             GPS              : fix at lat 48.100000, long 17.200000, alt 140.0m\n\
             Update available : 1.3.0 (installed: false)\n\
             ==========================="
        );
    }

    #[test]
    fn system_info_skips_missing_fields() {
        let status = Status {
            gps: Some(status::Gps {
                data: Some(status::gps::Data::NoFix(true)),
            }),
            ..Default::default()
        };
        assert_eq!(
            system_info(&status),
            "\n=== Ubidium system info ===\n\
             GPS              : no fix\n\
             ==========================="
        );
    }

    #[test]
    fn status_line_with_and_without_fields() {
        assert_eq!(
            status_line(&Status::default()),
            "Status: id=? name=? fw=? temp=-"
        );
        let s = Status {
            id: Some("U-1".into()),
            name: Some("Start".into()),
            version: Some("2.0".into()),
            temperature: Some(39.96),
            ..Default::default()
        };
        assert_eq!(
            status_line(&s),
            "Status: id=U-1 name=Start fw=2.0 temp=40.0°C"
        );
    }
}
