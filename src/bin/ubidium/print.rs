//! Human-readable printing of Ubidium messages, shared by the subcommands.

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
fn print_active_transponder(p: &Passing) {
    if let Some(transponder::Data::Active(a)) = p.transponder.as_ref().and_then(|t| t.data.as_ref())
    {
        println!(
            "        active transponder: battery={} temperature={}°C wakeups={}",
            a.battery, a.temperature, a.wakeup_counter
        );
    }
}

/// Detailed one-line passing: id, transponder, file/no, hits, RSSI, input, time.
pub fn passing_detailed(p: &Passing) {
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

    println!(
        "[{}] id={} transponder={} {no} hits={} rssi={} input={input} time={when}",
        kind(p),
        p.id,
        transponder_id(p),
        p.hits,
        p.rssi
    );
    print_active_transponder(p);
}

/// Compact one-line passing, prefixed with `prefix` (e.g. a device ID).
pub fn passing_short(prefix: &str, p: &Passing) {
    println!(
        "{prefix}[{}] passing id={} transponder={} hits={} rssi={}",
        kind(p),
        p.id,
        transponder_id(p),
        p.hits,
        p.rssi
    );
    print_active_transponder(p);
}

/// Pretty-print the interesting fields of a `Status` snapshot.
pub fn system_info(status: &Status) {
    fn opt(label: &str, value: &Option<String>) {
        if let Some(v) = value {
            println!("{label:<17}: {v}");
        }
    }

    println!("\n=== Ubidium system info ===");
    opt("Device ID", &status.id);
    opt("Name", &status.name);
    opt("Firmware version", &status.version);

    if let Some(cust_no) = status.cust_no {
        println!("Customer number  : {cust_no}");
    }
    if let Some(temp) = status.temperature {
        println!("Board temperature: {temp:.1} °C");
    }
    if let Some(passing_id) = status.passing_id {
        println!("Latest passing id: {passing_id}");
    }
    if let Some(time) = &status.time {
        if let Some(utc) = &time.utc {
            println!(
                "Device time (UTC): {}s +{}ns, local offset {}s",
                utc.seconds, utc.nanos, time.offset
            );
        }
    }
    match status.gps.as_ref().and_then(|g| g.data.as_ref()) {
        Some(status::gps::Data::Location(loc)) => println!(
            "GPS              : fix at lat {:.6}, long {:.6}, alt {:.1}m",
            loc.lat, loc.long, loc.alt
        ),
        Some(status::gps::Data::NoFix(_)) => println!("GPS              : no fix"),
        None => {}
    }
    if let Some(update) = &status.update {
        if let Some(v) = &update.update_version {
            println!(
                "Update available : {v} (installed: {})",
                update.installed.unwrap_or(false)
            );
        }
    }
    println!("===========================");
}

/// One-line status summary used by the streaming `client` subcommand.
pub fn status_line(s: &Status) {
    let id = s.id.as_deref().unwrap_or("?");
    let name = s.name.as_deref().unwrap_or("?");
    let version = s.version.as_deref().unwrap_or("?");
    let temp = s
        .temperature
        .map(|t| format!("{t:.1}°C"))
        .unwrap_or_else(|| "-".into());
    println!("Status: id={id} name={name} fw={version} temp={temp}");
}
