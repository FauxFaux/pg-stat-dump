use std::convert::TryInto;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use lazy_static::lazy_static;
use native_tls::TlsConnector;
use postgres::types::Oid;
use postgres_native_tls::MakeTlsConnector;
use regex::Regex;
use std::fs;
use std::io::Write;

lazy_static! {
    static ref WS: Regex = Regex::new("\\s+").expect("static regex");
}

fn main() -> Result<()> {
    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build()?;
    let connector = MakeTlsConnector::new(connector);

    let mut client =
        postgres::Client::connect("host=localhost user=postgres sslmode=require", connector)?;

    let stat = client.prepare(
        concat!(
            "select now(), datid, datname, pid, usesysid, usename, application_name, client_addr::varchar, client_hostname, client_port, backend_start, xact_start, query_start, state_change, wait_event_type, wait_event, state, backend_xid::varchar, backend_xmin::varchar, query",
            " from pg_stat_activity order by backend_start, pid"))
        .with_context(|| anyhow!("preparing select pg_stat_activity"))?;

    let started_time = Utc::now();
    let path = format!(
        "stat-activity-{}.zst",
        started_time.to_rfc3339_opts(SecondsFormat::Secs, true)
    );
    let mut output = zstd::Encoder::new(fs::File::create(path)?, 9)?.auto_finish();

    let columns = stat.columns();
    let headers: Vec<_> = columns.iter().map(|c| c.name().to_string()).collect();

    let mut mins: Vec<usize> = headers.iter().map(|s| s.len()).collect();

    let mut lines = Vec::with_capacity(32);
    lines.push(headers);

    for row in client.query(&stat, &[])? {
        let mut strings = Vec::with_capacity(columns.len());
        for (i, column) in columns.iter().enumerate() {
            strings.push(match column.type_().name() {
                "timestamptz" => tso(row.get(i)),
                "oid" => auto(&row.get::<_, Option<Oid>>(i)),
                "name" | "text" | "varchar" => auto(&row.get::<_, Option<String>>(i)),
                "int4" => auto(&row.get::<_, Option<i32>>(i)),
                other => panic!("unknown type: {:?}", other),
            });
        }

        lines.push(strings);
    }

    for line in &lines {
        for (col, min) in line.iter().zip(mins.iter_mut()) {
            if col.len() > *min {
                *min = col.len();
            }
        }
    }

    let mut buf = String::with_capacity(lines.len() * 300);
    for line in &lines {
        let last = mins.len() - 1;
        for (col, min) in line.iter().zip(mins.iter()).take(last) {
            buf.push_str(&format!("{:1$}", col, min + 3));
        }
        buf.push_str(&line[last]);
        buf.push('\n');
    }

    output.write_all(buf.as_bytes())?;
    output.flush()?;
    Ok(())
}

fn ts(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn tso(v: Option<DateTime<Utc>>) -> String {
    v.map(|v| ts(v)).unwrap_or_default()
}

fn auto<T: ToString>(v: &Option<T>) -> String {
    match v {
        Some(v) => clean_ws(&v.to_string()),
        None => String::new(),
    }
}

fn clean_ws(s: &str) -> String {
    WS.replace_all(s, " ").to_string()
}
