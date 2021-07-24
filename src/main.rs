use std::convert::TryInto;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use lazy_static::lazy_static;
use native_tls::TlsConnector;
use postgres::types::Oid;
use postgres_native_tls::MakeTlsConnector;
use regex::Regex;

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
            "select datid, datname, pid, usesysid, usename, application_name, client_addr::varchar, client_hostname, client_port, backend_start, xact_start, query_start, state_change, wait_event_type, wait_event, state, backend_xid::varchar, backend_xmin::varchar, query",
            " from pg_stat_activity order by backend_start, pid"))
        .with_context(|| anyhow!("preparing select pg_stat_activity"))?;

    let mut lines = Vec::with_capacity(32);
    let headers = [
        "now",
        "datid",
        "datname",
        "pid",
        "usesysid",
        "usename",
        "application_name",
        "client_addr",
        "client_hostname",
        "client_port",
        "backend_start",
        "xact_start",
        "query_start",
        "state_change",
        "wait_event_type",
        "wait_event",
        "state",
        "backend_xid",
        "backend_xmin",
        "query",
    ];
    let mut mins: [usize; 20] = headers.map(|s| s.len()).try_into().unwrap();

    for row in client.query(&stat, &[])? {
        let act = StatActivity {
            datid: row.get(0),
            datname: row.get(1),
            pid: row.get(2),
            usesysid: row.get(3),
            usename: row.get(4),
            application_name: row.get(5),
            client_addr: row.get(6),
            client_hostname: row.get(7),
            client_port: row.get(8),
            backend_start: row.get(9),
            xact_start: row.get(10),
            query_start: row.get(11),
            state_change: row.get(12),
            wait_event_type: row.get(13),
            wait_event: row.get(14),
            state: row.get(15),
            backend_xid: row.get(16),
            backend_xmin: row.get(17),
            query: row.get(18),
        };

        let strings = [
            ts(Utc::now()),
            auto(&act.datid),
            auto(&act.datname),
            auto(&act.pid),
            auto(&act.usesysid),
            auto(&act.usename),
            auto(&act.application_name),
            auto(&act.client_addr),
            auto(&act.client_hostname),
            auto(&act.client_port),
            tso(act.backend_start),
            tso(act.xact_start),
            tso(act.query_start),
            tso(act.state_change),
            auto(&act.wait_event_type),
            auto(&act.wait_event),
            auto(&act.state),
            auto(&act.backend_xid),
            auto(&act.backend_xmin),
            auto(&act.query),
        ];

        println!("{:?}", mins);

        for i in 0..mins.len() {
            if strings[i].len() > mins[i] {
                mins[i] = strings[i].len();
            }
        }

        lines.push(strings);
    }

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

type Inet = String;
type Xid = String;

#[derive(Debug)]
struct StatActivity {
    datid: Option<Oid>,
    datname: Option<String>,
    pid: Option<i32>,
    usesysid: Option<Oid>,
    usename: Option<String>,
    application_name: Option<String>,
    client_addr: Option<Inet>,
    client_hostname: Option<String>,
    client_port: Option<i32>,
    backend_start: Option<DateTime<Utc>>,
    xact_start: Option<DateTime<Utc>>,
    query_start: Option<DateTime<Utc>>,
    state_change: Option<DateTime<Utc>>,
    wait_event_type: Option<String>,
    wait_event: Option<String>,
    state: Option<String>,
    backend_xid: Option<Xid>,
    backend_xmin: Option<Xid>,
    query: Option<String>,
}

fn clean_ws(s: &str) -> String {
    WS.replace_all(s, " ").to_string()
}
