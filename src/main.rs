mod printer;

use std::env::VarError;
use std::fs;
use std::io::Write;
use std::sync::mpsc::{Receiver, RecvTimeoutError, TrySendError};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use lazy_static::lazy_static;
use native_tls::TlsConnector;
use postgres::{Client, Row, Statement};
use postgres_native_tls::MakeTlsConnector;
use regex::Regex;
use serde::{Deserialize, Serialize};

lazy_static! {
    static ref WS: Regex = Regex::new("\\s+").expect("static regex");
}

struct Pg {
    client: Client,
    stat: Statement,
}

fn connect(config: &Config) -> Result<Pg> {
    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build()
        .with_context(|| anyhow!("configuring tls connection"))?;
    let connector = MakeTlsConnector::new(connector);

    let mut client = Client::connect(&config.conn_string, connector)
        .with_context(|| anyhow!("connecting to database"))?;

    // millis
    client
        .execute("set statement_timeout to 5000", &[])
        .with_context(|| anyhow!("setting statement timeout"))?;

    let stat = client.prepare(
        concat!(
            "select now(), datid::int, datname, pid, usesysid::int, usename, application_name, client_addr::varchar, client_hostname, client_port, backend_start, xact_start, query_start, state_change, wait_event_type, wait_event, state, backend_xid::varchar, backend_xmin::varchar, query",
            " from pg_stat_activity where state != 'idle' order by backend_start, pid"))
        .with_context(|| anyhow!("preparing select pg_stat_activity"))?;

    Ok(Pg { client, stat })
}

fn fetch(conn: &mut Pg) -> Result<Vec<Row>> {
    Ok(conn
        .client
        .query(&conn.stat, &[])
        .with_context(|| anyhow!("executing prepared query"))?)
}

fn open() -> Result<zstd::Encoder<'static, fs::File>> {
    let path = format!(
        "stat-activity-{}.jsonl.zst",
        Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
    );
    Ok(zstd::Encoder::new(fs::File::create(path)?, 9)?)
}

fn attempt_close(conn: Pg) {
    if conn.client.is_closed() {
        return;
    }

    drop(conn.stat);

    if let Err(e) = conn.client.close() {
        eprintln!("{:?} error closing: {:?}", Utc::now(), e);
    }
}

fn expect_ctrl_c() -> Result<Receiver<()>> {
    let (initiate_shutdown, shutdown_requested) = std::sync::mpsc::sync_channel(1);
    ctrlc::set_handler(move || match initiate_shutdown.try_send(()) {
        Ok(()) => eprintln!("{:?} started clean shutdown", Utc::now()),
        Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
            eprintln!("{:?} second exit request; dying", Utc::now());
            std::process::exit(6)
        }
    })?;
    Ok(shutdown_requested)
}

struct Config {
    poll_interval: Duration,
    max_uptime: Duration,
    conn_string: String,
}

fn secs_to_duration(secs: &str) -> Result<Duration> {
    let secs = secs
        .parse()
        .with_context(|| anyhow!("parsing {:?} as float", secs))?;

    if secs < 1. / 1e9 || secs > ((1u64 << 32) as f64) {
        bail!("seconds values must roughly be between 1ns and 100 years");
    }

    Ok(Duration::from_secs_f64(secs))
}

fn env_var(name: &'static str) -> Result<Option<String>> {
    Ok(match std::env::var(name) {
        Ok(v) => Some(v),
        Err(VarError::NotUnicode(_)) => bail!("{}: invalid unicode", name),
        Err(VarError::NotPresent) => None,
    })
}

fn duration_from_env(name: &'static str, default: Duration) -> Result<Duration> {
    Ok(match env_var(name)? {
        Some(v) => secs_to_duration(&v).with_context(|| anyhow!("interpreting {}", name))?,
        None => default,
    })
}

fn config() -> Result<Config> {
    Ok(Config {
        poll_interval: duration_from_env("PSD_POLL_INTERVAL_SECS", Duration::from_secs(53))?,
        max_uptime: duration_from_env("PSD_MAX_UPTIME_SECS", Duration::from_secs(60 * 60))?,
        conn_string: env_var("PSD_CONN_STRING")?.ok_or_else(|| {
            anyhow!("PSD_CONN_STRING required, e.g.: host=localhost user=postgres sslmode=require")
        })?,
    })
}

#[derive(Serialize, Deserialize)]
struct Line {
    when: Option<DateTime<Utc>>,
    records: Vec<Record>,
}

#[derive(Serialize, Deserialize)]
struct Record {
    datid: Option<i32>,
    datname: Option<String>,
    pid: Option<i32>,
    usesysid: Option<i32>,
    usename: Option<String>,
    application_name: Option<String>,
    client_addr: Option<String>,
    client_hostname: Option<String>,
    client_port: Option<i32>,
    backend_start: Option<DateTime<Utc>>,
    xact_start: Option<DateTime<Utc>>,
    query_start: Option<DateTime<Utc>>,
    state_change: Option<DateTime<Utc>>,
    wait_event_type: Option<String>,
    wait_event: Option<String>,
    state: Option<String>,
    backend_xid: Option<String>,
    backend_xmin: Option<String>,
    query: Option<String>,
}

fn record_from_row(row: &Row) -> Record {
    Record {
        datid: row.get(1),
        datname: row.get(2),
        pid: row.get(3),
        usesysid: row.get(4),
        usename: row.get(5),
        application_name: row.get(6),
        client_addr: row.get(7),
        client_hostname: row.get(8),
        client_port: row.get(9),
        backend_start: row.get(10),
        xact_start: row.get(11),
        query_start: row.get(12),
        state_change: row.get(13),
        wait_event_type: row.get(14),
        wait_event: row.get(15),
        state: row.get(16),
        backend_xid: row.get(17),
        backend_xmin: row.get(18),
        query: row.get(19),
    }
}

fn main() -> Result<()> {
    let cfg = config()?;

    let mut conn = connect(&cfg)?;

    let started_time = Instant::now();
    let mut output = open()?;

    let shutdown_requested = expect_ctrl_c()?;

    loop {
        let rows = match fetch(&mut conn) {
            Ok(rows) => rows,
            Err(e) => {
                eprintln!("{:?} retrying error: {:?}", Utc::now(), e);
                attempt_close(conn);
                conn = connect(&cfg).with_context(|| anyhow!("reconnecting after fetch error"))?;
                fetch(&mut conn).with_context(|| anyhow!("fetch after reconnection"))?
            }
        };

        let when = rows.get(0).map(|row| row.get::<_, DateTime<Utc>>(0));
        let records = rows.iter().map(record_from_row).collect();

        serde_json::to_writer(&mut output, &Line { when, records })?;

        output.write_all(b"\n")?;
        output
            .flush()
            .with_context(|| anyhow!("flushing compressed data"))?;

        if started_time.elapsed().gt(&cfg.max_uptime) {
            break;
        }

        match shutdown_requested.recv_timeout(cfg.poll_interval) {
            Err(RecvTimeoutError::Timeout) => (),
            Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
        }
    }

    attempt_close(conn);

    output
        .do_finish()
        .with_context(|| anyhow!("finalising output file during clean exit"))?;

    eprintln!("{:?} clean exit", Utc::now());

    Ok(())
}

fn clean_ws(s: &str) -> String {
    WS.replace_all(s, " ").to_string()
}
