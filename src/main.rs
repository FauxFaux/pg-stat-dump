use std::fs;
use std::io::Write;
use std::sync::mpsc::{Receiver, RecvTimeoutError, TrySendError};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use lazy_static::lazy_static;
use native_tls::TlsConnector;
use postgres::types::Oid;
use postgres::{Client, Statement};
use postgres_native_tls::MakeTlsConnector;
use regex::Regex;
use zstd::Encoder;

lazy_static! {
    static ref WS: Regex = Regex::new("\\s+").expect("static regex");
}

struct Pg {
    client: Client,
    stat: Statement,
}

fn connect() -> Result<Pg> {
    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build()
        .with_context(|| anyhow!("configuring tls connection"))?;
    let connector = MakeTlsConnector::new(connector);

    let mut client =
        postgres::Client::connect("host=localhost user=postgres sslmode=require", connector)
            .with_context(|| anyhow!("connecting to database"))?;

    // millis
    client
        .execute("set statement_timeout to 5000", &[])
        .with_context(|| anyhow!("setting statement timeout"))?;

    let stat = client.prepare(
        concat!(
            "select now(), datid, datname, pid, usesysid, usename, application_name, client_addr::varchar, client_hostname, client_port, backend_start, xact_start, query_start, state_change, wait_event_type, wait_event, state, backend_xid::varchar, backend_xmin::varchar, query",
            " from pg_stat_activity order by backend_start, pid"))
        .with_context(|| anyhow!("preparing select pg_stat_activity"))?;

    Ok(Pg { client, stat })
}

fn fetch(conn: &mut Pg) -> Result<Vec<Vec<String>>> {
    let columns = conn.stat.columns();
    let headers: Vec<_> = columns.iter().map(|c| c.name().to_string()).collect();

    let mut lines = Vec::with_capacity(32);
    lines.push(headers);

    for row in conn
        .client
        .query(&conn.stat, &[])
        .with_context(|| anyhow!("executing prepared query"))?
    {
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

    Ok(lines)
}

fn render(lines: &[Vec<String>], mins: &mut [usize]) -> String {
    for line in lines {
        for (col, min) in line.iter().zip(mins.iter_mut()) {
            if col.len() > *min {
                *min = col.len();
            }
        }
    }

    let mut buf = String::with_capacity(lines.len() * 300);
    for line in lines {
        let last = mins.len() - 1;
        for (col, min) in line.iter().zip(mins.iter()).take(last) {
            buf.push_str(&format!("{:1$}", col, min + 3));
        }
        buf.push_str(&line[last]);
        buf.push('\n');
    }

    buf
}

fn open() -> Result<Encoder<'static, fs::File>> {
    let path = format!(
        "stat-activity-{}.zst",
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

fn main() -> Result<()> {
    let mut conn = connect()?;

    let started_time = Instant::now();
    let mut output = open()?;

    let mut mins: Box<[usize]> = vec![0usize; conn.stat.columns().len()].into_boxed_slice();

    let shutdown_requested = expect_ctrl_c()?;

    loop {
        let lines = match fetch(&mut conn) {
            Ok(lines) => lines,
            Err(e) => {
                eprintln!("{:?} retrying error: {:?}", Utc::now(), e);
                attempt_close(conn);
                conn = connect().with_context(|| anyhow!("reconnecting after fetch error"))?;
                fetch(&mut conn).with_context(|| anyhow!("fetch after reconnection"))?
            }
        };

        let buf = render(&lines, &mut mins);

        output
            .write_all(buf.as_bytes())
            .with_context(|| anyhow!("compressing / writing"))?;
        output
            .flush()
            .with_context(|| anyhow!("flushing compressed data"))?;

        if started_time.elapsed().gt(&Duration::from_secs(60 * 60)) {
            break;
        }

        match shutdown_requested.recv_timeout(Duration::from_secs(57)) {
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
