use crate::clean_ws;
use chrono::{DateTime, SecondsFormat, Utc};
use postgres::types::Oid;
use postgres::{Column, Row};

pub fn convert_to_strings(
    columns: &[Column],
    rows: impl IntoIterator<Item = Row>,
) -> Vec<Vec<String>> {
    let columns = columns;
    let headers: Vec<_> = columns.iter().map(|c| c.name().to_string()).collect();

    let mut lines = Vec::with_capacity(32);
    lines.push(headers);

    for row in rows {
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

    lines
}

pub fn render(lines: &[Vec<String>], mins: &mut [usize]) -> String {
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

fn ts(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn tso(v: Option<DateTime<Utc>>) -> String {
    v.map(ts).unwrap_or_default()
}

fn auto<T: ToString>(v: &Option<T>) -> String {
    match v {
        Some(v) => clean_ws(&v.to_string()),
        None => String::new(),
    }
}
