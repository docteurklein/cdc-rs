use async_stream::try_stream;
use futures::stream::Stream;
use futures::sink::{self, SinkExt};
use futures::pin_mut;
use futures::stream::StreamExt;
use rhai::AST;

use std::convert::identity;
use std::path::PathBuf;
use std::collections::BTreeMap;
use std::str::from_utf8;
use mysql_async::binlog::events::*;
use mysql_async::binlog::row::BinlogRow;
use mysql_async::{BinlogStreamRequest, Conn, Opts, Row, Value};
use google_cloud_pubsub::client::{ClientConfig, Client};
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::publisher::Publisher;
use anyhow::{Error, Result};
use regex::Regex;
use clap::Parser;
use rhai::{Dynamic, Engine, Scope};
use anyhow::anyhow;
use std::fmt;

#[derive(Parser, Debug, Clone)]
struct Args {
    #[arg(short, long, env)]
    state: String,

    #[arg(short, long, env)]
    server_id: u32,

    #[arg(short, long, env)]
    regex: String,

    #[arg(short, long, env)]
    source: String,

    #[arg(short, long, env)]
    script: Option<PathBuf>,
}

#[derive(Clone, Debug, Default)]
enum ChangeType {
    #[default]
    Insert,
    Update,
    Delete,
    Other,
}

impl fmt::Display for ChangeType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(Clone, Debug, Default)]
struct Change<T> {
    log_pos: u32,
    filename: String,
    db: String,
    table: String,
    op: ChangeType,
    rows: Vec<T>,
    ts: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    // let subscriber = tracing_subscriber::FmtSubscriber::new();
    // tracing::subscriber::set_global_default(subscriber)?;
    // console_subscriber::init();

    let args = Args::parse();

    let mut dsl = DSL::new(args.clone().script);

    let s = mysql_changes(args.clone());
    pin_mut!(s); // needed for iteration

    let pubsub = Client::new(
        ClientConfig::default().with_auth().await?
    ).await?;

    let mut publishers: BTreeMap<String, Publisher> = BTreeMap::new();

    let connection = sqlite::open(&args.state)?;

    while let Some(change) = s.next().await {
        let Change {log_pos, filename, db, table, op, rows, ts} = change?;
        let msgs: Vec<PubsubMessage> = rows.iter().map(|(before, after)| {
            let before = before.clone().map(row_image_to_map).map_or(BTreeMap::new(), identity);
            let after = after.clone().map(row_image_to_map).map_or(BTreeMap::new(), identity);

            let data = dsl.transform(
                db.clone(),
                table.clone(),
                op.clone(),
                before,
                after,
                ts
            );
            PubsubMessage {
                data: data.to_string().into(),
                ..Default::default()
            }
        }).collect();

        let topic = dsl.topic(db.clone(), table.clone());

        let publisher = publishers.entry(topic.clone()).or_insert_with(|| {
             dbg!(&topic);
             pubsub.topic(&topic).new_publisher(None)
        });

        // publisher.publish_immediately(msgs, None).await?;

        let mut statement = connection.prepare("
            insert into log_pos (server_id, pos, filename) values (?, max(4, ?), ?)
            on conflict do update set
            pos = excluded.pos,
            filename = excluded.filename
        ")?;
        statement.bind((1, args.server_id as i64))?;
        statement.bind((2, log_pos as i64))?;
        statement.bind((3, filename.as_str()))?;
        statement.next()?;
    }

    Ok(())
}

fn mysql_changes(args: Args) -> impl Stream<Item = Result<Change<(Option<BinlogRow>, Option<BinlogRow>)>>> {
    try_stream! {
        let (mut log_pos, mut filename) = setup_state(&args.state, args.server_id)?;
        dbg!(&log_pos, &filename);

        let re = Regex::new(&args.regex)?;

        let mysql = Conn::new(Opts::from_url(&args.source)?).await?;
        let mut binlog_stream = mysql.get_binlog_stream(BinlogStreamRequest::new(args.server_id)
            .with_pos(log_pos as u64)
            .with_filename(filename.as_bytes())
        ).await?;

        while let Some(Ok(event)) = binlog_stream.next().await {
            if let Some(e) = event.read_data()? {
                match e {
                    EventData::RotateEvent(e) => {
                        log_pos = event.header().log_pos() as i64;
                        filename = e.name().to_string();
                    }
                    EventData::RowsEvent(rows_event) => {
                        let tme = binlog_stream.get_tme(rows_event.table_id()).ok_or(anyhow!("no tme"))?;
                        let (db, table) = (tme.database_name().to_string(), tme.table_name().to_string());

                        if ! re.is_match(&format!("{}.{}", db, table)) {
                            continue;
                        }

                        let change_type = match rows_event {
                            RowsEventData::WriteRowsEvent(_) => ChangeType::Insert,
                            RowsEventData::UpdateRowsEvent(_) => ChangeType::Update,
                            RowsEventData::DeleteRowsEvent(_) => ChangeType::Delete,
                            _ => ChangeType::Other,
                        };

                        match rows_event {
                            RowsEventData::WriteRowsEvent(_) |
                            RowsEventData::UpdateRowsEvent(_) |
                            RowsEventData::DeleteRowsEvent(_) => {
                                let new_pos = event.header().log_pos();
                                if new_pos > 0 {
                                    log_pos = new_pos as i64;
                                }

                                yield Change {
                                    db: db.to_string(),
                                    table: table.to_string(),
                                    op: change_type.clone(),
                                    log_pos: log_pos as u32,
                                    filename: filename.clone(),
                                    ts: event.header().timestamp(),
                                    rows: rows_event.clone().rows(tme)
                                        .map(|row| {
                                            match row {
                                                Ok((before, after)) => (before, after),
                                                _ => panic!("{:?}", row),
                                            }
                                        })
                                    .collect(),
                                };
                            },
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

struct DSL<'a> {
    rhai: Engine,
    ast: AST,
    scope: Scope<'a>,
}

impl DSL<'_> {
    fn new<'a>(script: Option<PathBuf>) -> Self {
        let rhai = Engine::new();
        let scope = Scope::new();
        let ast = script.map(|path| {
            rhai.compile_file_with_scope(&scope, path)
        }).unwrap().unwrap();

        DSL {rhai, ast, scope}
    }

    fn transform(&mut self, db: String, table: String, op: ChangeType, before: rhai::Map, after: rhai::Map, ts: u32) -> String {
        let fields = self.rhai.call_fn::<rhai::Map>(
            &mut self.scope,
            &self.ast,
            "transform",
            (
                db.clone(),
                table.clone(),
                op.to_string(),
                before,
                after,
                ts
            )
        ).map_err(|e| dbg!(e)).unwrap();

        rhai::format_map_as_json(&fields)
    }

    fn topic(&mut self, db: String, table: String) -> String {
        self.rhai.call_fn::<String>(
            &mut self.scope,
            &self.ast,
            "topic",
            (
                db.clone(),
                table.clone()
            )
        ).map_err(|e| dbg!(e)).unwrap()
    }
}

fn setup_state(path: &String, server_id: u32) -> Result<(i64, String), Error> {
    let connection = sqlite::open(path)?;

    connection.execute("create table if not exists log_pos (server_id integer primary key, pos integer not null, filename text not null) strict")?;

    let mut statement = connection.prepare("select max(4, pos) pos, filename from log_pos where server_id = ?")?;
    statement.bind((1, server_id as i64))?;
    let (pos, filename) = statement.next().and_then(|s| {
        match s {
            sqlite::State::Row => Ok((statement.read::<i64, _>("pos").unwrap(), statement.read::<String, _>("filename").unwrap())),
            _ => sqlite::Result::Err(sqlite::Error { code: None, message: None }),
        }
    }).unwrap_or((4, "".into())); // 4 means first value :shrug:

    Ok((pos, filename))
}

fn row_image_to_map(row_image: BinlogRow) -> rhai::Map {
    let cols: Vec<String> = row_image.columns_ref().iter().map(|c| {
        c.name_str().to_string()
    }).collect();

    let fields: rhai::Map = Row::try_from(row_image).unwrap()
        .unwrap() // as Vec
        .iter()
        .enumerate()
        .map(|(i, mv)| {
            let k = cols.get(i).unwrap();
            let v = match mv {
                Value::Int(v) => Dynamic::from(*v),
                Value::UInt(v) => Dynamic::from(*v),
                Value::Float(v) => Dynamic::from(*v),
                Value::Double(v) => Dynamic::from(*v),
                Value::Date(y, m, d, 0, 0, 0, 0) => Dynamic::from(format!("{:04}-{:02}-{:02}", y, m, d)),
                Value::Date(year, month, day, hour, minute, second, 0) => Dynamic::from(format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                    year, month, day, hour, minute, second
                )),
                Value::Date(year, month, day, hour, minute, second, micros) => Dynamic::from(format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                    year, month, day, hour, minute, second, micros
                )),
                Value::Time(neg, d, h, i, s, 0) => {
                    Dynamic::from(if neg.clone() {
                        format!("-{:03}:{:02}:{:02}", d * 24 + u32::from(h.clone()), i, s)
                    } else {
                        format!("{:03}:{:02}:{:02}", d * 24 + u32::from(h.clone()), i, s)
                    })
                }
                Value::Time(neg, days, hours, minutes, seconds, micros) => {
                    Dynamic::from(if neg.clone() {
                        format!(
                            "-{:03}:{:02}:{:02}.{:06}",
                            days * 24 + u32::from(hours.clone()),
                            minutes,
                            seconds,
                            micros
                        )
                    } else {
                        format!(
                            "{:03}:{:02}:{:02}.{:06}",
                            days * 24 + u32::from(hours.clone()),
                            minutes,
                            seconds,
                            micros
                        )
                    })
                },
                Value::Bytes(ref bytes) => match from_utf8(bytes) {
                    Ok(v) => Dynamic::from(v.to_string()),
                    Err(_) => {
                        let mut s = String::new();
                        for c in bytes.iter() {
                            s.extend(format!("{:02X}", *c).chars())
                        }
                        Dynamic::from(s)
                    },
                }
                Value::NULL => Dynamic::from(()),
            };
            (k.into(), v)
        })
        .collect()
    ;
    fields
}
