use std::convert::identity;
use std::path::PathBuf;
use std::collections::BTreeMap;
use std::str::from_utf8;
use mysql_async::binlog::events::*;
use mysql_async::binlog::row::BinlogRow;
use mysql_async::{Value, Row, Conn, Opts, BinlogStreamRequest};
use google_cloud_pubsub::client::{ClientConfig, Client};
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::publisher::Publisher;
use anyhow::{Error, Result};
use regex::Regex;
use clap::Parser;
use rhai::{Dynamic, Engine, Scope};
use tokio::sync::mpsc;
use futures::{StreamExt};
use anyhow::anyhow;
use std::fmt;

#[derive(Parser, Debug)]
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

#[derive(Clone, Debug)]
enum ChangeType {
    Insert,
    Update,
    Delete,
    Other,
}

impl fmt::Display for ChangeType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // write!(f, "{:?}", self)
        fmt::Debug::fmt(self, f)
    }
}

#[derive(Clone, Debug)]
struct Change {
    op: ChangeType,
    before: Option<BinlogRow>,
    after: Option<BinlogRow>,
    ts: u32,
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
    }).unwrap_or((4, "".into()));

    Ok((pos, filename))
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Args::parse();

    let (log_pos_tx, mut log_pos_rx) = mpsc::unbounded_channel::<(i64, String)>();

    let (pos, mut filename) = setup_state(&cli.state, cli.server_id)?;
    dbg!(&pos, &filename);

    let log_pos_task = tokio::spawn(async move {
        let connection = sqlite::open(&cli.state.clone()).unwrap();
        while let Some((pos, filename)) = log_pos_rx.recv().await {
            let mut statement = connection.prepare("
                insert into log_pos (server_id, pos, filename) values (?, max(4, ?), ?)
                on conflict do update set
                pos = excluded.pos,
                filename = excluded.filename
            ").unwrap();
            statement.bind((1, cli.server_id as i64))?;
            statement.bind((2, pos))?;
            statement.bind((3, filename.as_str()))?;
            statement.next()?;
            dbg!((pos, filename));
        }
        Ok::<(), Error>(())
    });

    let (pubsub_tx, mut pubsub_rx) = mpsc::unbounded_channel::<(String, Vec<String>)>();

    let (transform_tx, transform_rx) = std::sync::mpsc::channel::<(String, String, Vec<Change>)>();

    let transformer_thread = std::thread::spawn(move || {
        let rhai = Engine::new();
        let mut scope = Scope::new();
        let ast = cli.script.map(|path| {
            rhai.compile_file_with_scope(&scope, path)
        }).unwrap().unwrap();

        while let Ok((db, table, changes)) = transform_rx.recv() {
            let msgs = changes.into_iter().map(|Change {op, before, after, ts}| {

                let before = before.clone().map(row_image_to_map).map_or(BTreeMap::new(), identity);
                let after = after.clone().map(row_image_to_map).map_or(BTreeMap::new(), identity);

                let fields = rhai.call_fn::<rhai::Map>(
                    &mut scope,
                    &ast,
                    "transform",
                    (
                        db.clone(),
                        table.clone(),
                        op.to_string(),
                        before,
                        after,
                        ts
                    )
                ).map_err(|e| dbg!(e))
                .unwrap();

                rhai::format_map_as_json(&fields)
            }).collect();

            let topic = rhai
                .call_fn::<String>(&mut scope, &ast, "topic", (db.to_string(), table.to_string(),))
                .map_err(|e| dbg!(e))
                .unwrap_or(table.to_string())
            ;

            pubsub_tx.send((topic, msgs)).map_err(|e| dbg!(e)).unwrap();
        }
    });

    let publisher_task = tokio::spawn(async move {
        let pubsub = Client::new(
            ClientConfig::default().with_auth().await?
        ).await?;

        let mut publishers: BTreeMap<String, Publisher> = BTreeMap::new();

        while let Some((topic, msgs)) = pubsub_rx.recv().await {
            let msgs = msgs.iter().map(|data| {
                PubsubMessage {
                    data: data.to_string().into(),
                    ..Default::default()
                }
            }).collect();

            let publisher = publishers.entry(topic.clone()).or_insert_with(|| {
                 dbg!(&topic);
                 pubsub.topic(&topic).new_publisher(None)
            });

            dbg!(&msgs);
            publisher.publish_immediately(msgs, None).await?;
        };
        Ok::<(), Error>(())
    });

    let mysql = Conn::new(Opts::from_url(&cli.source)?).await?;
    let mut binlog_stream = mysql.get_binlog_stream(BinlogStreamRequest::new(cli.server_id)
        .with_pos(pos as u64)
        .with_filename(filename.as_bytes())
    ).await?;

    while let Some(event) = binlog_stream.next().await {
        let event = event.unwrap();

        if let Some(e) = event.read_data()? {
            match e {
                EventData::RotateEvent(e) => {
                    filename = e.name().to_string();
                }
                EventData::RowsEvent(rows_event) => {
                    let tme = binlog_stream.get_tme(rows_event.table_id()).ok_or(anyhow!("no tme"))?;
                    let (db, table) = (tme.database_name().to_string(), tme.table_name().to_string());

                    let re = Regex::new(&cli.regex).unwrap();
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
                            let changes: Vec<Change> = rows_event.clone().rows(tme).map(|row| {
                                 match row {
                                    Ok((before, after)) => {

                                        let ts = event.header().timestamp();

                                        Change {
                                            op: change_type.clone(),
                                            before,
                                            after,
                                            ts,
                                        }
                                    },
                                    _ => panic!("{:?}", row)
                                }
                            }).collect();

                            transform_tx.send((db.to_string(), table.to_string(), changes))?;
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
            if event.header().log_pos() > 0 {
                log_pos_tx.send((event.header().log_pos() as i64, filename.clone()))?;
            }
        }
    }

    // transformer_thread.join().unwrap();

    publisher_task.await?
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
                Value::NULL => Dynamic::from(()),
                Value::Bytes(ref bytes) => match from_utf8(bytes) {
                    Ok(v) => Dynamic::from(v.to_string()),
                    Err(_) => {
                        let mut s = String::from("0x");
                        for c in bytes.iter() {
                            s.extend(format!("{:02X}", *c).chars())
                        }
                        Dynamic::from(s)
                    },
                }
                Value::Int(v) => Dynamic::from(*v),
                Value::UInt(v) => Dynamic::from(*v),
                Value::Float(v) => Dynamic::from(*v),
                Value::Double(v) => Dynamic::from(*v),
                // mysql::Value::Date(v) => Dynamic::from(v),
                // mysql::Value::Time(v) => Dynamic::from(v),
                _ => Dynamic::from("NOPE"),
            };
            (k.into(), v)
        })
        .collect()
    ;
    fields
}
