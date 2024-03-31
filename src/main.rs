use async_stream::stream;
use async_stream::try_stream;
use futures::stream::Stream;
use futures::pin_mut;
use futures::stream::StreamExt;

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
struct Change {
    db: String,
    table: String,
    op: ChangeType,
    before: Option<BinlogRow>,
    after: Option<BinlogRow>,
    ts: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let s = persist(args.clone(),
        publish(
            transform(
                args.clone(),
                mysql_changes(args.clone()),
            )
        )
    );
    pin_mut!(s); // needed for iteration

    while let Some(value) = s.next().await {
        println!("got {:?}", value);
    };

    Ok(())
}

fn mysql_changes(args: Args) -> impl Stream<Item = Result<(u32, String, Change)>> {
    try_stream! {
        let (pos, mut filename) = setup_state(&args.state, args.server_id).unwrap();
        dbg!(&pos, &filename);

        let mysql = Conn::new(Opts::from_url(&args.source)?).await?;
        let mut binlog_stream = mysql.get_binlog_stream(BinlogStreamRequest::new(args.server_id)
            .with_pos(pos as u64)
            .with_filename(filename.as_bytes())
        ).await?;

        while let Some(Ok(event)) = binlog_stream.next().await {

            if let Some(e) = event.read_data()? {
                match e {
                    EventData::RotateEvent(e) => {
                        filename = e.name().to_string();
                    }
                    EventData::RowsEvent(rows_event) => {
                        let tme = binlog_stream.get_tme(rows_event.table_id()).ok_or(anyhow!("no tme"))?;
                        let (db, table) = (tme.database_name().to_string(), tme.table_name().to_string());

                        let re = Regex::new(&args.regex).unwrap();
                        if ! re.is_match(&format!("{}.{}", db, table)) {
                            // yield Err(());
                            // continue;
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
                                for row in rows_event.clone().rows(tme) {
                                     match row {
                                        Ok((before, after)) => {

                                            let ts = event.header().timestamp();

                                            yield (
                                                event.header().log_pos(),
                                                filename.clone(),
                                                Change {
                                                    db: db.to_string(),
                                                    table: table.to_string(),
                                                    op: change_type.clone(),
                                                    before,
                                                    after,
                                                    ts,
                                                }
                                            );
                                        },
                                        _ => panic!("{:?}", row)
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

fn transform<S: Stream<Item = Result<(u32, String, Change)>>>(args: Args, input: S)
    -> impl Stream<Item = (u32, String, String, String)>
{
    stream! {
        let rhai = Engine::new();
        let mut scope = Scope::new();
        let ast = args.script.map(|path| {
            rhai.compile_file_with_scope(&scope, path)
        }).unwrap().unwrap();

        for await change in input {
            let (pos, filename, Change {db, table, op, before, after, ts}) = change.unwrap();
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

            let topic = rhai
                .call_fn::<String>(&mut scope, &ast, "topic", (db.to_string(), table.to_string(),))
                .map_err(|e| dbg!(e))
                .unwrap_or(table.to_string())
            ;

            yield (
                pos,
                filename,
                topic,
                rhai::format_map_as_json(&fields)
            );
        }
    }
}

fn publish<S: Stream<Item = (u32, String, String, String)>>(input: S)
    -> impl Stream<Item = Result<(u32, String)>>
{
    try_stream! {
        let pubsub = Client::new(
            ClientConfig::default().with_auth().await?
        ).await?;

        let mut publishers: BTreeMap<String, Publisher> = BTreeMap::new();

        for await (pos, filename, topic, data) in input {
            let msg = PubsubMessage {
                data: data.into(),
                ..Default::default()
            };

            let publisher = publishers.entry(topic.clone()).or_insert_with(|| {
                 dbg!(&topic);
                 pubsub.topic(&topic).new_publisher(None)
            });

            // publisher.publish_immediately(vec!(msg), None).await?;

            yield (pos, filename);
        }
    }
}
fn persist<S: Stream<Item = Result<(u32, String)>>>(args: Args, input: S)
    -> impl Stream<Item = Result<()>>
{
    try_stream! {
        let connection = sqlite::open(&args.state).unwrap();
        for await p in input {
            let (pos, filename) = p?;
            let mut statement = connection.prepare("
                insert into log_pos (server_id, pos, filename) values (?, max(4, ?), ?)
                on conflict do update set
                pos = excluded.pos,
                filename = excluded.filename
            ").unwrap();
            statement.bind((1, args.server_id as i64))?;
            statement.bind((2, pos as i64))?;
            statement.bind((3, filename.as_str()))?;
            statement.next()?;
            // dbg!((pos, filename));

            yield ();
        }
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
    }).unwrap_or((4, "".into()));

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
                Value::Int(v) => Dynamic::from(*v),
                Value::UInt(v) => Dynamic::from(*v),
                Value::Float(v) => Dynamic::from(*v),
                Value::Double(v) => Dynamic::from(*v),
                // mysql::Value::Date(v) => Dynamic::from(v),
                // mysql::Value::Time(v) => Dynamic::from(v),
                // Value::NULL => Dynamic::from(()),
                _ => Dynamic::from(()),
            };
            (k.into(), v)
        })
        .collect()
    ;
    fields
}
