use std::path::PathBuf;
use std::collections::BTreeMap;
use std::str::from_utf8;
use std::borrow::Cow;
use mysql::binlog::events::*;
use mysql::binlog::EventType;
use mysql::consts::ColumnType;
use mysql::{Column, Row, Conn, Opts, BinlogRequest, from_value};
use serde_json::*;
use serde::{Serialize};
use google_cloud_pubsub::client::{ClientConfig, Client};
use google_cloud_googleapis::pubsub::v1::{PubsubMessage};
use google_cloud_pubsub::publisher::Publisher;
use anyhow::Result;
use anyhow::anyhow;
use regex::Regex;
use sqlite;
use clap::{Parser};
use rhai::{CallFnOptions, Dynamic, Engine, Scope, AST, CustomType, TypeBuilder};

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    state: String,

    #[arg(short, long)]
    server_id: u32,

    #[arg(short, long)]
    regex: String,

    #[arg(short, long)]
    source: String,

    #[arg(short, long)]
    script: Option<PathBuf>,
}

// #[derive(Debug, Clone, CustomType, Serialize)]
// #[rhai_type()]
// struct Event {
//     after: BTreeMap<String, Value>,
// }

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Args::parse();

    let mut rhai = Engine::new();
    // rhai.build_type::<Event>();

    let mut scope = Scope::new();
    let ast = cli.script.map(|path| {
        rhai.compile_file_with_scope(&scope, path)
    }).unwrap().unwrap();

    // let options = CallFnOptions::new().eval_ast(false).rewind_scope(false);

    let connection = sqlite::open(cli.state)?;
    connection.execute("create table if not exists log_pos (server_id integer primary key, pos integer not null, filename text not null) strict")?;

    let mut statement = connection.prepare("select max(4, pos) pos, filename from log_pos where server_id = ?")?;
    statement.bind((1, cli.server_id as i64))?;
    let (mut pos, mut filename) = statement.next().and_then(|s| {
        match s {
            sqlite::State::Row => Ok((statement.read::<i64, _>("pos").unwrap(), statement.read::<String, _>("filename").unwrap())),
            _ => sqlite::Result::Err(sqlite::Error { code: None, message: None }),
        }
    }).unwrap_or((4, "".into()));
    dbg!(&pos, &filename);

    let mut new_filename: String = filename.clone();
    
    let mut mysql = Conn::new(Opts::from_url(&cli.source)?)?;
    let mut binlog_stream = mysql.get_binlog_stream(
            BinlogRequest::new(cli.server_id)
            .with_pos(pos as u32)
            .with_filename(filename.as_bytes().to_vec())
    )?;
    let mut events: Vec<RowsEventData> = vec!();

    let pubsub = Client::new(
        ClientConfig::default().with_auth().await?
    ).await?;

    let mut publishers: BTreeMap<String, Publisher> = BTreeMap::new();

    while let Some(event) = binlog_stream.next() {
        let event = event.unwrap();

        if let Some(e) = event.read_data()? {
            let msgs: Vec<PubsubMessage> = vec!();
            match e {
                EventData::RotateEvent(e) => {
                    new_filename = e.name().to_string();
                }
                // EventData::TableMapEvent(e) => {
                //     tmes.insert(e.table_id(), e);
                // }
                EventData::RowsEvent(rowsEvent) => {
                    // let tme = tmes.get(&rowsEvent.table_id()).unwrap();
                    let tme = binlog_stream.get_tme(rowsEvent.table_id());
                    // let tme = tmes.get(rowsEvent.table_id()).unwrap();
                    if tme.is_none() {
                        continue;
                    }
                    let tme = tme.unwrap();
                    let (db, table) = (tme.database_name().to_string(), tme.table_name().to_string());

                    let re = Regex::new(&cli.regex).unwrap();
                    if ! re.is_match(&format!("{}.{}", db, table)) {
                        continue;
                    }

                    let changeType = match rowsEvent {
                        RowsEventData::WriteRowsEvent(_) => "UPSERT",
                        RowsEventData::UpdateRowsEvent(_) => "UPSERT",
                        RowsEventData::DeleteRowsEvent(_) => "DELETE",
                        _ => ""
                    };

                    match rowsEvent {
                        RowsEventData::WriteRowsEvent(_) |
                        RowsEventData::UpdateRowsEvent(_) |
                        RowsEventData::DeleteRowsEvent(_) => {
                            let msgs: Vec<PubsubMessage> = rowsEvent.clone().rows(tme).map(|row| {
                                 match row {
                                    Ok((before, after)) => {
                                        let rowImage = after.or(before);

                                        let cols: Vec<String> = rowImage.as_ref().map(|rowImage| {
                                            rowImage.columns_ref().iter().map(|c| {
                                                c.name_str().to_string()
                                            }).collect()
                                        }).unwrap();
                                        
                                        let mut fields: rhai::Map = Row::try_from(rowImage.unwrap())
                                            .unwrap() // Result
                                            .unwrap() // as Vec
                                            .iter()
                                            .enumerate()
                                            .map(|(i, mv)| {
                                                let k = cols.get(i).unwrap();
                                                let v = match mv {
                                                    mysql::Value::NULL => Dynamic::from(()),
                                                    mysql::Value::Bytes(ref bytes) => match from_utf8(&*bytes) {
                                                        Ok(v) => Dynamic::from(v.to_string()),
                                                        Err(_) => {
                                                            let mut s = String::from("0x");
                                                            for c in bytes.iter() {
                                                                s.extend(format!("{:02X}", *c).chars())
                                                            }
                                                            Dynamic::from(s)
                                                        },
                                                    }
                                                    mysql::Value::Int(v) => Dynamic::from(v.clone()),
                                                    mysql::Value::UInt(v) => Dynamic::from(v.clone()),
                                                    mysql::Value::Float(v) => Dynamic::from(v.clone()),
                                                    mysql::Value::Double(v) => Dynamic::from(v.clone()),
                                                    // mysql::Value::Date(v) => Dynamic::from(v),
                                                    // mysql::Value::Time(v) => Dynamic::from(v),
                                                    _ => Dynamic::from("NOPE"),
                                                };
                                                (k.into(), v)
                                            })
                                            .collect()
                                        ;
                                        // fields.insert("_CHANGE_TYPE".into(), Dynamic::from(changeType));
                                        // fields.insert("db".into(), Dynamic::from(db.clone()));

                                        // let event = Event {
                                        //     after: fields,
                                        // };
                                        // dbg!(&json!(event));
                                        // let dynFields = fields.clone();

                                        let fields = rhai
                                            .call_fn::<rhai::Map>(&mut scope, &ast, "transform", (db.clone(), table.clone(), changeType, fields.clone()))
                                            .map_err(|e| dbg!(e))
                                            .unwrap_or(fields)
                                        ;
                                        dbg!(&fields);

                                        PubsubMessage {
                                            data: rhai::format_map_as_json(&fields).into(),
                                            ..Default::default()
                                        }
                                    },
                                    _ => panic!("{:?}", row)
                                }
                            }).collect();

                            let publisher = publishers.entry(tme.table_name().to_string()).or_insert_with(|| {
                                let topic = rhai
                                    .call_fn::<String>(&mut scope, &ast, "topic", (db, tme.table_name().to_string(),))
                                    .map_err(|e| dbg!(e))
                                    .unwrap_or(tme.table_name().to_string())
                                ;
                                dbg!(&topic);
                                pubsub.topic(&topic).new_publisher(None)
                            });
                    
                            publisher.publish_immediately(msgs, None).await?;

                            pos = event.header().log_pos() as i64;
                            filename = new_filename.clone();
                        }
                        _ => {}
                    }
                }
                _ => {}
            }

            let mut statement = connection.prepare("
                insert into log_pos (server_id, pos, filename) values (?, max(4, ?), ?)
                on conflict do update set
                pos = excluded.pos,
                filename = excluded.filename
            ")?;
            statement.bind((1, cli.server_id as i64))?;
            statement.bind((2, pos))?;
            statement.bind((3, filename.as_str()))?;
            statement.next()?;

            dbg!(&pos, &filename);
        }
    }
    Ok(())
}
