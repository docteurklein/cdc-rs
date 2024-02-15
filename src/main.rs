use std::convert::identity;
use std::path::PathBuf;
use std::collections::BTreeMap;
use std::str::from_utf8;
use mysql::binlog::events::*;
use mysql::binlog::row::BinlogRow;
use mysql::{Row, Conn, Opts, BinlogRequest};
use google_cloud_pubsub::client::{ClientConfig, Client};
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::publisher::Publisher;
use anyhow::{Error, Result};
use regex::Regex;
use clap::Parser;
use rhai::{Dynamic, Engine, Scope};
use tokio::sync::mpsc;

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

    let rhai = Engine::new();

    let (tx, mut rx) = mpsc::channel::<(i64, String)>(12);

    let mut scope = Scope::new();
    let ast = cli.script.map(|path| {
        rhai.compile_file_with_scope(&scope, path)
    }).unwrap().unwrap();

    let (pos, mut filename) = setup_state(&cli.state, cli.server_id)?;
    dbg!(&pos, &filename);

    let connection = sqlite::open(&cli.state.clone()).unwrap();
    let persister = tokio::spawn(async move {
        while let Some((pos, filename)) = rx.recv().await {
            let mut statement = connection.prepare("
                insert into log_pos (server_id, pos, filename) values (?, max(4, ?), ?)
                on conflict do update set
                pos = excluded.pos,
                filename = excluded.filename
            ").unwrap();
            statement.bind((1, cli.server_id as i64)).unwrap();
            statement.bind((2, pos)).unwrap();
            statement.bind((3, filename.as_str())).unwrap();
            statement.next().unwrap();
            dbg!((pos, filename));
            ()
        }
    });

    let mysql = Conn::new(Opts::from_url(&cli.source)?)?;
    let mut binlog_stream = mysql.get_binlog_stream(
            BinlogRequest::new(cli.server_id)
            .with_pos(pos as u32)
            .with_filename(filename.as_bytes().to_vec())
    )?;

    // let pubsub = Client::new(
    //     ClientConfig::default().with_auth().await?
    // ).await?;

    let mut publishers: BTreeMap<String, Publisher> = BTreeMap::new();

    while let Some(event) = binlog_stream.next() {
        let event = event.unwrap();

        if let Some(e) = event.read_data()? {
            match e {
                EventData::RotateEvent(e) => {
                    filename = e.name().to_string();
                    // tx.send((event.header().log_pos() as i64, filename)).await?;
                }
                EventData::RowsEvent(rows_event) => {
                    let tme = binlog_stream.get_tme(rows_event.table_id()).unwrap();
                    let (db, table) = (tme.database_name().to_string(), tme.table_name().to_string());

                    let re = Regex::new(&cli.regex).unwrap();
                    if ! re.is_match(&format!("{}.{}", db, table)) {
                        continue;
                    }

                    let change_type = match rows_event {
                        RowsEventData::WriteRowsEvent(_) => "INSERT",
                        RowsEventData::UpdateRowsEvent(_) => "UPDATE",
                        RowsEventData::DeleteRowsEvent(_) => "DELETE",
                        _ => ""
                    };

                    match rows_event {
                        RowsEventData::WriteRowsEvent(_) |
                        RowsEventData::UpdateRowsEvent(_) |
                        RowsEventData::DeleteRowsEvent(_) => {
                            let msgs: Vec<PubsubMessage> = rows_event.clone().rows(tme).map(|row| {
                                 match row {
                                    Ok((before, after)) => {

                                        let before = before.clone().map(row_image_to_map).map_or(BTreeMap::new(), identity);
                                        let after = after.clone().map(row_image_to_map).map_or(BTreeMap::new(), identity);

                                        let ts = event.header().timestamp();

                                        let fields = rhai
                                            .call_fn::<rhai::Map>(&mut scope, &ast, "transform", (db.clone(), table.clone(), change_type, before, after, ts))
                                            .map_err(|e| dbg!(e))
                                            .unwrap()
                                        ;

                                        let data = rhai::format_map_as_json(&fields);
                                        dbg!(&data);
                                        PubsubMessage {
                                            data: data.into(),
                                            ..Default::default()
                                        }
                                    },
                                    _ => panic!("{:?}", row)
                                }
                            }).collect();

                            // let publisher = publishers.entry(tme.table_name().to_string()).or_insert_with(|| {
                            //     let topic = rhai
                            //         .call_fn::<String>(&mut scope, &ast, "topic", (db, table.clone(),))
                            //         .map_err(|e| dbg!(e))
                            //         .unwrap_or(table.clone())
                            //     ;
                            //     dbg!(&topic);
                            //     pubsub.topic(&topic).new_publisher(None)
                            // });
                    
                            //publisher.publish_immediately(msgs, None).await?;

                        }
                        _ => {}
                    }
                }
                _ => {}
            }
            if event.header().log_pos() > 0 {
                tx.send((event.header().log_pos() as i64, filename.clone())).await?;
            }
        }
    }
    persister.await?;

    Ok(())
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
                mysql::Value::NULL => Dynamic::from(()),
                mysql::Value::Bytes(ref bytes) => match from_utf8(bytes) {
                    Ok(v) => Dynamic::from(v.to_string()),
                    Err(_) => {
                        let mut s = String::from("0x");
                        for c in bytes.iter() {
                            s.extend(format!("{:02X}", *c).chars())
                        }
                        Dynamic::from(s)
                    },
                }
                mysql::Value::Int(v) => Dynamic::from(*v),
                mysql::Value::UInt(v) => Dynamic::from(*v),
                mysql::Value::Float(v) => Dynamic::from(*v),
                mysql::Value::Double(v) => Dynamic::from(*v),
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
