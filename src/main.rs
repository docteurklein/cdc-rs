use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::collections::HashMap;
use std::str::from_utf8;
use std::borrow::Cow;
use futures::future::join_all;
use mysql::binlog::events::*;
use mysql::binlog::EventType;
use mysql::consts::ColumnType;
use mysql::{Column, Row, Conn, Opts, BinlogRequest, from_value};
// use mysql::prelude::{FromRow};
use serde_json::*;
use google_cloud_pubsub::client::{ClientConfig, Client};
use google_cloud_googleapis::pubsub::v1::{PubsubMessage};
use google_cloud_pubsub::publisher::Publisher;
use anyhow::Result;
use regex::Regex;
use tempfile::NamedTempFile;
use sqlite;

// #[derive(FromRow)]
// #[mysql(crate_name = "cdc_rs")]
// struct Product {
//     id: u64,
//     product_model_id: u64,
//     raw_values: Value,
// }

fn read_log_pos(file: &str) -> Result<u32, std::io::Error> {
    File::open(file).map(|f| {
        let mut input = BufReader::new(f);
        let mut buffer = [0_u8; std::mem::size_of::<u32>()];
        input.read_exact(&mut buffer);
        dbg!(&buffer);
        u32::from_be_bytes(buffer)
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let server_id: u32 = 1;
    // let pos: u32 = read_log_pos("./log_pos").unwrap_or(4);

    let connection = sqlite::open("cdc-rs.sqlite")?;
    connection.execute("create table if not exists log_pos (server_id integer primary key, pos integer) strict")?;

    let mut statement = connection.prepare("select pos from log_pos where server_id = ?")?;
    statement.bind((1, server_id as i64))?;
    let pos = statement.next().and_then(|s| {
        match s {
            sqlite::State::Row => statement.read::<i64, _>("pos"),
            _ => sqlite::Result::Err(sqlite::Error { code: None, message: None }),
        }
    }).unwrap_or(4);
    dbg!(&pos);
    
    let mut mysql = Conn::new(Opts::from_url("mysql://root:root@127.0.0.1:3306")?)?;
    let mut binlog_stream = mysql.get_binlog_stream(
            BinlogRequest::new(server_id)
            .with_pos(pos as u32)
    )?;
    let mut events: Vec<RowsEventData> = vec!();

    let pubsub = Client::new(
        ClientConfig::default().with_auth().await?
    ).await?;

    let mut publishers: HashMap<String, Publisher> = HashMap::new();

    let colMap = HashMap::from([
        ("pim_catalog_product", HashMap::from([
            ("@0", "uuid"),
            ("@1", "family_id"),
            ("@2", "product_model_id"),
            ("@3", "family_variant_id"),
            ("@4", "id"),
            ("@5", "is_enabled"),
            ("@6", "identifier"),
            ("@7", "raw_values"),
            ("@8", "created"),
            ("@9", "updated"),
        ])),
    ]);

    while let Some(Ok(event)) = binlog_stream.next() {
        if let Some(e) = event.read_data()? {
            let msgs: Vec<PubsubMessage> = vec!();
            match e {
                EventData::RowsEvent(rowsEvent) => {
                    let tme = &binlog_stream.get_tme(rowsEvent.table_id()).unwrap();

                    let re = Regex::new(r"^pim.*\.pim_catalog_product$").unwrap();
                    if ! re.is_match(&format!("{}.{}", tme.database_name(), tme.table_name())) {
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
                            let msgs: Vec<PubsubMessage> = rowsEvent.rows(tme).map(|row| {
                                 match row {
                                    Ok((before, after)) => {
                                        let rowImage = after.or(before);
                                        let cols: Vec<(String, ColumnType)> = rowImage.clone().map(|rowImage| {
                                            rowImage.columns_ref().iter().map(|c| {
                                                let col = c.name_str();
                                                let colName = colMap.get(tme.table_name().as_ref())
                                                    .map_or(col.as_ref(), |t| t.get(&col.as_ref()).unwrap())
                                                ;
                                                (colName.to_string(), c.column_type())
                                            }).collect()
                                        }).unwrap();
                                        
                                        let mut data: Map<String, Value> = Row::try_from(rowImage.unwrap())
                                            .unwrap() // Result
                                            .unwrap() // as Vec
                                            .iter()
                                            .enumerate()
                                            .map(|(i, mv)| {
                                                let (k, typ) = cols.get(i).unwrap();
                                                let v = match mv {
                                                    mysql::Value::NULL => json!(null),
                                                    mysql::Value::Bytes(ref bytes) => match from_utf8(&*bytes) {
                                                        Ok(string) => json!(string),
                                                        Err(_) => {
                                                            let mut s = String::from("0x");
                                                            for c in bytes.iter() {
                                                                s.extend(format!("{:02X}", *c).chars())
                                                            }
                                                            json!(s)
                                                        },
                                                    }
                                                    mysql::Value::Int(v) => json!(v),
                                                    mysql::Value::UInt(v) => json!(v),
                                                    mysql::Value::Float(v) => json!(v),
                                                    mysql::Value::Double(v) => json!(v),
                                                    // mysql::Value::Date(v) => json!(v),
                                                    // mysql::Value::Time(v) => json!(v),
                                                    _ => json!("NOPE"),
                                                };
                                                (k.to_string(), v)
                                            })
                                            .collect()
                                        ;
                                        data.insert("_CHANGE_TYPE".to_string(), changeType.into());
                                        data.insert("db".to_string(), tme.database_name().into());

                                        dbg!(&json!(data));

                                        PubsubMessage {
                                            data: json!(data).to_string().into(),
                                            ..Default::default()
                                        }
                                    },
                                    _ => panic!("{:?}", row)
                                }
                            }).collect();

                            let publisher = publishers.entry(tme.table_name().to_string()).or_insert_with(|| {
                                let topic = pubsub.topic(&format!("projects/akecld-prd-pim-saas-fci/topics/all_pims.{}", tme.table_name().as_ref()));
                                dbg!(&topic.fully_qualified_name());
                                topic.new_publisher(None)
                            });
                    
                            publisher.publish_immediately(msgs, None).await?;
                        }
                        _ => {}
                    }
                }
                _ => {}
            }

            // let mut file = NamedTempFile::new()?;
            // write!(file, "{}", event.header().log_pos())?;
            // file.persist("./log_pos")?;

            let mut statement = connection.prepare("insert into log_pos (server_id, pos) values (?, ?) on conflict do update set pos = excluded.pos")?;
            statement.bind((1, server_id as i64))?;
            statement.bind((2, event.header().log_pos() as i64))?;
            statement.next()?;
        }
    }
    Ok(())
}
