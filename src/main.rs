use std::collections::HashMap;
use std::str::from_utf8;
use std::borrow::Cow;
use mysql::binlog::events::*;
use mysql::binlog::EventType;
use mysql::consts::ColumnType;
use mysql::{Column, Row, Conn, Opts, BinlogRequest, from_value};
use serde_json::*;
use google_cloud_pubsub::client::{ClientConfig, Client};
use google_cloud_googleapis::pubsub::v1::{PubsubMessage};
use google_cloud_pubsub::publisher::Publisher;
use anyhow::Result;
use anyhow::anyhow;
use regex::Regex;
use sqlite;

#[tokio::main]
async fn main() -> Result<()> {
    let server_id: u32 = 1;

    let connection = sqlite::open("cdc-rs.sqlite")?;
    connection.execute("create table if not exists log_pos (server_id integer primary key, pos integer not null, filename text not null) strict")?;

    let mut statement = connection.prepare("select max(4, pos) pos, filename from log_pos where server_id = ?")?;
    statement.bind((1, server_id as i64))?;
    let (mut pos, mut filename) = statement.next().and_then(|s| {
        match s {
            sqlite::State::Row => Ok((statement.read::<i64, _>("pos").unwrap(), statement.read::<String, _>("filename").unwrap())),
            _ => sqlite::Result::Err(sqlite::Error { code: None, message: None }),
        }
    }).unwrap_or((4, "".into()));
    dbg!(&pos, &filename);
    
    let mut mysql = Conn::new(Opts::from_url("mysql://root:root@127.0.0.1:3306")?)?;
    let mut binlog_stream = mysql.get_binlog_stream(
            BinlogRequest::new(server_id)
            .with_pos(pos as u32)
            .with_filename(filename.as_bytes().to_vec())
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

    while let event = binlog_stream.next() {
        if ! event.is_some() {
            continue;
        }
        let event = event.unwrap()?;

        if let Some(e) = event.read_data()? {
            let msgs: Vec<PubsubMessage> = vec!();
            match e {
                EventData::RotateEvent(e) => {
                    filename = e.name().to_string();
                }
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
                                                dbg!(&col);
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

            if event.header().log_pos() > 0 {
                pos = event.header().log_pos() as i64;
            }

            // let mut file = NamedTempFile::new()?;
            // write!(file, "{}", event.header().log_pos())?;
            // file.persist("./log_pos")?;

            let mut statement = connection.prepare("
                insert into log_pos (server_id, pos, filename) values (?, max(4, ?), ?)
                on conflict do update set
                pos = excluded.pos,
                filename = excluded.filename
            ")?;
            statement.bind((1, server_id as i64))?;
            statement.bind((2, pos))?;
            statement.bind((3, filename.as_str()))?;
            statement.next()?;

            dbg!(&pos, &filename);
        }
    }
    Ok(())
}
