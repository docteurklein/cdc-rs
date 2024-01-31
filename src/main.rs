use std::collections::HashMap;
use futures::future::join_all;
use mysql::binlog::events::*;
use mysql::{Column, Row, Conn, Opts, BinlogRequest};
use serde_json::*;
use serde::{Serialize};
// use postgres_types::{ToSql, Json};
use std::iter::zip;
use google_cloud_pubsub::client::{ClientConfig, Client};
use google_cloud_googleapis::pubsub::v1::{PubsubMessage};
use anyhow::Result;

// use postgres::{Client, NoTls};

#[derive(Debug, Serialize)]
struct Event {
    db: String,
    table: String,
    rows: Vec<EventRow>
}

#[derive(Debug, Serialize)]
struct EventRow {
    before: Option<Vec<Value>>,
    after: Option<Vec<Value>>,
}

// #[derive(Debug, Serialize)]
// struct Debezium {
//     op: &str,
//     schema: HashMap<_, _>,
//     payload: {
//         before: Option<Vec<Value>>,
//         after: Option<Vec<Value>>,
//     },
// }

#[tokio::main]
async fn main() -> Result<()> {
    // let mut pg = Client::connect("host=localhost user=postgres port=4566", NoTls).unwrap();
    let config = ClientConfig::default().with_auth().await?;
    let pos: u64 = 4; // pg.query_one("select max(next_position) from \"mysql binlog\".event", &[]).unwrap().get(4);
    let mut mysql = Conn::new(Opts::from_url("mysql://root:root@127.0.0.1:3306")?)?;
    let mut binlog_stream = mysql.get_binlog_stream(
            BinlogRequest::new(1)
            .with_pos(pos)
    )?;
    let mut tmes = HashMap::new();
    let mut events: Vec<RowsEventData> = vec!();

    let pubsub = Client::new(config).await?;

    let topic = pubsub.topic("projects/my-project-id/subscriptions/all_pims.pim_catalog_product");
    let publisher = topic.new_publisher(None);

    while let Some(Ok(event)) = binlog_stream.next() {
        if let Some(e) = event.read_data()? {
            match e {
                EventData::RowsEvent(ee) => {
                    match ee {
                        RowsEventData::WriteRowsEvent(_) => {
                            events.push(ee.into_owned());
                        },
                        RowsEventData::UpdateRowsEvent(_) => {
                            events.push(ee.into_owned());
                        },
                        RowsEventData::DeleteRowsEvent(_) => {
                            events.push(ee.into_owned());
                        },
                        _ => {}
                    }
                }
                EventData::TableMapEvent(ee) => {
                    tmes.insert(ee.table_id(), ee.into_owned());
                }
                EventData::XidEvent(ee) => {
                    for rowsEvent in events.iter() {
                        dbg!(&ee.xid);
                        dbg!(&event.header().log_pos());

                        let msgs: Vec<PubsubMessage> = rowsEvent.rows(&tmes[&rowsEvent.table_id()]).map(|row| {
                             match row {
                                Ok((before, Some(after))) => {
                                    for col in after.columns_ref() {
                                        dbg!(col.name_str());
                                    }
                                    // dbg!(&after.columns_ref().iter().map(|c| c.name_ref().into()).collect::<Vec<String>>());

                                    PubsubMessage {
                                       data: "test".into(),
                                       // ordering_key: "order".into(),
                                       ..Default::default()
                                    }
                                },
                                _ => panic!("{:?}", row)
                            }
                        }).collect();

                        let mut awaiters = publisher.publish_bulk(msgs).await;
                        // join_all(awaiters.iter().map(|awaiter| awaiter.get()).collect::<Vec<_>>()).await;
                    }
                    events.clear();
                }
                _ => {}
            }
        };
    }
    Ok(())
}
