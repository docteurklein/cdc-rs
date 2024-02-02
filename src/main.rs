use std::collections::HashMap;
use futures::future::join_all;
use mysql::binlog::events::*;
use mysql::binlog::EventType;
use mysql::{Column, Row, Conn, Opts, BinlogRequest};
// use mysql::prelude::{FromRow};
use serde_json::*;
use google_cloud_pubsub::client::{ClientConfig, Client};
use google_cloud_googleapis::pubsub::v1::{PubsubMessage};
use anyhow::Result;

// #[derive(FromRow)]
// #[mysql(crate_name = "cdc_rs")]
// struct Product {
//     id: u64,
//     product_model_id: u64,
//     raw_values: Value,
// }

#[tokio::main]
async fn main() -> Result<()> {
    let pos: u64 = 4; // pg.query_one("select max(next_position) from \"mysql binlog\".event", &[]).unwrap().get(4);
    let mut mysql = Conn::new(Opts::from_url("mysql://root:root@127.0.0.1:3306")?)?;
    let mut binlog_stream = mysql.get_binlog_stream(
            BinlogRequest::new(1)
            .with_pos(pos)
    )?;
    let mut events: Vec<RowsEventData> = vec!();

    let pubsub = Client::new(
        ClientConfig::default().with_auth().await?
    ).await?;

    let topic = pubsub.topic("projects/my-project-id/topics/all_pims.pim_catalog_product");
    let publisher = topic.new_publisher(None);

    while let Some(Ok(event)) = binlog_stream.next() {
        if let Some(e) = event.read_data()? {
            let msgs: Vec<PubsubMessage> = vec!();
            match e {
                EventData::RowsEvent(rowsEvent) => {
                    let tme = &binlog_stream.get_tme(rowsEvent.table_id()).unwrap();
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
                                    Ok((_before, after)) => {
                                        let cols: Vec<String> = after.clone().map(|after| after.columns_ref().iter().map(|c| c.name_str().into()).collect()).unwrap();
                                        
                                        let mut data: HashMap<String, String> = Row::try_from(after.unwrap())
                                            .unwrap() // Result
                                            .unwrap() // as Vec
                                            .iter()
                                            .enumerate()
                                            .map(|(i, v)| (cols.get(i).unwrap().to_string(), v.as_sql(true)))
                                            .collect()
                                        ;
                                        data.insert("_CHANGE_TYPE".to_string(), changeType.into());

                                        let msg = PubsubMessage {
                                            data: json!(data).to_string().into(),
                                            ..Default::default()
                                        };
                                        dbg!(&msg);
                                        msg
                                    },
                                    _ => panic!("{:?}", row)
                                }
                            }).collect();

                            publisher.publish_immediately(msgs, None).await?;
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}
