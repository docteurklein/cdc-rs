use std::collections::HashMap;
use mysql::binlog::events::*;
use mysql::{Column, Row, Conn, Opts, BinlogRequest};
use serde_json::*;
use serde::{Serialize};
use postgres_types::{ToSql, Json};
use std::iter::zip;

use postgres::{Client, NoTls};

#[derive(Debug, Serialize, ToSql)]
struct Event {
    table_: String,
    name: String,
    index_: i8,
    rows: Vec<EventRow>
}

#[derive(Debug, Serialize, ToSql)]
struct EventRow {
    before: Option<Vec<Value>>,
    after: Option<Vec<Value>>,
}

fn main() -> mysql::Result<()> {
    let mut pg = Client::connect("host=localhost user=postgres", NoTls).unwrap();
    let pos: u64 = 4;//pg.query_one("select max(next_position) from \"mysql binlog\".event", &[]).unwrap().get(0);
    let mut mysql = Conn::new(Opts::from_url("mysql://root:root@127.0.0.1:49154")?)?;
    let mut binlog_stream = mysql.get_binlog_stream(
            BinlogRequest::new(11114)
            .with_filename("de9f0c68a241-bin.000001".as_bytes().to_vec())
            .with_pos(pos)
    ).unwrap();
    let mut tmes = HashMap::new();
    let mut events: Vec<RowsEventData> = vec!();

    while let Some(event) = binlog_stream.next() {
        let event = event.unwrap();
        if  let Some(e) = event.read_data()? {
            match e {
                EventData::RowsEvent(ee) => {
                    match ee {
                        RowsEventData::WriteRowsEvent(_) => {
                            //dbg!(eee);
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
                    let rows = events.iter().enumerate().map(|(index, event)| Event {
                        table_: tmes[&event.table_id()].table_name().to_string(),
                        name: "writerows".to_string(), // todo
                        index_: index as i8,
                        rows: event.rows(&tmes[&event.table_id()]).map(|r| {
                            match r {
                                Ok((before, after)) => {
                                    EventRow {
                                        before: before.map(|before| Some(Row::try_from(before).unwrap().unwrap().iter().map(|a| json!(a.as_sql(true))).collect())).unwrap_or(None),
                                        after: after.map(|after| Some(Row::try_from(after).unwrap().unwrap().iter().map(|a| json!(a.as_sql(true))).collect())).unwrap_or(None)
                                    }
                                },
                                _ => panic!("{:?}", r)
                            }
                        }).collect()
                    }).collect();
                    dbg!(&ee.xid);
                    dbg!(&event.header().log_pos());
                    pg.execute("insert into \"mysql binlog\".event
                        (tenant, table_, name, xid, timestamp_, next_position, index_, rows)
                        select $1, table_, name, $2, $3, $4, index_, rows
                        from jsonb_populate_recordset(null::\"mysql binlog\".event, $5) _
                        on conflict do nothing;
                    ",
                    &[
                        &"tenant#1",
                        &(ee.xid as i64),
                        &(event.header().timestamp() as i64),
                        &(event.header().log_pos() as i64),
                        &Json::<Vec<Event>>(rows),
                    ]).unwrap();
                    events.clear();
                }
                _ => {}
            }
        };
    }
    Ok(())
}
