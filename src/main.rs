use fallible_iterator::FallibleIterator;
use std::time::Duration;
use std::pin::Pin;
use core::task::{Context, Poll};
use async_stream::try_stream;
use tokio_rusqlite::{params, Connection, OptionalExtension};
use tokio_stream::{StreamExt, Stream};
use mysql_async::{FromRowError, Params};
use rhai::AST;
use std::path::PathBuf;
use std::collections::BTreeMap;
use std::str::from_utf8;
use mysql_async::prelude::*;
use mysql_async::binlog::events::*;
use mysql_async::{BinlogStreamRequest, Conn, Opts, Row, Value};
use google_cloud_pubsub::client::{ClientConfig, Client};
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::publisher::Publisher;
use anyhow::{Error, Result};
use regex::Regex;
use clap::Parser;
use rhai::{Dynamic, Engine, Scope};
use std::fmt;

#[derive(Parser, Debug, Clone)]
struct Config {
    #[arg(long, env)]
    state: String,

    #[arg(short, long, env)]
    server_id: u32,

    #[arg(short, long, env)]
    regex: String,

    #[arg(long, env)]
    source: String,

    #[arg(long, env)]
    script: Option<PathBuf>,

    #[arg(long, env, default_value_t = 1000)]
    backfill_batch_size: i16,
}

#[derive(Clone, Debug, Default)]
enum ChangeType {
    #[default]
    Insert,
    Update,
    Delete,
    Backfill,
}

#[derive(Clone, Debug, Default)]
struct Change {
    op: ChangeType,
    db: String,
    table: String,
    row: (Option<rhai::Map>, Option<rhai::Map>),
    ts: u32,
    pkey: String,
}

impl fmt::Display for ChangeType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl Change {
    fn update_with(self, change: Change) -> Self {
        if self.db != change.db || self.table != change.table {
            return self;
        }
        match (&self.row, &change.row) {
            ((_, Some(row)), (_, Some(new))) => {
                let rowpkey = row.get::<str>(self.pkey.as_ref()).unwrap().to_string();
                let newpkey = new.get::<str>(self.pkey.as_ref()).unwrap().to_string();
                if rowpkey == newpkey {
                    return change;
                }
            }
            _ => (),
        };
        self
    }
}

struct CorrectedBackfill {
    backfills: Pin<Box<dyn Stream<Item = Result<Change, Error>>>>,
    changes: Pin<Box<dyn Stream<Item = Result<Change, Error>>>>,
}

impl Stream for CorrectedBackfill {
    type Item = Result<Change, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> Poll<Option<Self::Item>>
    {
        let both = (
            self.backfills.as_mut().poll_next(cx),
            self.changes.as_mut().poll_next(cx)
        );

        match both {
            (Poll::Ready(Some(Ok(backfill))), Poll::Ready(Some(Ok(change)))) =>
                Poll::Ready(Some(Ok(
                    backfill.update_with(change.to_owned())
                ))),
            (Poll::Pending, Poll::Ready(Some(Ok(change)))) => Poll::Ready(Some(Ok(change.to_owned()))),
            (Poll::Ready(Some(Ok(backfill))), Poll::Pending) => Poll::Ready(Some(Ok(backfill))),
            (Poll::Ready(Some(Ok(backfill))), Poll::Ready(None)) => Poll::Ready(Some(Ok(backfill))),
            (Poll::Ready(None), Poll::Ready(None)) => Poll::Ready(None),
            // (Poll::Ready(Some(Err(e1))), Poll::Ready(Some(Err(e2)))) => Poll::Ready(Some(Err((e1, e2)))),
            (Poll::Ready(Some(Err(e))), _) => Poll::Ready(Some(Err(e))),
            (_, Poll::Ready(Some(Err(e)))) => Poll::Ready(Some(Err(e))),
            _ => Poll::Pending,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();
    let sqlite = Connection::open(&config.state).await?;
    let mut dsl = DSL::new(config.clone().script);
    let pubsub = Client::new(
        ClientConfig::default().with_auth().await?
    ).await?;
    let mut publishers: BTreeMap<String, Publisher> = BTreeMap::new();

    // let _ = setup_state(sqlite.clone());

    sqlite.call(|conn| {
        conn.execute_batch(
            "create table if not exists log_pos (
                server_id integer primary key,
                pos integer not null,
                filename text not null
            ) strict;

            create table if not exists backfill (
                db text not null,
                relation text not null,
                selection text not null default '*',
                pkey text not null,
                range text not null,
                status text,
                position blob,
                primary key (db, relation, range)
            ) strict
        ")?;
        Ok(())
    }).await?;

    let changes = mysql_changes(sqlite.clone(), config.clone());
    let backfills = backfill(sqlite.clone(), config.clone());

    let mut both = CorrectedBackfill { backfills: Box::pin(backfills), changes: Box::pin(changes), };

    while let Some(change) = both.next().await {
        let Change {db, table, op, row: (before, after), ts, pkey: _} = change?;

        let data = dsl.transform(
            db.clone(),
            table.clone(),
            op.clone(),
            before.clone(),
            after.clone(),
            ts
        );

        let _msg = PubsubMessage {
            data: data.to_string().into(),
            ..Default::default()
        };


        let topic = dsl.topic(db.clone(), table.clone());

        let _publisher = publishers.entry(topic.clone()).or_insert_with(|| {
             dbg!(&topic);
             pubsub.topic(&topic).new_publisher(None)
        });

        // publisher.publish_immediately(vec!(msg), None).await?;
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct RawRow(Row);

impl FromRow for RawRow {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError> {
        Ok(Self(row))
    }
}

fn mysql_changes(sqlite: Connection, config: Config) -> impl Stream<Item = Result<Change>> {
    try_stream! {
        let (mut log_pos, mut filename) = sqlite.call(move |connection| {
            let mut statement = connection.prepare("select max(4, pos) pos, filename from log_pos where server_id = ?1")?;
            Ok(statement
                .query_row([&config.server_id], |row| {
                    Ok((
                        row.get(0)?,
                        row.get::<usize, String>(1)?,
                    ))
                })
                .optional().map(|r| {
                    r.or_else(|| {
                        Some((4i64, "".into()))
                    })
                }))
        }).await?.unwrap().unwrap();
        
        let re = Regex::new(&config.regex)?;

        let mysql = Conn::new(Opts::from_url(&config.source)?).await?;

        let mut binlog_stream = mysql.get_binlog_stream(BinlogStreamRequest::new(config.server_id)
            // .with_gtid()
            .with_pos(log_pos as u64)
            .with_filename(filename.as_bytes())
        ).await?;

        // for await event in &binlog_stream {
        while let Some(Ok(event)) = binlog_stream.next().await {
            match event.read_data()? {
                Some(EventData::RotateEvent(e)) if ! e.is_fake() => {
                    log_pos = e.position() as i64;
                    dbg!(&log_pos);
                    filename = e.name().to_string();
                }
                Some(EventData::TableMapEvent(_)) => {
                    let filename = filename.clone();
                    sqlite.call(move |connection| {
                        // let tx = Transaction::new(connection, TransactionBehavior::Immediate)?;
                        connection.execute("
                            insert into log_pos (server_id, pos, filename) values (?1, ?2, ?3)
                            on conflict do update set
                            pos = excluded.pos,
                            filename = excluded.filename;
                        ", params![
                            config.server_id as i64,
                            log_pos as i64,
                            filename
                        ])
                        .map_err(|e| e.into())
                        // tx.commit().map_err(|e| e.into())
                    }).await?;
                }
                Some(EventData::RowsEvent(rows_event)) => {
                    let tme = binlog_stream.get_tme(rows_event.table_id()).expect("no TME");
                    let (db, table) = (tme.database_name().to_string(), tme.table_name().to_string());

                    if ! re.is_match(&format!("{}.{}", db, table)) {
                        continue;
                    }

                    match rows_event {
                        RowsEventData::WriteRowsEvent(_) |
                        RowsEventData::UpdateRowsEvent(_) |
                        RowsEventData::DeleteRowsEvent(_) => {
                            let new_pos = event.header().log_pos();
                            if new_pos > 0 {
                                log_pos = new_pos as i64;
                                dbg!(&log_pos);
                            }

                            for row in rows_event.clone().rows(tme) {
                                yield Change {
                                    pkey: "".into(), // @TODO
                                    db: db.to_string(),
                                    table: table.to_string(),
                                    op: match rows_event {
                                        RowsEventData::WriteRowsEvent(_) => ChangeType::Insert,
                                        RowsEventData::UpdateRowsEvent(_) => ChangeType::Update,
                                        RowsEventData::DeleteRowsEvent(_) => ChangeType::Delete,
                                        _ => unreachable!(),
                                    },
                                    ts: event.header().timestamp(),
                                    row: match row {
                                        Ok((before, after)) => (
                                            before.map(Row::try_from).map(Result::unwrap).map(row_to_map),
                                            after.map(Row::try_from).map(Result::unwrap).map(row_to_map),
                                        ),
                                        _ => (None, None), //panic!("{:?}", row),
                                    }
                                };
                            }
                        },
                        _ => {}
                    }
                }
                _ => {}
            }
        }
    }
}

fn backfill(sqlite: Connection, config: Config) -> impl Stream<Item = Result<Change>> {
    try_stream! {
        loop {
            let rows = sqlite.call(move |sqlite| {
                let mut stmt = sqlite.prepare("select db, relation, selection, range, pkey from backfill where status = ?1")?;
                let rows = stmt.query([
                    "todo"
                ])?;
                rows
                    .map(|r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)))
                    .collect::<Vec<(String, String, String, String, String)>>()
                    .map_err(|e| e.into())
            }).await?;

            for (db, relation, selection, _range, pkey) in rows {
                let mut filter: String  = "true or ?".to_owned();
                let mut params = vec![Value::NULL];
                loop {
                    let mysql = Conn::new(Opts::from_url(&config.source)?).await?; // @TODO reuse shared?

                    let query = format!(
                        "select {}
                        from {}.{}
                        where {}
                        order by {} asc
                        limit {}",
                        selection.clone(),
                        db.clone(),
                        relation.clone(),
                        filter,
                        pkey,
                        config.backfill_batch_size,
                    );
                    let page: Vec<RawRow> = query.with(Params::Positional(params.clone())).fetch(mysql).await?;

                    if let Some(RawRow(row)) = page.last() {
                        let last: Option<String> = row.get(pkey.as_ref());
                        filter = format!(
                            "{} > ?",
                            pkey,
                        );
                        params = vec![
                            last.unwrap().into(),
                        ];
                    }
                    else {
                        sqlite.call(move |connection| {
                            // let tx = Transaction::new(connection, TransactionBehavior::Immediate)?;
                            connection.execute("
                                update backfill
                                set status = ?1
                                where (db, relation) = (?2, ?3)
                            ", params![
                                "done",
                                db.clone(),
                                relation.clone()
                            ])
                            .map_err(|e| e.into())
                            // tx.commit().map_err(|e| e.into())
                        }).await?;

                        break;
                    }

                    for row in page {
                        yield Change {
                            pkey: pkey.clone(),
                            db: db.clone(),
                            table: relation.clone(),
                            op: ChangeType::Backfill,
                            ts: 1, // @TODO
                            row: (
                                None,
                                Some(row_to_map(row.0.clone())),
                            ),
                        };
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(1000)).await;
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

    fn transform(&mut self, db: String, table: String, op: ChangeType, before: Option<rhai::Map>, after: Option<rhai::Map>, ts: u32) -> String {
        let fields = self.rhai.call_fn::<rhai::Map>(
            &mut self.scope,
            &self.ast,
            "transform",
            (
                db.clone(),
                table.clone(),
                op.to_string(),
                before.map_or(Dynamic::from(()), Dynamic::from), // either pass the map to dynamic::from, or pass Unit
                after.map_or(Dynamic::from(()), Dynamic::from),
                ts
            )
        ).unwrap();

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
        ).unwrap()
    }
}

// async fn setup_state(sqlite: Connection) -> Result<(), Error> {
//     await
//     .map_err(|e| e.into())
// }

fn row_to_map(row: Row) -> rhai::Map {
    let cols: Vec<String> = row.columns_ref().iter().map(|c| {
        c.name_str().to_string()
    }).collect();

    row.unwrap() // as Vec
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
                },
                Value::NULL => Dynamic::from(()),
            };
            (k.into(), v)
        })
    .collect()
}
