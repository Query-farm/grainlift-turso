// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Many clients at once, through the native driver, for a while.
//!
//! Each client loops over a mix of operations (bound-batch inserts, queries,
//! committed and rolled-back transactions, parameterized queries, catalog
//! metadata) against one local database, then the totals and the server's
//! handle counts are checked. Runs for `GRAINLIFT_TURSO_SOAK_SECONDS`
//! (default 5) with `GRAINLIFT_TURSO_SOAK_CLIENTS` clients (default 12).
//! Skips unless `GRAINLIFT_DRIVER` names the native driver library.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{ObjectDepth, OptionConnection, OptionValue};
use adbc_core::{Connection, Optionable, Statement};
use adbc_driver_manager::ManagedConnection;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use common::{Server, connect};

fn setting(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn scalar(connection: &mut ManagedConnection, sql: &str) -> Result<i64> {
    let mut statement = connection.new_statement()?;
    statement.set_sql_query(sql)?;
    let batches = statement
        .execute()?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(batches[0].column(0).as_primitive::<Int64Type>().value(0))
}

fn update(connection: &mut ManagedConnection, sql: &str) -> Result<Option<i64>> {
    let mut statement = connection.new_statement()?;
    statement.set_sql_query(sql)?;
    statement.execute_update()
}

/// Whether `error` is a write that waited out the busy timeout: retryable,
/// and nothing it attempted was committed.
fn is_busy(error: &Error) -> bool {
    let sqlstate = error
        .sqlstate
        .iter()
        .map(|byte| *byte as u8 as char)
        .collect::<String>();
    error.status == Status::Timeout && sqlstate == "40001"
}

/// What one client did.
#[derive(Default)]
struct Outcome {
    committed: i64,
    operations: u64,
    busy: u64,
}

/// One client's loop. Writes that fail as busy are counted and abandoned (a
/// real client would retry them); everything else must succeed.
fn client(url: String, id: i64, until: Instant) -> Result<Outcome> {
    let mut connection = connect(&url, Some("test-token"))?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("client", DataType::Int64, false),
        Field::new("n", DataType::Int64, false),
    ]));
    let mut outcome = Outcome::default();
    let mut round = 0i64;
    while Instant::now() < until {
        round += 1;
        outcome.operations += 1;
        match round % 5 {
            // Insert a bound batch of 100 rows, atomically.
            0 => {
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![id; 100])),
                        Arc::new(Int64Array::from_iter_values(0..100)),
                    ],
                )
                .expect("valid batch");
                let mut statement = connection.new_statement()?;
                statement.set_sql_query("INSERT INTO t VALUES (?, ?)")?;
                statement.bind(batch)?;
                match statement.execute_update() {
                    Ok(changed) => {
                        assert_eq!(changed, Some(100));
                        outcome.committed += 100;
                    }
                    Err(error) if is_busy(&error) => outcome.busy += 1,
                    Err(error) => return Err(error),
                }
            }
            // Read back what this client has committed so far.
            1 => {
                let seen = scalar(
                    &mut connection,
                    &format!("SELECT count(*) FROM t WHERE client = {id}"),
                )?;
                assert_eq!(
                    seen, outcome.committed,
                    "client {id} sees exactly its own commits"
                );
            }
            // A transaction that rolls back, then one that commits.
            2 => {
                connection.set_option(OptionConnection::AutoCommit, OptionValue::from("false"))?;
                let attempt = (|| {
                    update(&mut connection, &format!("INSERT INTO t VALUES ({id}, -1)"))?;
                    connection.rollback()?;
                    update(&mut connection, &format!("INSERT INTO t VALUES ({id}, -2)"))?;
                    connection.commit()
                })();
                match attempt {
                    Ok(()) => outcome.committed += 1,
                    Err(error) if is_busy(&error) => {
                        outcome.busy += 1;
                        connection.rollback()?;
                    }
                    Err(error) => return Err(error),
                }
                connection.set_option(OptionConnection::AutoCommit, OptionValue::from("true"))?;
            }
            // A parameterized query.
            3 => {
                let mut statement = connection.new_statement()?;
                statement.set_sql_query("SELECT ? + 1")?;
                let parameter =
                    Arc::new(Schema::new(vec![Field::new("p", DataType::Int64, false)]));
                statement.bind(
                    RecordBatch::try_new(parameter, vec![Arc::new(Int64Array::from(vec![round]))])
                        .expect("valid batch"),
                )?;
                let batches = statement
                    .execute()?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                assert_eq!(
                    batches[0].column(0).as_primitive::<Int64Type>().value(0),
                    round + 1
                );
            }
            // Catalog metadata.
            _ => {
                let objects =
                    connection.get_objects(ObjectDepth::All, None, None, Some("t"), None, None)?;
                let batches = objects.collect::<std::result::Result<Vec<_>, _>>()?;
                assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            }
        }
    }
    Ok(outcome)
}

#[test]
fn many_clients_for_a_while() {
    let _driver = require_driver!();
    let seconds = setting("GRAINLIFT_TURSO_SOAK_SECONDS", 5);
    let clients = setting("GRAINLIFT_TURSO_SOAK_CLIENTS", 12) as i64;
    let (backend, _directory) = common::local_backend();
    let server = Server::start(backend, &[("test-token", "alice")], None);
    {
        let mut connection = connect(&server.url, Some("test-token")).unwrap();
        update(
            &mut connection,
            "CREATE TABLE t (client INTEGER, n INTEGER)",
        )
        .unwrap();
    }
    let until = Instant::now() + Duration::from_secs(seconds);
    let workers = (0..clients)
        .map(|id| {
            let url = server.url.clone();
            std::thread::spawn(move || client(url, id, until))
        })
        .collect::<Vec<_>>();
    let (mut committed, mut operations, mut busy) = (0, 0, 0);
    for worker in workers {
        let outcome = worker.join().unwrap().unwrap();
        committed += outcome.committed;
        operations += outcome.operations;
        busy += outcome.busy;
    }
    let mut connection = connect(&server.url, Some("test-token")).unwrap();
    let total = scalar(&mut connection, "SELECT count(*) FROM t").unwrap();
    assert_eq!(total, committed, "every committed row, and only those");
    assert_eq!(
        scalar(&mut connection, "SELECT count(*) FROM t WHERE n = -1").unwrap(),
        0
    );
    drop(connection);
    assert_eq!(server.open_handles(), (0, 0), "no session or result leaked");
    eprintln!(
        "{clients} clients, {seconds}s: {operations} operations, {committed} rows committed, \
         {busy} writes abandoned as busy"
    );
}
