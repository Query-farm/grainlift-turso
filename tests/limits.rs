// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Deadlines, cancellation, concurrency and bounded memory.

use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use adbc_core::error::{Result, Status};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Int64Array, RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use grainlift_server::backend::{Backend, BackendConnection};
use grainlift_server::config::TargetConfig;
use grainlift_turso::{DEV_TARGET, Limits, Location, TargetSpec, TursoBackend};

/// Counts forever: only a deadline or a cancel stops it.
const ENDLESS: &str =
    "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c";

fn target() -> TargetConfig {
    TargetConfig {
        driver: DEV_TARGET.into(),
        entrypoint: None,
        database_options: Vec::new(),
        connection_options: Vec::new(),
        allow_client_database_options: false,
        allow_client_connection_options: false,
        allowed_client_database_options: Vec::new(),
        allowed_client_connection_options: Vec::new(),
        init_statements: Vec::new(),
    }
}

fn limits(timeout: Duration) -> Limits {
    Limits {
        operation_timeout: timeout,
        busy_timeout: Duration::from_millis(timeout.as_millis() as u64 / 2),
    }
}

/// A backend over a fresh local file with `timeout` per operation.
fn local(timeout: Duration) -> (TursoBackend, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("limits.db");
    let spec = TargetSpec {
        limits: limits(timeout),
        ..TargetSpec::new(Location::parse(path.to_str().unwrap(), None, false))
    };
    (
        TursoBackend::new([(DEV_TARGET.to_string(), spec)]).unwrap(),
        directory,
    )
}

fn connect(backend: &TursoBackend) -> Box<dyn BackendConnection> {
    backend.open(&target(), Vec::new(), Vec::new()).unwrap()
}

fn query(connection: &mut dyn BackendConnection, sql: &str) -> Result<Vec<RecordBatch>> {
    let mut statement = connection.new_statement()?;
    statement.set_sql_query(sql)?;
    statement
        .execute_result()?
        .into_reader()
        .map(|batch| {
            batch.map_err(|error| match error {
                ArrowError::ExternalError(inner) => {
                    match inner.downcast::<adbc_core::error::Error>() {
                        Ok(error) => *error,
                        Err(other) => adbc_core::error::Error::with_message_and_status(
                            other.to_string(),
                            Status::Unknown,
                        ),
                    }
                }
                other => adbc_core::error::Error::with_message_and_status(
                    other.to_string(),
                    Status::Unknown,
                ),
            })
        })
        .collect()
}

fn update(connection: &mut dyn BackendConnection, sql: &str) -> Result<Option<i64>> {
    let mut statement = connection.new_statement()?;
    statement.set_sql_query(sql)?;
    statement.execute_update()
}

fn scalar(connection: &mut dyn BackendConnection, sql: &str) -> i64 {
    query(connection, sql).unwrap()[0]
        .column(0)
        .as_primitive::<Int64Type>()
        .value(0)
}

#[test]
fn a_local_operation_stops_at_its_deadline() {
    let (backend, _directory) = local(Duration::from_secs(1));
    let mut connection = connect(&backend);
    let started = Instant::now();
    let error = query(&mut *connection, ENDLESS).unwrap_err();
    assert_eq!(error.status, Status::Timeout, "{}", error.message);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "stopped after {:?}",
        started.elapsed()
    );
    // The connection is still usable.
    assert_eq!(scalar(&mut *connection, "SELECT 41 + 1"), 42);
}

#[test]
fn a_local_operation_can_be_cancelled() {
    let (backend, _directory) = local(Duration::from_secs(30));
    let mut connection = connect(&backend);
    let cancel = connection.cancel_handle();
    let canceller = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(300));
        cancel.try_cancel().unwrap();
    });
    let started = Instant::now();
    let error = query(&mut *connection, ENDLESS).unwrap_err();
    canceller.join().unwrap();
    assert_eq!(error.status, Status::Cancelled, "{}", error.message);
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(scalar(&mut *connection, "SELECT 7"), 7);
    // Cancelling with nothing running is harmless.
    connection.cancel_handle().try_cancel().unwrap();
    assert_eq!(scalar(&mut *connection, "SELECT 8"), 8);
}

#[test]
fn a_slow_reader_is_not_timed_out() {
    let (backend, _directory) = local(Duration::from_secs(1));
    let mut connection = connect(&backend);
    let mut statement = connection.new_statement().unwrap();
    statement
        .set_sql_query("WITH RECURSIVE n(x) AS (SELECT 0 UNION ALL SELECT x + 1 FROM n WHERE x < 4999) SELECT x FROM n")
        .unwrap();
    let mut reader = statement.execute_result().unwrap().into_reader();
    let mut rows = reader.next().unwrap().unwrap().num_rows();
    // Longer than the deadline: the deadline is per fetch, not per result.
    std::thread::sleep(Duration::from_millis(1_500));
    for batch in reader {
        rows += batch.unwrap().num_rows();
    }
    assert_eq!(rows, 5000);
}

#[test]
fn concurrent_local_writers_wait_for_each_other() {
    let (backend, _directory) = local(Duration::from_secs(20));
    let backend = Arc::new(backend);
    update(
        &mut *connect(&backend),
        "CREATE TABLE t (writer INTEGER, n INTEGER)",
    )
    .unwrap();
    let writers = (0..8)
        .map(|writer| {
            let backend = Arc::clone(&backend);
            std::thread::spawn(move || {
                let mut connection = connect(&backend);
                for n in 0..100 {
                    update(
                        &mut *connection,
                        &format!("INSERT INTO t VALUES ({writer}, {n})"),
                    )
                    .unwrap();
                }
                // And a transaction per writer.
                connection
                    .set_option("adbc.connection.autocommit", "false".into())
                    .unwrap();
                for n in 100..150 {
                    update(
                        &mut *connection,
                        &format!("INSERT INTO t VALUES ({writer}, {n})"),
                    )
                    .unwrap();
                }
                connection.commit().unwrap();
            })
        })
        .collect::<Vec<_>>();
    for writer in writers {
        writer.join().unwrap();
    }
    assert_eq!(
        scalar(&mut *connect(&backend), "SELECT count(*) FROM t"),
        8 * 150
    );
}

/// A reader that counts the batches pulled from it.
struct Counting {
    schema: SchemaRef,
    remaining: usize,
    pulled: Arc<AtomicUsize>,
}

impl Iterator for Counting {
    type Item = std::result::Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let first = self.pulled.fetch_add(1, Ordering::SeqCst) as i64 * 1000;
        Some(Ok(RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(first..first + 1000))],
        )
        .unwrap()))
    }
}

impl RecordBatchReader for Counting {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[test]
fn bound_streams_are_read_while_executing() {
    let (backend, _directory) = local(Duration::from_secs(20));
    let mut connection = connect(&backend);
    update(&mut *connection, "CREATE TABLE t (n INTEGER)").unwrap();
    let pulled = Arc::new(AtomicUsize::new(0));
    let reader = Counting {
        schema: Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
        remaining: 50,
        pulled: Arc::clone(&pulled),
    };
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query("INSERT INTO t VALUES (?)").unwrap();
    statement.bind_stream(Box::new(reader)).unwrap();
    assert_eq!(pulled.load(Ordering::SeqCst), 0, "binding reads nothing");
    assert_eq!(statement.execute_update().unwrap(), Some(50_000));
    assert_eq!(pulled.load(Ordering::SeqCst), 50);
    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::InvalidState, "a stream is read once");
    assert_eq!(scalar(&mut *connection, "SELECT count(*) FROM t"), 50_000);
}

/// A server that accepts connections and never answers.
fn black_hole() -> (String, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let thread = std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in listener.incoming().take(16) {
            held.push(stream);
        }
    });
    (url, thread)
}

fn unresponsive_cloud(timeout: Duration) -> TursoBackend {
    let (url, _thread) = black_hole();
    // No server token: nothing is contacted until a client connects.
    let spec = TargetSpec {
        limits: limits(timeout),
        ..TargetSpec::new(Location::parse(&url, None, false))
    };
    TursoBackend::new([(DEV_TARGET.to_string(), spec)]).unwrap()
}

fn connect_with_token(backend: &TursoBackend) -> Box<dyn BackendConnection> {
    backend
        .open(
            &target(),
            vec![("turso.auth_token".into(), "token".into())],
            Vec::new(),
        )
        .unwrap()
}

#[test]
fn a_turso_cloud_request_stops_at_its_deadline() {
    let backend = unresponsive_cloud(Duration::from_secs(1));
    let mut connection = connect_with_token(&backend);
    let started = Instant::now();
    let error = query(&mut *connection, "SELECT 1").unwrap_err();
    assert_eq!(error.status, Status::Timeout, "{}", error.message);
    assert!(started.elapsed() < Duration::from_secs(5));
    let error = update(&mut *connection, "CREATE TABLE t (x)").unwrap_err();
    assert_eq!(error.status, Status::Timeout, "{}", error.message);
}

#[test]
fn a_turso_cloud_request_can_be_cancelled() {
    let backend = unresponsive_cloud(Duration::from_secs(30));
    let mut connection = connect_with_token(&backend);
    let cancel = connection.cancel_handle();
    let canceller = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(300));
        cancel.try_cancel().unwrap();
    });
    let started = Instant::now();
    let error = query(&mut *connection, "SELECT 1").unwrap_err();
    canceller.join().unwrap();
    assert_eq!(error.status, Status::Cancelled, "{}", error.message);
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[test]
fn a_server_token_target_that_does_not_answer_fails_to_start() {
    let (url, _thread) = black_hole();
    let spec = TargetSpec {
        limits: limits(Duration::from_secs(1)),
        ..TargetSpec::new(Location::parse(&url, Some("token".into()), false))
    };
    let started = Instant::now();
    let error = TursoBackend::new([("cloud".to_string(), spec)])
        .err()
        .unwrap();
    assert_eq!(error.status, Status::Timeout, "{}", error.message);
    assert!(error.message.contains("\"cloud\""), "{}", error.message);
    assert!(!error.message.contains("token"), "{}", error.message);
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[test]
fn an_interrupt_does_not_wedge_a_connection_with_an_open_result() {
    let (backend, _directory) = local(Duration::from_secs(1));
    let mut connection = connect(&backend);
    // An unfinished result stays open on the connection.
    let mut open = connection.new_statement().unwrap();
    open.set_sql_query(
        "WITH RECURSIVE n(x) AS (SELECT 0 UNION ALL SELECT x + 1 FROM n WHERE x < 99999) SELECT x FROM n",
    )
    .unwrap();
    let mut reader = open.execute_result().unwrap().into_reader();
    assert!(reader.next().unwrap().is_ok());
    // Another operation on the same connection times out...
    let error = query(&mut *connection, ENDLESS).unwrap_err();
    assert_eq!(error.status, Status::Timeout, "{}", error.message);
    // ...and the connection still works afterwards.
    assert_eq!(scalar(&mut *connection, "SELECT 5"), 5);
    // The open result was closed by the interrupt, and says so.
    let closed = reader.next().unwrap().unwrap_err().to_string();
    assert!(closed.contains("interrupted"), "{closed}");
}
