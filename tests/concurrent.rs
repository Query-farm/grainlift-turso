// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Concurrent transactions on a Turso Cloud database on the Turso Database
//! engine (`turso db create --tursodb`).
//!
//! Skips unless `TURSO_TEST_TURSODB_URL` and `TURSO_TEST_TURSODB_AUTH_TOKEN`
//! name such a database. On this engine any schema change aborts other
//! connections' concurrent transactions, so the scenarios run one after
//! another, in their own test binary, away from the other Cloud tests.

use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::OptionValue;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use grainlift_server::backend::{Backend, BackendConnection};
use grainlift_server::config::TargetConfig;
use grainlift_turso::{DEV_TARGET, Location, TargetSpec, TransactionMode, TursoBackend};

fn setting(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn unique() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos())
        .unwrap_or_default();
    format!("{}_{nanos}", std::process::id())
}

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

fn execute(connection: &mut Box<dyn BackendConnection>, sql: &str) -> Result<Option<i64>> {
    let mut statement = connection.new_statement()?;
    statement.set_sql_query(sql)?;
    statement.execute_update()
}

fn scalar_text(connection: &mut Box<dyn BackendConnection>, sql: &str) -> String {
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query(sql).unwrap();
    let batches = statement
        .execute_result()
        .unwrap()
        .into_reader()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    batches[0].column(0).as_string::<i32>().value(0).to_string()
}

fn count(connection: &mut Box<dyn BackendConnection>, sql: &str) -> i64 {
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query(sql).unwrap();
    let batches = statement
        .execute_result()
        .unwrap()
        .into_reader()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    batches[0].column(0).as_primitive::<Int64Type>().value(0)
}

fn manual(backend: &TursoBackend) -> Box<dyn BackendConnection> {
    let mut connection = backend.open(&target(), Vec::new(), Vec::new()).unwrap();
    connection
        .set_option("adbc.connection.autocommit", OptionValue::from("false"))
        .unwrap();
    connection
}

fn sqlstate(error: &Error) -> String {
    error
        .sqlstate
        .iter()
        .map(|byte| *byte as u8 as char)
        .collect()
}

#[test]
fn concurrent_transactions() {
    let (Some(url), Some(token)) = (
        setting("TURSO_TEST_TURSODB_URL"),
        setting("TURSO_TEST_TURSODB_AUTH_TOKEN"),
    ) else {
        eprintln!("skipped: TURSO_TEST_TURSODB_URL and TURSO_TEST_TURSODB_AUTH_TOKEN are not set");
        return;
    };
    let spec = TargetSpec {
        transactions: TransactionMode::Concurrent,
        ..TargetSpec::new(Location::parse(&url, Some(token), false))
    };
    let backend = TursoBackend::new([(DEV_TARGET.to_string(), spec)]).unwrap();
    let table = format!("grainlift_turso_concurrent_{}", unique());
    let mut setup = backend.open(&target(), Vec::new(), Vec::new()).unwrap();
    execute(
        &mut setup,
        &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, v TEXT)"),
    )
    .unwrap();
    execute(
        &mut setup,
        &format!("INSERT INTO {table} VALUES (1, 'a'), (2, 'b')"),
    )
    .unwrap();

    // Writers on different rows both commit.
    let (mut first, mut second) = (manual(&backend), manual(&backend));
    execute(
        &mut first,
        &format!("UPDATE {table} SET v = 'first' WHERE id = 1"),
    )
    .unwrap();
    execute(
        &mut second,
        &format!("UPDATE {table} SET v = 'second' WHERE id = 2"),
    )
    .unwrap();
    first.commit().unwrap();
    second.commit().unwrap();
    assert_eq!(
        scalar_text(&mut setup, &format!("SELECT v FROM {table} WHERE id = 2")),
        "second"
    );

    // Writers on the same row: the second loses with a retryable conflict,
    // its commit fails rather than reporting success, and a retry works.
    execute(
        &mut first,
        &format!("UPDATE {table} SET v = 'winner' WHERE id = 1"),
    )
    .unwrap();
    let conflict = execute(
        &mut second,
        &format!("UPDATE {table} SET v = 'loser' WHERE id = 1"),
    )
    .unwrap_err();
    assert_eq!(sqlstate(&conflict), "40001", "{}", conflict.message);
    assert_eq!(conflict.status, Status::InvalidState);
    first.commit().unwrap();
    let commit = second.commit().unwrap_err();
    assert_eq!(sqlstate(&commit), "40001", "{}", commit.message);
    assert!(commit.message.contains("rolled back"), "{}", commit.message);
    execute(
        &mut second,
        &format!("UPDATE {table} SET v = 'retried' WHERE id = 1"),
    )
    .unwrap();
    second.commit().unwrap();
    assert_eq!(
        scalar_text(&mut setup, &format!("SELECT v FROM {table} WHERE id = 1")),
        "retried"
    );

    // Atomic multi-row writes use BEGIN CONCURRENT too.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Utf8, false),
    ]));
    let rows = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(3..503)),
            Arc::new(StringArray::from_iter_values(
                (3..503).map(|n| format!("r{n}")),
            )),
        ],
    )
    .unwrap();
    let mut insert = setup.new_statement().unwrap();
    insert
        .set_sql_query(&format!("INSERT INTO {table} VALUES (?, ?)"))
        .unwrap();
    insert.bind(rows).unwrap();
    assert_eq!(insert.execute_update().unwrap(), Some(500));
    assert_eq!(
        count(&mut setup, &format!("SELECT count(*) FROM {table}")),
        502
    );

    // A schema change aborts another connection's concurrent transaction,
    // also as a retryable error.
    execute(
        &mut first,
        &format!("UPDATE {table} SET v = 'pending' WHERE id = 2"),
    )
    .unwrap();
    let other = format!("grainlift_turso_other_{}", unique());
    execute(&mut setup, &format!("CREATE TABLE {other} (x)")).unwrap();
    let aborted = first.commit().unwrap_err();
    assert_eq!(sqlstate(&aborted), "40001", "{}", aborted.message);
    first.rollback().unwrap();

    execute(&mut setup, &format!("DROP TABLE {other}")).unwrap();
    execute(&mut setup, &format!("DROP TABLE {table}")).unwrap();
}
