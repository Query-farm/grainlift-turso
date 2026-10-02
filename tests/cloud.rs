// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! The backend against a real Turso Cloud database.
//!
//! Skips unless `TURSO_TEST_DATABASE_URL` (and usually `TURSO_TEST_AUTH_TOKEN`)
//! name a database the test may write to. It creates and drops one table.

mod common;

use std::sync::Arc;

use adbc_core::options::OptionValue;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use grainlift_server::backend::{Backend, BackendConnection};
use grainlift_server::config::TargetConfig;
use grainlift_turso::{Location, TursoBackend};

/// A test setting from the environment. CI passes unset secrets as empty
/// strings, which count as unset.
fn setting(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

#[test]
fn turso_cloud_round_trip() {
    let Some(url) = setting("TURSO_TEST_DATABASE_URL") else {
        eprintln!("skipped: TURSO_TEST_DATABASE_URL is not set");
        return;
    };
    let location = Location::parse(&url, setting("TURSO_TEST_AUTH_TOKEN"), false);
    assert!(
        location.is_remote(),
        "TURSO_TEST_DATABASE_URL must be a Turso Cloud URL"
    );
    let backend = TursoBackend::open(&location).unwrap();
    let target = target();
    let mut connection = backend.open(&target, Vec::new(), Vec::new()).unwrap();
    let table = format!("grainlift_turso_test_{}", std::process::id());

    let run = |connection: &mut Box<dyn BackendConnection>, sql: &str| {
        let mut statement = connection.new_statement().unwrap();
        statement.set_sql_query(sql).unwrap();
        statement.execute_update().unwrap()
    };
    let read = |connection: &mut Box<dyn BackendConnection>, sql: &str| {
        let mut statement = connection.new_statement().unwrap();
        statement.set_sql_query(sql).unwrap();
        statement
            .execute_result()
            .unwrap()
            .into_reader()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };

    run(
        &mut connection,
        &format!("CREATE TABLE {table} (id INTEGER, name TEXT)"),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let rows = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(0..600)),
            Arc::new(StringArray::from_iter_values(
                (0..600).map(|n| format!("n{n}")),
            )),
        ],
    )
    .unwrap();
    let mut insert = connection.new_statement().unwrap();
    insert
        .set_sql_query(&format!("INSERT INTO {table} VALUES (?, ?)"))
        .unwrap();
    insert.bind(rows).unwrap();
    assert_eq!(insert.execute_update().unwrap(), Some(600));

    connection
        .set_option("adbc.connection.autocommit", OptionValue::from("false"))
        .unwrap();
    run(&mut connection, &format!("DELETE FROM {table}"));
    connection.rollback().unwrap();
    connection
        .set_option("adbc.connection.autocommit", OptionValue::from("true"))
        .unwrap();

    let batches = read(
        &mut connection,
        &format!("SELECT count(*), max(name) FROM {table}"),
    );
    assert_eq!(
        batches[0].column(0).as_primitive::<Int64Type>().value(0),
        600
    );
    assert!(connection.get_table_schema(None, None, &table).is_ok());
    run(&mut connection, &format!("DROP TABLE {table}"));
}

fn target() -> TargetConfig {
    TargetConfig {
        driver: "grainlift-turso".into(),
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

/// Clients that send their own token through the native driver, to a server
/// that holds none.
#[test]
fn client_supplied_tokens() {
    use adbc_core::error::Status;
    use adbc_core::{Connection, Statement};

    let (Some(url), Some(token), Some(read_only)) = (
        setting("TURSO_TEST_DATABASE_URL"),
        setting("TURSO_TEST_AUTH_TOKEN"),
        setting("TURSO_TEST_READ_ONLY_TOKEN"),
    ) else {
        eprintln!(
            "skipped: TURSO_TEST_DATABASE_URL, _AUTH_TOKEN and _READ_ONLY_TOKEN are not all set"
        );
        return;
    };
    if common::driver_path().is_none() {
        common::skip("GRAINLIFT_DRIVER does not name the native driver library");
        return;
    }
    let backend = TursoBackend::open(&Location::parse(&url, None, false)).unwrap();
    assert!(backend.requires_client_token());
    let server = common::Server::start(backend, &[("test-token", "alice")], None);
    let table = format!("grainlift_turso_client_{}", std::process::id());
    let run = |turso_token: Option<&str>, sql: &str| {
        let extra = turso_token
            .map(|token| vec![("turso.auth_token", token)])
            .unwrap_or_default();
        let mut connection = common::connect_with(&server.url, Some("test-token"), &extra)?;
        let mut statement = connection.new_statement()?;
        statement.set_sql_query(sql)?;
        statement.execute_update()
    };

    run(Some(&token), &format!("CREATE TABLE {table} (id INTEGER)")).unwrap();
    run(Some(&token), &format!("INSERT INTO {table} VALUES (1)")).unwrap();

    // A read-only token reads, and Turso refuses its writes.
    let mut reader = common::connect_with(
        &server.url,
        Some("test-token"),
        &[("turso.auth_token", &read_only)],
    )
    .unwrap();
    let mut statement = reader.new_statement().unwrap();
    statement
        .set_sql_query(format!("SELECT count(*) FROM {table}"))
        .unwrap();
    let batches = statement
        .execute()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches[0].column(0).as_primitive::<Int64Type>().value(0), 1);
    let refused = run(Some(&read_only), &format!("INSERT INTO {table} VALUES (2)")).unwrap_err();
    assert_eq!(refused.status, Status::Unauthorized, "{}", refused.message);
    assert!(!refused.message.contains(&read_only));

    let missing = run(None, "SELECT 1").unwrap_err();
    assert_eq!(missing.status, Status::Unauthenticated);
    let wrong = run(Some("not-a-turso-token"), "SELECT 1").unwrap_err();
    assert_eq!(wrong.status, Status::Unauthenticated, "{}", wrong.message);
    assert!(!wrong.message.contains("not-a-turso-token"));

    run(Some(&token), &format!("DROP TABLE {table}")).unwrap();
}

/// Ingestion splits large values across statements and requests.
#[test]
fn ingests_large_values() {
    let Some(url) = setting("TURSO_TEST_DATABASE_URL") else {
        eprintln!("skipped: TURSO_TEST_DATABASE_URL is not set");
        return;
    };
    let location = Location::parse(&url, setting("TURSO_TEST_AUTH_TOKEN"), false);
    let backend = TursoBackend::open(&location).unwrap();
    let mut connection = backend.open(&target(), Vec::new(), Vec::new()).unwrap();
    let table = format!("grainlift_turso_large_{}", std::process::id());
    // 40 rows of 256 KiB: 10 MiB, more than one request carries.
    let text = "x".repeat(256 * 1024);
    let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, false)]));
    let rows = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from_iter_values(
            (0..40).map(|_| text.as_str()),
        ))],
    )
    .unwrap();
    let mut ingest = connection.new_statement().unwrap();
    ingest
        .set_option(
            "adbc.ingest.target_table",
            OptionValue::from(table.as_str()),
        )
        .unwrap();
    ingest.bind(rows).unwrap();
    assert_eq!(ingest.execute_update().unwrap(), Some(40));

    let mut statement = connection.new_statement().unwrap();
    statement
        .set_sql_query(&format!("SELECT count(*), sum(length(body)) FROM {table}"))
        .unwrap();
    let batches = statement
        .execute_result()
        .unwrap()
        .into_reader()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        batches[0].column(0).as_primitive::<Int64Type>().value(0),
        40
    );
    assert_eq!(
        batches[0].column(1).as_primitive::<Int64Type>().value(0),
        40 * 256 * 1024
    );
    let mut drop = connection.new_statement().unwrap();
    drop.set_sql_query(&format!("DROP TABLE {table}")).unwrap();
    drop.execute_update().unwrap();
}

/// Queries outside a transaction stream from the cursor endpoint; queries in a
/// transaction run on the connection's stream and see its uncommitted rows.
#[test]
fn streamed_and_transactional_queries() {
    use adbc_core::error::Status;

    let Some(url) = setting("TURSO_TEST_DATABASE_URL") else {
        eprintln!("skipped: TURSO_TEST_DATABASE_URL is not set");
        return;
    };
    let location = Location::parse(&url, setting("TURSO_TEST_AUTH_TOKEN"), false);
    let backend = TursoBackend::open(&location).unwrap();
    let mut connection = backend.open(&target(), Vec::new(), Vec::new()).unwrap();
    let read = |connection: &mut Box<dyn BackendConnection>, sql: &str| {
        let mut statement = connection.new_statement()?;
        statement.set_sql_query(sql)?;
        statement
            .execute_result()?
            .into_reader()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                adbc_core::error::Error::with_message_and_status(error.to_string(), Status::Unknown)
            })
    };

    let batches = read(
        &mut connection,
        "WITH RECURSIVE n(x) AS (SELECT 0 UNION ALL SELECT x + 1 FROM n WHERE x < 2499) SELECT x FROM n",
    )
    .unwrap();
    let sizes = batches
        .iter()
        .map(RecordBatch::num_rows)
        .collect::<Vec<_>>();
    assert_eq!(sizes, [1024, 1024, 452]);
    assert_eq!(
        read(&mut connection, "SELEC 1").unwrap_err().status,
        Status::InvalidArguments
    );
    let empty = read(&mut connection, "SELECT 1 AS x WHERE 0").unwrap();
    assert!(empty.iter().all(|batch| batch.num_rows() == 0));

    let table = format!("grainlift_turso_stream_{}", std::process::id());
    let mut statement = connection.new_statement().unwrap();
    statement
        .set_sql_query(&format!("CREATE TABLE {table} (id INTEGER)"))
        .unwrap();
    statement.execute_update().unwrap();
    connection
        .set_option("adbc.connection.autocommit", OptionValue::from("false"))
        .unwrap();
    let mut insert = connection.new_statement().unwrap();
    insert
        .set_sql_query(&format!("INSERT INTO {table} VALUES (1), (2)"))
        .unwrap();
    insert.execute_update().unwrap();
    let batches = read(&mut connection, &format!("SELECT count(*) FROM {table}")).unwrap();
    assert_eq!(batches[0].column(0).as_primitive::<Int64Type>().value(0), 2);
    connection.rollback().unwrap();
    connection
        .set_option("adbc.connection.autocommit", OptionValue::from("true"))
        .unwrap();
    let batches = read(&mut connection, &format!("SELECT count(*) FROM {table}")).unwrap();
    assert_eq!(batches[0].column(0).as_primitive::<Int64Type>().value(0), 0);
    let mut drop = connection.new_statement().unwrap();
    drop.set_sql_query(&format!("DROP TABLE {table}")).unwrap();
    drop.execute_update().unwrap();
}
