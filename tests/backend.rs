// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! The backend against a local Turso database, called directly (no driver).

mod common;

use std::sync::Arc;

use adbc_core::error::Status;
use adbc_core::options::{ObjectDepth, OptionValue};
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use grainlift_server::backend::{Backend, BackendConnection};
use grainlift_server::config::TargetConfig;

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

fn connect(backend: &impl Backend) -> Box<dyn BackendConnection> {
    backend.open(&target(), Vec::new(), Vec::new()).unwrap()
}

fn query(
    connection: &mut dyn BackendConnection,
    sql: &str,
) -> adbc_core::error::Result<Vec<RecordBatch>> {
    let mut statement = connection.new_statement()?;
    statement.set_sql_query(sql)?;
    let reader = statement.execute_result()?.into_reader();
    reader
        .map(|batch| {
            batch.map_err(|error| {
                adbc_core::error::Error::with_message_and_status(
                    error.to_string(),
                    Status::Internal,
                )
            })
        })
        .collect()
}

fn update(connection: &mut dyn BackendConnection, sql: &str) -> i64 {
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query(sql).unwrap();
    statement.execute_update().unwrap().unwrap()
}

fn count(connection: &mut dyn BackendConnection, table: &str) -> i64 {
    let batches = query(connection, &format!("SELECT count(*) AS n FROM {table}")).unwrap();
    batches[0].column(0).as_primitive::<Int64Type>().value(0)
}

#[test]
fn queries_stream_typed_batches() {
    let (backend, _directory) = common::local_backend();
    let mut connection = connect(&backend);
    update(
        &mut *connection,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score REAL, ok BOOLEAN, raw BLOB, loose)",
    );
    update(
        &mut *connection,
        "INSERT INTO t VALUES (1, 'a', 1.5, 1, x'00ff', 7), (2, NULL, 2, 0, NULL, 8)",
    );

    let batches = query(
        &mut *connection,
        "SELECT *, id * 2.5 AS computed FROM t ORDER BY id",
    )
    .unwrap();
    let schema = batches[0].schema();
    let types = schema
        .fields()
        .iter()
        .map(|field| field.data_type().clone())
        .collect::<Vec<_>>();
    assert_eq!(
        types,
        [
            DataType::Int64,
            DataType::Utf8,
            DataType::Float64,
            DataType::Boolean,
            DataType::Binary,
            DataType::Int64, // undeclared: inferred from its values
            DataType::Float64,
        ]
    );
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.column(2).as_primitive::<Float64Type>().value(1), 2.0);
    assert!(batch.column(3).as_boolean().value(0));
    assert_eq!(batch.column(4).as_binary::<i32>().value(0), [0, 255]);
    assert!(batch.column(1).is_null(1));

    // A recursive CTE larger than one batch arrives in bounded batches.
    let batches = query(
        &mut *connection,
        "WITH RECURSIVE n(x) AS (SELECT 0 UNION ALL SELECT x + 1 FROM n WHERE x < 2499) SELECT x FROM n",
    )
    .unwrap();
    let sizes = batches
        .iter()
        .map(RecordBatch::num_rows)
        .collect::<Vec<_>>();
    assert_eq!(sizes, [1024, 1024, 452]);
    let last = batches[2].column(0).as_primitive::<Int64Type>();
    assert_eq!(last.value(last.len() - 1), 2499);

    // An empty result still has its declared schema.
    let empty = query(&mut *connection, "SELECT id, name FROM t WHERE id < 0").unwrap();
    assert!(empty.iter().all(|batch| batch.num_rows() == 0));
}

#[test]
fn values_that_do_not_fit_the_declared_type_are_errors() {
    let (backend, _directory) = common::local_backend();
    let mut connection = connect(&backend);
    update(&mut *connection, "CREATE TABLE t (n INTEGER)");
    update(
        &mut *connection,
        "INSERT INTO t VALUES (1), ('not a number')",
    );
    let error = query(&mut *connection, "SELECT n FROM t").unwrap_err();
    assert!(error.message.contains("CAST"), "{}", error.message);
    // CAST chooses the type.
    let batches = query(&mut *connection, "SELECT CAST(n AS TEXT) AS n FROM t").unwrap();
    assert_eq!(
        batches[0].column(0).as_string::<i32>().value(1),
        "not a number"
    );
}

#[test]
fn errors_carry_status() {
    let (backend, _directory) = common::local_backend();
    let mut connection = connect(&backend);
    let error = query(&mut *connection, "SELEC 1").unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    let error = query(&mut *connection, "SELECT * FROM missing").unwrap_err();
    assert!(error.message.contains("missing"), "{}", error.message);

    update(&mut *connection, "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    update(&mut *connection, "INSERT INTO t VALUES (1)");
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query("INSERT INTO t VALUES (1)").unwrap();
    assert_eq!(
        statement.execute_update().unwrap_err().status,
        Status::Integrity
    );
}

#[test]
fn binds_parameters() {
    let (backend, _directory) = common::local_backend();
    let mut connection = connect(&backend);
    update(&mut *connection, "CREATE TABLE t (id INTEGER, name TEXT)");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let rows = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ],
    )
    .unwrap();
    let mut insert = connection.new_statement().unwrap();
    insert.set_sql_query("INSERT INTO t VALUES (?, ?)").unwrap();
    assert_eq!(insert.get_parameter_schema().unwrap().fields().len(), 2);
    insert.bind(rows).unwrap();
    assert_eq!(insert.execute_update().unwrap(), Some(3));

    let mut select = connection.new_statement().unwrap();
    select
        .set_sql_query("SELECT name FROM t WHERE id = :id")
        .unwrap();
    let id = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    select
        .bind(RecordBatch::try_new(id, vec![Arc::new(Int64Array::from(vec![3]))]).unwrap())
        .unwrap();
    let batches = select
        .execute_result()
        .unwrap()
        .into_reader()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches[0].column(0).as_string::<i32>().value(0), "c");
}

#[test]
fn failed_multi_row_update_writes_nothing() {
    let (backend, _directory) = common::local_backend();
    let mut connection = connect(&backend);
    update(&mut *connection, "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut insert = connection.new_statement().unwrap();
    insert.set_sql_query("INSERT INTO t VALUES (?)").unwrap();
    insert
        .bind(
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 1]))]).unwrap(),
        )
        .unwrap();
    assert_eq!(
        insert.execute_update().unwrap_err().status,
        Status::Integrity
    );
    assert_eq!(count(&mut *connection, "t"), 0);
}

#[test]
fn transactions_commit_and_roll_back() {
    let (backend, _directory) = common::local_backend();
    let mut writer = connect(&backend);
    let mut reader = connect(&backend);
    update(&mut *writer, "CREATE TABLE t (id INTEGER)");

    writer
        .set_option("adbc.connection.autocommit", OptionValue::from("false"))
        .unwrap();
    assert_eq!(
        writer
            .get_option_string("adbc.connection.autocommit")
            .unwrap(),
        "false"
    );
    update(&mut *writer, "INSERT INTO t VALUES (1)");
    assert_eq!(
        count(&mut *reader, "t"),
        0,
        "uncommitted rows are invisible"
    );
    writer.rollback().unwrap();
    assert_eq!(count(&mut *writer, "t"), 0);

    update(&mut *writer, "INSERT INTO t VALUES (2)");
    writer.commit().unwrap();
    assert_eq!(count(&mut *reader, "t"), 1);

    // Turning autocommit back on commits the open transaction.
    update(&mut *writer, "INSERT INTO t VALUES (3)");
    writer
        .set_option("adbc.connection.autocommit", OptionValue::from("true"))
        .unwrap();
    assert_eq!(count(&mut *reader, "t"), 2);
    assert_eq!(writer.commit().unwrap_err().status, Status::InvalidState);
}

#[test]
fn ingests_arrow_data() {
    let (backend, _directory) = common::local_backend();
    let mut connection = connect(&backend);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("day", DataType::Date32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow_array::Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(arrow_array::Date32Array::from(vec![20_362, 20_363])),
        ],
    )
    .unwrap();
    let ingest = |connection: &mut dyn BackendConnection, mode: &str| {
        let mut statement = connection.new_statement().unwrap();
        statement
            .set_option("adbc.ingest.target_table", OptionValue::from("people"))
            .unwrap();
        statement
            .set_option("adbc.ingest.mode", OptionValue::from(mode))
            .unwrap();
        statement.bind(batch.clone()).unwrap();
        statement.execute_update()
    };
    assert_eq!(
        ingest(&mut *connection, "adbc.ingest.mode.create").unwrap(),
        Some(2)
    );
    assert_eq!(
        ingest(&mut *connection, "adbc.ingest.mode.create")
            .unwrap_err()
            .status,
        Status::InvalidArguments,
        "create fails when the table exists"
    );
    ingest(&mut *connection, "adbc.ingest.mode.append").unwrap();
    assert_eq!(count(&mut *connection, "people"), 4);
    ingest(&mut *connection, "adbc.ingest.mode.replace").unwrap();
    assert_eq!(count(&mut *connection, "people"), 2);

    let schema = connection.get_table_schema(None, None, "people").unwrap();
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
    let batches = query(&mut *connection, "SELECT day FROM people ORDER BY id").unwrap();
    assert_eq!(
        batches[0].column(0).as_string::<i32>().value(0),
        "2025-10-01"
    );
}

#[test]
fn describes_the_catalog() {
    let (backend, _directory) = common::local_backend();
    let mut connection = connect(&backend);
    update(
        &mut *connection,
        "CREATE TABLE cities (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
    );
    update(
        &mut *connection,
        "CREATE VIEW names AS SELECT name FROM cities",
    );

    let objects = connection
        .get_objects(ObjectDepth::All, None, None, Some("cit%"), None, None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(objects[0].num_rows(), 1);
    let json = String::from_utf8(arrow_json_writer(&objects[0])).unwrap();
    assert!(json.contains("\"table_name\":\"cities\""), "{json}");
    assert!(json.contains("PRIMARY KEY"), "{json}");
    assert!(!json.contains("\"names\""), "{json}");

    let schema = connection
        .get_table_schema(Some("main"), None, "cities")
        .unwrap();
    assert!(!schema.field(1).is_nullable());
    assert_eq!(
        connection
            .get_table_schema(None, None, "nope")
            .unwrap_err()
            .status,
        Status::NotFound
    );
    assert_eq!(
        connection
            .get_table_types()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .num_rows(),
        2
    );
    assert_eq!(
        connection
            .get_info(None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .num_rows(),
        5
    );
}

#[test]
fn execute_schema_uses_declarations_and_refuses_unknown_writes() {
    let (backend, _directory) = common::local_backend();
    let mut connection = connect(&backend);
    update(&mut *connection, "CREATE TABLE t (id INTEGER, name TEXT)");
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query("SELECT id, name FROM t").unwrap();
    let schema = statement.execute_schema().unwrap();
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

    statement.set_sql_query("SELECT 1.5 AS x").unwrap();
    assert_eq!(
        statement.execute_schema().unwrap().field(0).data_type(),
        &DataType::Float64
    );

    statement
        .set_sql_query("INSERT INTO t VALUES (1, 'a') RETURNING id + 1")
        .unwrap();
    assert_eq!(
        statement.execute_schema().unwrap_err().status,
        Status::InvalidState
    );
    assert_eq!(
        count(&mut *connection, "t"),
        0,
        "execute_schema did not write"
    );
}

fn arrow_json_writer(batch: &RecordBatch) -> Vec<u8> {
    let mut writer = arrow_json::LineDelimitedWriter::new(Vec::new());
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    writer.into_inner()
}

#[test]
fn local_databases_refuse_client_database_options() {
    let (backend, _directory) = common::local_backend();
    let open = |key: &str| {
        backend
            .open(
                &target(),
                vec![(key.into(), OptionValue::from("secret-value"))],
                Vec::new(),
            )
            .err()
            .unwrap()
    };
    let error = open("turso.auth_token");
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(!error.message.contains("secret-value"));
    assert_eq!(open("uri").status, Status::InvalidArguments);
}

#[test]
fn a_new_query_forgets_earlier_parameters() {
    let (backend, _directory) = common::local_backend();
    let mut connection = connect(&backend);
    update(&mut *connection, "CREATE TABLE t (id INTEGER)");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query("INSERT INTO t VALUES (?)").unwrap();
    statement
        .bind(RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap())
        .unwrap();
    assert_eq!(statement.execute_update().unwrap(), Some(2));
    // The same statement, reused for a query without parameters.
    statement.set_sql_query("SELECT count(*) FROM t").unwrap();
    let batches = statement
        .execute_result()
        .unwrap()
        .into_reader()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches[0].column(0).as_primitive::<Int64Type>().value(0), 2);
}

#[test]
fn read_only_files_refuse_writes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ro.db");
    let path = path.to_str().unwrap();
    {
        let writable = grainlift_turso::TursoBackend::open(&grainlift_turso::Location::parse(
            path, None, false,
        ))
        .unwrap();
        let mut connection = connect(&writable);
        update(&mut *connection, "CREATE TABLE t (id INTEGER)");
        update(&mut *connection, "INSERT INTO t VALUES (1)");
    }
    let read_only =
        grainlift_turso::TursoBackend::open(&grainlift_turso::Location::parse(path, None, true))
            .unwrap();
    let mut connection = connect(&read_only);
    assert_eq!(count(&mut *connection, "t"), 1);
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query("INSERT INTO t VALUES (2)").unwrap();
    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::Unauthorized, "{}", error.message);
}

#[test]
fn ingests_into_temporary_tables() {
    let (backend, _directory) = common::local_backend();
    let mut connection = connect(&backend);
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut statement = connection.new_statement().unwrap();
    statement
        .set_option("adbc.ingest.target_table", OptionValue::from("scratch"))
        .unwrap();
    statement
        .set_option("adbc.ingest.temporary", OptionValue::from("true"))
        .unwrap();
    statement
        .bind(
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap(),
        )
        .unwrap();
    assert_eq!(statement.execute_update().unwrap(), Some(3));
    assert_eq!(count(&mut *connection, "temp.scratch"), 3);
    assert!(
        query(&mut *connection, "SELECT * FROM main.scratch").is_err(),
        "the table is temporary, not in main"
    );
}
