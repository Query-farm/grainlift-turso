// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Real C-ABI coverage through the native Grainlift ADBC driver, against a
//! local Turso database served over loopback HTTP.
//!
//! Every test skips (passes with a note) unless `GRAINLIFT_DRIVER` names the
//! native driver library; set `GRAINLIFT_REQUIRE_NATIVE` to fail instead.

mod common;

use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{IngestMode, ObjectDepth, OptionConnection, OptionStatement, OptionValue};
use adbc_core::{Connection, Optionable, Statement};
use adbc_driver_manager::ManagedConnection;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Int64Array, RecordBatch, RecordBatchReader, StringArray};
use arrow_schema::{DataType, Field, Schema};
use common::{Server, connect};

fn server() -> (Server, tempfile::TempDir) {
    let (backend, directory) = common::local_backend();
    (
        Server::start(backend, &[("test-token", "alice")], None),
        directory,
    )
}

fn query(connection: &mut ManagedConnection, sql: &str) -> Result<Vec<RecordBatch>> {
    let mut statement = connection.new_statement()?;
    statement.set_sql_query(sql)?;
    statement
        .execute()?
        .map(|batch| batch.map_err(Error::from))
        .collect()
}

fn update(connection: &mut ManagedConnection, sql: &str) -> Result<Option<i64>> {
    let mut statement = connection.new_statement()?;
    statement.set_sql_query(sql)?;
    statement.execute_update()
}

fn count(connection: &mut ManagedConnection, table: &str) -> i64 {
    let batches = query(connection, &format!("SELECT count(*) FROM {table}")).unwrap();
    batches[0].column(0).as_primitive::<Int64Type>().value(0)
}

#[test]
fn queries_updates_and_errors() {
    let _driver = require_driver!();
    let (server, _directory) = server();
    let mut connection = connect(&server.url, Some("test-token")).unwrap();

    update(
        &mut connection,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .unwrap();
    assert_eq!(
        update(&mut connection, "INSERT INTO t VALUES (1, 'a'), (2, 'b')").unwrap(),
        Some(2)
    );
    let batches = query(&mut connection, "SELECT id, name FROM t ORDER BY id").unwrap();
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(batches[0].column(1).as_string::<i32>().value(1), "b");

    let batches = query(
        &mut connection,
        "WITH RECURSIVE n(x) AS (SELECT 0 UNION ALL SELECT x + 1 FROM n WHERE x < 2499) SELECT x FROM n",
    )
    .unwrap();
    let sizes = batches
        .iter()
        .map(RecordBatch::num_rows)
        .collect::<Vec<_>>();
    assert_eq!(sizes, [1024, 1024, 452]);

    let error = query(&mut connection, "SELECT * FROM missing").unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("missing"), "{}", error.message);
    let error = update(&mut connection, "INSERT INTO t VALUES (1, 'dup')").unwrap_err();
    assert_eq!(error.status, Status::Integrity);

    drop(connection);
    assert_eq!(server.open_handles(), (0, 0));
}

#[test]
fn prepared_statements_with_parameters() {
    let _driver = require_driver!();
    let (server, _directory) = server();
    let mut connection = connect(&server.url, Some("test-token")).unwrap();
    update(&mut connection, "CREATE TABLE t (id INTEGER, name TEXT)").unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let rows = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(0..5000)),
            Arc::new(StringArray::from_iter_values(
                (0..5000).map(|n| format!("name {n}")),
            )),
        ],
    )
    .unwrap();
    let mut insert = connection.new_statement().unwrap();
    insert.set_sql_query("INSERT INTO t VALUES (?, ?)").unwrap();
    insert.prepare().unwrap();
    assert_eq!(insert.get_parameter_schema().unwrap().fields().len(), 2);
    insert.bind(rows).unwrap();
    assert_eq!(insert.execute_update().unwrap(), Some(5000));

    let mut select = connection.new_statement().unwrap();
    select
        .set_sql_query("SELECT name FROM t WHERE id = ?")
        .unwrap();
    select.prepare().unwrap();
    let id = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    select
        .bind(RecordBatch::try_new(id, vec![Arc::new(Int64Array::from(vec![4321]))]).unwrap())
        .unwrap();
    let schema = select.execute_schema().unwrap();
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    let batches = select
        .execute()
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        batches[0].column(0).as_string::<i32>().value(0),
        "name 4321"
    );
}

#[test]
fn transactions() {
    let _driver = require_driver!();
    let (server, _directory) = server();
    let mut writer = connect(&server.url, Some("test-token")).unwrap();
    let mut reader = connect(&server.url, Some("test-token")).unwrap();
    update(&mut writer, "CREATE TABLE t (id INTEGER)").unwrap();

    writer
        .set_option(OptionConnection::AutoCommit, OptionValue::from("false"))
        .unwrap();
    update(&mut writer, "INSERT INTO t VALUES (1)").unwrap();
    assert_eq!(count(&mut reader, "t"), 0);
    writer.rollback().unwrap();
    update(&mut writer, "INSERT INTO t VALUES (2)").unwrap();
    writer.commit().unwrap();
    assert_eq!(count(&mut reader, "t"), 1);
}

#[test]
fn bulk_ingestion_and_metadata() {
    let _driver = require_driver!();
    let (server, _directory) = server();
    let mut connection = connect(&server.url, Some("test-token")).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("city", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Oslo", "Lima", "Pune"])),
        ],
    )
    .unwrap();
    let mut ingest = connection.new_statement().unwrap();
    ingest
        .set_option(OptionStatement::TargetTable, OptionValue::from("cities"))
        .unwrap();
    ingest
        .set_option(OptionStatement::IngestMode, IngestMode::Create.into())
        .unwrap();
    ingest.bind(batch).unwrap();
    assert_eq!(ingest.execute_update().unwrap(), Some(3));
    assert_eq!(count(&mut connection, "cities"), 3);

    let table = connection.get_table_schema(None, None, "cities").unwrap();
    assert_eq!(table.field(1).data_type(), &DataType::Utf8);
    let objects = connection
        .get_objects(ObjectDepth::All, None, None, None, None, None)
        .unwrap();
    let batches = objects.collect::<std::result::Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches[0].column(0).as_string::<i32>().value(0), "main");
    let types = connection.get_table_types().unwrap();
    assert_eq!(types.schema().field(0).name(), "table_type");
    let info = connection.get_info(None).unwrap();
    let info = info.collect::<std::result::Result<Vec<_>, _>>().unwrap();
    assert!(info.iter().map(RecordBatch::num_rows).sum::<usize>() >= 4);
}

#[test]
fn authentication_required() {
    let _driver = require_driver!();
    let (server, _directory) = server();
    let error = connect(&server.url, Some("wrong-token"))
        .and_then(|mut connection| query(&mut connection, "SELECT 1"))
        .unwrap_err();
    assert_eq!(error.status, Status::Unauthenticated);
}
