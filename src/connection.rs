// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! ADBC connections: statements, transactions and catalog metadata.
//!
//! Metadata is laid out like the ADBC SQLite driver's: one catalog, `main`,
//! holding one unnamed schema, so DuckDB's `ATTACH` browses a Turso database
//! the way it browses SQLite.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionValue};
use adbc_core::schemas::{GET_INFO_SCHEMA, GET_OBJECTS_SCHEMA, GET_TABLE_TYPES_SCHEMA};
use arrow_array::{
    Array, ArrayRef, Int64Array, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray,
    UInt32Array, UnionArray, new_empty_array,
};
use arrow_schema::{DataType, Field, Schema};
use grainlift_server::backend::{BackendConnection, BackendStatement};
use serde_json::{Value as Json, json};

use crate::db::{Value, error};
use crate::session::Session;
use crate::statement::TursoStatement;
use crate::types;

const AUTOCOMMIT: &str = "adbc.connection.autocommit";
const CATALOG: &str = "main";
/// Turso has no schemas; like the ADBC SQLite driver, report one unnamed one.
const DB_SCHEMA: &str = "";

type Reader = Box<dyn RecordBatchReader + Send + 'static>;

/// One ADBC connection: a Turso connection and its statements.
pub struct TursoConnection {
    session: Arc<Session>,
}

impl TursoConnection {
    pub(crate) fn new(session: Session) -> Self {
        Self {
            session: Arc::new(session),
        }
    }

    /// Run a metadata query and return its rows.
    fn rows(&self, sql: String) -> Result<Vec<Vec<Value>>> {
        self.session.run(move |connection| async move {
            let mut rows = connection.query(sql, Vec::new()).await?;
            Ok(rows.next_rows(usize::MAX, usize::MAX).await?.0)
        })
    }

    /// User tables and views, as `(name, type)`, ordered by name.
    fn tables(&self) -> Result<Vec<(String, String)>> {
        let rows = self.rows(
            "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY name"
                .into(),
        )?;
        Ok(rows
            .iter()
            .map(|row| (text(&row[0]), text(&row[1])))
            .filter(|(name, _)| !is_internal(name))
            .collect())
    }

    fn table_info(&self, table: &str) -> Result<Vec<ColumnInfo>> {
        let rows = self.rows(format!("PRAGMA table_info({})", quote(table)))?;
        Ok(rows
            .iter()
            .map(|row| ColumnInfo {
                position: integer(&row[0]) + 1,
                name: text(&row[1]),
                declared: text(&row[2]),
                not_null: integer(&row[3]) != 0,
                default: (!matches!(row[4], Value::Null)).then(|| text(&row[4])),
                primary_key: integer(&row[5]),
            })
            .collect())
    }

    fn constraints(&self, table: &str, columns: &[ColumnInfo]) -> Result<Vec<Json>> {
        let mut constraints = Vec::new();
        let mut key = columns
            .iter()
            .filter(|column| column.primary_key > 0)
            .collect::<Vec<_>>();
        key.sort_by_key(|column| column.primary_key);
        if !key.is_empty() {
            constraints.push(json!({
                "constraint_name": null,
                "constraint_type": "PRIMARY KEY",
                "constraint_column_names": key.iter().map(|column| &column.name).collect::<Vec<_>>(),
                "constraint_column_usage": [],
            }));
        }
        // Columns: id, seq, table, from, to, on_update, on_delete, match.
        let rows = self.rows(format!("PRAGMA foreign_key_list({})", quote(table)))?;
        let mut keys: BTreeMap<i64, Vec<&Vec<Value>>> = BTreeMap::new();
        for row in &rows {
            keys.entry(integer(&row[0])).or_default().push(row);
        }
        for mut rows in keys.into_values() {
            rows.sort_by_key(|row| integer(&row[1]));
            constraints.push(json!({
                "constraint_name": null,
                "constraint_type": "FOREIGN KEY",
                "constraint_column_names": rows.iter().map(|row| text(&row[3])).collect::<Vec<_>>(),
                "constraint_column_usage": rows.iter().map(|row| json!({
                    "fk_catalog": CATALOG,
                    "fk_db_schema": DB_SCHEMA,
                    "fk_table": text(&row[2]),
                    // A key that names no column references the primary key.
                    "fk_column_name": if matches!(row[4], Value::Null) { text(&row[3]) } else { text(&row[4]) },
                })).collect::<Vec<_>>(),
            }));
        }
        Ok(constraints)
    }

    fn vendor_version(&self) -> Result<String> {
        let rows = self.rows("SELECT sqlite_version()".into())?;
        Ok(rows.first().map(|row| text(&row[0])).unwrap_or_default())
    }
}

/// Engine-internal tables, which clients should not see.
fn is_internal(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.starts_with("sqlite_") || name.starts_with("__turso")
}

struct ColumnInfo {
    position: i64,
    name: String,
    declared: String,
    not_null: bool,
    default: Option<String>,
    primary_key: i64,
}

fn text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Integer(value) => value.to_string(),
        Value::Real(value) => value.to_string(),
        Value::Text(value) => value.clone(),
        Value::Blob(value) => String::from_utf8_lossy(value).into_owned(),
    }
}

fn integer(value: &Value) -> i64 {
    match value {
        Value::Integer(value) => *value,
        Value::Text(value) => value.parse().unwrap_or_default(),
        _ => 0,
    }
}

fn quote(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// ADBC metadata filters are SQL `LIKE` patterns (`%` and `_`, ASCII case
/// insensitive); `None` matches everything.
fn like(pattern: Option<&str>, value: &str) -> bool {
    fn matches(pattern: &[u8], value: &[u8]) -> bool {
        match pattern.split_first() {
            None => value.is_empty(),
            Some((b'%', rest)) => (0..=value.len()).any(|skip| matches(rest, &value[skip..])),
            Some((b'_', rest)) => !value.is_empty() && matches(rest, &value[1..]),
            Some((byte, rest)) => {
                value
                    .first()
                    .is_some_and(|first| first.eq_ignore_ascii_case(byte))
                    && matches(rest, &value[1..])
            }
        }
    }
    pattern.is_none_or(|pattern| matches(pattern.as_bytes(), value.as_bytes()))
}

fn check_location(catalog: Option<&str>, db_schema: Option<&str>) -> Result<()> {
    let catalog_ok = catalog.is_none_or(|catalog| catalog == CATALOG);
    let schema_ok = db_schema.is_none_or(|schema| schema == DB_SCHEMA || schema == CATALOG);
    if catalog_ok && schema_ok {
        Ok(())
    } else {
        Err(error(
            "Turso has one catalog, main, and no schemas",
            Status::NotFound,
            b"3F000",
        ))
    }
}

fn reader(batch: RecordBatch) -> Reader {
    let schema = batch.schema();
    Box::new(RecordBatchIterator::new([Ok(batch)], schema))
}

fn invalid_option(key: &str) -> Error {
    Error::with_message_and_status(
        format!("{key} must be \"true\" or \"false\""),
        Status::InvalidArguments,
    )
}

impl BackendConnection for TursoConnection {
    fn new_statement(&mut self) -> Result<Box<dyn BackendStatement>> {
        Ok(Box::new(TursoStatement::new(Arc::clone(&self.session))))
    }

    /// Only autocommit can be set. Turning it off makes the next statement
    /// open a transaction; turning it back on commits any open one.
    fn set_option(&mut self, key: &str, value: OptionValue) -> Result<()> {
        if key != AUTOCOMMIT {
            return Err(Error::with_message_and_status(
                format!("Connection option {key} is not supported"),
                Status::NotImplemented,
            ));
        }
        let autocommit = match &value {
            OptionValue::String(value) if value == "true" => true,
            OptionValue::String(value) if value == "false" => false,
            _ => return Err(invalid_option(key)),
        };
        if autocommit && self.session.is_manual() {
            self.session.end_transaction("COMMIT")?;
        }
        self.session.set_manual(!autocommit);
        Ok(())
    }

    fn get_option_string(&self, key: &str) -> Result<String> {
        if key == AUTOCOMMIT {
            return Ok((!self.session.is_manual()).to_string());
        }
        Err(Error::with_message_and_status(
            format!("Connection option {key} is not known"),
            Status::NotFound,
        ))
    }

    fn commit(&mut self) -> Result<()> {
        self.require_manual()?;
        self.session.end_transaction("COMMIT")
    }

    fn rollback(&mut self) -> Result<()> {
        self.require_manual()?;
        self.session.end_transaction("ROLLBACK")
    }

    fn get_table_types(&self) -> Result<Reader> {
        let types = StringArray::from(vec!["table", "view"]);
        Ok(reader(RecordBatch::try_new(
            GET_TABLE_TYPES_SCHEMA.clone(),
            vec![Arc::new(types)],
        )?))
    }

    /// A table's columns, typed as its query results are. Columns declared
    /// without a type are text here; their query results take the type of
    /// their values.
    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        check_location(catalog, db_schema)?;
        let columns = self.table_info(table_name)?;
        if columns.is_empty() {
            return Err(error(
                format!("Table {table_name:?} does not exist"),
                Status::NotFound,
                b"42P01",
            ));
        }
        Ok(Schema::new(
            columns
                .iter()
                .map(|column| {
                    let data_type =
                        types::declared_type(&column.declared).unwrap_or(DataType::Utf8);
                    Field::new(&column.name, data_type, !column.not_null)
                })
                .collect::<Vec<_>>(),
        ))
    }

    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Reader> {
        let mut catalogs = Vec::new();
        if like(catalog, CATALOG) {
            let schemas = match depth {
                ObjectDepth::Catalogs => Json::Null,
                _ if !like(db_schema, DB_SCHEMA) => json!([]),
                ObjectDepth::Schemas => {
                    json!([{ "db_schema_name": DB_SCHEMA, "db_schema_tables": null }])
                }
                ObjectDepth::Tables | ObjectDepth::Columns | ObjectDepth::All => {
                    let with_columns = !matches!(depth, ObjectDepth::Tables);
                    let types = table_type.map(|types| {
                        types
                            .iter()
                            .map(|kind| kind.to_ascii_lowercase())
                            .collect::<HashSet<_>>()
                    });
                    let mut tables = Vec::new();
                    for (name, kind) in self.tables()? {
                        if !like(table_name, &name)
                            || types.as_ref().is_some_and(|types| !types.contains(&kind))
                        {
                            continue;
                        }
                        let (columns, constraints) = if with_columns {
                            let info = self.table_info(&name)?;
                            let constraints = if kind == "table" {
                                Json::from(self.constraints(&name, &info)?)
                            } else {
                                json!([])
                            };
                            let columns = info
                                .iter()
                                .filter(|column| like(column_name, &column.name))
                                .map(column_json)
                                .collect::<Vec<_>>();
                            (Json::from(columns), constraints)
                        } else {
                            (Json::Null, Json::Null)
                        };
                        tables.push(json!({
                            "table_name": name,
                            "table_type": kind,
                            "table_columns": columns,
                            "table_constraints": constraints,
                        }));
                    }
                    json!([{ "db_schema_name": DB_SCHEMA, "db_schema_tables": tables }])
                }
            };
            catalogs.push(json!({ "catalog_name": CATALOG, "catalog_db_schemas": schemas }));
        }
        let mut decoder =
            arrow_json::ReaderBuilder::new(GET_OBJECTS_SCHEMA.clone()).build_decoder()?;
        decoder.serialize(&catalogs)?;
        let batch = decoder
            .flush()?
            .unwrap_or_else(|| RecordBatch::new_empty(GET_OBJECTS_SCHEMA.clone()));
        Ok(reader(batch))
    }

    fn get_info(&self, codes: Option<HashSet<InfoCode>>) -> Result<Reader> {
        let wanted = |code: &InfoCode| codes.as_ref().is_none_or(|codes| codes.contains(code));
        let mut strings = Vec::new();
        if wanted(&InfoCode::VendorName) {
            strings.push((InfoCode::VendorName, "Turso".to_string()));
        }
        if wanted(&InfoCode::VendorVersion) {
            strings.push((InfoCode::VendorVersion, self.vendor_version()?));
        }
        if wanted(&InfoCode::DriverName) {
            strings.push((InfoCode::DriverName, env!("CARGO_PKG_NAME").to_string()));
        }
        if wanted(&InfoCode::DriverVersion) {
            strings.push((
                InfoCode::DriverVersion,
                env!("CARGO_PKG_VERSION").to_string(),
            ));
        }
        // ADBC 1.1.0, encoded as major * 1_000_000 + minor * 1_000.
        let integers = if wanted(&InfoCode::DriverAdbcVersion) {
            vec![(InfoCode::DriverAdbcVersion, 1_001_000_i64)]
        } else {
            Vec::new()
        };
        info_batch(&strings, &integers).map(reader)
    }
}

impl TursoConnection {
    fn require_manual(&self) -> Result<()> {
        if self.session.is_manual() {
            Ok(())
        } else {
            Err(Error::with_message_and_status(
                "Autocommit is on; turn it off to use transactions",
                Status::InvalidState,
            ))
        }
    }
}

impl Drop for TursoConnection {
    fn drop(&mut self) {
        self.session.close();
    }
}

fn column_json(column: &ColumnInfo) -> Json {
    json!({
        "column_name": column.name,
        "ordinal_position": column.position,
        "remarks": null,
        "xdbc_data_type": null,
        "xdbc_type_name": (!column.declared.is_empty()).then_some(&column.declared),
        "xdbc_column_size": null,
        "xdbc_decimal_digits": null,
        "xdbc_num_prec_radix": null,
        "xdbc_nullable": if column.not_null { 0 } else { 1 },
        "xdbc_column_def": column.default,
        "xdbc_sql_data_type": null,
        "xdbc_datetime_sub": null,
        "xdbc_char_octet_length": null,
        "xdbc_is_nullable": if column.not_null { "NO" } else { "YES" },
        "xdbc_scope_catalog": null,
        "xdbc_scope_schema": null,
        "xdbc_scope_table": null,
        "xdbc_is_autoincrement": null,
        "xdbc_is_generatedcolumn": null,
    })
}

/// A GetInfo result with string and int64 values; its other union members
/// stay empty.
fn info_batch(strings: &[(InfoCode, String)], integers: &[(InfoCode, i64)]) -> Result<RecordBatch> {
    const STRING: i8 = 0;
    const INT64: i8 = 2;
    let DataType::Union(fields, _) = GET_INFO_SCHEMA.field(1).data_type() else {
        unreachable!("info_value is a union")
    };
    let names = strings
        .iter()
        .map(|(code, _)| u32::from(code))
        .chain(integers.iter().map(|(code, _)| u32::from(code)))
        .collect::<UInt32Array>();
    let type_ids = (strings.iter().map(|_| STRING))
        .chain(integers.iter().map(|_| INT64))
        .collect::<Vec<_>>();
    let offsets = (0..strings.len() as i32)
        .chain(0..integers.len() as i32)
        .collect::<Vec<_>>();
    let children = fields
        .iter()
        .map(|(type_id, field)| -> ArrayRef {
            match type_id {
                STRING => Arc::new(
                    strings
                        .iter()
                        .map(|(_, value)| Some(value.as_str()))
                        .collect::<StringArray>(),
                ),
                INT64 => Arc::new(
                    integers
                        .iter()
                        .map(|(_, value)| *value)
                        .collect::<Int64Array>(),
                ),
                _ => new_empty_array(field.data_type()),
            }
        })
        .collect::<Vec<_>>();
    let values = UnionArray::try_new(
        fields.clone(),
        type_ids.into(),
        Some(offsets.into()),
        children,
    )?;
    debug_assert_eq!(values.len(), names.len());
    Ok(RecordBatch::try_new(
        GET_INFO_SCHEMA.clone(),
        vec![Arc::new(names), Arc::new(values)],
    )?)
}

#[cfg(test)]
mod tests {
    use super::{info_batch, like};
    use adbc_core::options::InfoCode;

    #[test]
    fn like_patterns() {
        assert!(like(None, "anything"));
        assert!(like(Some("ci%"), "Cities"));
        assert!(like(Some("c_ty"), "city"));
        assert!(!like(Some("c_ty"), "cities"));
        assert!(like(Some("%"), ""));
    }

    #[test]
    fn builds_info() {
        let batch = info_batch(
            &[(InfoCode::VendorName, "Turso".into())],
            &[(InfoCode::DriverAdbcVersion, 1_001_000)],
        )
        .unwrap();
        assert_eq!(batch.num_rows(), 2);
    }
}
