// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Turso values as Arrow, and Arrow parameters as Turso values.
//!
//! Turso, like SQLite, types values rather than columns, while every Arrow
//! batch of a result needs one type per column. A result column's type
//! therefore comes from:
//!
//! 1. the declared type of the table column it reads, by SQLite's affinity
//!    rules ([`declared_type`]), when there is one; otherwise
//! 2. the values in the result's first batch ([`inferred_type`]).
//!
//! Later values must fit that type. Lossless conversions are made (an integer
//! in a `REAL` column, a number in a `TEXT` column); anything else is an
//! `INVALID_DATA` error that names the column, and a `CAST` in the query
//! chooses the type explicitly.

use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::db::{Column, Value, error};

/// The Arrow type for a column declared as `declared`, following SQLite's
/// affinity rules, or `None` for an undeclared column.
///
/// `BOOLEAN` columns become Arrow booleans, `NUMERIC` and `DECIMAL` columns
/// doubles, and any other declaration, such as `DATE` or `TIMESTAMP`, text:
/// Turso keeps dates and times as text or numbers, never as a date type.
pub fn declared_type(declared: &str) -> Option<DataType> {
    let declared = declared.trim().to_ascii_uppercase();
    let has = |part: &str| declared.contains(part);
    Some(if declared.is_empty() {
        return None;
    } else if has("INT") {
        DataType::Int64
    } else if has("CHAR") || has("CLOB") || has("TEXT") {
        DataType::Utf8
    } else if has("BLOB") {
        DataType::Binary
    } else if has("REAL") || has("FLOA") || has("DOUB") {
        DataType::Float64
    } else if has("BOOL") {
        DataType::Boolean
    } else if has("DEC") || has("NUMERIC") {
        DataType::Float64
    } else {
        DataType::Utf8
    })
}

/// The narrowest Arrow type that holds every value: integers are int64, a mix
/// of integers and reals float64, any text utf8, any blob binary. A column of
/// only NULLs is `fallback`.
pub fn inferred_type<'a>(
    values: impl IntoIterator<Item = &'a Value>,
    fallback: DataType,
) -> DataType {
    let mut inferred: Option<DataType> = None;
    for value in values {
        let kind = match value {
            Value::Null => continue,
            Value::Integer(_) => DataType::Int64,
            Value::Real(_) => DataType::Float64,
            Value::Text(_) => DataType::Utf8,
            Value::Blob(_) => DataType::Binary,
        };
        inferred = Some(match (inferred, kind) {
            (None, kind) => kind,
            (Some(current), kind) if current == kind => current,
            (Some(DataType::Binary), _) | (_, DataType::Binary) => DataType::Binary,
            (Some(DataType::Utf8), _) | (_, DataType::Utf8) => DataType::Utf8,
            _ => DataType::Float64,
        });
    }
    inferred.unwrap_or(fallback)
}

/// Whether a declared type is trustworthy. Turso reports `BLOB` for computed
/// columns of common table expressions, where SQLite reports no declared
/// type, so a `BLOB` declaration only says the column may hold blobs.
fn is_reliable(declared: &str) -> bool {
    !declared.trim().eq_ignore_ascii_case("BLOB")
}

/// The schema of a result whose first rows are `rows`.
///
/// An undeclared column of only NULLs is int64, as in the ADBC SQLite driver;
/// a `BLOB` one is binary.
pub fn result_schema(columns: &[Column], rows: &[Vec<Value>]) -> SchemaRef {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let declared = column.decl_type.as_deref().unwrap_or_default();
            let values = rows.iter().map(|row| &row[index]);
            let data_type = match declared_type(declared) {
                Some(data_type) if is_reliable(declared) => data_type,
                Some(data_type) => inferred_type(values, data_type),
                None => inferred_type(values, DataType::Int64),
            };
            Field::new(&column.name, data_type, true)
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

/// The schema of a result known from its declarations alone, or `None` when
/// a column has no declared type and its values decide.
pub fn declared_schema(columns: &[Column]) -> Option<Schema> {
    columns
        .iter()
        .map(|column| {
            let declared = column
                .decl_type
                .as_deref()
                .filter(|declared| is_reliable(declared))?;
            let data_type = declared_type(declared)?;
            Some(Field::new(&column.name, data_type, true))
        })
        .collect::<Option<Vec<_>>>()
        .map(Schema::new)
}

/// Build a batch of `rows` with `schema`. `first_row` (zero-based) numbers
/// rows in error messages.
pub fn build_batch(
    schema: &SchemaRef,
    rows: &[Vec<Value>],
    first_row: usize,
) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| build_column(field, rows, index, first_row))
        .collect::<Result<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &options,
    )?)
}

fn build_column(
    field: &Field,
    rows: &[Vec<Value>],
    index: usize,
    first_row: usize,
) -> Result<ArrayRef> {
    let mismatch = |row: usize, value: &Value| {
        let kind = match value {
            Value::Null => "NULL",
            Value::Integer(_) => "an integer",
            Value::Real(_) => "a real",
            Value::Text(_) => "text",
            Value::Blob(_) => "a blob",
        };
        error(
            format!(
                "Column {:?} has type {} but row {} holds {kind}; CAST the column in the query \
                 to choose its type",
                field.name(),
                field.data_type(),
                first_row + row + 1,
            ),
            Status::InvalidData,
            b"22000",
        )
    };
    let values = rows.iter().map(|row| &row[index]).enumerate();
    Ok(match field.data_type() {
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(rows.len());
            for (row, value) in values {
                match value {
                    Value::Null => builder.append_null(),
                    Value::Integer(value) => builder.append_value(*value),
                    other => return Err(mismatch(row, other)),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(rows.len());
            for (row, value) in values {
                match value {
                    Value::Null => builder.append_null(),
                    Value::Integer(value) => builder.append_value(*value as f64),
                    Value::Real(value) => builder.append_value(*value),
                    other => return Err(mismatch(row, other)),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(rows.len());
            for (row, value) in values {
                match value {
                    Value::Null => builder.append_null(),
                    Value::Integer(value) => builder.append_value(*value != 0),
                    other => return Err(mismatch(row, other)),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for (row, value) in values {
                match value {
                    Value::Null => builder.append_null(),
                    Value::Text(value) => builder.append_value(value),
                    Value::Integer(value) => builder.append_value(value.to_string()),
                    Value::Real(value) => builder.append_value(format!("{value:?}")),
                    other => return Err(mismatch(row, other)),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::new();
            for (row, value) in values {
                match value {
                    Value::Null => builder.append_null(),
                    Value::Blob(value) => builder.append_value(value),
                    Value::Text(value) => builder.append_value(value.as_bytes()),
                    other => return Err(mismatch(row, other)),
                }
            }
            Arc::new(builder.finish())
        }
        other => unreachable!("results never have type {other}"),
    })
}

/// Approximate bytes a row occupies in an Arrow batch.
pub fn row_bytes(row: &[Value]) -> usize {
    row.iter()
        .map(|value| match value {
            Value::Text(text) => text.len() + 4,
            Value::Blob(blob) => blob.len() + 4,
            _ => 8,
        })
        .sum()
}

/// The rows of `batch` as parameter values, one `Vec` per row.
///
/// Integers, floats, booleans (as 0 and 1), strings and binary map to the
/// matching Turso values. Dates, times, timestamps, decimals and other
/// scalar types are bound as their text form (for example
/// `2026-10-01T12:00:00`), which Turso's date functions understand.
pub fn batch_rows(batch: &RecordBatch) -> Result<Vec<Vec<Value>>> {
    let columns = batch
        .columns()
        .iter()
        .zip(batch.schema().fields())
        .map(|(array, field)| column_values(array, field.name()))
        .collect::<Result<Vec<_>>>()?;
    Ok((0..batch.num_rows())
        .map(|row| columns.iter().map(|column| column[row].clone()).collect())
        .collect())
}

fn column_values(array: &ArrayRef, name: &str) -> Result<Vec<Value>> {
    let cast = |to: &DataType| {
        arrow_cast::cast(array, to).map_err(|_| {
            Error::with_message_and_status(
                format!(
                    "Parameter {name:?} has type {}, which Turso cannot store",
                    array.data_type()
                ),
                Status::NotImplemented,
            )
        })
    };
    let nullable = |array: &dyn Array, value: &dyn Fn(usize) -> Value| {
        (0..array.len())
            .map(|row| {
                if array.is_null(row) {
                    Value::Null
                } else {
                    value(row)
                }
            })
            .collect::<Vec<_>>()
    };
    Ok(match array.data_type() {
        DataType::Null => vec![Value::Null; array.len()],
        DataType::Boolean => {
            let values = array.as_boolean();
            nullable(values, &|row| Value::Integer(values.value(row).into()))
        }
        DataType::UInt64 => {
            let values = array.as_primitive::<arrow_array::types::UInt64Type>();
            if let Some(large) = values
                .iter()
                .flatten()
                .find(|value| i64::try_from(*value).is_err())
            {
                return Err(error(
                    format!(
                        "Parameter {name:?} holds {large}, which is larger than Turso's 64-bit integers"
                    ),
                    Status::InvalidArguments,
                    b"22003",
                ));
            }
            let values = cast(&DataType::Int64)?;
            let values = values.as_primitive::<Int64Type>();
            nullable(values, &|row| Value::Integer(values.value(row)))
        }
        data_type if data_type.is_integer() => {
            let values = cast(&DataType::Int64)?;
            let values = values.as_primitive::<Int64Type>();
            nullable(values, &|row| Value::Integer(values.value(row)))
        }
        data_type if data_type.is_floating() => {
            let values = cast(&DataType::Float64)?;
            let values = values.as_primitive::<Float64Type>();
            nullable(values, &|row| Value::Real(values.value(row)))
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => {
            let values = cast(&DataType::LargeBinary)?;
            let values = values.as_binary::<i64>();
            nullable(values, &|row| Value::Blob(values.value(row).to_vec()))
        }
        DataType::Dictionary(_, value_type) => {
            let values = cast(value_type)?;
            column_values(&values, name)?
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(..)
        | DataType::Struct(_)
        | DataType::Map(..)
        | DataType::Union(..)
        | DataType::RunEndEncoded(..) => {
            return Err(Error::with_message_and_status(
                format!(
                    "Parameter {name:?} has type {}, which Turso cannot store",
                    array.data_type()
                ),
                Status::NotImplemented,
            ));
        }
        _ => {
            let values = cast(&DataType::LargeUtf8)?;
            let values = values.as_string::<i64>();
            nullable(values, &|row| Value::Text(values.value(row).to_string()))
        }
    })
}

/// The column type declared for an Arrow field by bulk ingestion. Reading the
/// table back maps each declaration to the same Arrow type, except decimals,
/// dates and times, which are stored and read back as text.
pub fn column_declaration(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Boolean => "BOOLEAN",
        data_type if data_type.is_integer() => "INTEGER",
        data_type if data_type.is_floating() => "REAL",
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BLOB",
        DataType::Dictionary(_, value_type) => column_declaration(value_type),
        _ => "TEXT",
    }
}

/// The parameters of `sql` as SQLite numbers them: `?` takes the next number,
/// `?NNN` number NNN, and each distinct `:name`, `@name` or `$name` the next
/// number. Each entry is the parameter's name, or `None` for a plain `?`.
pub fn parameters(sql: &str) -> Vec<Option<String>> {
    let mut slots: Vec<Option<String>> = Vec::new();
    let bytes = sql.as_bytes();
    let mut index = 0;
    let identifier = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_' || byte >= 0x80;
    while index < bytes.len() {
        let byte = bytes[index];
        index += 1;
        match byte {
            b'\'' | b'"' | b'`' => {
                while index < bytes.len() && bytes[index] != byte {
                    index += 1;
                }
                index += 1;
            }
            b'[' => {
                while index < bytes.len() && bytes[index] != b']' {
                    index += 1;
                }
                index += 1;
            }
            b'-' if bytes.get(index) == Some(&b'-') => {
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index) == Some(&b'*') => {
                index += 1;
                while index < bytes.len() && !(bytes[index - 1] == b'*' && bytes[index] == b'/') {
                    index += 1;
                }
                index += 1;
            }
            b'?' => {
                let start = index;
                while index < bytes.len() && bytes[index].is_ascii_digit() {
                    index += 1;
                }
                match sql[start..index].parse::<usize>() {
                    Ok(number) if number > 0 => {
                        if slots.len() < number {
                            slots.resize(number, None);
                        }
                        slots[number - 1].get_or_insert_with(|| format!("?{number}"));
                    }
                    _ => slots.push(None),
                }
            }
            b':' | b'@' | b'$' if bytes.get(index).copied().is_some_and(identifier) => {
                let start = index - 1;
                while index < bytes.len() && identifier(bytes[index]) {
                    index += 1;
                }
                let name = &sql[start..index];
                if !slots.iter().any(|slot| slot.as_deref() == Some(name)) {
                    slots.push(Some(name.to_string()));
                }
            }
            _ => {}
        }
    }
    slots
}

/// The schema `get_parameter_schema` reports: one null-typed field per
/// parameter, named as in the SQL or by its 1-based position.
pub fn parameter_schema(sql: &str) -> Schema {
    let fields = parameters(sql)
        .into_iter()
        .enumerate()
        .map(|(index, name)| {
            Field::new(
                name.unwrap_or_else(|| (index + 1).to_string()),
                DataType::Null,
                true,
            )
        })
        .collect::<Vec<_>>();
    Schema::new(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Date32Array, Int32Array, StringArray};

    #[test]
    fn declared_types_follow_affinity() {
        assert_eq!(declared_type("BIGINT"), Some(DataType::Int64));
        assert_eq!(declared_type("varchar(20)"), Some(DataType::Utf8));
        assert_eq!(declared_type("BLOB"), Some(DataType::Binary));
        assert_eq!(declared_type("double precision"), Some(DataType::Float64));
        assert_eq!(declared_type("BOOLEAN"), Some(DataType::Boolean));
        assert_eq!(declared_type("DECIMAL(10,2)"), Some(DataType::Float64));
        assert_eq!(declared_type("DATE"), Some(DataType::Utf8));
        assert_eq!(declared_type(""), None);
    }

    #[test]
    fn infers_the_narrowest_type() {
        let infer = |values: &[Value]| inferred_type(values, DataType::Int64);
        assert_eq!(infer(&[Value::Integer(1), Value::Null]), DataType::Int64);
        assert_eq!(
            infer(&[Value::Integer(1), Value::Real(1.5)]),
            DataType::Float64
        );
        assert_eq!(
            infer(&[Value::Integer(1), Value::Text("a".into())]),
            DataType::Utf8
        );
        assert_eq!(
            infer(&[Value::Text("a".into()), Value::Blob(vec![1])]),
            DataType::Binary
        );
        assert_eq!(infer(&[Value::Null]), DataType::Int64);
        assert_eq!(
            inferred_type(&[Value::Null], DataType::Binary),
            DataType::Binary
        );
    }

    #[test]
    fn builds_batches_and_rejects_mismatches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Float64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let rows = vec![
            vec![Value::Integer(2), Value::Real(1.5)],
            vec![Value::Null, Value::Text("x".into())],
        ];
        let batch = build_batch(&schema, &rows, 0).unwrap();
        assert_eq!(batch.column(0).as_primitive::<Float64Type>().value(0), 2.0);
        assert_eq!(batch.column(1).as_string::<i32>().value(0), "1.5");

        let bad = vec![vec![Value::Text("x".into()), Value::Null]];
        let error = build_batch(&schema, &bad, 1024).unwrap_err();
        assert_eq!(error.status, Status::InvalidData);
        assert!(error.message.contains("row 1025"), "{}", error.message);
    }

    #[test]
    fn converts_parameters() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, true),
            Field::new("b", DataType::Boolean, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("d", DataType::Date32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(7), None])),
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Date32Array::from(vec![0, 20_362])),
            ],
        )
        .unwrap();
        assert_eq!(
            batch_rows(&batch).unwrap(),
            vec![
                vec![
                    Value::Integer(7),
                    Value::Integer(1),
                    Value::Text("a".into()),
                    Value::Text("1970-01-01".into())
                ],
                vec![
                    Value::Null,
                    Value::Integer(0),
                    Value::Text("b".into()),
                    Value::Text("2025-10-01".into())
                ],
            ]
        );
    }

    #[test]
    fn numbers_parameters_like_sqlite() {
        assert_eq!(parameters("SELECT ?, ?"), vec![None, None]);
        assert_eq!(
            parameters("SELECT :a, ?3, :a, '?' -- ?\n, \":b\", @c"),
            vec![
                Some(":a".into()),
                None,
                Some("?3".into()),
                Some("@c".into())
            ]
        );
        assert_eq!(parameter_schema("SELECT ?, $x").field(1).name(), "$x");
        assert_eq!(parameter_schema("SELECT ?, $x").field(0).name(), "1");
    }
}
