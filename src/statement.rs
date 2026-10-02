// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! ADBC statements: queries, updates, parameter binding and bulk ingestion.

use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::OptionValue;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use grainlift_server::backend::{BackendStatement, QueryResult};

use crate::db::{self, Rows, Value, error};
use crate::session::Session;
use crate::types;

/// Most rows per Arrow batch.
pub const BATCH_ROWS: usize = 1024;
/// Most approximate bytes per Arrow batch, below Grainlift's default 1 MiB
/// batch limit.
pub const BATCH_BYTES: usize = 768 * 1024;

const TARGET_TABLE: &str = "adbc.ingest.target_table";
const TARGET_DB_SCHEMA: &str = "adbc.ingest.target_db_schema";
const TARGET_CATALOG: &str = "adbc.ingest.target_catalog";
const INGEST_MODE: &str = "adbc.ingest.mode";
const TEMPORARY: &str = "adbc.ingest.temporary";

/// Bulk ingestion settings, from the standard `adbc.ingest.*` options.
#[derive(Debug, Default)]
struct Ingest {
    table: Option<String>,
    catalog: Option<String>,
    db_schema: Option<String>,
    mode: Option<String>,
    temporary: bool,
}

/// Parameters bound with `bind` or `bind_stream`.
struct Bound {
    schema: SchemaRef,
    rows: Vec<Vec<Value>>,
}

/// One ADBC statement on a [`Session`].
pub struct TursoStatement {
    session: Arc<Session>,
    sql: Option<String>,
    ingest: Ingest,
    bound: Option<Bound>,
}

impl TursoStatement {
    pub(crate) fn new(session: Arc<Session>) -> Self {
        Self {
            session,
            sql: None,
            ingest: Ingest::default(),
            bound: None,
        }
    }

    fn sql(&self) -> Result<String> {
        self.sql.clone().ok_or_else(|| {
            Error::with_message_and_status(
                "Set a query or an ingestion target table before executing the statement",
                Status::InvalidState,
            )
        })
    }

    /// The single row of parameters a query runs with.
    fn query_parameters(&self) -> Result<Vec<Value>> {
        match self.bound.as_ref().map(|bound| bound.rows.as_slice()) {
            None => Ok(Vec::new()),
            Some([row]) => Ok(row.clone()),
            Some(rows) => Err(Error::with_message_and_status(
                format!(
                    "A query runs with one row of parameters, but {} rows are bound; use \
                     execute_update to run a statement once per row",
                    rows.len()
                ),
                Status::InvalidArguments,
            )),
        }
    }

    /// Start the query and read its first batch, which decides any column
    /// types the declarations leave open.
    fn start(&mut self) -> Result<TursoReader> {
        if self.ingest.table.is_some() {
            return Err(Error::with_message_and_status(
                "Bulk ingestion runs with execute_update",
                Status::InvalidState,
            ));
        }
        let sql = self.sql()?;
        let params = self.query_parameters()?;
        let (rows, columns, first) = self.session.run(move |connection| async move {
            let mut rows = connection.query(sql, params).await?;
            let columns = rows.columns();
            let first = rows.next_rows(BATCH_ROWS, BATCH_BYTES).await?;
            Ok((rows, columns, first))
        })?;
        let (first, done) = first;
        let schema = types::result_schema(&columns, &first);
        let rows = (!done).then_some(rows);
        TursoReader::new(self.session.runtime().clone(), rows, schema, first)
    }

    fn ingest(&mut self) -> Result<i64> {
        let table = self.ingest.table.clone().unwrap_or_default();
        let mode = self
            .ingest
            .mode
            .as_deref()
            .unwrap_or("adbc.ingest.mode.create");
        if let Some(catalog) = self.ingest.catalog.as_deref()
            && catalog != "main"
            && !(self.ingest.temporary && catalog == "temp")
        {
            return Err(not_found(format!("Turso has no catalog {catalog:?}")));
        }
        if let Some(schema) = self.ingest.db_schema.as_deref()
            && !schema.is_empty()
            && schema != "main"
        {
            return Err(not_found(format!("Turso has no schema {schema:?}")));
        }
        let bound = self.bound.take().ok_or_else(|| {
            Error::with_message_and_status(
                "Bind the data to ingest before executing the statement",
                Status::InvalidState,
            )
        })?;
        let name = quote(&table);
        let declarations = bound
            .schema
            .fields()
            .iter()
            .map(|field| {
                format!(
                    "{} {}",
                    quote(field.name()),
                    types::column_declaration(field.data_type())
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let temporary = if self.ingest.temporary { "TEMP " } else { "" };
        let create = format!("CREATE {temporary}TABLE {name} ({declarations})");
        let setup = match mode {
            "adbc.ingest.mode.create" => vec![create],
            "adbc.ingest.mode.append" => Vec::new(),
            "adbc.ingest.mode.replace" => vec![format!("DROP TABLE IF EXISTS {name}"), create],
            "adbc.ingest.mode.create_append" => {
                vec![format!(
                    "CREATE {temporary}TABLE IF NOT EXISTS {name} ({declarations})"
                )]
            }
            other => {
                return Err(Error::with_message_and_status(
                    format!("Unknown ingestion mode {other:?}"),
                    Status::InvalidArguments,
                ));
            }
        };
        let insert = format!(
            "INSERT INTO {name} ({}) VALUES",
            bound
                .schema
                .fields()
                .iter()
                .map(|field| quote(field.name()))
                .collect::<Vec<_>>()
                .join(", "),
        );
        let width = bound.schema.fields().len();
        let count = bound.rows.len();
        self.session.run(move |connection| async move {
            db_atomically(&connection, async {
                for sql in setup {
                    connection.execute(sql, Vec::new()).await?;
                }
                connection.insert_rows(&insert, width, bound.rows).await
            })
            .await
        })?;
        // Change counts can include trigger effects; report the rows ingested.
        Ok(count as i64)
    }
}

/// Run `work` atomically: inside the connection's open transaction, or else
/// in a transaction of its own.
async fn db_atomically<T>(
    connection: &db::Connection,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    if !connection.is_autocommit()? {
        return work.await;
    }
    connection.execute("BEGIN".into(), Vec::new()).await?;
    match work.await {
        Ok(value) => {
            connection.execute("COMMIT".into(), Vec::new()).await?;
            Ok(value)
        }
        Err(failure) => {
            // The original failure matters more than a failed rollback.
            let _ = connection.execute("ROLLBACK".into(), Vec::new()).await;
            Err(failure)
        }
    }
}

impl BackendStatement for TursoStatement {
    /// A new query starts without parameters: clients such as Python's DB-API
    /// reuse one statement per cursor, binding only when a call has parameters.
    fn set_sql_query(&mut self, query: &str) -> Result<()> {
        self.sql = Some(query.to_string());
        self.ingest.table = None;
        self.bound = None;
        Ok(())
    }

    fn set_option(&mut self, key: &str, value: OptionValue) -> Result<()> {
        let text = || match &value {
            OptionValue::String(text) => Ok(text.clone()),
            _ => Err(Error::with_message_and_status(
                format!("{key} takes a string"),
                Status::InvalidArguments,
            )),
        };
        match key {
            TARGET_TABLE => {
                self.ingest.table = Some(text()?);
                self.sql = None;
            }
            TARGET_CATALOG => self.ingest.catalog = Some(text()?),
            TARGET_DB_SCHEMA => self.ingest.db_schema = Some(text()?),
            INGEST_MODE => self.ingest.mode = Some(text()?),
            TEMPORARY => {
                self.ingest.temporary = match text()?.as_str() {
                    "true" => true,
                    "false" => false,
                    other => {
                        return Err(Error::with_message_and_status(
                            format!("{TEMPORARY} must be true or false, not {other:?}"),
                            Status::InvalidArguments,
                        ));
                    }
                }
            }
            _ => {
                return Err(Error::with_message_and_status(
                    format!("Statement option {key} is not supported"),
                    Status::NotImplemented,
                ));
            }
        }
        Ok(())
    }

    /// Check the query by preparing it; clients such as DuckDB's
    /// `adbc_scanner` prepare before executing.
    fn prepare(&mut self) -> Result<()> {
        if self.ingest.table.is_some() {
            return Ok(());
        }
        let sql = self.sql()?;
        self.session
            .run(move |connection| async move { connection.describe(sql).await })
            .map(drop)
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        Ok(types::parameter_schema(&self.sql()?))
    }

    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        self.bound = Some(Bound {
            schema: batch.schema(),
            rows: types::batch_rows(&batch)?,
        });
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        let schema = reader.schema();
        let mut rows = Vec::new();
        for batch in reader {
            rows.extend(types::batch_rows(&batch?)?);
        }
        self.bound = Some(Bound { schema, rows });
        Ok(())
    }

    /// The result schema. Declared column types answer without running the
    /// query; when a column has none, a read-only query runs to look at its
    /// first batch, and any other statement must be executed instead.
    fn execute_schema(&mut self) -> Result<Schema> {
        let sql = self.sql()?;
        let described = sql.clone();
        let columns = self
            .session
            .run(move |connection| async move { connection.describe(described).await })?;
        if let Some(schema) = types::declared_schema(&columns) {
            return Ok(schema);
        }
        if !is_read_only(&sql) {
            return Err(Error::with_message_and_status(
                "This statement's result types depend on the values it returns, and it may \
                 write, so it must be executed to learn them",
                Status::InvalidState,
            ));
        }
        Ok(self.start()?.schema().as_ref().clone())
    }

    fn execute_result(&mut self) -> Result<QueryResult> {
        Ok(QueryResult::from_reader(Box::new(self.start()?)))
    }

    /// Run the statement once per bound row (once if none are bound), all in
    /// one transaction, or ingest the bound rows.
    fn execute_update(&mut self) -> Result<Option<i64>> {
        if self.ingest.table.is_some() {
            return self.ingest().map(Some);
        }
        let sql = self.sql()?;
        let rows = match &self.bound {
            None => vec![Vec::new()],
            Some(bound) => bound.rows.clone(),
        };
        let changed = self.session.run(move |connection| async move {
            if let [params] = rows.as_slice() {
                return connection.execute(sql, params.clone()).await;
            }
            db_atomically(&connection, connection.execute_each(sql, rows)).await
        })?;
        Ok(Some(changed as i64))
    }
}

/// Whether `sql` reads without writing: it starts with `SELECT` or `VALUES`,
/// or with `WITH` or `EXPLAIN` and names no statement that writes.
/// Conservative: a write keyword anywhere, even in a string, counts.
fn is_read_only(sql: &str) -> bool {
    let words = sql
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .filter(|word| !word.is_empty())
        .map(str::to_ascii_uppercase)
        .collect::<Vec<_>>();
    let writes = [
        "INSERT", "UPDATE", "DELETE", "REPLACE", "CREATE", "DROP", "ALTER", "PRAGMA",
    ];
    match words.first().map(String::as_str) {
        Some("SELECT" | "VALUES") => true,
        Some("WITH" | "EXPLAIN") => !words.iter().any(|word| writes.contains(&word.as_str())),
        _ => false,
    }
}

fn quote(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn not_found(message: String) -> Error {
    error(message, Status::NotFound, b"3F000")
}

/// A query's result, read lazily in bounded batches from Turso's cursor.
pub struct TursoReader {
    runtime: db::Runtime,
    rows: Option<Rows>,
    schema: SchemaRef,
    first: Option<RecordBatch>,
    read: usize,
}

impl TursoReader {
    fn new(
        runtime: db::Runtime,
        rows: Option<Rows>,
        schema: SchemaRef,
        first: Vec<Vec<Value>>,
    ) -> Result<Self> {
        let batch = (!first.is_empty())
            .then(|| types::build_batch(&schema, &first, 0))
            .transpose()?;
        Ok(Self {
            runtime,
            rows,
            schema,
            first: batch,
            read: first.len(),
        })
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if let Some(batch) = self.first.take() {
            return Ok(Some(batch));
        }
        let Some(mut rows) = self.rows.take() else {
            return Ok(None);
        };
        let (rows, fetched) = self.runtime.run(async move {
            let fetched = rows.next_rows(BATCH_ROWS, BATCH_BYTES).await;
            (rows, fetched)
        })?;
        let (fetched, done) = fetched?;
        if !done {
            self.rows = Some(rows);
        }
        if fetched.is_empty() {
            return Ok(None);
        }
        let batch = types::build_batch(&self.schema, &fetched, self.read)?;
        self.read += fetched.len();
        Ok(Some(batch))
    }
}

impl Iterator for TursoReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_batch() {
            Ok(batch) => batch.map(Ok),
            Err(failure) => {
                self.rows = None;
                Some(Err(ArrowError::ExternalError(Box::new(failure))))
            }
        }
    }
}

impl RecordBatchReader for TursoReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::is_read_only;

    #[test]
    fn recognizes_reads() {
        assert!(is_read_only("select 1"));
        assert!(is_read_only("  VALUES (1)"));
        assert!(is_read_only("WITH t AS (SELECT 1) SELECT * FROM t"));
        assert!(!is_read_only(
            "WITH t AS (SELECT 1) INSERT INTO u SELECT * FROM t"
        ));
        assert!(!is_read_only("INSERT INTO t VALUES (1) RETURNING *"));
        assert!(!is_read_only("PRAGMA user_version = 3"));
    }
}
