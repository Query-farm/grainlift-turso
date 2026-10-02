// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! One interface over Turso's two Rust clients.
//!
//! - [`turso`]: the embedded Turso Database engine, for a local file.
//! - [`turso_serverless`]: Turso Cloud over its SQL-over-HTTP protocol.
//!
//! Both clients are async and share an API shape, so [`Database`],
//! [`Connection`] and [`Rows`] are thin enums over them. Grainlift's backend
//! traits are synchronous, so every call runs on a [`Runtime`] owned by the
//! backend and the calling thread waits for it.

use std::future::Future;
use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
pub use turso::Value;

use crate::cursor::{Cursor, Endpoint};

/// Where the database lives.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Location {
    /// A local database file opened with the embedded engine; `:memory:` is a
    /// private in-memory database shared by every client connection.
    Local {
        /// File path, or `:memory:`.
        path: String,
        /// Open the file read-only.
        read_only: bool,
    },
    /// A Turso Cloud database.
    Remote {
        /// `libsql://`, `https://` or `http://` URL.
        url: String,
        /// Database auth token. A read-only token makes a read-only service.
        auth_token: Option<String>,
    },
}

impl Location {
    /// A remote location for `libsql://`, `https://` and `http://` URLs, and a
    /// local file for anything else (an optional `file:` prefix is removed).
    pub fn parse(url: &str, auth_token: Option<String>, read_only: bool) -> Self {
        let remote = ["libsql://", "https://", "http://"]
            .iter()
            .any(|scheme| url.starts_with(scheme));
        if remote {
            Self::Remote {
                url: url.to_string(),
                auth_token,
            }
        } else {
            Self::Local {
                path: url.strip_prefix("file:").unwrap_or(url).to_string(),
                read_only,
            }
        }
    }

    /// Whether the database is reached over the network.
    pub fn is_remote(&self) -> bool {
        matches!(self, Self::Remote { .. })
    }
}

/// The Tokio runtime that drives both clients.
///
/// Grainlift calls backends synchronously, sometimes from inside its own Tokio
/// runtime, where blocking on another runtime is not allowed. Each call is
/// therefore spawned here and the caller waits with a runtime-agnostic
/// executor.
#[derive(Clone)]
pub struct Runtime(Arc<tokio::runtime::Runtime>);

impl Runtime {
    /// Start a multi-threaded runtime.
    pub fn new() -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("grainlift-turso")
            .enable_all()
            .build()
            .map_err(|error| io_error(format!("Could not start the Turso runtime: {error}")))?;
        Ok(Self(Arc::new(runtime)))
    }

    /// Run `future` to completion on this runtime and return its output.
    pub fn run<F>(&self, future: F) -> Result<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        futures::executor::block_on(self.0.spawn(future)).map_err(|_| {
            Error::with_message_and_status("A Turso operation failed", Status::Internal)
        })
    }
}

/// An open database.
#[derive(Clone)]
pub enum Database {
    Local(turso::Database),
    Remote(turso_serverless::Database, Endpoint),
}

impl Database {
    /// Open the database at `location`.
    pub async fn open(location: &Location) -> Result<Self> {
        match location {
            Location::Local { path, read_only } => turso::Builder::new_local(path)
                .read_only(*read_only)
                .build()
                .await
                .map(Self::Local)
                .map_err(local_error),
            Location::Remote { url, auth_token } => {
                let mut builder = turso_serverless::Builder::new_remote(url.clone());
                if let Some(token) = auth_token {
                    builder = builder.with_auth_token(token.clone());
                }
                let database = builder.build().await.map_err(remote_error)?;
                Ok(Self::Remote(
                    database,
                    Endpoint::new(url, auth_token.clone()),
                ))
            }
        }
    }

    /// Open a new connection, with its own transaction state.
    pub fn connect(&self) -> Result<Connection> {
        match self {
            Self::Local(database) => database
                .connect()
                .map(Connection::Local)
                .map_err(local_error),
            Self::Remote(database, endpoint) => database
                .connect()
                .map(|connection| Connection::Remote(connection, endpoint.clone()))
                .map_err(remote_error),
        }
    }
}

/// A result column: its name and, when it reads a table column directly, the
/// type that column was declared with.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Column {
    pub name: String,
    pub decl_type: Option<String>,
}

/// One connection. Clones share the connection and its transaction.
#[derive(Clone)]
pub enum Connection {
    Local(turso::Connection),
    Remote(turso_serverless::Connection, Endpoint),
}

impl Connection {
    /// Run a query and return its rows.
    pub async fn query(&self, sql: String, params: Vec<Value>) -> Result<Rows> {
        match self {
            Self::Local(connection) => connection
                .query(sql, params)
                .await
                .map(Rows::Local)
                .map_err(local_error),
            // A transaction lives on the connection's server-side stream, so
            // its queries go through that stream and arrive whole.
            Self::Remote(connection, _) if !connection.is_autocommit().map_err(remote_error)? => {
                connection
                    .query(sql, remote_params(params))
                    .await
                    .map(Rows::Remote)
                    .map_err(remote_error)
            }
            Self::Remote(_, endpoint) => Cursor::open(endpoint, sql, params)
                .await
                .map(|cursor| Rows::Stream(Box::new(cursor))),
        }
    }

    /// Run a statement and return the number of rows it changed.
    /// Rows it returns, as with `RETURNING` or a `SELECT`, are discarded.
    pub async fn execute(&self, sql: String, params: Vec<Value>) -> Result<u64> {
        match self {
            Self::Local(connection) => {
                let mut statement = connection.prepare(&sql).await.map_err(local_error)?;
                run_local(&mut statement, params).await
            }
            Self::Remote(connection, _) => connection
                .execute(sql, remote_params(params))
                .await
                .map_err(remote_error),
        }
    }

    /// Run one statement once per row of parameters, in order, and return the
    /// total number of rows changed. Stops at the first failure; the caller
    /// owns the transaction around it.
    pub async fn execute_each(&self, sql: String, rows: Vec<Vec<Value>>) -> Result<u64> {
        match self {
            Self::Local(connection) => {
                let mut statement = connection.prepare(&sql).await.map_err(local_error)?;
                let mut changed = 0;
                for params in rows {
                    changed += run_local(&mut statement, params).await?;
                    statement.reset().map_err(local_error)?;
                }
                Ok(changed)
            }
            Self::Remote(connection, _) => {
                let statements = rows.into_iter().map(|params| (sql.clone(), params));
                remote_batches(connection, statements).await
            }
        }
    }

    /// Insert `rows` of `width` values with `insert`, an `INSERT INTO t (...)
    /// VALUES` prefix, and return the number of rows inserted. The caller owns
    /// the transaction around it.
    ///
    /// Locally each row runs through one prepared statement. Turso Cloud
    /// receives multi-row `INSERT`s, many per request, so a large ingestion
    /// takes few round trips.
    pub async fn insert_rows(
        &self,
        insert: &str,
        width: usize,
        rows: Vec<Vec<Value>>,
    ) -> Result<u64> {
        let tuple = format!("({})", vec!["?"; width].join(", "));
        match self {
            Self::Local(_) => self.execute_each(format!("{insert} {tuple}"), rows).await,
            Self::Remote(connection, _) => {
                let per_statement = (MAX_PARAMETERS / width.max(1)).clamp(1, MAX_ROWS_PER_INSERT);
                let mut statements = Vec::new();
                let mut rows = rows.into_iter().peekable();
                while rows.peek().is_some() {
                    let mut chunk = Vec::new();
                    let mut bytes = 0;
                    while chunk.len() < per_statement && bytes < MAX_REQUEST_BYTES / 2 {
                        let Some(row) = rows.next() else { break };
                        bytes += crate::types::row_bytes(&row);
                        chunk.push(row);
                    }
                    let sql = format!("{insert} {}", vec![tuple.as_str(); chunk.len()].join(", "));
                    statements.push((sql, chunk.into_iter().flatten().collect()));
                }
                remote_batches(connection, statements).await
            }
        }
    }

    /// Prepare `sql` without running it, and describe its result columns.
    pub async fn describe(&self, sql: String) -> Result<Vec<Column>> {
        match self {
            Self::Local(connection) => {
                let statement = connection.prepare(&sql).await.map_err(local_error)?;
                Ok(statement
                    .columns()
                    .iter()
                    .map(|column| Column {
                        name: column.name().to_string(),
                        decl_type: column.decl_type().map(str::to_string),
                    })
                    .collect())
            }
            Self::Remote(connection, _) => {
                let statement = connection.prepare(&sql).await.map_err(remote_error)?;
                Ok(statement
                    .columns()
                    .into_iter()
                    .map(|column| Column {
                        name: column.name,
                        decl_type: column.decl_type,
                    })
                    .collect())
            }
        }
    }

    /// Close the connection. A Turso Cloud stream is released, rolling back
    /// any open transaction; a local connection closes when dropped.
    pub async fn close(&self) {
        if let Self::Remote(connection, _) = self {
            // Closing cannot fail in a way the caller could act on.
            let _ = connection.close().await;
        }
    }

    /// Whether no transaction is open.
    pub fn is_autocommit(&self) -> Result<bool> {
        match self {
            Self::Local(connection) => connection.is_autocommit().map_err(local_error),
            Self::Remote(connection, _) => connection.is_autocommit().map_err(remote_error),
        }
    }
}

/// The rows of a running query.
///
/// The embedded engine steps its cursor one row at a time; Turso Cloud sends
/// the whole result in one response.
pub enum Rows {
    Local(turso::Rows),
    Remote(turso_serverless::Rows),
    Stream(Box<Cursor>),
}

impl Rows {
    /// The result columns.
    pub fn columns(&self) -> Vec<Column> {
        match self {
            Self::Local(rows) => rows
                .columns()
                .iter()
                .map(|column| Column {
                    name: column.name().to_string(),
                    decl_type: column.decl_type().map(str::to_string),
                })
                .collect(),
            Self::Stream(cursor) => cursor.columns(),
            Self::Remote(rows) => rows
                .columns()
                .into_iter()
                .map(|column| Column {
                    name: column.name,
                    decl_type: column.decl_type,
                })
                .collect(),
        }
    }

    /// Read the next rows, up to `max_rows` rows or until they reach about
    /// `max_bytes` (always at least one row). The flag is true once the result
    /// is exhausted.
    pub async fn next_rows(
        &mut self,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<(Vec<Vec<Value>>, bool)> {
        if let Self::Stream(cursor) = self {
            return cursor.next_rows(max_rows, max_bytes).await;
        }
        let mut rows = Vec::new();
        let mut bytes = 0;
        while rows.len() < max_rows && bytes < max_bytes {
            let row = match self {
                Self::Local(cursor) => match cursor.next().await.map_err(local_error)? {
                    Some(row) => (0..row.column_count())
                        .map(|index| row.get_value(index).map_err(local_error))
                        .collect::<Result<Vec<_>>>()?,
                    None => return Ok((rows, true)),
                },
                Self::Remote(cursor) => match cursor.next().await.map_err(remote_error)? {
                    Some(row) => (0..row.column_count())
                        .map(|index| row.get_value(index).map(from_remote).map_err(remote_error))
                        .collect::<Result<Vec<_>>>()?,
                    None => return Ok((rows, true)),
                },
                Self::Stream(_) => unreachable!("streamed above"),
            };
            bytes += crate::types::row_bytes(&row);
            rows.push(row);
        }
        Ok((rows, false))
    }
}

/// Most bound parameters in one statement (SQLite's default limit, which
/// Turso Cloud enforces).
const MAX_PARAMETERS: usize = 32_766;
/// Most rows in one multi-row `INSERT` sent to Turso Cloud.
const MAX_ROWS_PER_INSERT: usize = 1_000;
/// Most statements in one request to Turso Cloud.
const MAX_STATEMENTS_PER_REQUEST: usize = 256;
/// Approximate largest request body sent to Turso Cloud. Values travel as
/// JSON, so text grows by escaping and blobs by base64.
const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;

/// Run `statements` on Turso Cloud, in order, in as few requests as the
/// request limits allow, and return the total number of rows changed. Stops at
/// the first failure.
async fn remote_batches(
    connection: &turso_serverless::Connection,
    statements: impl IntoIterator<Item = (String, Vec<Value>)>,
) -> Result<u64> {
    async fn send(
        connection: &turso_serverless::Connection,
        batch: Vec<turso_serverless::BatchStatement>,
    ) -> Result<u64> {
        let results = connection.batch(batch).await.map_err(remote_error)?;
        Ok(results.iter().map(|result| result.rows_affected()).sum())
    }
    let mut changed = 0;
    let mut batch = Vec::new();
    let mut bytes = 0;
    for (sql, params) in statements {
        let size = sql.len() + crate::types::row_bytes(&params) * 3 / 2;
        if !batch.is_empty()
            && (batch.len() == MAX_STATEMENTS_PER_REQUEST || bytes + size > MAX_REQUEST_BYTES)
        {
            changed += send(connection, std::mem::take(&mut batch)).await?;
            bytes = 0;
        }
        batch.push(
            turso_serverless::BatchStatement::new(sql, remote_params(params))
                .map_err(remote_error)?,
        );
        bytes += size;
    }
    if !batch.is_empty() {
        changed += send(connection, batch).await?;
    }
    Ok(changed)
}

/// Step a local statement to completion, discarding any rows (the engine's
/// own `execute` refuses statements that return rows), and return the number
/// of rows it changed.
async fn run_local(statement: &mut turso::Statement, params: Vec<Value>) -> Result<u64> {
    let mut rows = statement.query(params).await.map_err(local_error)?;
    while rows.next().await.map_err(local_error)?.is_some() {}
    drop(rows);
    Ok(statement.n_change())
}

fn remote_params(params: Vec<Value>) -> Vec<turso_serverless::Value> {
    params.into_iter().map(to_remote).collect()
}

pub(crate) fn to_remote(value: Value) -> turso_serverless::Value {
    match value {
        Value::Null => turso_serverless::Value::Null,
        Value::Integer(value) => turso_serverless::Value::Integer(value),
        Value::Real(value) => turso_serverless::Value::Real(value),
        Value::Text(value) => turso_serverless::Value::Text(value),
        Value::Blob(value) => turso_serverless::Value::Blob(value),
    }
}

pub(crate) fn from_remote(value: turso_serverless::Value) -> Value {
    match value {
        turso_serverless::Value::Null => Value::Null,
        turso_serverless::Value::Integer(value) => Value::Integer(value),
        turso_serverless::Value::Real(value) => Value::Real(value),
        turso_serverless::Value::Text(value) => Value::Text(value),
        turso_serverless::Value::Blob(value) => Value::Blob(value),
    }
}

/// An ADBC error with a SQLSTATE.
pub fn error(message: impl Into<String>, status: Status, sqlstate: &[u8; 5]) -> Error {
    let mut error = Error::with_message_and_status(message, status);
    error.sqlstate = sqlstate.map(|byte| byte as std::ffi::c_char);
    error
}

fn io_error(message: String) -> Error {
    error(message, Status::IO, b"58000")
}

/// The ADBC status and SQLSTATE for a SQLite result code category.
fn classify(kind: &str) -> (Status, &'static [u8; 5]) {
    match kind {
        "constraint" => (Status::Integrity, b"23000"),
        "busy" => (Status::Timeout, b"40001"),
        "interrupt" => (Status::Cancelled, b"57014"),
        "misuse" => (Status::InvalidState, b"HY010"),
        "readonly" => (Status::Unauthorized, b"25006"),
        "full" => (Status::IO, b"53100"),
        "corrupt" => (Status::InvalidData, b"XX001"),
        "io" => (Status::IO, b"58030"),
        "conversion" => (Status::InvalidData, b"22000"),
        _ => (Status::InvalidArguments, b"42000"),
    }
}

/// Convert an embedded-engine error. Messages are the engine's own, such as
/// `no such table: t`; they never contain credentials.
pub fn local_error(failure: turso::Error) -> Error {
    let (status, sqlstate) = classify(local_kind(&failure));
    error(failure.to_string(), status, sqlstate)
}

fn local_kind(failure: &turso::Error) -> &'static str {
    use turso::Error as E;
    match failure {
        E::BatchStatementFailed { error, .. } => local_kind(error),
        E::Constraint(_) => "constraint",
        E::Busy(_) | E::BusySnapshot(_) => "busy",
        E::Interrupt(_) => "interrupt",
        E::Misuse(_) => "misuse",
        E::Readonly(_) => "readonly",
        E::DatabaseFull(_) => "full",
        E::NotAdb(_) | E::Corrupt(_) => "corrupt",
        E::IoError(..) => "io",
        E::ToSqlConversionFailure(_) | E::ConversionFailure(_) => "conversion",
        _ => "error",
    }
}

/// Convert a Turso Cloud error. HTTP failures describe the request, never the
/// auth token, which travels in a header.
pub fn remote_error(failure: turso_serverless::Error) -> Error {
    if let turso_serverless::Error::Http(message) = &failure {
        let message = format!("Turso Cloud request failed: {message}");
        // The client reports the HTTP status and Turso's reason in the text.
        return if message.contains("HTTP status 401") || message.contains("JWT error") {
            error(message, Status::Unauthenticated, b"28000")
        } else if message.contains("HTTP status 403") {
            error(message, Status::Unauthorized, b"42501")
        } else {
            io_error(message)
        };
    }
    let mut kind = remote_kind(&failure);
    // A read-only token's writes fail with a generic error code.
    if kind == "error"
        && failure
            .to_string()
            .contains("write operations are forbidden")
    {
        kind = "readonly";
    }
    let (status, sqlstate) = classify(kind);
    error(failure.to_string(), status, sqlstate)
}

fn remote_kind(failure: &turso_serverless::Error) -> &'static str {
    use turso_serverless::Error as E;
    match failure {
        E::BatchStatementFailed { error, .. } => remote_kind(error),
        E::Constraint(_) => "constraint",
        E::Busy(_) | E::BusySnapshot(_) => "busy",
        E::Interrupt(_) => "interrupt",
        E::Misuse(_) => "misuse",
        E::Readonly(_) => "readonly",
        E::DatabaseFull(_) => "full",
        E::NotAdb(_) | E::Corrupt(_) => "corrupt",
        E::Http(_) => "io",
        E::ToSqlConversionFailure(_) | E::ConversionFailure(_) => "conversion",
        _ => "error",
    }
}

#[cfg(test)]
mod tests {
    use super::Location;

    #[test]
    fn parses_locations() {
        assert_eq!(
            Location::parse("libsql://db-org.turso.io", Some("t".into()), false),
            Location::Remote {
                url: "libsql://db-org.turso.io".into(),
                auth_token: Some("t".into())
            }
        );
        assert!(Location::parse("https://db.example", None, false).is_remote());
        assert_eq!(
            Location::parse("file:data/app.db", None, true),
            Location::Local {
                path: "data/app.db".into(),
                read_only: true
            }
        );
        assert!(!Location::parse(":memory:", None, false).is_remote());
    }
}
