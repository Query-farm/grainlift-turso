// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! One synchronous interface over Turso's two Rust clients.
//!
//! - A local file runs in process on the Turso Database engine, through
//!   [`turso_sdk_kit`] (the layer beneath the `turso` crate). Its synchronous
//!   API suits Grainlift's synchronous backend traits, and it exposes what a
//!   server needs and the `turso` crate hides: interrupting a running
//!   statement and a busy timeout.
//! - Turso Cloud is reached with [`turso_serverless`], which is async, so its
//!   requests run on a [`Runtime`] owned by the backend while the calling
//!   thread waits.
//!
//! Every operation runs under [`Operations`]: a deadline, and cancellation by
//! another thread.

use std::future::Future;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use adbc_core::CancelHandle;
use adbc_core::error::{Error, Result, Status};
pub use turso::Value;
use turso_sdk_kit::rsapi::{
    TursoConnection, TursoDatabase, TursoDatabaseConfig, TursoError, TursoStatement,
    TursoStatusCode,
};

use crate::cursor::{Cursor, Endpoint};
use crate::ops::{Interruption, Operations, interruption_error};

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

    /// A description safe to log: the host of a Turso Cloud URL, or the path
    /// of a local file. Never the token.
    pub fn describe(&self) -> String {
        match self {
            Self::Local { path, read_only } => {
                format!(
                    "local file {path}{}",
                    if *read_only { " (read-only)" } else { "" }
                )
            }
            Self::Remote { url, .. } => {
                let host = url.split("://").nth(1).unwrap_or(url);
                let host = host.split(['/', '?']).next().unwrap_or(host);
                format!("Turso Cloud {host}")
            }
        }
    }
}

/// Per-operation limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    /// Longest one operation may run before it is stopped with ADBC `TIMEOUT`.
    pub operation_timeout: Duration,
    /// How long a local write waits for another connection's lock before it
    /// fails as busy.
    pub busy_timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            operation_timeout: Duration::from_secs(60),
            busy_timeout: Duration::from_secs(5),
        }
    }
}

/// The Tokio runtime that drives Turso Cloud requests and the local
/// watchdogs.
///
/// Grainlift may call a backend from inside its own Tokio runtime, where
/// blocking on another runtime is not allowed, so work is spawned here and the
/// caller waits with a runtime-agnostic executor.
#[derive(Clone)]
pub struct Runtime(Arc<OwnedRuntime>);

/// Shuts its runtime down without blocking when dropped, so the backend can
/// be dropped anywhere, including inside another runtime (such as at the end
/// of the production host).
struct OwnedRuntime(Option<tokio::runtime::Runtime>);

impl OwnedRuntime {
    fn get(&self) -> &tokio::runtime::Runtime {
        self.0.as_ref().expect("the runtime lives until dropped")
    }
}

impl Drop for OwnedRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

impl Runtime {
    /// Start a multi-threaded runtime.
    pub fn new() -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("grainlift-turso")
            .enable_all()
            .build()
            .map_err(|error| io_error(format!("Could not start the Turso runtime: {error}")))?;
        Ok(Self(Arc::new(OwnedRuntime(Some(runtime)))))
    }

    /// Run `future` to completion on this runtime and return its output.
    pub fn run<F>(&self, future: F) -> Result<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        futures::executor::block_on(self.0.get().spawn(future)).map_err(|_| {
            Error::with_message_and_status("A Turso operation failed", Status::Internal)
        })
    }

    /// Run `future` in the background, without waiting for it.
    pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.0.get().spawn(future)
    }
}

/// An open database.
#[derive(Clone)]
pub enum Database {
    /// A local database, and whether it is read-only.
    Local(Arc<TursoDatabase>, bool),
    Remote(turso_serverless::Database, Endpoint),
}

impl Database {
    /// Open the database at `location`. Opening a Turso Cloud database makes
    /// no request.
    pub fn open(location: &Location, runtime: &Runtime) -> Result<Self> {
        match location {
            Location::Local { path, read_only } => {
                let database = TursoDatabase::new(TursoDatabaseConfig {
                    path: path.clone(),
                    experimental_features: None,
                    // The engine runs its own I/O, so every call completes
                    // synchronously on the calling thread.
                    async_io: false,
                    encryption: None,
                    vfs: turso_sdk_kit::IoBackend::Default,
                    io: None,
                    db_file: None,
                    page_codec: None,
                    open_flags: if *read_only {
                        turso_core::OpenFlags::ReadOnly
                    } else {
                        turso_core::OpenFlags::default()
                    },
                });
                if database.open().map_err(local_error)?.is_io() {
                    return Err(io_error("The Turso database did not finish opening".into()));
                }
                Ok(Self::Local(database, *read_only))
            }
            Location::Remote { url, auth_token } => {
                let mut builder = turso_serverless::Builder::new_remote(url.clone());
                if let Some(token) = auth_token {
                    builder = builder.with_auth_token(token.clone());
                }
                let database = runtime
                    .run(async move { builder.build().await })?
                    .map_err(remote_error)?;
                Ok(Self::Remote(
                    database,
                    Endpoint::new(url, auth_token.clone())?,
                ))
            }
        }
    }

    /// Open a new connection, with its own transaction state.
    pub fn connect(&self, runtime: &Runtime, limits: Limits) -> Result<Connection> {
        let (kind, operations) = match self {
            Self::Local(database, read_only) => {
                let connection = database.connect().map_err(local_error)?;
                connection.set_busy_timeout(limits.busy_timeout);
                if *read_only {
                    // Defense in depth: Turso reuses an already-open database
                    // for the same path without checking its read-only flag.
                    let mut statement = connection
                        .prepare_single("PRAGMA query_only = 1")
                        .map_err(local_error)?;
                    execute_local(&mut statement)?;
                }
                let operations =
                    Operations::new(Some(Arc::clone(&connection)), limits.operation_timeout);
                (Kind::Local(connection), operations)
            }
            Self::Remote(database, endpoint) => {
                let connection = database.connect().map_err(remote_error)?;
                let operations = Operations::new(None, limits.operation_timeout);
                (Kind::Remote(connection, endpoint.clone()), operations)
            }
        };
        Ok(Connection {
            kind,
            runtime: runtime.clone(),
            operations,
            open: Arc::default(),
        })
    }
}

/// A result column: its name and, when it reads a table column directly, the
/// type that column was declared with.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Column {
    pub name: String,
    pub decl_type: Option<String>,
}

#[derive(Clone)]
enum Kind {
    Local(Arc<TursoConnection>),
    Remote(turso_serverless::Connection, Endpoint),
}

/// A local result's statement, shared with its connection so that an
/// interrupt can close it.
type SharedStatement = Arc<StatementSlot>;
type StatementSlot = Mutex<Option<Box<TursoStatement>>>;

/// One connection. Clones share the connection, its transaction and its
/// operation limits.
#[derive(Clone)]
pub struct Connection {
    kind: Kind,
    runtime: Runtime,
    operations: Arc<Operations>,
    /// The connection's open local results.
    open: Arc<Mutex<Vec<Weak<StatementSlot>>>>,
}

impl Connection {
    /// Cancels this connection's running operation.
    pub fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::clone(&self.operations) as Arc<dyn CancelHandle>
    }

    /// Run `work` on the local engine under a deadline: a watchdog interrupts
    /// the running statement when it expires.
    ///
    /// The engine honors an interrupt only while a statement is stepping, so
    /// the watchdog repeats it until the operation ends. Its flag stays set
    /// while any statement on the connection is active, so after an
    /// interrupted operation the connection's open results are closed: left
    /// open, they would make every later statement fail as interrupted.
    fn local<T>(
        &self,
        connection: &TursoConnection,
        work: impl FnOnce(&TursoConnection) -> Result<T>,
    ) -> Result<T> {
        let id = self.operations.begin();
        let operations = Arc::clone(&self.operations);
        let timeout = operations.timeout();
        let watchdog = self.runtime.spawn(async move {
            tokio::time::sleep(timeout).await;
            if operations.interrupt(Some(id), Interruption::TimedOut) {
                tracing::warn!(
                    timeout_seconds = timeout.as_secs_f64(),
                    "Turso operation timed out"
                );
            }
            loop {
                tokio::time::sleep(Duration::from_millis(50)).await;
                if !operations.interrupt(Some(id), Interruption::TimedOut) {
                    break;
                }
            }
        });
        let result = work(connection);
        watchdog.abort();
        match (result, self.operations.end(id)) {
            (result, Some(why)) => {
                self.close_open_results();
                match result {
                    Err(_) => Err(interruption_error(why, timeout)),
                    finished => finished,
                }
            }
            (result, None) => result,
        }
    }

    /// Track a local result's statement.
    fn register(&self, statement: Box<TursoStatement>) -> SharedStatement {
        let shared = Arc::new(Mutex::new(Some(statement)));
        let mut open = self
            .open
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        open.retain(|statement| statement.strong_count() > 0);
        open.push(Arc::downgrade(&shared));
        shared
    }

    /// Close every open local result, releasing its statement.
    fn close_open_results(&self) {
        let open = std::mem::take(
            &mut *self
                .open
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()),
        );
        for statement in open.iter().filter_map(Weak::upgrade) {
            statement
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .take();
        }
    }

    /// Run a Turso Cloud request under a deadline and cancellation.
    fn remote<T, F>(&self, request: F) -> Result<T>
    where
        F: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        let id = self.operations.begin();
        let operations = Arc::clone(&self.operations);
        let timeout = operations.timeout();
        let result = self.runtime.run(async move {
            tokio::select! {
                result = request => result,
                () = tokio::time::sleep(timeout) => {
                    operations.interrupt(Some(id), Interruption::TimedOut);
                    tracing::warn!(timeout_seconds = timeout.as_secs_f64(), "Turso Cloud request timed out");
                    Err(interruption_error(Interruption::TimedOut, timeout))
                }
                why = operations.interrupted(id) => Err(interruption_error(why, timeout)),
            }
        });
        self.operations.end(id);
        result?
    }

    /// Run a query and return its rows, which are read on demand.
    pub fn query(&self, sql: String, params: Vec<Value>) -> Result<Rows> {
        let inner = match &self.kind {
            Kind::Local(connection) => self.local(connection, |connection| {
                let mut statement = connection.prepare_single(&sql).map_err(local_error)?;
                bind(&mut statement, params)?;
                Ok(RowsInner::Local(self.register(statement)))
            })?,
            // A transaction lives on the connection's server-side stream, so
            // its queries go through that stream, and arrive whole.
            Kind::Remote(connection, _) if !connection.is_autocommit().map_err(remote_error)? => {
                let connection = connection.clone();
                let rows = self.remote(async move {
                    connection
                        .query(sql, remote_params(params))
                        .await
                        .map_err(remote_error)
                })?;
                RowsInner::Buffered(rows)
            }
            Kind::Remote(_, endpoint) => {
                let endpoint = endpoint.clone();
                let cursor =
                    self.remote(async move { Cursor::open(&endpoint, sql, params).await })?;
                RowsInner::Stream(Box::new(cursor))
            }
        };
        let columns = match &inner {
            RowsInner::Local(statement) => {
                let statement = statement
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner());
                match statement.as_ref() {
                    Some(statement) => local_columns(statement)?,
                    None => return Err(closed_result_error()),
                }
            }
            RowsInner::Buffered(rows) => rows
                .columns()
                .into_iter()
                .map(|column| Column {
                    name: column.name,
                    decl_type: column.decl_type,
                })
                .collect(),
            RowsInner::Stream(cursor) => cursor.columns(),
            RowsInner::Finished => Vec::new(),
        };
        Ok(Rows {
            connection: self.clone(),
            inner,
            columns,
        })
    }

    /// Run a statement and return the number of rows it changed. Rows it
    /// returns, as with `RETURNING` or a `SELECT`, are discarded.
    pub fn execute(&self, sql: &str, params: Vec<Value>) -> Result<u64> {
        match &self.kind {
            Kind::Local(connection) => self.local(connection, |connection| {
                let mut statement = connection.prepare_single(sql).map_err(local_error)?;
                bind(&mut statement, params)?;
                execute_local(&mut statement)
            }),
            Kind::Remote(connection, _) => {
                let connection = connection.clone();
                let sql = sql.to_string();
                self.remote(async move {
                    connection
                        .execute(sql, remote_params(params))
                        .await
                        .map_err(remote_error)
                })
            }
        }
    }

    /// Run one statement once per row of parameters, in order, and return the
    /// total number of rows changed. Stops at the first failure; the caller
    /// owns the transaction around it.
    pub fn execute_each(&self, sql: &str, rows: Vec<Vec<Value>>) -> Result<u64> {
        match &self.kind {
            Kind::Local(connection) => self.local(connection, |connection| {
                let mut statement = connection.prepare_single(sql).map_err(local_error)?;
                let mut changed = 0;
                for params in rows {
                    statement.reset().map_err(local_error)?;
                    bind(&mut statement, params)?;
                    changed += execute_local(&mut statement)?;
                }
                Ok(changed)
            }),
            Kind::Remote(connection, _) => {
                let connection = connection.clone();
                let sql = sql.to_string();
                self.remote(async move {
                    let statements = rows.into_iter().map(|params| (sql.clone(), params));
                    remote_batches(&connection, statements).await
                })
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
    pub fn insert_rows(&self, insert: &str, width: usize, rows: Vec<Vec<Value>>) -> Result<u64> {
        let tuple = format!("({})", vec!["?"; width].join(", "));
        match &self.kind {
            Kind::Local(_) => self.execute_each(&format!("{insert} {tuple}"), rows),
            Kind::Remote(connection, _) => {
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
                let connection = connection.clone();
                self.remote(async move { remote_batches(&connection, statements).await })
            }
        }
    }

    /// Prepare `sql` without running it, and describe its result columns.
    pub fn describe(&self, sql: &str) -> Result<Vec<Column>> {
        match &self.kind {
            Kind::Local(connection) => self.local(connection, |connection| {
                let statement = connection.prepare_single(sql).map_err(local_error)?;
                local_columns(&statement)
            }),
            Kind::Remote(connection, _) => {
                let connection = connection.clone();
                let sql = sql.to_string();
                self.remote(async move {
                    let statement = connection.prepare(&sql).await.map_err(remote_error)?;
                    Ok(statement
                        .columns()
                        .into_iter()
                        .map(|column| Column {
                            name: column.name,
                            decl_type: column.decl_type,
                        })
                        .collect())
                })
            }
        }
    }

    /// Whether no transaction is open.
    pub fn is_autocommit(&self) -> Result<bool> {
        match &self.kind {
            Kind::Local(connection) => Ok(connection.get_auto_commit()),
            Kind::Remote(connection, _) => connection.is_autocommit().map_err(remote_error),
        }
    }

    /// Run `work` atomically: inside the connection's open transaction, or
    /// else in a transaction of its own.
    pub fn atomically<T>(&self, work: impl FnOnce(&Self) -> Result<T>) -> Result<T> {
        if !self.is_autocommit()? {
            return work(self);
        }
        self.execute("BEGIN", Vec::new())?;
        match work(self) {
            Ok(value) => {
                self.execute("COMMIT", Vec::new())?;
                Ok(value)
            }
            Err(failure) => {
                // The original failure matters more than a failed rollback.
                if !self.is_autocommit().unwrap_or(true) {
                    let _ = self.execute("ROLLBACK", Vec::new());
                }
                Err(failure)
            }
        }
    }

    /// Release the connection. A Turso Cloud stream is closed in the
    /// background, rolling back any open transaction; nothing waits for it.
    /// A local connection closes when its last handle is dropped.
    pub fn close(&self) {
        if let Kind::Remote(connection, _) = &self.kind {
            let connection = connection.clone();
            self.runtime.spawn(async move {
                // Turso Cloud also expires idle streams, so a failure is harmless.
                let _ = tokio::time::timeout(Duration::from_secs(10), connection.close()).await;
            });
        }
    }
}

enum RowsInner {
    Local(SharedStatement),
    Buffered(turso_serverless::Rows),
    Stream(Box<Cursor>),
    Finished,
}

/// The rows of a running query, read on demand: the embedded engine steps its
/// cursor, Turso Cloud streams its cursor endpoint, and inside a Turso Cloud
/// transaction the result arrives whole.
pub struct Rows {
    connection: Connection,
    inner: RowsInner,
    columns: Vec<Column>,
}

impl Rows {
    /// The result columns.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Read the next rows, up to `max_rows` rows or until they reach about
    /// `max_bytes` (always at least one row). The flag is true once the result
    /// is exhausted, and the cursor is then released.
    pub fn next_rows(
        &mut self,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<(Vec<Vec<Value>>, bool)> {
        match std::mem::replace(&mut self.inner, RowsInner::Finished) {
            RowsInner::Finished => Ok((Vec::new(), true)),
            RowsInner::Local(statement) => {
                let Kind::Local(connection) = &self.connection.kind else {
                    unreachable!("local rows belong to a local connection")
                };
                let read = self.connection.local(connection, |_| {
                    let mut guard = statement
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner());
                    match guard.as_mut() {
                        Some(statement) => read_local(statement, max_rows, max_bytes),
                        None => Err(closed_result_error()),
                    }
                });
                if let Ok((_, false)) = read {
                    self.inner = RowsInner::Local(statement);
                } else {
                    // Finished or failed: release the statement now.
                    statement
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .take();
                }
                read
            }
            RowsInner::Buffered(mut rows) => {
                let read =
                    futures::executor::block_on(read_buffered(&mut rows, max_rows, max_bytes));
                if let Ok((_, false)) = read {
                    self.inner = RowsInner::Buffered(rows);
                }
                read
            }
            RowsInner::Stream(mut cursor) => {
                let read = self.connection.remote(async move {
                    let read = cursor.next_rows(max_rows, max_bytes).await;
                    read.map(|(rows, done)| (rows, done, cursor))
                });
                match read {
                    Ok((rows, done, cursor)) => {
                        if !done {
                            self.inner = RowsInner::Stream(cursor);
                        }
                        Ok((rows, done))
                    }
                    Err(failure) => Err(failure),
                }
            }
        }
    }
}

/// The error a result closed by an interrupt reports.
fn closed_result_error() -> Error {
    error(
        "This result was closed because an operation on its connection was interrupted",
        Status::Cancelled,
        b"HY008",
    )
}

fn bind(statement: &mut TursoStatement, params: Vec<Value>) -> Result<()> {
    for (index, value) in params.into_iter().enumerate() {
        statement
            .bind_positional(index + 1, value.into())
            .map_err(local_error)?;
    }
    Ok(())
}

fn local_columns(statement: &TursoStatement) -> Result<Vec<Column>> {
    (0..statement.column_count())
        .map(|index| {
            Ok(Column {
                name: statement.column_name(index).map_err(local_error)?,
                decl_type: statement.column_decltype(index),
            })
        })
        .collect()
}

/// Step a local statement to completion, discarding any rows, and return the
/// number of rows it changed.
fn execute_local(statement: &mut TursoStatement) -> Result<u64> {
    loop {
        match statement.step(None).map_err(local_error)? {
            TursoStatusCode::Row => {}
            TursoStatusCode::Done => return Ok(statement.n_change().max(0) as u64),
            TursoStatusCode::Io => statement.run_io().map_err(local_error)?,
        }
    }
}

fn read_local(
    statement: &mut TursoStatement,
    max_rows: usize,
    max_bytes: usize,
) -> Result<(Vec<Vec<Value>>, bool)> {
    let mut rows = Vec::new();
    let mut bytes = 0;
    while rows.len() < max_rows && bytes < max_bytes {
        match statement.step(None).map_err(local_error)? {
            TursoStatusCode::Row => {
                let row = (0..statement.column_count())
                    .map(|index| {
                        statement
                            .row_value(index)
                            .map(Value::from)
                            .map_err(local_error)
                    })
                    .collect::<Result<Vec<_>>>()?;
                bytes += crate::types::row_bytes(&row);
                rows.push(row);
            }
            TursoStatusCode::Done => return Ok((rows, true)),
            TursoStatusCode::Io => statement.run_io().map_err(local_error)?,
        }
    }
    Ok((rows, false))
}

async fn read_buffered(
    rows: &mut turso_serverless::Rows,
    max_rows: usize,
    max_bytes: usize,
) -> Result<(Vec<Vec<Value>>, bool)> {
    let mut read = Vec::new();
    let mut bytes = 0;
    while read.len() < max_rows && bytes < max_bytes {
        let Some(row) = rows.next().await.map_err(remote_error)? else {
            return Ok((read, true));
        };
        let row = (0..row.column_count())
            .map(|index| row.get_value(index).map(from_remote).map_err(remote_error))
            .collect::<Result<Vec<_>>>()?;
        bytes += crate::types::row_bytes(&row);
        read.push(row);
    }
    Ok((read, false))
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

pub(crate) fn io_error(message: String) -> Error {
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
pub fn local_error(failure: TursoError) -> Error {
    let kind = match &failure {
        TursoError::Error(message) if message.contains("query_only mode") => "readonly",
        TursoError::Constraint(_) => "constraint",
        TursoError::Busy(_) | TursoError::BusySnapshot(_) => "busy",
        TursoError::Interrupt(_) => "interrupt",
        TursoError::Misuse(_) => "misuse",
        TursoError::Readonly(_) => "readonly",
        TursoError::DatabaseFull(_) => "full",
        TursoError::NotAdb(_) | TursoError::Corrupt(_) => "corrupt",
        TursoError::IoError(..) => "io",
        TursoError::Error(_) => "error",
    };
    let (status, sqlstate) = classify(kind);
    error(local_message(&failure), status, sqlstate)
}

fn local_message(failure: &TursoError) -> String {
    match failure {
        TursoError::Busy(message)
        | TursoError::BusySnapshot(message)
        | TursoError::Interrupt(message)
        | TursoError::Error(message)
        | TursoError::Misuse(message)
        | TursoError::Constraint(message)
        | TursoError::Readonly(message)
        | TursoError::DatabaseFull(message)
        | TursoError::NotAdb(message)
        | TursoError::Corrupt(message) => message.clone(),
        TursoError::IoError(kind, operation) => format!("I/O error ({operation}): {kind}"),
    }
}

/// Convert a Turso Cloud error. HTTP failures describe the request, never the
/// auth token, which travels in a header.
///
/// Two cases are recognized by their text, because `turso_serverless` reports
/// them only as text: an HTTP status (its `HTTP status NNN` wording, which
/// [`crate::cursor`] reproduces), and a read-only token's refused write.
/// `tests/backend.rs` pins both wordings for the locked client version.
pub fn remote_error(failure: turso_serverless::Error) -> Error {
    if let turso_serverless::Error::Http(message) = &failure {
        let message = format!("Turso Cloud request failed: {message}");
        return if message.contains("HTTP status 401") || message.contains("JWT error") {
            error(message, Status::Unauthenticated, b"28000")
        } else if message.contains("HTTP status 403") {
            error(message, Status::Unauthorized, b"42501")
        } else {
            io_error(message)
        };
    }
    let mut kind = remote_kind(&failure);
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
    use super::*;

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

    #[test]
    fn descriptions_never_include_tokens() {
        let remote = Location::parse(
            "libsql://db-org.turso.io/path?x=1",
            Some("secret".into()),
            false,
        );
        assert_eq!(remote.describe(), "Turso Cloud db-org.turso.io");
        assert_eq!(
            Location::parse("app.db", None, true).describe(),
            "local file app.db (read-only)"
        );
    }

    #[test]
    fn classifies_turso_cloud_errors_by_their_wording() {
        use turso_serverless::Error as E;
        let status = |failure| remote_error(failure).status;
        assert_eq!(
            status(E::Http("HTTP status 401 Unauthorized".into())),
            Status::Unauthenticated
        );
        assert_eq!(
            status(E::Http(
                "HTTP status 400 Bad Request: JWT error: InvalidToken".into()
            )),
            Status::Unauthenticated
        );
        assert_eq!(
            status(E::Http("HTTP status 403 Forbidden".into())),
            Status::Unauthorized
        );
        assert_eq!(
            status(E::Http("request failed: connection refused".into())),
            Status::IO
        );
        assert_eq!(
            status(E::Error(
                "Operation was blocked: SQL write operations are forbidden (current session doesn't have write permission)"
                    .into()
            )),
            Status::Unauthorized
        );
        assert_eq!(
            status(E::Constraint("UNIQUE constraint failed".into())),
            Status::Integrity
        );
    }
}
