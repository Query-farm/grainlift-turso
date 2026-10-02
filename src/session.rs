// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! One client connection's Turso connection and transaction mode.

use std::sync::atomic::{AtomicBool, Ordering};

use adbc_core::error::Result;

use crate::db::{Connection, Runtime};

/// A Turso connection shared by one ADBC connection and its statements.
///
/// With autocommit on (the ADBC default), every statement commits on its own.
/// With it off, the first statement opens a transaction that lasts until
/// commit or rollback. Opening it lazily, rather than right after the
/// previous commit, keeps an idle client from holding a Turso Cloud stream
/// open.
pub struct Session {
    runtime: Runtime,
    connection: Connection,
    manual: AtomicBool,
}

impl Session {
    pub fn new(runtime: Runtime, connection: Connection) -> Self {
        Self {
            runtime,
            connection,
            manual: AtomicBool::new(false),
        }
    }

    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    /// Whether autocommit is off.
    pub fn is_manual(&self) -> bool {
        self.manual.load(Ordering::Acquire)
    }

    pub fn set_manual(&self, manual: bool) {
        self.manual.store(manual, Ordering::Release);
    }

    /// Run `work` on the connection, first opening a transaction when
    /// autocommit is off and none is open.
    pub fn run<T, F>(&self, work: impl FnOnce(Connection) -> F) -> Result<T>
    where
        F: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        let connection = self.connection.clone();
        let manual = self.is_manual();
        let work = work(connection.clone());
        self.runtime.run(async move {
            if manual && connection.is_autocommit()? {
                connection.execute("BEGIN".into(), Vec::new()).await?;
            }
            work.await
        })?
    }

    /// Run `sql` (`COMMIT` or `ROLLBACK`) if a transaction is open.
    pub fn end_transaction(&self, sql: &'static str) -> Result<()> {
        let connection = self.connection.clone();
        self.runtime.run(async move {
            if !connection.is_autocommit()? {
                connection.execute(sql.into(), Vec::new()).await?;
            }
            Ok(())
        })?
    }

    /// Release the connection's server-side resources.
    pub fn close(&self) {
        let connection = self.connection.clone();
        // Nothing useful can be done with a failure while closing.
        let _ = self.runtime.run(async move { connection.close().await });
    }
}
