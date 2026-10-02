// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! One client connection's Turso connection and transaction mode.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use adbc_core::CancelHandle;
use adbc_core::error::{Error, Result, Status};

use crate::db::{Connection, error};

/// A Turso connection shared by one ADBC connection and its statements.
///
/// With autocommit on (the ADBC default), every statement commits on its own.
/// With it off, the first statement opens a transaction that lasts until
/// commit or rollback. Opening it lazily, rather than right after the
/// previous commit, keeps an idle client from holding a Turso Cloud stream
/// open.
///
/// Turso can end a transaction itself: a write conflict under
/// `BEGIN CONCURRENT`, or Turso Cloud expiring an idle stream. The session
/// remembers the transaction it opened, so the client then gets an error
/// (from its next statement, or from `commit`) instead of a commit that
/// silently saves nothing.
pub struct Session {
    connection: Connection,
    manual: AtomicBool,
    /// Whether the session opened a transaction that it has not ended.
    open: AtomicBool,
}

impl Session {
    pub fn new(connection: Connection) -> Self {
        Self {
            connection,
            manual: AtomicBool::new(false),
            open: AtomicBool::new(false),
        }
    }

    /// Whether autocommit is off.
    pub fn is_manual(&self) -> bool {
        self.manual.load(Ordering::Acquire)
    }

    pub fn set_manual(&self, manual: bool) {
        self.manual.store(manual, Ordering::Release);
    }

    /// The connection, after opening a transaction when autocommit is off and
    /// none is open. Every statement goes through here.
    pub fn connection(&self) -> Result<&Connection> {
        if self.is_manual() && self.connection.is_autocommit()? {
            if self.open.swap(false, Ordering::AcqRel) {
                return Err(rolled_back());
            }
            self.connection.begin()?;
            self.open.store(true, Ordering::Release);
        }
        Ok(&self.connection)
    }

    /// End the open transaction with `COMMIT` or `ROLLBACK`. Committing a
    /// transaction that Turso already rolled back is an error.
    pub fn end_transaction(&self, sql: &str) -> Result<()> {
        let opened = self.open.swap(false, Ordering::AcqRel);
        if !self.connection.is_autocommit()? {
            self.connection.execute(sql, Vec::new())?;
        } else if opened && sql.eq_ignore_ascii_case("COMMIT") {
            return Err(rolled_back());
        }
        Ok(())
    }

    /// Cancels the running operation on this connection.
    pub fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        self.connection.cancel_handle()
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.connection.close();
    }
}

/// The error for a transaction Turso ended before the client did. SQLSTATE
/// `40001` tells the client to retry it.
fn rolled_back() -> Error {
    error(
        "The transaction was rolled back by Turso before it was committed (a write conflict, \
         or an idle Turso Cloud stream); retry it",
        Status::InvalidState,
        b"40001",
    )
}
