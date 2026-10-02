// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! One client connection's Turso connection and transaction mode.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use adbc_core::CancelHandle;
use adbc_core::error::Result;

use crate::db::Connection;

/// A Turso connection shared by one ADBC connection and its statements.
///
/// With autocommit on (the ADBC default), every statement commits on its own.
/// With it off, the first statement opens a transaction that lasts until
/// commit or rollback. Opening it lazily, rather than right after the
/// previous commit, keeps an idle client from holding a Turso Cloud stream
/// open.
pub struct Session {
    connection: Connection,
    manual: AtomicBool,
}

impl Session {
    pub fn new(connection: Connection) -> Self {
        Self {
            connection,
            manual: AtomicBool::new(false),
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
            self.connection.execute("BEGIN", Vec::new())?;
        }
        Ok(&self.connection)
    }

    /// Run `sql` (`COMMIT` or `ROLLBACK`) if a transaction is open.
    pub fn end_transaction(&self, sql: &str) -> Result<()> {
        if !self.connection.is_autocommit()? {
            self.connection.execute(sql, Vec::new())?;
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
