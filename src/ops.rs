// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Deadlines and cancellation for the operations on one connection.
//!
//! Grainlift calls a backend from one worker thread per session, and its own
//! operation timeout only stops *waiting*: a stuck call keeps running and the
//! session stays wedged behind it. So every Turso operation here (one execute,
//! one fetched batch, one metadata query) gets its own deadline, and a cancel
//! request interrupts whichever operation is running.
//!
//! - The embedded engine is interrupted with `TursoConnection::interrupt`,
//!   which aborts the running statement at its next step, like
//!   `sqlite3_interrupt`. A watchdog task requests it at the deadline. The
//!   engine's own per-statement timeout is not used: it starts with a
//!   statement's first step and would interrupt a client that is still reading
//!   a large result slowly.
//! - Turso Cloud requests are futures, so the deadline and cancellation simply
//!   drop them, which aborts the HTTP request.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use adbc_core::CancelHandle;
use adbc_core::error::{Error, Result, Status};
use turso_sdk_kit::rsapi::TursoConnection;

use crate::db::error;

/// Why an operation was interrupted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Interruption {
    TimedOut,
    Cancelled,
}

#[derive(Default)]
struct Current {
    /// The running operation, if any.
    running: Option<u64>,
    /// Why the running operation was interrupted.
    interruption: Option<Interruption>,
}

/// The operations of one connection: at most one runs at a time, because
/// Grainlift serializes calls on a session.
pub struct Operations {
    current: Mutex<Current>,
    next: AtomicU64,
    notify: tokio::sync::Notify,
    /// The embedded-engine connection to interrupt, for a local database.
    local: Option<Arc<TursoConnection>>,
    timeout: Duration,
}

impl Operations {
    pub fn new(local: Option<Arc<TursoConnection>>, timeout: Duration) -> Arc<Self> {
        Arc::new(Self {
            current: Mutex::new(Current::default()),
            next: AtomicU64::new(1),
            notify: tokio::sync::Notify::new(),
            local,
            timeout,
        })
    }

    /// The deadline of each operation.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Start an operation and return its id.
    pub fn begin(&self) -> u64 {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let mut current = self
            .current
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        current.running = Some(id);
        current.interruption = None;
        id
    }

    /// Finish operation `id`, returning why it was interrupted, if it was.
    pub fn end(&self, id: u64) -> Option<Interruption> {
        let mut current = self
            .current
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if current.running != Some(id) {
            return None;
        }
        current.running = None;
        current.interruption.take()
    }

    /// Interrupt operation `id` (or whichever is running, for `None`). The
    /// first reason recorded wins. Returns whether an operation was running.
    pub fn interrupt(&self, id: Option<u64>, why: Interruption) -> bool {
        let mut current = self
            .current
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let Some(running) = current.running else {
            return false;
        };
        if id.is_some_and(|id| id != running) {
            return false;
        }
        current.interruption.get_or_insert(why);
        // Interrupt under the lock, so a finished operation is never hit by
        // an interrupt meant for it.
        if let Some(connection) = &self.local {
            connection.interrupt();
        }
        drop(current);
        self.notify.notify_waiters();
        true
    }

    /// Resolves once operation `id` is interrupted.
    pub async fn interrupted(&self, id: u64) -> Interruption {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let current = self
                    .current
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner());
                if current.running == Some(id)
                    && let Some(why) = current.interruption
                {
                    return why;
                }
            }
            notified.await;
        }
    }
}

impl CancelHandle for Operations {
    /// Cancel the running operation, if any. Nonblocking and thread-safe, as
    /// Grainlift requires.
    fn try_cancel(&self) -> Result<()> {
        self.interrupt(None, Interruption::Cancelled);
        Ok(())
    }
}

/// The error an interrupted operation reports.
pub fn interruption_error(why: Interruption, timeout: Duration) -> Error {
    match why {
        Interruption::TimedOut => error(
            format!(
                "The Turso operation exceeded its {}s deadline and was stopped",
                timeout.as_secs_f64()
            ),
            Status::Timeout,
            b"HYT00",
        ),
        Interruption::Cancelled => error(
            "The Turso operation was cancelled",
            Status::Cancelled,
            b"HY008",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interrupts_only_the_running_operation() {
        let operations = Operations::new(None, Duration::from_secs(1));
        assert!(
            !operations.interrupt(None, Interruption::Cancelled),
            "nothing running"
        );
        let first = operations.begin();
        assert_eq!(operations.end(first), None);
        // A late watchdog for a finished operation does nothing.
        assert!(!operations.interrupt(Some(first), Interruption::TimedOut));
        let second = operations.begin();
        assert!(!operations.interrupt(Some(first), Interruption::TimedOut));
        assert!(operations.interrupt(None, Interruption::Cancelled));
        assert!(operations.interrupt(Some(second), Interruption::TimedOut));
        assert_eq!(
            operations.end(second),
            Some(Interruption::Cancelled),
            "first reason wins"
        );
    }

    #[test]
    fn interrupted_resolves_for_its_operation() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let operations = Operations::new(None, Duration::from_secs(1));
        let id = operations.begin();
        let waiting = Arc::clone(&operations);
        runtime.block_on(async move {
            let waiter = tokio::spawn(async move { waiting.interrupted(id).await });
            tokio::task::yield_now().await;
            operations.try_cancel().unwrap();
            assert_eq!(waiter.await.unwrap(), Interruption::Cancelled);
        });
    }
}
