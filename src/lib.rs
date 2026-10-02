// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! An ADBC service for Turso databases.
//!
//! Any ADBC application reaches a database through the native Grainlift
//! driver; this crate is the Grainlift backend that talks to Turso:
//!
//! - a local database file, on the embedded Turso Database engine; or
//! - a Turso Cloud database, over its SQL-over-HTTP protocol.
//!
//! SQL passes straight through to Turso. Results stream in bounded Arrow
//! batches typed from the columns' declared types (see [`types`]).
//! Transactions, parameter binding, bulk ingestion and catalog metadata map to
//! their Turso equivalents. Every operation has a deadline and can be
//! cancelled (see [`ops`]).
//!
//! [`TursoBackend`] serves one or more named targets. The `grainlift-turso`
//! command hosts it, either for development or in production from a
//! configuration file ([`config`], [`host`]).

pub mod config;
pub mod connection;
mod cursor;
pub mod db;
pub mod host;
pub mod ops;
mod session;
pub mod statement;
pub mod types;

use std::collections::BTreeMap;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::OptionValue;
use grainlift_server::backend::{Backend, BackendConnection};
use grainlift_server::config::TargetConfig;

pub use connection::TursoConnection;
pub use db::{Limits, Location, TransactionMode};
pub use statement::TursoStatement;

use crate::db::{Database, Runtime};
use crate::session::Session;

/// Database URL for the development host: a `libsql://` or `https://` Turso
/// Cloud URL, or a local file.
pub const DATABASE_URL_VARIABLE: &str = "TURSO_DATABASE_URL";
/// Turso Cloud auth token for the development host.
pub const AUTH_TOKEN_VARIABLE: &str = "TURSO_AUTH_TOKEN";
/// Set to `true` to open the development host's local file read-only.
pub const READ_ONLY_VARIABLE: &str = "GRAINLIFT_TURSO_READ_ONLY";
/// Database option carrying the client's own Turso Cloud auth token.
pub const AUTH_TOKEN_OPTION: &str = "turso.auth_token";
/// The development host's target name.
pub const DEV_TARGET: &str = "turso";

/// How one target reaches its database.
#[derive(Clone, Debug)]
pub struct TargetSpec {
    /// Where the database lives.
    pub location: Location,
    /// Whether clients may send their own Turso Cloud token in
    /// [`AUTH_TOKEN_OPTION`].
    pub allow_client_auth_token: bool,
    /// Per-operation limits.
    pub limits: Limits,
    /// How transactions begin.
    pub transactions: TransactionMode,
}

impl TargetSpec {
    /// A target with default limits that accepts client tokens.
    pub fn new(location: Location) -> Self {
        Self {
            location,
            allow_client_auth_token: true,
            limits: Limits::default(),
            transactions: TransactionMode::default(),
        }
    }

    /// Whether clients must send their own token: a Turso Cloud database with
    /// no server token.
    pub fn requires_client_token(&self) -> bool {
        matches!(
            self.location,
            Location::Remote {
                auth_token: None,
                ..
            }
        )
    }
}

struct Target {
    spec: TargetSpec,
    database: Database,
}

/// Serves one or more Turso databases, each as a named Grainlift target.
/// Every client connection gets its own Turso connection and transaction
/// state.
pub struct TursoBackend {
    runtime: Runtime,
    targets: BTreeMap<String, Target>,
}

impl TursoBackend {
    /// Open every target's database and check that it answers. A Turso Cloud
    /// database without a server token is not checked: its clients bring
    /// their own tokens.
    pub fn new(targets: impl IntoIterator<Item = (String, TargetSpec)>) -> Result<Self> {
        let runtime = Runtime::new()?;
        let targets = targets.into_iter().collect::<Vec<_>>();
        check_distinct_files(&targets)?;
        let mut opened = BTreeMap::new();
        for (name, spec) in targets {
            let database = Database::open(&spec.location, &runtime)
                .map_err(|failure| for_target(&name, "could not be opened", failure))?;
            if spec.transactions == TransactionMode::Concurrent && !spec.location.is_remote() {
                return Err(Error::with_message_and_status(
                    format!(
                        "Target {name:?}: concurrent transactions need a Turso Cloud database on \
                         the Turso Database engine"
                    ),
                    Status::InvalidArguments,
                ));
            }
            let target = Target { spec, database };
            if !target.spec.requires_client_token() {
                probe(&target, &runtime)
                    .map_err(|failure| for_target(&name, "did not answer", failure))?;
            }
            tracing::info!(target = %name, database = %target.spec.location.describe(), "Turso target ready");
            opened.insert(name, target);
        }
        if opened.is_empty() {
            return Err(Error::with_message_and_status(
                "Configure at least one Turso target",
                Status::InvalidArguments,
            ));
        }
        Ok(Self {
            runtime,
            targets: opened,
        })
    }

    /// Serve the database at `location` as the [`DEV_TARGET`] target, with
    /// default limits.
    pub fn open(location: &Location) -> Result<Self> {
        Self::new([(DEV_TARGET.to_string(), TargetSpec::new(location.clone()))])
    }

    /// The configured target names.
    pub fn target_names(&self) -> impl Iterator<Item = &str> {
        self.targets.keys().map(String::as_str)
    }

    /// Whether clients of `target` must send their own token.
    pub fn requires_client_token(&self, target: &str) -> bool {
        self.targets
            .get(target)
            .is_some_and(|target| target.spec.requires_client_token())
    }

    /// Check that every target answers; a readiness probe. Targets whose
    /// clients bring their own tokens are not checked.
    pub fn check(&self) -> Result<()> {
        for (name, target) in &self.targets {
            if !target.spec.requires_client_token() {
                probe(target, &self.runtime)
                    .map_err(|failure| for_target(name, "did not answer", failure))?;
            }
        }
        Ok(())
    }

    /// The database a client connects to: the target's own, or for Turso
    /// Cloud the same URL authenticated with the client's token.
    fn database_for(
        &self,
        target: &Target,
        options: Vec<(String, OptionValue)>,
    ) -> Result<Database> {
        let mut token = None;
        for (key, value) in options {
            match (key.as_str(), value) {
                (AUTH_TOKEN_OPTION, OptionValue::String(value)) if !value.is_empty() => {
                    token = Some(value);
                }
                // The value is a credential: never echo it.
                (AUTH_TOKEN_OPTION, _) => {
                    return Err(Error::with_message_and_status(
                        format!("{AUTH_TOKEN_OPTION} must be a non-empty string"),
                        Status::InvalidArguments,
                    ));
                }
                _ => {
                    return Err(Error::with_message_and_status(
                        format!(
                            "Database option {key} is not supported; the server chooses the \
                             database, and clients may set only {AUTH_TOKEN_OPTION}"
                        ),
                        Status::InvalidArguments,
                    ));
                }
            }
        }
        match (token, &target.spec.location) {
            (None, _) if target.spec.requires_client_token() => {
                Err(Error::with_message_and_status(
                    format!(
                        "This service needs your Turso auth token in the {AUTH_TOKEN_OPTION} option"
                    ),
                    Status::Unauthenticated,
                ))
            }
            (None, _) => Ok(target.database.clone()),
            (Some(_), Location::Local { .. }) => Err(Error::with_message_and_status(
                format!("{AUTH_TOKEN_OPTION} applies only to Turso Cloud databases"),
                Status::InvalidArguments,
            )),
            (Some(_), _) if !target.spec.allow_client_auth_token => {
                Err(Error::with_message_and_status(
                    format!("This target does not accept {AUTH_TOKEN_OPTION}"),
                    Status::InvalidArguments,
                ))
            }
            (Some(token), Location::Remote { url, .. }) => Database::open(
                &Location::Remote {
                    url: url.clone(),
                    auth_token: Some(token),
                },
                &self.runtime,
            ),
        }
    }
}

/// Refuse two targets on the same local file. Turso shares one open
/// database per path within a process, ignoring the second opener's flags,
/// so a read-only target could otherwise end up writable.
fn check_distinct_files(targets: &[(String, TargetSpec)]) -> Result<()> {
    let mut seen: BTreeMap<std::path::PathBuf, &str> = BTreeMap::new();
    for (name, spec) in targets {
        let Some(path) = local_file(&spec.location) else {
            continue;
        };
        if let Some(other) = seen.insert(path, name) {
            return Err(Error::with_message_and_status(
                format!("Targets {other:?} and {name:?} use the same database file"),
                Status::InvalidArguments,
            ));
        }
    }
    Ok(())
}

/// The absolute path of a local database file, or `None` for Turso Cloud and
/// `:memory:` (each in-memory database is separate).
pub(crate) fn local_file(location: &Location) -> Option<std::path::PathBuf> {
    match location {
        Location::Local { path, .. } if path != ":memory:" => {
            let path = std::path::Path::new(path);
            Some(
                std::fs::canonicalize(path)
                    .or_else(|_| std::path::absolute(path))
                    .unwrap_or_else(|_| path.to_path_buf()),
            )
        }
        _ => None,
    }
}

fn for_target(name: &str, what: &str, failure: Error) -> Error {
    let mut error = Error::with_message_and_status(
        format!("Target {name:?} {what}: {}", failure.message),
        failure.status,
    );
    error.sqlstate = failure.sqlstate;
    error
}

/// Check that a target answers, and that a `concurrent` target's database
/// supports `BEGIN CONCURRENT` (a libSQL database refuses it).
fn probe(target: &Target, runtime: &Runtime) -> Result<()> {
    let connection =
        target
            .database
            .connect(runtime, target.spec.limits, target.spec.transactions)?;
    let mut result = connection.execute("SELECT 1", Vec::new()).map(drop);
    if result.is_ok() && target.spec.transactions == TransactionMode::Concurrent {
        result = connection
            .begin()
            .and_then(|()| connection.execute("ROLLBACK", Vec::new()).map(drop))
            .map_err(|failure| {
                Error::with_message_and_status(
                    format!(
                        "transaction_mode = \"concurrent\" needs a database on the Turso \
                         Database engine (turso db create --tursodb): {}",
                        failure.message
                    ),
                    Status::InvalidArguments,
                )
            });
    }
    connection.close();
    result
}

impl Backend for TursoBackend {
    /// Open a Turso connection for the target Grainlift names in
    /// `target.driver`. The server chooses the database; the only client
    /// database option is [`AUTH_TOKEN_OPTION`], and the only connection
    /// option is `adbc.connection.autocommit`.
    fn open(
        &self,
        target: &TargetConfig,
        database_options: Vec<(String, OptionValue)>,
        connection_options: Vec<(String, OptionValue)>,
    ) -> Result<Box<dyn BackendConnection>> {
        let entry = self.targets.get(&target.driver).ok_or_else(|| {
            Error::with_message_and_status("The target is not configured", Status::NotFound)
        })?;
        let database = self.database_for(entry, database_options)?;
        let connection =
            database.connect(&self.runtime, entry.spec.limits, entry.spec.transactions)?;
        let mut connection = TursoConnection::new(Session::new(connection));
        for (key, value) in connection_options {
            connection.set_option(&key, value)?;
        }
        tracing::debug!(target = %target.driver, "connection opened");
        Ok(Box::new(connection))
    }
}

/// The development host's location, from [`DATABASE_URL_VARIABLE`],
/// [`AUTH_TOKEN_VARIABLE`] and [`READ_ONLY_VARIABLE`].
pub fn location_from_env() -> Result<Location> {
    let variable = |name| {
        std::env::var(name)
            .ok()
            .filter(|value: &String| !value.is_empty())
    };
    let url = variable(DATABASE_URL_VARIABLE).ok_or_else(|| {
        Error::with_message_and_status(
            format!(
                "Set {DATABASE_URL_VARIABLE} to a Turso Cloud URL (libsql://...) or a local \
                 database file, or run `grainlift-turso serve --config FILE`"
            ),
            Status::InvalidArguments,
        )
    })?;
    let read_only = match variable(READ_ONLY_VARIABLE).as_deref() {
        None | Some("false") => false,
        Some("true") => true,
        Some(_) => {
            return Err(Error::with_message_and_status(
                format!("{READ_ONLY_VARIABLE} must be true or false"),
                Status::InvalidArguments,
            ));
        }
    };
    let location = Location::parse(&url, variable(AUTH_TOKEN_VARIABLE), read_only);
    if read_only && location.is_remote() {
        return Err(Error::with_message_and_status(
            format!(
                "{READ_ONLY_VARIABLE} applies to local files; for Turso Cloud, use a read-only \
                 auth token"
            ),
            Status::InvalidArguments,
        ));
    }
    Ok(location)
}
