// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! An ADBC service for a Turso database.
//!
//! Any ADBC application reaches the database through the native Grainlift
//! driver; this crate is the Grainlift backend that talks to Turso:
//!
//! - a local database file, through the embedded Turso Database engine
//!   ([`turso`]); or
//! - a Turso Cloud database, over its SQL-over-HTTP protocol
//!   ([`turso_serverless`]).
//!
//! SQL passes straight through to Turso. Results stream in bounded Arrow
//! batches typed from the columns' declared types (see [`types`]).
//! Transactions, parameter binding, bulk ingestion and catalog metadata map to
//! their Turso equivalents.

pub mod connection;
mod cursor;
pub mod db;
mod session;
pub mod statement;
pub mod types;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::OptionValue;
use grainlift_server::backend::{Backend, BackendConnection};
use grainlift_server::config::TargetConfig;

pub use connection::TursoConnection;
pub use db::Location;
pub use statement::TursoStatement;

use crate::db::{Database, Runtime};
use crate::session::Session;

/// Database URL: a `libsql://` or `https://` Turso Cloud URL, or a local file.
pub const DATABASE_URL_VARIABLE: &str = "TURSO_DATABASE_URL";
/// Turso Cloud auth token.
pub const AUTH_TOKEN_VARIABLE: &str = "TURSO_AUTH_TOKEN";
/// Set to `true` to open a local file read-only.
pub const READ_ONLY_VARIABLE: &str = "GRAINLIFT_TURSO_READ_ONLY";
/// Database option carrying the client's own Turso Cloud auth token.
pub const AUTH_TOKEN_OPTION: &str = "turso.auth_token";

/// Serves one Turso database; every client connection gets its own Turso
/// connection and transaction state.
pub struct TursoBackend {
    runtime: Runtime,
    location: Location,
    database: Database,
}

impl TursoBackend {
    /// Open the database at `location` and check that it answers. A Turso
    /// Cloud database without a server token is not checked: its clients
    /// supply their own tokens.
    pub fn open(location: &Location) -> Result<Self> {
        let runtime = Runtime::new()?;
        let opening = location.clone();
        let database = runtime.run(async move {
            let database = Database::open(&opening).await?;
            if !matches!(
                opening,
                Location::Remote {
                    auth_token: None,
                    ..
                }
            ) {
                let connection = database.connect()?;
                connection.execute("SELECT 1".into(), Vec::new()).await?;
                connection.close().await;
            }
            Ok::<_, Error>(database)
        })??;
        Ok(Self {
            runtime,
            location: location.clone(),
            database,
        })
    }

    /// Whether clients must send their own token in [`AUTH_TOKEN_OPTION`].
    pub fn requires_client_token(&self) -> bool {
        matches!(
            self.location,
            Location::Remote {
                auth_token: None,
                ..
            }
        )
    }

    /// The database a client connects to: the server's, or for Turso Cloud
    /// the same URL authenticated with the client's own token.
    fn database_for(&self, database_options: Vec<(String, OptionValue)>) -> Result<Database> {
        let mut token = None;
        for (key, value) in database_options {
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
        match (token, &self.location) {
            (None, _) if self.requires_client_token() => Err(Error::with_message_and_status(
                format!(
                    "This service needs your Turso auth token in the {AUTH_TOKEN_OPTION} option"
                ),
                Status::Unauthenticated,
            )),
            (None, _) => Ok(self.database.clone()),
            (Some(token), Location::Remote { url, .. }) => {
                let location = Location::Remote {
                    url: url.clone(),
                    auth_token: Some(token),
                };
                // Opening a Turso Cloud database makes no request.
                self.runtime
                    .run(async move { Database::open(&location).await })?
            }
            (Some(_), Location::Local { .. }) => Err(Error::with_message_and_status(
                format!("{AUTH_TOKEN_OPTION} applies only to Turso Cloud databases"),
                Status::InvalidArguments,
            )),
        }
    }

    /// The location configured by [`DATABASE_URL_VARIABLE`],
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
                     database file"
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
                    "{READ_ONLY_VARIABLE} applies to local files; for Turso Cloud, use a \
                     read-only auth token"
                ),
                Status::InvalidArguments,
            ));
        }
        Ok(location)
    }
}

impl Backend for TursoBackend {
    /// Open a Turso connection. The server chooses the database; the only
    /// client database option is [`AUTH_TOKEN_OPTION`], and the only
    /// connection option is `adbc.connection.autocommit`.
    fn open(
        &self,
        _target: &TargetConfig,
        database_options: Vec<(String, OptionValue)>,
        connection_options: Vec<(String, OptionValue)>,
    ) -> Result<Box<dyn BackendConnection>> {
        let database = self.database_for(database_options)?;
        let session = Session::new(self.runtime.clone(), database.connect()?);
        let mut connection = TursoConnection::new(session);
        for (key, value) in connection_options {
            connection.set_option(&key, value)?;
        }
        Ok(Box::new(connection))
    }
}
