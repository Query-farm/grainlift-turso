// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Production configuration.
//!
//! The file is Grainlift's own server configuration (`[server]`, `[auth]`,
//! `[tcp]`, with the same fields, defaults and validation as the
//! `grainlift-server` binary), with a `[targets.NAME]` table per Turso
//! database in place of Grainlift's ADBC driver targets:
//!
//! ```toml
//! [server]
//! listen = "127.0.0.1:8080"
//!
//! [auth.static_bearer_tokens]
//! "replace-with-a-long-random-token" = "analytics"
//!
//! [targets.app]
//! url = "libsql://app-myorg.turso.io"
//! auth_token_env = "TURSO_APP_TOKEN"
//! ```
//!
//! Turso tokens never live in the file: a target names the environment
//! variable (`auth_token_env`) or file (`auth_token_file`) that holds one.

use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::Duration;

use grainlift_server::config::{AuthConfig, ServerConfig, TargetConfig, TcpConfig};
use serde::Deserialize;

use crate::{AUTH_TOKEN_OPTION, Limits, Location, TargetSpec, TransactionMode};

/// A production configuration file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Listener, timeouts and session limits (Grainlift's `[server]`).
    #[serde(default)]
    pub server: ServerConfig,
    /// Client authentication and per-principal target permissions
    /// (Grainlift's `[auth]`).
    #[serde(default)]
    pub auth: AuthConfig,
    /// An optional raw TCP or mTLS listener (Grainlift's `[tcp]`).
    pub tcp: Option<TcpConfig>,
    /// The Turso databases, by target name.
    #[serde(default)]
    pub targets: BTreeMap<String, TargetSettings>,
}

/// One Turso database.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetSettings {
    /// A `libsql://` or `https://` Turso Cloud URL, or a local file path.
    pub url: String,
    /// Environment variable holding the Turso Cloud auth token.
    pub auth_token_env: Option<String>,
    /// File holding the Turso Cloud auth token, such as a mounted secret.
    pub auth_token_file: Option<PathBuf>,
    /// Open a local file read-only. For Turso Cloud, use a read-only token.
    #[serde(default)]
    pub read_only: bool,
    /// Let clients send their own Turso Cloud token in `turso.auth_token`.
    #[serde(default)]
    pub allow_client_auth_token: bool,
    /// Longest one Turso operation may run before it is stopped. Must be
    /// shorter than `server.driver_operation_timeout_seconds`.
    #[serde(default = "default_operation_timeout_seconds")]
    pub operation_timeout_seconds: u64,
    /// How long a local write waits for another connection's lock.
    #[serde(default = "default_busy_timeout_ms")]
    pub busy_timeout_ms: u64,
    /// `deferred` (`BEGIN`), or `concurrent` (`BEGIN CONCURRENT`, for Turso
    /// Cloud databases on the Turso Database engine).
    #[serde(default)]
    pub transaction_mode: TransactionMode,
}

const fn default_operation_timeout_seconds() -> u64 {
    60
}

const fn default_busy_timeout_ms() -> u64 {
    5_000
}

impl Config {
    /// Read and validate a configuration file.
    pub fn from_path(path: &Path) -> Result<Self, Box<dyn Error>> {
        let contents = std::fs::read_to_string(path)
            .map_err(|error| format!("could not read {}: {error}", path.display()))?;
        Self::from_toml(&contents)
    }

    /// Parse and validate a configuration.
    pub fn from_toml(contents: &str) -> Result<Self, Box<dyn Error>> {
        // TOML errors quote the input, which can contain credentials (static
        // bearer tokens). Report only the location.
        let config: Self = toml::from_str(contents).map_err(|error: toml::de::Error| {
            let message = "invalid configuration syntax or field";
            match error.span() {
                Some(span) => format!("{message} at byte {}", span.start),
                None => message.to_owned(),
            }
        })?;
        config.validate()?;
        Ok(config)
    }

    /// Check the configuration: Grainlift's own rules for the server, auth and
    /// TCP sections, and the Turso rules for each target.
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        self.grainlift().validate()?;
        let driver_timeout = self.server.driver_operation_timeout_seconds;
        let mut files = BTreeMap::new();
        for (name, target) in &self.targets {
            if let Some(file) = crate::local_file(&Location::parse(&target.url, None, false))
                && let Some(other) = files.insert(file, name)
            {
                return Err(format!(
                    "targets.{other} and targets.{name} use the same database file"
                )
                .into());
            }
        }
        for (name, target) in &self.targets {
            let fail = |message: String| -> Result<(), Box<dyn Error>> {
                Err(format!("targets.{name}: {message}").into())
            };
            if name.is_empty() || name.chars().any(char::is_whitespace) {
                return Err(
                    format!("target name {name:?} must be non-empty without spaces").into(),
                );
            }
            if target.url.trim().is_empty() {
                return fail("url must not be empty".into());
            }
            let location = Location::parse(&target.url, None, target.read_only);
            if target.auth_token_env.is_some() && target.auth_token_file.is_some() {
                return fail("set auth_token_env or auth_token_file, not both".into());
            }
            if !location.is_remote() {
                if target.auth_token_env.is_some() || target.auth_token_file.is_some() {
                    return fail("auth tokens apply only to Turso Cloud URLs".into());
                }
                if target.allow_client_auth_token {
                    return fail("allow_client_auth_token applies only to Turso Cloud URLs".into());
                }
                if target.transaction_mode == TransactionMode::Concurrent {
                    return fail(
                        "transaction_mode = \"concurrent\" applies only to Turso Cloud databases \
                         on the Turso Database engine"
                            .into(),
                    );
                }
            } else {
                if target.read_only {
                    return fail(
                        "read_only applies to local files; use a read-only Turso token".into(),
                    );
                }
                if target.auth_token_env.is_none()
                    && target.auth_token_file.is_none()
                    && !target.allow_client_auth_token
                {
                    return fail(
                        "a Turso Cloud target needs auth_token_env, auth_token_file or \
                         allow_client_auth_token"
                            .into(),
                    );
                }
            }
            if target.operation_timeout_seconds == 0
                || target.operation_timeout_seconds >= driver_timeout
            {
                return fail(format!(
                    "operation_timeout_seconds must be positive and shorter than \
                     server.driver_operation_timeout_seconds ({driver_timeout})"
                ));
            }
            if target.busy_timeout_ms >= target.operation_timeout_seconds * 1_000 {
                return fail(
                    "busy_timeout_ms must be shorter than operation_timeout_seconds".into(),
                );
            }
        }
        Ok(())
    }

    /// The equivalent Grainlift configuration. Each target's `driver` names
    /// the Turso target, which is how the backend tells them apart.
    pub fn grainlift(&self) -> grainlift_server::config::Config {
        let targets = self
            .targets
            .iter()
            .map(|(name, target)| {
                let target = TargetConfig {
                    driver: name.clone(),
                    entrypoint: None,
                    database_options: Vec::new(),
                    connection_options: Vec::new(),
                    allow_client_database_options: false,
                    allow_client_connection_options: false,
                    allowed_client_database_options: if target.allow_client_auth_token {
                        vec![AUTH_TOKEN_OPTION.to_string()]
                    } else {
                        Vec::new()
                    },
                    allowed_client_connection_options: vec!["adbc.connection.autocommit".into()],
                    init_statements: Vec::new(),
                };
                (name.clone(), target)
            })
            .collect::<HashMap<_, _>>();
        grainlift_server::config::Config {
            server: self.server.clone(),
            auth: self.auth.clone(),
            tcp: self.tcp.clone(),
            iroh: None,
            targets,
        }
    }

    /// Each target's settings, with its token read from the environment or
    /// its file. Errors name the variable or file, never the token.
    pub fn target_specs(&self) -> Result<Vec<(String, TargetSpec)>, Box<dyn Error>> {
        self.targets
            .iter()
            .map(|(name, target)| {
                let token = match (&target.auth_token_env, &target.auth_token_file) {
                    (Some(variable), _) => Some(
                        std::env::var(variable)
                            .ok()
                            .filter(|token| !token.trim().is_empty())
                            .ok_or_else(|| {
                                format!(
                                    "targets.{name}: environment variable {variable} is not set"
                                )
                            })?
                            .trim()
                            .to_string(),
                    ),
                    (None, Some(path)) => Some(read_token_file(name, path)?),
                    (None, None) => None,
                };
                let spec = TargetSpec {
                    location: Location::parse(&target.url, token, target.read_only),
                    allow_client_auth_token: target.allow_client_auth_token,
                    limits: Limits {
                        operation_timeout: Duration::from_secs(target.operation_timeout_seconds),
                        busy_timeout: Duration::from_millis(target.busy_timeout_ms),
                    },
                    transactions: target.transaction_mode,
                };
                Ok((name.clone(), spec))
            })
            .collect()
    }
}

fn read_token_file(name: &str, path: &Path) -> Result<String, Box<dyn Error>> {
    let token = std::fs::read_to_string(path)
        .map_err(|error| format!("targets.{name}: could not read {}: {error}", path.display()))?;
    let token = token.trim();
    if token.is_empty() {
        return Err(format!("targets.{name}: {} is empty", path.display()).into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(metadata) = std::fs::metadata(path)
            && metadata.permissions().mode() & 0o004 != 0
        {
            tracing::warn!(file = %path.display(), "Turso token file is readable by every user");
        }
    }
    Ok(token.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "[auth.static_bearer_tokens]\n\"client-token\" = \"analytics\"\n";

    fn parse(targets: &str) -> Result<Config, String> {
        Config::from_toml(&format!("{BASE}{targets}")).map_err(|error| error.to_string())
    }

    #[test]
    fn parses_targets_with_defaults() {
        let config = parse(
            "[targets.local]\nurl = \"app.db\"\n\n\
             [targets.cloud]\nurl = \"libsql://db.turso.io\"\nauth_token_env = \"TOKEN\"\n",
        )
        .unwrap();
        assert_eq!(config.targets.len(), 2);
        assert_eq!(config.targets["local"].operation_timeout_seconds, 60);
        assert_eq!(
            config.targets["local"].transaction_mode,
            TransactionMode::Deferred
        );
        let grainlift = config.grainlift();
        assert_eq!(grainlift.targets["cloud"].driver, "cloud");
        assert!(
            grainlift.targets["cloud"]
                .allowed_client_database_options
                .is_empty()
        );
    }

    #[test]
    fn rejects_unsafe_or_inconsistent_targets() {
        let error = |targets: &str| parse(targets).unwrap_err();
        assert!(error("").contains("at least one target"));
        assert!(
            error("[targets.a]\nurl = \"libsql://db.turso.io\"\n").contains("needs auth_token_env")
        );
        assert!(
            error("[targets.a]\nurl = \"libsql://x\"\nauth_token_env = \"A\"\nauth_token_file = \"f\"\n")
                .contains("not both")
        );
        assert!(
            error("[targets.a]\nurl = \"app.db\"\nauth_token_env = \"A\"\n")
                .contains("only to Turso Cloud")
        );
        assert!(
            error("[targets.a]\nurl = \"libsql://x\"\nauth_token_env = \"A\"\nread_only = true\n")
                .contains("read-only Turso token")
        );
        assert!(
            error("[targets.a]\nurl = \"app.db\"\noperation_timeout_seconds = 600\n")
                .contains("shorter than server.driver_operation_timeout_seconds")
        );
        assert!(
            error("[targets.a]\nurl = \"app.db\"\nunknown = 1\n").contains("invalid configuration")
        );
        assert!(
            error("[targets.a]\nurl = \"same.db\"\n[targets.b]\nurl = \"file:same.db\"\nread_only = true\n")
                .contains("same database file")
        );
        assert!(
            error("[targets.a]\nurl = \"app.db\"\ntransaction_mode = \"concurrent\"\n")
                .contains("only to Turso Cloud")
        );
        assert!(
            error("[targets.a]\nurl = \"libsql://x\"\nauth_token_env = \"A\"\ntransaction_mode = \"eager\"\n")
                .contains("invalid configuration")
        );
    }

    #[test]
    fn the_example_configuration_is_valid() {
        let config = Config::from_toml(include_str!("../turso.example.toml")).unwrap();
        assert_eq!(config.targets.len(), 3);
        assert!(config.targets["events"].allow_client_auth_token);
        assert_eq!(
            config.targets["events"].transaction_mode,
            TransactionMode::Concurrent
        );
        assert!(config.targets["reference"].read_only);
        assert_eq!(
            config.auth.target_permissions["analytics"],
            ["app", "events"]
        );
    }

    #[test]
    fn errors_never_quote_the_file() {
        let error = Config::from_toml("[auth.static_bearer_tokens]\n\"secret-token-value\" = \n")
            .unwrap_err()
            .to_string();
        assert!(!error.contains("secret-token-value"), "{error}");
    }

    #[test]
    fn reads_tokens_from_files_and_the_environment() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("token");
        std::fs::write(&file, "file-token\n").unwrap();
        // SAFETY: tests in this module do not read this variable concurrently.
        unsafe { std::env::set_var("GRAINLIFT_TURSO_TEST_TOKEN", "env-token") };
        let config = parse(&format!(
            "[targets.a]\nurl = \"libsql://a\"\nauth_token_file = \"{}\"\n\n\
             [targets.b]\nurl = \"libsql://b\"\nauth_token_env = \"GRAINLIFT_TURSO_TEST_TOKEN\"\n\n\
             [targets.c]\nurl = \"libsql://c\"\nauth_token_env = \"GRAINLIFT_TURSO_UNSET_VARIABLE\"\n",
            file.display()
        ))
        .unwrap();
        let error = config.target_specs().unwrap_err().to_string();
        assert!(
            error.contains("GRAINLIFT_TURSO_UNSET_VARIABLE is not set"),
            "{error}"
        );
        let mut config = config;
        config.targets.remove("c");
        let specs = config.target_specs().unwrap();
        let token = |index: usize| match &specs[index].1.location {
            Location::Remote { auth_token, .. } => auth_token.clone(),
            Location::Local { .. } => None,
        };
        assert_eq!(token(0).as_deref(), Some("file-token"));
        assert_eq!(token(1).as_deref(), Some("env-token"));
    }
}
