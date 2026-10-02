// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! The `grainlift-turso` command.
//!
//! - `grainlift-turso serve --config FILE`: the production host.
//! - `grainlift-turso check --config FILE`: validate the file, read its
//!   secrets and connect to every database, then exit.
//! - `grainlift-turso [--host http|mtls] [--port N] [--auth token|anonymous]`:
//!   the development host, serving `TURSO_DATABASE_URL` as the `turso` target
//!   on loopback.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use grainlift_server::dev::{self, RunOptions};
use grainlift_turso::config::Config;
use grainlift_turso::{DEV_TARGET, Location, TursoBackend, host, location_from_env};

#[derive(Debug, Parser)]
#[command(
    name = "grainlift-turso",
    version,
    about = "Grainlift ADBC service for Turso databases"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve the databases in a configuration file.
    Serve {
        /// The configuration file.
        #[arg(long, short, env = "GRAINLIFT_TURSO_CONFIG")]
        config: PathBuf,
        /// Server identity reported to clients (default: grainlift-turso-PID).
        #[arg(long)]
        server_id: Option<String>,
    },
    /// Validate a configuration file and connect to every database.
    Check {
        /// The configuration file.
        #[arg(long, short, env = "GRAINLIFT_TURSO_CONFIG")]
        config: PathBuf,
    },
}

fn main() -> ExitCode {
    let first = std::env::args().nth(1);
    match first.as_deref() {
        Some("serve" | "check" | "--version" | "-V") => production(Cli::parse()),
        _ => development(),
    }
}

fn production(cli: Cli) -> ExitCode {
    let result = match cli.command {
        Command::Serve { config, server_id } => serve(config, server_id),
        Command::Check { config } => check(config),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn load(path: &std::path::Path) -> Result<(Config, TursoBackend), Box<dyn std::error::Error>> {
    let config = Config::from_path(path)?;
    let backend = TursoBackend::new(config.target_specs()?).map_err(|error| error.message)?;
    Ok((config, backend))
}

fn check(path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let (_, backend) = load(&path)?;
    for name in backend.target_names() {
        let note = if backend.requires_client_token(name) {
            " (clients send their own Turso token; not connected)"
        } else {
            ""
        };
        println!("target {name}: ok{note}");
    }
    println!("Configuration is valid.");
    Ok(())
}

fn serve(path: PathBuf, server_id: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        let tracer = host::init_observability()?;
        let (config, backend) = load(&path)?;
        let listener = tokio::net::TcpListener::bind(host::listen_address(&config)).await?;
        let server_id =
            server_id.unwrap_or_else(|| format!("grainlift-turso-{}", std::process::id()));
        let result = host::serve(
            config,
            Arc::new(backend),
            listener,
            server_id,
            grainlift_server::hosting::shutdown_signal(),
        )
        .await;
        if let Some(provider) = tracer {
            provider.shutdown()?;
        }
        result
    })
}

/// Serve the database named by `TURSO_DATABASE_URL` as the `turso` target on
/// loopback; run with `--help` for hosting options. The service can write, so
/// it requires a bearer token by default.
fn development() -> ExitCode {
    let help = std::env::args().any(|arg| arg == "--help" || arg == "-h");
    let location = match location_from_env() {
        Ok(location) => location,
        // `--help` needs no database; show it with a scratch one.
        Err(_) if help => Location::parse(":memory:", None, false),
        Err(error) => {
            eprintln!("error: {}", error.message);
            return ExitCode::FAILURE;
        }
    };
    let backend = match TursoBackend::open(&location) {
        Ok(backend) => backend,
        Err(error) => {
            eprintln!(
                "error: could not open the Turso database: {}",
                error.message
            );
            return ExitCode::FAILURE;
        }
    };
    if backend.requires_client_token(DEV_TARGET) {
        println!(
            "TURSO_AUTH_TOKEN is unset: clients must send their own Turso token in the \
             {} option",
            grainlift_turso::AUTH_TOKEN_OPTION
        );
    }
    dev::run(
        backend,
        DEV_TARGET,
        RunOptions::new(
            "Grainlift ADBC service for a Turso database (development host). Set \
             TURSO_DATABASE_URL to a Turso Cloud URL or a local database file. For Turso \
             Cloud, set TURSO_AUTH_TOKEN, or let each client send its own token in the \
             turso.auth_token database option. For production, run \
             `grainlift-turso serve --config FILE`.",
        ),
    )
}
