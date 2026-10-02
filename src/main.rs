// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! The `grainlift-turso` command.

use std::process::ExitCode;

use grainlift_server::dev::{self, RunOptions};
use grainlift_turso::{AUTH_TOKEN_OPTION, Location, TursoBackend};

/// Serve the database named by `TURSO_DATABASE_URL` as the `turso` target on
/// loopback; run with `--help` for hosting options.
///
/// The service can write, so it requires a bearer token by default.
fn main() -> ExitCode {
    let help = std::env::args().any(|arg| arg == "--help" || arg == "-h");
    let location = match TursoBackend::location_from_env() {
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
    if backend.requires_client_token() {
        println!(
            "TURSO_AUTH_TOKEN is unset: clients must send their own Turso token in the \
             {AUTH_TOKEN_OPTION} option"
        );
    }
    dev::run(
        backend,
        "turso",
        RunOptions::new(
            "Grainlift ADBC service for a Turso database. Set TURSO_DATABASE_URL to a \
             Turso Cloud URL or a local database file. For Turso Cloud, set \
             TURSO_AUTH_TOKEN, or let each client send its own token in the turso.auth_token \
             database option.",
        ),
    )
}
