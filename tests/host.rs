// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! The production host and the `grainlift-turso` command.

mod common;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use adbc_core::error::Status;
use adbc_core::{Connection, Statement};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use grainlift_turso::TursoBackend;
use grainlift_turso::config::Config;

/// A configuration with two local targets and two principals: alice may use
/// both targets, bob only `alpha`.
fn config(directory: &Path, listen: &str) -> String {
    format!(
        r#"
[server]
listen = "{listen}"
shutdown_grace_seconds = 5

[auth.static_bearer_tokens]
"alice-token" = "alice"
"bob-token" = "bob"

[auth.target_permissions]
alice = ["*"]
bob = ["alpha"]

[targets.alpha]
url = "{alpha}"
operation_timeout_seconds = 30

[targets.beta]
url = "{beta}"
"#,
        alpha = directory.join("alpha.db").display(),
        beta = directory.join("beta.db").display(),
    )
}

/// `GET path` on `address`; the HTTP status code.
fn get(address: SocketAddr, path: &str) -> Option<u16> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .ok()?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    response.split_whitespace().nth(1)?.parse().ok()
}

/// The host, serving `config` on an ephemeral port until dropped.
struct Host {
    address: SocketAddr,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<Result<(), String>>>,
}

impl Host {
    fn start(config: &str) -> Self {
        let config = Config::from_toml(config).unwrap();
        let backend = Arc::new(TursoBackend::new(config.target_specs().unwrap()).unwrap());
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let (bound, address) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                bound.send(listener.local_addr().unwrap()).unwrap();
                grainlift_turso::host::serve(config, backend, listener, "test".into(), async {
                    let _ = stopped.await;
                })
                .await
                .map_err(|error| error.to_string())
            })
        });
        Self {
            address: address.recv().unwrap(),
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    fn url(&self) -> String {
        format!("http://{}", self.address)
    }

    /// Stop the host and wait for it to drain.
    fn stop(mut self) -> Result<(), String> {
        let _ = self.stop.take().unwrap().send(());
        self.thread.take().unwrap().join().unwrap()
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[test]
fn the_host_serves_targets_to_permitted_principals() {
    let directory = tempfile::tempdir().unwrap();
    let host = Host::start(&config(directory.path(), "127.0.0.1:0"));
    assert_eq!(get(host.address, "/healthz"), Some(204));
    assert_eq!(get(host.address, "/readyz"), Some(204));

    if let Some(_driver) = common::driver_path() {
        let run = |target: &str, token: &str, sql: &str| {
            let mut connection = common::connect_target(&host.url(), target, Some(token), &[])?;
            let mut statement = connection.new_statement()?;
            statement.set_sql_query(sql)?;
            statement.execute_update()
        };
        let count = |target: &str| {
            let mut connection =
                common::connect_target(&host.url(), target, Some("alice-token"), &[]).unwrap();
            let mut statement = connection.new_statement().unwrap();
            statement.set_sql_query("SELECT count(*) FROM t").unwrap();
            let batches = statement
                .execute()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            batches[0].column(0).as_primitive::<Int64Type>().value(0)
        };
        for target in ["alpha", "beta"] {
            run(target, "alice-token", "CREATE TABLE t (n INTEGER)").unwrap();
        }
        run("alpha", "alice-token", "INSERT INTO t VALUES (1), (2)").unwrap();
        run("beta", "alice-token", "INSERT INTO t VALUES (3)").unwrap();
        assert_eq!(count("alpha"), 2, "targets are separate databases");
        assert_eq!(count("beta"), 1);

        run("alpha", "bob-token", "INSERT INTO t VALUES (4)").unwrap();
        let denied = run("beta", "bob-token", "SELECT 1").unwrap_err();
        assert_eq!(denied.status, Status::Unauthorized, "{}", denied.message);
        let rejected = run("alpha", "wrong-token", "SELECT 1").unwrap_err();
        assert_eq!(
            rejected.status,
            Status::Unauthenticated,
            "{}",
            rejected.message
        );
        let missing = run("gamma", "alice-token", "SELECT 1").unwrap_err();
        assert!(
            matches!(missing.status, Status::NotFound | Status::Unauthorized),
            "{:?}: {}",
            missing.status,
            missing.message
        );
    } else {
        common::skip("GRAINLIFT_DRIVER does not name the native driver library");
    }
    host.stop().unwrap();
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_grainlift-turso"))
}

#[test]
fn check_validates_a_configuration_and_its_databases() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("turso.toml");
    std::fs::write(&file, config(directory.path(), "127.0.0.1:8080")).unwrap();
    let output = command()
        .args(["check", "--config"])
        .arg(&file)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("target alpha: ok") && stdout.contains("target beta: ok"),
        "{stdout}"
    );

    // A broken file fails without quoting its contents.
    std::fs::write(
        &file,
        "[auth.static_bearer_tokens]\n\"secret-token-value\" = 7\n",
    )
    .unwrap();
    let output = command()
        .args(["check", "--config"])
        .arg(&file)
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(!stderr.contains("secret-token-value"), "{stderr}");
}

#[cfg(unix)]
#[test]
fn serve_runs_until_terminated() {
    let directory = tempfile::tempdir().unwrap();
    let port = free_port();
    let file = directory.path().join("turso.toml");
    std::fs::write(
        &file,
        config(directory.path(), &format!("127.0.0.1:{port}")),
    )
    .unwrap();
    let mut child = command()
        .args(["serve", "--config"])
        .arg(&file)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let started = Instant::now();
    while get(address, "/healthz") != Some(204) {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the server did not start"
        );
        assert!(
            child.try_wait().unwrap().is_none(),
            "the server exited early"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(get(address, "/readyz"), Some(204));
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let stopping = Instant::now();
    let exit = loop {
        if let Some(exit) = child.try_wait().unwrap() {
            break exit;
        }
        assert!(
            stopping.elapsed() < Duration::from_secs(15),
            "the server did not stop"
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    let mut log = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut log)
        .unwrap();
    assert!(exit.success(), "{exit:?}\n{log}");
    assert!(log.contains("Turso target ready"), "{log}");
    assert!(
        log.contains("closed ADBC sessions during shutdown"),
        "{log}"
    );
}
