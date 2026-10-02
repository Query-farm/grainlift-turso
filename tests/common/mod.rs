// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Serve a backend over loopback HTTP and connect through the native driver.

#![allow(dead_code)]

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;

use adbc_core::error::Result;
use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Database, Driver};
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use grainlift_server::backend::Backend;
use grainlift_server::dev::Service;
use grainlift_server::hosting::http_authenticator;

/// Fail instead of skipping when `GRAINLIFT_REQUIRE_NATIVE` is set (as in CI).
pub fn skip(reason: &str) {
    assert!(
        std::env::var_os("GRAINLIFT_REQUIRE_NATIVE").is_none(),
        "GRAINLIFT_REQUIRE_NATIVE is set but {reason}"
    );
    eprintln!("skipped: {reason}");
}

/// The native driver library from `GRAINLIFT_DRIVER`, or `None` to skip.
pub fn driver_path() -> Option<PathBuf> {
    let path = std::env::var_os("GRAINLIFT_DRIVER")?;
    Some(
        PathBuf::from(path)
            .canonicalize()
            .expect("GRAINLIFT_DRIVER must name an existing driver library"),
    )
}

/// Skip the calling test when `GRAINLIFT_DRIVER` is unset.
#[macro_export]
macro_rules! require_driver {
    () => {
        match common::driver_path() {
            Some(path) => path,
            None => {
                common::skip("GRAINLIFT_DRIVER does not name the native driver library");
                return;
            }
        }
    };
}

/// Connect to `endpoint` through the native Grainlift ADBC driver, with a
/// bearer token or anonymously.
pub fn connect(endpoint: &str, token: Option<&str>) -> Result<ManagedConnection> {
    connect_with(endpoint, token, &[])
}

/// [`connect`] with extra database options, which the driver forwards to the
/// backend.
pub fn connect_with(
    endpoint: &str,
    token: Option<&str>,
    extra: &[(&str, &str)],
) -> Result<ManagedConnection> {
    connect_target(endpoint, "turso", token, extra)
}

/// [`connect_with`] for a named target.
pub fn connect_target(
    endpoint: &str,
    target: &str,
    token: Option<&str>,
    extra: &[(&str, &str)],
) -> Result<ManagedConnection> {
    let driver_path = driver_path().expect("GRAINLIFT_DRIVER");
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        driver_path,
        Some(b"AdbcDriverGrainliftInit"),
        AdbcVersion::V110,
    )?;
    let mut options: Vec<(OptionDatabase, OptionValue)> = vec![
        (
            OptionDatabase::Other("grainlift.uri".into()),
            endpoint.into(),
        ),
        (
            OptionDatabase::Other("grainlift.target".into()),
            target.into(),
        ),
    ];
    for (key, value) in extra {
        options.push((OptionDatabase::Other((*key).into()), (*value).into()));
    }
    if let Some(token) = token {
        options.push((
            OptionDatabase::Other("grainlift.auth.bearer_token".into()),
            token.into(),
        ));
    }
    driver.new_database_with_opts(options)?.new_connection()
}

/// The authenticated `(domain, principal)` of every accepted HTTP request.
pub type Principals = Arc<Mutex<BTreeSet<(String, String)>>>;

/// A backend served over HTTP on an ephemeral loopback port until dropped.
pub struct Server {
    pub url: String,
    pub service: Arc<Service>,
    pub principals: Principals,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Server {
    /// Serve `backend` as `turso` with bearer `tokens` (token, principal)
    /// and optional anonymous access.
    pub fn start(
        backend: impl Backend + 'static,
        tokens: &[(&str, &str)],
        anonymous_principal: Option<&str>,
    ) -> Self {
        let service = Arc::new(Service::new(backend, "turso"));
        let tokens = tokens
            .iter()
            .map(|(token, principal)| (token.to_string(), principal.to_string()))
            .collect::<HashMap<_, _>>();
        let authenticate = http_authenticator(tokens, anonymous_principal).unwrap();
        let principals = Principals::default();
        let seen = Arc::clone(&principals);
        let recording: vgi_rpc::Authenticate = Arc::new(move |request| {
            let auth = authenticate(request)?;
            if auth.authenticated {
                let identity = (auth.domain.clone(), auth.principal.clone());
                seen.lock().unwrap().insert(identity);
            }
            Ok(auth)
        });
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let (bound_tx, bound_rx) = mpsc::channel();
        let serving = Arc::clone(&service);
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                bound_tx.send(listener.local_addr().unwrap()).unwrap();
                serving
                    .serve_http(listener, recording, async {
                        let _ = stopped.await;
                    })
                    .await
                    .unwrap();
            });
        });
        let address = bound_rx.recv().unwrap();
        Self {
            url: format!("http://{address}"),
            service,
            principals,
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    /// Open sessions and results still held by the service.
    pub fn open_handles(&self) -> (usize, usize) {
        let counts = self.service.manager().resource_counts().unwrap();
        (counts.sessions, counts.results)
    }

    /// The principals seen so far, as `(domain, principal)` pairs.
    pub fn principals(&self) -> Vec<(String, String)> {
        self.principals.lock().unwrap().iter().cloned().collect()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// A backend over a fresh database file in a temporary directory, which
/// lives as long as the returned guard.
pub fn local_backend() -> (grainlift_turso::TursoBackend, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("test.db");
    let location = grainlift_turso::Location::parse(path.to_str().unwrap(), None, false);
    (
        grainlift_turso::TursoBackend::open(&location).unwrap(),
        directory,
    )
}
