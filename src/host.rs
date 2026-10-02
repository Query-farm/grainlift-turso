// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! The production host: Grainlift's HTTP and TCP/mTLS listeners, client
//! authentication, session limits and graceful shutdown, serving a
//! [`TursoBackend`].
//!
//! It follows the `grainlift-server` binary, so a [`Config`] behaves as the
//! same file would there: static bearer tokens or JWT validation, OAuth
//! protected-resource metadata, per-principal target permissions, CORS, the
//! session reaper, and a drain deadline on shutdown. It adds a readiness probe
//! that checks every database answers. Iroh is not offered.

use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::http::StatusCode;
use axum::routing::get;
use grainlift_server::backend::Backend;
use grainlift_server::config::{AuthConfig, TcpTlsConfig};
use grainlift_server::hosting::{self, load_mtls_config, start_tcp_listener};
use grainlift_server::service::build_server_with_max_bind;
use grainlift_server::session::SessionManager;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use vgi_rpc::AuthContext;
use vgi_rpc::auth::Authenticate;
use vgi_rpc::auth::bearer::bearer_authenticate_static;
use vgi_rpc::auth::jwt::{JwtConfig, jwt_authenticate};
use vgi_rpc::auth::oauth::OAuthResourceMetadata;
use vgi_rpc::http::HttpState;
use vgi_rpc::tcp::{TcpIdentityOptions, TcpMutualTlsConfig, TcpMutualTlsOptions};

use crate::TursoBackend;
use crate::config::Config;

/// How long a readiness probe waits for the databases.
const READINESS_TIMEOUT: Duration = Duration::from_secs(10);

/// Serve `backend` as `config` describes on `listener` (the HTTP listener;
/// a `[tcp]` listener is opened here) until `shutdown` resolves, then close
/// every session and drain within `server.shutdown_grace_seconds`.
pub async fn serve(
    config: Config,
    backend: Arc<TursoBackend>,
    listener: tokio::net::TcpListener,
    server_id: String,
    shutdown: impl Future<Output = ()>,
) -> Result<(), Box<dyn Error>> {
    let grainlift = config.grainlift();
    let server_config = &grainlift.server;
    let manager = Arc::new(SessionManager::with_limits_authorizer_and_timeout(
        Arc::clone(&backend) as Arc<dyn Backend>,
        grainlift.targets.clone(),
        Duration::from_secs(server_config.session_ttl_seconds),
        server_config.require_authentication,
        server_config.session_limits(),
        grainlift.target_authorizer(),
        Duration::from_secs(server_config.driver_operation_timeout_seconds),
    ));
    let server = Arc::new(build_server_with_max_bind(
        Arc::clone(&manager),
        server_id,
        server_config.max_bind_bytes,
    ));

    let authenticate = build_authenticator(&grainlift.auth);
    let mut state = HttpState::builder()
        .server(Arc::clone(&server))
        .authenticate(if server_config.require_authentication {
            hosting::require_credentials(authenticate)
        } else {
            authenticate
        })
        .max_body_size(server_config.max_request_body_bytes)
        .max_request_bytes(server_config.max_request_body_bytes)
        .request_timeout(Duration::from_secs(server_config.request_timeout_seconds));
    if let Some(origins) = &server_config.cors_origins {
        state = state.cors_origins(origins.clone());
    }
    if let Some(max_age) = server_config.cors_max_age_seconds {
        state = state.cors_max_age(max_age);
    }
    if let Some(metadata) = oauth_resource_metadata(&grainlift.auth) {
        state = state.oauth_resource_metadata(metadata);
    }
    let ready = Arc::clone(&backend);
    let app = vgi_rpc::http::build_router(state.build())
        .route("/healthz", get(|| async { StatusCode::NO_CONTENT }))
        .route("/readyz", get(move || readiness(Arc::clone(&ready))));

    let tcp_shutdown = Arc::new(AtomicBool::new(false));
    let mut tcp_task = match &grainlift.tcp {
        Some(tcp) => {
            let transport = if tcp.tls.is_some() { "tls+tcp" } else { "tcp" };
            let tls = match &tcp.tls {
                Some(tls) => Some(
                    TcpMutualTlsOptions::new(
                        build_tcp_tls(tls).map_err(|error| error.to_string())?,
                    )
                    .with_identity(TcpIdentityOptions {
                        policy: Some(vgi_rpc::peer_identity_primary("spiffe")),
                        ..TcpIdentityOptions::default()
                    }),
                ),
                None => None,
            };
            let listener = start_tcp_listener(
                Arc::clone(&server),
                tcp.listen,
                tls,
                Arc::clone(&tcp_shutdown),
            )
            .await?;
            info!(address = %listener.address, transport, "grainlift-turso listening");
            Some(listener.task)
        }
        None => None,
    };

    let address = listener.local_addr()?;
    let stop = CancellationToken::new();
    let http_stop = stop.clone();
    let mut http_task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(http_stop.cancelled_owned())
            .await
    });
    info!(%address, transport = "http", "grainlift-turso listening");

    let reaper_manager = Arc::downgrade(&manager);
    let reap_interval = Duration::from_secs(server_config.session_reap_interval_seconds);
    let reaper = tokio::spawn(async move {
        let mut interval = tokio::time::interval(reap_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(manager) = reaper_manager.upgrade() else {
                break;
            };
            match manager.reap_expired() {
                Ok(count) if count > 0 => info!(count, "reaped expired ADBC sessions"),
                Ok(_) => {}
                Err(error) => warn!(%error, "could not reap expired ADBC sessions"),
            }
        }
    });

    let has_tcp = tcp_task.is_some();
    let mut http_finished = false;
    let mut tcp_finished = false;
    let mut service_error: Option<Box<dyn Error>> = None;
    tokio::select! {
        () = shutdown => info!("shutting down"),
        result = &mut http_task => {
            http_finished = true;
            service_error = Some(match result {
                Ok(Ok(())) => "HTTP listener exited unexpectedly".into(),
                Ok(Err(error)) => error.into(),
                Err(error) => error.into(),
            });
        }
        result = async { tcp_task.as_mut().expect("guarded TCP task").await }, if has_tcp => {
            tcp_finished = true;
            service_error = Some(match result {
                Ok(Ok(())) => "TCP listener exited unexpectedly".into(),
                Ok(Err(error)) => error.into(),
                Err(error) => error.into(),
            });
        }
    }
    stop.cancel();
    tcp_shutdown.store(true, Ordering::Release);
    let closed = manager.close_all()?;
    info!(closed, "closed ADBC sessions during shutdown");
    let grace = Duration::from_secs(server_config.shutdown_grace_seconds);
    let drain = async {
        if !http_finished {
            (&mut http_task).await??;
        }
        if let Some(task) = tcp_task.as_mut()
            && !tcp_finished
        {
            task.await??;
        }
        Ok::<(), Box<dyn Error>>(())
    };
    match tokio::time::timeout(grace, drain).await {
        Ok(result) => result?,
        Err(_) => {
            warn!(
                ?grace,
                "shutdown drain deadline expired; detaching remaining work"
            );
            http_task.abort();
            if let Some(task) = tcp_task.as_ref() {
                task.abort();
            }
        }
    }
    reaper.abort();
    service_error.map_or(Ok(()), Err)
}

/// `204` once every database answers, `503` otherwise.
async fn readiness(backend: Arc<TursoBackend>) -> StatusCode {
    let check = tokio::task::spawn_blocking(move || backend.check());
    match tokio::time::timeout(READINESS_TIMEOUT, check).await {
        Ok(Ok(Ok(()))) => StatusCode::NO_CONTENT,
        Ok(Ok(Err(failure))) => {
            warn!(error = %failure.message, "readiness check failed");
            StatusCode::SERVICE_UNAVAILABLE
        }
        _ => {
            warn!("readiness check timed out");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

fn build_tcp_tls(
    config: &TcpTlsConfig,
) -> Result<TcpMutualTlsConfig, Box<dyn Error + Send + Sync>> {
    load_mtls_config(
        &config.server_certificate_chain,
        &config.server_private_key,
        &config.client_ca,
        config.trust_domains.clone(),
        Duration::from_secs(config.handshake_timeout_seconds),
    )
}

/// RFC 9728 metadata for `[auth.oauth]`; validation guarantees the
/// `[auth.jwt]` it depends on.
fn oauth_resource_metadata(config: &AuthConfig) -> Option<OAuthResourceMetadata> {
    let (oauth, jwt) = (config.oauth.as_ref()?, config.jwt.as_ref()?);
    let mut metadata = OAuthResourceMetadata::new(&oauth.resource).with_client_id(&oauth.client_id);
    for server in oauth.authorization_servers(jwt) {
        metadata = metadata.with_authorization_server(server);
    }
    for scope in &oauth.scopes {
        metadata = metadata.with_scope(scope);
    }
    if let Some(name) = &oauth.resource_name {
        metadata = metadata.with_resource_name(name);
    }
    metadata.use_id_token_as_bearer = oauth.use_id_token_as_bearer;
    metadata.client_secret = oauth.client_secret.clone().unwrap_or_default();
    metadata.device_code_client_id = oauth.device_code_client_id.clone().unwrap_or_default();
    metadata.device_code_client_secret =
        oauth.device_code_client_secret.clone().unwrap_or_default();
    Some(metadata)
}

/// JWT validation when `[auth.jwt]` is set, otherwise the static bearer tokens.
fn build_authenticator(config: &AuthConfig) -> Authenticate {
    if let Some(jwt) = &config.jwt {
        // VGI-RPC checks one audience per authenticator; accept a token that
        // any configured audience accepts.
        let authenticators = jwt
            .audience
            .values()
            .into_iter()
            .map(|audience| {
                jwt_authenticate(
                    JwtConfig::new(&jwt.issuer)
                        .with_audience(audience)
                        .with_jwks_url(&jwt.jwks_url)
                        .with_principal_claim(&jwt.principal_claim)
                        .with_refresh_interval(Duration::from_secs(jwt.refresh_interval_seconds))
                        .with_leeway(Duration::from_secs(jwt.leeway_seconds)),
                )
            })
            .collect::<Vec<_>>();
        if let [single] = authenticators.as_slice() {
            return single.clone();
        }
        return Arc::new(move |request: &vgi_rpc::AuthRequest<'_>| {
            let mut last = None;
            for authenticate in &authenticators {
                match authenticate(request) {
                    Ok(auth) if auth.authenticated => return Ok(auth),
                    Ok(auth) => last = Some(Ok(auth)),
                    Err(error) => last = Some(Err(error)),
                }
            }
            last.expect("at least one audience is configured")
        });
    }
    let tokens: HashMap<String, AuthContext> = config
        .static_bearer_tokens
        .iter()
        .map(|(token, principal)| {
            (
                token.clone(),
                AuthContext::for_principal("bearer", principal),
            )
        })
        .collect();
    bearer_authenticate_static(tokens)
}

/// Log to stderr, filtered by `RUST_LOG` (default `info` for this service and
/// Grainlift): one JSON object per line when `GRAINLIFT_TURSO_LOG_FORMAT=json`,
/// otherwise text, colored only on a terminal. Export traces over OTLP when
/// `OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is
/// set. Returns the tracer provider to flush on exit.
pub fn init_observability()
-> Result<Option<opentelemetry_sdk::trace::SdkTracerProvider>, Box<dyn Error>> {
    use std::io::IsTerminal as _;

    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::{Protocol, WithExportConfig};
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::layer::SubscriberExt as _;
    use tracing_subscriber::util::SubscriberInitExt as _;

    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        "grainlift_turso=info,grainlift_server=info,vgi_rpc=info,vgi_rpc.otel=info".into()
    });
    let json = std::env::var("GRAINLIFT_TURSO_LOG_FORMAT")
        .is_ok_and(|format| format.eq_ignore_ascii_case("json"));
    let fmt = if json {
        tracing_subscriber::fmt::layer()
            .json()
            .with_writer(std::io::stderr)
            .boxed()
    } else {
        tracing_subscriber::fmt::layer()
            .with_ansi(std::io::stderr().is_terminal())
            .with_writer(std::io::stderr)
            .boxed()
    };
    let disabled =
        std::env::var("OTEL_SDK_DISABLED").is_ok_and(|value| value.eq_ignore_ascii_case("true"));
    let configured = std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_some()
        || std::env::var_os("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").is_some();
    if configured && !disabled {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .build()?;
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_resource(
                opentelemetry_sdk::Resource::builder()
                    .with_service_name("grainlift-turso")
                    .build(),
            )
            .with_batch_exporter(exporter)
            .build();
        let tracer = provider.tracer("grainlift-turso");
        opentelemetry::global::set_tracer_provider(provider.clone());
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt)
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .try_init()?;
        Ok(Some(provider))
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt)
            .try_init()?;
        Ok(None)
    }
}

/// The address a configuration's HTTP listener binds.
pub fn listen_address(config: &Config) -> SocketAddr {
    config.server.listen
}
