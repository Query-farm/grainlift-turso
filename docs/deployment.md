# Deploying grainlift-turso

This guide covers running `grainlift-turso` as a long-lived service: the
configuration file, authentication, health checks, logging, containers and
scaling. For a first look, start with the [quick start](../README.md#quick-start).

## The two commands

```bash
grainlift-turso check --config turso.toml   # validate the file, read its secrets, connect to every database
grainlift-turso serve --config turso.toml   # serve until SIGTERM
```

Run `check` in your deployment pipeline: it fails on a bad file, a missing
secret or an unreachable database, without starting a server.

## The configuration file

The file uses Grainlift's own server configuration (`[server]`, `[auth]` and
`[tcp]`, with the same fields, defaults and validation as the
`grainlift-server` binary), plus one `[targets.NAME]` table per Turso
database. Clients pick a database by its target name.
[`turso.example.toml`](../turso.example.toml) is a complete, annotated example
(a test keeps it valid).

```toml
[server]
listen = "127.0.0.1:8080"

[auth.static_bearer_tokens]
"replace-with-a-long-random-token" = "analytics"

[auth.target_permissions]
analytics = ["app"]

[targets.app]
url = "libsql://app-myorg.turso.io"
auth_token_env = "TURSO_APP_TOKEN"     # or auth_token_file = "/run/secrets/turso"
operation_timeout_seconds = 60

[targets.reference]
url = "/var/lib/grainlift-turso/reference.db"
read_only = true
```

### Target settings

| Setting | Default | |
|---|---|---|
| `url` | | A `libsql://` or `https://` Turso Cloud URL, or a local file path |
| `auth_token_env`, `auth_token_file` | | Where the Turso Cloud token comes from. Tokens never live in the file. |
| `allow_client_auth_token` | `false` | Let clients send their own Turso token (see [Turso Cloud tokens](#turso-cloud-tokens)) |
| `read_only` | `false` | Open a local file read-only. Two targets may not share a file. |
| `operation_timeout_seconds` | `60` | Longest one operation may run before it is stopped. Must be shorter than `server.driver_operation_timeout_seconds`. |
| `busy_timeout_ms` | `5000` | How long a local write waits for another writer's lock |
| `transaction_mode` | `deferred` | `deferred` opens transactions with `BEGIN`; `concurrent` uses `BEGIN CONCURRENT`, for Turso Cloud databases on the Turso Database engine (see [concurrent writes](how-it-works.md#concurrent-writes-on-turso-clouds-new-engine)) |

### Server settings

`[server]` is Grainlift's: the listener address, session lifetime
(`session_ttl_seconds`), request and operation timeouts, session limits
(`max_sessions`, `max_sessions_per_principal`, …), CORS and the shutdown grace
period. See the comments in [`turso.example.toml`](../turso.example.toml) and
Grainlift's [configuration documentation](https://github.com/Query-farm/grainlift).

## Authentication

Two separate credentials are involved, and neither substitutes for the other.

### Clients to grainlift-turso

`[auth]` decides who may connect:

- **Static bearer tokens** (`[auth.static_bearer_tokens]`), each mapped to a
  principal name.
- **JWT validation** (`[auth.jwt]`) against your identity provider, with
  optional **OAuth discovery** (`[auth.oauth]`) so browser and command-line
  clients can sign in.
- **Mutual TLS** on an optional raw TCP listener (`[tcp]` with `[tcp.tls]`);
  clients are identified by the SPIFFE URI in their certificate.
- **Per-principal permissions** (`[auth.target_permissions]`) decide which
  principals may use which targets. Without this table every principal may use
  every target; once present, unlisted principals are denied.

See also Grainlift's
[security guide](https://github.com/Query-farm/grainlift/blob/main/docs/security.md).

### Turso Cloud tokens

A target's `auth_token_env` or `auth_token_file` gives the service its own
Turso token. With `allow_client_auth_token = true`, each client can instead
(or as well) send its own token in the `turso.auth_token` database option,
which the Grainlift driver forwards:

```sql
CREATE SECRET turso (TYPE adbc, DRIVER '...', URI 'http://127.0.0.1:8080', SCOPE 'http://127.0.0.1:8080',
    EXTRA_OPTIONS MAP {'grainlift.target': 'app', 'grainlift.auth.bearer_token': '...',
                       'turso.auth_token': '...'});
```

That connection then authenticates to Turso as the client's token, so Turso
applies its permissions: a read-only token gets a read-only connection. When a
target has no token of its own, every client must send one. Use database
tokens (`turso db tokens create <db>` or `turso group tokens create <group>`),
not Platform API tokens. The token never appears in logs, errors or option
reads.

### Read-only access

Read-only access is enforced by Turso, not by inspecting SQL: use a read-only
Turso token (`turso db tokens create --read-only`) for Turso Cloud, or
`read_only = true` for a local file.

## Health checks

| Endpoint | |
|---|---|
| `GET /healthz` | `204` while the process serves |
| `GET /readyz` | `204` once every database answers a query, `503` otherwise |

## Shutdown

On SIGTERM the service stops accepting work, closes every session and drains
in-flight requests within `server.shutdown_grace_seconds`, then exits `0`.

## Logging and tracing

Logs go to stderr, filtered by `RUST_LOG` (default: `info`), as text, or as one
JSON object per line with `GRAINLIFT_TURSO_LOG_FORMAT=json`. Traces are
exported over OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` (or
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`) is set. Neither ever contains SQL,
values or credentials.

## Containers

The [`Dockerfile`](../Dockerfile) builds a slim image that runs as an
unprivileged user (uid 10001) and serves `/etc/grainlift-turso/turso.toml`:

```bash
docker build -t grainlift-turso .
docker run -p 8080:8080 -e TURSO_APP_TOKEN \
  -v ./turso.toml:/etc/grainlift-turso/turso.toml:ro grainlift-turso
```

Inside a container, the configuration must listen on `0.0.0.0` with
`allow_insecure_remote = true`. The HTTP listener is plaintext (as in
`grainlift-server`), so put a load balancer or sidecar that terminates TLS in
front of it.

## Scaling

Sessions, open transactions and open results live in the process that created
them, so route every request from a client to the same process (sticky
sessions). Scale out by giving different clients different processes, not by
load-balancing one client's requests.

## The development host

For local development, `grainlift-turso` with no subcommand serves the database
named by `TURSO_DATABASE_URL` (with `TURSO_AUTH_TOKEN` for Turso Cloud, or
`GRAINLIFT_TURSO_READ_ONLY=true` for a read-only file) as the target `turso` on
loopback. It requires a bearer token (printed at startup unless
`GRAINLIFT_TOKEN` is set); `--auth anonymous` also admits clients without one,
and `--host mtls` serves mutual TLS. Run `grainlift-turso --help` for the
options.
