<p align="center">
  <a href="https://github.com/Query-farm/grainlift">
    <img src="https://raw.githubusercontent.com/Query-farm/grainlift-turso/main/docs/grainlift-logo.svg" alt="Grainlift logo" width="200">
  </a>
</p>

<h1 align="center">grainlift-turso</h1>

<p align="center">
  <a href="https://turso.tech">Turso</a> databases for every ADBC client: DuckDB, Python, Go, Rust and more.<br>
  Local files and Turso Cloud, with typed Arrow results, transactions and bulk ingestion.<br>
  A <a href="https://github.com/Query-farm/grainlift">Grainlift</a> worker, built by <a href="https://query.farm">🚜 Query.Farm</a>
</p>

<p align="center">
  <a href="https://github.com/Query-farm/grainlift-turso/actions/workflows/ci.yml"><img src="https://github.com/Query-farm/grainlift-turso/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License: Apache-2.0"></a>
  <img src="https://img.shields.io/badge/rust-1.97%2B-orange.svg" alt="Rust 1.97+">
  <a href="https://github.com/Query-farm/grainlift"><img src="https://img.shields.io/badge/Grainlift-0.4.2-2f7d32.svg" alt="Grainlift 0.4.2"></a>
</p>

---

> **SQL passes straight through to Turso; nothing is emulated.** Clients load
> the native [Grainlift ADBC driver](https://github.com/Query-farm/grainlift),
> which talks to this service over VGI-RPC, and this service talks to Turso
> with Turso's own Rust clients. The server chooses the database: a client can
> never point it at another host. Every client authenticates with a bearer
> token or mTLS. A client may bring its own Turso token, which is never logged,
> echoed in an error or readable back as an option. Read-only access is
> enforced by Turso itself (a read-only token, or a file opened read-only), not
> by inspecting SQL. Every Turso operation has a deadline and can be cancelled,
> so a stuck query or an unresponsive network cannot wedge a session.

## Run

For development, the service serves one database, chosen by
`TURSO_DATABASE_URL`, on loopback (for production, see [Production](#production)):

```bash
# A local database file, opened in-process by the Turso Database engine
TURSO_DATABASE_URL=app.db cargo run --release

# A Turso Cloud database
export TURSO_DATABASE_URL=libsql://my-db-my-org.turso.io
export TURSO_AUTH_TOKEN=$(turso db tokens create my-db)
cargo run --release
```

It listens on `http://127.0.0.1:8080` and prints a bearer token for clients,
unless `GRAINLIFT_TOKEN` already holds one. Clients connect to target `turso`.

Clients need the native Grainlift ADBC driver, built once from the same
Grainlift release this crate pins:

```bash
git clone --branch v0.4.2 https://github.com/Query-farm/grainlift.git ../grainlift
(cd ../grainlift && cargo build --locked -p adbc-driver-grainlift)
export GRAINLIFT_DRIVER=$PWD/../grainlift/target/debug/libadbc_driver_grainlift.dylib  # .so on Linux
```

### From SQL (Haybarn or DuckDB)

[`examples/query.sql`](examples/query.sql) bulk-loads a DuckDB result into
Turso, updates it, queries it with parameters, then attaches the database and
joins its table against local data:

```bash
export GRAINLIFT_TOKEN=...   # the token the service printed
uvx haybarn-cli < examples/query.sql
```

```sql
SET VARIABLE turso = (SELECT adbc_connect({'secret': 'turso'}));

SELECT * FROM adbc_insert(getvariable('turso')::BIGINT, 'cities', (SELECT ...), mode := 'replace');
CALL adbc_execute(getvariable('turso')::BIGINT, 'UPDATE cities SET ...');   -- rows_affected: 1

ATTACH 'http://127.0.0.1:8080' AS t (TYPE adbc, SECRET 'turso');
SELECT c.name, k.country, c.population FROM t.cities c JOIN countries k ON c.country = k.code;
```

```
┌─────────┬─────────┬────────────┐
│  name   │ country │ population │
│ varchar │ varchar │   int64    │
├─────────┼─────────┼────────────┤
│ Lima    │ Peru    │   10092000 │
│ Pune    │ India   │    7166000 │
│ Rome    │ Italy   │    2873000 │
│ Oslo    │ Norway  │     710000 │
└─────────┴─────────┴────────────┘
```

This needs [`adbc_scanner`](https://github.com/Query-farm/adbc_scanner) at
`2d696f8` or newer, which is what [Haybarn](https://github.com/Query-farm-haybarn/haybarn)
1.5.5 installs (`FORCE INSTALL adbc_scanner FROM community` updates an older
copy). The `adbc_scanner` that stock DuckDB downloads predates two fixes this
service relies on: its `adbc_insert` deadlocks with any Grainlift service, and
its `adbc_execute` reports 0 rows affected for everything.

### From Python

Any ADBC driver manager works. With `adbc_driver_manager` and its DB-API:

```python
import os
import adbc_driver_manager.dbapi as dbapi

with dbapi.connect(
    driver=os.environ["GRAINLIFT_DRIVER"],
    entrypoint="AdbcDriverGrainliftInit",
    db_kwargs={
        "grainlift.uri": "http://127.0.0.1:8080",
        "grainlift.target": "turso",
        "grainlift.auth.bearer_token": os.environ["GRAINLIFT_TOKEN"],
    },
) as conn, conn.cursor() as cur:
    cur.execute("CREATE TABLE IF NOT EXISTS readings (sensor TEXT, value REAL)")
    cur.executemany("INSERT INTO readings VALUES (?, ?)", [("a", 1.5), ("b", 2.5)])
    conn.commit()
    cur.execute("SELECT sensor, avg(value) AS mean FROM readings GROUP BY sensor")
    print(cur.fetch_arrow_table())
```

The DB-API turns autocommit off, so that runs in a real Turso transaction
until `commit()`.

## Production

`grainlift-turso serve` runs the production host from a configuration file,
and `grainlift-turso check` validates the same file, reads its secrets and
connects to every database without serving:

```bash
grainlift-turso check --config turso.toml
grainlift-turso serve --config turso.toml
```

The file is Grainlift's own server configuration (`[server]`, `[auth]`,
`[tcp]`, with the same fields, defaults and validation as the `grainlift-server`
binary) plus one `[targets.NAME]` table per Turso database. Clients choose a
database by its target name. [`turso.example.toml`](turso.example.toml) is an
annotated example, and a test keeps it valid:

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

| Target setting | Default | |
|---|---|---|
| `url` | | A `libsql://` or `https://` Turso Cloud URL, or a local file |
| `auth_token_env`, `auth_token_file` | | Where the Turso Cloud token comes from. Tokens never live in the file. |
| `allow_client_auth_token` | `false` | Let clients send their own Turso token (see [Authentication](#authentication)) |
| `read_only` | `false` | Open a local file read-only. Two targets may not share a file. |
| `operation_timeout_seconds` | `60` | Longest one operation may run. Must be shorter than `server.driver_operation_timeout_seconds`. |
| `busy_timeout_ms` | `5000` | How long a local write waits for another writer's lock |

The host provides what `grainlift-server` does: static bearer tokens or JWT
validation (`[auth.jwt]`), OAuth discovery for browser and CLI sign-in
(`[auth.oauth]`), per-principal target permissions, an optional raw TCP or mTLS
listener (`[tcp]`), CORS, session limits and idle expiry, and graceful
shutdown. On SIGTERM it stops accepting work, closes every session and drains
within `server.shutdown_grace_seconds`. Iroh is not offered.

| Endpoint | |
|---|---|
| `GET /healthz` | `204` while the process serves |
| `GET /readyz` | `204` once every database answers a query, `503` otherwise |

**Logging and tracing.** Logs go to stderr, filtered by `RUST_LOG`, as text or,
with `GRAINLIFT_TURSO_LOG_FORMAT=json`, one JSON object per line. Traces are
exported over OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Neither ever
contains SQL, values or credentials.

**Container.** The [`Dockerfile`](Dockerfile) builds a slim image that runs as
an unprivileged user (uid 10001) and serves `/etc/grainlift-turso/turso.toml`:

```bash
docker build -t grainlift-turso .
docker run -p 8080:8080 -e TURSO_APP_TOKEN \
  -v ./turso.toml:/etc/grainlift-turso/turso.toml:ro grainlift-turso
```

Inside a container the configuration must listen on `0.0.0.0` with
`allow_insecure_remote = true`, behind a load balancer or sidecar that
terminates TLS: like `grainlift-server`, the HTTP listener is plaintext.

**One process per set of clients.** Sessions, open transactions and open
results live in the process that created them, so route every request from a
client to the same process (sticky sessions). Scale out by sharding clients,
not by load-balancing requests.

## How it reaches Turso

Turso's Rust crates cover both kinds of database, behind one synchronous
interface in [`src/db.rs`](src/db.rs):

| URL | Client | What it is |
|---|---|---|
| a path, `file:` path or `:memory:` | [`turso_sdk_kit`](https://crates.io/crates/turso_sdk_kit) | The Turso Database engine itself, in process: a rewrite of SQLite in Rust |
| `libsql://`, `https://`, `http://` | [`turso_serverless`](https://crates.io/crates/turso_serverless) | Turso Cloud over its SQL-over-HTTP protocol |

The local engine is driven through `turso_sdk_kit`, the layer beneath the
`turso` crate, because it exposes what a server needs and `turso` hides:
interrupting a running statement, and a busy timeout for concurrent writers.
Its synchronous mode also suits Grainlift's synchronous backend traits, so a
local query runs on the calling thread with no async runtime in between.

Rust is the natural fit on both sides: Turso's engine and clients are Rust
crates, and Grainlift's reference server library, `grainlift-server`, is Rust
and lives beside the native driver in the same repository. Nothing crosses an
FFI boundary between the database and the Arrow batches.

`turso_serverless` is async, and Grainlift may call a backend from inside its
own Tokio runtime, so Turso Cloud requests run on a runtime owned by the
backend while the calling thread waits with a runtime-agnostic executor rather
than nesting runtimes.

## Surface

| ADBC | Turso |
|---|---|
| Queries | SQL passes straight through. Results stream in Arrow batches of at most 1,024 rows and about 768 KiB, under Grainlift's 1 MiB batch limit. |
| `ExecuteUpdate` | Rows changed. With several rows of parameters bound, the statement runs once per row, all in one transaction. Rows a statement returns (`RETURNING`) are discarded. |
| Prepare and parameters | `?`, `?NNN`, `:name`, `@name` and `$name`, numbered as SQLite numbers them. Bind one row for a query, any number for an update. |
| Transactions | Turn off `adbc.connection.autocommit`; the next statement opens a transaction that lasts until commit or rollback. |
| Bulk ingestion | `adbc.ingest.target_table` in the `create`, `append`, `replace` and `create_append` modes, plus `adbc.ingest.temporary`. Each ingestion is atomic. |
| Metadata | `GetObjects`, `GetTableSchema`, `GetTableTypes` and `GetInfo`, laid out like the ADBC SQLite driver's: one catalog, `main`, holding one unnamed schema. |
| Cancellation | `AdbcStatementCancel` and `AdbcConnectionCancel` stop the running operation; it fails with ADBC `CANCELLED`. |
| Not implemented | Partitions, Substrait and statistics return ADBC `NOT_IMPLEMENTED`. |

### Column types

Turso types values, not columns, but every Arrow column needs one type. A
result column's type comes from:

1. **its declared type**, when it reads a table column. SQLite's affinity rules
   apply: `INTEGER` → int64, `TEXT`/`VARCHAR` → utf8, `REAL`/`DOUBLE` → float64,
   `BLOB` → binary, `BOOLEAN` → boolean, `NUMERIC`/`DECIMAL` → float64, and
   anything else, such as `DATE` or `TIMESTAMP`, → utf8, because Turso stores
   dates as text or numbers;
2. otherwise, **the values in the first batch**: int64, float64 for mixed
   integers and reals, utf8 if any value is text, binary if any is a blob, and
   int64 if every value is NULL (the ADBC SQLite driver's convention).

Later values must fit the chosen type. Lossless conversions are made, such as
an integer in a `REAL` column or a number in a `TEXT` column. Anything else is
an `INVALID_DATA` error that names the column and the row, rather than a
silently coerced value:

```
Column "n" has type Int64 but row 2 holds text; CAST the column in the query to choose its type
```

`ExecuteSchema` answers from declared types without running the statement. When
a column has none, a read-only query (`SELECT`, `VALUES`, or a `WITH` or
`EXPLAIN` that names no write) runs to read its first batch; any other
statement must be executed to learn its result types.

Bound Arrow parameters map integers, floats, booleans (0 and 1), strings and
binary to the matching Turso values. Dates, timestamps and decimals bind as
text (`2026-10-01T12:00:00`), which Turso's date functions understand.
Ingestion declares each column so that it reads back as the same Arrow type;
decimals, dates and times read back as text.

## Design notes

**Every operation has a deadline and can be cancelled.** Grainlift runs each
session's calls on one worker thread, and its own operation timeout only stops
*waiting*: the stuck call keeps running and the session stays wedged behind it.
So each Turso operation here (an execute, one fetched batch, one catalog query)
gets its own deadline, `operation_timeout_seconds`, and a cancel request from
the client stops whichever operation is running ([`src/ops.rs`](src/ops.rs)).
The local engine is interrupted at its next step, like `sqlite3_interrupt`;
a Turso Cloud request is simply dropped, which aborts it. Both fail with a
structured ADBC `TIMEOUT` or `CANCELLED`, and the connection stays usable.

An interrupt applies to the whole local connection, as `sqlite3_interrupt`
does, so it also closes that connection's other open results (they then report
that they were closed); otherwise the engine's interrupt flag would stay set
and fail every later statement on the connection.

The deadline is per operation, not per result, so a client reading a large
result slowly is never cut off between batches. That is also why the engine's
own per-statement timeout is not used: it starts at a statement's first step
and would interrupt a slow reader.

**Read-only local files are enforced twice.** Turso shares one open database
per path within a process and ignores a second opener's read-only flag, so a
read-only target opened while the same file was open read-write elsewhere in
the process could accept writes (a test caught this). The file is opened
read-only, every connection also runs `PRAGMA query_only = 1`, and two targets
may not use the same file.

**Memory is bounded on both sides.** Results stream in bounded batches. Bound
parameters stay as Arrow and are converted to Turso values one batch at a time
while the statement runs, so a large binding costs its Arrow size plus one
batch; Grainlift caps the Arrow size itself (`server.max_bind_bytes`). A bound
stream is read during execution, once. Catalog queries refuse results beyond
100,000 rows instead of holding them.

**Turso Cloud results stream; they are not buffered.** `turso_serverless` reads
a query's whole HTTP response before returning a row. Outside a transaction,
this service instead reads Turso Cloud's cursor endpoint as newline-delimited
JSON, one batch at a time ([`src/cursor.rs`](src/cursor.rs)), and HTTP
backpressure holds the server while a client is not fetching. Reading 100,000
rows of 1 KB text (about 100 MB):

| | peak memory | time |
|---|---|---|
| buffered by `turso_serverless` | 138 MB | 6.2 s |
| streamed from the cursor endpoint | **40 MB** | **1.4 s** |

A streamed query runs on its own server-side stream, which is a separate
database connection, so it cannot see the connection's `TEMP` tables. Inside a
transaction, queries go through the transaction's own stream instead, see its
uncommitted rows, and arrive whole.

**Bulk ingestion to Turso Cloud is batched three ways.** Rows go in multi-row
`INSERT`s of up to 1,000 rows or 32,766 parameters (Turso Cloud's limit,
measured), many statements per request, in requests of up to about 4 MiB. Each
statement is also capped by size, so a few very wide rows cannot produce one
enormous statement. From a laptop to `aws-us-east-1`:

| rows | one statement per row | multi-row `INSERT`s |
|---|---|---|
| 20,000 | 4.0 s | **0.65 s** |
| 200,000 | | **3.9 s** (≈ 52,000 rows/s) |

Locally the engine is in process, so ingestion runs one prepared statement per
row inside a transaction, with no round trips to save.

**Writes are atomic.** A multi-row update or an ingestion runs in its own
transaction, or inside the client's open one, so a constraint failure on any
row leaves none of the others behind.

**Transactions open lazily.** With autocommit off, `BEGIN` is sent before the
first statement that needs it, not right after the previous commit. On Turso
Cloud a transaction lives in a server-side stream that expires when idle, so
an idle client then holds nothing open. Keep transactions short anyway.

**`ExecuteUpdate` accepts any statement.** The `turso` crate's own `execute`
refuses a statement that returns rows, so a `SELECT`, a `PRAGMA` or an
`INSERT … RETURNING` would fail. This service steps such a statement to
completion, discards its rows and returns the change count, as ADBC expects.

**Turso reports `BLOB` for computed CTE columns.** Where SQLite reports no
declared type for `WITH n(x) AS (SELECT 0 …) SELECT x FROM n`, Turso reports
`BLOB`, which would have made every recursive CTE a binary column. A `BLOB`
declaration is therefore treated as a hint: such columns are typed by their
values and fall back to binary when every value is NULL.

**A new query forgets earlier parameters.** Python's DB-API reuses one ADBC
statement per cursor and binds only when a call has parameters, so
`executemany(...)` followed by `execute("SELECT …")` must not run the `SELECT`
with the insert's rows. Setting a query clears the bound parameters.

**Errors keep their meaning.** Turso's error categories map to ADBC statuses
and SQLSTATEs, so clients can tell a bad query from a lost connection:

| Turso | ADBC status | SQLSTATE |
|---|---|---|
| SQL error (`no such table`, syntax) | `INVALID_ARGUMENT` | `42000` |
| constraint violation | `INTEGRITY` | `23000` |
| read-only token or file | `UNAUTHORIZED` | `25006` |
| rejected or expired Turso token | `UNAUTHENTICATED` | `28000` |
| busy (another writer held the lock past `busy_timeout_ms`) | `TIMEOUT` | `40001` |
| operation deadline expired | `TIMEOUT` | `HYT00` |
| cancelled | `CANCELLED` | `HY008` |
| network failure | `IO` | `58000` |
| value does not fit its column | `INVALID_DATA` | `22000` |

## Authentication

Two separate credentials are involved, and neither substitutes for the other.

**Clients to this service.** In production, `[auth]` configures it (see
[Production](#production)). The development host binds to loopback and
requires a bearer token by default; mTLS and anonymous access are options:

```bash
cargo run --release -- --help
cargo run --release -- --host mtls --port 8443 \
  --tls-cert server.pem --tls-key server-key.pem \
  --client-ca clients-ca.pem --client-uri spiffe://example.org/client
```

`--auth anonymous` also admits clients without a token, as one shared
principal. Use it only for a read-only database. See also Grainlift's
[security guide](https://github.com/Query-farm/grainlift/blob/main/docs/security.md).

**This service to Turso Cloud.** A target's `auth_token_env` or
`auth_token_file` (`TURSO_AUTH_TOKEN` for the development host) gives the
service its own token. Alternatively, or as well, with `allow_client_auth_token`
each client can send its own token in the `turso.auth_token` database option,
which the driver forwards:

```sql
CREATE SECRET turso (TYPE adbc, DRIVER '...', URI 'http://127.0.0.1:8080', SCOPE 'http://127.0.0.1:8080',
    EXTRA_OPTIONS MAP {'grainlift.target': 'turso', 'grainlift.auth.bearer_token': '...',
                       'turso.auth_token': '...'});
```

That client's connection then authenticates as that token, so Turso applies
its permissions: a read-only token gets a read-only connection, and its writes
fail with `UNAUTHORIZED`. When the target has no token of its own, every
client must send one. Use database tokens (`turso db tokens create <db>` or `turso group tokens
create <group>`), not Platform API tokens. The token travels with each
connection, so serve HTTPS or mTLS anywhere but loopback.

For a local file, `read_only = true` (`GRAINLIFT_TURSO_READ_ONLY=true` for the
development host) opens it read-only.

## Limitations

- **A local file has one writer at a time,** as in SQLite. Concurrent writers
  wait up to `busy_timeout_ms` for the lock, and Turso's busy handler retries
  on a timer rather than queueing writers fairly, so under heavy write
  contention a writer can still fail with a retryable busy `TIMEOUT`. Clients
  should retry those; nothing the failed statement attempted is committed. A
  32-client, two-minute soak test exercises exactly this.
- **An interrupted statement inside a transaction** may leave the transaction
  open or rolled back, as in SQLite; roll back and retry.
- **Idle Turso Cloud transactions expire** with their server-side stream.
- **`TEMP` tables are invisible to streamed Cloud queries,** which run on their
  own stream (see above).
- **Through `adbc_scanner`'s `ATTACH`,** tables created after attaching appear
  only once its catalog cache is cleared, and `DROP TABLE` is not supported;
  use `adbc_execute` for DDL.
- **Turso Cloud's newer engine is untested.** The Cloud tests ran against a
  libSQL-backed database. `turso_serverless` speaks the same protocol to both.

## Developing

```bash
git clone https://github.com/Query-farm/grainlift-turso
cd grainlift-turso

cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
GRAINLIFT_DRIVER=/path/to/libadbc_driver_grainlift.dylib cargo test --locked
```

`grainlift-server` is pinned to the `v0.4.2` revision of
[Query-farm/grainlift](https://github.com/Query-farm/grainlift), and the native
driver must come from the same revision, since the protocol version must match.
Never commit a path override for a local checkout.

| File | |
|---|---|
| [`src/db.rs`](src/db.rs) | One interface over both Turso clients, the shared runtime, error mapping |
| [`src/ops.rs`](src/ops.rs) | Per-operation deadlines and cancellation |
| [`src/config.rs`](src/config.rs) | The production configuration file |
| [`src/host.rs`](src/host.rs) | The production host: listeners, authentication, health, shutdown, logging |
| [`src/cursor.rs`](src/cursor.rs) | Streamed Turso Cloud results from the cursor endpoint |
| [`src/types.rs`](src/types.rs) | Turso values to Arrow, Arrow parameters to Turso, SQL parameter numbering |
| [`src/statement.rs`](src/statement.rs) | Queries, updates, binding, ingestion |
| [`src/connection.rs`](src/connection.rs) | Transactions and catalog metadata |
| [`src/session.rs`](src/session.rs) | A connection's transaction mode |
| [`src/main.rs`](src/main.rs) | The `grainlift-turso` command |

## Tests

```bash
cargo test --locked
```

| Suite | Tests | Runs against |
|---|---|---|
| unit (`src/`) | 18 | type mapping, parameter numbering, configuration, deadlines, error wording |
| [`tests/backend.rs`](tests/backend.rs) | 14 | the backend, called directly, on local database files |
| [`tests/limits.rs`](tests/limits.rs) | 9 | deadlines and cancellation (local and Turso Cloud), slow readers, concurrent writers, lazily read bindings |
| [`tests/native.rs`](tests/native.rs) | 5 | the native driver over HTTP, through the C ABI |
| [`tests/host.rs`](tests/host.rs) | 3 | the production host and the `serve` and `check` commands, including SIGTERM |
| [`tests/soak.rs`](tests/soak.rs) | 1 | many concurrent clients doing mixed work; exact totals and no leaked sessions |
| [`tests/cloud.rs`](tests/cloud.rs) | 4 | a real Turso Cloud database |

The Turso Cloud deadline and cancellation tests use a local server that
accepts connections and never answers. The soak test runs for
`GRAINLIFT_TURSO_SOAK_SECONDS` (default 5) with `GRAINLIFT_TURSO_SOAK_CLIENTS`
clients (default 12). A 32-client, two-minute run did 17,424 operations and
committed 350,995 rows, exactly as counted; 4 writes were abandoned as busy and
retried correctly, and the whole test process (server and clients) peaked at
273 MB.

The native tests skip unless `GRAINLIFT_DRIVER` names the driver library; set
`GRAINLIFT_REQUIRE_NATIVE=1` to fail instead. The Cloud tests skip unless
`TURSO_TEST_DATABASE_URL` and `TURSO_TEST_AUTH_TOKEN` are set, and the
client-token test also needs `TURSO_TEST_READ_ONLY_TOKEN`. They create and drop
their own tables.

## CI

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs on every push, on
Linux and macOS. It checks formatting and Clippy, builds the native driver from
the revision pinned in `Cargo.lock`, and runs every suite with
`GRAINLIFT_REQUIRE_NATIVE=1`. The Cloud suite runs when the repository's
`TURSO_TEST_*` secrets are set. It then starts the service and runs
[`examples/query.sql`](examples/query.sql) through the Haybarn CLI, so the
example in this README is checked against a live service. Separate jobs build
the container image and probe a running container (health, readiness, the
unprivileged user, a clean `docker stop`), and run `cargo audit`, which fails
on any dependency with a known vulnerability.

## Dependencies

`cargo audit` currently reports no vulnerabilities and three warnings, all in
transitive dependencies and accepted: `bincode` 1 (unmaintained, via
VGI-RPC), `paste` (unmaintained, via Iroh's networking in `grainlift-server`),
and `lru` 0.16 (`RUSTSEC-2026-0253`, unsound `LruCache::pop` under a panic),
which arrives through `tantivy`, the engine behind Turso's full-text search.
Dropping it would remove full-text search.

## License

Copyright © 2026 [Query Farm LLC](https://query.farm)

Released under the **Apache License 2.0**; see [LICENSE](LICENSE).

The data in a Turso database belongs to its owner, and Turso Cloud is subject
to [Turso's terms](https://turso.tech/terms-of-service). See [NOTICE](NOTICE).
This project is not affiliated with or endorsed by Turso.

---

<p align="center">
  Built with <a href="https://github.com/Query-farm/grainlift">Grainlift</a><br>
  by <a href="https://query.farm">🚜 Query.Farm</a>
</p>
