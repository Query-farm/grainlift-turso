# Developing grainlift-turso

## Building and checking

Requires Rust 1.97 or newer.

```bash
git clone https://github.com/Query-farm/grainlift-turso
cd grainlift-turso

cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
GRAINLIFT_DRIVER=/path/to/libadbc_driver_grainlift.dylib cargo test --locked
```

`grainlift-server` is pinned to the `v0.4.2` revision of
[Query-farm/grainlift](https://github.com/Query-farm/grainlift), and the native
driver must come from the same revision, since the protocol versions must
match. Never commit a path override for a local checkout.

## Source layout

| File | |
|---|---|
| [`src/db.rs`](../src/db.rs) | One interface over both Turso clients, the shared runtime, error mapping |
| [`src/ops.rs`](../src/ops.rs) | Per-operation deadlines and cancellation |
| [`src/cursor.rs`](../src/cursor.rs) | Streamed Turso Cloud results from the cursor endpoint |
| [`src/types.rs`](../src/types.rs) | Turso values to Arrow, Arrow parameters to Turso, SQL parameter numbering |
| [`src/statement.rs`](../src/statement.rs) | Queries, updates, binding, ingestion |
| [`src/connection.rs`](../src/connection.rs) | Transactions and catalog metadata |
| [`src/session.rs`](../src/session.rs) | A connection's transaction mode |
| [`src/config.rs`](../src/config.rs) | The production configuration file |
| [`src/host.rs`](../src/host.rs) | The production host: listeners, authentication, health, shutdown, logging |
| [`src/main.rs`](../src/main.rs) | The `grainlift-turso` command |

## Tests

```bash
cargo test --locked
```

| Suite | Tests | Runs against |
|---|---|---|
| unit (`src/`) | 18 | type mapping, parameter numbering, configuration, deadlines, error wording |
| [`tests/backend.rs`](../tests/backend.rs) | 15 | the backend, called directly, on local database files |
| [`tests/limits.rs`](../tests/limits.rs) | 9 | deadlines and cancellation (local and Turso Cloud), slow readers, concurrent writers, lazily read bindings |
| [`tests/native.rs`](../tests/native.rs) | 5 | the native driver over HTTP, through the C ABI |
| [`tests/host.rs`](../tests/host.rs) | 3 | the production host and the `serve` and `check` commands, including SIGTERM |
| [`tests/soak.rs`](../tests/soak.rs) | 1 | many concurrent clients doing mixed work; exact totals and no leaked sessions |
| [`tests/cloud.rs`](../tests/cloud.rs) | 5 | a real Turso Cloud database, on either engine |
| [`tests/concurrent.rs`](../tests/concurrent.rs) | 1 | concurrent transactions on the Turso Database engine: parallel writers, conflicts, rollbacks, schema changes |

Some suites need outside resources and skip without them:

| Variable | Needed by |
|---|---|
| `GRAINLIFT_DRIVER` | the native, host and soak tests (path to the driver library); set `GRAINLIFT_REQUIRE_NATIVE=1` to fail instead of skipping |
| `TURSO_TEST_DATABASE_URL`, `TURSO_TEST_AUTH_TOKEN`, `TURSO_TEST_READ_ONLY_TOKEN` | the Turso Cloud tests (either engine) |
| `TURSO_TEST_TURSODB_URL`, `TURSO_TEST_TURSODB_AUTH_TOKEN` | the concurrent-transaction tests (a Turso Database engine database) |

The Cloud tests create and drop their own tables. The Turso Cloud deadline and
cancellation tests use a local server that accepts connections and never
answers.

The soak test runs for `GRAINLIFT_TURSO_SOAK_SECONDS` (default 5) with
`GRAINLIFT_TURSO_SOAK_CLIENTS` clients (default 12). A 32-client, two-minute
run did 17,424 operations and committed 350,995 rows, exactly as counted; 4
writes were abandoned as busy and retried correctly, and the whole test process
(server and clients) peaked at 273 MB.

## Continuous integration

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs on every push:

- **Tests** on Linux and macOS: formatting, Clippy, the native driver built
  from the revision pinned in `Cargo.lock`, every suite with
  `GRAINLIFT_REQUIRE_NATIVE=1` (the Cloud suite against the libSQL test
  database), and [`examples/query.sql`](../examples/query.sql) run through the
  Haybarn CLI against a live service.
- **Turso Cloud, Turso Database engine (preview)**: the Cloud and
  concurrent-transaction suites against a new-engine database. It does not
  block merges while Turso calls the feature a preview, and runs as one job
  because schema changes on that engine abort other connections' concurrent
  transactions.
- **Container image**: builds the image and probes a running container
  (health, readiness, the unprivileged user, a clean `docker stop`).
- **Dependency audit**: `cargo audit`, failing on any known vulnerability.
