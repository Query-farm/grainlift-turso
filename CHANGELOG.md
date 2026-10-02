# Changelog

All notable changes to this project are documented here.

## 0.1.0 (2026-10-02)

The first release: a Grainlift ADBC service for Turso databases.

### Databases

- Local database files on the embedded Turso Database engine, driven through
  `turso_sdk_kit`, and Turso Cloud databases over SQL-over-HTTP
  (`turso_serverless`).
- Any number of databases per process, each a named Grainlift target.
- Clients may send their own Turso Cloud token (`turso.auth_token`), so Turso
  applies each token's permissions.

### ADBC

- Queries stream as typed Arrow batches (at most 1,024 rows, about 768 KiB),
  typed from declared column types or the first batch.
- `ExecuteUpdate`, prepared statements and parameters (`?`, `?NNN`, `:name`,
  `@name`, `$name`), transactions, atomic multi-row updates and bulk
  ingestion (`create`, `append`, `replace`, `create_append`, temporary).
- `GetObjects`, `GetTableSchema`, `GetTableTypes`, `GetInfo`.
- Statement and connection cancellation.

### Production

- `grainlift-turso serve --config FILE`: Grainlift's configuration format
  (static tokens or JWT, OAuth discovery, target permissions, TCP/mTLS,
  limits) with a `[targets.NAME]` table per database. Turso tokens come from
  environment variables or files.
- `grainlift-turso check --config FILE` validates a file and connects to every
  database.
- Per-operation deadlines (`operation_timeout_seconds`) and cancellation for
  local and Turso Cloud operations; a busy timeout for concurrent local
  writers.
- `/healthz` and `/readyz`, graceful SIGTERM drain, text or JSON logs, OTLP
  tracing.
- Release binaries for Linux (x86_64, ARM64), macOS (Apple Silicon, Intel)
  and Windows, and a multi-architecture container image,
  `ghcr.io/query-farm/grainlift-turso`, running as a non-root user.

### Turso Cloud's Turso Database engine

- Tested against Turso Cloud databases on both libSQL and the Turso Database
  engine.
- `transaction_mode = "concurrent"` opens transactions with
  `BEGIN CONCURRENT` for parallel writers; conflicts and schema-change aborts
  report the retryable SQLSTATE `40001`.
- A transaction Turso rolled back on its own is never reported as committed.

### Performance

- Turso Cloud results stream from the cursor endpoint: 100 MB read at 40 MB
  peak memory instead of 138 MB, 4× faster.
- Turso Cloud ingestion sends multi-row `INSERT`s in size-bounded requests:
  20,000 rows in 0.65 s instead of 4.0 s.
- Bound parameters stay as Arrow and convert one batch at a time.

### Types

- Ingested decimals are stored as numbers (`NUMERIC`), so comparisons and
  arithmetic work in Turso; they read back as doubles.
