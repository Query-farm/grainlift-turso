# How grainlift-turso works

This document explains the design: how the service reaches Turso, how Turso
values become Arrow columns, how it keeps sessions healthy, and what it
deliberately does not do. For using the service, see the
[README](../README.md); for running it, the [deployment guide](deployment.md).

## Architecture

```
your application ─ADBC─▶ Grainlift driver ─VGI-RPC─▶ grainlift-turso ─▶ Turso
                         (native library)            (this service)     local file or Turso Cloud
```

SQL passes straight through to Turso; nothing is emulated. The server chooses
the database, so a client can never point it at another host.

Turso's Rust crates cover both kinds of database, behind one synchronous
interface in [`src/db.rs`](../src/db.rs):

| URL | Client | What it is |
|---|---|---|
| a path, `file:` path or `:memory:` | [`turso_sdk_kit`](https://crates.io/crates/turso_sdk_kit) | The Turso Database engine itself, in process: a rewrite of SQLite in Rust |
| `libsql://`, `https://`, `http://` | [`turso_serverless`](https://crates.io/crates/turso_serverless) | Turso Cloud over its SQL-over-HTTP protocol |

The local engine is driven through `turso_sdk_kit`, the layer beneath the
`turso` crate, because it exposes what a server needs and `turso` hides:
interrupting a running statement, and a busy timeout for concurrent writers.
Its synchronous mode suits Grainlift's synchronous backend traits, so a local
query runs on the calling thread with no async runtime in between.

`turso_serverless` is async, and Grainlift may call a backend from inside its
own Tokio runtime, so Turso Cloud requests run on a runtime owned by the
backend while the calling thread waits with a runtime-agnostic executor.

Rust is the natural fit on both sides: Turso's engine and clients are Rust
crates, and Grainlift's reference server library, `grainlift-server`, is Rust
and lives beside the native driver. Nothing crosses an FFI boundary between the
database and the Arrow batches.

## ADBC surface

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

## Column types

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

`ExecuteSchema` answers from declared types without running the statement.
When a column has none, a read-only query (`SELECT`, `VALUES`, or a `WITH` or
`EXPLAIN` that names no write) runs to read its first batch; any other
statement must be executed to learn its result types.

Bound Arrow parameters map integers, floats, booleans (0 and 1), strings and
binary to the matching Turso values. Dates, timestamps and decimals bind as
text (`2026-10-01T12:00:00`), which Turso's date functions understand.
Ingestion declares each column so that it reads back as the same Arrow type.
Decimals go into a `NUMERIC` column as their exact text, which Turso stores as
integers or doubles, so numeric comparisons and sums work; they read back as
doubles. Dates and times are stored, and read back, as ISO text.

## Design decisions

### Every operation has a deadline and can be cancelled

Grainlift runs each session's calls on one worker thread, and its own operation
timeout only stops *waiting*: the stuck call keeps running and the session
stays wedged behind it. So each Turso operation here (an execute, one fetched
batch, one catalog query) gets its own deadline, `operation_timeout_seconds`,
and a cancel request from the client stops whichever operation is running
([`src/ops.rs`](../src/ops.rs)). The local engine is interrupted at its next
step, like `sqlite3_interrupt`; a Turso Cloud request is dropped, which aborts
it. Both fail with a structured ADBC `TIMEOUT` or `CANCELLED`, and the
connection stays usable.

An interrupt applies to the whole local connection, as `sqlite3_interrupt`
does, so it also closes that connection's other open results (they then report
that they were closed); otherwise the engine's interrupt flag would stay set
and fail every later statement on the connection.

The deadline is per operation, not per result, so a client reading a large
result slowly is never cut off between batches. That is also why the engine's
own per-statement timeout is not used: it starts at a statement's first step
and would interrupt a slow reader.

### Concurrent writes on Turso Cloud's new engine

Turso Cloud can run a database on the Turso Database engine instead of libSQL
(`turso db create --tursodb`, after enabling it under the organization's
Settings → General). That engine offers `BEGIN CONCURRENT`: transactions write
in parallel, and conflicts are detected per row. With
`transaction_mode = "concurrent"`, every transaction this service opens (a
client's, with autocommit off, and its own atomic multi-row writes) begins that
way. Startup refuses the setting for a libSQL database or a local file.

A transaction can then lose to another: it wrote a row another committed
first, or another connection changed the schema while it ran. Turso rolls the
loser back at once, and the client gets SQLSTATE `40001`, the standard signal
to retry. Measured against Turso Cloud: two writers on different rows both
commit, and on the same row the second fails immediately at its `UPDATE`. With
plain `BEGIN` on that engine, writers are serialized, and Turso rolls back an
idle lock holder ("the stream was idle for too long").

### A transaction Turso ended is never reported as committed

When Turso rolls a transaction back on its own (a conflict, or an expired Turso
Cloud stream), a `commit` that finds nothing to commit would otherwise succeed
and silently save nothing. The session remembers the transaction it opened, so
the client's next statement or `commit` fails with SQLSTATE `40001` instead.

### Read-only local files are enforced twice

Turso shares one open database per path within a process and ignores a second
opener's read-only flag, so a read-only target opened while the same file was
open read-write elsewhere in the process could accept writes (a test caught
this). The file is opened read-only, every connection also runs
`PRAGMA query_only = 1`, and two targets may not use the same file.

### Memory is bounded on both sides

Results stream in bounded batches. Bound parameters stay as Arrow and are
converted to Turso values one batch at a time while the statement runs, so a
large binding costs its Arrow size plus one batch; Grainlift caps the Arrow
size itself (`server.max_bind_bytes`). A bound stream is read during
execution, once. Catalog queries refuse results beyond 100,000 rows instead of
holding them.

### Turso Cloud results stream; they are not buffered

`turso_serverless` reads a query's whole HTTP response before returning a row.
Outside a transaction, this service instead reads Turso Cloud's cursor endpoint
as newline-delimited JSON, one batch at a time
([`src/cursor.rs`](../src/cursor.rs)), and HTTP backpressure holds the server
while a client is not fetching. Reading 100,000 rows of 1 KB text (about
100 MB):

| | peak memory | time |
|---|---|---|
| buffered by `turso_serverless` | 138 MB | 6.2 s |
| streamed from the cursor endpoint | **40 MB** | **1.4 s** |

A streamed query runs on its own server-side stream, which is a separate
database connection, so it cannot see the connection's `TEMP` tables. Inside a
transaction, queries go through the transaction's own stream instead, see its
uncommitted rows, and arrive whole.

### Bulk ingestion to Turso Cloud is batched three ways

Rows go in multi-row `INSERT`s of up to 1,000 rows or 32,766 parameters (Turso
Cloud's limit, measured), many statements per request, in requests of up to
about 4 MiB. Each statement is also capped by size, so a few very wide rows
cannot produce one enormous statement. From a laptop to `aws-us-east-1`:

| rows | one statement per row | multi-row `INSERT`s |
|---|---|---|
| 20,000 | 4.0 s | **0.65 s** |
| 200,000 | | **3.9 s** (≈ 52,000 rows/s) |

Locally the engine is in process, so ingestion runs one prepared statement per
row inside a transaction, with no round trips to save.

### Smaller decisions

- **Writes are atomic.** A multi-row update or an ingestion runs in its own
  transaction, or inside the client's open one, so a constraint failure on any
  row leaves none of the others behind.
- **Transactions open lazily.** With autocommit off, `BEGIN` is sent before the
  first statement that needs it, not right after the previous commit, so an
  idle client holds no Turso Cloud stream open.
- **`ExecuteUpdate` accepts any statement.** Turso's own `execute` refuses a
  statement that returns rows; this service steps it to completion, discards
  its rows and returns the change count, as ADBC expects.
- **Turso reports `BLOB` for computed CTE columns** locally, where SQLite
  reports no declared type. A `BLOB` declaration is therefore treated as a
  hint: such columns are typed by their values.
- **A new query forgets earlier parameters,** because Python's DB-API reuses
  one ADBC statement per cursor.

## Errors

Turso's error categories map to ADBC statuses and SQLSTATEs, so clients can
tell a bad query from a lost connection. SQLSTATE `40001` always means
"retry the transaction".

| Turso | ADBC status | SQLSTATE |
|---|---|---|
| SQL error (`no such table`, syntax) | `INVALID_ARGUMENT` | `42000` |
| constraint violation | `INTEGRITY` | `23000` |
| read-only token or file | `UNAUTHORIZED` | `25006` |
| rejected or expired Turso token | `UNAUTHENTICATED` | `28000` |
| busy (another writer held the lock past `busy_timeout_ms`) | `TIMEOUT` | `40001` |
| concurrent-transaction conflict, or a transaction Turso rolled back | `INVALID_STATE` | `40001` |
| operation deadline expired | `TIMEOUT` | `HYT00` |
| cancelled | `CANCELLED` | `HY008` |
| network failure | `IO` | `58000` |
| value does not fit its column | `INVALID_DATA` | `22000` |

## Limitations

- **A local file has one writer at a time,** as in SQLite. Concurrent writers
  wait up to `busy_timeout_ms` for the lock, and Turso's busy handler retries
  on a timer rather than queueing writers fairly, so under heavy write
  contention a writer can still fail with a retryable busy error. Nothing the
  failed statement attempted is committed. On Turso Cloud, a Turso Database
  engine database with `transaction_mode = "concurrent"` lifts the limit; the
  embedded engine's equivalent is still experimental.
- **An interrupted statement inside a transaction** may leave the transaction
  open or rolled back, as in SQLite; roll back and retry.
- **Idle Turso Cloud transactions expire** with their server-side stream.
- **`TEMP` tables are invisible to streamed Turso Cloud queries,** which run
  on their own stream.
- **Concurrent writes are a Turso Cloud preview.** The Cloud tests pass against
  both engines, but Turso calls the new engine's concurrent writes an early
  preview.
- **Iroh** transport is not offered.

## Dependencies

`cargo audit` reports no vulnerabilities and three warnings, all in transitive
dependencies and accepted: `bincode` 1 (unmaintained, via VGI-RPC), `paste`
(unmaintained, via Iroh's networking in `grainlift-server`), and `lru` 0.16
(`RUSTSEC-2026-0253`, unsound `LruCache::pop` under a panic), which arrives
through `tantivy`, the engine behind Turso's full-text search. Dropping it
would remove full-text search.
