# Working on grainlift-turso

- A Grainlift backend for Turso. `src/db.rs` is one synchronous interface over
  the local Turso Database engine (driven through `turso_sdk_kit`, for
  statement interrupts and busy timeouts) and Turso Cloud (`turso_serverless`,
  plus the streaming cursor reader in `src/cursor.rs`). `src/types.rs` maps
  values to Arrow; `src/statement.rs` and `src/connection.rs` implement the
  `grainlift-server` traits; `src/config.rs` and `src/host.rs` are the
  production configuration and host.
- Every Turso operation goes through `Connection::local` or
  `Connection::remote` in `src/db.rs`, which apply the per-operation deadline
  and cancellation from `src/ops.rs`. Never call Turso around them.
- Backend traits are synchronous and may run inside Grainlift's Tokio
  runtime. Run Turso Cloud futures through `db::Runtime::run`, never
  `block_on` on an ambient runtime; the backend's runtime shuts down in the
  background so it can be dropped anywhere.
- Pass SQL through; do not emulate database behavior. Unsupported operations
  return ADBC `NOT_IMPLEMENTED` through the trait defaults.
- Keep memory bounded: results stream in `BATCH_ROWS`/`BATCH_BYTES` batches,
  bound parameters stay as Arrow until a batch executes, and catalog queries
  are capped.
- Never log queries, credentials, values or raw downstream errors, and keep
  auth tokens out of client-visible messages and configuration errors.
- `turso.example.toml` is tested; keep it valid and annotated.
- Pin `grainlift-server` to a pushed Git revision of Query-farm/grainlift and
  build the native driver from the same revision. Never commit path overrides.
- Rust 1.97+. Run `cargo fmt --check`, `cargo clippy --all-targets --locked
  -- -D warnings` and `cargo test --locked` (with `GRAINLIFT_DRIVER` for the
  native, host and soak tests).
