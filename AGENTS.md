# Working on grainlift-turso

- A Grainlift backend for Turso: `src/db.rs` wraps the two Turso clients
  (`turso` embedded, `turso_serverless` for Turso Cloud) behind one async
  interface; `src/types.rs` maps values to Arrow; `src/statement.rs` and
  `src/connection.rs` implement the `grainlift-server` traits.
- Backend traits are synchronous and may run inside Grainlift's Tokio
  runtime. Run Turso futures through `db::Runtime::run` (or `Session::run`),
  never `block_on` on an ambient runtime.
- Pass SQL through; do not emulate database behavior. Unsupported operations
  return ADBC `NOT_IMPLEMENTED` through the trait defaults.
- Keep results lazy and bounded (`BATCH_ROWS`, `BATCH_BYTES`).
- Never log queries, credentials, values or raw downstream errors, and keep
  auth tokens out of client-visible messages.
- Pin `grainlift-server` to a pushed Git revision of Query-farm/grainlift and
  build the native driver from the same revision. Never commit path overrides.
- Rust 1.97+. Run `cargo fmt --check`, `cargo clippy --all-targets --locked
  -- -D warnings` and `cargo test --locked` (with `GRAINLIFT_DRIVER` for the
  native tests).
