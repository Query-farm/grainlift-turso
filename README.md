<p align="center">
  <a href="https://github.com/Query-farm/grainlift">
    <img src="https://raw.githubusercontent.com/Query-farm/grainlift-turso/main/docs/grainlift-logo.svg" alt="Grainlift logo" width="200">
  </a>
</p>

<h1 align="center">grainlift-turso</h1>

<p align="center">
  Query your <a href="https://turso.tech">Turso</a> databases from DuckDB, Python, Go, Rust<br>
  and any other tool that speaks <a href="https://arrow.apache.org/adbc/">ADBC</a>.<br>
  Built on <a href="https://github.com/Query-farm/grainlift">Grainlift</a> by <a href="https://query.farm">Query.Farm</a>
</p>

<p align="center">
  <sub>WORKS WITH</sub><br>
  <a href="https://turso.tech">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="docs/turso-logo-white.svg">
      <img src="docs/turso-logo-dark.svg" alt="Turso" height="44">
    </picture>
  </a>
</p>

<p align="center">
  <a href="https://github.com/Query-farm/grainlift-turso/actions/workflows/ci.yml"><img src="https://github.com/Query-farm/grainlift-turso/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License: Apache-2.0"></a>
  <a href="https://github.com/Query-farm/grainlift"><img src="https://img.shields.io/badge/Grainlift-0.4.2-2f7d32.svg" alt="Grainlift 0.4.2"></a>
</p>

---

**grainlift-turso** is a small server that sits in front of a Turso database
and lets analytics and data tools work with it directly. Point DuckDB at it and
your Turso tables become tables you can join with local data. Use it from
Python and you get results as Apache Arrow, ready for pandas or Polars. Load
data into Turso from a DuckDB query in one statement.

It works with both kinds of Turso database:

- **Turso Cloud**: databases hosted by Turso, on either of its engines.
- **Local files**: a database file on the server, run by the embedded Turso
  Database engine.

## Why use it

- **Your tools, your Turso data.** Anything with an ADBC driver can read and
  write Turso: DuckDB, Haybarn, Python, Go, Rust, Java and more.
- **Fast, typed results.** Rows arrive as Arrow columns with proper types
  (integers stay integers), streamed in batches, so even very large results
  use little memory.
- **Bulk loading.** Copy a DuckDB query result or a pandas table into Turso in
  one call; Turso Cloud loads arrive in about 50,000 rows per second.
- **Safe to share.** Clients authenticate with tokens or certificates, see only
  the databases you allow, and never see your Turso credentials. Read-only
  access is enforced by Turso itself.
- **Built for production.** Runaway queries are stopped, clients can cancel,
  and it ships with health checks, structured logs, tracing and a container
  image.

## How it works

```
DuckDB, Python, ...  ──▶  Grainlift ADBC driver  ──▶  grainlift-turso  ──▶  Turso
   your tools             (a library your tool loads)    (this server)        Cloud or local file
```

Your tool loads the Grainlift ADBC driver
([`pip install adbc-driver-grainlift`](https://pypi.org/project/adbc-driver-grainlift/)),
which talks to grainlift-turso over the network. grainlift-turso runs your SQL
on Turso as written and streams the results back.

## Quick start

This takes about five minutes and needs Python 3.13 or newer (for the driver),
[Rust](https://rustup.rs) 1.97 or newer (to build the server) and
[uv](https://docs.astral.sh/uv/) (to run Haybarn, a DuckDB distribution).

**1. Install the Grainlift driver** from PyPI, and note where its library is:

```bash
pip install adbc-driver-grainlift
export GRAINLIFT_DRIVER=$(python -c "import adbc_driver_grainlift; print(adbc_driver_grainlift.driver_path())")
```

**2. Start grainlift-turso** on a new local database file:

```bash
git clone https://github.com/Query-farm/grainlift-turso.git
cd grainlift-turso
export GRAINLIFT_TOKEN=choose-a-secret
TURSO_DATABASE_URL=demo.db cargo run --release
```

It prints `Grainlift listening on http://127.0.0.1:8080`. Leave it running.

**3. Query it from DuckDB.** In a second terminal (with the same two
`export`s), run the [example](examples/query.sql), which loads a table into
Turso, updates it, and joins it with local data:

```bash
uvx haybarn-cli < examples/query.sql
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

## Using it

### From DuckDB

Install the `adbc_scanner` extension, store the connection in a secret, and
attach the database. Its tables then behave like local ones. `DRIVER` is the
library path that `adbc_driver_grainlift.driver_path()` prints:

```sql
FORCE INSTALL adbc_scanner FROM community;
LOAD adbc_scanner;

CREATE SECRET turso (
    TYPE adbc,
    DRIVER '/path/to/libadbc_driver_grainlift.dylib',
    URI 'http://127.0.0.1:8080',
    SCOPE 'http://127.0.0.1:8080',
    EXTRA_OPTIONS MAP {'grainlift.target': 'turso', 'grainlift.auth.bearer_token': 'choose-a-secret'}
);

ATTACH 'http://127.0.0.1:8080' AS turso (TYPE adbc, SECRET 'turso');
SELECT * FROM turso.cities WHERE population > 1000000;
```

To run statements in Turso, or load data into it:

```sql
SET VARIABLE conn = (SELECT adbc_connect({'secret': 'turso'}));
CALL adbc_execute(getvariable('conn')::BIGINT, 'CREATE INDEX cities_country ON cities (country)');
SELECT * FROM adbc_insert(getvariable('conn')::BIGINT, 'sales', (SELECT * FROM 'sales.parquet'), mode := 'create');
```

Use [Haybarn](https://github.com/Query-farm-haybarn/haybarn) 1.5.5 or newer:
the `adbc_scanner` that stock DuckDB currently downloads is too old to load
data through Grainlift.

### From Python

```python
from adbc_driver_grainlift import dbapi

with dbapi.connect(db_kwargs={
    "grainlift.uri": "http://127.0.0.1:8080",
    "grainlift.target": "turso",
    "grainlift.auth.bearer_token": "choose-a-secret",
}) as conn, conn.cursor() as cur:
    cur.execute("SELECT name, population FROM cities")
    table = cur.fetch_arrow_table()      # or cur.fetch_df() for pandas
```

`pip install adbc-driver-grainlift pyarrow` provides everything this needs.
From Go, Rust, Java or C++, load the same library (`driver_path()`) through
that language's ADBC driver manager, with the entry point
`AdbcDriverGrainliftInit` and the same `grainlift.*` settings.

### With Turso Cloud

Point the server at your database's URL and give it a
[database token](https://docs.turso.tech/cli/db/tokens/create):

```bash
export TURSO_DATABASE_URL=libsql://my-db-my-org.turso.io
export TURSO_AUTH_TOKEN=$(turso db tokens create my-db)
cargo run --release
```

Prefer to have each person use their own Turso token, so Turso applies their
permissions? Leave `TURSO_AUTH_TOKEN` unset, and each client adds its token as
`turso.auth_token` next to its other connection settings.

## Running it in production

For a long-running deployment, describe your databases and who may use them in
a configuration file, then check and serve it:

```toml
[server]
listen = "127.0.0.1:8080"

[auth.static_bearer_tokens]
"replace-with-a-long-random-token" = "analytics"

[targets.app]                         # clients connect to the target "app"
url = "libsql://app-myorg.turso.io"
auth_token_env = "TURSO_APP_TOKEN"    # the Turso token comes from the environment
```

```bash
grainlift-turso check --config turso.toml    # validates the file and connects to every database
grainlift-turso serve --config turso.toml
```

One server can serve many databases, each with its own settings, and supports
single sign-on (JWT), mutual TLS, per-user database permissions, health checks,
JSON logs, tracing and graceful shutdown. A [`Dockerfile`](Dockerfile) builds a
small container image. The **[deployment guide](docs/deployment.md)** covers
all of it, and [`turso.example.toml`](turso.example.toml) is a complete,
annotated example.

## What you can do

- Run any SQL that Turso supports, and read results with proper column types.
- Write: inserts, updates, deletes, schema changes, with transactions.
- Load whole tables at once (bulk ingestion), atomically.
- Use parameterized queries (`?`, `:name`, ...).
- Browse tables and columns from tools that show a catalog, like DuckDB's
  `ATTACH`.
- Cancel a long-running query.
- On Turso Cloud's newer engine, let many clients write at the same time
  (`transaction_mode = "concurrent"`).

## Troubleshooting

**"retry it" or SQLSTATE `40001`.** Another writer got there first: your
transaction was rolled back, and nothing it did was saved. Run it again. This
is normal under heavy concurrent writing.

**"exceeded its … deadline".** The query ran longer than the server allows
(60 seconds by default). Narrow the query, or raise
`operation_timeout_seconds` in the configuration.

**A write fails as unauthorized.** The database is read-only for you: a
read-only Turso token, or a file served with `read_only = true`.

**"Column … has type Int64 but row … holds text".** A column mixes kinds of
values, which SQLite-style databases allow. Add a `CAST` in your query to
choose the type you want.

**`adbc_insert` hangs in DuckDB.** Your `adbc_scanner` extension is too old.
Run `FORCE INSTALL adbc_scanner FROM community;` in Haybarn 1.5.5 or newer.

**A new table doesn't appear in an attached database.** DuckDB caches the
table list when you attach. Detach and attach again.

## Learn more

- [Deployment guide](docs/deployment.md): configuration, authentication,
  health checks, logging, containers and scaling.
- [How it works](docs/how-it-works.md): architecture, type mapping, design
  decisions, error codes and limitations.
- [Development](docs/development.md): building, testing and CI.
- [Changelog](CHANGELOG.md)

## License

Copyright © 2026 [Query Farm LLC](https://query.farm). Released under the
**Apache License 2.0**; see [LICENSE](LICENSE).

The data in a Turso database belongs to its owner, and Turso Cloud is subject
to [Turso's terms](https://turso.tech/terms-of-service). See [NOTICE](NOTICE).
This project is not affiliated with or endorsed by Turso.

---

<p align="center">
  <a href="https://query.farm"><img src="docs/query-farm-logo.svg" alt="Query.Farm" height="48"></a><br>
  Built with <a href="https://github.com/Query-farm/grainlift">Grainlift</a><br>
  by <a href="https://query.farm">Query.Farm</a>
</p>
