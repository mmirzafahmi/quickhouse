# etlhouse

Fast **PostgreSQL/MySQL/BigQuery → ClickHouse** ETL with a **Rust engine**, driven from Python.

The hot path is native Rust: the source's native wire protocol → Apache Arrow →
ClickHouse `FORMAT ArrowStream`, with parallel range-partitioned reads,
bounded-memory streaming (backpressure), automatic DDL, and both full-refresh
and incremental (watermark) sync modes. The Python layer is a thin, typed API.

```python
import etlhouse

src = etlhouse.Postgres("postgresql://user:pw@localhost:5432/odoo")
# or: src = etlhouse.MySQL("mysql://user:pw@localhost:3306/odoo")
# or: src = etlhouse.BigQuery("my-gcp-project")  # source_table="dataset.table"
dst = etlhouse.ClickHouse("http://localhost:8123", database="analytics")

result = etlhouse.sync(
    src, dst,
    dest_table="account_move_line",
    source_table="account_move_line",
    mode="incremental",           # or "full"
    watermark="write_date",       # required for incremental
    key=["id"],                   # ORDER BY / dedup key
    create_if_missing=True,       # auto-generate the ClickHouse table
    parallelism=8,
    batch_rows=100_000,
    exclude=["display_name"],
    rename={"amount": "amt"},
    type_overrides={"amt": "Decimal(18, 2)"},
    on_progress=lambda p: print(f"{p.rows_written:,} rows @ {p.rows_per_sec:,.0f}/s"),
)
print(result)   # rows_read, rows_written, bytes_written, duration_secs, new_watermark
```

`source` accepts `etlhouse.Postgres(...)`, `etlhouse.MySQL(...)`, or
`etlhouse.BigQuery(...)` — everything else about `sync()` is identical
either way.

## Why it's fast

| Concern | Approach |
| --- | --- |
| Deserialization | PostgreSQL: binary `COPY` decoded straight into Arrow in Rust. MySQL: `mysql_async`'s typed binary-protocol rows appended straight into Arrow builders. BigQuery: the Storage Read API's wire format is already Arrow. Either way, no per-row Python objects. |
| Parallelism | Postgres/MySQL: table split into key ranges, one connection + Tokio task per partition. BigQuery: `parallelism` is passed as a stream-count hint to BigQuery's own server-side parallel preparation (see the BigQuery note below). |
| Memory | Batches flushed every `batch_rows` rows and streamed to ClickHouse — RSS stays flat regardless of table size |
| GIL | Entire transfer runs inside `Python::allow_threads`; the GIL is only touched for `on_progress` |
| Insert | Arrow IPC stream ingested by ClickHouse's native `ArrowStream` format, gzip on the wire |

## Sync modes

- **`full`** — loads into a staging table, then `EXCHANGE TABLES` to swap it into
  place atomically. A crash mid-run never leaves the destination empty/partial.
- **`incremental`** — reads the last watermark from an internal
  `_etlhouse_state` table in ClickHouse, copies only rows past it (snapshotting
  the current max up front for consistency), and dedupes via
  `ReplacingMergeTree(<watermark>)`. Re-running with no new data is a no-op.

## Progress reporting

`on_progress` is a plain callback (see `sync()` parameters below), so you can
wire up anything — a `print`, a logger, a custom UI. For a ready-made
progress bar, `etlhouse.progress_bar()` wraps [tqdm](https://github.com/tqdm/tqdm)
(`pip install etlhouse[progress]`):

```python
with etlhouse.progress_bar() as on_progress:
    etlhouse.sync(src, dst, dest_table="t", source_table="t", on_progress=on_progress)
```

Pass `total=<row count>` if you know it in advance (e.g. from a prior
`COUNT(*)`) for a percentage/ETA bar instead of a running count; any other
keyword arguments are passed straight through to `tqdm.tqdm`. The bar closes
automatically on exit, including when `sync()` raises.

## Logging

Every `sync()` call prints step-by-step progress to **stderr** via `tracing`:
connecting to the source, resolving columns and partitions, watermark
resolution (incremental mode), DDL/staging-table creation, per-partition read
start/completion, the full-refresh table swap, watermark persistence, and a
final summary (rows, duration, rows/sec). This is independent of, and
complementary to, `on_progress`/`progress_bar()` above — the callback only
fires during the actual row-ingestion loop (never during connect/DDL/swap),
while the log lines cover the whole pipeline including setup and teardown.

Default level is `INFO` for `etlhouse_core` (dependency internals like
tokio/hyper/tonic stay quiet). Override with the standard `RUST_LOG`
environment variable, e.g.:

```bash
RUST_LOG=etlhouse_core=debug python my_script.py   # + actual SQL/DDL text
RUST_LOG=debug python my_script.py                 # everything, incl. deps
```

## Prerequisites

- **Rust** toolchain (1.75+): install from <https://rustup.rs>
- **Python** 3.9+
- **maturin**: `pip install maturin`

## Build & install (local dev)

```bash
# From the repo root
pip install maturin
maturin develop --release        # compiles the Rust engine, installs into the active venv
python -c "import etlhouse; print(etlhouse.version())"
```

Build a wheel to distribute:

```bash
maturin build --release          # produces target/wheels/etlhouse-*.whl
```

## Running the tests

Integration tests need live services (skipped automatically if unavailable):

```bash
docker compose up -d             # PostgreSQL + MySQL + ClickHouse
pip install -e '.[test]'
maturin develop --release
pytest -v

# Rust unit tests (decoders, type map, DDL) need no services:
cargo test -p etlhouse-core
```

## `sync()` parameters

| Parameter | Meaning |
| --- | --- |
| `source_table` / `source_query` | Read a whole table, or a custom `SELECT` (one is required) |
| `dest_table` | Destination table in the ClickHouse database |
| `mode` | `"full"` or `"incremental"` |
| `watermark` | Monotonic column (e.g. `write_date`, `id`) — required for incremental |
| `key` | Business key → ClickHouse `ORDER BY` and dedup key |
| `create_if_missing` | Auto-run generated `CREATE TABLE` when the destination is absent |
| `engine`, `order_by`, `partition_by`, `primary_key` | DDL knobs (sensible defaults per mode) |
| `parallelism` | Number of concurrent partition streams |
| `batch_rows` | Rows per Arrow batch / insert flush (memory vs round-trips) |
| `partition_column` | Integer column to range-split on (defaults to first `key`) |
| `type_overrides` | Per-column ClickHouse type (e.g. `{"qty": "Decimal(18, 3)"}`) |
| `rename` | Source → destination column renames |
| `include` / `exclude` | Column allow/deny lists |
| `on_progress` | Callback receiving a `Progress` (`rows_written`, `rows_per_sec`, …) |

## Type mapping

### PostgreSQL

| PostgreSQL | Arrow | ClickHouse |
| --- | --- | --- |
| `int2/4/8` | `Int16/32/64` | `Int16/32/64` |
| `float4/8`, `numeric`* | `Float32/64` | `Float32/64` |
| `bool` | `Boolean` | `Bool` |
| `text/varchar/json/jsonb` | `Utf8` | `String` |
| `uuid` | `Utf8` | `UUID` |
| `date` | `Date32` | `Date32` |
| `timestamp[tz]` | `Timestamp(µs)` | `DateTime64(6[, tz])` |

Nullable PostgreSQL columns become `Nullable(T)`. `numeric` maps to `Float64`
by default (arbitrary precision is unknown from the type OID); override to a
`Decimal(P, S)` via `type_overrides` and ClickHouse will convert on insert.

### MySQL

| MySQL | Arrow | ClickHouse |
| --- | --- | --- |
| `TINYINT(1)` | `Boolean` | `Bool` |
| `TINYINT` / `SMALLINT` / `INT` / `BIGINT` (± `UNSIGNED`) | `Int8..64` / `UInt8..64` | matching `Int*`/`UInt*` |
| `FLOAT` | `Float32` | `Float32` |
| `DOUBLE`, `DECIMAL`/`NUMERIC`* | `Float64` | `Float64` |
| `VARCHAR/TEXT/ENUM/SET/JSON` | `Utf8` | `String` |
| `BLOB` family, `BIT` | `Binary` | `String` |
| `DATE` | `Date32` | `Date32` |
| `DATETIME`/`TIMESTAMP` | `Timestamp(µs)` | `DateTime64(6)` |

`TINYINT(1)` is treated as MySQL's de facto boolean convention (matching most
MySQL client libraries); other `TINYINT` widths map to `Int8`/`UInt8`. Column
nullability comes directly from MySQL's wire-protocol column metadata
(`NOT_NULL_FLAG`) — unlike PostgreSQL, this works even for `source_query`
(no separate catalog lookup needed). `DECIMAL`/`NUMERIC` maps to `Float64` by
default, same override policy as PostgreSQL's `numeric`.

### BigQuery

| BigQuery | Arrow | ClickHouse |
| --- | --- | --- |
| `BOOLEAN`/`BOOL` | `Boolean` | `Bool` |
| `INTEGER`/`INT64` | `Int64` | `Int64` |
| `FLOAT`/`FLOAT64` | `Float64` | `Float64` |
| `NUMERIC`/`BIGNUMERIC`/`DECIMAL`* | `Float64` | `Float64` |
| `STRING`, `JSON` | `Utf8` | `String` |
| `BYTES` | `Binary` | `String` |
| `DATE` | `Date32` | `Date32` |
| `TIME` | `Time64(µs)` | `String` |
| `TIMESTAMP` | `Timestamp(µs, UTC)` | `DateTime64(6, 'UTC')` |
| `DATETIME` | `Timestamp(µs)` | `DateTime64(6)` |

`NUMERIC`/`BIGNUMERIC` map to `Float64` by default (same override policy as
the other sources' arbitrary-precision types). `RECORD`/`STRUCT` and
repeated (`ARRAY`) fields aren't supported in v1 — same scalar-only scope as
the Postgres/MySQL sources.

## Project layout

```
crates/etlhouse-core/   # pure-Rust engine (unit-testable, no Python)
  src/source/postgres.rs   # PostgreSQL: binary COPY, schema/partition queries
  src/source/mysql.rs      # MySQL: streaming SELECT, schema/partition queries
  src/source/bigquery.rs   # BigQuery: auth, schema resolution, Storage Read API
  src/decode.rs            # PostgreSQL COPY wire format -> Arrow
  src/decode_mysql.rs       # MySQL typed rows -> Arrow
  src/decode_bigquery.rs    # BigQuery typed rows -> Arrow
  src/types.rs              # per-source type -> Arrow -> ClickHouse mapping
  src/sync.rs               # orchestration; dispatches on the `Source` enum
crates/etlhouse-py/     # PyO3 bindings (cdylib -> etlhouse._etlhouse)
python/etlhouse/        # typed Python surface (__init__.py, .pyi stubs)
tests/                  # pytest integration tests
docker-compose.yml      # local PostgreSQL + MySQL + ClickHouse
```

## Limitations / roadmap (v1)

- TLS uses rustls, trusting the public CA roots (`webpki-roots`) plus,
  optionally, an extra CA file via `ca_cert_file=...` on either `Postgres` or
  `MySQL` — needed for providers like AWS RDS whose certificates chain to a
  private regional CA rather than a public one. For PostgreSQL, whether TLS is
  used follows the normal libpq `sslmode` query parameter on the DSN
  (`disable` | `prefer` (default) | `require`); MySQL has no such DSN
  convention, so use `MySQL(..., require_tls=True)` explicitly. Client-certificate
  (mTLS) auth isn't supported for either source yet.
- Array and `time` types have limited support; extend `types.rs` + `decode.rs`
  (Postgres) / `decode_mysql.rs` (MySQL) / `decode_bigquery.rs` (BigQuery).
- **BigQuery parallelism**: `parallelism` is passed to BigQuery as a
  stream-count hint (server-side parallel preparation), but rows are
  currently consumed on a single connection rather than fanned out across
  concurrent local tasks like Postgres/MySQL. Multi-statement BigQuery
  *script* jobs aren't supported for `source_query` (only single `SELECT`
  statements) — the destination-table resolution needed for the Storage Read
  API doesn't follow child jobs.
- No CLI yet — a config-driven CLI over the same engine is planned.
- Logical-replication CDC and arbitrary transform callbacks are future work.

## Releasing

CI (`.github/workflows/release.yml`) builds wheels for Linux (manylinux x86_64),
macOS (Intel + Apple Silicon), and Windows (x86_64) plus an sdist, then publishes
via **PyPI Trusted Publishing** (OIDC — no API tokens stored anywhere).

**One-time setup:**

1. In the GitHub repo settings, create two **Environments**: `testpypi` and `pypi`
   (Settings → Environments). On `pypi`, add yourself as a **required reviewer** —
   this gives you a manual approval gate before the irreversible real-PyPI publish.
2. On [test.pypi.org](https://test.pypi.org) and [pypi.org](https://pypi.org),
   add a **Trusted Publisher** for the `etlhouse` project (Account settings →
   Publishing), pointing at this repo, workflow file `release.yml`, and the
   matching environment name (`testpypi` / `pypi`). Since the project doesn't
   exist yet on either index, use each site's "publish a new project" /
   pending-publisher flow.

**Cutting a release:**

```bash
# bump the version in Cargo.toml and pyproject.toml, commit, then:
git tag v0.1.0
git push origin v0.1.0
```

Pushing the tag triggers the workflow: it builds all wheels, publishes to
TestPyPI automatically, then waits for your approval on the `pypi` environment
before publishing the real release. Verify the TestPyPI install first:

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ etlhouse
```

## License

MIT
