# quickhouse

Fast **PostgreSQL / MySQL / BigQuery → ClickHouse** ETL: a native **Rust engine**
driven from a small, typed **Python** API.

The hot path never materializes Python objects — each source's native wire
protocol flows straight into Apache Arrow and out to ClickHouse's
`FORMAT ArrowStream`, with parallel range-partitioned reads, bounded-memory
streaming, automatic table creation, and both full-refresh and incremental
(watermark) sync modes.

```python
import quickhouse

src = quickhouse.Postgres("postgresql://user:pw@localhost:5432/shop")
dst = quickhouse.ClickHouse("http://localhost:8123", database="analytics")

result = quickhouse.sync(src, dst, dest_table="orders",
                         source_table="orders", key=["id"])
print(result)   # rows_read, rows_written, bytes_written, duration_secs, new_watermark
```

## Key features

- **Three sources, one API.** PostgreSQL, MySQL, and Google BigQuery — swap the
  source object, and everything else about `sync()` is identical.
- **Fast by construction.** Source rows are decoded straight into Arrow in Rust
  (no per-row Python), tables are split into key ranges read in parallel, and
  decoding overlaps uploading — each finished batch's insert is spawned as a
  background task while the next batch is still being decoded.
- **Bounded memory.** A single hard ceiling, `max_memory_bytes`, caps *total*
  in-flight batch memory across every partition and every upload, measured
  against each batch's real Arrow allocation. When it's reached, decoding blocks
  (backpressure) — so peak RSS stays flat regardless of `parallelism`, row
  width, or partition skew.
- **Streaming compressed inserts.** Arrow IPC ingested by ClickHouse's native
  `ArrowStream`, compressed on the fly with zstd (default), gzip, or none — the
  body is produced incrementally, never buffered in full.
- **Atomic full refresh.** Loads into a staging table, then `EXCHANGE TABLES` to
  swap it into place; a crash mid-run never leaves the destination partial.
- **Idempotent incremental.** Tracks a watermark in an internal ClickHouse state
  table, copies only new rows, and dedupes via `ReplacingMergeTree`. Re-running
  with no new data is a no-op.
- **Resilient.** Transient insert failures (dropped connections, timeouts, HTTP
  5xx/429) retry with exponential backoff (up to 4 attempts); deterministic 4xx
  errors (bad SQL, auth) fail fast. Messy legacy data — zero-dates, out-of-range
  dates — is coerced rather than crashing the run.
- **Automatic DDL.** Generates the ClickHouse `CREATE TABLE` from the source
  schema, with per-column type overrides, renames, and include/exclude lists.
- **GIL-free.** The whole transfer runs inside `Python::allow_threads`; the GIL
  is only re-acquired to fire your `on_progress` callback.

## Installation

Prebuilt wheels — no Rust toolchain required:

```bash
pip install quickhouse
pip install "quickhouse[progress]"   # + a ready-made tqdm progress bar
```

Python 3.9+ on Linux, macOS (Intel + Apple Silicon), and Windows (x86_64).

Building from source (for development, or to run an unreleased version) needs the
Rust toolchain and maturin — see [CONTRIBUTING.md](CONTRIBUTING.md).

## How to use

```python
import quickhouse as qh

src = qh.Postgres("postgresql://user:pw@localhost:5432/shop")
# or: src = qh.MySQL("mysql://user:pw@localhost:3306/shop")
# or: src = qh.BigQuery("my-gcp-project")   # source_table="dataset.table"
dst = qh.ClickHouse("http://localhost:8123", database="analytics")

result = qh.sync(
    src, dst,
    dest_table="orders",
    source_table="orders",
    mode="incremental",           # or "full"
    watermark="updated_at",       # required for incremental
    key=["id"],                   # ORDER BY / dedup key
    create_if_missing=True,       # auto-generate the ClickHouse table
    parallelism=8,
    batch_rows=100_000,
    exclude=["internal_notes"],
    rename={"amount": "amt"},
    type_overrides={"amt": "Decimal(18, 2)"},
    on_progress=lambda p: print(f"{p.rows_written:,} rows @ {p.rows_per_sec:,.0f}/s"),
)
print(result)   # rows_read, rows_written, bytes_written, duration_secs, new_watermark
```

### Choosing a source

`sync()`'s first argument accepts any of:

```python
qh.Postgres("postgresql://user:pw@host:5432/db", statement_timeout_secs=0, ca_cert_file=None)
qh.MySQL("mysql://user:pw@host:3306/db", require_tls=False, ca_cert_file=None)
qh.BigQuery("my-gcp-project", credentials_file=None)   # None → Application Default Credentials
```

Read a whole table with `source_table=...`, or a custom `SELECT` with
`source_query=...` (exactly one is required).

### Sync modes

- **`full`** — loads into a staging table, then `EXCHANGE TABLES` to swap it into
  place atomically. A crash mid-run never leaves the destination empty/partial.
- **`incremental`** — reads the last watermark from an internal
  `_quickhouse_state` table in ClickHouse, copies only rows past it (snapshotting
  the current max up front for consistency), and dedupes via
  `ReplacingMergeTree(<watermark>)`. Re-running with no new data is a no-op.

### Progress reporting

`on_progress` is a plain callback, so you can wire up anything — a `print`, a
logger, a custom UI. For a ready-made bar, `qh.progress_bar()` wraps
[tqdm](https://github.com/tqdm/tqdm) (`pip install "quickhouse[progress]"`):

```python
with qh.progress_bar() as on_progress:
    qh.sync(src, dst, dest_table="t", source_table="t", on_progress=on_progress)
```

Pass `total=<row count>` (e.g. from a prior `COUNT(*)`) for a percentage/ETA bar
instead of a running count; other keyword arguments pass straight through to
`tqdm.tqdm`. The bar closes automatically on exit, including when `sync()` raises.

### Logging

Every `sync()` call prints step-by-step progress to **stderr** via `tracing`:
connecting, resolving columns/partitions, watermark resolution, DDL/staging
creation, per-partition read start/completion, the full-refresh swap, watermark
persistence, and a final summary (rows, duration, rows/sec). This complements
`on_progress` (which only fires during the row-ingestion loop, never during
connect/DDL/swap).

Default level is `INFO` for `quickhouse_core` (dependency internals stay quiet).
Override with the standard `RUST_LOG` environment variable:

```bash
RUST_LOG=quickhouse_core=debug python my_script.py   # + actual SQL/DDL text
RUST_LOG=debug python my_script.py                   # everything, incl. deps
```

### Errors

Every failure raises a plain Python `RuntimeError` whose message is built to be
actionable on its own, without needing to scroll back through stderr logs:

- **Every error names the table it happened to** — `sync()` is often called in
  a loop over many tables (see the quickstart), so the message is always
  prefixed with `"<source> -> <dest_table>: ..."`.
- **Bad config or a data-shape problem says exactly what's wrong**, e.g. an
  incremental `watermark` column that doesn't exist in the source names the
  columns that do; a source column type with no ClickHouse mapping (an array,
  a geometry type, BigQuery `RECORD`) names the real engine, the real type, and
  suggests a fix — `exclude=[...]` the column, or cast it in a `source_query`.
- **A message starting with "internal error"** means the failure couldn't have
  been caused by your data or config — please file an issue with the message.
- Everything else (connection failures, a rejected `INSERT`, etc.) surfaces the
  underlying database's own error text — e.g. Postgres's SQLSTATE code and
  message, or ClickHouse's exception code — rather than a generic wrapper.

### `sync()` parameters

| Parameter | Meaning |
| --- | --- |
| `source_table` / `source_query` | Read a whole table, or a custom `SELECT` (one is required) |
| `dest_table` | Destination table in the ClickHouse database |
| `mode` | `"full"` or `"incremental"` |
| `watermark` | Monotonic column (e.g. `updated_at`, `id`) — required for incremental; ignored (cleared to `None`) in full mode |
| `key` | Business key → ClickHouse `ORDER BY` and dedup key |
| `create_if_missing` | Auto-run generated `CREATE TABLE` when the destination is absent |
| `engine`, `order_by`, `partition_by`, `primary_key` | DDL knobs (sensible defaults per mode) |
| `parallelism` | Number of concurrent partition streams |
| `batch_rows` | Max rows per Arrow batch / insert — a per-batch granularity knob, **not** the memory ceiling |
| `batch_bytes` | Also cap each batch at this many estimated source bytes (default 4 MiB); `0` disables |
| `max_memory_bytes` | **The memory ceiling.** Hard cap on total in-flight Arrow memory across all partitions and uploads; decoding blocks when reached. Default 512 MiB; `0` = unbounded |
| `partition_column` | Integer column to range-split on (defaults to first `key`) |
| `type_overrides` | Per-column ClickHouse type (e.g. `{"qty": "Decimal(18, 3)"}`) |
| `rename` | Source → destination column renames |
| `include` / `exclude` | Column allow/deny lists |
| `on_progress` | Callback receiving a `Progress` (`rows_written`, `rows_per_sec`, …) |

## Type mapping

Nullable source columns become `Nullable(T)`. Arbitrary-precision numeric types
(`numeric`/`DECIMAL`/`NUMERIC`/`BIGNUMERIC`, marked \*) map to `Float64` by
default — precision isn't recoverable from the type alone; override to a
`Decimal(P, S)` via `type_overrides` and ClickHouse converts on insert. `TIME`
values are transferred as canonical `[-]HH:MM:SS[.ffffff]` **text** into a
`String` column (ClickHouse has no time-of-day type; text preserves MySQL's
negative / >24h durations losslessly). Dates outside ClickHouse's
`[1900-01-01, 2299-12-31]` window — and MySQL zero-dates like `0000-00-00` — are
coerced to `NULL` (with a per-partition warning) rather than aborting the run.

### PostgreSQL

| PostgreSQL | Arrow | ClickHouse |
| --- | --- | --- |
| `int2/4/8` | `Int16/32/64` | `Int16/32/64` |
| `float4/8`, `numeric`\* | `Float32/64` | `Float32/64` |
| `bool` | `Boolean` | `Bool` |
| `text/varchar/json/jsonb` | `Utf8` | `String` |
| `uuid` | `Utf8` | `UUID` |
| `date` | `Date32` | `Date32` |
| `time` | `Utf8` | `String` |
| `timestamp[tz]` | `Timestamp(µs)` | `DateTime64(6[, tz])` |

### MySQL

| MySQL | Arrow | ClickHouse |
| --- | --- | --- |
| `TINYINT(1)` | `Boolean` | `Bool` |
| `TINYINT` / `SMALLINT` / `INT` / `BIGINT` (± `UNSIGNED`) | `Int8..64` / `UInt8..64` | matching `Int*`/`UInt*` |
| `FLOAT` | `Float32` | `Float32` |
| `DOUBLE`, `DECIMAL`/`NUMERIC`\* | `Float64` | `Float64` |
| `VARCHAR/TEXT/ENUM/SET/JSON` | `Utf8` | `String` |
| `BLOB` family, `BIT` | `Binary` | `String` |
| `DATE` | `Date32` | `Date32` |
| `TIME` | `Utf8` | `String` |
| `DATETIME`/`TIMESTAMP` | `Timestamp(µs)` | `DateTime64(6)` |

`TINYINT(1)` follows MySQL's de facto boolean convention (matching most client
libraries); other `TINYINT` widths map to `Int8`/`UInt8`. Column nullability
comes directly from MySQL's wire-protocol metadata (`NOT_NULL_FLAG`), so it works
even for `source_query`.

### BigQuery

| BigQuery | Arrow | ClickHouse |
| --- | --- | --- |
| `BOOLEAN`/`BOOL` | `Boolean` | `Bool` |
| `INTEGER`/`INT64` | `Int64` | `Int64` |
| `FLOAT`/`FLOAT64` | `Float64` | `Float64` |
| `NUMERIC`/`BIGNUMERIC`/`DECIMAL`\* | `Float64` | `Float64` |
| `STRING`, `JSON` | `Utf8` | `String` |
| `BYTES` | `Binary` | `String` |
| `DATE` | `Date32` | `Date32` |
| `TIME` | `Utf8` | `String` |
| `TIMESTAMP` | `Timestamp(µs, UTC)` | `DateTime64(6, 'UTC')` |
| `DATETIME` | `Timestamp(µs)` | `DateTime64(6)` |

`RECORD`/`STRUCT` and repeated (`ARRAY`) fields aren't supported in v1 — same
scalar-only scope as the Postgres/MySQL sources.

## Limitations / roadmap (v1)

- TLS uses rustls, trusting the public CA roots plus, optionally, an extra CA
  file via `ca_cert_file=...` on `Postgres`/`MySQL` (needed for providers like
  AWS RDS with a private regional CA). PostgreSQL follows the libpq `sslmode` DSN
  parameter (`disable` | `prefer` (default) | `require`); MySQL has no such
  convention, so use `MySQL(..., require_tls=True)`. Client-certificate (mTLS)
  auth isn't supported yet.
- Array and composite (`RECORD`/`STRUCT`) types aren't supported; extend
  `types.rs` + the relevant `decode*.rs`.
- **BigQuery parallelism**: `parallelism` is passed as a stream-count hint
  (server-side parallel preparation), but rows are consumed on a single
  connection rather than fanned out across concurrent local tasks. Only single
  `SELECT` statements are supported for `source_query` (not multi-statement
  scripts).
- No CLI yet — a config-driven CLI over the same engine is planned.
- Logical-replication CDC and arbitrary transform callbacks are future work.

## Contributing

Bug reports, source/type-mapping additions, and PRs are welcome. Build steps,
the test workflow, project layout, and the release process live in
[CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT
