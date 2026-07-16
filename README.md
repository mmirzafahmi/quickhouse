# etlhouse

Fast **PostgreSQL → ClickHouse** ETL with a **Rust engine**, driven from Python.

The hot path is native Rust: PostgreSQL binary `COPY` → Apache Arrow → ClickHouse
`FORMAT ArrowStream`, with parallel range-partitioned reads, bounded-memory
streaming (backpressure), automatic DDL, and both full-refresh and incremental
(watermark) sync modes. The Python layer is a thin, typed API.

```python
import etlhouse

src = etlhouse.Postgres("postgresql://user:pw@localhost:5432/odoo")
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

## Why it's fast

| Concern | Approach |
| --- | --- |
| Deserialization | PostgreSQL **binary** `COPY` decoded straight into Arrow columns in Rust (no per-row Python objects) |
| Parallelism | Table split into key ranges; one `COPY` stream + Tokio task per partition |
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
docker compose up -d             # PostgreSQL + ClickHouse
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

## Project layout

```
crates/etlhouse-core/   # pure-Rust engine (unit-testable, no Python)
crates/etlhouse-py/     # PyO3 bindings (cdylib -> etlhouse._etlhouse)
python/etlhouse/        # typed Python surface (__init__.py, .pyi stubs)
tests/                  # pytest integration tests
docker-compose.yml      # local PostgreSQL + ClickHouse
```

## Limitations / roadmap (v1)

- TLS to PostgreSQL is not yet wired (uses `NoTls`); add a connector in
  `source/postgres.rs`.
- Array and `time` types have limited support; extend `types.rs` + `decode.rs`.
- No CLI yet — a config-driven CLI over the same engine is planned.
- Logical-replication CDC and arbitrary transform callbacks are future work.

## License

MIT
