# quickhouse

**Move tables from PostgreSQL, MySQL, or BigQuery into ClickHouse or BigQuery — fast, in one function call.**

quickhouse is a small, typed Python API on top of a native Rust engine. You
hand it a source, a destination, and a table name; it figures out the schema,
creates the destination table, streams the rows across in parallel, and keeps
memory flat the whole way. The heavy lifting never touches Python objects —
each database's native wire protocol flows straight into Apache Arrow and out
the other side.

```python
import quickhouse

src = quickhouse.Postgres("postgresql://user:pw@localhost:5432/shop")
dst = quickhouse.ClickHouse("http://localhost:8123", database="analytics")

result = quickhouse.sync(src, dst, dest_table="orders",
                         source_table="orders", key=["id"])
print(result)   # rows_read, rows_written, bytes_written, duration_secs, new_watermark
```

## Why quickhouse

- **It's fast.** Rows are decoded straight off the wire into Arrow, in Rust —
  no per-row Python, no intermediate DataFrame. Tables are split into ranges
  and read in parallel, and decoding overlaps uploading. On a laptop-class box
  a 1M-row, 20-column full refresh runs at **hundreds of thousands of rows per
  second** while peak memory stays flat (under ~180 MB) no matter how much you
  parallelize. Reproduce it with `python benchmarks/bench_transfer.py`.

- **It's one function call.** `sync()` replaces the cursor loop, manual
  batching, retry logic, and `CREATE TABLE` you'd otherwise write by hand.
  Defaults handle table creation, type mapping, parallelism, and batching, and
  a typed stub gives you autocomplete on every argument.

- **It's safe with real, messy data.** Full refreshes swap in atomically, so a
  crash never leaves a half-written table. Incremental syncs are idempotent —
  safe to re-run or retry. Transient network blips retry automatically. And
  legacy quirks like MySQL zero-dates or out-of-range timestamps are coerced to
  `NULL` with a warning instead of aborting the run.

- **There's nothing to stand up.** `pip install quickhouse` and you're done —
  no JVM, no Spark cluster, no separate service. It's an ordinary Python
  dependency that runs wherever your jobs already run: cron, Airflow, Dagster,
  a Lambda, or a plain script.

## Install

```bash
pip install quickhouse
pip install "quickhouse[progress]"   # adds a ready-made tqdm progress bar
```

Prebuilt wheels ship for Python 3.9+ on Linux, macOS (Intel + Apple Silicon),
and Windows (x86_64) — no Rust toolchain needed. Building from source is only
for development; see [CONTRIBUTING.md](CONTRIBUTING.md).

## Using it

A fuller call, with the options you'll reach for most:

```python
import quickhouse as qh

src = qh.Postgres("postgresql://user:pw@localhost:5432/shop")
dst = qh.ClickHouse("http://localhost:8123", database="analytics")

qh.sync(
    src, dst,
    dest_table="orders",
    source_table="orders",        # or source_query="SELECT ..."
    mode="incremental",           # or "full"
    watermark="updated_at",       # required for incremental
    key=["id"],                   # dedup key / ORDER BY
    parallelism=8,
    exclude=["internal_notes"],
    rename={"amount": "amt"},
    on_progress=lambda p: print(f"{p.rows_written:,} rows @ {p.rows_per_sec:,.0f}/s"),
)
```

### Sources and destinations

Pick a source and a destination by constructing the matching object —
everything else about `sync()` stays the same:

```python
# sources
qh.Postgres("postgresql://user:pw@host:5432/db")
qh.MySQL("mysql://user:pw@host:3306/db", require_tls=True)
qh.BigQuery("my-gcp-project")                       # source_table="dataset.table"

# destinations
qh.ClickHouse("http://host:8123", database="analytics")
qh.BigQuery("my-gcp-project", dataset_id="analytics")
```

BigQuery authenticates with a service-account key (`credentials_file=...`) or
Application Default Credentials. As a **destination** it also takes
`write_method`: the default `"insert_all"` (simple, proven) or the opt-in
`"storage_write"` (the gRPC Storage Write API — free and higher-throughput).

The DDL knobs (`engine`, `partition_by`, `order_by`, `primary_key`, `key`) are
interpreted per destination — for ClickHouse they shape the `MergeTree`
DDL; for BigQuery they map to partitioning and clustering. quickhouse creates
the table for you (`create_if_missing=True` by default) with a sensible schema
derived from the source.

### Full vs. incremental

**Full** reloads the whole table into a staging table, then swaps it into place
atomically — a crash mid-run never leaves the destination partial.

**Incremental** tracks a high-water mark (the `watermark` column) in a small
state table in the destination and copies only newer rows. Updated rows are
deduplicated on `key` — via ClickHouse's `ReplacingMergeTree`, or a `MERGE`
upsert on BigQuery (where `key` is therefore required). Re-running with no new
data does nothing.

For daily syncs that need to catch late-arriving or edited rows, set
`lookback_seconds` to re-scan a trailing window (e.g. `3 * 86400` for the last
three days) — the dedup above keeps that overlap from creating duplicates.

### Watching progress and diagnosing failures

`on_progress` is a plain callback you can point at anything; `qh.progress_bar()`
wraps [tqdm](https://github.com/tqdm/tqdm) for a ready-made bar. Every `sync()`
also logs each step to stderr (`RUST_LOG=quickhouse_core=debug` for the actual
SQL).

When something goes wrong, `sync()` raises a `RuntimeError` written to be
actionable on its own: it names the table involved, and for a bad config or an
unmappable column it says exactly what's wrong and how to fix it (e.g.
`exclude=` the column or cast it in a `source_query`). Underlying database
errors are surfaced verbatim rather than wrapped in something generic.

### Full parameter list

| Parameter | Meaning |
| --- | --- |
| `source_table` / `source_query` | Read a whole table, or a custom `SELECT` (one required) |
| `dest_table` | Destination table name |
| `mode` | `"full"` or `"incremental"` |
| `watermark` | Monotonic column for incremental (e.g. `updated_at`); ignored in full mode |
| `lookback_seconds` | Re-scan a trailing window of the watermark to catch late/edited rows; `0` disables (default) |
| `key` | Dedup key (required for BigQuery incremental) |
| `create_if_missing` | Auto-create the destination table (default `True`) |
| `engine`, `order_by`, `partition_by`, `primary_key` | DDL knobs, interpreted per destination |
| `parallelism` | Concurrent read streams |
| `batch_rows` / `batch_bytes` | Per-batch size knobs (rows, or estimated bytes) |
| `max_memory_bytes` | Hard ceiling on total in-flight memory; decoding blocks when hit (default 512 MiB, `0` = unbounded) |
| `type_overrides` | Force a destination column type, e.g. `{"qty": "Decimal(18, 3)"}` |
| `rename`, `include`, `exclude` | Column renames and allow/deny lists |
| `on_progress` | Progress callback |

## How types are mapped

quickhouse maps each source type to a sensible destination type automatically:
integers to integers, floats to floats, text/JSON/UUID to strings, dates and
timestamps across as-is, and booleans preserved. A few deliberate choices worth
knowing:

- **Arbitrary-precision decimals** (`numeric`/`DECIMAL`/`NUMERIC`) default to
  `Float64`, since precision can't be recovered from the type alone. Pin an
  exact type with `type_overrides` (e.g. `"Decimal(18, 2)"`).
- **`TIME`** columns transfer as canonical text into a `String` column
  (ClickHouse has no time-of-day type).
- **Out-of-range dates** (and MySQL zero-dates like `0000-00-00`) coerce to
  `NULL` with a warning rather than failing the transfer.
- **Nullable** source columns stay nullable in the destination.

Arrays and composite (`RECORD`/`STRUCT`) types aren't supported yet.

## Limitations

- **mTLS** (client-certificate auth) isn't supported; server TLS is, including
  an extra CA file via `ca_cert_file=...` for providers like AWS RDS.
- **Array / composite types** aren't mapped yet.
- **BigQuery as a source** reads through a single connection — `parallelism`
  becomes a server-side hint rather than true client-side fan-out (a limitation
  of the underlying crate's read API).
- **No CLI yet**, and CDC / custom transforms are future work.

## Contributing

Bug reports, new source/type mappings, and PRs are welcome — see
[CONTRIBUTING.md](CONTRIBUTING.md) for build steps, tests, and layout.

## License

MIT
