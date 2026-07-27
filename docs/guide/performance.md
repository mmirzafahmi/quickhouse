# Performance & safety

## How it's fast

Rows are decoded straight off the wire into Apache Arrow, in Rust — no per-row
Python, no intermediate DataFrame. Tables are split into ranges and read in
parallel, and decoding overlaps uploading. On a laptop-class box a 1M-row,
20-column full refresh runs at **hundreds of thousands of rows per second**
while peak memory stays flat (under ~180 MB) no matter how much you parallelize.
Reproduce it with `python benchmarks/bench_transfer.py`.

## Parallelism and batching

`parallelism`
: Number of concurrent read streams. The source table is split into ranges read
  in parallel (Postgres/MySQL); for BigQuery it's a server-side stream hint.

`batch_rows` / `batch_bytes`
: How big each individual Arrow batch (and thus each insert) is — a
  throughput/overhead granularity knob.

`max_memory_bytes`
: The **hard ceiling** on total in-flight batch memory across all partitions and
  uploads, measured against each batch's real Arrow allocation. Decoding
  overlaps with concurrent uploads and blocks (backpressure) when the ceiling is
  reached, so peak RSS stays bounded regardless of `parallelism` or row width.
  Default 512 MiB; `0` disables the ceiling.

## Being gentle on a small production database

Set `read_max_rows_per_sec` and quickhouse paces the read to that aggregate rate
across all partitions. Because `COPY`/streaming results only produce as fast as
the client consumes, the source scan itself backs off — you're throttling the
database's work, not just your own.

For the lightest possible footprint on a small instance, combine:

```python
qh.sync(
    qh.Postgres("postgresql://user@replica:5432/db", statement_timeout_secs=300),
    dst,
    dest_table="orders", source_table="orders",
    mode="incremental", watermark="updated_at", key=["id"],  # only new rows
    parallelism=1,                # one connection, one scan
    read_max_rows_per_sec=50_000, # cap the aggregate read rate
)
```

- `read_max_rows_per_sec` applies to PostgreSQL and MySQL; it's ignored for a
  BigQuery source (its read path is a separately-metered managed API).
- The Postgres connection reports itself as `application_name = 'quickhouse'`, so
  a DBA can see and kill it in `pg_stat_activity` (override with
  `application_name=`).

## Safety with real, messy data

- **Atomic full refresh.** A crash mid-run never leaves the destination partial
  — the staging table is swapped in only once fully written.
- **Idempotent incremental.** Safe to re-run or retry; the cursor advances only
  on success.
- **Automatic retries.** Transient sink/write blips are retried with backoff.
  `retry_max_attempts` (default `1` = no retry) additionally re-runs the whole
  transfer on a *transient source* error — PostgreSQL hot-standby
  recovery-conflict/statement-cancel, MySQL server-gone-away/lock-wait/deadlock.
  Each retry starts clean.
- **Messy data is coerced, not fatal.** MySQL zero-dates and out-of-range
  timestamps become `NULL` with a warning instead of aborting the run (see
  [Type mapping](type-mapping.md)).

## Watching progress and diagnosing failures

`on_progress` is a plain callback you can point at anything:

```python
qh.sync(..., on_progress=lambda p: print(f"{p.rows_written:,} @ {p.rows_per_sec:,.0f}/s"))
```

`quickhouse.progress_bar()` wraps [tqdm](https://github.com/tqdm/tqdm) for a
ready-made bar (`pip install "quickhouse[progress]"`):

```python
with qh.progress_bar() as on_progress:
    qh.sync(..., on_progress=on_progress)
```

Every `sync()` also logs each step to stderr; set
`RUST_LOG=quickhouse_core=debug` to see the actual SQL/DDL text.

When something goes wrong, `sync()` raises a `RuntimeError` written to be
actionable on its own: it names the table involved, and for a bad config or an
unmappable column it says exactly what's wrong and how to fix it (e.g. `exclude=`
the column or cast it in a `source_query`). Underlying database errors are
surfaced verbatim rather than wrapped in something generic.
