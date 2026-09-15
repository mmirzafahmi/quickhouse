# Performance & safety

## How it's fast

Rows are decoded straight off the wire into Apache Arrow, in Rust — no per-row
Python, no intermediate DataFrame. Tables are split into ranges and read in
parallel, and decoding overlaps uploading. On a laptop-class box a 1M-row,
20-column full refresh runs at **hundreds of thousands of rows per second**
while peak memory stays flat (under ~180 MB) no matter how much you parallelize.
Reproduce it with `python benchmarks/bench_transfer.py`. For quickhouse measured
head-to-head against other tools, see the [Benchmark](benchmark.md) page.

## Parallelism and batching

```{raw} html
<div class="qh-params">
  <div>
    <div>
      <div class="qh-params__name">parallelism</div>
      <div class="qh-params__type">int</div>
    </div>
    <p class="qh-params__desc">Number of concurrent read streams. The source table is split into ranges read in parallel (Postgres/MySQL/ClickHouse); for BigQuery it's a server-side stream hint.</p>
  </div>
  <div>
    <div>
      <div class="qh-params__name">batch_rows / batch_bytes</div>
      <div class="qh-params__type">int</div>
    </div>
    <p class="qh-params__desc">How big each individual Arrow batch (and thus each insert) is &mdash; a throughput/overhead granularity knob.</p>
  </div>
  <div>
    <div>
      <div class="qh-params__name">max_memory_bytes</div>
      <div class="qh-params__type">int = 512 MiB</div>
    </div>
    <p class="qh-params__desc">The <strong>hard ceiling</strong> on total in-flight batch memory across all partitions and uploads, measured against each batch's real Arrow allocation. Decoding overlaps with concurrent uploads and blocks (backpressure) when the ceiling is reached, so peak RSS stays bounded regardless of <code>parallelism</code> or row width. <code>0</code> disables the ceiling.</p>
  </div>
</div>
```

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

- `read_max_rows_per_sec` applies to PostgreSQL, MySQL and ClickHouse; it's ignored for a
  BigQuery source (its read path is a separately-metered managed API).
- The Postgres connection reports itself as `application_name = 'quickhouse'`, so
  a DBA can see and kill it in `pg_stat_activity` (override with
  `application_name=`).

## Two timeouts, and which one you actually want

```{admonition} `statement_timeout_secs` bounds the whole transfer, not the query
:class: warning
quickhouse streams the source result set straight into the destination, so the
statement producing it stays open from the first row read to the last one
written. The server counts **read + decode + destination write + backpressure**
against `statement_timeout_secs` — it is a ceiling on the transfer, not on the
query, despite the name.

Two consequences bite in production. The failure **points at the wrong
subsystem**: a measured 0.64s source scan (EXPLAIN ANALYZE, all shared-buffer
hits) failed 9 consecutive times against a 90s ceiling because the *destination*
was throttling that day, and it failed as `57014 canceling statement due to
statement timeout` — sending an operator hunting for a slow query and a missing
index that did not exist. And **retries cannot rescue it**: every attempt
restarts from zero against the same ceiling, so a table that crosses the line
fails every attempt, on a condition that has nothing to do with the source.
```

`read_idle_timeout_secs` (new in 0.15.0) is the knob the name above suggests:
fail when **no source rows arrive** for that long.

```python
qh.sync(
    qh.Postgres(dsn, statement_timeout_secs=1800),  # guard the source database
    dst, ...,
    read_idle_timeout_secs=120,                     # detect a hung source
)
```

The timer wraps the awaits on the source stream and nothing else. Time spent
decoding, inserting, or blocked on the memory budget doesn't count toward it, so
a slow destination can't trip it — which is what makes it safe to set tightly —
while a genuinely hung source does. The resulting error is classified transient,
so `retry_max_attempts` retries the whole transfer.

Set both, and `statement_timeout_secs` goes back to meaning what it should: a
guard on the source database against a runaway scan, sized for that database
rather than for the destination's worst day.

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
- **...and since 0.15.0 those warnings are data, not just log text.** Every
  coercion quickhouse performs, plus the conditions it can only warn about
  (rows excluded forever by a NULL watermark; a permitted full-refresh shrink;
  an unclustered MERGE target), lands on `TransferResult.warnings` — see
  [Failing a run on a warning](#failing-a-run-on-a-warning) below.

(failing-a-run-on-a-warning)=
## Failing a run on a warning

Some things quickhouse detects can't be errors — the transfer succeeded, the
rows landed, and stopping would be worse than continuing. But they changed what
the destination holds, and only the caller knows whether that's acceptable.

Since 0.15.0 those reach you as data on `TransferResult.warnings`, not just as
log lines an orchestrator can't branch on:

```python
result = qh.sync(...)

FATAL = {"collapsed_bool", "null_watermark", "coerced_decimal"}
for w in result.warnings:
    print(f"{w.kind} {w.column}: {w.count}")
    if w.kind in FATAL:
        raise RuntimeError(str(w))
```

Each warning carries `.kind` (stable, machine-readable — match on this, never on
`.message`), `.column`, `.count`, `.sample` and `.message`, aggregated per
`(kind, column)` across the run and ordered most-affected first.

| `kind` | What happened |
| --- | --- |
| `null_watermark` | The watermark column is nullable and rows hold `NULL` there. `WHERE watermark > x` never matches `NULL`, so those rows are excluded from this and **every future** incremental run. The most dangerous one — the transfer reports success while permanently dropping rows. |
| `collapsed_bool` | A MySQL `tinyint(1)` value outside `{0, 1}` was flattened to a boolean, losing e.g. the difference between 2 and 3. `type_overrides` can't repair it after the fact, so early detection is the only defence — see `tinyint1_as_bool=False`. |
| `coerced_decimal` | A decimal became `NULL`: it exceeded the declared `Decimal(P,S)`, or was NaN/Infinity. Silent data loss with a correct-looking row count. |
| `coerced_date` | A date/datetime became `NULL` — a zero-date, or a year outside ClickHouse's representable 1900–2299 window. |
| `coerced_scalar` | An API source's scalar didn't parse as its declared type and became `NULL`. |
| `full_refresh_shrink` | A full refresh left the destination smaller than it was, permitted by `allow_full_refresh_shrink=True`. (Without that flag the same condition is a hard error.) |
| `unclustered_merge_target` | A BigQuery `MERGE` ran against a destination not clustered by the merge key, so the key bound pruned nothing and the statement scanned the whole table. A cost problem, not a data one. |

Nothing is raised for you: these are values, and which of them should fail a
pipeline is a decision about your data, not about quickhouse.

## Where the time actually went

`TransferResult` also breaks the run down by phase, so you can tell which
subsystem to tune instead of inferring it from job metadata afterwards:

```python
r = qh.sync(...)
print(f"read {r.read_secs:.1f}s  stage {r.stage_secs:.1f}s  promote {r.promote_secs:.1f}s")
```

- **`read_secs`** — time spent *waiting on source rows*: the awaits on the source
  stream alone, excluding decode, insert and backpressure. Summed across parallel
  readers, so with `parallelism > 1` it can legitimately exceed `stage_secs`;
  compare `read_secs / parallelism` against `stage_secs`.
- **`stage_secs`** — the streaming phase: reading, decoding and writing every row
  into the destination (or this run's staging table).
- **`promote_secs`** — what follows: the full-refresh swap, the incremental
  `MERGE` or insert-select, the window-scoped delete, the watermark persist. On a
  BigQuery destination this is usually most of the run — a measured 299,540-row
  window spent 70–85% of its time in the `MERGE` and ~9% in the read.

If `promote_secs` dominates, tune the destination DDL (clustering, the merge
prunes above). If `read_secs / parallelism` approaches `stage_secs`, the source
query is the bottleneck.

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
