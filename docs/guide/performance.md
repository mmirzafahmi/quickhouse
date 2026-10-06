# Performance & safety

## How it's fast

Rows are decoded straight off the wire into Apache Arrow, in Rust — no per-row
Python, no intermediate DataFrame. Tables are split into ranges and read in
parallel, and decoding overlaps uploading. On a laptop-class box a 1M-row,
20-column full refresh runs at **hundreds of thousands of rows per second**,
and memory is bounded by `max_memory_bytes` rather than by table size (see
[what memory scales with](#what-memory-scales-with) below). Reproduce it with
`python benchmarks/bench_transfer.py`. For quickhouse measured head-to-head
against other tools, see the [Benchmark](benchmark.md) page.

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
    <p class="qh-params__desc">The <strong>hard ceiling</strong> on total in-flight batch memory across all partitions and uploads, measured against each batch's real Arrow allocation. Decoding overlaps with concurrent uploads and blocks (backpressure) when the ceiling is reached, so batch memory stays bounded regardless of <code>parallelism</code> or row width. Each stream also has working memory outside it; see below. <code>0</code> disables the ceiling.</p>
  </div>
</div>
```

(what-memory-scales-with)=
## What memory scales with

Memory never grows with table size, but it does grow with `parallelism`. Each
stream collects up to `insert_bytes` (32 MiB) of batches before it sends an
insert, and those count against `max_memory_bytes`. Each stream also has
working memory the ceiling doesn't cover: the batch being decoded and the insert
being serialized.

Peak RSS measured on a 2-vCPU VM, PostgreSQL → ClickHouse, TPC-H `lineitem`
(6M rows, 16 columns), default settings:

| `parallelism` | 1 | 2 | 4 | 8 | 16 |
| --- | --- | --- | --- | --- | --- |
| Peak RSS | ~300–390 MB | ~350–450 MB | ~490–540 MB | ~620–700 MB | ~850–930 MB |

On that box the extra streams added no speed: every one of those runs took
about 25 s, because the source, ClickHouse and quickhouse shared two cores. More
streams pay off when the source has idle cores to serialize `COPY` output. To
hold memory down, lower `insert_bytes` or `max_memory_bytes` before lowering
`parallelism`.

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

## When the watermark column has no index

An incremental run probes the watermark column before any data moves: a
`MAX(watermark)` to pin the window's upper bound, and — on a nullable column —
a `count(*) WHERE watermark IS NULL` to catch rows a `>` predicate would
silently exclude forever. With an index both are trivial. Without one, each is
a full sequential scan of the whole table, **every run**.

Measured against a PostgreSQL 16 standby, on a 14.9 GB table:

| Probe | Unindexed | Indexed (same server) |
| --- | --- | --- |
| `count(*) WHERE wm IS NULL` | 56.65s (planner cost 3,031,034) | cost 2.07 |
| `MAX(wm)` | 57.19s (cost 3,031,091) | cost 0.65 |

quickhouse asks the planner what each would cost — `EXPLAIN`, never `ANALYZE`,
so nothing is executed — and skips those above `probe_max_cost`. Asking the
planner rather than the catalog matters: an index lookup has no base table to
consult when the transfer reads through a `source_query`, whereas `EXPLAIN`
plans the real statement.

When the `MAX` probe is skipped, the cursor is taken from the rows actually
read. That needs `lookback_seconds > 0` — see below. A skipped `MAX` is what
`unindexed_watermark` reports, and what switches the read to a sweep of bounded
key windows; a skipped NULL count alone does neither (an indexed watermark with
many `NULL`s prices that count high too), and is reported as
`null_check_skipped`.

Through a `source_query`, the `MAX` inherits the query's filter: `MAX(id)` over
`... WHERE is_test = 0` can't come from the primary key, even though the read
itself, `WHERE id > x` pushed into the query, is a range scan. Set `source_table`
as well, and a `MAX` too costly through the query is probed on the table,
unfiltered. It then only bounds the read: the cursor saved is the largest
watermark the read actually returned, so a row the filter excludes can't carry
the cursor past rows not read yet. This assumes `source_query` projects the
table's watermark column unchanged; for one it converts (a time-zone shift,
say), set `probe_max_cost=0` and the query's own `MAX` is always used. The
table is not probed for a `chunk_rows` read, a `watermark_source_expr`, a
watermark the transfer leaves out, transforms or overrides, a `skip_to_max`
first run, or a table whose `MAX` doesn't read as the query column's own type:
those keep the query's plan. With a cursor, a watermark that is the window key
itself is never swept either: the read's own `key > cursor` already bounds it.

```{admonition} The skipped check is a real check
:class: warning
Where the nullable-watermark completeness count is skipped, quickhouse can no
longer tell you whether rows hold a NULL watermark — and such rows are excluded
from this and every future incremental run. The `null_check_skipped` warning
says so, and quotes the estimate that caused it. (A read that returns rows with
no watermark at all is still reported, as `null_watermark`.) Treat the warning
as a prompt to add the index:

    CREATE INDEX CONCURRENTLY ON your_table (your_watermark_column);  -- PostgreSQL
    ALTER TABLE your_table ADD INDEX (your_watermark_column),
        ALGORITHM=INPLACE, LOCK=NONE;                                 -- MySQL
```

```{admonition} Why the stream cursor needs a lookback
:class: note
Without the frozen upper bound, rows written mid-read with high watermarks are
read too, so the cursor can land *above* the `MAX` that bound would have used —
and anything in that widened band which was not read would then be skipped. A
lookback exceeding the read's own duration re-covers it on the next run. With
`lookback_seconds=0` quickhouse pays for the `MAX` scan rather than take that
risk. A read of several statements (a sweep of windows, range partitions,
`chunk_rows`) can take longer than the lookback, so its cursor is moved back by
the difference: a row updated in a window already read is then read on the next
run.
```

The filter itself still scans — nothing but an index fixes that. On a hot
standby the cost is not only time: a multi-second scan is a candidate for
`max_standby_streaming_delay` cancellation (`SQLSTATE 40001, canceling
statement due to conflict with recovery`). Retrying does not rescue it — every
attempt restarts from zero against the same ceiling and re-pays the same scan,
and the backoff schedule is far shorter than the conflict window. Remove the
scan, or raise the standby's tolerance.

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
  recovery-conflict/statement-cancel, MySQL server-gone-away/lock-wait/deadlock/
  interrupted query/`MAX_EXECUTION_TIME`. Each retry starts clean, so rows a
  failed attempt already inserted are inserted again. Into a ClickHouse engine
  that keeps those copies (a `MergeTree` other than `ReplacingMergeTree`), an
  incremental run with retries stages each attempt and moves it into the
  destination at the end, so a failed one leaves nothing behind. Anywhere else
  a retry after a partial write raises `retried_after_partial_write` and counts
  the rows in `TransferResult.rows_written_failed_attempts`.
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
for w in result.warnings:
    print(f"{w.kind} {w.column}: {w.count}")
```

To fail a run on some of them, name them in `fail_on_warnings` (0.20.7):

```python
qh.sync(..., fail_on_warnings={"collapsed_bool", "null_watermark", "coerced_decimal"})
```

The check runs inside the transfer, before each step that makes anything
permanent: before a full refresh's swap, before an incremental `MERGE` or
insert-select, before the cursor is saved, and before each `chunk_rows` chunk is
committed. Stopped before its `MERGE` or swap, the destination is untouched. An
incremental that inserts straight into ClickHouse has written its rows, but the
cursor isn't saved, so once the cause is fixed the next run reads the same range
again (a `ReplacingMergeTree` converges on it). The error names the kind, the
column, the count and what was left in place.

Raising on `result.warnings` after `sync()` returns can't do that. By then the
cursor is saved: the run goes red once, the orchestrator's retry starts past
the rows the warning was about, and goes green.

Each warning carries `.kind` (stable, machine-readable — match on this, never on
`.message`), `.column`, `.count`, `.sample` and `.message`, aggregated per
`(kind, column)` across the run and ordered most-affected first.

| `kind` | What happened |
| --- | --- |
| `null_watermark` | The watermark column is nullable and rows hold `NULL` there. `WHERE watermark > x` never matches `NULL`, so those rows are excluded from this and **every future** incremental run. The most dangerous one — the transfer reports success while permanently dropping rows. Also raised when an incremental read returns rows and none has a watermark: there is no cursor to save, so every run re-reads them all. |
| `collapsed_bool` | A MySQL `tinyint(1)` value outside `{0, 1}` was flattened to a boolean, losing e.g. the difference between 2 and 3. `type_overrides` can't repair it after the fact, so early detection is the only defence — see `tinyint1_as_bool=False`. |
| `coerced_decimal` | A decimal became `NULL`: it exceeded the declared `Decimal(P,S)`, or was NaN/Infinity. Silent data loss with a correct-looking row count. |
| `coerced_date` | A date/datetime became `NULL` — a zero-date, or a year outside ClickHouse's representable 1900–2299 window. |
| `coerced_scalar` | An API source's scalar didn't parse as its declared type and became `NULL`. |
| `full_refresh_shrink` | A full refresh left the destination smaller than it was, permitted by `allow_full_refresh_shrink=True`. (Without that flag the same condition is a hard error.) |
| `unclustered_merge_target` | A BigQuery `MERGE` ran against a destination not clustered by the merge key, so the key bound pruned nothing and the statement scanned the whole table. A cost problem, not a data one. |
| `watermark_not_advanced` | The `MAX(watermark)` probe found a value past the run's lower bound, yet the read returned 0 rows. The row holding that MAX matches the filter, so the cursor and the predicate disagree: the cursor may never advance again. Only raised when the probe ran. |
| `watermark_ahead_of_source` | The saved cursor (or `seed_watermark`) is past the source's `MAX(watermark)`: shifted by a time-zone conversion, seeded from another table, or the source's newest rows were deleted. Rows between the cursor's true position and the MAX may have been skipped. Only raised when the probe ran. A `source_query` that filters out the newest rows also has a MAX below a correct cursor: set `source_table` too and the cursor is checked against the unfiltered table instead, with nothing raised unless it is ahead of that as well; without it, the message names the filter as a possible cause. Either way the cursor goes back to the MAX, which costs a re-read at most. |
| `decimal_mapping_mixed` | The destination mixes exact `Decimal` and `Float64` columns that are all fed by declared-precision source decimals. ClickHouse has no arithmetic or common type across the two. |
| `shifted_timestamp` | A MySQL `TIMESTAMP` column is read in a session whose time zone isn't UTC. MySQL renders each value in that zone and quickhouse stores the wall-clock time as UTC, so every value lands shifted by the offset (the `sample`, e.g. `UTC+07:00`). Fix it with `utc_session=True` on `MySQL(...)`: see [type mapping](type-mapping.md). |
| `window_bounds_unavailable` | The read plans as a sequential scan and should have been swept in key windows, but the key-bounds probe that windowing needs failed (after retries, for a transient error), so the read ran in one pass: the long scan a hot standby tends to cancel. Set `source_table` alongside a filtered `source_query` so the bounds come from the table. |
| `null_check_skipped` | The nullable watermark's completeness count was too costly to run (see `probe_max_cost`), so the run can't say whether rows hold a `NULL` watermark. Its own kind since 0.20.7: an indexed watermark with many `NULL`s prices the count high too, and that says nothing about the read. |
| `retried_after_partial_write` | `retry_max_attempts` re-ran the transfer after an attempt that had already written `count` rows into the destination, and the retry writes them again. A `ReplacingMergeTree` collapses the copies at its next merge; an engine that keeps duplicates keeps them. |
| `storage_write_count_mismatch` | A BigQuery Storage Write stream finalized with a row count other than the rows appended to it: a retried append was duplicated, or one was lost. `count` is the difference. It fails the run before the `MERGE` or swap that would promote that staging table, and before a `chunk_rows` chunk is committed, whether or not `fail_on_warnings` names it; an append, whose rows are in the destination already, reports it. Use `write_method="insert_all"` if it recurs. |

Unless you name them in `fail_on_warnings`, nothing is raised for you: these
are values, and which of them should fail a pipeline is a decision about your
data, not about quickhouse.

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
  BigQuery destination it dominates small runs — a measured 299,540-row window
  spent 70–85% of its time in the `MERGE` and ~9% in the read — while a
  10M-row run spent most of its time in `stage_secs` (76–96 s, against an
  11–12 s `MERGE`).

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
