# Benchmark

*Last updated: 2026-09-29*

This page reports a head-to-head benchmark of quickhouse against two widely used
Python/Go EL tools — [dlt](https://dlthub.com/) and [Sling](https://slingdata.io/) —
moving the same data, under the same constraints, into both BigQuery and
ClickHouse. It also includes an [ADBC](https://arrow.apache.org/adbc/)-based
measurement that isolates how much of the total time is spent reading the
source versus writing the destination.

This benchmark was run against production-shaped tables and queries from a
real deployment (not a synthetic schema), and the raw scripts are linked at
the bottom so the numbers can be reproduced or challenged.

This edition moves **10 million rows** per run. The previous one (2026-07-31)
moved a ~300k-row slice; its headline numbers are kept under
[Previous edition](#previous-edition-300k-rows) for comparison, and some of
its conclusions did not hold at this scale.

```{note}
This benchmark is maintained by quickhouse's author. We've tried to be fair —
every tool reads byte-identical SQL, every result is checked for row-level
correctness, and the [Limitations](#limitations) section below is not an
afterthought. But you should still treat "the author benchmarked their own
tool" as the caveat it is, and we'd welcome PRs that improve or challenge any
part of this.
```

## TL;DR

Same 10M-row slice, same source query, same primary-key merge semantics,
3 timed runs per tool after one untimed priming run, into a warm
(already-populated) destination table.

```{raw} html
<ul class="qh-bench__facts">
  <li>2 vCPU / 16 GB GCE VM</li>
  <li>quickhouse 0.20.4</li>
  <li>Sling 1.6.4</li>
  <li>dlt 1.30.0</li>
  <li>3 runs · min–max</li>
  <li>row + checksum verified against the source</li>
</ul>

<div class="qh-bench">
  <div class="qh-bench__switch" role="tablist" aria-label="Destination">
    <button type="button" role="tab" aria-selected="true" tabindex="0">&rarr; ClickHouse</button>
    <button type="button" role="tab" aria-selected="false" tabindex="-1">&rarr; BigQuery</button>
  </div>

  <div class="qh-bench__panel" role="tabpanel" aria-label="ClickHouse destination">
    <div class="qh-bench__caption">
      <span>10M-row primary-key merge, wall clock</span>
      <span>lower is better</span>
    </div>
    <div class="qh-bars">
      <div class="qh-bar qh-bar--lead">
        <span class="qh-bar__name">quickhouse</span>
        <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:3%;--qh-delay:80ms"></span></span>
        <span class="qh-bar__value">35.1–35.7 s</span>
      </div>
      <div class="qh-bar">
        <span class="qh-bar__name">Sling</span>
        <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:24%;--qh-delay:200ms"></span></span>
        <span class="qh-bar__value">295–302 s</span>
      </div>
      <div class="qh-bar">
        <span class="qh-bar__name">dlt</span>
        <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:100%;--qh-delay:320ms"></span></span>
        <span class="qh-bar__value">1,256–1,258 s</span>
      </div>
    </div>
    <p class="qh-bench__takeaway"><strong>8.3–8.6× faster than Sling, ~35× faster than dlt.</strong>
    quickhouse moved 10M rows from MySQL into ClickHouse at ~280,000 rows/s;
    reading them off MySQL took ~10 s of that.</p>
  </div>

  <div class="qh-bench__panel" role="tabpanel" aria-label="BigQuery destination" hidden>
    <div class="qh-bench__caption">
      <span>10M-row primary-key merge, wall clock</span>
      <span>lower is better</span>
    </div>
    <div class="qh-bars">
      <div class="qh-bar qh-bar--lead">
        <span class="qh-bar__name">quickhouse</span>
        <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:7%;--qh-delay:80ms"></span></span>
        <span class="qh-bar__value">90–111 s</span>
      </div>
      <div class="qh-bar">
        <span class="qh-bar__name">Sling</span>
        <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:23%;--qh-delay:200ms"></span></span>
        <span class="qh-bar__value">282–284 s</span>
      </div>
      <div class="qh-bar">
        <span class="qh-bar__name">dlt</span>
        <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:100%;--qh-delay:320ms"></span></span>
        <span class="qh-bar__value">1,221–1,253 s</span>
      </div>
    </div>
    <p class="qh-bench__takeaway"><strong>2.5–3.2× faster than Sling, 11–14× faster than dlt</strong>
    — and the fewest bytes billed of the three.</p>

    <div class="qh-bench__sub">
      <h4>Bytes billed per run — <code>INFORMATION_SCHEMA.JOBS</code>, top-level jobs only</h4>
      <div class="qh-bars">
        <div class="qh-bar qh-bar--lead">
          <span class="qh-bar__name">quickhouse</span>
          <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:68%;--qh-delay:80ms"></span></span>
          <span class="qh-bar__value">~3.3 GiB</span>
        </div>
        <div class="qh-bar">
          <span class="qh-bar__name">Sling</span>
          <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:87%;--qh-delay:200ms"></span></span>
          <span class="qh-bar__value">~4.2 GiB</span>
        </div>
        <div class="qh-bar">
          <span class="qh-bar__name">dlt</span>
          <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:100%;--qh-delay:320ms"></span></span>
          <span class="qh-bar__value">~4.8 GiB</span>
        </div>
      </div>
      <div class="qh-modes">
        <div class="qh-mode qh-mode--current">
          <div class="qh-mode__name">quickhouse</div>
          <div class="qh-mode__desc">$20 per 1,000 syncs &middot; 1.0&times; baseline</div>
        </div>
        <div class="qh-mode">
          <div class="qh-mode__name">Sling</div>
          <div class="qh-mode__desc">$26 per 1,000 syncs &middot; 1.3&times; the bytes</div>
        </div>
        <div class="qh-mode">
          <div class="qh-mode__name">dlt</div>
          <div class="qh-mode__desc">$30 per 1,000 syncs &middot; 1.5&times; the bytes</div>
        </div>
      </div>
      <p class="qh-bench__takeaway">Derived from the bytes above at on-demand pricing
      ($6.25/TiB scanned), not measured off an invoice. The gap is much narrower
      than on the 300k-row slice (4–7&times;): here every row in the destination is
      part of the merge, so every tool has to scan the whole 2 GiB table, and the
      fixed per-run queries that dominated a small run are now noise.</p>
    </div>
  </div>
</div>
```

The biggest structural difference is on the write side: quickhouse issues
**one** `MERGE` against the destination, while dlt and Sling stage into a
temporary table and then run a `DELETE` followed by an `INSERT` — two
destination-side passes instead of one. That is where the BigQuery bytes gap
comes from. It is *not* what decides wall clock at this scale: Sling and dlt
take the same time into either destination, and quickhouse's own BigQuery time
is mostly streaming rows in, not the `MERGE` (see
[Where the time actually goes](#where-the-time-actually-goes)).

```{raw} html
<div class="qh-passes">
  <div class="is-lead">
    <h4>quickhouse</h4>
    <div class="qh-passes__ops">stage &rarr; <b>MERGE</b></div>
    <p>One destination-side pass.</p>
  </div>
  <div>
    <h4>dlt · Sling</h4>
    <div class="qh-passes__ops">stage &rarr; <b>DELETE</b> &rarr; <b>INSERT</b></div>
    <p>Two passes — the extra bytes billed on BigQuery.</p>
  </div>
</div>
```

## Methodology

- **Environment:** a single GCE VM (n2-highmem-2: 2 vCPU / 16 GB RAM), same box
  for every tool, same network path to the source databases and destinations.
  The previous edition ran on a 4 vCPU box, so absolute numbers are not
  comparable across editions.
- **Source data:** a bounded, fixed `id` window of exactly 10,000,000 rows
  pulled directly from production MySQL and PostgreSQL read replicas — not a
  synthetic benchmark schema. The window stops short of the newest rows, which
  are still changing, so every run reads the same data. Two tables were used:
  - `user_order` — MySQL, 21 columns, 10,000,000 rows (orders created
    2026-06-21 to 2026-09-20).
  - `sale_order_line` (odoo) — PostgreSQL, 29 columns, 10,000,000 rows (used
    for the ADBC extract-only measurement).
- **Identical SQL across tools.** Every tool read quickhouse's own generated
  source query (the one it already uses in production, which `CAST`s a
  handful of columns to normalize types across MySQL/Postgres/BigQuery). This
  matters: without those casts, at least one competing tool crashes on a
  `TIME` column (see [Gotchas](#gotchas-and-integration-notes)) — giving every
  tool the same query is the fair comparison, not a handicap for quickhouse.
- **Identical semantics.** Every tool was configured for an upsert on the
  table's primary key (`merge` in dlt, `--primary-key` in Sling, `key=` in
  quickhouse). Each tool first ran once, untimed, to create and fill its own
  destination table; every timed run then merges into that warm table, so it
  is a genuine merge, not a first-time bulk load.
- **Defaults otherwise.** Each tool ran single-stream with its default settings.
  quickhouse got the two options its production job passes for this table:
  `partition_by` on BigQuery and a `ReplacingMergeTree` on ClickHouse.
- **3 timed runs per tool per destination.** Reported ranges are min–max across
  those runs, not a single sample. Within each destination the tool order was
  rotated every round, so no tool always ran first.
- **Wall clock and memory measured from outside the process**: every tool was
  launched as a subprocess and timed end to end, including interpreter start-up
  and credential lookup. Memory is peak RSS summed over the whole process tree,
  sampled every 0.2 s — not a Python-level profiler, which only traces Python
  allocations and would under-report quickhouse, whose buffering happens in
  Rust.
- **Correctness checked against the source**, not assumed: exact row count,
  exact distinct-primary-key count, and a checksum on a numeric ("money")
  column, each compared with the same aggregates computed on MySQL itself.
- **BigQuery cost measured from `INFORMATION_SCHEMA.JOBS`**, counting only
  top-level query jobs issued by the benchmark's service account inside each
  run's time window (a `SCRIPT`-wrapped job restates its children's billed
  bytes, which double-counts if not filtered out).

## Results: BigQuery destination

MySQL `user_order`, 21 columns, 10,000,000 rows:

| Tool | Wall-clock | Rows/sec | Peak RSS | Bytes billed / run |
|---|---:|---:|---:|---:|
| **quickhouse** 0.20.4 | 89.7 – 110.8 s | 90,000 – 111,000 | 320 – 352 MB | 3.0 – 3.5 GiB |
| Sling 1.6.4 | 281.9 – 284.2 s | 35,200 – 35,500 | 286 – 288 MB | 4.2 GiB |
| dlt 1.30.0 (parquet loader) | 1,220.7 – 1,253.0 s | 8,000 – 8,200 | 520 – 522 MB | 4.8 GiB |

quickhouse's memory is higher than on the 300k-row slice (~100 MB) because it
is bounded by its buffers (`max_memory_bytes`, `insert_bytes`), not by table
size, and a 10M-row run fills them; see
[what memory scales with](performance.md#what-memory-scales-with).

### Where the time actually goes

quickhouse reports its own split on every run (`read_secs`, `stage_secs`,
`promote_secs`; see [Performance](performance.md)). For the MySQL runs above:

| quickhouse, MySQL `user_order` | → BigQuery | → ClickHouse |
|---|---:|---:|
| Read the source query alone | 8.9 – 9.4 s | 10.0 – 10.4 s |
| Stage: read, decode and write every row | 76.1 – 95.8 s | 32.9 – 33.6 s |
| Promote: the `MERGE` (ClickHouse inserts directly) | 11.0 – 12.5 s | 0 s |

At this scale the `MERGE` is only **~12%** of quickhouse's BigQuery time.
Streaming 10M rows into the staging table through the Storage Write API is
**~85%**. Reading is about 10% on either destination. The 300k-row edition
found the opposite (the `MERGE` was 70–85% of the run), because a `MERGE` has a
large fixed cost that a small slice cannot amortize.

To isolate the read side without any tool's load layer we used
[Apache Arrow ADBC](https://arrow.apache.org/adbc/), which only extracts. ADBC
has no MySQL driver, so this measurement uses the PostgreSQL source
(`sale_order_line`, 29 columns, 10,000,000 rows). The replica cancels any
query whose snapshot outlives its 30 s `max_standby_streaming_delay`, so both
read in the same 500,000-row keyset chunks (quickhouse's `chunk_rows`, which
production uses for this table; a hand-written keyset loop for ADBC):

| Stage | Time | Rows/sec |
|---|---:|---:|
| ADBC extract → Arrow (streaming, 20 chunks) | **111.6 – 123.4 s** | 81,000 – 90,000 |
| quickhouse, full extract + load + merge (20 chunks) | 321.8 – 367.5 s | 27,200 – 31,100 |
| — of which reading the source (`read_secs`) | 100.5 – 129.1 s | — |
| — of which the 20 BigQuery `MERGE`s | **117 – 131 s** | — |

quickhouse's reader runs at the extract-only ceiling: its `read_secs` over the
same chunks is about as long as ADBC's whole extract. The twenty `MERGE`s are
over a third of the run, far more than the single 11–12 s `MERGE` of the
unchunked MySQL runs above: chunking pays a `MERGE`'s fixed cost once per
chunk. What is left, under a third, is streaming rows into BigQuery. The
extra `MERGE`s are the price of surviving the standby's 30 s limit; a primary,
or a replica with a longer delay, can read in fewer, larger chunks.

## Results: ClickHouse destination

Same MySQL `user_order` window, into a ClickHouse Cloud instance:

| Tool | → BigQuery | → ClickHouse | Rows/sec on ClickHouse | Peak RSS |
|---|---:|---:|---:|---:|
| **quickhouse** | 89.7 – 110.8 s | **35.1 – 35.7 s** | 280,000 – 285,000 | 268 – 306 MB |
| Sling | 281.9 – 284.2 s | 295.1 – 302.4 s | 33,000 – 34,000 | 360 – 367 MB |
| dlt | 1,220.7 – 1,253.0 s | 1,255.8 – 1,258.5 s | ~8,000 | 341 – 343 MB |

Three things stand out here:

1. **Sling and dlt take the same time into either destination.** Sling ran
   282–284 s into BigQuery and 295–302 s into ClickHouse; dlt 1,221–1,253 s and
   1,256–1,258 s. At 10M rows their bottleneck is upstream of the destination
   — reading and serializing rows — so no destination-side optimization will
   help either of them. (On the 300k-row slice this held for dlt only; Sling
   was 3× slower into BigQuery, where its temp-table + `DELETE` + `INSERT`
   fixed cost dominated a small run.)
2. **quickhouse is the only tool whose time depends on the destination**
   (35 s vs. 90–111 s). Its reader is not the constraint — about 10 s for
   10M rows on both — so what remains is the destination write path.
3. **dlt looks CPU-bound in Python.** A mid-run sample showed its process at
   one fully used core, which fits how little its times vary between runs
   (under 3%).

## Data integrity and storage footprint

All three tools produced results that match the source exactly on both
destinations: 10,000,000 rows, 10,000,000 distinct primary keys, and a
`SUM(uor_subtotal)` equal to MySQL's own, checked after each tool's last run.
But "logically correct" and "no ongoing cost" are not the same thing, and this
is worth surfacing rather than burying:

| Tool | Engine used | Logical rows | Physical rows on disk | Disk used |
|---|---|---:|---:|---:|
| quickhouse | `ReplacingMergeTree` | 10,000,000 | 11,732,105 (1.17×) | 324 MiB |
| Sling | `MergeTree` | 10,000,000 | 10,000,000 (1.0×) | 230 MiB |
| dlt | `MergeTree` + lightweight deletes | 10,000,000 | **20,000,000 (2.0×)** | **695 MiB** |

Figures are active parts after four runs (the priming run plus three timed
ones) and were unchanged when measured again two hours later.

dlt implements its merge on ClickHouse via
[lightweight deletes](https://clickhouse.com/docs/en/sql-reference/statements/delete):
the "old" version of an updated row is marked deleted but stays physically on
disk until a background mutation reclaims it. dlt's table held 2× the physical
rows of Sling's for the same logical data, and 3× its disk. `SELECT count(*)`
won't show this (it correctly excludes masked rows); you have to check
`system.parts ... WHERE active` to see it. On a long-running pipeline this is
ongoing merge and storage cost that compounds with every sync cycle.

quickhouse is not at 1.0× either, which the 300k-row slice did not show.
Its `ReplacingMergeTree` drops an older copy of a row only when a background
merge combines the parts holding both copies. Four 10M-row inserts wrote 40M
rows; ClickHouse had collapsed them to 11.7M across five parts and merged no
further in the next two hours, since background merges are opportunistic and
never promise a single part. Queries need `FINAL` (or
`LIMIT 1 BY <key>`) for exact results until then, and
`OPTIMIZE TABLE ... FINAL` forces the collapse at the cost of rewriting the
table.

## Limitations

Publishing your own benchmark without stating where it's weak isn't a fair
benchmark, so:

- **dlt's high-throughput path was never successfully benchmarked.** dlt's
  recommended fast configuration is its `sql_database` source with a
  Rust/Arrow-backed extraction backend (`connectorx`), not the plain Python
  generator resource used for the numbers above. The previous edition
  attempted this five times and hit five distinct integration issues (see
  [Gotchas](#gotchas-and-integration-notes)) before setting it aside; this
  edition used the same plain resource, over a streaming (server-side) MySQL
  cursor. **The dlt numbers above reflect "dlt with a straightforward Python
  resource," not dlt's ceiling — the gap could be much smaller with that
  backend working.**
- **Whole-table merges.** Every run merges its window into a table that holds
  only that window, so every row is an update and there is nothing to prune.
  That is the worst case for merge cost. A 10M-row delta into a much larger,
  clustered table would bill differently, and quickhouse's key-range pruning
  would matter more.
- **Two slice sizes so far.** ~300k rows (a single incremental sync cycle in
  the source deployment) and 10M rows (a large catch-up or backfill). The
  ordering of the three tools held at both; the margins, and where
  quickhouse's time goes, did not.
- **A small machine.** 2 vCPU. quickhouse's stage and dlt's normalize step are
  both CPU-bound here, and both would likely be faster on more cores.
- **Destinations were not interleaved.** All ClickHouse runs happened before
  all BigQuery runs (a harness bug discarded the first BigQuery attempt).
  Within each destination the tool order rotated every round.
- **MySQL only for the three-way comparison.** ADBC has no MySQL driver
  (Postgres, BigQuery, Snowflake, SQLite, and Flight SQL only as of this
  writing), so the extract-ceiling measurement used PostgreSQL instead, read
  in chunks for the reason given above.
- **Only 3 runs per configuration.** Enough to see clear separation between
  tools, not enough to characterize tail latency. quickhouse's BigQuery runs
  varied 89.7–110.8 s, almost all of it in the streaming stage.
- **This measures throughput and destination cost only**, not overall
  capability. dlt in particular offers schema evolution, a large built-in
  connector catalog, and incremental-state management that quickhouse does
  not — this benchmark says nothing about which tool fits a given team's
  broader needs.

## Gotchas and integration notes

Recorded here in case they save someone else the debugging time:

- **Sling** speaks ClickHouse's **native protocol**, not HTTP — use
  `port: 9440, secure: true`. Pointing it at the HTTPS port (8443) fails with
  a bare "connection reset by peer" and no further explanation.
- **Sling**'s `--mode incremental` combined with custom SQL and
  `--update-key` requires the SQL to contain an `{incremental_where_cond}` or
  `{incremental_value}` placeholder — using it without one (as you would for
  a fixed test window) fails outright. `--primary-key` alone works for a
  pk-merge without a watermark.
- **Sling**'s `--src-stream` must be the raw SQL text; a `file://`-prefixed
  value is interpreted as a filesystem source, not a query.
- **dlt** defaults its BigQuery destination to the US multi-region and will
  404 against a dataset in any other location unless you pass
  `dlt.destinations.bigquery(location=...)` explicitly.
- **dlt**'s `resource.with_name(...)` *returns* a renamed copy — it does not
  rename in place. Discarding the return value silently leaves the pipeline
  targeting the *source* table's name instead of the intended destination
  table.
- **dlt**'s `connectorx` backend maps MySQL `DECIMAL(15,3)` to `FLOAT`,
  which is a silent precision downgrade on money-shaped columns — worth
  checking explicitly if you use that backend for financial data.
- **dlt** keeps every loaded package's files on local disk by default — about
  half a gigabyte per 10M-row run here. Clear the pipeline's `load/loaded`
  directory between runs on a small disk.
- **quickhouse**'s `mode="incremental"` needs a `watermark=` even when `key=`
  drives the merge. To re-merge the same fixed window on every run, as a
  benchmark does, pass `advance_watermark=False` (or a fresh `state_key` per
  run); otherwise the second run reads nothing.
- **A PostgreSQL hot standby** cancels a long read whose snapshot outlives
  `max_standby_streaming_delay` (30 s on this replica). A 10M-row read in one
  query does not survive it; read in keyset chunks, each its own query
  (quickhouse's `chunk_rows`).
- **Memory profiling across languages**: a Python-level profiler (e.g.
  `memray`) only sees Python heap activity, so it will make a Rust-backed
  tool look artificially lean. Measure whole-process-tree RSS instead if
  comparing tools implemented in different languages.

## Previous edition (300k rows)

The 2026-07-31 edition ran the same comparison on a ~300k-row window
(`user_order`, 299,540 rows) on a 4 vCPU VM, with Sling 1.5.22 and dlt 1.29.1:

| Tool | → BigQuery | → ClickHouse | Bytes billed / run |
|---|---:|---:|---:|
| **quickhouse** | 6.3 – 8.3 s | **0.8 – 1.0 s** | ~28 MiB |
| Sling | 30 – 34 s | 10.6 – 10.7 s | ~200 MiB |
| dlt | 49.6 – 57.1 s | 47.8 – 50.6 s | ~122 MiB |

At that size quickhouse was 4–7× faster than Sling and 6–7× faster than dlt
on BigQuery, 11–13× and ~55× on ClickHouse, and billed 4–7× fewer bytes. Its
BigQuery time was 70–85% `MERGE`, measured the same way as above.

## Reproducing this benchmark

The scripts used to produce every number on this page — source-window
selection, each tool's sync script, the process-tree RSS wrapper, the ADBC
extract-only measurement, and the verification and billing queries — are
available at *[link to be added — scripts currently living outside the
published repo]*. Every table is checked against row count, distinct
primary-key count, and a value checksum computed on the source before its
numbers are reported, and all benchmark tables are dropped from the
destinations after each session.
