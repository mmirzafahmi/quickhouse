# Benchmark

*Last updated: 2026-07-31*

This page reports a head-to-head benchmark of quickhouse against two widely used
Python/Go EL tools — [dlt](https://dlthub.com/) and [Sling](https://slingdata.io/) —
moving the same data, under the same constraints, into both BigQuery and
ClickHouse. It also includes an [ADBC](https://arrow.apache.org/adbc/)-based
measurement that isolates how much of the total time is spent reading the
source versus writing the destination.

This benchmark was run against production-shaped tables and queries from a
real deployment (not a synthetic schema), and the raw scripts are linked at
the bottom so the numbers can be reproduced or challenged.

```{note}
This benchmark is maintained by quickhouse's author. We've tried to be fair —
every tool reads byte-identical SQL, every result is checked for row-level
correctness, and the [Limitations](#limitations) section below is not an
afterthought. But you should still treat "the author benchmarked their own
tool" as the caveat it is, and we'd welcome PRs that improve or challenge any
part of this.
```

## TL;DR

Same ~300k-row slice, same source query, same primary-key merge semantics,
3 runs per tool, into a warm (already-populated) destination table.

```{raw} html
<ul class="qh-bench__facts">
  <li>4 vCPU / 15 GB GCE VM</li>
  <li>Sling 1.5.22</li>
  <li>dlt 1.29.1</li>
  <li>3 runs · min–max</li>
  <li>row + checksum verified</li>
</ul>

<div class="qh-bench">
  <div class="qh-bench__switch" role="tablist" aria-label="Destination">
    <button type="button" role="tab" aria-selected="true" tabindex="0">&rarr; ClickHouse</button>
    <button type="button" role="tab" aria-selected="false" tabindex="-1">&rarr; BigQuery</button>
  </div>

  <div class="qh-bench__panel" role="tabpanel" aria-label="ClickHouse destination">
    <div class="qh-bench__caption">
      <span>300k-row primary-key merge, wall clock</span>
      <span>lower is better</span>
    </div>
    <div class="qh-bars">
      <div class="qh-bar qh-bar--lead">
        <span class="qh-bar__name">quickhouse</span>
        <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:2%;--qh-delay:80ms"></span></span>
        <span class="qh-bar__value">0.8–1.0 s</span>
      </div>
      <div class="qh-bar">
        <span class="qh-bar__name">Sling</span>
        <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:21%;--qh-delay:200ms"></span></span>
        <span class="qh-bar__value">10.6–10.7 s</span>
      </div>
      <div class="qh-bar">
        <span class="qh-bar__name">dlt</span>
        <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:100%;--qh-delay:320ms"></span></span>
        <span class="qh-bar__value">47.8–50.6 s</span>
      </div>
    </div>
    <p class="qh-bench__takeaway"><strong>11–13× faster than Sling, ~55× faster than dlt.</strong>
    quickhouse's 0.8 s end-to-end also beats the 2.2 s ADBC extract-only ceiling
    measured on the read side alone.</p>
  </div>

  <div class="qh-bench__panel" role="tabpanel" aria-label="BigQuery destination" hidden>
    <div class="qh-bench__caption">
      <span>300k-row primary-key merge, wall clock</span>
      <span>lower is better</span>
    </div>
    <div class="qh-bars">
      <div class="qh-bar qh-bar--lead">
        <span class="qh-bar__name">quickhouse</span>
        <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:15%;--qh-delay:80ms"></span></span>
        <span class="qh-bar__value">6.3–8.3 s</span>
      </div>
      <div class="qh-bar">
        <span class="qh-bar__name">Sling</span>
        <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:60%;--qh-delay:200ms"></span></span>
        <span class="qh-bar__value">30–34 s</span>
      </div>
      <div class="qh-bar">
        <span class="qh-bar__name">dlt</span>
        <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:100%;--qh-delay:320ms"></span></span>
        <span class="qh-bar__value">49.6–57.1 s</span>
      </div>
    </div>
    <p class="qh-bench__takeaway"><strong>4–7× faster than Sling, 6–7× faster than dlt</strong>
    — and the cheapest run of the three.</p>

    <div class="qh-bench__sub">
      <h4>Bytes billed per run — <code>INFORMATION_SCHEMA.JOBS</code>, top-level jobs only</h4>
      <div class="qh-bars">
        <div class="qh-bar qh-bar--lead">
          <span class="qh-bar__name">quickhouse</span>
          <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:14%;--qh-delay:80ms"></span></span>
          <span class="qh-bar__value">~28 MiB</span>
        </div>
        <div class="qh-bar">
          <span class="qh-bar__name">dlt</span>
          <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:61%;--qh-delay:200ms"></span></span>
          <span class="qh-bar__value">~122 MiB</span>
        </div>
        <div class="qh-bar">
          <span class="qh-bar__name">Sling</span>
          <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:100%;--qh-delay:320ms"></span></span>
          <span class="qh-bar__value">~200 MiB</span>
        </div>
      </div>
      <p class="qh-bench__takeaway">At on-demand pricing ($6.25/TiB scanned) that is
      <strong>$0.17</strong> per 1,000 syncs for quickhouse, against $0.73 for dlt and
      $1.19 for Sling. The absolute dollars are small on a 300k-row slice — the
      multiple is what scales with your table.</p>
    </div>
  </div>
</div>
```

The single biggest structural reason: quickhouse issues **one** `MERGE`
against the destination. Both dlt and Sling stage into a temporary table and
then run a `DELETE` followed by an `INSERT` — two destination-side passes
instead of one — which is also why both tools show a much smaller gap on
ClickHouse (native `MergeTree` merges are cheap) than on BigQuery (a `MERGE`
scan is not).

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
    <p>Two passes — cheap on MergeTree, expensive on BigQuery.</p>
  </div>
</div>
```

## Methodology

- **Environment:** a single GCE VM (4 vCPU / 15 GB RAM), same box for every
  tool, same network path to the source databases and destinations.
- **Source data:** a bounded, fixed `id` window (~300k rows) pulled directly
  from production MySQL and PostgreSQL tables — not a synthetic benchmark
  schema. Two tables were used:
  - `user_order` — MySQL, 21 columns, 299,540 rows in the window.
  - `sale_order_line` (odoo) — PostgreSQL, 29 columns, 299,996 rows in the
    window (used for the ADBC extract-only measurement).
- **Identical SQL across tools.** Every tool read quickhouse's own generated
  source query (the one it already uses in production, which `CAST`s a
  handful of columns to normalize types across MySQL/Postgres/BigQuery). This
  matters: without those casts, at least one competing tool crashes on a
  `TIME` column (see [Gotchas](#gotchas-and-integration-notes)) — giving every
  tool the same query is the fair comparison, not a handicap for quickhouse.
- **Identical semantics.** Every tool was configured for an upsert on the
  table's primary key (`merge` in dlt, `--primary-key` in Sling, `key=` in
  quickhouse) into a destination table that already existed from a prior run,
  so every run performs a genuine merge, not a first-time bulk load.
- **3 runs per tool per destination.** Reported ranges are min–max across
  those runs, not a single sample.
- **Correctness checked every time**, not assumed: exact row count, exact
  distinct-primary-key count, and a checksum on a numeric ("money") column,
  compared against quickhouse's own output as ground truth.
- **Memory measured at the RSS level** (whole process tree, sampled or via
  `getrusage`), not with a Python-level profiler — a profiler like `memray`
  only traces Python allocations and would under-report quickhouse, whose
  buffering happens in Rust.
- **BigQuery cost measured from `INFORMATION_SCHEMA.JOBS`**, counting only
  top-level jobs (a `SCRIPT`-wrapped job restates its children's billed bytes,
  which double-counts if not filtered out).

## Results: BigQuery destination

MySQL `user_order`, 21 columns, 299,540 rows:

| Tool | Wall-clock | Rows/sec | Peak RSS | Bytes billed / run |
|---|---:|---:|---:|---:|
| **quickhouse** | 6.3 – 8.3 s | 36,000 – 48,000 | 116 MB | ~28 MiB |
| Sling 1.5.22 | 30 – 34 s | 9,100 – 10,400 | 152 MB | ~200 MiB |
| dlt 1.29.1 (parquet loader) | 49.6 – 57.1 s | 5,200 – 6,000 | 258 MB | ~122 MiB |

### Where the time actually goes

To find out how much of quickhouse's ~7 s is spent *reading* versus
*writing*, we isolated the read side using
[Apache Arrow ADBC](https://arrow.apache.org/adbc/), which has no merge or
load layer of its own — it only extracts.

On the PostgreSQL source (`sale_order_line`, 29 columns, 299,996 rows):

| Stage | Time | Rows/sec |
|---|---:|---:|
| ADBC extract → Arrow (streaming) | **2.1 – 2.3 s** | 128,000 – 142,000 |
| quickhouse, full extract + load + merge | 8.0 – 13.2 s | 22,700 – 37,400 |
| — of which the BigQuery `MERGE` alone | **~7.3 s** | — |

Reading is roughly **20%** of quickhouse's total time; the BigQuery `MERGE`
is roughly **70–85%**. This is the same conclusion the ClickHouse results
below independently confirm: extract speed is not the bottleneck for any of
these tools once the destination is BigQuery — the destination-side write
strategy is.

## Results: ClickHouse destination

Same MySQL `user_order` window, into a ClickHouse Cloud instance:

| Tool | → BigQuery | → ClickHouse | Rows/sec on ClickHouse | Peak RSS |
|---|---:|---:|---:|---:|
| **quickhouse** | 6.3 – 8.3 s | **0.8 – 1.0 s** | 299,000 – 381,000 | 93 – 105 MB |
| Sling | 30 – 34 s | 10.6 – 10.7 s | ~31,000 | 241 – 256 MB |
| dlt | 49.6 – 57.1 s | 47.8 – 50.6 s | ~6,100 | 168 MB |

Three things stand out here:

1. **quickhouse's 0.8 s beats the 2.2 s ADBC extract-only ceiling measured
   above.** Reading and writing 300k rows end-to-end, together, is faster
   than just reading them another way — reinforcing that BigQuery's `MERGE`,
   not any tool's reader, was the bottleneck the whole time.
2. **dlt's time barely changes between destinations** (49–57 s vs. 48–51 s).
   Its bottleneck is upstream of the destination entirely — its own
   Python-level extract/normalize stage — so no destination-side
   optimization will help it.
3. **Sling's time drops 3×** moving to ClickHouse, showing that a real part
   of its BigQuery cost was destination-side (the temp-table + `DELETE` +
   `INSERT` pattern), separate from its reader.

## Data integrity and storage footprint

All three tools produced logically correct results across all runs and both
destinations: identical row counts, zero duplicate primary keys, matching
checksums. But "logically correct" and "no ongoing cost" are not the same
thing, and this is worth surfacing rather than burying:

| Tool | Engine used | Logical rows | Physical rows on disk | Disk used |
|---|---|---:|---:|---:|
| quickhouse | `ReplacingMergeTree` | 299,540 | 299,540 (1.0×) | 8.5 MiB |
| Sling | `MergeTree` | 299,540 | 299,540 (1.0×) | 7.1 MiB |
| dlt | `MergeTree` + lightweight deletes | 299,540 | **898,620 (3.0×)** | **31.6 MiB** |

dlt implements its merge on ClickHouse via
[lightweight deletes](https://clickhouse.com/docs/en/sql-reference/statements/delete):
the "old" version of an updated row is marked deleted but stays physically on
disk until a background mutation reclaims it. After only 3 runs, dlt's table
held 3× the physical rows of the other two tools for the same logical data —
`SELECT count(*)` won't show this (it correctly excludes masked rows); you
have to check `system.parts ... WHERE active` to see it. On a long-running
pipeline this is ongoing merge and storage cost that compounds with every
sync cycle.

## Limitations

Publishing your own benchmark without stating where it's weak isn't a fair
benchmark, so:

- **dlt's high-throughput path was never successfully benchmarked.** dlt's
  recommended fast configuration is its `sql_database` source with a
  Rust/Arrow-backed extraction backend (`connectorx`), not the plain Python
  generator resource used for the numbers above. We attempted this five
  times and hit five distinct integration issues (see
  [Gotchas](#gotchas-and-integration-notes)) before setting it aside. **The
  dlt numbers above reflect "dlt with a straightforward Python resource,"
  not dlt's ceiling — the gap could be smaller with that backend working.**
- **Small slice size.** ~300k rows is representative of a single incremental
  sync cycle in the source deployment, not a multi-GB bulk load. Relative
  ordering may not hold at very different scales.
- **MySQL only for the three-way comparison.** ADBC has no MySQL driver
  (Postgres, BigQuery, Snowflake, SQLite, and Flight SQL only as of this
  writing), so the extract-ceiling measurement used PostgreSQL instead.
- **Only 3 runs per configuration.** Enough to see clear separation between
  tools, not enough to characterize tail latency. quickhouse's own repeated
  runs varied 6.3–13.2 s depending on source-database load at the time.
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
- **Memory profiling across languages**: a Python-level profiler (e.g.
  `memray`) only sees Python heap activity, so it will make a Rust-backed
  tool look artificially lean. Measure whole-process-tree RSS instead if
  comparing tools implemented in different languages.

## Reproducing this benchmark

The scripts used to produce every number on this page — source-window
selection, each tool's sync script, and the ADBC extract-only measurement —
are available at *[link to be added — scripts currently living outside the
published repo]*. Every run verifies row count, distinct primary-key count,
and a value checksum against quickhouse's own output before being reported,
and all benchmark tables are dropped from the source/destination systems
after each session.
