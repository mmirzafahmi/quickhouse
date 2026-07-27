---
sd_hide_title: true
---

# quickhouse

<div class="qh-hero">
<div class="qh-hero-grid">
<div>
<p class="qh-eyebrow">Python API · Rust engine</p>
<h1 class="qh-headline">Move whole tables<br>in one function call.</h1>
<p class="qh-lede">PostgreSQL, MySQL, or BigQuery into ClickHouse or BigQuery. Native wire protocols straight into Apache Arrow — no per-row Python, flat memory.</p>
<div class="qh-install"><span class="cmd"><b>$</b> pip install quickhouse</span><span class="copy">copy</span></div>
<div class="qh-chips"><span class="qh-chip">wheels, no toolchain</span><span class="qh-chip">py 3.9 – 3.13</span><span class="qh-chip">MIT</span></div>
<div class="qh-cta"><a class="qh-btn" href="quickstart.html">Quickstart →</a><a class="qh-btn-ghost" href="api.html">API reference</a></div>
</div>
<div class="qh-panel">
<div class="qh-panel-head"><span class="tab on">full refresh</span><span class="tab">incremental</span><span class="tab">CLI</span></div>
<pre><span class="kw">import</span> quickhouse

src = quickhouse.<span class="fn">Postgres</span>(<span class="str">"postgresql://user:pw@host:5432/shop"</span>)
dst = quickhouse.<span class="fn">ClickHouse</span>(<span class="str">"http://localhost:8123"</span>,
                             database=<span class="str">"analytics"</span>)

result = quickhouse.<span class="fn">sync</span>(src, dst,
    dest_table=<span class="str">"orders"</span>, source_table=<span class="str">"orders"</span>,
    key=[<span class="str">"id"</span>])</pre>
<div class="qh-output"><div class="em">→ TransferResult</div><div>rows_read=<span class="val">1_000_000</span>&nbsp;&nbsp;rows_written=<span class="val">1_000_000</span></div><div>duration_secs=<span class="em">4.31</span>&nbsp;&nbsp;new_watermark=None</div></div>
</div>
</div>
</div>

```{admonition} Status: pre-1.0
:class: warning
quickhouse is used against real production data and is covered by an
integration test suite, but the Python API may still change between minor
versions before 1.0. Pin a compatible range (e.g. `quickhouse~=0.12`) and watch
the [changelog](changelog.md). A few knobs are marked *experimental* — those may
change without a major bump.
```

<div class="qh-features">
<div class="qh-feature"><div class="t"><a href="installation.html">Installation</a></div><div class="d">Prebuilt wheels, no Rust toolchain needed.</div></div>
<div class="qh-feature"><div class="t"><a href="quickstart.html">Quickstart</a></div><div class="d">Your first sync in a dozen lines, full and incremental.</div></div>
<div class="qh-feature"><div class="t"><a href="guide/sources.html">User guide</a></div><div class="d">Sources, sync modes, type mapping, performance, safety.</div></div>
<div class="qh-feature"><div class="t"><a href="api.html">API reference</a></div><div class="d">Every <code>sync()</code> argument and descriptor.</div></div>
</div>

## Supported sources and destinations

| Source | → ClickHouse | → BigQuery |
|---|:--:|:--:|
| PostgreSQL | ● | ● |
| MySQL | ● | ● |
| BigQuery | ● | ● |
| CleverTap (HTTP API) | ● | ● |
| AppsFlyer (HTTP API) | ● | ● |

ClickHouse is a destination only; BigQuery is both a source and a destination.

## Why quickhouse

<ul class="qh-why">
<li><b>It's fast.</b> Rows decode straight off the wire into Arrow, in Rust — no per-row Python, no intermediate DataFrame. Ranges read in parallel; decode overlaps upload. Hundreds of thousands of rows/sec with peak memory flat under ~180&nbsp;MB.</li>
<li><b>It's one function call.</b> <a href="api.html#quickhouse.sync"><code>sync()</code></a> replaces the cursor loop, manual batching, retry logic, and <code>CREATE TABLE</code> you'd otherwise write by hand.</li>
<li><b>It's safe with messy data.</b> Atomic full-refresh swaps, idempotent incrementals, automatic retries on transient blips; MySQL zero-dates coerce to <code>NULL</code> with a warning instead of aborting.</li>
<li><b>It's gentle on a small database.</b> <code>read_max_rows_per_sec</code> paces the read, and the scan itself backs off since streaming results only produce as fast as the client consumes.</li>
<li><b>Nothing to stand up.</b> An ordinary Python dependency — no JVM, no Spark cluster, no separate service.</li>
</ul>

## When to use quickhouse

Reach for it when you want to move whole tables — full refresh or incremental —
from PostgreSQL/MySQL/BigQuery into ClickHouse or BigQuery, fast, from your own
Python jobs with almost no setup. It fits cron/Airflow/Dagster tasks and one-off
backfills well.

Look elsewhere when you need in-warehouse SQL transformations (use **dbt**),
change-data-capture or streaming (use **Debezium/Kafka**), a large catalog of
SaaS connectors (use **Airbyte / Fivetran / dlt**), or arbitrary
source↔destination pairs — quickhouse deliberately supports a focused, fast set.

```{toctree}
:hidden:
:caption: Getting started

installation
quickstart
```

```{toctree}
:hidden:
:caption: User guide

guide/sources
guide/sync-modes
guide/type-mapping
guide/performance
cli
examples
```

```{toctree}
:hidden:
:caption: Reference

api
changelog
contributing
```

```{toctree}
:hidden:
:caption: Links

GitHub repository <https://github.com/mmirzafahmi/quickhouse>
PyPI package <https://pypi.org/project/quickhouse/>
```
