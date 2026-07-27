---
sd_hide_title: true
---

# quickhouse

```{raw} html
<div class="qh-hero">
  <div>
    <p class="qh-eyebrow">Python API · Rust engine</p>
    <h1 class="qh-title">Move whole tables<br />in one function call.</h1>
    <p class="qh-lede">PostgreSQL, MySQL, or BigQuery into ClickHouse or BigQuery. Native wire protocols straight into Apache Arrow — no per-row Python, flat memory.</p>

    <div class="qh-install">
      <code>pip install quickhouse</code>
      <button type="button" data-clipboard="pip install quickhouse">copy</button>
    </div>
    <div class="qh-chips">
      <span>wheels, no toolchain</span>
      <span>py 3.9 – 3.13</span>
      <span>MIT</span>
    </div>
    <div class="qh-cta">
      <a href="quickstart.html">Quickstart →</a>
      <a class="secondary" href="api.html">API reference</a>
    </div>
  </div>

  <div class="qh-panel">
    <div class="qh-panel__tabs">
      <nav>
        <span data-tab="full" aria-selected="true">full refresh</span>
        <span data-tab="incremental">incremental</span>
        <span data-tab="cli">CLI</span>
      </nav>
      <span>copy</span>
    </div>
    <div class="qh-tabpanel" data-panel="full">
<pre><span class="k">import</span> quickhouse

src = quickhouse.<span class="nc">Postgres</span>(<span class="s">"postgresql://user:pw@host:5432/shop"</span>)
dst = quickhouse.<span class="nc">ClickHouse</span>(<span class="s">"http://localhost:8123"</span>,
                             database=<span class="s">"analytics"</span>)

result = quickhouse.<span class="nc">sync</span>(src, dst,
    dest_table=<span class="s">"orders"</span>, source_table=<span class="s">"orders"</span>,
    key=[<span class="s">"id"</span>])</pre>
      <div class="qh-result">
        <div>→ TransferResult</div>
        <div>rows_read=<b>1_000_000</b>  rows_written=<b>1_000_000</b></div>
        <div>duration_secs=<em>4.31</em>  new_watermark=None</div>
      </div>
    </div>
    <div class="qh-tabpanel" data-panel="incremental" hidden>
<pre><span class="k">import</span> quickhouse

src = quickhouse.<span class="nc">Postgres</span>(<span class="s">"postgresql://user:pw@host:5432/shop"</span>)
dst = quickhouse.<span class="nc">ClickHouse</span>(<span class="s">"http://localhost:8123"</span>, database=<span class="s">"analytics"</span>)

result = quickhouse.<span class="nc">sync</span>(src, dst,
    dest_table=<span class="s">"orders"</span>, source_table=<span class="s">"orders"</span>,
    mode=<span class="s">"incremental"</span>, watermark=<span class="s">"updated_at"</span>, key=[<span class="s">"id"</span>])</pre>
      <div class="qh-result">
        <div>→ TransferResult <span class="c"># only new rows — re-running is a no-op</span></div>
        <div>rows_read=<b>12_480</b>  rows_written=<b>12_480</b></div>
        <div>duration_secs=<em>0.19</em>  new_watermark=<b>"2026-07-27T14:30:00Z"</b></div>
      </div>
    </div>
    <div class="qh-tabpanel" data-panel="cli" hidden>
<pre><span class="c"># job.toml — drive a sync from cron/CI, no Python</span>
[source]
type = <span class="s">"postgres"</span>
dsn  = <span class="s">"${PG_DSN}"</span>

[target]
type     = <span class="s">"clickhouse"</span>
url      = <span class="s">"http://localhost:8123"</span>
database = <span class="s">"analytics"</span>

[sync]
dest_table   = <span class="s">"orders"</span>
source_table = <span class="s">"orders"</span>
key          = [<span class="s">"id"</span>]</pre>
      <div class="qh-result">
        <div>$ quickhouse run job.toml</div>
        <div>→ TransferResult rows_written=<b>1_000_000</b>  duration_secs=<em>4.31</em></div>
      </div>
    </div>
  </div>
</div>

<div class="qh-cards">
  <a href="installation.html"><strong>Installation</strong><span>Prebuilt wheels, no Rust toolchain needed.</span></a>
  <a href="quickstart.html"><strong>Quickstart</strong><span>Your first sync in a dozen lines, full and incremental.</span></a>
  <a href="guide/sources.html"><strong>User guide</strong><span>Sources, sync modes, type mapping, performance, safety.</span></a>
  <a href="api.html"><strong>API reference</strong><span>Every <code>sync()</code> argument and descriptor.</span></a>
</div>

<div class="qh-split">
  <div>
    <h2>Supported pairs</h2>
    <table class="qh-matrix">
      <thead><tr><th>Source</th><th>ClickHouse</th><th>BigQuery</th></tr></thead>
      <tbody>
        <tr><td>PostgreSQL</td><td class="yes">✓</td><td class="yes">✓</td></tr>
        <tr><td>MySQL</td><td class="yes">✓</td><td class="yes">✓</td></tr>
        <tr><td>BigQuery</td><td class="yes">✓</td><td class="yes">✓</td></tr>
        <tr><td>CleverTap</td><td class="yes">✓</td><td class="yes">✓</td></tr>
        <tr><td>AppsFlyer</td><td class="yes">✓</td><td class="yes">✓</td></tr>
      </tbody>
    </table>
  </div>
  <div class="qh-why">
    <h2>Why quickhouse</h2>
    <p><strong>It's fast.</strong> Rows decode straight off the wire into Arrow, in Rust. Ranges read in parallel; decode overlaps upload.</p>
    <p><strong>It's safe with messy data.</strong> Atomic swaps, idempotent incrementals, automatic retries on transient blips.</p>
    <p><strong>Nothing to stand up.</strong> An ordinary Python dependency — no JVM, no Spark, no service.</p>
  </div>
</div>
```

```{admonition} Status: pre-1.0
:class: warning
quickhouse is used against real production data and is covered by an
integration test suite, but the Python API may still change between minor
versions before 1.0. Pin a compatible range (e.g. `quickhouse~=0.12`) and watch
the [changelog](changelog.md). A few knobs are marked *experimental* — those may
change without a major bump.
```

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
