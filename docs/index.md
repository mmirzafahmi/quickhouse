---
sd_hide_title: true
---

# quickhouse

```{raw} html
<div class="qh-hero">
  <div class="qh-hero__top">
    <div class="qh-hero__pitch">
      <p class="qh-hero__eyebrow">Python API · Rust engine</p>
      <h1>Move whole tables in <em>one</em> function call.</h1>
      <p class="qh-hero__sub">PostgreSQL, MySQL, or BigQuery into ClickHouse or BigQuery. Native wire
      protocols straight into Apache Arrow — no per-row Python, flat memory.</p>
    </div>
    <div class="qh-hero__actions">
      <div class="qh-install">
        <code><span class="qh-prompt">$</span> pip install quickhouse</code>
        <button type="button" class="qh-copy" data-qh-copy="pip install quickhouse">copy</button>
      </div>
      <div class="qh-btns">
        <a class="qh-btn qh-btn--solid" href="quickstart.html">Quickstart</a>
        <a class="qh-btn qh-btn--ghost" href="api.html">API reference</a>
      </div>
      <div class="qh-hero__meta">
        <span>wheels, no toolchain</span><span aria-hidden="true">·</span>
        <span>py 3.9–3.13</span><span aria-hidden="true">·</span>
        <span>MIT</span>
      </div>
    </div>
  </div>

  <div class="qh-slab">
    <div class="qh-slab__bar">
      <div class="qh-slab__tabs" role="tablist">
        <button type="button" class="qh-slab__tab" role="tab" aria-selected="true">full refresh</button>
        <button type="button" class="qh-slab__tab" role="tab" aria-selected="false">incremental</button>
        <button type="button" class="qh-slab__tab" role="tab" aria-selected="false">CLI</button>
      </div>
      <button type="button" class="qh-copy" data-qh-copy="import quickhouse as qh">copy</button>
    </div>
    <div class="qh-slab__body">
      <div>
<div class="qh-slab__panel" data-active="1" role="tabpanel"><pre><span class="k">import</span> quickhouse <span class="k">as</span> qh

src = qh.<span class="n">Postgres</span>(<span class="s">"postgresql://user:pw@host:5432/shop"</span>)
dst = qh.<span class="n">ClickHouse</span>(<span class="s">"http://localhost:8123"</span>, database=<span class="s">"analytics"</span>)

qh.<span class="n">sync</span>(
    src, dst,
    source_table=<span class="s">"public.orders"</span>,
    dest_table=<span class="s">"orders"</span>, key=[<span class="s">"id"</span>]
)</pre></div>
<div class="qh-slab__panel" role="tabpanel"><pre><span class="k">import</span> quickhouse <span class="k">as</span> qh

qh.<span class="n">sync</span>(
    src, dst,
    source_table=<span class="s">"public.events"</span>,
    dest_table=<span class="s">"events"</span>,
    mode=<span class="s">"incremental"</span>,
    watermark=<span class="s">"updated_at"</span>, key=[<span class="s">"id"</span>]
)</pre></div>
<div class="qh-slab__panel" role="tabpanel"><pre><span class="k">$</span> quickhouse run job.toml

<span class="k">#</span> job.toml drives the same sync from cron or CI,
<span class="k">#</span> no Python — see the CLI reference.</pre></div>
      </div>
      <div class="qh-result">
        <div class="qh-result__label">TransferResult</div>
        <div class="qh-result__row"><span>rows_read</span><b>1_000_000</b></div>
        <div class="qh-result__row"><span>rows_written</span><b>1_000_000</b></div>
        <div class="qh-result__row"><span>duration_secs</span><b class="qh-hi">4.31</b></div>
        <div class="qh-result__row"><span>new_watermark</span><b>None</b></div>
      </div>
    </div>
  </div>

  <dl class="qh-stats">
    <div><dt>4.3s</dt><dd>1M rows, Postgres → ClickHouse, single worker.</dd></div>
    <div><dt>Arrow</dt><dd>Zero-copy batches end to end. Memory stays flat.</dd></div>
    <div><dt>5</dt><dd>Sources, two destinations, one uniform API.</dd></div>
    <div><dt>0</dt><dd>Toolchains to install. Prebuilt wheels, MIT licensed.</dd></div>
  </dl>


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

```{raw} html
<nav class="qh-index" aria-label="Documentation index">
  <div>
    <h2>Getting started</h2>
    <ul>
      <li><a href="installation.html">Installation</a></li>
      <li><a href="quickstart.html">Quickstart</a></li>
    </ul>
  </div>
  <div>
    <h2>User guide</h2>
    <ul>
      <li><a href="guide/sources.html">Sources &amp; destinations</a></li>
      <li><a href="guide/sync-modes.html">Sync modes</a></li>
      <li><a href="guide/type-mapping.html">Type mapping</a></li>
      <li><a href="guide/performance.html">Performance &amp; safety</a></li>
      <li><a href="cli.html">Command-line interface</a></li>
      <li><a href="examples.html">Examples</a></li>
    </ul>
  </div>
  <div>
    <h2>Reference</h2>
    <ul>
      <li><a href="api.html">API reference</a></li>
      <li><a href="changelog.html">Changelog</a></li>
      <li><a href="contributing.html">Contributing to quickhouse</a></li>
    </ul>
  </div>
  <div>
    <h2>Links</h2>
    <ul>
      <li><a href="https://github.com/mmirzafahmi/quickhouse">GitHub repository<span class="qh-ext">&#8599;</span></a></li>
      <li><a href="https://pypi.org/project/quickhouse/">PyPI package<span class="qh-ext">&#8599;</span></a></li>
    </ul>
  </div>
</nav>
```

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
