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
        <button type="button" class="qh-copy" data-qh-copy="pip install quickhouse" aria-live="polite">copy</button>
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
      <div class="qh-slab__tabs" role="tablist" aria-label="Sync example">
        <button type="button" class="qh-slab__tab" role="tab" id="qh-tab-full" aria-selected="true" aria-controls="qh-panel-full" tabindex="0">full refresh</button>
        <button type="button" class="qh-slab__tab" role="tab" id="qh-tab-incremental" aria-selected="false" aria-controls="qh-panel-incremental" tabindex="-1">incremental</button>
        <button type="button" class="qh-slab__tab" role="tab" id="qh-tab-cli" aria-selected="false" aria-controls="qh-panel-cli" tabindex="-1">CLI</button>
      </div>
      <button type="button" class="qh-copy" data-qh-copy="import quickhouse as qh" aria-live="polite">copy</button>
    </div>
    <div class="qh-slab__body">
      <div>
<div class="qh-slab__panel" data-active="1" role="tabpanel" id="qh-panel-full" aria-labelledby="qh-tab-full"><pre><span class="k">import</span> quickhouse <span class="k">as</span> qh

src = qh.<span class="n">Postgres</span>(<span class="s">"postgresql://user:pw@host:5432/shop"</span>)
dst = qh.<span class="n">ClickHouse</span>(<span class="s">"http://localhost:8123"</span>, database=<span class="s">"analytics"</span>)

qh.<span class="n">sync</span>(
    src, dst,
    source_table=<span class="s">"public.orders"</span>,
    dest_table=<span class="s">"orders"</span>, key=[<span class="s">"id"</span>]
)</pre></div>
<div class="qh-slab__panel" role="tabpanel" id="qh-panel-incremental" aria-labelledby="qh-tab-incremental"><pre><span class="k">import</span> quickhouse <span class="k">as</span> qh

qh.<span class="n">sync</span>(
    src, dst,
    source_table=<span class="s">"public.events"</span>,
    dest_table=<span class="s">"events"</span>,
    mode=<span class="s">"incremental"</span>,
    watermark=<span class="s">"updated_at"</span>, key=[<span class="s">"id"</span>]
)</pre></div>
<div class="qh-slab__panel" role="tabpanel" id="qh-panel-cli" aria-labelledby="qh-tab-cli"><pre><span class="k">$</span> quickhouse run job.toml

<span class="k">#</span> job.toml drives the same sync from cron or CI,
<span class="k">#</span> no Python — see the CLI reference.</pre></div>
      </div>
      <div class="qh-result">
        <div class="qh-result__label">TransferResult</div>
        <div class="qh-result__row"><span>rows_read</span><b>299_540</b></div>
        <div class="qh-result__row"><span>rows_written</span><b>299_540</b></div>
        <div class="qh-result__row"><span>duration_secs</span><b class="qh-hi">0.94</b></div>
        <div class="qh-result__row"><span>new_watermark</span><b>None</b></div>
      </div>
    </div>
  </div>

  <dl class="qh-stats">
    <div><dt>0.9s</dt><dd>300k-row merge, MySQL → ClickHouse. <a href="guide/benchmark.html">See the benchmark</a>.</dd></div>
    <div><dt>Arrow</dt><dd>Zero-copy batches end to end. Memory stays flat.</dd></div>
    <div><dt>5</dt><dd>Sources, two destinations, one uniform API.</dd></div>
    <div><dt>0</dt><dd>Toolchains to install. Prebuilt wheels, MIT licensed.</dd></div>
  </dl>

  <div class="qh-split">
    <div class="qh-teaser">
      <div class="qh-teaser__head">
        <span class="qh-teaser__title">300k-row merge → ClickHouse, wall clock</span>
        <a href="guide/benchmark.html">benchmark &rarr;</a>
      </div>

      <div class="qh-teaser__slide"
           data-title="300k-row merge → ClickHouse, wall clock"
           data-note="lower is better · 3 runs">
        <div class="qh-bars">
          <div class="qh-bar qh-bar--lead">
            <span class="qh-bar__name">quickhouse</span>
            <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:2%;--qh-delay:80ms"></span></span>
            <span class="qh-bar__value">0.9 s</span>
          </div>
          <div class="qh-bar">
            <span class="qh-bar__name">Sling</span>
            <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:21%;--qh-delay:200ms"></span></span>
            <span class="qh-bar__value">10.7 s</span>
          </div>
          <div class="qh-bar">
            <span class="qh-bar__name">dlt</span>
            <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:100%;--qh-delay:320ms"></span></span>
            <span class="qh-bar__value">50.6 s</span>
          </div>
        </div>
      </div>

      <div class="qh-teaser__slide" hidden
           data-title="300k-row merge → BigQuery, cost per 1,000 syncs"
           data-note="lower is better · on-demand at $6.25/TiB">
        <div class="qh-bars">
          <div class="qh-bar qh-bar--lead">
            <span class="qh-bar__name">quickhouse</span>
            <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:14%;--qh-delay:80ms"></span></span>
            <span class="qh-bar__value">$0.17</span>
          </div>
          <div class="qh-bar">
            <span class="qh-bar__name">dlt</span>
            <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:61%;--qh-delay:200ms"></span></span>
            <span class="qh-bar__value">$0.73</span>
          </div>
          <div class="qh-bar">
            <span class="qh-bar__name">Sling</span>
            <span class="qh-bar__track"><span class="qh-bar__fill" style="--qh-w:100%;--qh-delay:320ms"></span></span>
            <span class="qh-bar__value">$1.19</span>
          </div>
        </div>
      </div>

      <div class="qh-teaser__foot">
        <button type="button" class="qh-teaser__dot" aria-selected="true" aria-label="Wall clock into ClickHouse"></button>
        <button type="button" class="qh-teaser__dot" aria-selected="false" aria-label="Cost into BigQuery"></button>
        <span class="qh-teaser__label">lower is better · 3 runs</span>
      </div>
    </div>

    <div class="qh-why">
      <h2>Why quickhouse</h2>
      <p><strong>It's fast.</strong> Rows decode straight off the wire into Arrow, in Rust. Ranges read in parallel; decode overlaps upload.</p>
      <p><strong>It's safe with messy data.</strong> Atomic swaps, idempotent incrementals, automatic retries on transient blips.</p>
      <p><strong>Nothing to stand up.</strong> An ordinary Python dependency — no JVM, no Spark, no service.</p>
    </div>
  </div>

  <div class="qh-newband">
    <div>
      <span class="qh-newband__tag">New in 0.13</span>
      <h2>Bad data never reaches the table.</h2>
      <p>Pass a <code>Validation</code> to <code>sync()</code> and your
      <a href="https://greatexpectations.io/">Great Expectations</a> suite runs against the
      per-run staging table <em>before</em> promotion. If it fails, the swap or
      <code>MERGE</code> is aborted, staging is dropped and <code>sync()</code> raises —
      a preventive gate, not a post-mortem.</p>
      <p style="margin-top:0.9rem"><a href="guide/data-quality.html">Read the data-quality guide &rarr;</a></p>
    </div>
    <div>
      <div class="qh-newband__checks">
        <div><span>stage &rarr; validate &rarr; promote</span><b>gated</b></div>
        <div><span>full refresh, both destinations</span><b>✓</b></div>
        <div><span>incremental, both destinations</span><b>✓</b></div>
        <div><span>on failure</span><b>ValidationFailed</b></div>
      </div>
    </div>
  </div>

  <h2>Supported pairs</h2>
  <div class="qh-flow">
    <div class="qh-flow__col">
      <div class="qh-flow__node">PostgreSQL</div>
      <div class="qh-flow__node">MySQL</div>
      <div class="qh-flow__node">BigQuery</div>
      <div class="qh-flow__node">CleverTap</div>
      <div class="qh-flow__node">AppsFlyer</div>
    </div>
    <div class="qh-flow__lanes" aria-hidden="true">
      <div class="qh-flow__lane" style="--qh-delay:0s"></div>
      <div class="qh-flow__lane" style="--qh-delay:0.42s"></div>
      <div class="qh-flow__lane" style="--qh-delay:0.84s"></div>
      <div class="qh-flow__lane" style="--qh-delay:1.26s"></div>
      <div class="qh-flow__lane" style="--qh-delay:1.68s"></div>
    </div>
    <div class="qh-flow__hub" aria-hidden="true"></div>
    <div class="qh-flow__lanes" aria-hidden="true">
      <div class="qh-flow__lane" style="--qh-delay:0s"></div>
      <div class="qh-flow__lane" style="--qh-delay:0.75s"></div>
    </div>
    <div class="qh-flow__col">
      <div class="qh-flow__dest"><b>ClickHouse</b><span>ReplacingMergeTree · atomic swap</span></div>
      <div class="qh-flow__dest"><b>BigQuery</b><span>single MERGE on key</span></div>
    </div>
  </div>
  <p class="qh-hero__meta">Every source moves into either destination.</p>
</div>
```

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
      <li><a href="guide/sources.html">Sources</a></li>
      <li><a href="guide/destinations.html">Destinations</a></li>
      <li><a href="guide/sync-modes.html">Sync modes</a></li>
      <li><a href="guide/type-mapping.html">Type mapping</a></li>
      <li><a href="guide/data-quality.html">Data quality</a></li>
      <li><a href="guide/performance.html">Performance &amp; safety</a></li>
      <li><a href="guide/benchmark.html">Benchmark</a></li>
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
guide/destinations
guide/sync-modes
guide/type-mapping
guide/data-quality
guide/performance
guide/benchmark
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
