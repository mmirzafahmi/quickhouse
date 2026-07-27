---
sd_hide_title: true
---

# quickhouse

```{div} sd-text-center sd-fs-2 sd-font-weight-bold
quickhouse
```

```{div} sd-text-center sd-fs-5 sd-text-muted
Move tables from PostgreSQL, MySQL, or BigQuery into ClickHouse or BigQuery —
fast, in one function call.
```

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

```{admonition} Status: pre-1.0
:class: warning
quickhouse is used against real production data and is covered by an
integration test suite, but the Python API may still change between minor
versions before 1.0. Pin a compatible range (e.g. `quickhouse~=0.11`) and watch
the [changelog](changelog.md). A few knobs are marked *experimental* — those may
change without a major bump.
```

## Get started

::::{grid} 1 1 2 2
:gutter: 3

:::{grid-item-card} {octicon}`rocket` Installation
:link: installation
:link-type: doc
`pip install quickhouse` — prebuilt wheels, no Rust toolchain needed.
:::

:::{grid-item-card} {octicon}`zap` Quickstart
:link: quickstart
:link-type: doc
Your first sync in a dozen lines, full-refresh and incremental.
:::

:::{grid-item-card} {octicon}`book` User guide
:link: guide/sources
:link-type: doc
Sources, sync modes, type mapping, performance, and safety.
:::

:::{grid-item-card} {octicon}`code` API reference
:link: api
:link-type: doc
Every `sync()` argument and connection descriptor, documented.
:::
::::

## When to use quickhouse

Reach for it when you want to move whole tables — full refresh or incremental —
from PostgreSQL/MySQL/BigQuery into ClickHouse or BigQuery, fast, from your own
Python jobs with almost no setup. It fits cron/Airflow/Dagster tasks and one-off
backfills well.

Look elsewhere when you need in-warehouse SQL transformations (use **dbt**),
change-data-capture or streaming (use **Debezium/Kafka**), a large catalog of
SaaS connectors (use **Airbyte / Fivetran / dlt**), or arbitrary
source↔destination pairs — quickhouse deliberately supports a focused, fast set.

## Supported sources and destinations

| Source | → ClickHouse | → BigQuery |
|---|:--:|:--:|
| PostgreSQL | ✅ | ✅ |
| MySQL | ✅ | ✅ |
| BigQuery | ✅ | ✅ |
| CleverTap (HTTP API) | ✅ | ✅ |
| AppsFlyer (HTTP API) | ✅ | ✅ |

ClickHouse is a destination only; BigQuery is both a source and a destination.

## Why quickhouse

- **It's fast.** Rows are decoded straight off the wire into Arrow, in Rust —
  no per-row Python, no intermediate DataFrame. Tables are split into ranges
  and read in parallel, and decoding overlaps uploading. On a laptop-class box
  a 1M-row, 20-column full refresh runs at **hundreds of thousands of rows per
  second** while peak memory stays flat (under ~180 MB).
- **It's one function call.** {func}`~quickhouse.sync` replaces the cursor loop,
  manual batching, retry logic, and `CREATE TABLE` you'd otherwise write by hand.
- **It's safe with real, messy data.** Full refreshes swap in atomically;
  incremental syncs are idempotent; transient network blips retry automatically;
  legacy quirks like MySQL zero-dates coerce to `NULL` with a warning instead of
  aborting.
- **It's gentle on a small production database.** `read_max_rows_per_sec` paces
  the read, and the scan itself backs off since streaming results only produce as
  fast as the client consumes.
- **There's nothing to stand up.** An ordinary Python dependency — no JVM, no
  Spark cluster, no separate service.

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
