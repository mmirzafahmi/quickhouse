# Data quality

`sync()` can run a [Great Expectations](https://greatexpectations.io/) suite
against your data **before it reaches the destination** — a *preventive* gate.
The data lands in a per-run staging table first; the suite runs against that
staging table; only if it passes does quickhouse promote it (the atomic swap for
a full refresh, or the `MERGE` for a BigQuery incremental). If the suite fails,
the promotion is aborted, staging is dropped, and `sync()` raises — so rejected
data never touches the live table.

Install the optional dependency:

```bash
pip install 'quickhouse[quality]'
```

## Quickstart

Build a Great Expectations context, register a SQL datasource pointing at your
**destination** database/dataset, and pass a {class}`quickhouse.Validation` to
`sync()`'s `validate=`:

```python
import great_expectations as gx
import quickhouse

context = gx.get_context()
context.data_sources.add_sql(
    name="analytics_ch",
    connection_string="clickhouse+http://default:@localhost:8123/analytics",
)

suite = gx.ExpectationSuite(name="orders_quality")
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="amount", min_value=0))

src = quickhouse.Postgres("postgresql://user:pw@localhost:5432/shop")
dst = quickhouse.ClickHouse("http://localhost:8123", database="analytics")

quickhouse.sync(
    src, dst, dest_table="orders", source_table="orders", mode="full",
    validate=quickhouse.Validation(
        suite=suite,
        context=context,
        datasource="analytics_ch",
    ),
)
```

If a row violates an expectation, `sync()` raises {class}`quickhouse.ValidationFailed`
and `analytics.orders` is left exactly as it was.

```{admonition} The datasource must point at the destination
:class: important
quickhouse only injects the per-run staging **table name** into the datasource
you registered — it never builds the connection, so your credentials stay in
your GX config. Point `datasource=` at the same database/dataset as the
destination (for ClickHouse, include the database in the connection string; for
BigQuery, the dataset). quickhouse creates a temporary table asset aimed at the
staging table, validates, then removes it.
```

## Where the gate runs

The gate runs wherever a run can be staged before promotion — full-refresh and
incremental, into either destination:

| Mode | ClickHouse | BigQuery |
| --- | --- | --- |
| `full` | ✅ gated | ✅ gated |
| `incremental` | ✅ gated | ✅ gated |
| `append` | ❌ not supported | ❌ not supported |

A ClickHouse **incremental** sync normally inserts rows straight into the
destination (dedup happens lazily via `ReplacingMergeTree`). When you attach a
`validate=`, quickhouse transparently routes that run through a staging table so
the gate has something to check, then promotes it with `INSERT … SELECT` — the
`ReplacingMergeTree` still dedups the promoted rows exactly as a direct insert
would. The only paths with no single staging table to gate are `append` mode (a
bronze-landing direct insert) and `chunk_rows` (keyset resumable reads commit
each chunk straight into the destination); attaching `validate=` to either
raises a clear error rather than silently skipping validation.

## Reacting to results

Pass `on_result=` to observe every run (pass *or* fail) — e.g. to log a summary
or build [data docs](https://docs.greatexpectations.io/) — before a failure
raises:

```python
def report(result):
    print("validation passed" if result.success else "validation FAILED")

validate = quickhouse.Validation(
    suite=suite, context=context, datasource="analytics_ch", on_result=report,
)
```

## Custom gates

`validate=` accepts **any** `callable(info) -> None` that raises to reject, where
`info` exposes `staging_table`, `database`, `dest_kind` (`"clickhouse"` /
`"bigquery"`), and `rows_written`. {class}`quickhouse.Validation` is the
batteries-included Great Expectations implementation; a bare callable lets you
plug in your own checks:

```python
def no_empty_load(info):
    if info.rows_written == 0:
        raise RuntimeError(f"refusing to promote an empty load into {info.staging_table}")

quickhouse.sync(src, dst, dest_table="orders", source_table="orders",
                mode="full", validate=no_empty_load)
```
```{admonition} Failure type across the boundary
:class: note
The gate runs inside the native engine, so a Python exception raised from
`validate` surfaces from `sync()` as a `RuntimeError` whose message carries the
original failure (the suite name and staging table). Catch it around the
`sync()` call to handle a rejected load.
```
