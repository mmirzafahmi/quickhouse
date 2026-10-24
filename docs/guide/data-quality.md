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
bronze-landing direct insert) and `chunk_rows` (keyset resumable reads land
each chunk in the destination before reading the next); attaching `validate=` to either
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

## Checking values after the fact

The gate checks a batch before it lands. It can't see damage a sync does to rows
already in the destination. In one production case a `source_query` had lost
columns the destination held, and every incremental run wrote them as NULL over
existing values, for two months, on 26 tables. Row counts matched throughout.
{func}`quickhouse.reconcile_values` is the check for that: it reads the same key
window from both sides and compares every column the two share, value by value.

```python
r = quickhouse.reconcile_values(
    src, dst, "account_move",
    source_table="account_move",
    key="id",
    window="id > 41000000",            # required: the newest keys, say
    tolerance={"amount_total": 0.01},
)
for c in r.columns:
    if c.mismatched:
        print(c.column, c.mismatched, c.null_in_dest_only, c.sample)
```

Each column reports `compared` and `mismatched`, `null_in_dest_only` (NULL in
the destination where the source holds a value, the signature of the case
above) and `null_in_source_only`, with sample keys. The result also counts keys
missing on either side; {func}`quickhouse.reconcile_keys` covers those in depth.

Both sides are read through quickhouse's own readers and compared as typed
values, not as text:

- **Numbers.** Integers and decimals compare exactly. A float on either side
  compares as a double, within `tolerance` if you give one. ClickHouse truncates
  a float written into a `Decimal` column (0.29 lands in `Decimal(15, 2)` as
  0.28), so such a column needs a tolerance of one step to read clean.
- **Datetimes** compare as instants, whatever timezone either type names, at the
  destination column's precision: a `DateTime` holds whole seconds, so a
  source's microseconds aren't drift.
- **Booleans.** MySQL's `tinyint(1)` matches a `Bool` column by truth and a
  `UInt8` one by value.
- **Wrappers.** `LowCardinality(T)` and `Nullable(T)` compare as the `T` they
  wrap.
- **NULLs.** A ClickHouse column that can't hold NULL stores one as its
  `DEFAULT`, or its type's default (`0`, `''`). That matches a source NULL, and
  over a source value it counts as `null_in_dest_only`.
- **Several rows per key.** The destination is read with `FINAL` where its
  engine merges rows, so an unmerged `ReplacingMergeTree` is compared on its
  newest row per key. A key still doubled after that, on either side, is counted
  in `duplicate_keys` and left out.

Both sides' rows are held in memory, which is why `window` is required. The
destination must be ClickHouse for now. It measures only: comparing per window
by hashing, to make a whole-table check cheap enough to schedule, and
`repair=True`, which re-syncs only the keys that differ, are next.
