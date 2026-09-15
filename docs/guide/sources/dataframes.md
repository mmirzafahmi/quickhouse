# DataFrames

A frame you already hold in memory goes in through `from_pandas`, not `sync`:

```python
import quickhouse as qh

dst = qh.ClickHouse("http://localhost:8123", database="analytics")
qh.from_pandas(df, dst, dest_table="orders", mode="full", key=["id"])
```

Needs `pip install 'quickhouse[pandas]'`. Everything after the frame is the same
`sync()` you already know — `**sync_kwargs` is forwarded verbatim, so `engine`,
`order_by`, `partition_by`, `type_overrides`, `include`/`exclude`, `on_progress`
and `validate` all work, and the return value is the same `TransferResult`.

## Not just pandas

The conversion is Arrow, so pandas is the headline rather than the requirement.
A `pyarrow.Table` or `RecordBatch`, a `polars.DataFrame`, a DuckDB relation, or
anything exposing the Arrow PyCapsule interface (`__arrow_c_stream__`) all work:

```python
qh.from_pandas(pl.read_parquet("orders.parquet"), dst, dest_table="orders", mode="full")
qh.from_pandas(duckdb.sql("SELECT * FROM orders"), dst, dest_table="orders", mode="full")
```

## Modes

```{raw} html
<div class="qh-params">
  <div>
    <div>
      <div class="qh-params__name">mode="full"</div>
      <div class="qh-params__type">replace</div>
    </div>
    <p class="qh-params__desc">Loads a staging table, then swaps it in atomically. Shrinking the destination is refused unless <code>allow_full_refresh_shrink=True</code>.</p>
  </div>
  <div>
    <div>
      <div class="qh-params__name">mode="append"</div>
      <div class="qh-params__type">insert</div>
    </div>
    <p class="qh-params__desc">Straight insert &mdash; no staging, no merge, no dedup. Bronze landing, for when you know the rows are new.</p>
  </div>
  <div>
    <div>
      <div class="qh-params__name">mode="incremental"</div>
      <div class="qh-params__type">upsert on key</div>
    </div>
    <p class="qh-params__desc">Upserts on <code>key</code>, and needs <strong>no watermark</strong> &mdash; unlike every other source.</p>
  </div>
</div>
```

### Incremental needs no watermark

A database source needs a watermark because it has to know where to resume
*reading*. A frame does not: you already hold every row, so the frame **is** the
delta. `key` takes over the job of making incremental meaningful, and is
required in the watermark's place:

```python
qh.from_pandas(df, dst, dest_table="orders", mode="incremental", key=["id"])
```

If the frame contains two rows with the same key, quickhouse keeps the last and
warns. It resolves that here rather than in SQL because neither destination can
order duplicates *within* one batch without a version column — ClickHouse would
keep whichever part merged last, BigQuery whichever row `ROW_NUMBER` saw first.
Pass `watermark="updated_at"` to order by that column instead.

## The index

The index is **dropped by default**, and you get a warning if it looked
meaningful (a `MultiIndex`, a named index, or anything that is not a plain
`RangeIndex`). pandas' own `to_sql` writes the index by default, so the warning
exists to catch the difference rather than let you discover it later.

```python
qh.from_pandas(df, dst, dest_table="t", mode="full", index=True)   # keep it
df.reset_index().pipe(qh.from_pandas, dst, dest_table="t", mode="full")  # same thing
```

With `index=True` every index level must be named — the name becomes the column
name. quickhouse will not invent `__index_level_0__` for you.

## Types

See [Type mapping](../type-mapping.md#dataframes) for the full table. The short
version: quickhouse normalises what it safely can and **refuses the rest by
name**, rather than coercing silently.

Converted for you: nanosecond timestamps → microseconds, categoricals →
their values, `float16` → `float32`, `date64` → `date32`, time-of-day → text,
`large_string`/`string_view` → `string`, and a fixed-offset timezone → UTC (with
a warning; ClickHouse's `DateTime64` takes a zone *name*, and the instant is
preserved either way).

Refused, naming the column and the fix: sub-microsecond precision, values
outside ClickHouse's 1900–2299 window, nested types (`list`/`struct`/`map`),
`Decimal256`, durations and intervals, all-null columns, duplicate column names
and non-string column names.

## Memory

This is the one place quickhouse's bounded-memory promise does not apply — the
frame is in RAM by definition. Expect a transient peak of roughly 3–4x the
frame's Arrow footprint while it is converted, serialized and decoded. For a
frame near your machine's limit, split it and use `mode="append"`.

## What does not apply

A frame has no source to connect to, so `source_table`, `source_query`,
`chunk_rows`, `lookback_seconds`, `read_max_rows_per_sec`,
`read_idle_timeout_secs`, `partition_column`, `partition_source_expr`,
`watermark_source_expr`, `seed_watermark` and `retry_max_attempts` are all
rejected with a message naming the knob — never silently ignored.

`parallelism` is accepted and ignored on the read side (the frame is decoded
single-stream), though inserts still go out concurrently.
