# Your first sync

**What you'll learn**

- How to copy a whole table from PostgreSQL into ClickHouse with one call.
- What quickhouse does behind the scenes, and what it reports back.
- The safety nets that stop a full refresh from damaging your data.

**Before you start:** finish [Set up a practice environment](setup.md), or have
a PostgreSQL database and a ClickHouse server you can use.

## The task

Daily Grind's store managers want a dashboard that shows sales per store. The
dashboard reads from ClickHouse, but the data lives in the ordering app's
PostgreSQL database. Your first job is to get the `stores` table across.

## Copy a table

```python
import quickhouse as qh

src = qh.Postgres("postgresql://etl:etl@localhost:5432/etl")
dst = qh.ClickHouse("http://localhost:8123", database="analytics")

result = qh.sync(
    src, dst,
    source_table="stores",
    dest_table="stores",
    mode="full",
)
print(result)
```

That is the whole program. Line by line:

- `qh.Postgres(...)` describes **where to read from**. It doesn't connect yet;
  it only holds the connection details.
- `qh.ClickHouse(...)` describes **where to write to**: the server address and
  the database.
- `qh.sync(...)` does the work. It connects to both, copies the rows, and
  returns a summary.
- `source_table` and `dest_table` name the table on each side. They don't have
  to match.
- `mode="full"` means "replace the destination table with a fresh copy of the
  source". This is called a **full refresh**.

Now look at the result in ClickHouse:

```bash
docker compose exec clickhouse clickhouse-client --query "SELECT * FROM analytics.stores"
```

```text
1   Central Station   Jakarta    2021-03-01
2   Riverside Mall    Bandung    2022-07-15
3   Airport T3        Jakarta    2023-01-10
4   Old Town          Surabaya   2024-05-20
```

## What just happened

You didn't create the `stores` table in ClickHouse, yet it exists. On that one
call quickhouse:

1. **Read the source table's columns and types**, and worked out matching
   ClickHouse types: `integer` became `Int32`, `text` became `String`, `date`
   became `Date32`.
2. **Created the destination table**, because it didn't exist yet. You can turn
   this off with `create_if_missing=False` if you prefer to create tables
   yourself.
3. **Loaded the rows into a temporary staging table**, named something like
   `stores_quickhouse_tmp_1792830455`.
4. **Swapped the staging table into place** in one atomic step, then dropped
   the old copy.

Step 4 is the important one. Anyone querying `stores` during the sync sees the
complete old table right up until the moment they see the complete new one.
They never see a half-loaded table, and if the sync crashes halfway, the old
table is left untouched.

## Read the result

`sync()` returns a `TransferResult`. Printed, it looks like this:

```text
TransferResult(rows_read=4, rows_written=4, bytes_written=429, rows_deleted=0,
duration_secs=0.053, read_secs=0.000, stage_secs=0.012, promote_secs=0.006,
new_watermark=None, warnings=0, rows_written_failed_attempts=0)
```

The fields you'll use most:

| Field | What it tells you |
| --- | --- |
| `rows_read` | How many rows quickhouse read from the source. |
| `rows_written` | How many rows it wrote to the destination. |
| `duration_secs` | How long the whole sync took, in seconds. |
| `warnings` | A list of things that went *slightly* wrong but didn't stop the run, such as a value that had to be changed to fit. Empty on a clean run. |
| `new_watermark` | Only used by incremental syncs. Always `None` for a full refresh. |

You can read each one as an attribute:

```python
print(f"Copied {result.rows_written:,} rows in {result.duration_secs:.1f}s")
for w in result.warnings:
    print("warning:", w.message)
```

[Catching bad data](data-quality.md) explains the warnings in detail.

## Copy a bigger table

The `orders` table has 50,000 rows. Copy it the same way, but this time also
say which column identifies a row:

```python
result = qh.sync(
    src, dst,
    source_table="orders",
    dest_table="orders",
    mode="full",
    key=["id"],
)
print(f"{result.rows_written:,} rows")
```

`key=["id"]` tells quickhouse that `id` is the column that makes each row
unique. In ClickHouse it becomes the table's **sort order** (`ORDER BY id`),
which makes lookups by `id` fast. You'll see in
[Keeping tables up to date](incremental.md) that the key matters even more once
rows start changing.

```{tip}
Always pass `key` when the table has one, even for a full refresh. It costs
nothing and gives you a better-organised table.
```

## Watch the progress

A big table can take a while. Pass `on_progress` a function and quickhouse will
call it regularly with how far it has got:

```python
qh.sync(
    src, dst,
    source_table="orders", dest_table="orders", mode="full", key=["id"],
    on_progress=lambda p: print(f"{p.rows_written:,} rows, {p.rows_per_sec:,.0f} rows/s"),
)
```

Or, if you installed the `progress` extra, use the ready-made progress bar:

```python
with qh.progress_bar(desc="orders") as on_progress:
    qh.sync(src, dst, source_table="orders", dest_table="orders",
            mode="full", key=["id"], on_progress=on_progress)
```

quickhouse also logs each step to the terminal (on stderr), so you can see what
it is doing even without a callback.

## The safety nets

A full refresh *replaces* a table, which makes it the most dangerous thing
quickhouse can do. Three rules protect you.

### You must always choose a mode

There is no default. If you leave `mode` out, quickhouse refuses to run:

```text
RuntimeError: sync() requires mode=: "full" REPLACES the destination table ...
```

This is deliberate. Replacing a table should never happen by accident. The
three modes are:

- `"full"`: replace the table with a fresh copy.
- `"incremental"`: add new rows and update changed ones. See
  [Keeping tables up to date](incremental.md).
- `"append"`: add rows without checking for duplicates. Only for HTTP API and
  DataFrame sources.

### A full refresh won't shrink your table

Imagine a colleague edits the job to copy only the Jakarta stores, but forgets
that it's a full refresh:

```python
qh.sync(src, dst, dest_table="stores", mode="full",
        source_query="SELECT * FROM stores WHERE city = 'Jakarta'")
```

Run as-is, it would silently replace four stores with two. quickhouse stops it
before anything changes:

```text
RuntimeError: ... refusing full-refresh: swapping staging into 'stores' would
REPLACE 4 row(s) with only 2 row(s), shrinking the destination by 2. ...
```

Sometimes the source really did lose rows, for example when a store closes and
its row is deleted. Then you tell quickhouse it is expected:

```python
qh.sync(src, dst, source_table="stores", dest_table="stores", mode="full",
        allow_full_refresh_shrink=True)
```

The run then goes ahead and records a `full_refresh_shrink` warning on the
result, so the shrink is still visible afterwards.

### The swap is all or nothing

As described above, the new data only becomes visible once it is completely
loaded. A failed run leaves the old table exactly as it was. You can safely
re-run a failed full refresh.

## When to use a full refresh

A full refresh is the simplest way to keep a copy correct, because every run
starts from scratch. It suits:

- **Small tables**, such as `stores`, product lists, or price lists.
- **Tables where rows get deleted**, because a fresh copy drops deleted rows
  automatically.
- **Summary tables** you rebuild completely each time.

It's a poor fit for big tables that grow every day, like `orders`. Copying five
years of orders every hour to pick up the last hour's worth wastes time and
puts load on the source database. That is what incremental syncs are for.

## The same call works everywhere

Changing the source or destination only changes the descriptor objects. The
`sync()` call stays the same. Here is the same copy into BigQuery instead of
ClickHouse:

```python
dst = qh.BigQuery("daily-grind-data", dataset_id="analytics")

qh.sync(src, dst, source_table="stores", dest_table="stores", mode="full")
```

[Reading from databases](databases.md) and [Loading into BigQuery](bigquery.md)
cover the other sources and destinations.

## Recap

- `qh.sync(source, destination, source_table=..., dest_table=..., mode=...)`
  copies one table.
- quickhouse creates the destination table for you, with matching types.
- A full refresh loads into a staging table, then swaps it in atomically.
- `mode` is required, and a full refresh refuses to shrink a table unless you
  pass `allow_full_refresh_shrink=True`.
- Pass `key` whenever the table has an identifying column.

## Next

[Choosing what to copy](choosing-data.md): copy only some columns, rename them,
and copy the result of a SQL query.
