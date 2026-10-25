# Loading DataFrames

**What you'll learn**

- How to write a pandas DataFrame into ClickHouse or BigQuery with
  `from_pandas()`.
- How to update a table from a frame without a watermark column.
- What happens to the index, and which column types need care.
- How to load polars, pyarrow and DuckDB data the same way.

**Before you start:** read [Your first sync](first-sync.md), and install the
pandas extra:

```bash
pip install "quickhouse[pandas]"
```

## The task

Daily Grind's finance team sets a monthly revenue target for every store and
keeps the targets in an Excel file. Store managers want to see their target
next to their actual sales on the dashboard. The data isn't in a database, so
you read it with pandas and hand the DataFrame to quickhouse.

## Load a spreadsheet

If you don't have finance's file, make a practice one. Reading and writing
Excel files in pandas needs `openpyxl` (`pip install openpyxl`):

```python
import pandas as pd

pd.DataFrame({
    "month": pd.to_datetime(["2026-10-01"] * 4 + ["2026-11-01"] * 4),
    "store_id": [1, 2, 3, 4] * 2,
    "revenue_target": [120000, 80000, 150000, 60000, 125000, 82000, 155000, 62000],
}).to_excel("store_targets.xlsx", sheet_name="Targets", index=False)
```

Now load it into ClickHouse:

```python
import pandas as pd
import quickhouse as qh

dst = qh.ClickHouse("http://localhost:8123", database="analytics")

targets = pd.read_excel("store_targets.xlsx", sheet_name="Targets")
targets["month"] = targets["month"].dt.date      # keep the date, drop the time

result = qh.from_pandas(targets, dst, dest_table="store_targets",
                        mode="full", key=["month", "store_id"])
print(result.rows_written)
```

```text
8
```

`from_pandas()` takes the frame and a destination instead of a source. Behind
the scenes it converts the frame to Apache Arrow (the in-memory format
quickhouse uses everywhere) and runs an ordinary sync. Everything after
`dest_table` is passed on to `sync()`, so `key`, `type_overrides`, `rename`,
`include`, `exclude`, `on_progress` and the table-design options all work, and
you get the same `TransferResult` back.

The new table looks like this:

```text
month            Date32
store_id         Int64
revenue_target   Nullable(Int64)
```

Why the `.dt.date` line? Excel has no separate date type, so pandas reads the
`month` column as a date *and time* (midnight). Without the conversion, the
column would land as a timestamp, with `00:00:00` on every value.

The same call writes to BigQuery: pass
`qh.BigQuery("daily-grind-data", dataset_id="analytics")` as the destination.

## The three modes

`from_pandas()` takes the same three modes as `sync()`:

- `mode="full"` replaces the table, through a staging table and an atomic swap.
  The [shrink safety net](first-sync.md#a-full-refresh-wont-shrink-your-table)
  applies: if finance removes a store from the sheet, pass
  `allow_full_refresh_shrink=True`.
- `mode="incremental"` adds new rows and replaces changed ones, matched on
  `key`.
- `mode="append"` inserts the rows as they are: no staging table and no check
  for duplicates.

If finance always sends the whole sheet, a full refresh is the simplest choice.
Incremental mode is for when you only have the rows that changed.

### Updating rows without a watermark

A database source needs a watermark to know where it stopped reading. A frame
doesn't: you are holding every row you want to write. The frame *is* the
change, so incremental mode needs only a `key`:

```python
from datetime import date

qh.from_pandas(targets, dst, dest_table="store_targets_live",
               mode="incremental", key=["month", "store_id"])

# Later: finance raises Old Town's November target.
revised = pd.DataFrame({"month": [date(2026, 11, 1)], "store_id": [4],
                        "revenue_target": [70000]})
qh.from_pandas(revised, dst, dest_table="store_targets_live",
               mode="incremental", key=["month", "store_id"])
```

In ClickHouse, read the table with `FINAL` to see one row per key, as in
[Keeping tables up to date](incremental.md):

```sql
SELECT revenue_target FROM analytics.store_targets_live FINAL
WHERE month = '2026-11-01' AND store_id = 4      -- 70000
```

Leave out `key` and quickhouse refuses to run, because it would have nothing to
match rows on.

```{warning}
On ClickHouse, choose a table's mode when you first load it. A table that
quickhouse creates with `mode="full"` uses a plain `MergeTree` engine, which
keeps every row it's given. A later `mode="incremental"` load into that table
adds the revised row *next to* the old one instead of replacing it: loaded
into `store_targets` from the first example, Old Town's November target has
two rows, 62,000 and 70,000. quickhouse warns about it with
`non_replacing_incremental_target`, and
`fail_on_warnings={"non_replacing_incremental_target"}` refuses the load
instead. Load the table incrementally from the start, as above, if you'll be
sending changed rows. BigQuery doesn't have this problem: its incremental loads
always merge on `key`.
```

### Duplicate keys in one frame

If a frame holds two rows with the same key, which one should win? quickhouse
keeps the **last** one in the frame, and warns you:

```text
QuickhouseWarning: the frame has 1 row(s) sharing a key with another row;
keeping the last of each (key=['month', 'store_id']). ...
```

If the frame has a column that says which version is newer, pass it as
`watermark` and the row with the highest value wins instead:

```python
revisions = pd.DataFrame({
    "month": [date(2026, 11, 1)] * 2,
    "store_id": [4, 4],
    "revenue_target": [73000, 74000],
    "revised_at": pd.to_datetime(["2026-10-20 10:00", "2026-10-19 09:00"]),
})

qh.from_pandas(revisions, dst, dest_table="target_revisions",
               mode="incremental", key=["month", "store_id"],
               watermark="revised_at")
```

The 73,000 row wins, because it was revised later, even though it comes first
in the frame.

## The index

pandas frames carry an **index**, the row labels on the left. quickhouse
**drops the index** by default. That catches people out after a `groupby`,
which moves the grouping column into the index:

```python
orders = pd.DataFrame({"store_id": [1, 1, 2], "total": [3.50, 4.00, 5.25]})
by_store = orders.groupby("store_id").sum()

qh.from_pandas(by_store, dst, dest_table="sales_by_store", mode="full")
```

```text
QuickhouseWarning: from_pandas() is dropping the frame's index, which is not a
plain RangeIndex (Index, name='store_id'). ...
```

The table ends up with a `total` column and no `store_id`. quickhouse only
chooses a table's columns when it creates the table, so a corrected frame sent
to the same table would still lose `store_id`, this time with a
`dropped_source_column` warning on the result. Drop the broken table first:

```bash
docker compose exec clickhouse clickhouse-client --query "DROP TABLE analytics.sales_by_store"
```

Then keep the index in one of two ways:

```python
qh.from_pandas(by_store.reset_index(), dst, dest_table="sales_by_store",
               mode="full", key=["store_id"])

qh.from_pandas(by_store, dst, dest_table="sales_by_store",
               mode="full", key=["store_id"], index=True)
```

With `index=True`, every index level needs a name, because the name becomes
the column name. An unnamed index is refused with a message suggesting
`df.rename_axis("id")`.

## Column types that need care

quickhouse converts what it safely can and refuses the rest with a message
that names the column and the fix. The cases you are most likely to meet:

- **Nanosecond timestamps.** quickhouse stores time to the microsecond. If a
  column really holds smaller fractions of a second, the load stops and tells
  you to round it: `df["ts"] = df["ts"].dt.floor("us")`.
- **Time zones.** A column in a named zone, such as `Asia/Jakarta`, keeps that
  zone in ClickHouse. A fixed offset such as `+07:00` can't be named, so it is
  converted to UTC with a warning. The moment in time is the same either way.
  Use `.dt.tz_convert("Asia/Jakarta")` to keep a zone.
- **`Decimal` values.** The precision is worked out from the values in the
  frame. A first load of prices like `4.50` creates a `Decimal(3, 2)` column,
  and a later frame with much bigger numbers can fail with
  `Decimal convert overflow`. Fix the type up front:
  `type_overrides={"price": "Decimal(12, 2)"}`.
- **Missing numbers.** In a pandas float column, `NaN` means missing and lands
  as `NULL`. In polars and pyarrow, `NaN` is a real value ("not a number") and
  is stored as one. Use `None` when you mean missing.
- **Large IDs with gaps.** A plain pandas integer column with a missing value
  becomes floats, and `9007199254740993` silently turns into
  `9007199254740992`. Use the nullable `Int64` type to keep it exact:
  `df["customer_id"] = df["customer_id"].astype("Int64")`.
- **Lists and dictionaries** in a cell can't become a column. Convert them to
  text first: `df["items"] = df["items"].map(json.dumps)`.
- **Very old or far-future dates.** quickhouse accepts the years 1900 to 2299,
  the range ClickHouse can store. A value outside them stops the load before
  anything is written.

Categories become their plain values, and a column with no values at all
becomes a text column with a warning. The full list is in
[Type mapping](../guide/type-mapping.md).

## Not just pandas

quickhouse works with anything that can turn into Arrow, so other DataFrame
libraries go through the same call. Say the menu team exports its price list
as `menu.parquet`:

```python
import duckdb
import polars as pl
import pyarrow.parquet as pq

qh.from_pandas(pl.read_parquet("menu.parquet"), dst, dest_table="menu",
               mode="full", key=["sku"])

qh.from_pandas(pq.read_table("menu.parquet"), dst, dest_table="menu",
               mode="full", key=["sku"])

qh.from_pandas(duckdb.sql("SELECT * FROM 'menu.parquet' WHERE price > 3.5"),
               dst, dest_table="menu_premium", mode="full", key=["sku"])
```

A polars DataFrame, a pyarrow `Table` or `RecordBatch`, a DuckDB query result,
and any object that supports the Arrow stream interface all work. pandas
itself is only needed for pandas frames.

## Big frames

A frame lives in your computer's memory, so this is the one place where
quickhouse can't keep memory use low for you. While converting and sending,
expect a peak of about three to four times the frame's own size.

For a file too big to load comfortably, read it in pieces and append each one:

```python
for chunk in pd.read_csv("pos_export.csv", parse_dates=["paid_at"],
                         chunksize=500_000):
    qh.from_pandas(chunk, dst, dest_table="pos_transactions", mode="append")
```

If the data already lives in a database, a `sync()` from that database is
better still: it streams rows across without holding the whole table in
memory.

## Options that don't apply to a frame

A frame has no database behind it, so options about reading from a source are
refused with a message that names the option. That includes `source_table`,
`source_query`, `lookback_seconds`, `chunk_rows`, `read_max_rows_per_sec`,
`read_idle_timeout_secs`, `seed_watermark` and `retry_max_attempts`:

```text
RuntimeError: ... configuration error: source_table/source_query do not apply to
a DataFrame source: it reads the frame you passed, not a table or a query.
Filter the frame itself before handing it over.
```

`parallelism` is accepted. The frame is read as one stream, but the inserts
still go out in parallel.

## Recap

- `qh.from_pandas(df, destination, dest_table=..., mode=...)` writes a frame.
  Everything after `dest_table` is passed to `sync()`.
- Incremental mode needs only `key`. Duplicate keys keep the last row, or the
  row with the highest `watermark` value.
- On ClickHouse, load a table incrementally from the start if you'll send
  changed rows later.
- The index is dropped unless you `reset_index()` or pass `index=True`.
- Round nanosecond timestamps, prefer named time zones, pin `Decimal`
  precision, and use `Int64` for IDs with gaps.
- polars, pyarrow and DuckDB data go through the same call.
- Big files: read in chunks and use `mode="append"`.

## Next

[Designing ClickHouse tables](clickhouse-tables.md): sort keys, partitions,
deduplication and schema changes.
