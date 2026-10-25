# Catching bad data

**What you'll learn**

- How quickhouse tells you about data it had to change to make it fit.
- How to make a run stop on the problems you care about, before anything
  becomes permanent.
- How to run your own checks on the data before it goes live, including Great
  Expectations suites.

**Before you start:** read [Your first sync](first-sync.md) and
[Keeping tables up to date](incremental.md). The examples use the sample data
from [Set up a practice environment](setup.md).

## The task

Last year, Daily Grind's stock report showed every delivery as "priority 1",
even the urgent ones. The import had squeezed the priority levels into
true/false values on the way in. Every sync reported success, and nobody
noticed for a month.

Your job is to make sure that doesn't happen again. You'll build three layers of
protection, from the simplest to the strictest:

1. **Read the warnings** quickhouse attaches to every result.
2. **Fail the run** on the warnings that matter to you.
3. **Check the data yourself** before it reaches the live table.

## Layer 1: read the warnings

### Do a trial run

Before you schedule a new sync, run it once into a scratch table and look at
what quickhouse reports. Here is a trial run of the inventory system's
`stock_moves` table:

```python
import quickhouse as qh

src = qh.MySQL("mysql://etl:etl@localhost:3306/etl")
dst = qh.ClickHouse("http://localhost:8123", database="analytics")

result = qh.sync(
    src, dst,
    source_table="stock_moves",
    dest_table="stock_moves_trial",
    mode="full",
    key=["id"],
)
print(result.rows_written, "rows,", len(result.warnings), "warning(s)")
for w in result.warnings:
    print(w.kind, w.column, w.count)
```

```text
1000 rows, 1 warning(s)
collapsed_bool priority 500
```

The run succeeded and all 1,000 rows arrived, but 500 values in the `priority`
column were changed on the way. In MySQL, `tinyint(1)` is the usual way to
store true/false, so quickhouse reads such a column as a boolean. Daily Grind's
inventory system uses it for priority levels 0 to 3 instead, so every 2 and 3
became `true`. Count the values in `analytics.stock_moves_trial` and you'll
find only `false` (250 rows) and `true` (750 rows). That is last year's bug,
caught on the first run.

### What a warning tells you

Each item in `result.warnings` is a `TransferWarning` with these fields:

| Field | What it holds |
| --- | --- |
| `kind` | A short, fixed name for the problem, such as `collapsed_bool`. |
| `column` | The column it happened in, or `None` if it's about the whole table. |
| `count` | How many values or rows were affected, added up over the whole run. |
| `sample` | An example of an offending value, when one was available. Often `None`. |
| `message` | A sentence for humans, usually with advice on how to fix it. |

Write your code against `kind`, never against `message`. The kinds are stable
names; the wording of the messages can change between versions.

### The warnings worth knowing

quickhouse has more than twenty kinds of warning. These six mean your data
changed, so they deserve the most attention:

- **`null_watermark`**: some rows have an empty (`NULL`) watermark, so an
  incremental sync skips them on this run and every future one, while still
  reporting success. This is the most dangerous one.
- **`null_key`**: some rows have an empty (`NULL`) value in a `key` column, so
  they were left out: a key can't be empty in the destination. `count` says
  how many.
- **`collapsed_bool`**: a MySQL `tinyint(1)` held numbers other than 0 and 1,
  and they were squeezed into true/false, as you just saw.
- **`coerced_decimal`**: a decimal number didn't fit the column's declared size
  (or wasn't a number at all), so it was stored as `NULL`.
- **`coerced_date`**: a date couldn't be stored, such as MySQL's `0000-00-00` or
  a year outside ClickHouse's range of 1900 to 2299, so it was stored as
  `NULL`. In a key or watermark column that can't be `NULL`, the row is left
  out instead, and the message says so.
- **`full_refresh_shrink`**: a full refresh left the table smaller than before.
  You only see it when you allowed that with `allow_full_refresh_shrink=True`.

Four more compare the run with the destination table that's already there, and
are raised before anything is written:

- **`dropped_source_column`**: a source column has no column in the ClickHouse
  table, so it's left out of every row
  ([Designing ClickHouse tables](clickhouse-tables.md#when-the-source-gets-a-new-column)).
- **`unsourced_destination_column`**: the table has a column that no source
  column fills, so the rows written leave it empty
  ([Choosing what to copy](choosing-data.md#rename-columns)).
- **`non_replacing_incremental_target`**: an incremental sync is writing into a
  ClickHouse table that keeps every copy of a row
  ([Keeping tables up to date](incremental.md#starting-from-now)).
- **`recreated_destination`**: the incremental table was dropped, so the run
  creates it again and reads from the start
  ([Keeping tables up to date](incremental.md#where-quickhouse-keeps-its-place)).

The complete list is in the
[performance and safety guide](../guide/performance.md#failing-a-run-on-a-warning).

A warning doesn't stop the run (apart from one rare BigQuery case the guide
describes). The rows are written, and an incremental sync saves its new
position. At the very least, log the warnings of every scheduled sync so
someone sees them.

```{tip}
[Loading DataFrames](dataframes.md) has warnings of its own. When
`from_pandas()` has to make a choice about your frame, such as which of two
rows with the same key to keep, it issues a Python warning of the class
`qh.QuickhouseWarning`. To turn those into errors, call
`warnings.simplefilter("error", qh.QuickhouseWarning)` before loading.
```

## Layer 2: stop the run on a warning

### fail_on_warnings

Name the kinds you never want to accept, and quickhouse turns them into errors:

```python
qh.sync(src, dst, source_table="stock_moves", dest_table="stock_moves_trial",
        mode="full", key=["id"],
        fail_on_warnings={"collapsed_bool", "coerced_decimal", "coerced_date"})
```

```text
RuntimeError: stock_moves -> stock_moves_trial: fail_on_warnings: collapsed_bool
on column 'priority' (count 500) stopped the run. The destination is untouched:
the staged rows were dropped before the swap. ...
```

The error names the kind, the column, the count, and what was left in place. A
misspelled kind is an error too, so a typo can't quietly switch the check off.

quickhouse checks before every step that makes something permanent: before a
full refresh swaps in the new table, before rows move from a staging table into
the real one (a `MERGE`, for example), before an incremental run saves its
position, and before each chunk of a `chunk_rows` read is committed. So:

- **A full refresh, or an incremental sync into BigQuery**, stops before the
  swap or the `MERGE`, and the destination is untouched. If the run created
  the destination, it is left there, empty.
- **An incremental sync into ClickHouse** writes rows straight into the table,
  so they are already there when the check runs. It stops before saving its
  position, so the next run reads the same rows again. Once you've fixed the
  cause, that run writes corrected copies, and ClickHouse keeps the newest copy
  of each row.

### Why not check the warnings yourself afterwards?

It's tempting to write this instead:

```python
result = qh.sync(src, dst, ..., mode="incremental", ...)
if any(w.kind == "collapsed_bool" for w in result.warnings):
    raise RuntimeError("bad data")      # too late
```

This fails the job, but by the time `sync()` returns, the incremental run has
already saved its position. When your scheduler retries the job, the next run
starts after the bad rows, finds nothing new, raises no warning, and succeeds.
The bad rows stay in the table and the job looks healthy. `fail_on_warnings`
checks *inside* the run, before the position is saved, so a retry reads those
rows again.

### Fix the cause

For `collapsed_bool`, the fix is to read the column as a number with
`tinyint1_as_bool=False`. [Reading from databases](databases.md) explains the
setting.

There's one catch. The trial table's `priority` column was created as `Bool`.
quickhouse never changes the type of an existing column, and ClickHouse would
quietly turn the numbers back into true/false as they arrive. So change the
column first:

```sql
ALTER TABLE analytics.stock_moves_trial MODIFY COLUMN priority Int8
```

Then run again with the fix and the check both in place:

```python
result = qh.sync(src, dst, source_table="stock_moves", dest_table="stock_moves_trial",
                 mode="full", key=["id"], tinyint1_as_bool=False,
                 fail_on_warnings={"collapsed_bool", "coerced_decimal", "coerced_date"})
print(result.rows_written, result.warnings)   # 1000 []
```

No warnings, and `priority` holds 0, 1, 2 and 3 again.

```{tip}
A good starting set for `fail_on_warnings` is the warnings that mean data was
lost: `null_watermark`, `null_key`, `collapsed_bool`, `coerced_decimal`,
`coerced_date`, `dropped_source_column` and `unsourced_destination_column`.
Leave out any you've decided to accept, such as `coerced_date` on a legacy
table you know is full of `0000-00-00` dates.
```

## Layer 3: check the data before it goes live

Warnings cover problems quickhouse can spot by itself. Your business has its
own rules: a total can't be negative, refunds should stay below a certain
share. For those, give `sync()` a **validation gate**.

### How a gate works

A gate is a function you pass as `validate=`. quickhouse first loads the rows
into a temporary *staging table* in the destination, then calls your function.
If the function returns normally, the rows move into the real table. If it
raises an exception, quickhouse drops the staging table, leaves the real table
untouched, and `sync()` raises your exception, unchanged, with a note that
names the transfer.

Your function receives one argument with four fields: `staging_table` (the
staging table's name), `database` (the database or BigQuery dataset it's in),
`dest_kind` (`"clickhouse"` or `"bigquery"`) and `rows_written` (how many rows
it holds).

### Your first gate: a refund check

Daily Grind refunds about a quarter of its orders. A bug in the till software
once marked a whole day of paid orders as refunded, and the dashboards showed a
revenue crash that never happened. This gate refuses to publish `orders` when
more than 30% of them are refunds:

```python
import urllib.request

MAX_REFUND_RATE = 0.30


def clickhouse_query(sql):
    request = urllib.request.Request("http://localhost:8123/", data=sql.encode())
    with urllib.request.urlopen(request) as response:
        return response.read().decode().strip()


def check_refund_rate(info):
    if info.rows_written == 0:
        return  # nothing to check
    rate = float(clickhouse_query(
        f"SELECT countIf(status = 'refunded') / count() "
        f"FROM {info.database}.{info.staging_table}"
    ))
    print(f"refund rate in this load: {rate:.1%}")
    if rate > MAX_REFUND_RATE:
        raise ValueError(f"refund rate is {rate:.0%}, expected at most {MAX_REFUND_RATE:.0%}")


src = qh.Postgres("postgresql://etl:etl@localhost:5432/etl")

qh.sync(src, dst, source_table="orders", dest_table="orders", mode="full",
        key=["id"], validate=check_refund_rate)
```

```text
refund rate in this load: 25.0%
```

`clickhouse_query` sends SQL to ClickHouse's HTTP interface, so the gate can
look inside the staging table. 25% is within the limit, so the new copy of
`orders` goes live. To watch the gate reject a load without breaking your
data, set `MAX_REFUND_RATE = 0.20` and run it again:

```text
refund rate in this load: 25.0%
Traceback (most recent call last):
  ...
ValueError: refund rate is 25%, expected at most 20%
orders -> orders: data-quality validation rejected the staged data
```

`sync()` raised the gate's own `ValueError`. The last line is the note
quickhouse adds to it, which Python 3.11 and later print below the error. The
dashboards keep showing the last good copy of `orders`. Note the early
`return`: the gate runs even when there's nothing to load, with `rows_written`
set to `0`, and an empty load shouldn't count as a failure.

### Gates on incremental syncs

In an incremental sync, the staging table holds only this run's new and changed
rows. A refund-rate check would be a poor fit there: in a quiet hour, the only
changes might all be refunds. A check on each row works well:

```python
def check_totals(info):
    bad = int(clickhouse_query(
        f"SELECT countIf(total <= 0) FROM {info.database}.{info.staging_table}"
    ))
    if bad:
        raise ValueError(f"{bad} order(s) with a zero or negative total")


qh.sync(src, dst, source_table="orders", dest_table="orders_live",
        mode="incremental", watermark="updated_at", key=["id"],
        validate=check_totals)
```

Simulate a bug in the ordering app by giving two orders a negative total:

```sql
UPDATE orders SET total = -4.50, updated_at = now() WHERE id IN (49990, 49991);
```

The next run refuses them:

```text
ValueError: 2 order(s) with a zero or negative total
orders -> orders_live: data-quality validation rejected the staged data
```

Nothing reached `orders_live`, and the saved position didn't move, so every run
reads those two orders again and refuses them until the source is fixed. Set
their totals back to a positive amount (with a new `updated_at`), and the next
run goes through.

Normally an incremental sync into ClickHouse writes straight into the table.
With a gate attached, quickhouse loads a staging table first, checks it, then
copies it into the real table. The end result is the same.

### Great Expectations

If your team already writes [Great Expectations](https://greatexpectations.io/)
suites, quickhouse can run one as the gate. Install the extra, which also
brings the drivers Great Expectations needs to reach ClickHouse and BigQuery:

```bash
pip install "quickhouse[quality]"
```

Register a data source that points at the **destination** database, build your
suite, and wrap both in `qh.Validation`:

```python
import great_expectations as gx

context = gx.get_context()
context.data_sources.add_sql(
    name="analytics_ch",
    connection_string="clickhouse+http://default:@localhost:8123/analytics",
)

suite = gx.ExpectationSuite(name="orders_quality")
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="total", min_value=0))

qh.sync(
    src, dst, source_table="orders", dest_table="orders", mode="full", key=["id"],
    validate=qh.Validation(
        suite=suite,
        context=context,
        datasource="analytics_ch",
        on_result=lambda r: print("orders_quality passed:", r.success),
    ),
)
```

- **The data source must point at the destination database.** quickhouse only
  tells Great Expectations which staging table to check and never builds the
  connection, so your credentials stay in your Great Expectations setup. For
  BigQuery, point it at the destination dataset, for example
  `bigquery://daily-grind-data/analytics`.
- **`on_result` is called after every check, passed or failed.** Use it to log
  the outcome or build Great Expectations' data docs.
- **A failed suite raises `qh.ValidationFailed`**, and its `result` attribute
  holds the full Great Expectations result.

Give two orders a negative total again, and catch the failure to see which
expectation failed:

```python
try:
    qh.sync(
        src, dst, source_table="orders", dest_table="orders", mode="full", key=["id"],
        validate=qh.Validation(suite=suite, context=context, datasource="analytics_ch"),
    )
except qh.ValidationFailed as e:
    print([r.expectation_config.type for r in e.result.results if not r.success])
    raise
```

```text
['expect_column_values_to_be_between']
Traceback (most recent call last):
  ...
quickhouse.quality.ValidationFailed: data-quality suite 'orders_quality' failed against staging table 'orders_quickhouse_tmp_1792909139782865911' — the destination was left unchanged
orders -> orders: data-quality validation rejected the staged data
```

`ValidationFailed` is a kind of `RuntimeError`, the type of every other failed
sync, so an `except RuntimeError` around `sync()` catches it too. A gate
function of your own is different: `sync()` raises whatever it raised, such as
the `ValueError` from the refund check.

The [data-quality guide](../guide/data-quality.md) has more on the setup.

### Where gates work

Gates work for full refreshes and incremental syncs, into ClickHouse and
BigQuery alike. They don't work for `mode="append"` or for incremental syncs
with `chunk_rows`: both write rows into the destination as they go, so there's
no single staging table to check. Passing `validate=` with either is a
configuration error before anything is copied, rather than a check that
silently never runs.

## Recap

- Every `TransferResult` carries `warnings`. Match on `kind`, and at least log
  them for every scheduled sync.
- `fail_on_warnings` stops a run before anything becomes permanent. Checking
  `result.warnings` after `sync()` returns is too late for an incremental sync.
- A `validate=` gate checks the staging table before the rows go live. If it
  raises, the real table is untouched and `sync()` raises the gate's own
  exception.
- In an incremental sync, the gate sees only that run's new and changed rows.
- `qh.Validation` runs a Great Expectations suite as the gate. A failed suite
  raises `qh.ValidationFailed`, with the full result as `.result`.

## Next

[Handling deleted rows](deleted-rows.md): find and remove rows that the source
no longer has.
