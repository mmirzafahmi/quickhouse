# Handling deleted rows

**What you'll learn**

- Why an incremental sync never notices a deleted row, and why that is easy to
  miss.
- How to measure how far a destination has drifted from its source, with
  `reconcile_keys()`.
- How to remove the rows the source no longer has, with guards against a bad
  delete.
- How to find rows whose values drifted, with `reconcile_values()`.
- How to keep recent rows in step on every sync, with `delete_stale_in_window`.

**Before you start:** read [Keeping tables up to date](incremental.md). The
examples use the `stock_moves` table from
[Set up a practice environment](setup.md).

## The task

Daily Grind's inventory system records every stock movement, such as a delivery
to a store or a transfer between stores, as a row in `stock_moves`. When a
movement is cancelled, the system deletes its row. Store managers have started
asking why the stock dashboard still shows deliveries that were cancelled days
ago.

## Why deleted rows stay

Set up an incremental sync of `stock_moves` into ClickHouse and run it:

```python
import quickhouse as qh

src = qh.MySQL("mysql://etl:etl@localhost:3306/etl")
dst = qh.ClickHouse("http://localhost:8123", database="analytics")


def sync_stock_moves():
    return qh.sync(
        src, dst,
        source_table="stock_moves",
        dest_table="stock_moves_live",
        mode="incremental",
        watermark="write_date",
        key=["id"],
        tinyint1_as_bool=False,  # priority holds numbers, not true/false
    )


print(sync_stock_moves().rows_written)   # 1000
```

Now cancel three movements in the inventory system, and sync again:

```bash
docker compose exec mysql mysql -uetl -petl etl -e "DELETE FROM stock_moves WHERE id IN (120, 480, 870)"
```

```python
print(sync_stock_moves().rows_written)   # 0
```

Nothing to copy. Compare the two sides:

```bash
docker compose exec mysql mysql -uetl -petl etl -e "SELECT COUNT(*) FROM stock_moves"
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM analytics.stock_moves_live FINAL"
```

The inventory system has 997 movements, but the dashboard still has 1,000. An
incremental sync asks the source one question: "which rows have a `write_date`
later than last time?" A deleted row isn't in the source anymore, so it can
never be part of the answer. Its copy in ClickHouse stays forever, and every
cancellation adds another one.

It's also easy to miss, because deleted rows don't throw every number off by
the same amount. On one measured production table, the destination had 1.30%
more rows than its source, but its total quantity was off by only 0.0168%.
Every report that counted rows was noticeably wrong, while every report that
added up quantities looked fine.

## The simplest fix: a full refresh

For a small table, you don't need the rest of this page. A full refresh copies
the table from scratch, so deleted rows simply aren't in the new copy (see
[When to use a full refresh](first-sync.md#when-to-use-a-full-refresh)). Pass
`allow_full_refresh_shrink=True`, because deleted rows make the table shrink.

For a big table, copying everything on every run is too slow. The alternative
takes three steps: measure, repair, prevent.

## Step 1: measure the drift

`reconcile_keys()` compares the keys on both sides and reports the difference.
It changes nothing unless you ask it to.

```python
import datetime as dt

end = dt.date.today() + dt.timedelta(days=1)
start = end - dt.timedelta(days=30)
window = f"create_date >= '{start}' AND create_date < '{end}'"

report = qh.reconcile_keys(
    src, dst,
    dest_table="stock_moves_live",
    source_table="stock_moves",
    key="id",
    window=window,
)
print(report)
print(report.orphan_sample)
```

```text
ReconcileResult(source_keys=703, dest_keys=705, orphan_keys=2, missing_keys=0, rows_deleted=0, duration_secs=0.015)
['870', '480']
```

Your counts depend on the day you loaded the sample data. The fields mean:

| Field | Meaning |
| --- | --- |
| `source_keys`, `dest_keys` | How many keys each side has in the window. |
| `orphan_keys` | Keys the destination has and the source doesn't. This is the drift. |
| `missing_keys` | Keys the source has and the destination doesn't. |
| `orphan_sample`, `missing_sample` | Up to ten example keys of each, as text (`sample_limit=` changes how many). |
| `rows_deleted` | Rows removed. Always `0` unless you pass `delete=True`. |

The two orphans are movements 480 and 870, which you cancelled. Movement 120
isn't listed because it was created more than 30 days ago, outside the window:
a reconcile only sees what's inside its window. To check older history, run it
for earlier windows too, for example one month at a time.

`missing_keys` is the other direction. A few are normal: rows added since the
last sync, which the next sync copies. A large number means a load is
incomplete. That's a different problem, which you fix by re-syncing or
backfilling, never by deleting.

### Choosing the window and the key

The window is a SQL condition quickhouse adds to the query on each side:

- **Use a column that never changes**, like `create_date`, not `write_date`. A
  row that changed since the last sync has a different `write_date` on each
  side, so it could fall inside the window on one side and outside on the
  other, and look like drift that isn't there.
- **Keep it small.** quickhouse holds both lists of keys in memory while it
  compares them. For a big table, check one month at a time.
- **Write it in the source's SQL.** The destination gets the same condition
  unless you pass `dest_window=` in the destination's SQL, which you need when
  the column has a different name there. The date comparison above works in
  both MySQL and ClickHouse.

If your sync reads a `source_query`, pass the same query to `reconcile_keys()`
with `source_query=`, so both sides hold the same rows.

The key must be a single column, in practice an integer or text. quickhouse
compares keys as text, and some types come out differently from each database:
a timestamp is `2026-09-25 00:28:58` from MySQL but
`2026-09-25 00:28:58.000000` from ClickHouse, so no key would ever match. When
the two sides share no keys at all, quickhouse refuses to give a result rather
than report every row as drift.

## Step 2: remove the orphans

Once you've read the report and it makes sense, run it again with
`delete=True`:

```python
report = qh.reconcile_keys(
    src, dst,
    dest_table="stock_moves_live",
    source_table="stock_moves",
    key="id",
    window=window,
    delete=True,
    max_delete_keys=100,
)
print(report.orphan_keys, report.rows_deleted)   # 2 2
```

Movements 480 and 870 are gone from the dashboard. Deleting is the risky part,
so three guards stand in the way of a mistake:

- **A window is required.** Deleting every key the source lacks across the
  whole table is really a full refresh, and a less safe one, so quickhouse
  refuses.
- **`max_delete_keys` sets a ceiling.** If there are more orphans than this,
  nothing is deleted and you get an error saying so. Set it to a few times the
  drift you normally see, so a window that means something different on each
  side fails loudly instead of deleting half the table. The default, `0`, means
  no ceiling.
- **No keys in common means stop.** Real drift is a small difference. If the
  two sides share no keys at all, the key or the window is wrong, and
  quickhouse refuses whether or not you asked it to delete.

```{note}
`rows_deleted` counts rows and `orphan_keys` counts keys, so they can differ.
In a ClickHouse table filled by incremental syncs, an updated row can have two
copies for a while, the old version and the new one, until ClickHouse merges
them in the background. Deleting that key removes both copies.
```

### Run the measurement on a schedule

Measuring is safe, so run it regularly, and keep deleting as a step a person
approves. End a script like step 1 with an exit code, and your scheduler can
alert you when drift appears:

```python
import sys

print(f"{report.orphan_keys} orphan(s), {report.missing_keys} missing, e.g. {report.orphan_sample}")
sys.exit(1 if report.orphan_keys else 0)
```

The repository has a fuller version with command-line options for the window
and a `--repair` switch:
[`examples/reconcile_drift.py`](https://github.com/mmirzafahmi/quickhouse/blob/main/examples/reconcile_drift.py).
It's written for a PostgreSQL source; swap `qh.Postgres` for `qh.MySQL` to use
it with a MySQL one.

`reconcile_keys()` takes PostgreSQL, MySQL and ClickHouse sources, and either
destination. It refuses a BigQuery source, which is usually a copy itself, and
an HTTP API source, which has no list of keys to compare.

### Rows that are there but wrong

`reconcile_keys()` compares keys, so it can't see a row that both sides have
with different values. That happens when the source changes a row without
moving its watermark. Say someone corrects two quantities straight in the
inventory database and doesn't touch `write_date`:

```bash
docker compose exec mysql mysql -uetl -petl etl -e "UPDATE stock_moves SET qty = qty + 1 WHERE id IN (900, 901)"
```

The incremental sync never sees that change. `reconcile_values()` reads the
same window from both sides and compares every column they share, value by
value:

```python
values = qh.reconcile_values(
    src, dst,
    dest_table="stock_moves_live",
    source_table="stock_moves",
    key="id",
    window=window,
)
print(values)
for c in values.columns:
    if c.mismatched or c.null_in_dest_only:
        print(c.column, c.mismatched, c.null_in_dest_only, c.sample)
```

```text
ValueReconcileResult(source_rows=699, dest_rows=699, compared_keys=699, missing_keys=0, orphan_keys=0, duplicate_keys=0, columns=6, drifting_columns=1, duration_secs=0.052)
qty 2 0 ['900', '901']
```

Two `qty` values differ, on movements 900 and 901. `null_in_dest_only` counts
values that are `NULL` in the destination while the source has one, which is
what a sync that wrote a column as empty over real values leaves behind.
`reconcile_values()` only measures. To repair the rows, copy them again with a
one-off backfill (see [Backfilling history](incremental.md#backfilling-history)).
It reads PostgreSQL, MySQL and ClickHouse sources, and for now only a
ClickHouse destination.

## Step 3: keep recent rows in step on every sync

Measuring and repairing cleans up after the fact. For tables whose rows change
or get deleted while they're young, you can also have every sync remove them as
it goes, with `delete_stale_in_window=True`.

### How it works

quickhouse loads the run's rows into a staging table first. It finds the
smallest and largest value of one column among them, say `create_date`. That
range is the run's *window*. Any destination row whose `create_date` is inside
the window, but whose key isn't among the run's rows, is treated as deleted at
the source and removed. You name the column with `merge_prune_partition_by`.

This is only safe if every run reads **every** source row in its window. An
ordinary incremental sync on `write_date` doesn't: it reads only the rows that
changed. Suppose a movement from six weeks ago and one from today are both
edited. The run's window then spans six weeks of `create_date`, and every
movement created in those six weeks that *wasn't* edited would be deleted from
the destination. Before quickhouse 0.20.10, two edits like these deleted 989 of
1,000 rows that the source still had.

So quickhouse refuses that combination. Add the two options to the
`write_date` job from the start of this page, and the run stops before it
reads anything:

```python
qh.sync(src, dst, source_table="stock_moves", dest_table="stock_moves_live",
        mode="incremental", watermark="write_date", key=["id"],
        merge_prune_partition_by="create_date", delete_stale_in_window=True,
        tinyint1_as_bool=False)
```

```text
RuntimeError: stock_moves -> stock_moves_live: configuration error:
delete_stale_in_window needs watermark to be the merge_prune_partition_by column
'create_date', not 'write_date': ... Set watermark='create_date', a
lookback_seconds that covers how long a row can still change or be deleted, and
a state_key of its own ...
```

The safe set-up the error describes uses the window column as the watermark
too, with a lookback:

```python
result = qh.sync(
    src, dst,
    source_table="stock_moves",
    dest_table="stock_moves_live",
    state_key="stock_moves:create_date",
    mode="incremental",
    watermark="create_date",
    lookback_seconds=7 * 86400,
    key=["id"],
    merge_prune_partition_by="create_date",
    delete_stale_in_window=True,
    tinyint1_as_bool=False,
)
print(result.rows_written, "written,", result.rows_deleted, "deleted")
```

Each run now re-reads every movement created in the seven days before the
newest one it has seen. The run always holds the whole recent window, so a
movement missing from it really was deleted.

- **`state_key`** gives this job its own saved position, because it tracks a
  different watermark from the earlier job. With no saved position yet, the
  first run reads the whole table, so its window covers all of history and it
  also removes movement 120.
- **`lookback_seconds`** sets how far back the window reaches. Make it cover
  how long rows stay "young" in your system.

The first run prints `997 written, 1 deleted`. Now cancel two recent
movements:

```bash
docker compose exec mysql mysql -uetl -petl etl -e "DELETE FROM stock_moves WHERE id IN (990, 991)"
```

The next run removes them:

```text
165 written, 4 deleted
```

Four, for two movements? `rows_deleted` counts rows. The first run wrote every
movement again, and ClickHouse hadn't yet merged away the older copies, so each
movement had two rows. Depending on timing, you may see 2.

This pattern has a cost. Every run re-reads the last seven days, here 165 rows,
even when nothing changed. And an edit to a movement older than seven days is
no longer picked up, which is why [Keeping tables up to date](incremental.md)
warns against `created_at`-style watermarks in general. It suits tables like
`stock_moves`, whose rows are edited or cancelled within days and then left
alone. Keep `reconcile_keys()` running on a schedule for anything older.

### ClickHouse and BigQuery do it differently

- **ClickHouse** runs a separate `DELETE` just before it copies the rows from
  staging into the table. The two don't happen as one step, and `rows_deleted`
  reports exactly how many rows were removed.
- **BigQuery** does the delete inside the same `MERGE` that writes the rows, so
  both happen together or not at all. BigQuery reports one combined count for
  the statement, so `rows_deleted` stays `0`.

On BigQuery, `merge_prune_partition_by` also limits how much of the table each
`MERGE` reads, which is where its name comes from (see
[Loading into BigQuery](bigquery.md)). On either destination it must name a
column whose value never changes for a row.

Besides a `watermark` other than the window column, these combinations are
configuration errors, refused before anything is copied:

- `delete_stale_in_window` without `merge_prune_partition_by`, because the
  delete would have no window.
- `delete_stale_in_window` with `chunk_rows`, because each chunk holds only
  part of the window.
- `delete_stale_in_window` with `watermark_source_expr`, because quickhouse
  can't tell which column an expression reads.

A [DataFrame](dataframes.md) source is the one exception to the watermark rule,
since nothing filters its rows: the frame you pass must hold the whole window
itself.

## Putting it together

Daily Grind's inventory pipeline now runs like this:

- **Every 15 minutes:** the windowed sync from step 3, which removes
  cancellations from the last seven days as part of each run.
- **Every night:** the measurement from step 1 over the last three months, one
  month at a time, alerting the data team when it finds orphans.
- **When an alert fires:** someone reads the report and, if it makes sense,
  runs the repair from step 2 with a `max_delete_keys` ceiling.

The [sync modes guide](../guide/sync-modes.md#converging-with-a-source-that-deletes-rows)
has the reference details for both tools.

## Recap

- An incremental sync never sees deleted rows, so their copies stay in the
  destination forever.
- For a small table, use a full refresh with `allow_full_refresh_shrink=True`.
- `reconcile_keys()` measures drift inside a window on a column that never
  changes. It only deletes with `delete=True`, guarded by a required window,
  `max_delete_keys`, and a refusal when the two sides share no keys.
- `reconcile_values()` finds rows both sides have but whose values differ.
- `delete_stale_in_window=True` removes deleted rows on every sync. It's only
  safe when every run reads every row in its window, so quickhouse requires the
  window column as the watermark; add a lookback and a `state_key` of its own.

## Next

[Running in production](production.md): job files, schedulers, retries,
timeouts and housekeeping.
