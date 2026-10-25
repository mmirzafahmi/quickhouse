# Keeping tables up to date

**What you'll learn**

- How an incremental sync copies only new and changed rows.
- How to pick a good watermark column, and what goes wrong with a bad one.
- How to catch rows that arrive late.
- How to move from an old pipeline, and how to backfill history without
  disturbing your schedule.

**Before you start:** read [Your first sync](first-sync.md). The examples reuse
its `src` and `dst`.

## The task

Daily Grind takes thousands of orders an hour. Store managers want their
dashboard to be at most 15 minutes behind. Copying the whole `orders` table
every 15 minutes would work for a while, but the table grows every day, and
each copy would take longer and put more load on the ordering app's database.

What you want is to copy **only the orders that are new or have changed since
the last run**. That is an incremental sync.

## How it works

An incremental sync needs two things from you:

- A **watermark** column that goes up whenever a row is added or changed. The
  `orders` table has `updated_at`, which the app sets to the current time on
  every change.
- A **key** that identifies each row, here `id`. When an order changes,
  quickhouse uses the key to replace the old version of that row instead of
  keeping both.

After each run, quickhouse remembers the highest watermark value it copied. The
next run asks the source only for rows above that value.

## Your first incremental sync

```python
result = qh.sync(
    src, dst,
    source_table="orders",
    dest_table="orders_live",
    mode="incremental",
    watermark="updated_at",
    key=["id"],
)
print(result.rows_written, result.new_watermark)
```

```text
50000 2026-10-24 08:26:30.831814+00
```

The first run has nothing to remember yet, so it copies every row.
`new_watermark` is the highest `updated_at` it saw. Run the same code again:

```text
0 2026-10-24 08:26:30.831814+00
```

Nothing has changed, so nothing is copied. Running it twice is harmless, which
is exactly what you want from a job that runs on a schedule.

Now change the source. Refund one order and take a new one:

```sql
UPDATE orders SET status = 'refunded', updated_at = now() WHERE id = 49999;
INSERT INTO orders VALUES (50001, 2, 42, 'paid', 4.50, now(), now());
```

Run the sync a third time:

```text
2 2026-10-24 08:26:46.475482+00
```

Two rows: the refunded order and the new one. That is all an incremental sync
reads, however big the table gets.

## Where quickhouse keeps its place

quickhouse stores the watermark in a small table called `_quickhouse_state`,
which it creates in the destination database. You can look at it:

```sql
SELECT source_table, dest_table, last_watermark, rows, run_ts
FROM analytics._quickhouse_state
ORDER BY run_ts DESC
```

Each run adds a row. The newest row for a table is the one the next run uses.
The watermark is only saved when a run succeeds, so a failed run is simply
tried again from the same place next time.

The saved position belongs to the sync job, not to the destination table, so
dropping the table doesn't remove it. quickhouse notices when a run has to
create the table while a position is still saved. It reads from the start, as
a first run does, so the new table gets every row, and it adds a
`recreated_destination` warning to the result. If you drop `orders_live`, the
next run copies every order again, and the warning's `message` says why:

```text
destination 'orders_live' does not exist, so this run creates it, but state_key
'orders' still holds the cursor 2026-10-25 06:07:46.870989+00 from the table it
replaces. Starting past it would fill the new table with only the rows after
it, so this run reads from the start, as a first run does, and clears it. To
start past it instead, pass seed_watermark='2026-10-25 06:07:46.870989+00'.
```

## Seeing each order once in ClickHouse

Count the orders in ClickHouse right after that third run:

```sql
SELECT count() FROM analytics.orders_live          -- 50002
SELECT count() FROM analytics.orders_live FINAL    -- 50001
```

Why two different numbers? For an incremental sync, quickhouse creates the
ClickHouse table with the `ReplacingMergeTree` engine. When the refunded order
arrives, ClickHouse stores it next to the old version, and removes the old one
later, in the background. Until then, both versions are there.

Adding `FINAL` after the table name tells ClickHouse to remove old versions
while it answers the query, so you always see one row per order:

```sql
SELECT status, count() AS orders, sum(total) AS revenue
FROM analytics.orders_live FINAL
GROUP BY status
```

```{tip}
Use `FINAL` in dashboards and reports that read incrementally-synced ClickHouse
tables. [Designing ClickHouse tables](clickhouse-tables.md) explains the engine
in more detail.
```

**BigQuery works differently.** It has no engine that removes old versions, so
quickhouse loads the changed rows into a staging table and runs a `MERGE`
statement that updates matching rows and inserts new ones. You always see one
row per key, with no `FINAL` needed. Because the `MERGE` matches on the key,
`key` is required for incremental syncs into BigQuery.

## Choosing a good watermark

The watermark decides which rows each run sees, so a poor choice means missing
data. A good watermark column:

- **Changes on every insert and every update.** `updated_at` usually does.
  `created_at` doesn't: an order refunded tomorrow keeps today's `created_at`,
  so the refund would never be copied.
- **Is never empty (`NULL`).** A row with a `NULL` watermark never matches
  "greater than the last value", so it is skipped on every run, forever.
- **Has an index** on the source. Each run filters on this column, and without
  an index the database scans the whole table every time. See
  [Big tables and busy databases](performance.md).

quickhouse checks the second point for you. Suppose `updated_at` allowed empty
values and 10 rows had none. The run would still succeed, but the result would
carry a `null_watermark` warning:

```python
for w in result.warnings:
    print(w.kind, w.column, w.count)
```

```text
null_watermark updated_at 10
```

Treat this warning as a bug to fix: either make the column `NOT NULL` in the
source, or load those rows separately. [Catching bad data](data-quality.md)
shows how to make a run fail on it instead.

### Tables that are never updated

Some tables only ever get new rows, such as a log of loyalty-card scans. There,
an increasing `id` makes a fine watermark:

```python
qh.sync(src, dst, source_table="card_scans", dest_table="card_scans",
        mode="incremental", watermark="id", key=["id"])
```

## Catching late rows

Daily Grind's airport store loses its internet connection now and then. The
till keeps taking orders and uploads them when it reconnects, hours later, with
the `updated_at` of when each order was taken. By then the sync's watermark has
moved past those times, so the late orders are never copied.

`lookback_seconds` fixes this. Each run starts reading a little *before* the
saved watermark, so rows that arrived late inside that window are picked up:

```python
qh.sync(
    src, dst,
    source_table="orders",
    dest_table="orders_live",
    mode="incremental",
    watermark="updated_at",
    key=["id"],
    lookback_seconds=3 * 86400,   # re-read the last 3 days on every run
)
```

Re-reading means some rows are copied more than once. That's fine, because the
key makes each copy replace the previous one instead of adding a duplicate.
For the same reason, quickhouse refuses `lookback_seconds` without a `key` (or
an `order_by`).

The cost is extra reading. In the sample data, a run with nothing new still
re-reads about 4,000 rows from the last three days. Pick the smallest window
that covers how late your data really arrives. A lookback needs a date or
timestamp watermark.

## Keeping a stable identity with state_key

quickhouse saves the watermark under a name built from the source and
destination tables. With a `source_query`, it uses the query's text instead.
That has a surprising side effect: **if you edit the query, quickhouse thinks
it's a new job and copies everything again from the start.**

Give the job its own fixed name with `state_key`, and you can edit the query
freely:

```python
qh.sync(
    src, dst,
    dest_table="paid_orders",
    mode="incremental",
    watermark="updated_at",
    key=["id"],
    source_query="SELECT * FROM orders WHERE status = 'paid'",
    state_key="paid_orders:updated_at",
)
```

You also need `state_key` when two syncs write into the same destination table
but track different watermark columns. Without distinct names they would
overwrite each other's saved position.

```{note}
The watermark column must be one of the columns the query returns. Here
`SELECT *` includes `updated_at`.
```

## Starting from now

Sometimes copying all of history on the first run is wasted work:

- You are replacing an older script that has already loaded the history into
  ClickHouse.
- A new dashboard only needs orders from now on.

`skip_to_max=True` makes the first run start from the newest row in the
source. It copies almost nothing and saves the source's current highest
watermark:

```python
result = qh.sync(src, dst, source_table="orders", dest_table="orders_recent",
                 mode="incremental", watermark="updated_at", key=["id"],
                 skip_to_max=True)
print(result.rows_written, result.new_watermark)
```

```text
0 2026-10-24 08:26:46.475482+00
```

If you know the old script loaded everything up to a certain point, start
there instead with `seed_watermark`. The first run then copies only the rows
newer than that value:

```python
qh.sync(src, dst, source_table="orders", dest_table="orders_recent",
        mode="incremental", watermark="updated_at", key=["id"],
        seed_watermark="2026-10-20 00:00:00+00")   # when the old script last ran
```

Both options only act on the very first run, when no watermark is saved yet.
After that they are ignored, so you can leave them in your code. You can use
one or the other, not both.

```{warning}
Taking over an existing ClickHouse table only works if that table replaces old
versions of a row: a `ReplacingMergeTree`, like the tables quickhouse creates
for incremental syncs. A table created by a full refresh, such as
`analytics.orders` from [Your first sync](first-sync.md), is a plain
`MergeTree`, which keeps every copy, so each changed order would appear twice.
Check with `SELECT engine FROM system.tables WHERE name = 'orders'`. Run into
such a table, quickhouse still writes the rows, but it adds a
`non_replacing_incremental_target` warning to the result, with the engine
(`MergeTree`) as its `sample`. To refuse such a run instead, pass
`fail_on_warnings={"non_replacing_incremental_target"}` (see
[Catching bad data](data-quality.md)). To fix the table, drop it and let the
next run create a `ReplacingMergeTree`.
[Designing ClickHouse tables](clickhouse-tables.md) explains the engines.
BigQuery tables don't have this problem, because the `MERGE` matches rows on
the key.
```

## Backfilling history

Now the opposite problem. `orders_recent` started from now, and finance asks
for the older orders too. You want to load them into the same table without
moving the regular job's saved position.

Run a separate sync with `advance_watermark=False`:

```python
result = qh.sync(
    src, dst,
    dest_table="orders_recent",
    mode="incremental",
    watermark="updated_at",
    key=["id"],
    source_query="""
        SELECT * FROM orders
        WHERE created_at < now() - interval '14 days'
    """,
    state_key="orders_recent:backfill",
    advance_watermark=False,
)
print(result.rows_written, result.new_watermark)
```

```text
29868 2026-10-10 08:57:30.831814+00
```

Your numbers will differ, because the sample data is dated from the moment you
loaded it. What happens:

- The backfill has its own `state_key`, which has no saved position, so it
  reads every row the query returns: here, every order older than two weeks.
- The rows go into `orders_recent` and replace any existing versions by `id`,
  like any incremental run.
- `advance_watermark=False` means nothing is saved. `new_watermark` is still
  reported so you can see it, but the regular job carries on exactly where it
  was. Run the `skip_to_max` sync again and it copies 0 rows.

For a very large backfill, split the date range into smaller pieces, for
example one month per run, and run them one after another. On a replica that
cancels long queries, `window_bounds` can split a range of ids for you (see
[Big tables and busy databases](performance.md#asking-for-windows-yourself)).

## What an incremental sync doesn't do

An incremental sync adds and updates rows. It never removes them. If the source
deletes a row, that row has no watermark to change, so the destination keeps it
forever. For tables where rows get deleted, see
[Handling deleted rows](deleted-rows.md).

## Recap

- `mode="incremental"` with a `watermark` and a `key` copies only new and
  changed rows. Running it again with no changes does nothing.
- The saved position lives in `_quickhouse_state` and only moves when a run
  succeeds.
- In ClickHouse, query with `FINAL` to see one row per key. BigQuery merges on
  the key and needs no `FINAL`.
- Choose a watermark that changes on every update, is never `NULL`, and is
  indexed.
- `lookback_seconds` catches late rows. `state_key` keeps a job's identity
  stable. `skip_to_max` and `seed_watermark` set the first run's starting point.
  `advance_watermark=False` backfills without moving the schedule.

## Next

[Reading from databases](databases.md): connect securely to PostgreSQL and
MySQL, and read from BigQuery and ClickHouse.
