# Designing ClickHouse tables

**What you'll learn**

- What kind of table quickhouse creates in ClickHouse, and why.
- How to choose a sort order and partitions that make dashboards fast.
- How ClickHouse removes old versions of a row, and how to read a table so you
  never count a row twice.
- How to add a new column, and how to tune inserts to a remote server.

**Before you start:** read [Your first sync](first-sync.md) and
[Keeping tables up to date](incremental.md). The examples use the practice
environment from [Set up a practice environment](setup.md).

## The task

The store managers' dashboard is popular, and it's getting slower. Almost every
chart asks the same kind of question: "how did *my* store do *this month*?" The
`orders` table in ClickHouse was created with quickhouse's defaults, which
don't know that. In this tutorial you'll design the table around the questions
the dashboard asks.

## The table quickhouse creates for you

When the destination table doesn't exist, quickhouse writes the `CREATE TABLE`
statement for you. Every ClickHouse table has an **engine**, which decides how
the data is stored, and a **sort order** (`ORDER BY`), which decides the order
rows are kept in on disk.

For a full refresh, quickhouse uses `MergeTree`, ClickHouse's standard engine,
sorted by `key`. With no key, as for the `stores` table in your first sync, the
table has no sort order at all: `ORDER BY tuple()`.

For an incremental sync, it picks a different engine. You can see the
statement with `SHOW CREATE TABLE`:

```bash
docker compose exec clickhouse clickhouse-client --query "SHOW CREATE TABLE analytics.orders_live FORMAT TSVRaw"
```

```text
CREATE TABLE analytics.orders_live
(
    `id` Int64,
    `store_id` Int32,
    `customer_id` Nullable(Int64),
    `status` String,
    `total` Nullable(Decimal(12, 2)),
    `created_at` Nullable(DateTime64(6, 'UTC')),
    `updated_at` DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id
```

`ReplacingMergeTree(updated_at)` is the engine that lets ClickHouse replace an
old version of a row with a newer one. The next section shows how it works.

```{note}
**Why are some columns `Nullable`?** In the source, `total` and `created_at`
are `NOT NULL`. quickhouse still makes date, timestamp and decimal columns
nullable, because a value ClickHouse can't store (a date outside the years 1900
to 2299, or a number too big for its decimal type) is saved as `NULL` with a
warning instead of failing the whole run. Columns that organise the table, the
sort order and the version column (`updated_at` here), are never nullable.
```

## How ClickHouse replaces old versions

A `ReplacingMergeTree` treats two rows as **the same row** when they have the
same values in every `ORDER BY` column. Here that is `id`. When it finds two
versions of the same row, it keeps the one with the highest `updated_at` (the
column in brackets, called the **version column**).

The catch is *when* that happens. ClickHouse doesn't check for old versions
when rows arrive. It removes them later, in the background, when it **merges**
the table's files together. Until then, both versions are in the table.

You can watch this happen. Refund three orders in PostgreSQL, then run the
incremental sync from [Keeping tables up to date](incremental.md) again:

```bash
docker compose exec postgres psql -U etl -d etl -c \
  "UPDATE orders SET status = 'refunded', updated_at = now() WHERE id IN (10, 20, 30)"
```

Straight after the sync, look at one of those orders, first as stored, then
with `FINAL`:

```sql
SELECT id, status, updated_at FROM analytics.orders_live WHERE id = 10;
SELECT id, status, updated_at FROM analytics.orders_live FINAL WHERE id = 10;
```

```text
10   paid       2026-09-19 15:16:30.831814
10   refunded   2026-10-24 08:33:08.434740

10   refunded   2026-10-24 08:33:08.434740
```

The word `FINAL` after the table name tells ClickHouse to finish the job while
it answers your query, so you only see the newest version of each row. Some
time later the background merge catches up, and the plain query returns the
same answer. You can't predict when, so **use `FINAL` whenever you read a
`ReplacingMergeTree` table**, in dashboards, reports and anywhere else.

```{note}
The same applies when you *copy* the table somewhere else, and quickhouse
handles it for you. When this table is a quickhouse `source_table`, quickhouse
sees that it's a `ReplacingMergeTree` and reads it with `FINAL`, so an order
whose old version hasn't been removed yet is copied once. If you read it
through a `source_query` instead, write `FINAL` in the query yourself:
`source_query="SELECT * FROM orders_live FINAL"`.
```

### Why an incremental table needs a key

Without `key` (or `order_by`), the sort order would be empty, and every row
would count as "the same row". The first background merge would then shrink the
whole table to a single row. quickhouse refuses to create such a table, with an
error that says `engine ReplacingMergeTree needs a sorting key`.

## Choosing the sort order

The sort order is the most important design decision for a ClickHouse table.
ClickHouse stores rows in that order, and keeps a small index of it. When a
query filters on the first columns of the sort order, ClickHouse can skip most
of the table instead of reading all of it.

The dashboard always filters by store, then by date. So sort by those, and add
`id` at the end to keep each row's place unique. (Options like this only apply
when quickhouse creates the table, so to try each example below, first run
`DROP TABLE analytics.orders`.)

```python
qh.sync(
    src, dst,
    source_table="orders",
    dest_table="orders",
    mode="full",
    key=["id"],
    order_by=["store_id", "created_at", "id"],
    primary_key=["store_id", "created_at"],
)
```

```text
ENGINE = MergeTree
PRIMARY KEY (store_id, created_at)
ORDER BY (store_id, created_at, id)
```

- `order_by` sets the sort order. When you leave it out, quickhouse uses `key`.
- `primary_key` is optional. It tells ClickHouse to index only the first few
  sort order columns, which keeps the index small on very large tables. It must
  be the leading columns of `order_by`.

### Sort order on an incremental table

Be careful with `order_by` on a `ReplacingMergeTree`. Two rows are "the same
row" only when **every** sort order column matches. If one of those columns can
change, the new version no longer matches the old one, and the old version is
never removed.

Here is order 100 in a table synced with `order_by=["status", "id"]`, after the
order was refunded, read **with** `FINAL`:

```text
100   paid       2026-09-19 16:46:30.831814
100   refunded   2026-10-24 08:38:17.806527
```

The order is now counted twice, forever. For an incremental table, build the
sort order only from the key and columns that never change for a row, such as
`store_id` and `created_at`.

## Splitting a table into partitions

A **partition** is a separate group of files for part of the table, such as one
month of orders. Partitions help in two ways: a query for October skips the
other months entirely, and deleting an old month is quick and cheap.

Use `partition_by` with a ClickHouse expression. Monthly partitions on
`created_at`:

```python
qh.sync(
    src, dst,
    source_table="orders", dest_table="orders", mode="full", key=["id"],
    partition_by="toYYYYMM(created_at)",
    not_null=["created_at"],
)
```

Why `not_null`? Remember the note above: `created_at` is a timestamp, so
quickhouse would make it `Nullable`, and ClickHouse won't partition by a
nullable column. Without `not_null`, the run fails with
`Partition key contains nullable columns`. `not_null` tells quickhouse the
column is never empty.

```text
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY id
```

Only use `not_null` for a column that really never holds `NULL`. If one turns
up there anyway, or in an `order_by` column, the row is still written, with the
column type's default (`''`, `0`, `1970-01-01`) in place of the `NULL`, and the
run raises a `null_defaulted` warning naming the column. Name it in
`fail_on_warnings` to stop the run instead
([Catching bad data](data-quality.md)).

```{tip}
Keep partitions large: a month suits most tables. Partitioning by day or hour
creates many small groups of files, which makes ClickHouse slower, not faster.
```

## Choosing a different engine

Pass `engine` to use a different ClickHouse engine. quickhouse writes it into
the `CREATE TABLE` exactly as you give it.

The most common reason is a table whose rows never change, like a log of card
payments. There is nothing to replace, so a plain `MergeTree` is simpler and
keeps every row:

```python
qh.sync(
    src, dst,
    source_table="payments", dest_table="payments",
    mode="incremental", watermark="created_at", key=["id"],
    engine="MergeTree",
)
```

Only do this when source rows are never updated. With a plain `MergeTree`, an
updated row is stored a second time, and both versions stay.

On **ClickHouse Cloud**, keep the default engines: Cloud replicates them for
you. On a cluster you run yourself, note that quickhouse creates tables on the
one server it connects to, without `ON CLUSTER`.

## Changing an existing table

The design options on this page (`order_by`, `primary_key`, `partition_by`,
`engine`, `not_null`, `type_overrides`, and `key`'s part in the sort order) only
shape the table when quickhouse **creates** it. An existing table keeps its
structure, even on a full refresh: running again with a different `order_by`
changes nothing.

To redesign a table, drop it (`DROP TABLE analytics.orders`) and let the next
sync create it again.

An incremental table works the same way. Dropping it doesn't remove the
watermark quickhouse saved for it, but a run that has to create the table while
a watermark is saved reads the whole source again, as a first run does, instead
of only the rows changed since the last run. The result carries a
`recreated_destination` warning whose `sample` is the old watermark. The job
keeps its `state_key`; there is nothing to rename.

### When the source gets a new column

Suppose the ordering app adds a `channel` column that says whether an order came
from the counter or the app. quickhouse never changes an existing table's
columns unless you allow it with `evolve_schema=True`:

```python
qh.sync(
    src, dst,
    source_table="orders", dest_table="orders_live",
    mode="incremental", watermark="updated_at", key=["id"],
    evolve_schema=True,
)
```

quickhouse adds the column to the ClickHouse table, as `Nullable`, and logs:

```text
evolve_schema: added 1 column(s) to 'orders_live': channel
```

It only ever adds columns. It never removes a column or changes its type.

Without `evolve_schema=True`, a sync into ClickHouse succeeds and leaves the
new column out, but it tells you. The result carries a `dropped_source_column`
warning whose `column` is `channel`, and whose `message` reads:

```text
source column 'channel' has no column in 'orders_live' to land in, and with
evolve_schema=False every row this run writes leaves it out. Turning
evolve_schema on later adds the column but leaves it NULL on the rows this run
writes, which a later run writes again only if they change. Pass
evolve_schema=True to add the column, or exclude it if the destination doesn't
need it.
```

```{warning}
Don't let that warning slide. On an incremental table, the rows read by that
run keep no value for the new column, even after you turn `evolve_schema` on.
In a test, two orders that changed to `app` in a run without `evolve_schema`
still held `NULL` in `channel` after the next run added the column. If you
think new columns are likely, turn `evolve_schema` on from the start, or stop
the run with `fail_on_warnings={"dropped_source_column"}` (see
[Catching bad data](data-quality.md)), which leaves the table and the saved
position untouched.
```

Rows synced before the column was added hold `NULL` in it. To fill them in,
read the history again with a one-off backfill, as described in
[Backfilling history](incremental.md#backfilling-history).

## Talking to a remote server

These options go on the `ClickHouse(...)` destination, not on `sync()`.

**Insert compression.** quickhouse compresses the data it sends with each
insert. The default, `zstd`, makes the data much smaller for a modest amount of
CPU work, which suits a slow or distant link such as ClickHouse Cloud over the
internet. When ClickHouse is on the same machine or a fast local network, the
network isn't the bottleneck, so `lz4` (faster, less compression) or `none` can
be quicker:

```python
dst = qh.ClickHouse("http://clickhouse.internal:8123", database="analytics",
                    compression="lz4")
```

Avoid `gzip`, which is about ten times slower than `zstd` for little gain (see
the [measured numbers](../guide/destinations/clickhouse.md#insert-compression)).

**ClickHouse settings.** `settings` sends any ClickHouse setting with every
request quickhouse makes. On ClickHouse Cloud, for example, the check that reads
the table right after a full refresh can land on a replica that hasn't caught
up yet. This setting makes reads wait for the latest data:

```python
dst = qh.ClickHouse("https://abc123.clickhouse.cloud:8443", database="analytics",
                    password=os.environ["CH_PASSWORD"],
                    settings={"select_sequential_consistency": "1"})
```

A misspelled setting name fails the run straight away with `UNKNOWN_SETTING`.

**Insert deduplication tokens.** If an insert's reply is lost on the network,
quickhouse retries it, and rarely ClickHouse had already saved the first
attempt, so the batch lands twice. `insert_dedup_token=True` lets a replicated
ClickHouse table spot and drop the repeat. It is off by default for a good
reason: ClickHouse deduplicates per block of rows, not per request, and in some
cases it can wrongly drop real rows. Only turn it on after testing it on your
own ClickHouse version and table, comparing row counts. A `ReplacingMergeTree`
table doesn't need it, because it removes duplicates anyway.

## Recap

- A full refresh creates a `MergeTree` table; an incremental sync creates a
  `ReplacingMergeTree(<watermark>)`. Both are sorted by `order_by`, else `key`.
- A `ReplacingMergeTree` removes old versions in the background. Read it with
  `FINAL`, and sort it only by columns that never change.
- Put the columns your queries filter on first in `order_by`.
- `partition_by` takes an expression such as `toYYYYMM(created_at)`; add the
  column to `not_null` if quickhouse made it nullable.
- Design options only apply when the table is created, so drop the table to
  change them; an incremental sync then starts over by itself. A new source
  column raises `dropped_source_column` until you allow it with
  `evolve_schema=True`.

## Next

[Loading into BigQuery](bigquery.md): credentials, partitioning, clustering,
and keeping `MERGE` costs down.
