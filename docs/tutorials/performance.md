# Big tables and busy databases

**What you'll learn**

- How to find out where a slow sync spends its time.
- How quickhouse reads a table in parallel, and how to keep memory in check.
- How to read from a busy production database without slowing it down.
- What to do when the watermark column has no index, or a replica cancels long
  queries, including during a backfill.
- How to make a very large load resumable.

**Before you start:** read [Keeping tables up to date](incremental.md) and
[Running in production](production.md).

## The task

Two years on, Daily Grind's `orders` table has hundreds of millions of rows.
The ordering app's database is busy at lunchtime, the data team reads from a
replica that cancels long queries, and the sync runs in a small container. The
syncs have to stay quick without slowing down the app or running out of memory.

## Find out where the time goes

Measure before you change any setting. Every `TransferResult` splits the run
into phases:

```python
result = qh.sync(src, dst, source_table="orders", dest_table="orders",
                 mode="full", key=["id"])
print(f"total {result.duration_secs:.2f}s  read {result.read_secs:.2f}s  "
      f"stage {result.stage_secs:.2f}s  promote {result.promote_secs:.2f}s")
```

```text
total 0.14s  read 0.08s  stage 0.10s  promote 0.01s
```

- `stage_secs` is the copying phase: reading, converting and writing every row.
- `read_secs` is the part spent waiting for the source to send rows. It adds up
  the waiting of every parallel reader, so divide it by the number of readers
  before comparing it with `stage_secs`.
- `promote_secs` is the final step: the swap, the `MERGE`, saving the
  watermark.

If `read_secs` divided by the readers is close to `stage_secs`, the source is
the slow part: look at parallel reads and indexes below. If `promote_secs` is
most of the run, the destination's last step is the slow part; on BigQuery
that is usually the `MERGE` (see [Loading into BigQuery](bigquery.md)). If
`stage_secs` is high but `read_secs` is low, the time goes into converting and
writing rows (see [Faster writes into ClickHouse](#faster-writes-into-clickhouse)).

## Read in parallel

By default, quickhouse opens one connection per CPU it may use, container
limits included. It splits the table into ranges of an integer column and
reads each range over its own connection. Check the log to see what it decided:

```text
INFO parallelism derived from the host: 2 CPU(s) available
INFO resolved 7 source column(s); computed 2 partition(s) for parallel read
```

Set `parallelism` yourself to use more or fewer connections. quickhouse splits
on `partition_column` if you set it, and otherwise on the first `key` column.
**That column must hold integers**, or the whole table is read over one
connection and the log shows `computed 1 partition(s)`. For example, a key that
starts with a text column needs an integer column named for the split:

```python
qh.sync(src, dst, source_table="orders", dest_table="orders_by_status",
        mode="full", key=["status", "id"], partition_column="id", parallelism=8)
```

Parallel reads work for PostgreSQL, MySQL and ClickHouse sources. For a
BigQuery source, `parallelism` is a hint, and BigQuery does the parallel work
on its side.

More connections aren't always faster. On a 2-CPU machine that also ran the
database and ClickHouse, every setting from 1 to 16 connections took about 25
seconds, because they all shared the same two cores. Parallel reads help when
the source has spare CPU, and each one costs memory, as shown below.

### Parallel reads from a query

With `source_query`, quickhouse can't see which column to split on, so it reads
in one stream and logs `parallelism=2 is inert for this transfer`. Name the
column of the query's result to split on with `partition_source_expr`:

```python
qh.sync(
    src, dst,
    source_query="""
        SELECT id, store_id, status, total, created_at
        FROM orders
        WHERE status = 'paid'
    """,
    dest_table="paid_orders",
    mode="full",
    key=["id"],
    partition_source_expr="id",
)
```

If the query changes the key, for example `id::text AS order_ref`, also return
the original column under another name (`id AS id_raw`), and pass
`partition_source_expr="id_raw"` with `exclude=["id_raw"]` so it isn't copied.
quickhouse finds the ranges by running `MIN` and `MAX` over your query, so a
query with joins does a little extra work each run. A missing or non-integer
column is an error, not a silent fall-back to one stream.

## Keep memory under control

Memory use doesn't grow with the size of the table. It grows with the number of
parallel readers. Three settings control it:

- `max_memory_bytes` (default 512 MiB) caps the data waiting between reading
  and writing. When it's reached, reading pauses until writes catch up. Each
  reader also needs some working memory on top.
- `max_memory_fraction` sets that cap as a share of the memory the container
  is allowed, so the same job fits containers of different sizes.
- `insert_bytes` (default 32 MiB) is how much each reader collects before it
  sends an insert. ClickHouse prefers fewer, bigger inserts.

```python
qh.sync(src, dst, source_table="orders", dest_table="orders",
        mode="full", key=["id"],
        max_memory_fraction=0.25,      # a quarter of the container's memory
        insert_bytes=8 * 1024 * 1024)  # smaller inserts, less memory per reader
```

As a rough guide, one measured 6-million-row table peaked at about 300 to
390 MB with 1 reader, 490 to 540 MB with 4, and 850 to 930 MB with 16. To use
less memory, lower `insert_bytes` or `max_memory_bytes` before you lower
`parallelism`. The [Performance & safety guide](../guide/performance.md#what-memory-scales-with)
has the full table.

## Be gentle with a busy database

The ordering app's database serves customers first. To keep a sync from
competing with it, combine these:

```python
src = qh.Postgres("postgresql://etl:etl@orders-replica.internal:5432/etl")

qh.sync(
    src, dst,
    source_table="orders",
    dest_table="orders_live",
    mode="incremental",           # read only new and changed rows
    watermark="updated_at",
    key=["id"],
    parallelism=1,                # one connection, one scan
    read_max_rows_per_sec=50_000, # cap the read speed
    application_name="quickhouse-orders",
)
```

- **Read from a replica** when you have one, so the app's main database does no
  extra work.
- **`read_max_rows_per_sec`** caps the rows read per second, in total across
  all connections. In a test, 300,000 rows capped at 100,000 per second took
  2.9 seconds. The database can only send rows as fast as quickhouse takes
  them, so on a large table its own work slows down too. It applies to
  PostgreSQL, MySQL and ClickHouse sources, not BigQuery.
- **`application_name`** is the name the connection shows in PostgreSQL's
  `pg_stat_activity` (default `quickhouse`), so your database administrator can
  see which job is running, and stop it if needed.

## When the watermark column has no index

Before copying anything, an incremental run asks the source for the highest
watermark and, if the column allows `NULL`, how many rows have none. With an
index on the column, both answers are instant. Without one, each scans the
whole table, on every run: on one real 14.9 GB table, almost a minute each.

quickhouse asks the database what each question would cost (`EXPLAIN`, which
runs nothing). Above `probe_max_cost` (50,000 by default) it skips the question
and warns. To see it on the practice database, drop the index and lower the
threshold, because the practice table is too small to cross the default:

```sql
DROP INDEX orders_updated_at_idx;
```

```python
result = qh.sync(src, dst, source_table="orders", dest_table="orders_live",
                 mode="incremental", watermark="updated_at", key=["id"],
                 probe_max_cost=100)
print([w.kind for w in result.warnings])   # ['unindexed_watermark']
```

What quickhouse does instead:

- **It still looks up the highest watermark** unless you set
  `lookback_seconds`. With a lookback, it takes the watermark from the rows it
  read instead, and the lookback covers rows written during the read.
- **It reads the table in slices of the key**, so the read still finishes on a
  replica that cancels long queries (see the next section).
- **On a column that allows `NULL`, it skips the count too** and warns with
  `null_check_skipped`, because it can no longer tell you whether rows are being
  missed. Make the column `NOT NULL`, or set `probe_max_cost=0` to always pay
  for the check.

The real fix is the index. Create it without locking the table, run the sync
again, and the warning is gone:

```sql
CREATE INDEX CONCURRENTLY ON orders (updated_at);                              -- PostgreSQL
ALTER TABLE stock_moves ADD INDEX (write_date), ALGORITHM=INPLACE, LOCK=NONE;  -- MySQL
```

## Replicas that cancel long queries

A PostgreSQL read replica (a "hot standby") cancels queries that hold up its
replication for longer than its `max_standby_streaming_delay` setting, with
`SQLSTATE 40001, canceling statement due to conflict with recovery`. Retrying a
long scan doesn't help: each attempt starts the same scan from zero and is
cancelled again. What helps is making each query short.

When the watermark filter would scan the whole table (the
`unindexed_watermark` case above), quickhouse switches to a **windowed read**.
It reads the table in ranges of the key column, one short query per range, and
resizes the ranges as it goes so each takes about `window_target_secs` (5 seconds
by default). A window that is cancelled anyway is tried again at half the size:

```text
INFO windowed read on "id" over [1, 300000], starting width 50000 (ceiling 5000000, target 5.0s)
INFO windowed read complete: 5 window(s), 0 shrink(s)
```

Keep the target well under the replica's limit, and cap the width of a window
with `read_window_rows` if you need to:

```python
qh.sync(src, dst, source_table="orders", dest_table="orders_live",
        mode="incremental", watermark="updated_at", key=["id"],
        lookback_seconds=3600, window_target_secs=3.0,
        read_window_rows=1_000_000, retry_max_attempts=3)
```

The whole table is still scanned, a window at a time, so the total work is the
same. The gain is that the run finishes. If quickhouse can't find the key's
range, it reads in one pass and warns with `window_bounds_unavailable`; with a
filtered `source_query`, also set `source_table` so the range comes from the
table.

### Asking for windows yourself

quickhouse only switches to windows by itself when the watermark lookup would
be too costly. A read can be long for other reasons. Say finance wants the
first 20,000 orders loaded again, on a replica where `id` is indexed: the
lookup is cheap, so the backfill would be read in one pass, and on a big table
the replica would cancel it on every retry. `window_bounds` sweeps a range of
the key in windows anyway:

```python
qh.sync(src, dst, source_table="orders", dest_table="orders_live",
        mode="incremental", watermark="id", key=["id"],
        advance_watermark=False,            # leave the regular job's position alone
        state_key="orders_live:backfill",
        window_bounds=(1, 20_000))          # the ids to read
```

```text
INFO windowed read on "id" over [1, 20000], starting width 50000 (ceiling 5000000, target 5.0s)
INFO windowed read complete: 1 window(s), 0 shrink(s)
```

The bounds are exactly the range the run reads: a row whose `id` is outside
them isn't read. The position such a run would save is the source's highest
`id`, past rows it never read, so quickhouse refuses `window_bounds` without
`advance_watermark=False`. `windowed=True` forces the same sweep over the key's
whole range, which it looks up, for example for the first load of a big table.
Both work for incremental syncs from PostgreSQL and MySQL, read over one
connection, and can't be combined with `chunk_rows` or a `parallelism` above 1.

## Make a huge load resumable

The first load of a very large table can take hours, and a failure near the
end means starting again. `chunk_rows` reads the table in key order, in chunks,
and saves its progress after each one, so the next run carries on where the
failed one stopped:

```python
qh.sync(src, dst, source_table="orders", dest_table="orders_live",
        mode="incremental", watermark="updated_at", key=["id"],
        chunk_rows=1_000_000)
```

```text
INFO keyset chunked read starting on 'id' (chunk_rows=1000000, resume_cursor=Some("200000"))
```

`chunk_rows` is experimental, and has rules:

- It works in **incremental mode only**, and reads over one connection.
- The chunk column (`partition_column`, or else the first `key` column) **must
  be a unique integer that is never `NULL`**. Otherwise rows could be skipped.
- It **can't be combined** with `validate=` or `delete_stale_in_window`,
  because each chunk lands in the destination before the next is read.
- The saved progress **belongs to one read**. When you split a backfill into
  ranges, a run of the next range under the same `state_key` doesn't resume an
  unfinished one: it reads its own range from the start and warns with
  `abandoned_resume_marker`, and the rest of the unfinished range stays unread
  until you run it again. Give each range its own `state_key`.

On BigQuery, each chunk is merged on its own, which stays cheap only when the
destination is clustered by the key (tables quickhouse creates are). With a
`source_query`, quickhouse must be able to prove the chunk column is never
`NULL`; if it can't, you can promise it with `keyset_not_null=True`, but a wrong
promise loses rows.

## When your query computes the watermark

Daily Grind's managers want order times in local time, so the query converts
them. With `watermark="updated_at_local"`, every run filters on the converted
value, which the index on `updated_at` can't help with. Return the original
column too, and filter on it with `watermark_source_expr`:

```python
qh.sync(
    src, dst,
    source_query="""
        SELECT id, store_id, total,
               updated_at AT TIME ZONE 'Asia/Jakarta' AS updated_at_local,
               updated_at AS updated_at_raw
        FROM orders
    """,
    dest_table="orders_local",
    mode="incremental",
    watermark="updated_at_local",
    watermark_source_expr="updated_at_raw",
    key=["id"],
    state_key="orders_local",
    exclude=["updated_at_raw"],
)
```

With `RUST_LOG=quickhouse_core=debug`, the log shows the filter is now
`WHERE updated_at_raw > ...`, which the index serves. This needs quickhouse to
look up the highest watermark first, so if that lookup is skipped as too
costly (see `probe_max_cost` above), the run is refused rather than risk a
wrong position.

## Faster writes into ClickHouse

Inserts into ClickHouse are compressed on the way. Choose by what limits your
connection:

- `"zstd"`, the default, for a slow or metered link such as ClickHouse Cloud.
- `"lz4"` for a fast network where CPU is the limit. It compresses about twice
  as fast.
- `"none"` for ClickHouse on the same machine or a fast local network.
- Avoid `"gzip"`: it is far slower than the others.

```python
dst = qh.ClickHouse("http://clickhouse.internal:8123", database="analytics",
                    compression="lz4")
```

The [ClickHouse destination guide](../guide/destinations/clickhouse.md) has the
measured speeds.

## Recap

- Measure first: `read_secs`, `stage_secs` and `promote_secs` show which part
  is slow.
- Parallel reads split on an integer column; for a `source_query`, set
  `partition_source_expr`. Memory grows with readers, so lower `insert_bytes`
  or `max_memory_bytes` first.
- Use a replica, `parallelism=1` and `read_max_rows_per_sec` to go easy on a
  busy database.
- Index the watermark column. Until then, windowed reads keep runs finishing on
  a replica. `window_bounds` and `windowed=True` ask for windows yourself, for
  example for a backfill.
- `chunk_rows` makes a huge load resumable, and `watermark_source_expr` keeps
  the index working when the query computes the watermark.

## Where to go from here

You've reached the end of the tutorials. From here:

- Go back to the [tutorials overview](index.md) and pick a topic you skipped.
- Read the [Performance & safety guide](../guide/performance.md) and
  [Sync modes](../guide/sync-modes.md) for the full detail behind these pages.
- Look up every parameter in the [API reference](../api.md).
- Browse the runnable [Examples](../examples.md).
