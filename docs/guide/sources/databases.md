# Databases

PostgreSQL, MySQL, BigQuery and ClickHouse. Connection descriptors and
credentials for all four are on the [Sources index](index.md#authentication).

## BigQuery as a source

`source_table` should be `"dataset.table"` or `"project.dataset.table"`. Reads
use the BigQuery Storage Read API; `parallelism` becomes a server-side
stream-count hint, but rows are consumed on a single client connection
(BigQuery parallelizes server-side).

## ClickHouse as a source

The same `ClickHouse` class is both a source and a destination, so a
cross-cluster copy, or promoting a table from a raw database into an analytics
one, is an ordinary `sync()`:

```python
src = qh.ClickHouse("http://ch-a:8123", database="raw")
dst = qh.ClickHouse("http://ch-b:8123", database="analytics")
qh.sync(src, dst, dest_table="orders", source_table="orders",
        mode="incremental", watermark="updated_at", key=["id"], parallelism=4)
```

Reads go over the HTTP interface in ClickHouse's own `FORMAT ArrowStream`, so
rows are never converted out of Arrow between the two servers. Everything the
Postgres and MySQL sources support works here too: range partitioning across
`parallelism` connections, incremental watermarks, `chunk_rows` resumable reads,
`column_transforms`, `read_max_rows_per_sec`, `reconcile_keys`, and
`reconcile_values`.

`source_table` may be `"table"` (in the descriptor's `database`) or
`"other_db.table"`. `batch_rows` is pushed down as the server's `max_block_size`,
since ClickHouse decides the record-batch boundaries; `batch_bytes` is applied
client-side on top of that.

`statement_timeout_secs` sets the server's `max_execution_time` — the same
whole-transfer ceiling the other sources' statement timeouts are, so size it
for the transfer and use `read_idle_timeout_secs` to catch a stalled source.
Anything else ClickHouse exposes goes through `settings={...}`, which is applied
last and therefore always wins.

A table whose engine merges rows is read with `FINAL`. A `ReplacingMergeTree`
(every table quickhouse creates for an incremental sync is one) keeps old
versions of a row until a background merge removes them, and without `FINAL`
those were copied too, so a copy could hold a key more than once. quickhouse
reads the engine from `system.tables` and adds `FINAL` for
`ReplacingMergeTree`, `CollapsingMergeTree`, `VersionedCollapsingMergeTree`,
`SummingMergeTree` and `AggregatingMergeTree`, including their `Replicated*`
forms, and logs it:

```text
source table 'orders' is a ReplacingMergeTree, which keeps old versions of a row until a background merge removes them: reading it with FINAL (pass final=False to read it as stored)
```

`FINAL` goes on every read of the rows, including each parallel range, each
`chunk_rows` chunk and an incremental read, and on the `MAX(watermark)` probe
that sets the cursor, so the cursor comes from rows that were read. A
`source_query` is never changed: put `FINAL` in it yourself.

`FINAL` makes ClickHouse merge a row's versions as it reads, which costs CPU,
memory and time. A filter on a column outside the sorting key, such as the
watermark, is applied after the merge, so it doesn't narrow what is merged;
the primary key and partition pruning still do. The parallel range read splits
on a sorting-key column, where every version of a row falls in the same range.
If `partition_column` (or the first `key`) isn't in the sorting key, the read
splits on the sorting key's first column instead, or runs in one stream when
that column isn't an integer, and the log says which.

Pass `final=` on the source descriptor to choose:

```python
qh.ClickHouse(url, database="raw")               # FINAL where the engine merges rows
qh.ClickHouse(url, database="raw", final=False)  # read every stored row, as before 0.20.12
qh.ClickHouse(url, database="raw", final=True)   # always, e.g. a Distributed table
```

`final=True` on an engine that doesn't merge rows fails before anything is
written: ClickHouse refuses `FINAL` on a plain `MergeTree` (`ILLEGAL_FINAL`). A
`Distributed` table and a materialized view pass `FINAL` on to the table they
read, so they need `final=True`, since only the table's own engine is looked
at. A plain `VIEW` accepts `FINAL` and ignores it, so put `FINAL` inside the
view. `final` does nothing when the descriptor is a destination.

### Types

Types that have no counterpart elsewhere are preserved rather than flattened:
`UUID`, `IPv4`/`IPv6`, `Enum8`/`Enum16`, `FixedString(N)` and
`LowCardinality(...)` are recreated as themselves at a ClickHouse destination,
and land as `STRING` in BigQuery. `Date`, `Date32`, `DateTime` and
`DateTime64(P[, tz])` keep their declared types at a ClickHouse destination,
time zone included. In BigQuery, `Date` and `Date32` land as `DATE`, and
`DateTime` and `DateTime64` as a UTC-aware `TIMESTAMP` — a ClickHouse datetime
is an absolute instant whatever timezone its type names — with
`type_overrides={"col": "DATETIME"}` as the per-column opt-out to a naive one.

Not readable yet: `Array`, `Map`, `Tuple`, `Nested`, `JSON`, the 256-bit
integers and `Decimal256`. Each is a clear error naming the column, not a silent
drop — `exclude` the column, or cast it to `String` in a `source_query`.
