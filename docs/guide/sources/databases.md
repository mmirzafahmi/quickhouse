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
`column_transforms`, `read_max_rows_per_sec`, and `reconcile_keys`.

`source_table` may be `"table"` (in the descriptor's `database`) or
`"other_db.table"`. `batch_rows` is pushed down as the server's `max_block_size`,
since ClickHouse decides the record-batch boundaries; `batch_bytes` is applied
client-side on top of that.

`statement_timeout_secs` sets the server's `max_execution_time` — the same
whole-transfer ceiling the other sources' statement timeouts are, so size it
for the transfer and use `read_idle_timeout_secs` to catch a stalled source.
Anything else ClickHouse exposes goes through `settings={...}`, which is applied
last and therefore always wins.

### Types

Types that have no counterpart elsewhere are preserved rather than flattened:
`UUID`, `IPv4`/`IPv6`, `Enum8`/`Enum16`, `FixedString(N)` and
`LowCardinality(...)` are recreated as themselves at a ClickHouse destination,
and land as `STRING` in BigQuery. `Date`, `Date32`, `DateTime` and
`DateTime64(P[, tz])` all resolve to a UTC-aware timestamp — a ClickHouse
datetime is an absolute instant whatever timezone its type names — with
`type_overrides={"col": "DATETIME"}` as the per-column opt-out to a naive one.

Not readable yet: `Array`, `Map`, `Tuple`, `Nested`, `JSON`, the 256-bit
integers and `Decimal256`. Each is a clear error naming the column, not a silent
drop — `exclude` the column, or cast it to `String` in a `source_query`.
