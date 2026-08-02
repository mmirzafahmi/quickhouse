# Destinations

Every transfer is `sync(source, target, ...)`. A **destination** is a
`ClickHouse` or `BigQuery` connection descriptor. The same `BigQuery` class
also works as a [source](sources.md); everything else about the call is
identical regardless of which engines you use.

```python
import quickhouse as qh

qh.ClickHouse("http://host:8123", database="analytics")
qh.BigQuery("my-gcp-project", dataset_id="analytics")
```

For the exact constructor signatures see the [API reference](../api.md).

```{raw} html
<div class="qh-modes">
  <a class="qh-mode qh-mode--current" href="#s3-archive-clickhouse-destinations">
    <div class="qh-mode__name">ClickHouse</div>
    <div class="qh-mode__desc">MergeTree-family DDL, atomic swap. Optional S3 archive of every synced batch.</div>
  </a>
  <a class="qh-mode" href="#bigquery-as-a-destination">
    <div class="qh-mode__name">BigQuery</div>
    <div class="qh-mode__desc">MERGE-based upsert. Requires a dataset_id; insert_all or Storage Write transport.</div>
  </a>
</div>
```

## BigQuery as a destination

```{note}
`dataset_id` is **required** — it's BigQuery's equivalent of ClickHouse's
`database`.
```

`write_method` selects how rows are written: `"insert_all"`
(default; `tabledata.insertAll`, proven) or `"storage_write"` (the gRPC Storage
Write API — free and higher-throughput). Both share the same atomic-swap /
MERGE flow; only the row-insert transport differs. See
[BigQuery authentication](sources.md#authentication) — the same credentials
work in either role.

```python
qh.sync(
    qh.BigQuery("my-project"),                                    # source
    qh.BigQuery("my-project", dataset_id="analytics"),           # destination
    dest_table="orders", source_table="raw.orders",
)
```

## S3 archive (ClickHouse destinations)

A ClickHouse destination can also archive every synced batch to S3 as a data
lake — a secondary, best-effort-free backup independent of ClickHouse's own
retention:

```python
qh.ClickHouse(
    "http://host:8123", database="analytics",
    archive=qh.S3Archive(bucket="my-data-lake", prefix="quickhouse"),
)
```

This streams Parquet — one file per parallel partition, never fully buffered in
memory — to a Hive-style layout directly queryable by Athena, Spark, or DuckDB:

```
s3://{bucket}/{prefix}/{dest_table}/dt=<date>/run=<id>/part-<partition>.parquet
```

Credentials fall back to the standard AWS chain (env vars, IAM role) unless
overridden; pass `endpoint=` for an S3-compatible service like MinIO. A
persistent upload failure fails the whole `sync()` call, same as a ClickHouse
insert failure — the archive never silently falls behind. Storage/request costs
are billed by AWS as usual (free on a self-hosted MinIO).

## Destination DDL

The DDL knobs (`engine`, `partition_by`, `order_by`, `primary_key`, `key`) are
interpreted per destination:

- **ClickHouse** — they shape the `MergeTree`-family DDL.
- **BigQuery** — `engine` is ignored; `partition_by` must be a bare
  `DATE`/`DATETIME`/`TIMESTAMP` column name (not a SQL expression);
  `order_by`/`key` become clustering columns (at most 4 total).

quickhouse creates the table for you (`create_if_missing=True` by default) with
a schema derived from the source. See [Type mapping](type-mapping.md) for how
source types become destination types.
