# ClickHouse

A ClickHouse destination writes through the HTTP interface with streaming
compressed inserts. The DDL knobs it accepts are on the
[Destinations index](index.md#destination-ddl).

## S3 archive

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
