# ClickHouse

A ClickHouse destination writes through the HTTP interface with streaming
compressed inserts. The DDL knobs it accepts are on the
[Destinations index](index.md#destination-ddl).

The same class is also a [source](../sources/databases.md#clickhouse-as-a-source);
`compression`, `archive` and `insert_dedup_token` below are write-path only and
are ignored in that role — an `archive` set there raises an
`ignored_source_archive` warning rather than writing anything.

## Insert compression

`compression=` picks how each insert body is compressed on the way to the
server. Choose it by what limits the link, since compression trades CPU for
bytes:

| `compression` | When | Measured, one core, 32 MiB inserts |
| --- | --- | --- |
| `"zstd"` (default) | A constrained link: WAN, ClickHouse Cloud | ~280 MB/s, 4.2× smaller |
| `"lz4"` | A fast network where CPU is the limit | ~610 MB/s, 2.35× smaller |
| `"none"` | Same host or a fast LAN | no CPU cost |
| `"gzip"` | Not recommended | ~22 MB/s, 4.75× smaller |

gzip compresses at roughly a tenth of zstd's speed for barely a better ratio, so
each insert can go no faster than about 22 MB/s per core. A transfer that sets
it logs a warning. It stays accepted for compatibility.

```python
qh.ClickHouse("http://clickhouse.internal:8123", compression="lz4")
```

## Cloud backup (Parquet archive)

A destination can also archive every synced batch to Google Cloud Storage or S3
as a data lake — a secondary, best-effort-free backup independent of
ClickHouse's own retention. This is not ClickHouse-specific: a
[BigQuery destination](bigquery.md#cloud-backup-parquet-archive) takes the same
`archive=`, and every source shape is archived, including HTTP API and
DataFrame sources.

```python
qh.ClickHouse(
    "http://host:8123", database="analytics",
    archive=qh.backup(destination="gcs", format="parquet",
                      bucket="my-data-lake", prefix="quickhouse"),
)
```

`backup()` is a factory over the two descriptors, which you can also name
directly — `qh.GcsArchive(bucket=...)` / `qh.S3Archive(bucket=...)`. Parquet is
the only supported `format`; any other value is rejected at the call.

This streams Parquet — one file per parallel partition, never fully buffered in
memory — to a Hive-style layout directly queryable by BigQuery external tables,
Athena, Spark, or DuckDB:

```
gs://{bucket}/{prefix}/{dest_table}/dt=<date>/run=<id>/part-<partition>.parquet
s3://{bucket}/{prefix}/{dest_table}/dt=<date>/run=<id>/part-<partition>.parquet
```

GCS credentials fall back to Application Default Credentials and the standard
`SERVICE_ACCOUNT` / `GOOGLE_SERVICE_ACCOUNT` environment, exactly as for the
`BigQuery` descriptor; pass `credentials_file=` or `credentials_json=` to
override. S3 credentials fall back to the standard AWS chain (env vars, IAM
role); pass `endpoint=` for an S3-compatible service like MinIO.

A persistent upload failure fails the whole `sync()` call, same as a ClickHouse
insert failure — the archive never silently falls behind. Storage and request
costs are billed by Google/AWS as usual (free on a self-hosted MinIO).
