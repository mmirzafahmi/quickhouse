# Backing up to cloud storage

**What you'll learn**

- How to keep a Parquet copy of everything quickhouse syncs, in Google Cloud
  Storage (GCS) or Amazon S3.
- Where the files go, and how to read them back with DuckDB or BigQuery.
- What happens when an upload fails, and how to keep the bucket tidy.

**Before you start:** read [Your first sync](first-sync.md). The practice
examples use MinIO, the S3-compatible storage service that the practice
environment from [Set up a practice environment](setup.md) already runs. To
read the files back you'll also need DuckDB (`pip install duckdb`).

## The task

Daily Grind's auditors want a copy of the sales data that lives outside
ClickHouse and BigQuery, so that a dropped table or a bad deploy can't take the
only copy with it. The data team likes the idea for its own reasons: a cheap
history of every row they've ever synced, which they can query or load back
into a table whenever they need to.

quickhouse can write that copy for you, **during** the sync. Every batch of
rows it writes to the destination is also written to a bucket as
[Parquet](https://parquet.apache.org/), a compact file format for tables that
almost every data tool can read. There is no second job to schedule and no
extra read from the source.

## Turn on the backup

First, create a bucket in MinIO:

```bash
docker compose exec minio mc mb local/daily-grind-lake
```

The backup is set on the **destination** with `archive=`. Here is a full
refresh of `orders` that also writes a backup:

```python
import quickhouse as qh

src = qh.Postgres("postgresql://etl:etl@localhost:5432/etl")
dst = qh.ClickHouse(
    "http://localhost:8123",
    database="analytics",
    archive=qh.backup(
        destination="s3", bucket="daily-grind-lake", prefix="quickhouse",
        endpoint="http://localhost:9010",     # MinIO instead of Amazon S3
        access_key_id="minioadmin", secret_access_key="minioadmin",
    ),
)

qh.sync(src, dst, source_table="orders", dest_table="orders", mode="full", key=["id"])
```

List what landed in the bucket:

```bash
docker compose exec minio mc ls --recursive local/daily-grind-lake
```

```text
493KiB STANDARD quickhouse/orders/dt=2026-10-24/run=1792831000692035093/part-range-0.parquet
453KiB STANDARD quickhouse/orders/dt=2026-10-24/run=1792831000692035093/part-range-1.parquet
```

### What just happened

quickhouse reads a table with several connections at once, each one reading
its own range of rows. With the archive turned on, each of those **read
streams** also wrote its rows to its own Parquet file: `part-range-0` and
`part-range-1` here. A machine with more CPUs uses more streams, so you may see
more files. The files are streamed to the bucket as the rows go by, so the
backup never needs much memory, however big the table is.

`qh.backup()` is a shortcut that picks the right descriptor for the cloud you
name: `qh.backup(destination="s3", ...)` is the same as `qh.S3Archive(...)`,
and `destination="gcs"` is the same as `qh.GcsArchive(...)`. It's handy when
the cloud comes from a config file. Parquet is the only format.

## Where the files go

Every file follows the same pattern:

```text
{bucket}/{prefix}/{dest_table}/dt={date}/run={run_id}/part-{stream}.parquet
```

- `prefix` is a folder of your choice, and each destination table gets its own
  folder inside it.
- `dt` is the day the sync ran, in UTC. It is *not* the date of the rows inside.
- `run` is a number that's unique to each sync run. It's the run's start time,
  so a larger number means a later run.

Folders named `name=value` are called a **Hive-style layout**. DuckDB, BigQuery
external tables, Spark and Athena all understand it, and can use `dt` and `run`
as if they were columns.

### Each incremental run adds its changes

A full refresh writes the whole table every run, so each `run=` folder is a
complete copy. An incremental sync only writes new and changed rows, so each
run's folder holds just those.

To see it, sync `orders` incrementally into a new table, `orders_history`,
using the same `dst` with the archive turned on:

```python
qh.sync(src, dst, source_table="orders", dest_table="orders_history",
        mode="incremental", watermark="updated_at", key=["id"])
```

Then refund three orders and run the same sync again:

```bash
docker compose exec postgres psql -U etl -d etl -c \
  "UPDATE orders SET status = 'refunded', updated_at = now() WHERE id IN (110, 120, 130)"
```

The archive now holds two runs:

```text
493KiB STANDARD quickhouse/orders_history/dt=2026-10-24/run=1792831509770637148/part-range-0.parquet
453KiB STANDARD quickhouse/orders_history/dt=2026-10-24/run=1792831509770637148/part-range-1.parquet
2.8KiB STANDARD quickhouse/orders_history/dt=2026-10-24/run=1792831510134735911/part-range-0.parquet
1002B  STANDARD quickhouse/orders_history/dt=2026-10-24/run=1792831510134735911/part-range-1.parquet
```

The first run copied every order. The second run's three rows all came through
one stream, so the other stream's file holds no rows, which is normal.
Together, the runs form a history of every version of every row: the refunded
orders appear once as `paid` and once as `refunded`.

```{note}
The archive only contains what syncs write **after** you turn it on. Added to
an incremental table that's already running, it starts with the next run's
changes. To put the whole table in the archive, run a one-off backfill with the
archive turned on, as described in
[Backfilling history](incremental.md#backfilling-history).
```

A sync with `chunk_rows` (see [Big tables and busy databases](performance.md))
writes one file per chunk instead of one per stream: `part-keyset-00000.parquet`,
`part-keyset-00001.parquet`, and so on. If a chunked run fails part way and the
next run resumes it, the chunks the failed run completed stay in its `run=`
folder, and the rest go into the new run's folder.

## Reading the backup back

Because of failed runs and resumed runs, the rule for reading the archive is:
**read the table's whole folder, not a single `run=` folder, and keep one row
per key.**

Start DuckDB (`duckdb` on the command line, or `duckdb.sql(...)` in Python) and
tell it how to reach MinIO:

```sql
INSTALL httpfs;
LOAD httpfs;
CREATE SECRET minio (TYPE s3, KEY_ID 'minioadmin', SECRET 'minioadmin',
                     ENDPOINT 'localhost:9010', URL_STYLE 'path', USE_SSL false);
```

See every run, and how many rows it wrote:

```sql
SELECT dt, run, count(*) AS rows
FROM read_parquet('s3://daily-grind-lake/quickhouse/orders_history/*/*/*.parquet',
                  hive_partitioning = true)
GROUP BY dt, run
ORDER BY run;
```

```text
┌────────────┬─────────────────────┬───────┐
│     dt     │         run         │ rows  │
│    date    │        int64        │ int64 │
├────────────┼─────────────────────┼───────┤
│ 2026-10-24 │ 1792831509770637148 │ 49998 │
│ 2026-10-24 │ 1792831510134735911 │     3 │
└────────────┴─────────────────────┴───────┘
```

`hive_partitioning = true` turns the `dt` and `run` folder names into columns.
To get the latest version of each order, keep the newest row per `id`:

```sql
CREATE VIEW orders_latest AS
SELECT *
FROM read_parquet('s3://daily-grind-lake/quickhouse/orders_history/*/*/*.parquet',
                  hive_partitioning = true)
QUALIFY row_number() OVER (PARTITION BY id ORDER BY updated_at DESC, run DESC) = 1;

SELECT status, count(*) AS orders FROM orders_latest GROUP BY status;
```

This matches `analytics.orders_history FINAL` in ClickHouse.

### Loading a backup into a table

The archive is also how you rebuild a table. A DuckDB result can go straight
into [`from_pandas()`](dataframes.md), so restoring the latest version of every
order into a new ClickHouse table takes a few lines of Python:

```python
import duckdb
import quickhouse as qh

con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql("""CREATE SECRET minio (TYPE s3, KEY_ID 'minioadmin', SECRET 'minioadmin',
           ENDPOINT 'localhost:9010', URL_STYLE 'path', USE_SSL false)""")
latest = con.sql("""
    SELECT * EXCLUDE (dt, run)
    FROM read_parquet('s3://daily-grind-lake/quickhouse/orders_history/*/*/*.parquet',
                      hive_partitioning = true)
    QUALIFY row_number() OVER (PARTITION BY id ORDER BY updated_at DESC, run DESC) = 1
""")

dst = qh.ClickHouse("http://localhost:8123", database="analytics")
qh.from_pandas(latest, dst, dest_table="orders_restored", mode="full", key=["id"])
```

The rebuilt table is close to the original but not identical: every column
except the key comes back nullable, for example.

### Reading it from BigQuery

When the archive is in GCS, BigQuery can query it in place with an external
table:

```sql
CREATE EXTERNAL TABLE `daily-grind-data.lake.orders_history_archive`
WITH PARTITION COLUMNS
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://daily-grind-lake/quickhouse/orders_history/*'],
  hive_partition_uri_prefix = 'gs://daily-grind-lake/quickhouse/orders_history'
);
```

`dt` and `run` become columns, and the same "newest row per key" rule applies.

## Using a real bucket

In production, point the archive at GCS or S3. Here is GCS on a BigQuery
destination (supported since quickhouse 0.19.0), and S3 on a ClickHouse one.
Every kind of source can be archived, including HTTP APIs and DataFrames:

```python
bq = qh.BigQuery(
    "daily-grind-data", dataset_id="analytics",
    archive=qh.backup(destination="gcs", bucket="daily-grind-lake", prefix="quickhouse"),
)

ch = qh.ClickHouse(
    "http://clickhouse.internal:8123", database="analytics",
    archive=qh.backup(destination="s3", bucket="daily-grind-lake",
                      prefix="quickhouse", region="ap-southeast-3"),
)
```

Keep keys out of your code where you can:

- **GCS** finds credentials the same way the `BigQuery` descriptor does: the
  `SERVICE_ACCOUNT` or `GOOGLE_SERVICE_ACCOUNT` environment variables, or
  Application Default Credentials (your `gcloud auth application-default
  login`, or the service account of the Google Cloud machine it runs on). To
  use a particular service account, pass `credentials_file=` or
  `credentials_json=` to `backup()`. GCS's `endpoint` option is only for a proxy
  or private endpoint: setting it turns credentials off entirely.
- **S3** uses the standard AWS credentials: the `AWS_ACCESS_KEY_ID` and
  `AWS_SECRET_ACCESS_KEY` environment variables, or the IAM role of the machine
  it runs on. Name the bucket's `region` (or set `AWS_REGION`). `endpoint` is
  only for S3-compatible services such as MinIO; when it's set, plain `http://`
  addresses are allowed.

Parquet files also compress their own contents. `compression` on the archive
picks how: `"zstd"` (the default) gives small files that modern tools read,
`"snappy"` gives slightly bigger files that very old tools can also read, and
`"uncompressed"` turns it off. This is separate from the `compression` option
on `ClickHouse(...)`, which compresses inserts on their way to ClickHouse.

## When an upload fails

The backup is never allowed to fall behind quietly. If an upload keeps failing,
the whole sync fails, just as it would if the destination itself failed. Here
is a run pointed at a bucket that doesn't exist:

```text
RuntimeError: orders -> orders: s3 archive: failed to finalize parquet file
'quickhouse/orders/dt=2026-10-24/run=.../part-range-0.parquet': ...
<Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message> ...
```

A failed run leaves things as any other failed run does: a full refresh doesn't
swap anything in, and an incremental sync doesn't save its watermark, so the
next run tries the same rows again.

On the way out, quickhouse cancels any upload it hadn't finished, so there are
no half-written files. It can leave **finished** files behind, though: a file is
finished as soon as its rows are read, so if the run fails after that (on its
last insert, say), the retry archives those rows again under a new `run=`
folder. One more reason to keep one row per key when you read the archive.

One case quickhouse can't clean up after is being killed outright, for example
by an out-of-memory error. Large files are uploaded in parts, and the parts of
an unfinished upload stay in the bucket, invisible but billed, until they are
removed. Give the bucket a lifecycle rule that removes them after a day. Both
S3 and GCS call this rule `AbortIncompleteMultipartUpload`. On S3, save this as
`lifecycle.json`:

```json
{"Rules": [{
  "ID": "abort-incomplete-uploads",
  "Status": "Enabled",
  "Filter": {"Prefix": "quickhouse/"},
  "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}
}]}
```

```bash
aws s3api put-bucket-lifecycle-configuration \
    --bucket daily-grind-lake --lifecycle-configuration file://lifecycle.json
```

## A common mistake: the archive on the source

`archive=` only works on the destination. A `ClickHouse` descriptor can be a
source too, and an archive set on a source writes nothing. quickhouse says so,
both in the log ("NO BACKUP WILL BE WRITTEN for this run unless archive= is also
set on the destination descriptor") and as an `ignored_source_archive` warning
on the result.

## Recap

- Set `archive=qh.backup(...)` on the **destination**, and every sync also
  writes its rows to Parquet files in GCS or S3.
- Files go to `{prefix}/{table}/dt={run date}/run={run id}/`. A full refresh
  archives a complete copy each run; an incremental sync, each run's changes.
- Read the whole table folder and keep the newest row per key.
- A failed upload fails the sync. Add an `AbortIncompleteMultipartUpload`
  lifecycle rule for runs that are killed outright.

## Next

[Catching bad data](data-quality.md): warnings, and quality checks before data
goes live.
