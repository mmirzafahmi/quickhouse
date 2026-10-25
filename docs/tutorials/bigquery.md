# Loading into BigQuery

**What you'll learn**

- How to give quickhouse access to BigQuery: with your own login, a key file,
  or a secret.
- How full refreshes and incremental syncs work in BigQuery, and what they cost.
- How to partition and cluster a table, and keep `MERGE` costs down as it grows.
- How to publish a ClickHouse table into BigQuery.

**Before you start:** read [Your first sync](first-sync.md) and
[Keeping tables up to date](incremental.md). You need a Google Cloud project
with BigQuery turned on. The examples use a project called `daily-grind-data`.

```{note}
Unlike the other tutorials, this one can't run on your laptop alone: BigQuery
is a cloud service, and the queries quickhouse runs there are billed to your
project. The amounts for these small examples are tiny, but they aren't zero.
```

## The task

ClickHouse serves the store managers' live dashboards, but the rest of Daily
Grind (finance, marketing, the management team) reports in BigQuery. They want
`orders` and `customers` there, updated every hour, without a surprise on the
monthly bill.

## Before your first sync

### Create a dataset

A BigQuery **dataset** is a folder for tables, like a database in ClickHouse.
quickhouse creates tables for you, but not datasets. Create one with the `bq`
tool from the Google Cloud CLI, or in the BigQuery console:

```bash
bq mk --dataset daily-grind-data:analytics
```

Datasets live in one location (a region such as `asia-southeast2`, or a
multi-region such as `US`). Pick the one closest to the rest of your data with
`bq --location=asia-southeast2 mk --dataset ...`. You can't change it later.

### Give quickhouse access

quickhouse finds BigQuery credentials in one of three ways.

**Your own login**, for trying things out on your laptop. Log in once with the
Google Cloud CLI, then create the descriptor with no credentials at all:

```bash
gcloud auth application-default login
```

```python
import quickhouse as qh

dst = qh.BigQuery("daily-grind-data", dataset_id="analytics")
```

This uses **Application Default Credentials**, Google's standard way of finding
credentials. It also picks up the `GOOGLE_APPLICATION_CREDENTIALS` environment
variable and, on Google Cloud machines (Compute Engine, GKE, Cloud Run), the
service account attached to the machine. That last one is usually the best
choice in production: there is no key to store or rotate.

**A key file**, for a service account:

```python
dst = qh.BigQuery("daily-grind-data", dataset_id="analytics",
                  credentials_file="/secrets/quickhouse-sa.json")
```

**The key's contents**, when your scheduler keeps secrets in a secret manager
or an environment variable rather than in a file:

```python
dst = qh.BigQuery("daily-grind-data", dataset_id="analytics",
                  credentials_json=os.environ["BQ_SERVICE_ACCOUNT_JSON"])
```

If you set both, `credentials_json` wins. The project ID is optional when the
credentials already name a project, but writing it out makes the code easier to
read.

Whichever way you log in, the account needs to create and change tables in the
dataset, and run queries in the project. With Google's predefined roles, that
is usually **BigQuery Data Editor** on the dataset and **BigQuery Job User** on
the project.

## Your first sync into BigQuery

The call is the same as for ClickHouse. Only the destination changes:

```python
src = qh.Postgres("postgresql://etl:etl@localhost:5432/etl")
dst = qh.BigQuery("daily-grind-data", dataset_id="analytics")

result = qh.sync(src, dst, source_table="stores", dest_table="stores", mode="full")
print(result)
```

`dataset_id` is required for a destination. Leave it out and quickhouse stops
before doing anything, with `BigQuery(...) used as a sync() target requires
dataset_id`.

### What happens in BigQuery

A full refresh works the same way as in ClickHouse: quickhouse creates the
`stores` table if needed, loads the rows into a staging table next to it
(`stores_quickhouse_tmp_<id>`), and then swaps the new data in. Readers see the
old table until the new one is complete, and the staging table is dropped at
the end, even when the run fails.

The swap is where BigQuery differs. quickhouse runs one transaction that empties
`stores` and copies the staging table into it with a query. That query is
**billed** like any other: it scans the staged data once. (BigQuery's free copy
job isn't used because it can silently skip rows that were written moments
earlier.) The swap keeps the table's existing partitioning and clustering.

## Keeping it fresh with incremental syncs

`orders` is too big to copy in full every hour, so sync it incrementally:

```python
result = qh.sync(src, dst, source_table="orders", dest_table="orders",
                 mode="incremental", watermark="updated_at", key=["id"])
```

BigQuery has nothing like ClickHouse's `ReplacingMergeTree`, so quickhouse
updates rows with a `MERGE` statement instead. Each run:

1. Loads the new and changed rows into a staging table.
2. Runs one `MERGE`: for every staged row, it **updates** the row in `orders`
   that has the same `id`, or **inserts** it if there isn't one.
3. Saves the new watermark and drops the staging table.

Because each row is matched on its key, the table always has exactly one row
per `id`. You don't need `FINAL` or any other trick when you query it. Running
the same rows through twice just updates them twice.

That's also why `key` is **required** for incremental syncs into BigQuery.
Without it the run stops with `key is required for incremental mode with this
destination (used as the MERGE match key; ...)`.

## Partitioning and clustering

Two table settings make BigQuery read (and bill) less:

- **Partitioning** splits the table into one section per day. A query that
  filters on the partition column only reads the days it asks for.
- **Clustering** keeps rows sorted by up to four columns. A query or `MERGE`
  that filters on those columns skips the blocks it doesn't need.

quickhouse sets both when it creates the table:

```python
qh.sync(src, dst, source_table="orders", dest_table="orders",
        mode="incremental", watermark="updated_at", key=["id"],
        partition_by="created_at")
```

The table is partitioned by day on `created_at` and clustered by `id`. Two
rules differ from ClickHouse:

- `partition_by` must be the **bare name** of a `DATE`, `DATETIME` or
  `TIMESTAMP` column, not an expression. `partition_by="toYYYYMM(created_at)"`
  fails with "for a BigQuery destination this must be a bare column name, not a
  SQL expression like ClickHouse's".
- Clustering columns are `order_by` and `key`, at most four in total. More
  fails with "BigQuery clustering supports at most 4 columns". For an
  incremental table, `key` comes first and `order_by` follows, because the
  `MERGE` savings below depend on the key leading. For a full refresh or an
  append, `order_by` comes first.

```{note}
Before quickhouse 0.20.11, `order_by` always came first. An incremental table
created by an earlier version with both `order_by` and `key` is clustered with
`order_by` leading, and raises the `unclustered_merge_target` warning described
below. quickhouse doesn't change an existing table's clustering, so recreate the
table to fix it.
```

Like in ClickHouse, these settings only apply when quickhouse creates the
table. To change them, drop the table. For an incremental table, the next sync
notices that it has to create the table while a position is still saved, and
reloads everything instead of starting from that position, with a
`recreated_destination` warning on the result.

## Keeping MERGE costs down

A `MERGE` that only says "match on `id`" makes BigQuery scan, and bill, the
whole destination table. Upserting 200 changed orders into a table of 50
million rows would cost as much as reading all 50 million. quickhouse gives you
three ways to shrink that scan, from safest to sharpest. The first one is on by
default.

### 1. The key range (automatic)

`merge_prune_key_range` is on by default. quickhouse finds the smallest and
largest `id` in the staged batch and tells BigQuery to only look at
destination rows in that range. It can never change the result: a row can only
match if its `id` is in the batch.

This works well for `orders`. New orders get new, higher `id`s, and changes
mostly touch recent orders, so the range is narrow and BigQuery skips most of
the table, **as long as the table is clustered by `id`**.

### 2. A list of exact keys

Now take `customers`. A customer from three years ago can change tier today, so
one hour's changes are scattered across the whole range of `id`s. The range
covers almost everything and saves almost nothing.

`merge_prune_key_list_max` fixes that. When the batch holds at most that many
different keys, quickhouse lists them exactly instead of using a range:

```python
qh.sync(src, dst, source_table="customers", dest_table="customers",
        mode="incremental", watermark="updated_at", key=["id"],
        merge_prune_key_list_max=5_000)
```

It is just as safe as the range. It costs one small extra query per `MERGE`,
and if a batch has more than 5,000 different keys, quickhouse falls back to the
range. It only works with a single-column `key`.

### 3. The partition column (use with care)

`merge_prune_partition_by` limits the `MERGE` to the partitions the batch
touches. On a table partitioned by `created_at`, an hour of changes might only
touch a few days, so BigQuery skips every other partition:

```python
qh.sync(src, dst, source_table="orders", dest_table="orders",
        mode="incremental", watermark="updated_at", key=["id"],
        partition_by="created_at", merge_prune_partition_by="created_at")
```

```{warning}
Only use a column whose value **never changes** for a row, like `created_at`.
Never use `updated_at`.

Here is why. An order created on 3 September lives in the 3 September
partition. If it's refunded on 20 October, its new `updated_at` is 20 October.
With `merge_prune_partition_by="updated_at"`, the `MERGE` would only look in
October, wouldn't find the old row, and would **insert a second copy** of the
order instead of updating it. quickhouse can't tell whether a column changes,
so this is your responsibility.
```

### When pruning can't help

Both key-based bounds only save money when the destination is clustered by the
key. A table created by quickhouse is. A table created some other way may not
be, and then every `MERGE` scans the whole table, however narrow the batch. In
that case quickhouse adds an `unclustered_merge_target` warning to the result:

```python
for w in result.warnings:
    if w.kind == "unclustered_merge_target":
        print(w.message)
```

The fix is to recreate the table clustered by its key.

## Seeing where the time goes

Every result splits the run into phases:

```python
print(f"read {result.read_secs:.1f}s  stage {result.stage_secs:.1f}s  "
      f"promote {result.promote_secs:.1f}s")
```

- `stage_secs` is reading from the source and loading the staging table.
- `promote_secs` is what comes after: the swap or the `MERGE`, and saving the
  watermark.

On BigQuery, `promote_secs` is usually most of a small run, because the
`MERGE` is a real query. If it keeps growing as the table grows, the `MERGE` is
scanning more and more: check the clustering, and try the pruning options
above. The BigQuery console's job history shows how many bytes each `MERGE`
was billed for.

## How rows are written

`write_method` decides how quickhouse sends rows to BigQuery:

- `"storage_write"` (the default) uses BigQuery's Storage Write API. It is the
  faster and cheaper option: free up to 2 TiB a month, then $0.025 per GB.
- `"insert_all"` uses the older streaming API. It costs about twice as much and
  is slower.

You normally never change it. quickhouse checks that BigQuery received exactly
the rows it sent, and fails the run before the swap or `MERGE` if not, with a
`storage_write_count_mismatch` warning. If that happens more than once, switch
to `write_method="insert_all"` on the `BigQuery(...)` destination.

## Dates and times

BigQuery has two kinds of date-time column. A `TIMESTAMP` is an exact moment
(stored in UTC). A `DATETIME` is a wall-clock time with no time zone.

- From **PostgreSQL**, `timestamptz` becomes `TIMESTAMP` and `timestamp`
  becomes `DATETIME`.
- From **MySQL**, both `DATETIME` and `TIMESTAMP` become `TIMESTAMP`: the
  wall-clock value MySQL sends is read as UTC. To keep a column as a plain
  `DATETIME`, override it: `type_overrides={"create_date": "DATETIME"}`.

MySQL `TIMESTAMP` columns have one more trap when the MySQL server isn't set to
UTC. [Reading from databases](databases.md) and the
[type mapping guide](../guide/type-mapping.md) explain it, and the
`utc_session=True` fix.

## Publishing a ClickHouse table into BigQuery

Sometimes the data starts in ClickHouse. Say the data team builds a
`daily_sales` summary in ClickHouse, and finance wants it in BigQuery. A
`ClickHouse` descriptor works as a source too:

```python
src = qh.ClickHouse("http://clickhouse.internal:8123", database="analytics",
                    user="reader", password=os.environ["CH_PASSWORD"])
dst = qh.BigQuery("daily-grind-data", dataset_id="analytics")

qh.sync(src, dst, source_table="daily_sales", dest_table="daily_sales",
        mode="full", key=["day", "store_id"])
```

ClickHouse types with no BigQuery equivalent, such as `UUID`, `IPv4`, `Enum`
and `LowCardinality(String)`, arrive as `STRING`.

```{important}
If the ClickHouse table is a `ReplacingMergeTree` (as every table quickhouse
creates for an incremental sync is), read it with `FINAL`, for example
`source_query="SELECT * FROM orders_live FINAL"`. Otherwise rows whose old
versions haven't been merged away yet are copied twice.
[Designing ClickHouse tables](clickhouse-tables.md) explains why.
```

## Recap

- Create the dataset first, then pass `dataset_id`. Credentials come from your
  login, a key file, or `credentials_json`.
- A full refresh swaps the new data in with a billed query. An incremental sync
  stages the changes and runs a `MERGE` on `key`, which is required.
- `partition_by` takes a bare date or timestamp column. Clustering is
  `order_by` then `key`, up to four columns; keep the key first.
- To cut `MERGE` costs: the key range is automatic, `merge_prune_key_list_max`
  helps tables with scattered updates, and `merge_prune_partition_by` is only
  for a column that never changes.
- Watch `promote_secs` and the `unclustered_merge_target` warning.

## Next

[Backing up to cloud storage](backups.md): a Parquet copy of every sync.
