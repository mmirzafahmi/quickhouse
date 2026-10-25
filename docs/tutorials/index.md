# Tutorials

These tutorials teach quickhouse one step at a time, from your first copied
table to a pipeline you can trust in production. Each page covers one feature.
It starts with the simplest use and works up to the advanced options, using
examples from a business you will get to know as you go.

If you only want to look up a parameter, the [API reference](../api.md) is
faster. If you want to know *why* and *when* to use something, start here.

## The example business

Every tutorial follows **Daily Grind**, a made-up chain of coffee shops. Daily
Grind's data lives in several places, like most companies' data does:

- **The ordering app** runs on PostgreSQL. It has `orders`, `customers` and
  `stores` tables.
- **The inventory system** runs on MySQL. It records every stock movement in a
  `stock_moves` table, and it deletes a row when a movement is cancelled.
- **The loyalty app** sends its events to CleverTap, and its install
  attribution comes from AppsFlyer.
- **The finance team** keeps monthly sales targets in a spreadsheet.

The data team wants all of this in two places:

- **ClickHouse**, which powers fast live dashboards for store managers.
- **BigQuery**, which the rest of the company uses for reports.

That is the job quickhouse does: it copies tables from where they live into
ClickHouse or BigQuery, and keeps those copies up to date.

## A few words you will see often

Source
: Where the data comes from: a database, an HTTP API, or a DataFrame.

Destination
: Where the data goes: ClickHouse or BigQuery. The docs also call it the
  *target*.

Sync
: One run of a copy, done by calling `quickhouse.sync()`.

Full refresh
: A sync that replaces the whole destination table with a fresh copy.

Incremental sync
: A sync that copies only the rows that are new or changed since the last run.

Watermark
: A column that tells quickhouse which rows are new, usually `updated_at`.
  quickhouse remembers the highest value it has seen, and next time it only
  reads rows above it.

Key
: The column (or columns) that identify a row, usually `id`. quickhouse uses it
  to replace an old version of a row with the new one instead of keeping both.

## The learning path

Work through the first part in order. After that, jump to whatever you need.

**Part 1: the basics**

1. [Set up a practice environment](setup.md): run the databases on your laptop
   and load sample data.
2. [Your first sync](first-sync.md): copy a whole table, and read what
   quickhouse tells you about the run.
3. [Choosing what to copy](choosing-data.md): pick columns, rename them, hide
   personal data, and copy the result of a query.
4. [Keeping tables up to date](incremental.md): copy only what changed, catch
   late updates, and backfill history.

**Part 2: sources**

5. [Reading from databases](databases.md): PostgreSQL, MySQL, BigQuery and
   ClickHouse, including secure connections and their quirks.
6. [Pulling from HTTP APIs](http-apis.md): CleverTap, AppsFlyer, and any other
   REST or CSV endpoint.
7. [Loading DataFrames](dataframes.md): pandas, polars, pyarrow and DuckDB.

**Part 3: destinations**

8. [Designing ClickHouse tables](clickhouse-tables.md): sort keys, partitions,
   deduplication and schema changes.
9. [Loading into BigQuery](bigquery.md): credentials, partitioning, clustering,
   and keeping `MERGE` costs down.
10. [Backing up to cloud storage](backups.md): a Parquet copy of every sync in
    GCS or S3.

**Part 4: production**

11. [Catching bad data](data-quality.md): warnings, failing a run on purpose,
    and quality checks before data goes live.
12. [Handling deleted rows](deleted-rows.md): find and remove rows the source
    no longer has.
13. [Running in production](production.md): job files, schedulers, retries,
    timeouts and housekeeping.
14. [Big tables and busy databases](performance.md): speed, memory, and reading
    without hurting the source.

```{toctree}
:hidden:

setup
first-sync
choosing-data
incremental
databases
http-apis
dataframes
clickhouse-tables
bigquery
backups
data-quality
deleted-rows
production
performance
```
