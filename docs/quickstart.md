# Quickstart

This page takes you from an empty environment to a working transfer, then shows
the two ways quickhouse keeps a table up to date.

## 1. Install

```bash
pip install quickhouse
```

Need a sandbox? The repository ships a
[`docker-compose.yml`](https://github.com/mmirzafahmi/quickhouse/blob/main/docker-compose.yml)
that starts PostgreSQL, MySQL, ClickHouse, and MinIO locally:

```bash
docker compose up -d
```

## 2. Your first sync (full refresh)

A full refresh loads the whole source table into a staging table, then swaps it
into place atomically — a crash mid-run never leaves the destination partial.

```python
import quickhouse as qh

src = qh.Postgres("postgresql://user:pw@localhost:5432/shop")
dst = qh.ClickHouse("http://localhost:8123", database="analytics")

result = qh.sync(
    src, dst,
    dest_table="orders",
    source_table="orders",
    on_progress=lambda p: print(f"{p.rows_written:,} rows @ {p.rows_per_sec:,.0f}/s"),
)
print(result)
# TransferResult(rows_read=..., rows_written=..., bytes_written=..., duration_secs=..., new_watermark=None)
```

quickhouse inspects the source schema, creates `orders` in ClickHouse with a
sensible `MergeTree` DDL (`create_if_missing=True` by default), streams the rows
across in parallel, and returns a {class}`~quickhouse.TransferResult`.

## 3. Keep it fresh (incremental)

Point quickhouse at a monotonic **watermark** column and it tracks a high-water
mark in a small state table in the destination, copying only newer rows. Updated
rows are deduplicated on `key`. Re-running with no new data does nothing.

```python
qh.sync(
    src, dst,
    dest_table="orders",
    source_table="orders",
    mode="incremental",
    watermark="updated_at",     # required for incremental
    key=["id"],                 # dedup key / ORDER BY
)
```

Run it once, then run it again — the second run is idempotent. This is the call
you would put on a cron/Airflow schedule.

## 4. The options you'll reach for most

```python
qh.sync(
    src, dst,
    dest_table="orders",
    source_table="orders",        # or source_query="SELECT ..."
    mode="incremental",
    watermark="updated_at",
    key=["id"],
    parallelism=8,                # concurrent read streams
    exclude=["internal_notes"],   # drop columns
    rename={"amount": "amt"},     # rename columns
    on_progress=lambda p: print(f"{p.rows_written:,} rows @ {p.rows_per_sec:,.0f}/s"),
)
```

## Switching engines

Pick a source and a destination by constructing the matching object —
everything else about `sync()` stays the same:

```python
# sources
qh.Postgres("postgresql://user:pw@host:5432/db")
qh.MySQL("mysql://user:pw@host:3306/db", require_tls=True)
qh.BigQuery("my-gcp-project")                       # source_table="dataset.table"

# destinations
qh.ClickHouse("http://host:8123", database="analytics")
qh.BigQuery("my-gcp-project", dataset_id="analytics")
```

## Where to next

- [Sources](guide/sources/index.md) — every source connection descriptor, auth,
  and the HTTP API sources.
- [Destinations](guide/destinations/index.md) — ClickHouse and BigQuery, the S3
  archive, and destination DDL.
- [Sync modes](guide/sync-modes.md) — full vs. incremental, the watermark cursor,
  and backfills.
- [API reference](api.md) — the complete `sync()` signature.
