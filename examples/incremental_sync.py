"""Incrementally sync a PostgreSQL table into ClickHouse on a watermark.

Incremental mode copies only rows whose `watermark` column is newer than the
last recorded high-water mark (tracked in a small `_quickhouse_state` table in
the destination). It's idempotent: safe to re-run or retry, and running it on a
schedule keeps the destination current without re-reading the whole table.

Run it twice — the second run copies only what changed since the first.

Prerequisites
-------------
- `pip install quickhouse`
- PostgreSQL + ClickHouse reachable (defaults match `docker-compose.yml`).
- The source table needs a monotonically-increasing timestamp column
  (`updated_at` by default) and a primary key.

Environment variables (optional; defaults target compose):
    PG_DSN, CH_URL, CH_DB, SRC_TABLE, DEST_TABLE
    WATERMARK   timestamp column driving the incremental cursor (default: updated_at)
    KEY         primary-key column (default: id)
"""

import os

import quickhouse as qh


def main() -> None:
    src = qh.Postgres(os.getenv("PG_DSN", "postgresql://etl:etl@localhost:5432/etl"))
    dst = qh.ClickHouse(
        os.getenv("CH_URL", "http://localhost:8123"),
        database=os.getenv("CH_DB", "analytics"),
    )
    src_table = os.getenv("SRC_TABLE", "orders")

    result = qh.sync(
        src,
        dst,
        dest_table=os.getenv("DEST_TABLE", src_table),
        source_table=src_table,
        mode="incremental",
        watermark=os.getenv("WATERMARK", "updated_at"),
        key=[os.getenv("KEY", "id")],
        create_if_missing=True,
        # Re-include a trailing window each run to catch late/edited rows that
        # don't monotonically bump the watermark (dedup'd by `key`).
        lookback_seconds=int(os.getenv("LOOKBACK_SECONDS", "0")),
    )
    print(result)
    print("new_watermark:", result.new_watermark)


if __name__ == "__main__":
    main()
