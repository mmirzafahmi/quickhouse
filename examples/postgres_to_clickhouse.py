"""Full-refresh a PostgreSQL table into ClickHouse.

The simplest quickhouse call: pick a source, a destination, and a table name.
Full-refresh mode reloads everything and swaps the result in atomically, so a
crash never leaves a half-written table.

Prerequisites
-------------
- `pip install quickhouse`
- A reachable PostgreSQL and ClickHouse. The defaults below match the repo's
  `docker-compose.yml` (`docker compose up -d`), so this runs as-is against it.

Configure via environment variables (all optional; defaults target compose):
    PG_DSN     e.g. postgresql://etl:etl@localhost:5432/etl
    CH_URL     e.g. http://localhost:8123
    CH_DB      e.g. analytics
    SRC_TABLE  source table name (default: orders)
    DEST_TABLE destination table name (default: same as SRC_TABLE)
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
    dest_table = os.getenv("DEST_TABLE", src_table)

    result = qh.sync(
        src,
        dst,
        dest_table=dest_table,
        source_table=src_table,
        key=["id"],            # used for ordering; not required for full refresh
        create_if_missing=True,
        parallelism=4,
        on_progress=lambda p: print(f"{p.rows_written:,} rows @ {p.rows_per_sec:,.0f}/s"),
    )
    print(result)


if __name__ == "__main__":
    main()
