"""Benchmark: read an existing PostgreSQL table into ClickHouse and report throughput.

Unlike bench_transfer.py this seeds nothing, so it can run against a real
database such as a read replica. On PostgreSQL it runs one index-backed probe
(to find the window's lowest id) plus each transfer's own reads. It reads the
last --rows rows by id, fully refreshes a scratch ClickHouse table
(`bench_<table>`) --runs times, and drops that table at the end.

Run it once per quickhouse build to compare them, e.g. on `main` and on a branch:

    QUICKHOUSE_PG_DSN=postgresql://user:pass@replica:5432/db \\
    QUICKHOUSE_CH_URL=http://localhost:8123 \\
    python benchmarks/bench_pg_table.py --table sale_order_line --rows 300000 --runs 3
"""

from __future__ import annotations

import argparse
import os
import statistics
import urllib.request

import psycopg

import quickhouse

PG_DSN = os.environ.get("QUICKHOUSE_PG_DSN", "postgresql://etl:etl@localhost:5432/etl")
# A private CA to trust for the source's TLS certificate, e.g. AWS RDS's bundle.
PG_CA_CERT_FILE = os.environ.get("QUICKHOUSE_PG_CA_CERT_FILE")
CH_URL = os.environ.get("QUICKHOUSE_CH_URL", "http://localhost:8123")


def window_lower_id(table: str, rows: int) -> int:
    """The smallest id among the last `rows` rows, via a backward PK index scan."""
    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        row = conn.execute(
            f'SELECT id FROM "{table}" ORDER BY id DESC LIMIT 1 OFFSET %s', (rows - 1,)
        ).fetchone()
    if row is None:
        raise SystemExit(f"{table} has fewer than {rows:,} rows")
    return row[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True)
    ap.add_argument("--rows", type=int, default=300_000)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--parallelism", type=int, default=1)
    ap.add_argument("--compression", default="zstd")
    ap.add_argument("--exclude", default="", help="comma-separated columns to skip")
    args = ap.parse_args()

    lo = window_lower_id(args.table, args.rows)
    source_query = f'SELECT * FROM "{args.table}" WHERE id >= {lo}'
    dest = f"bench_{args.table}"
    src = quickhouse.Postgres(PG_DSN, ca_cert_file=PG_CA_CERT_FILE)
    dst = quickhouse.ClickHouse(CH_URL, compression=args.compression)
    # `source_query` passes `id` through unchanged, so it can drive range
    # partitioning when --parallelism asks for more than one stream.
    fan_out = {"partition_source_expr": "id"} if args.parallelism > 1 else {}

    print(f"quickhouse {quickhouse.__version__}: {source_query}")
    walls = []
    try:
        for run in range(1, args.runs + 1):
            r = quickhouse.sync(
                src,
                dst,
                dest_table=dest,
                source_query=source_query,
                mode="full",
                key=["id"],
                parallelism=args.parallelism,
                exclude=[c for c in args.exclude.split(",") if c] or None,
                # A live replica can lose rows between runs; that is not a failure here.
                allow_full_refresh_shrink=True,
                application_name="quickhouse-bench",
                **fan_out,
            )
            walls.append(r.duration_secs)
            print(
                f"  run {run}: {r.rows_written:,} rows in {r.duration_secs:.2f}s "
                f"({r.rows_written / r.duration_secs:,.0f} rows/s)  "
                f"read {r.read_secs:.2f}s  stage {r.stage_secs:.2f}s  "
                f"promote {r.promote_secs:.2f}s"
            )
    finally:
        drop = f"DROP TABLE IF EXISTS `{dest}`".encode()
        urllib.request.urlopen(urllib.request.Request(CH_URL, data=drop)).read()
    print(f"median {statistics.median(walls):.2f}s over {len(walls)} runs")


if __name__ == "__main__":
    main()
