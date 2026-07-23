"""Benchmark: transfer N rows from PostgreSQL to ClickHouse and report throughput.

Seeds a table server-side via generate_series (avoids slow client-side row-by-row
inserts, which would dominate the measurement), then runs quickhouse.sync at a few
parallelism levels, reporting rows/sec, MB/sec, and peak RSS.

Usage:
    docker compose up -d
    python benchmarks/bench_transfer.py [--rows 1000000] [--parallelism 1,4,8]
"""

from __future__ import annotations

import argparse
import os
import threading
import time

import psycopg

import quickhouse

PG_DSN = os.environ.get("QUICKHOUSE_PG_DSN", "postgresql://etl:etl@localhost:5432/etl")
CH_URL = os.environ.get("QUICKHOUSE_CH_URL", "http://localhost:8123")
TABLE = "bench_transfer"


# 20 columns modeled on a typical accounting ledger line (a mix of bigint FKs,
# some nullable, floats, text, bool, and timestamps), so the benchmark reflects
# a realistic wide production column mix rather than a handful of synthetic types.
def seed(conn: psycopg.Connection, rows: int) -> float:
    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{TABLE}"')
        cur.execute(
            f"""
            CREATE TABLE "{TABLE}" (
                id              bigint PRIMARY KEY,
                move_name       text,
                account_id      bigint,
                partner_id      bigint,
                product_id      bigint,
                name            text,
                quantity        double precision,
                price_unit      double precision,
                discount        double precision,
                debit           double precision,
                credit          double precision,
                balance         double precision,
                amount_currency double precision,
                currency_id     integer,
                company_id      integer,
                state           text,
                blocked         boolean,
                reconciled      boolean,
                date_maturity   timestamp,
                write_date      timestamp NOT NULL
            )
            """
        )
        cur.execute(
            f"""
            INSERT INTO "{TABLE}" (
                id, move_name, account_id, partner_id, product_id, name,
                quantity, price_unit, discount, debit, credit, balance,
                amount_currency, currency_id, company_id, state,
                blocked, reconciled, date_maturity, write_date
            )
            SELECT
                i,
                'INV/2024/' || i,
                (i %% 500) + 1,
                CASE WHEN i %% 7 = 0 THEN NULL ELSE (i %% 10000) + 1 END,
                CASE WHEN i %% 5 = 0 THEN NULL ELSE (i %% 50000) + 1 END,
                'Line item ' || i,
                (i %% 100) + 0.5,
                (i %% 1000) * 1.25,
                (i %% 20)::float,
                CASE WHEN i %% 2 = 0 THEN (i %% 1000) * 1.1 ELSE 0 END,
                CASE WHEN i %% 2 = 1 THEN (i %% 1000) * 1.1 ELSE 0 END,
                CASE WHEN i %% 2 = 0 THEN (i %% 1000) * 1.1 ELSE -(i %% 1000) * 1.1 END,
                (i %% 1000) * 1.3,
                CASE WHEN i %% 3 = 0 THEN NULL ELSE (i %% 5) + 1 END,
                (i %% 3) + 1,
                (ARRAY['draft', 'posted', 'cancel'])[(i %% 3) + 1],
                (i %% 13 = 0),
                (i %% 2 = 0),
                CASE WHEN i %% 6 = 0 THEN NULL ELSE TIMESTAMP '2024-01-01' + (i || ' seconds')::interval END,
                TIMESTAMP '2024-01-01' + (i || ' seconds')::interval
            FROM generate_series(1, %s) AS i
            """,
            (rows,),
        )
    conn.commit()
    return time.time() - t0


def read_rss_mb() -> float | None:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except FileNotFoundError:
        return None
    return None


class RssSampler:
    def __init__(self, interval: float = 0.2):
        self.interval = interval
        self.peak_mb = 0.0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self._stop.is_set():
            rss = read_rss_mb()
            if rss is not None and rss > self.peak_mb:
                self.peak_mb = rss
            self._stop.wait(self.interval)

    def __enter__(self):
        self.peak_mb = read_rss_mb() or 0.0
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join()


def run_one(src, dst, rows: int, parallelism: int, batch_rows: int) -> None:
    dest_table = f"{TABLE}_p{parallelism}"
    last = {"rows": 0}

    def on_progress(p):
        last["rows"] = p.rows_written
        print(
            f"\r  {p.rows_written:>9,}/{rows:,} rows | {p.rows_per_sec:>9,.0f} rows/s",
            end="",
            flush=True,
        )

    with RssSampler() as rss:
        result = quickhouse.sync(
            src,
            dst,
            dest_table=dest_table,
            source_table=TABLE,
            mode="full",
            key=["id"],
            create_if_missing=True,
            parallelism=parallelism,
            batch_rows=batch_rows,
            on_progress=on_progress,
        )
    print()

    rows_per_sec = result.rows_written / result.duration_secs if result.duration_secs else 0
    mb_per_sec = (result.bytes_written / 1e6) / result.duration_secs if result.duration_secs else 0
    print(
        f"  parallelism={parallelism:<3} duration={result.duration_secs:>6.2f}s  "
        f"rows_written={result.rows_written:>9,}  "
        f"{rows_per_sec:>9,.0f} rows/s  {mb_per_sec:>7,.1f} MB/s  "
        f"peak_rss={rss.peak_mb:>7,.0f} MB"
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--parallelism", type=str, default="1,4,8")
    ap.add_argument("--batch-rows", type=int, default=100_000)
    args = ap.parse_args()

    conn = psycopg.connect(PG_DSN, autocommit=True)
    print(f"Seeding {args.rows:,} rows into '{TABLE}' ...")
    seed_secs = seed(conn, args.rows)
    print(f"Seeded in {seed_secs:.1f}s\n")

    src = quickhouse.Postgres(PG_DSN)
    dst = quickhouse.ClickHouse(CH_URL, database="default")

    print(f"{'':1}{'RESULTS':-<70}")
    for p in [int(x) for x in args.parallelism.split(",")]:
        print(f"--- parallelism={p} ---")
        run_one(src, dst, args.rows, p, args.batch_rows)


if __name__ == "__main__":
    main()
