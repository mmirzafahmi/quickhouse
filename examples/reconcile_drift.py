"""Measure — and optionally repair — the drift between a PostgreSQL source that
hard-deletes rows and a ClickHouse destination synced from it.

An incremental sync is insert-and-update only. It finds rows whose watermark
moved; a row the source *deleted* has no watermark to move, so nothing about it
ever reaches the destination again and it stays there forever. Against an ERP
that cancels reservations, a queue that drains, or a "soft delete" that is
really a DELETE, the destination drifts one-directionally and without bound.

The drift also hides well, because it is lopsided. One measured production table
carried +1.30% phantom rows against +0.0168% on a quantity sum: every
COUNT-based model over-reported materially while every SUM-based one looked
fine. You cannot judge exposure from row counts alone, which is why this script
reports the keyset diff rather than a count comparison.

Run it read-only first (the default). Only pass --repair once you have looked at
what it found.

    python examples/reconcile_drift.py \
        --window "2026-07-01" --window-end "2026-08-01" \
        stock_move_line stock_move_line

    python examples/reconcile_drift.py ... --repair --max-delete 50000
"""

from __future__ import annotations

import argparse
import os
import sys

import quickhouse as qh


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("source_table", help="table in the source database")
    p.add_argument("dest_table", help="table in the ClickHouse database")
    p.add_argument("--key", default="id", help="identifying column (default: id)")
    p.add_argument(
        "--window-column",
        default="create_date",
        help="immutable column to bound the comparison (default: create_date). "
        "Must be immutable per key: a write_date/updated_at column moves rows "
        "between windows and would read as drift.",
    )
    p.add_argument("--window", required=True, help="inclusive lower bound, e.g. 2026-07-01")
    p.add_argument("--window-end", required=True, help="exclusive upper bound")
    p.add_argument(
        "--repair",
        action="store_true",
        help="delete the orphans (default: measure only)",
    )
    p.add_argument(
        "--max-delete",
        type=int,
        default=0,
        help="refuse to delete above this many orphan keys (0 = no ceiling). "
        "A reconcile is only as good as its window; this is the guard against "
        "acting on a window that means something different on each side.",
    )
    args = p.parse_args()

    src = qh.Postgres(os.environ["PG_DSN"])
    dst = qh.ClickHouse(
        os.environ.get("CH_URL", "http://localhost:8123"),
        database=os.environ.get("CH_DB", "default"),
        user=os.environ.get("CH_USER", "default"),
        password=os.environ.get("CH_PASSWORD", ""),
    )

    window = (
        f"{args.window_column} >= '{args.window}' AND {args.window_column} < '{args.window_end}'"
    )
    result = qh.reconcile_keys(
        src,
        dst,
        args.dest_table,
        key=args.key,
        source_table=args.source_table,
        window=window,
        delete=args.repair,
        max_delete_keys=args.max_delete,
    )

    print(f"window            {window}")
    print(f"source keys       {result.source_keys:,}")
    print(f"destination keys  {result.dest_keys:,}")
    drift = result.orphan_keys / result.source_keys * 100 if result.source_keys else 0.0
    print(f"orphans           {result.orphan_keys:,}  (+{drift:.3f}% vs source)")
    print(f"missing           {result.missing_keys:,}")
    print(f"rows deleted      {result.rows_deleted:,}")
    print(f"took              {result.duration_secs:.1f}s")
    if result.orphan_sample:
        print(f"orphan sample     {', '.join(result.orphan_sample)}")
    if result.missing_sample:
        print(f"missing sample    {', '.join(result.missing_sample)}")

    if result.orphan_keys and not args.repair:
        print("\nRe-run with --repair to delete these. Nothing was changed.", file=sys.stderr)
    # A non-zero exit on unrepaired drift makes this usable as a scheduled check.
    return 1 if result.orphan_keys and not args.repair else 0


if __name__ == "__main__":
    raise SystemExit(main())
