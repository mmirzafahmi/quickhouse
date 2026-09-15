"""Write a pandas DataFrame into ClickHouse.

Shows the three modes `from_pandas` supports, and the one way it differs from
every other source: `mode="incremental"` needs no watermark column. A database
source needs one to know where to resume reading; a frame does not, because the
caller already holds every row — so `key=` alone is enough to upsert on.

Run it twice. The second run upserts rather than duplicating.

Prerequisites
-------------
- `pip install 'quickhouse[pandas]'`
- A reachable ClickHouse (defaults match `docker-compose.yml`).

Environment variables:
    CH_URL, CH_DB, CH_USER, CH_PASSWORD
    DEST_TABLE
"""

import datetime as dt
import decimal
import os

import pandas as pd

import quickhouse as qh


def build_frame() -> pd.DataFrame:
    """A frame with the dtypes that are actually interesting to move.

    Each of these is a case where the naive answer is wrong: a categorical is
    dictionary-encoded in Arrow, a Decimal keeps its precision only if it is
    never routed through a float, and a tz-aware timestamp has to carry a zone
    ClickHouse can name.
    """
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "customer": pd.Categorical(["acme", "globex", "acme"]),
            "amount": [
                decimal.Decimal("19.99"),
                decimal.Decimal("249.00"),
                decimal.Decimal("5.75"),
            ],
            "qty": pd.array([2, None, 7], dtype="Int64"),  # nullable, stays exact
            "ordered_at": pd.to_datetime(
                ["2024-05-01T09:00:00Z", "2024-05-02T11:30:00Z", "2024-05-03T14:15:00Z"]
            ).tz_convert("Asia/Jakarta"),
            "delivered_on": [dt.date(2024, 5, 3), dt.date(2024, 5, 4), None],
        }
    )


def main() -> None:
    target = qh.ClickHouse(
        os.getenv("CH_URL", "http://localhost:8123"),
        database=os.getenv("CH_DB", "default"),
        user=os.getenv("CH_USER", "default"),
        password=os.getenv("CH_PASSWORD", ""),
    )
    table = os.getenv("DEST_TABLE", "orders_from_pandas")
    df = build_frame()

    # full: replace the table through a staged atomic swap.
    print(qh.from_pandas(df, target, dest_table=table, mode="full", key=["id"]))

    # incremental: upsert on key. No watermark — the frame IS the delta.
    df.loc[df["id"] == 1, "amount"] = decimal.Decimal("29.99")
    print(qh.from_pandas(df, target, dest_table=table, mode="incremental", key=["id"]))

    # append: insert straight in, no staging and no dedup. For bronze landing
    # where you know the rows are new and consolidate downstream.
    # print(qh.from_pandas(df, target, dest_table=f"{table}_raw", mode="append"))

    # polars, pyarrow Tables and DuckDB relations go through the same call —
    # the conversion is Arrow, not pandas:
    #   qh.from_pandas(pl.read_parquet("orders.parquet"), target, dest_table=table, mode="full")


if __name__ == "__main__":
    main()
