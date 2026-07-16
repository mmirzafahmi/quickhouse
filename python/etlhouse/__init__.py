"""etlhouse — fast PostgreSQL → ClickHouse ETL powered by a Rust engine.

The heavy lifting (binary ``COPY`` decoding, Arrow batching, parallel streaming,
and ClickHouse ingestion) runs in native Rust. This module is a thin, typed
Python surface over the compiled extension.

Example
-------
>>> import etlhouse
>>> src = etlhouse.Postgres("postgresql://user:pw@localhost:5432/odoo")
>>> dst = etlhouse.ClickHouse("http://localhost:8123", database="analytics")
>>> result = etlhouse.sync(
...     src, dst, dest_table="account_move_line",
...     source_table="account_move_line",
...     mode="incremental", watermark="write_date", key=["id"],
...     create_if_missing=True, parallelism=8,
...     on_progress=lambda p: print(f"{p.rows_written:,} rows @ {p.rows_per_sec:,.0f}/s"),
... )
>>> print(result)
"""

from ._etlhouse import (
    ClickHouse,
    Postgres,
    Progress,
    TransferResult,
    __version__,
    sync,
    version,
)

__all__ = [
    "Postgres",
    "ClickHouse",
    "Progress",
    "TransferResult",
    "sync",
    "version",
    "__version__",
]
