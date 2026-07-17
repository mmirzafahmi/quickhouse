"""etlhouse — fast PostgreSQL/MySQL → ClickHouse ETL powered by a Rust engine.

The heavy lifting (wire-protocol decoding, Arrow batching, parallel streaming,
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

``source`` also accepts an :class:`etlhouse.MySQL` connection descriptor for
MySQL (e.g. AWS RDS for MySQL) sources, or an :class:`etlhouse.BigQuery`
descriptor for Google BigQuery — everything else about ``sync()`` stays the
same:

>>> src = etlhouse.BigQuery("my-gcp-project")  # or credentials_file="key.json"
>>> etlhouse.sync(src, dst, dest_table="t", source_table="my_dataset.my_table")

For a tqdm progress bar instead of a print callback (``pip install etlhouse[progress]``):

>>> with etlhouse.progress_bar() as on_progress:
...     etlhouse.sync(src, dst, dest_table="t", source_table="t", on_progress=on_progress)
"""

from ._etlhouse import (
    BigQuery,
    ClickHouse,
    MySQL,
    Postgres,
    Progress,
    TransferResult,
    __version__,
    sync,
    version,
)
from .progress import progress_bar

__all__ = [
    "Postgres",
    "MySQL",
    "BigQuery",
    "ClickHouse",
    "Progress",
    "TransferResult",
    "sync",
    "version",
    "__version__",
    "progress_bar",
]
