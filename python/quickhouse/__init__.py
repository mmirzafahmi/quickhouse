"""quickhouse — fast PostgreSQL/MySQL/BigQuery/ClickHouse and pandas ETL into
ClickHouse or BigQuery, powered by a Rust engine.

The heavy lifting (wire-protocol decoding, Arrow batching, parallel streaming,
and destination ingestion) runs in native Rust. This module is a thin, typed
Python surface over the compiled extension.

Example
-------
>>> import quickhouse
>>> src = quickhouse.Postgres("postgresql://user:pw@localhost:5432/shop")
>>> dst = quickhouse.ClickHouse("http://localhost:8123", database="analytics")
>>> result = quickhouse.sync(
...     src, dst, dest_table="orders",
...     source_table="orders",
...     mode="incremental", watermark="updated_at", key=["id"],
...     create_if_missing=True, parallelism=8,
...     on_progress=lambda p: print(f"{p.rows_written:,} rows @ {p.rows_per_sec:,.0f}/s"),
... )
>>> print(result)

``source`` also accepts an :class:`quickhouse.MySQL` connection descriptor for
MySQL (e.g. AWS RDS for MySQL) sources, or an :class:`quickhouse.BigQuery`
descriptor for Google BigQuery — everything else about ``sync()`` stays the
same:

>>> src = quickhouse.BigQuery("my-gcp-project")  # or credentials_file="key.json"
>>> quickhouse.sync(src, dst, dest_table="t", source_table="my_dataset.my_table")

``target`` also accepts a :class:`quickhouse.BigQuery` descriptor (with
``dataset_id`` set) to write into BigQuery instead of ClickHouse — the same
class works for either role:

>>> dst_bq = quickhouse.BigQuery("my-gcp-project", dataset_id="analytics")
>>> quickhouse.sync(src, dst_bq, dest_table="orders", source_table="orders")

:class:`quickhouse.ClickHouse` is dual-role too, so a ClickHouse table can be
the ``source`` — a cross-cluster copy, or publishing a mart into BigQuery:

>>> src_ch = quickhouse.ClickHouse("http://ch-a:8123", database="raw")
>>> dst_ch = quickhouse.ClickHouse("http://ch-b:8123", database="analytics")
>>> quickhouse.sync(src_ch, dst_ch, dest_table="orders", source_table="orders")

A DataFrame you already hold goes in through :func:`quickhouse.from_pandas`
(``pip install quickhouse[pandas]``) — a pandas, polars or pyarrow frame, with
the same modes, DDL and warnings as any other source. ``mode="incremental"``
upserts on ``key`` and needs no watermark, since the frame is the delta:

>>> quickhouse.from_pandas(df, dst, dest_table="orders",
...                        mode="incremental", key=["id"])

For a tqdm progress bar instead of a print callback (``pip install quickhouse[progress]``):

>>> with quickhouse.progress_bar() as on_progress:
...     quickhouse.sync(src, dst, dest_table="t", source_table="t", on_progress=on_progress)
"""

from ._quickhouse import (
    AppsFlyer,
    BigQuery,
    CleverTap,
    ClickHouse,
    HttpApi,
    MySQL,
    Postgres,
    Progress,
    ReconcileResult,
    S3Archive,
    TransferResult,
    TransferWarning,
    __version__,
    reconcile_keys,
    sync,
    version,
)
from .pandas import QuickhouseWarning, from_pandas
from .progress import progress_bar
from .quality import Validation, ValidationFailed

__all__ = [
    "Postgres",
    "MySQL",
    "BigQuery",
    "CleverTap",
    "AppsFlyer",
    "HttpApi",
    "ClickHouse",
    "S3Archive",
    "Progress",
    "TransferResult",
    "TransferWarning",
    "ReconcileResult",
    "Validation",
    "ValidationFailed",
    "QuickhouseWarning",
    "sync",
    "from_pandas",
    "reconcile_keys",
    "version",
    "__version__",
    "progress_bar",
]
