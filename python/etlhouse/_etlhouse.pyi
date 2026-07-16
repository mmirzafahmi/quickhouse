"""Type stubs for the compiled ``etlhouse._etlhouse`` extension module."""

from typing import Callable, Mapping, Optional, Sequence

__version__: str

class Postgres:
    """PostgreSQL source connection descriptor.

    Parameters
    ----------
    dsn:
        libpq connection string, e.g. ``postgresql://user:pw@host:5432/db``.
    statement_timeout_secs:
        Per-connection statement timeout in seconds (0 = server default).
    """

    def __init__(self, dsn: str, *, statement_timeout_secs: int = 0) -> None: ...

class ClickHouse:
    """ClickHouse destination connection descriptor.

    Parameters
    ----------
    url:
        Base HTTP(S) URL, e.g. ``http://host:8123``.
    database, user, password:
        Target database and credentials.
    compression:
        HTTP insert body compression: ``"gzip"`` (default) or ``"none"``.
    """

    def __init__(
        self,
        url: str,
        *,
        database: str = "default",
        user: str = "default",
        password: str = "",
        compression: str = "gzip",
    ) -> None: ...

class Progress:
    """Live progress snapshot passed to ``on_progress``."""

    rows_read: int
    rows_written: int
    bytes_written: int
    elapsed_secs: float
    rows_per_sec: float

class TransferResult:
    """Summary returned by :func:`sync`."""

    rows_read: int
    rows_written: int
    bytes_written: int
    duration_secs: float
    new_watermark: Optional[str]

def sync(
    source: Postgres,
    target: ClickHouse,
    dest_table: str,
    *,
    source_table: Optional[str] = None,
    source_query: Optional[str] = None,
    mode: str = "full",
    watermark: Optional[str] = None,
    key: Optional[Sequence[str]] = None,
    create_if_missing: bool = True,
    engine: Optional[str] = None,
    order_by: Optional[Sequence[str]] = None,
    partition_by: Optional[str] = None,
    primary_key: Optional[Sequence[str]] = None,
    parallelism: int = 4,
    batch_rows: int = 100_000,
    partition_column: Optional[str] = None,
    type_overrides: Optional[Mapping[str, str]] = None,
    rename: Optional[Mapping[str, str]] = None,
    include: Optional[Sequence[str]] = None,
    exclude: Optional[Sequence[str]] = None,
    on_progress: Optional[Callable[[Progress], None]] = None,
) -> TransferResult:
    """Transfer one table from PostgreSQL to ClickHouse.

    Either ``source_table`` or ``source_query`` must be provided. For
    ``mode="incremental"``, ``watermark`` is required and only rows newer than
    the last recorded watermark are copied.
    """
    ...

def version() -> str: ...
