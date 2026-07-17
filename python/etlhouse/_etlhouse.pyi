"""Type stubs for the compiled ``etlhouse._etlhouse`` extension module."""

from typing import Callable, Mapping, Optional, Sequence, Union

__version__: str

class Postgres:
    """PostgreSQL source connection descriptor.

    Parameters
    ----------
    dsn:
        libpq connection string, e.g. ``postgresql://user:pw@host:5432/db``.
        Whether TLS is used follows the standard ``sslmode`` query parameter
        (``disable`` | ``prefer`` (default) | ``require``).
    statement_timeout_secs:
        Per-connection statement timeout in seconds (0 = server default).
    ca_cert_file:
        Path to a PEM file with extra trusted CA certificate(s), trusted in
        addition to the public CA store. Needed when the server's certificate
        doesn't chain to a public CA — e.g. AWS RDS's regional CA bundle.
    """

    def __init__(
        self,
        dsn: str,
        *,
        statement_timeout_secs: int = 0,
        ca_cert_file: Optional[str] = None,
    ) -> None: ...

class MySQL:
    """MySQL source connection descriptor (e.g. AWS RDS for MySQL).

    Parameters
    ----------
    dsn:
        MySQL connection string, e.g. ``mysql://user:pw@host:3306/db``.
    statement_timeout_secs:
        Per-connection statement timeout in seconds (0 = server default).
    ca_cert_file:
        Path to a PEM file with extra trusted CA certificate(s), trusted in
        addition to the public CA store. Needed when the server's certificate
        doesn't chain to a public CA — e.g. AWS RDS's regional CA bundle.
    require_tls:
        Require TLS for the connection. MySQL has no `sslmode`-style DSN
        parameter convention, so this is explicit (unlike ``Postgres``).
    """

    def __init__(
        self,
        dsn: str,
        *,
        statement_timeout_secs: int = 0,
        ca_cert_file: Optional[str] = None,
        require_tls: bool = False,
    ) -> None: ...

class BigQuery:
    """Google BigQuery source connection descriptor.

    Parameters
    ----------
    project_id:
        GCP project ID. If omitted, resolved from the credentials (both ADC
        and service-account key files normally embed/resolve a project ID).
    credentials_file:
        Path to a service-account JSON key file. If omitted, falls back to
        Application Default Credentials (``GOOGLE_APPLICATION_CREDENTIALS``,
        ``GOOGLE_APPLICATION_CREDENTIALS_JSON``, the GCE/GKE metadata server,
        or the ``gcloud`` CLI's well-known ADC file).

    Notes
    -----
    ``source_table`` should be ``"dataset.table"`` or
    ``"project.dataset.table"``. Reads use the BigQuery Storage Read API;
    ``parallelism`` is passed through as BigQuery's own stream-count hint,
    but rows are still consumed on a single connection here (BigQuery does
    the parallel work server-side rather than via multiple local
    connections, unlike the Postgres/MySQL sources).
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        *,
        credentials_file: Optional[str] = None,
    ) -> None: ...

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
    source: Union[Postgres, MySQL, BigQuery],
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
    """Transfer one table from PostgreSQL, MySQL, or BigQuery to ClickHouse.

    ``source`` may be a ``Postgres``, ``MySQL``, or ``BigQuery`` connection
    descriptor; everything else about the call is identical either way.
    Either ``source_table`` or ``source_query`` must be provided. For
    ``mode="incremental"``, ``watermark`` is required and only rows newer than
    the last recorded watermark are copied.
    """
    ...

def version() -> str: ...
