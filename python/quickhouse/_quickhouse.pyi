"""Type stubs for the compiled ``quickhouse._quickhouse`` extension module."""

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
    """Google BigQuery connection descriptor — usable as either a ``source``
    or a ``target`` for :func:`sync`.

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
    dataset_id:
        Destination dataset (BigQuery's equivalent of ClickHouse's
        ``database``) — **required** when this is used as ``target=``;
        unused as a ``source=`` (``source_table``/``source_query`` already
        carry the dataset there).

    Notes
    -----
    As a source: ``source_table`` should be ``"dataset.table"`` or
    ``"project.dataset.table"``. Reads use the BigQuery Storage Read API;
    ``parallelism`` is passed through as BigQuery's own stream-count hint,
    but rows are still consumed on a single connection here (BigQuery does
    the parallel work server-side rather than via multiple local
    connections, unlike the Postgres/MySQL sources).

    As a destination: writes use the ``tabledata.insertAll`` streaming-insert
    API (JSON rows over REST); the full-refresh atomic swap uses a
    ``WRITE_TRUNCATE`` copy job (BigQuery has no `EXCHANGE TABLES`
    equivalent). ``partition_by`` must be a bare ``DATE``/``DATETIME``/
    ``TIMESTAMP`` column name (not a SQL expression like ClickHouse's);
    ``order_by``/``key`` become clustering columns (at most 4 total).
    Incremental mode has no engine-level dedup here (unlike ClickHouse's
    `ReplacingMergeTree`), so it upserts via a ``MERGE`` statement matched on
    ``key`` instead — making ``key`` **required** for incremental syncs into
    BigQuery specifically.
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        *,
        credentials_file: Optional[str] = None,
        dataset_id: Optional[str] = None,
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
        HTTP insert body compression: ``"zstd"`` (default), ``"gzip"``, or
        ``"none"``. zstd-fast is faster than gzip at a similar/better ratio;
        use ``"none"`` on a fast local network where CPU, not bandwidth, is
        the bottleneck.
    """

    def __init__(
        self,
        url: str,
        *,
        database: str = "default",
        user: str = "default",
        password: str = "",
        compression: str = "zstd",
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
    target: Union[ClickHouse, BigQuery],
    dest_table: str,
    *,
    source_table: Optional[str] = None,
    source_query: Optional[str] = None,
    mode: str = "full",
    watermark: Optional[str] = None,
    lookback_seconds: int = 0,
    key: Optional[Sequence[str]] = None,
    create_if_missing: bool = True,
    engine: Optional[str] = None,
    order_by: Optional[Sequence[str]] = None,
    partition_by: Optional[str] = None,
    primary_key: Optional[Sequence[str]] = None,
    parallelism: int = 4,
    batch_rows: int = 100_000,
    batch_bytes: int = 4_194_304,
    max_memory_bytes: int = 536_870_912,
    partition_column: Optional[str] = None,
    type_overrides: Optional[Mapping[str, str]] = None,
    rename: Optional[Mapping[str, str]] = None,
    include: Optional[Sequence[str]] = None,
    exclude: Optional[Sequence[str]] = None,
    on_progress: Optional[Callable[[Progress], None]] = None,
) -> TransferResult:
    """Transfer one table from PostgreSQL, MySQL, or BigQuery into ClickHouse
    or BigQuery.

    ``source`` may be a ``Postgres``, ``MySQL``, or ``BigQuery`` connection
    descriptor; ``target`` may be a ``ClickHouse`` or ``BigQuery`` one (the
    same ``BigQuery`` class works for either role — see its doc comment).
    Everything else about the call is identical regardless of which engines
    are used. Either ``source_table`` or ``source_query`` must be provided.
    For ``mode="incremental"``, ``watermark`` is required and only rows newer
    than the last recorded watermark are copied. In ``mode="full"`` the
    watermark is unused and ignored (cleared to ``None``), and the returned
    ``new_watermark`` is ``None``.

    ``lookback_seconds`` widens the tracked watermark's lower bound by this
    many seconds before filtering, so a run re-includes a trailing window of
    already-synced rows — catches late-arriving or edited rows that don't
    monotonically bump the watermark (e.g. a daily sync run with
    ``lookback_seconds=3 * 86400`` to safely reprocess the last 3 days).
    Requires ``key`` or ``order_by`` to be set (the destination's
    upsert/dedup replaces the re-synced overlap instead of duplicating it —
    see the dedup note below) and ``watermark`` to resolve to a date or
    timestamp column. For a BigQuery source, ``DATE``-typed watermarks have
    no sub-day granularity, so a sub-day ``lookback_seconds`` rounds *up* to
    a whole day. Default ``0`` disables lookback entirely (byte-identical to
    the plain watermark filter).

    ``engine``/``order_by``/``partition_by``/``primary_key``/``key`` are
    interpreted per destination: for ClickHouse they drive `MergeTree`-family
    DDL as before; for BigQuery, ``engine`` is ignored, ``partition_by`` must
    be a bare date/timestamp column name, and ``order_by``/``key`` become
    clustering columns (at most 4 total — see ``BigQuery``'s doc comment).

    Incremental-mode dedup of an updated row (same key, newer watermark)
    differs by destination: ClickHouse dedupes lazily via
    ``ReplacingMergeTree`` at merge time; BigQuery has no engine-level
    equivalent, so writes are staged then upserted via a ``MERGE`` statement
    matched on ``key`` — which is therefore **required** for BigQuery when
    ``mode="incremental"`` (unlike everywhere else it's optional). This bills
    for bytes scanned in both tables (unlike the free ``insertAll`` path used
    for full-refresh), but is naturally idempotent: a crashed/retried
    incremental run re-applies the same key-matched rows rather than
    duplicating them.

    Memory vs. batch sizing:

    - ``batch_rows`` / ``batch_bytes`` control how big each individual Arrow
      batch (and thus each insert) is — a throughput/overhead granularity knob.
    - ``max_memory_bytes`` is the hard ceiling on *total* in-flight batch
      memory across all partitions and all uploads currently in flight,
      measured against each batch's real Arrow allocation. Decoding overlaps
      with concurrent uploads and blocks (backpressure) when this ceiling is
      reached, so peak RSS stays bounded regardless of ``parallelism`` or row
      width. Default 512 MiB; ``0`` disables the ceiling (unbounded).
    """
    ...

def version() -> str: ...
