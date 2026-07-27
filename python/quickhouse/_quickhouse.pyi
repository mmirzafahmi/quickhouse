"""Type stubs for the compiled ``quickhouse._quickhouse`` extension module."""

from typing import Callable, Mapping, Optional, Sequence, Tuple, Union

# A declared API-source schema: a list of (name, bq_type) / (name, bq_type,
# path) tuples, or a {name: bq_type} dict.
_ApiColumns = Union[Sequence[Union[Tuple[str, str], Tuple[str, str, str]]], Mapping[str, str]]

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
        dsn: Optional[str] = None,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        statement_timeout_secs: int = 0,
        ca_cert_file: Optional[str] = None,
        client_cert_file: Optional[str] = None,
        client_key_file: Optional[str] = None,
    ) -> None:
        """Pass either ``dsn`` or discrete ``host``/``port``/``user``/
        ``password``/``database`` fields (not both). The discrete fields are
        percent-encoded and assembled into a DSN. For mTLS (client-certificate
        auth), set ``client_cert_file`` and ``client_key_file`` together (both
        PEM)."""
        ...

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
        dsn: Optional[str] = None,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        statement_timeout_secs: int = 0,
        ca_cert_file: Optional[str] = None,
        require_tls: bool = False,
        client_cert_file: Optional[str] = None,
        client_key_file: Optional[str] = None,
    ) -> None:
        """Pass either ``dsn`` or discrete ``host``/``port``/``user``/
        ``password``/``database`` fields (not both). The discrete fields are
        percent-encoded and assembled into a DSN. For mTLS (client-certificate
        auth), set ``client_cert_file`` and ``client_key_file`` together (DER or
        PEM)."""
        ...

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
    write_method:
        How rows are written when this is a ``target=`` (ignored as a
        ``source=``). ``"insert_all"`` (default) uses ``tabledata.insertAll``
        (JSON over REST — proven, but billed and lower-throughput).
        ``"storage_write"`` uses the BigQuery Storage Write API (gRPC +
        protobuf — free and higher-throughput). Both share the same atomic
        swap / MERGE flow; only the row-insert transport differs.

    Notes
    -----
    As a source: ``source_table`` should be ``"dataset.table"`` or
    ``"project.dataset.table"``. Reads use the BigQuery Storage Read API;
    ``parallelism`` is passed through as BigQuery's own stream-count hint,
    but rows are still consumed on a single connection here (BigQuery does
    the parallel work server-side rather than via multiple local
    connections, unlike the Postgres/MySQL sources).

    As a destination: rows are written via ``write_method`` (see above); the
    full-refresh atomic swap uses a ``WRITE_TRUNCATE`` copy job (BigQuery has
    no `EXCHANGE TABLES` equivalent). ``partition_by`` must be a bare
    ``DATE``/``DATETIME``/``TIMESTAMP`` column name (not a SQL expression like
    ClickHouse's); ``order_by``/``key`` become clustering columns (at most 4
    total). Incremental mode has no engine-level dedup here (unlike
    ClickHouse's `ReplacingMergeTree`), so it upserts via a ``MERGE``
    statement matched on ``key`` instead — making ``key`` **required** for
    incremental syncs into BigQuery specifically.
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        *,
        credentials_file: Optional[str] = None,
        credentials_json: Optional[str] = None,
        dataset_id: Optional[str] = None,
        write_method: str = "insert_all",
    ) -> None:
        """``credentials_json`` holds inline service-account JSON key contents
        (e.g. loaded from a secrets manager) as an alternative to
        ``credentials_file``; it takes precedence when both are set."""
        ...

class S3Archive:
    """Optional S3 (or S3-compatible, e.g. MinIO) data-lake archive attached
    to a :class:`ClickHouse` destination via its ``archive=`` parameter.

    Every batch synced into ClickHouse is also written as Parquet — one
    streamed file per parallel partition, never fully buffered in memory —
    to ``s3://{bucket}/{prefix}/{dest_table}/dt=<date>/run=<id>/
    part-<partition>.parquet``. A secondary, best-effort-free backup/
    historical side channel: omitting ``archive`` entirely disables this and
    has zero effect on the ClickHouse write path. A persistent S3 failure
    fails the whole ``sync()`` call (matching how the ClickHouse insert path
    itself already behaves), so the archive never silently falls behind.

    Parameters
    ----------
    bucket:
        Target S3 bucket (required).
    prefix:
        Key prefix within the bucket; empty (default) writes at the bucket
        root.
    region, access_key_id, secret_access_key:
        Left as ``None`` (default), these resolve the standard AWS
        credential chain (env vars, IAM role). Set explicitly to override.
    endpoint:
        Custom endpoint for an S3-compatible service (e.g.
        ``"http://localhost:9000"`` for MinIO). Plain HTTP is allowed
        automatically whenever this is set; real AWS S3 always uses HTTPS.
    compression:
        Parquet's own internal compression: ``"zstd"`` (default),
        ``"snappy"``, or ``"uncompressed"`` — distinct from ClickHouse's own
        HTTP transport compression, which is unaffected.

    Note
    ----
    S3 storage and request costs are billed by AWS as usual (free on a
    self-hosted MinIO).
    """

    def __init__(
        self,
        bucket: str,
        *,
        prefix: str = "",
        region: Optional[str] = None,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        endpoint: Optional[str] = None,
        compression: str = "zstd",
    ) -> None: ...

class CleverTap:
    """CleverTap Data Export API source (events). **BigQuery destination only.**

    API responses have no catalog, so you *declare* the output schema via
    ``columns`` (a list of ``(name, bq_type)`` / ``(name, bq_type, path)``
    tuples, or a ``{name: bq_type}`` dict). ``path`` (or ``paths={name: "a.b"}``)
    extracts a value from the nested event JSON by dotted path (e.g.
    ``"profile.email"``, ``"event_props.amount"``); default is ``name`` at the
    top level. ``bq_type`` is a BigQuery type name (STRING/INTEGER/FLOAT/
    BOOLEAN/TIMESTAMP/DATETIME/DATE/TIME/NUMERIC/BIGNUMERIC/BYTES/JSON); NUMERIC
    is delivered exactly (declare NUMERIC only for values sent as JSON strings
    or integers), BIGNUMERIC is lossy (Float64). Nested RECORD/STRUCT types can't
    be declared — point a JSON (or STRING) column at a nested object/array via
    ``path`` and it lands as compact JSON text. The top-level ``ts`` is a packed
    ``yyyyMMddHHmmSS`` integer in several regions (e.g. ``sg1``), **not** epoch
    seconds — declare it as TIMESTAMP/DATETIME (or DATE) and it is parsed as UTC
    civil time; 10-digit epoch seconds are also accepted. ``region`` selects the
    API host (default ``sg1`` ->
    ``https://sg1.api.clevertap.com``). ``[from_date, to_date]`` (``"YYYY-MM-DD"``)
    is the full-refresh window; in incremental mode ``from_date`` is only the
    first-run floor (thereafter the persisted watermark drives ``from``) and
    ``key`` is required (BigQuery MERGE dedup of the re-pulled boundary day).
    ``lookback_days`` re-pulls a rolling window on each resume to catch late or
    restated events past the boundary day.
    """

    def __init__(
        self,
        account_id: str,
        passcode: str,
        event_name: str,
        columns: _ApiColumns,
        *,
        region: str = "sg1",
        batch_size: int = 5000,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        lookback_days: int = 0,
        paths: Optional[Mapping[str, str]] = None,
        base_url: Optional[str] = None,
    ) -> None: ...

class AppsFlyer:
    """AppsFlyer raw-data Pull API source (CSV report). **BigQuery destination
    only.**

    Declare the output schema via ``columns`` (same forms as ``CleverTap``);
    each column reads the CSV header equal to its ``path`` (or ``name``). Auth
    is the V2.0 ``api_token``. ``report_type`` is e.g. ``installs_report`` /
    ``in_app_events_report`` / ``organic_installs_report``. The Pull API has
    **hard daily-call and row caps** — for high volume use AppsFlyer Data Locker
    instead. Times are in the account's timezone unless
    ``extra_params={"timezone": "UTC"}`` — declare DATETIME for wall-clock, or
    TIMESTAMP with a UTC timezone param. ``[from_date, to_date]`` as for
    ``CleverTap``. ``lookback_days`` re-pulls a rolling window on each resume
    (both APIs restate history — e.g. AppsFlyer attribution updates for days).
    """

    def __init__(
        self,
        api_token: str,
        app_id: str,
        report_type: str,
        columns: _ApiColumns,
        *,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        lookback_days: int = 0,
        paths: Optional[Mapping[str, str]] = None,
        extra_params: Optional[Mapping[str, str]] = None,
        base_url: str = "https://hq1.appsflyer.com",
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
    archive:
        Optional :class:`S3Archive` — also write every synced batch as
        Parquet to S3 for backup/historical analysis. ``None`` (default)
        disables this entirely.
    """

    def __init__(
        self,
        url: str,
        *,
        database: str = "default",
        user: str = "default",
        password: str = "",
        compression: str = "zstd",
        archive: Optional[S3Archive] = None,
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
    source: Union[Postgres, MySQL, BigQuery, CleverTap, AppsFlyer],
    target: Union[ClickHouse, BigQuery],
    dest_table: str,
    *,
    source_table: Optional[str] = None,
    source_query: Optional[str] = None,
    state_key: Optional[str] = None,
    mode: str = "full",
    watermark: Optional[str] = None,
    lookback_seconds: int = 0,
    seed_watermark: Optional[str] = None,
    skip_to_max: bool = False,
    advance_watermark: bool = True,
    key: Optional[Sequence[str]] = None,
    create_if_missing: bool = True,
    engine: Optional[str] = None,
    order_by: Optional[Sequence[str]] = None,
    partition_by: Optional[str] = None,
    primary_key: Optional[Sequence[str]] = None,
    merge_prune_partition_by: Optional[str] = None,
    delete_stale_in_window: bool = False,
    parallelism: int = 4,
    batch_rows: int = 100_000,
    batch_bytes: int = 4_194_304,
    max_memory_bytes: int = 536_870_912,
    partition_column: Optional[str] = None,
    read_max_rows_per_sec: Optional[int] = None,
    chunk_rows: Optional[int] = None,
    retry_max_attempts: int = 1,
    column_transforms: Optional[Mapping[str, str]] = None,
    evolve_schema: bool = False,
    state_table_name: str = "_quickhouse_state",
    staging_suffix: str = "_quickhouse_tmp",
    application_name: str = "quickhouse",
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
    ``new_watermark`` is ``None``. ``mode="append"`` (HTTP API sources only)
    inserts each window's rows straight into the destination with NO
    staging/merge/swap and no dedup — a bronze-landing write for when you run
    your own consolidation downstream; ``watermark`` drives the resume window
    and ``key`` is not required.

    **Experimental features** (may change without a major-version bump, and carry
    sharper edges — read their notes before relying on them):
    ``chunk_rows`` (keyset resumable reads; ClickHouse-destination incremental
    only, and requires a unique NOT-NULL integer keyset column),
    ``BigQuery(write_method="storage_write")``, ``merge_prune_partition_by`` and
    ``delete_stale_in_window`` (both can insert duplicate keys or delete history
    if pointed at the wrong column), and ``column_transforms`` (injects raw SQL
    into the source ``SELECT``).

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

    Incremental cursor control (all incremental-mode only):

    - ``state_key`` pins the identity of the persisted cursor in the internal
      ``_quickhouse_state`` table. By default the cursor is keyed by
      ``source_table`` (or the ``source_query`` text) + ``dest_table``. Set
      ``state_key`` to (a) keep the cursor stable when you edit a
      ``source_query``'s WHERE/SELECT — whose changed text would otherwise
      derive a new key and silently trigger a fresh full pull — and (b) give
      two syncs that share a ``dest_table`` but track different ``watermark``
      columns *distinct* cursors (they otherwise collide on one state row and
      clobber each other). Default ``None`` reproduces the pre-existing key
      exactly, so existing state is never orphaned.
    - ``seed_watermark`` / ``skip_to_max`` seed the cursor on the **first** run
      only (when no cursor is persisted yet), then self-retire. ``seed_watermark``
      is an explicit floor — the first run reads only rows past it.
      ``skip_to_max=True`` seeds to the source's current ``MAX(watermark)``,
      reading (almost) nothing — for when the destination already holds
      complete data from a prior/legacy pipeline and a full first pull would be
      a doomed waste. The two are mutually exclusive. Once a real watermark is
      persisted both are ignored, so they are safe to leave set.
    - ``advance_watermark=False`` reads and merges a window WITHOUT persisting
      (advancing) the cursor — the primitive a bounded backfill needs so it
      doesn't rewind the regular schedule. The computed watermark is still
      returned in ``TransferResult.new_watermark`` for observability but is not
      written to ``_quickhouse_state``.
    - ``chunk_rows`` reads the source in keyset-ordered chunks of this many rows,
      committing the cursor per chunk so a mid-read failure resumes instead of
      restarting — for very large tables on a source that cancels long queries
      (e.g. a hot-standby replica). MVP scope: **incremental mode + a ClickHouse
      destination only**, and the keyset column (``partition_column`` else the
      first ``key``) must be a **unique, NOT NULL integer** (ties or NULLs would
      silently skip rows). Chunked mode is single-stream (``parallelism`` is
      ignored). ``None`` (default) = one read, as before.

    Robustness & schema:

    - ``retry_max_attempts`` (default ``1`` = no retry) re-runs the whole
      transfer on a *transient source* error — PostgreSQL hot-standby recovery
      conflict / statement cancel, MySQL server-gone-away / lock-wait / deadlock.
      Each retry starts clean (fresh staging; cursor advances only on success).
      Sink/write blips are retried separately and always.
    - ``column_transforms={col: "<SQL expr>"}`` applies a per-column SQL value
      transform in the source SELECT (e.g. ``{"amt": "ROUND(amt, 9)"}``,
      ``{"ts": "ts AT TIME ZONE 'UTC'"}``) over ``source_table=`` — so range
      partitioning is preserved (unlike ``source_query=``). It changes the
      value, not the resolved type (pair with ``type_overrides`` if the type
      must change too). Not supported for a BigQuery source.
    - ``evolve_schema=True`` adds a column to the existing destination (as
      Nullable) when the source has one the destination lacks, instead of
      hard-erroring. ADD-only — never drops or retypes a column.

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

    By default that ``MERGE`` full-scans the destination table every run (it
    joins on ``key`` only), which on a large partitioned table bills the whole
    table to upsert a few delta rows. ``merge_prune_partition_by="<col>"``
    bounds the destination to the staging batch's range on ``<col>`` so
    BigQuery reads only the touched partitions. **Only safe when ``<col>`` is
    immutable per ``key``** (its value never changes across updates to a row)
    and it is the table's partition column — e.g. a ``created_at``/inserted-at
    column. Do **not** use a ``updated_at``/updated-at column: an updated row's
    new value points at a different partition than the existing row, so pruning
    would miss it and INSERT A DUPLICATE KEY instead of updating (the classic
    merge-filter dup bug). quickhouse can't detect mutability — this is a
    deliberate per-table opt-in. Default ``None`` keeps the safe full scan.

    ``delete_stale_in_window=True`` (BigQuery incremental only) additionally
    DELETEs destination rows inside the merged window that are absent from the
    source pull (``WHEN NOT MATCHED BY SOURCE``) — "replace this window", and a
    NULL merge key nets to a replace instead of duplicating. It **requires**
    ``merge_prune_partition_by`` (the DELETE is scoped to that column's staging
    ``[MIN, MAX]`` range); without it a ``WHEN NOT MATCHED BY SOURCE`` clause
    would delete the entire destination history outside the batch, so it is a
    hard config error.

    Internal names (defaults preserve prior behavior): ``state_table_name``
    (default ``_quickhouse_state``) is quickhouse's watermark/chunk-cursor
    bookkeeping table, created inside the destination — override it for
    table-naming policies (a cursor persisted under the old name isn't found
    after a rename, so treat a change as a first run). ``staging_suffix``
    (default ``_quickhouse_tmp``) names the per-run staging table. And
    ``application_name`` (default ``quickhouse``) is the PostgreSQL
    ``application_name`` announced to the source, visible in
    ``pg_stat_activity``.

    Memory vs. batch sizing:

    - ``batch_rows`` / ``batch_bytes`` control how big each individual Arrow
      batch (and thus each insert) is — a throughput/overhead granularity knob.
    - ``max_memory_bytes`` is the hard ceiling on *total* in-flight batch
      memory across all partitions and all uploads currently in flight,
      measured against each batch's real Arrow allocation. Decoding overlaps
      with concurrent uploads and blocks (backpressure) when this ceiling is
      reached, so peak RSS stays bounded regardless of ``parallelism`` or row
      width. Default 512 MiB; ``0`` disables the ceiling (unbounded).

    Being gentle to a small source database:

    - ``read_max_rows_per_sec`` caps how many source rows are pulled per
      second, summed across *all* parallel partitions (a global limiter, not
      per-connection). After each batch is read the reader pauses to hold the
      aggregate rate at this ceiling; because ``COPY``/streaming results only
      produce as fast as the client consumes, that pause pushes back on the
      server-side scan itself, so the source does proportionally less work —
      not just quickhouse. ``None`` (default) reads as fast as possible.
      Applies to PostgreSQL and MySQL sources; ignored for a BigQuery source
      (its read path is a separately-metered managed API). For the lightest
      possible footprint on a small instance, combine a modest
      ``read_max_rows_per_sec`` with ``parallelism=1`` (one connection, one
      scan), ``mode="incremental"`` (reads only new rows, not the whole
      table), and a ``statement_timeout_secs`` on the ``Postgres``/``MySQL``
      connection. The Postgres connection also reports itself as
      ``application_name = 'quickhouse'`` so the export is visible (and
      killable) in ``pg_stat_activity``.

    Datetime/timezone handling:

    - MySQL ``DATETIME``/``TIMESTAMP`` map to a UTC-aware timestamp — BigQuery
      ``TIMESTAMP``, ClickHouse ``DateTime64(6, 'UTC')`` — reading the
      wall-clock value as UTC (the same instant the legacy pandas/``to_gbq``
      path stored, and what an existing BigQuery ``TIMESTAMP`` column expects).
      To land a column as a naive BigQuery ``DATETIME`` (or ClickHouse
      ``DateTime64(6)``) instead, opt out per-column with
      ``type_overrides={"col": "DATETIME"}``; this flips the actual wire
      encoding, not just the declared destination type, so it works on the
      Storage Write path too. PostgreSQL keeps the distinction from the source
      type: ``timestamptz`` → UTC-aware, ``timestamp`` → naive.
    """
    ...

def version() -> str: ...
