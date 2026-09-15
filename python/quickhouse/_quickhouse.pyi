"""Type stubs for the compiled ``quickhouse._quickhouse`` extension module."""

from typing import Callable, List, Mapping, Optional, Sequence, Tuple, Union

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
        ``source=``). ``"storage_write"`` (default) uses the BigQuery Storage
        Write API (gRPC + protobuf — free up to 2 TiB/month, then $0.025/GB,
        and higher-throughput). ``"insert_all"`` uses the legacy
        ``tabledata.insertAll`` (JSON over REST), which bills $0.01 per 200 MiB
        — roughly double — and is slower; it remains available for
        compatibility. Both share the same atomic swap / MERGE flow; only the
        row-insert transport differs.

        .. versionchanged:: 0.14.0
           The default changed from ``"insert_all"`` to ``"storage_write"``.

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
        write_method: str = "storage_write",
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
    """CleverTap Data Export API source (events). Writes to BigQuery or ClickHouse.

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

class HttpApi:
    """Generic HTTP/REST or CSV API source. Writes to BigQuery or ClickHouse.

    The config-driven escape hatch for arbitrary endpoints (the ``CleverTap`` /
    ``AppsFlyer`` classes are purpose-built). Issues a ``GET``/``POST`` to
    ``url`` with ``headers`` (put auth here — they're never logged or echoed in
    ``repr``), then parses the response as JSON or CSV.

    Parameters
    ----------
    url:
        Endpoint. ``{from}`` / ``{to}`` are replaced with the window's date
        bounds (also substituted in ``body``).
    columns:
        Declared output schema — same forms as :class:`CleverTap` (``(name,
        bq_type[, path])`` tuples or a ``{name: bq_type}`` dict; ``paths=`` maps
        column → dotted path into each record).
    method:
        ``"GET"`` (default) or ``"POST"``.
    headers:
        Request headers, e.g. ``{"Authorization": "Bearer …"}``.
    body:
        Request body for ``POST`` (``{from}``/``{to}`` substituted).
    format:
        ``"json"`` (default) or ``"csv"``. For JSON, ``records_path`` is a dotted
        path to the array of record objects (``None`` = the body itself is the
        array, or a lone object is one record). CSV is parsed as a header row +
        data rows.
    records_path:
        JSON only — dotted path to the records array (e.g. ``"data.rows"``).
    next_cursor_path / cursor_param:
        Cursor pagination (JSON only): the dotted path to the next cursor in the
        response, and the query-param name to send it back as. Set both to
        paginate until the cursor is absent; leave both unset for a single
        request.
    state_id:
        Stable identity for the incremental cursor state (defaults to ``url``).
    from_date / to_date / lookback_days:
        As for :class:`CleverTap`.
    """

    def __init__(
        self,
        url: str,
        columns: _ApiColumns,
        *,
        method: str = "GET",
        headers: Optional[Mapping[str, str]] = None,
        body: Optional[str] = None,
        format: str = "json",
        records_path: Optional[str] = None,
        next_cursor_path: Optional[str] = None,
        cursor_param: Optional[str] = None,
        state_id: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        lookback_days: int = 0,
        paths: Optional[Mapping[str, str]] = None,
    ) -> None: ...

class ClickHouse:
    """ClickHouse connection descriptor — usable as either a source or a target.

    As a **source**, reads go over the same HTTP interface in ClickHouse's own
    ``FORMAT ArrowStream``, with the full partition / incremental / chunk-resume
    machinery behind them. That makes ClickHouse → ClickHouse (a cross-cluster
    or cross-database copy) and ClickHouse → BigQuery (publishing a mart into a
    warehouse) ordinary transfers::

        src = qh.ClickHouse("http://ch-a:8123", database="raw")
        dst = qh.ClickHouse("http://ch-b:8123", database="analytics")
        qh.sync(src, dst, dest_table="orders", source_table="orders",
                mode="incremental", watermark="updated_at", key=["id"])

    ``url``, ``database``, ``user``, ``password`` and ``settings`` apply in both
    roles. ``compression``, ``archive`` and ``insert_dedup_token`` are write-path
    only and are ignored as a ``source=``; ``statement_timeout_secs`` is
    read-path only and is ignored as a ``target=``.

    Types read from a ClickHouse source keep their declared type at the
    destination where one exists — ``UUID``, ``IPv4``/``IPv6``, ``Enum8``/
    ``Enum16``, ``FixedString`` and ``LowCardinality(...)`` all survive a
    ClickHouse → ClickHouse copy rather than flattening to ``String``.
    ``Array``/``Map``/``Tuple``/``JSON``, 256-bit integers and ``Decimal256``
    are not readable yet; cast them to ``String`` in a ``source_query``, or
    ``exclude`` them.

    Parameters
    ----------
    url:
        Base HTTP(S) URL, e.g. ``http://host:8123``.
    database, user, password:
        Database and credentials (the source database, or the target one).
    statement_timeout_secs:
        *Source only.* Server-side ``max_execution_time`` (seconds) applied to
        every request this source makes; ``0`` (default) leaves the server
        default alone. Like the other sources' statement timeouts this is a
        ceiling on the **whole transfer**, not on the query — the SELECT stays
        open from the first row read to the last one written, so a slow
        destination can trip it. Size it for the transfer and use
        ``read_idle_timeout_secs`` to fail on a stalled source. Equivalent to
        ``settings={"max_execution_time": "..."}``, which still wins if both
        are set.

        .. versionadded:: 0.16.0
    compression:
        HTTP insert body compression: ``"zstd"`` (default), ``"gzip"``, or
        ``"none"``. zstd-fast is faster than gzip at a similar/better ratio;
        use ``"none"`` on a fast local network where CPU, not bandwidth, is
        the bottleneck.
    archive:
        Optional :class:`S3Archive` — also write every synced batch as
        Parquet to S3 for backup/historical analysis. ``None`` (default)
        disables this entirely.
    settings:
        Arbitrary ClickHouse settings, sent as URL query parameters on **every**
        request this descriptor makes (as a target: DDL, inserts, reads, swaps;
        as a source: ``DESCRIBE``, the bounds probes and the bulk read) — the
        HTTP interface's own per-request settings mechanism. Names are passed
        through verbatim; ClickHouse itself rejects an unknown one. Avoid
        ``database``, which is already sent. As a source these are applied last,
        so an explicit value always wins over one quickhouse picked for the
        Arrow read path.

        This reaches server-side behaviour no client-side knob can, e.g.
        ``{"select_sequential_consistency": "1"}`` to stop the post-swap
        row-count guard reading a lagging ClickHouse Cloud replica and failing a
        run that actually succeeded, or ``{"async_insert": "1"}`` /
        ``{"max_execution_time": "300"}``.

        .. versionadded:: 0.14.0
    insert_dedup_token:
        Attach a generated ``insert_deduplication_token`` to every insert, so a
        retry whose original attempt the server had already committed is
        discarded instead of duplicating rows. ``False`` (default).

        **Opt-in deliberately.** ClickHouse deduplicates per *block*, not per
        request; a single insert large enough to be split server-side shares one
        token across its blocks, and if that makes later blocks look like
        duplicates of the first they are dropped silently. Verify against your
        own ClickHouse version — on a ``Replicated*MergeTree``, with a large
        insert, comparing row counts — before enabling it in production. It is
        also a no-op on engines without replicated dedup.

        .. versionadded:: 0.14.0
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
        settings: Optional[Mapping[str, str]] = None,
        insert_dedup_token: bool = False,
        statement_timeout_secs: int = 0,
    ) -> None: ...

class Progress:
    """Live progress snapshot passed to ``on_progress``."""

    rows_read: int
    rows_written: int
    bytes_written: int
    elapsed_secs: float
    rows_per_sec: float

class StagedInfo:
    """Context passed to a ``sync(validate=...)`` callback: the fully-loaded
    per-run staging table that is about to be promoted, and where it lives. The
    batteries-included ``quickhouse.Validation`` consumes this; a custom
    ``validate`` callable receives the same object."""

    staging_table: str
    database: str
    dest_kind: str  # "clickhouse" | "bigquery"
    rows_written: int

class TransferWarning:
    """One structured warning from a transfer (new in 0.15.0).

    A condition quickhouse detected, recovered from, and kept going past — but
    which changed what the destination now holds. These used to reach a caller
    only as log text, so an orchestrator could not act on one: a run that
    flattened a column to a boolean, or excluded every NULL-watermark row
    forever, reported success and the damage surfaced weeks later.

    Nothing is raised. These are values on :class:`TransferResult`; it is the
    caller who decides which of them should fail their pipeline::

        result = quickhouse.sync(...)
        fatal = {"collapsed_bool", "null_watermark", "coerced_decimal"}
        for w in result.warnings:
            if w.kind in fatal:
                raise RuntimeError(str(w))

    Named ``TransferWarning`` rather than ``Warning`` because ``Warning`` is a
    Python builtin exception class.
    """

    kind: str
    """Stable machine-readable kind. Match on this, never on ``message``.

    - ``"collapsed_bool"`` — a MySQL ``tinyint(1)`` value outside ``{0, 1}``
      was flattened to a boolean, losing e.g. the difference between 2 and 3.
      See ``tinyint1_as_bool``.
    - ``"coerced_date"`` — a date/datetime became NULL (a zero-date, or a year
      outside ClickHouse's representable 1900-2299 window).
    - ``"coerced_decimal"`` — a decimal became NULL (it exceeded the declared
      ``Decimal(P,S)``, or was NaN/Infinity).
    - ``"coerced_scalar"`` — an API source's scalar failed to parse and became
      NULL.
    - ``"null_watermark"`` — the watermark column is nullable and rows hold a
      NULL there, so they are excluded from this and every future incremental
      run. The most dangerous of these.
    - ``"full_refresh_shrink"`` — a full refresh left the destination smaller,
      permitted by ``allow_full_refresh_shrink``.
    - ``"unclustered_merge_target"`` — a BigQuery ``MERGE`` ran against a
      destination not clustered by the merge key, so the key bound pruned
      nothing and the statement scanned the whole table. A cost problem, not a
      data one.
    """

    column: Optional[str]
    """The source column responsible, or ``None`` for a table-level condition."""

    count: int
    """How many values/rows tripped it. Aggregated per ``(kind, column)`` across
    the whole run, so a badly-legacy table yields one entry with a large count
    rather than millions of entries."""

    sample: Optional[str]
    """A representative offending value where one was free to capture at the
    point of detection. ``None`` otherwise — an absent sample never means the
    count is uncertain."""

    message: str
    """Human-facing text, the same sentence written to the log. Not stable;
    match on ``kind``."""

class TransferResult:
    """Summary returned by :func:`sync`."""

    rows_read: int
    rows_written: int
    bytes_written: int
    rows_deleted: int
    """Destination rows deleted by this run: the window-scoped delete of
    ``delete_stale_in_window`` on a ClickHouse destination. ``0`` everywhere
    else — including for a *BigQuery* ``delete_stale_in_window``, which performs
    its delete inside the ``MERGE`` and reports only one combined affected-row
    count for the whole statement, leaving no way to attribute the delete
    portion. New in 0.15.0."""

    duration_secs: float
    read_secs: float
    """Cumulative time the source readers spent waiting on source rows — the
    awaits on the source stream itself, excluding decode, insert and
    backpressure. Summed across parallel readers, so with ``parallelism > 1``
    it can legitimately exceed ``stage_secs``; compare
    ``read_secs / parallelism`` against ``stage_secs`` to judge whether the
    source or the write path is the bottleneck.

    ``0.0`` for an HTTP API source (CleverTap/AppsFlyer/HttpApi): the timer
    instruments the PostgreSQL/MySQL/BigQuery/ClickHouse source streams, and API paging is
    bounded by per-request HTTP timeouts instead, so ``stage_secs`` covers the
    whole fetch-decode-insert loop there. New in 0.15.0."""

    stage_secs: float
    """Wall time of the streaming phase: reading, decoding and writing every row
    into the destination (or this run's staging table). New in 0.15.0."""

    promote_secs: float
    """Wall time of the promotion after streaming — the full-refresh swap, the
    incremental ``MERGE`` or insert-select, the window-scoped delete, and the
    watermark persist. On a BigQuery destination this is usually most of the
    run. New in 0.15.0."""

    new_watermark: Optional[str]
    warnings: List[TransferWarning]
    """Structured warnings, aggregated per ``(kind, column)`` and ordered
    most-affected first. Empty on a clean run. New in 0.15.0."""

class ReconcileResult:
    """What a :func:`reconcile_keys` diff found, and what it did about it."""

    source_keys: int
    """Distinct keys the source holds in the window."""

    dest_keys: int
    """Distinct keys the destination holds in the window."""

    orphan_keys: int
    """Keys the destination holds that the source no longer has — the drift."""

    missing_keys: int
    """Keys the source holds that the destination does not. Usually ordinary
    sync lag; a large number means the load itself is incomplete, which is a
    different problem from the one this repairs."""

    rows_deleted: int
    """Destination rows actually deleted. ``0`` unless ``delete=True``. Can
    exceed ``orphan_keys`` where the destination holds more than one row per key
    (an un-merged ``ReplacingMergeTree``, for one)."""

    orphan_sample: List[str]
    missing_sample: List[str]
    duration_secs: float

def sync(
    source: Union[Postgres, MySQL, BigQuery, ClickHouse, CleverTap, AppsFlyer, HttpApi],
    target: Union[ClickHouse, BigQuery],
    dest_table: str,
    *,
    source_table: Optional[str] = None,
    source_query: Optional[str] = None,
    state_key: Optional[str] = None,
    mode: str,
    watermark: Optional[str] = None,
    watermark_source_expr: Optional[str] = None,
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
    merge_prune_key_range: bool = True,
    merge_prune_key_list_max: int = 0,
    delete_stale_in_window: bool = False,
    allow_full_refresh_shrink: bool = False,
    parallelism: int = 0,
    batch_rows: int = 100_000,
    batch_bytes: int = 4_194_304,
    insert_bytes: int = 33_554_432,
    max_memory_bytes: int = 536_870_912,
    max_memory_fraction: float = 0.0,
    partition_column: Optional[str] = None,
    partition_source_expr: Optional[str] = None,
    read_max_rows_per_sec: Optional[int] = None,
    read_idle_timeout_secs: int = 0,
    chunk_rows: Optional[int] = None,
    retry_max_attempts: int = 1,
    column_transforms: Optional[Mapping[str, str]] = None,
    column_transform_types: Optional[Mapping[str, str]] = None,
    evolve_schema: bool = False,
    state_table_name: str = "_quickhouse_state",
    staging_suffix: str = "_quickhouse_tmp",
    application_name: str = "quickhouse",
    type_overrides: Optional[Mapping[str, str]] = None,
    rename: Optional[Mapping[str, str]] = None,
    include: Optional[Sequence[str]] = None,
    exclude: Optional[Sequence[str]] = None,
    not_null: Optional[Sequence[str]] = None,
    tinyint1_as_bool: bool = True,
    numeric_as_decimal: Optional[str] = None,
    on_progress: Optional[Callable[[Progress], None]] = None,
    validate: Optional[Callable[[StagedInfo], None]] = None,
) -> TransferResult:
    """Transfer one table from PostgreSQL, MySQL, BigQuery or ClickHouse into
    ClickHouse or BigQuery.

    ``source`` may be a ``Postgres``, ``MySQL``, ``BigQuery`` or ``ClickHouse``
    connection descriptor; ``target`` may be a ``ClickHouse`` or ``BigQuery``
    one (the same ``BigQuery`` and ``ClickHouse`` classes work for either role
    — see their doc comments).
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

    If ``watermark`` allows NULL and any current row has one, a
    ``WHERE watermark > x`` predicate never matches it — that row is silently
    excluded from every incremental run, forever, even though the transfer
    still reports success. A PostgreSQL/MySQL/ClickHouse source logs a warning
    (with the row count) when this is detected; consider a non-nullable
    watermark, or a separate backfill of the ``NULL`` rows.

    ``watermark_source_expr`` (PostgreSQL/MySQL/ClickHouse sources only) is a raw SQL
    expression substituted for ``watermark`` when building the incremental
    filter and the boundary-max probe — the projected ``watermark`` output is
    left untouched. Needed when ``source_query`` computes ``watermark`` from
    an expression (a cast, a timezone shift, ...) rather than a bare
    pass-through of an indexed base-table column: the generated read is
    ``SELECT ... FROM (<source_query>) AS _src WHERE watermark > $1``, and
    that predicate binds to whatever ``source_query`` projects as
    ``watermark`` — if it's computed, no index on the underlying column can
    serve it, so every incremental run does a full scan regardless of table
    size. Have ``source_query`` additionally project the raw, indexed column
    under a second name (e.g. ``write_date AS write_date_raw``) and set
    ``watermark_source_expr="write_date_raw"``.

    ``partition_source_expr`` (new in 0.14.0; PostgreSQL/MySQL/ClickHouse sources only) is
    the same idea applied to parallel reads, and it is what makes
    ``parallelism`` mean anything for a custom query. Range partitioning needs a
    key column it can probe ``MIN``/``MAX`` on and bound with an indexable
    predicate; ``source_query`` hides that column behind its own projection, so
    such a transfer silently ran single-stream however large it was — and since
    a ``CAST`` can only live in ``source_query``, that covered most non-trivial
    tables. Have ``source_query`` project the raw, indexed key column under a
    second name (e.g. ``id AS id_raw``) and set
    ``partition_source_expr="id_raw"`` to fan out. ``None`` (default) keeps the
    old single-stream behaviour, now logged so it isn't invisible.

    Two costs: the ``MIN``/``MAX`` probe runs against the wrapped query rather
    than a base table (a single-table query flattens and still uses the index; a
    query with joins pays for one extra evaluation per run), and an expression
    that is missing or non-integer is a hard error rather than a silent
    fallback — an explicitly requested fan-out that quietly collapses to one
    stream is the bug this fixes.

    ``mode`` is **required** (changed in 0.15.0; it used to default to
    ``"full"``). ``"full"`` REPLACES the destination table wholesale, ``"incremental"``
    upserts on ``key=`` or ``watermark=``, and ``"append"`` inserts without
    deduplicating. The old default handed the most destructive of the three to
    anyone who did not think about the argument.

    ``allow_full_refresh_shrink`` (new in 0.15.0, full mode only) permits a swap
    that would leave the destination with FEWER rows than it had. A full refresh
    replaces the table wholesale — ClickHouse ``EXCHANGE TABLES``, BigQuery
    ``TRUNCATE`` + ``INSERT ... SELECT`` — and **neither is partition-aware**, so
    a run covering one partition destroys every other partition, atomically and
    with a success exit. ``False`` (default) makes that a hard error before the
    swap. Set ``True`` only when the source genuinely did lose rows; to add rather
    than replace, use ``mode="incremental"`` with ``key=`` or ``mode="append"``.

    **Experimental features** (may change without a major-version bump, and carry
    sharper edges — read their notes before relying on them):
    ``chunk_rows`` (keyset resumable reads; ClickHouse-destination incremental
    only, and requires a unique NOT-NULL integer keyset column),
    ``BigQuery(write_method="storage_write")``, ``merge_prune_partition_by`` and
    ``delete_stale_in_window`` (both can insert duplicate keys or delete history
    if pointed at the wrong column), and ``column_transforms`` (injects raw SQL
    into the source ``SELECT``).

    ``validate`` is a data-quality gate: a ``callable(StagedInfo) -> None`` fired
    once after the per-run staging table is fully loaded but *before* it is
    promoted (swap / ``MERGE`` / insert). Raising from it aborts the promotion,
    drops staging, and fails the transfer — so rejected data never reaches the
    destination. Pass a :class:`quickhouse.Validation` to run a Great Expectations
    suite (``pip install quickhouse[quality]``), or any callable that raises to
    reject. Works in full-refresh and incremental mode into either destination —
    a ClickHouse incremental sync (which normally inserts directly) is
    transparently routed through a staging table when a gate is attached, then
    promoted via ``INSERT … SELECT``. Append mode and ``chunk_rows`` commit
    directly with no single staging table to gate, so they raise a clear error.

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
      value, not the resolved decode type or the declared destination type on
      its own — pairing it with ``type_overrides`` changes the *declared
      destination* type string but still not what this crate decodes the
      wire bytes as. If the transform changes the actual type (e.g. casting a
      boolean to text), also set ``column_transform_types={col: "<Arrow
      type>"}`` — one of ``"Boolean"``, ``"Int16"``, ``"Int32"``, ``"Int64"``,
      ``"UInt32"``, ``"Float32"``, ``"Float64"``, ``"Utf8"``, ``"Binary"``,
      ``"Date32"`` — so decoding follows the transform instead of the
      source's own type (a config error if set without a matching
      ``column_transforms`` entry). A datetime or decimal type change should
      still go through ``type_overrides`` alone (see below), which already
      carries the extra timezone/precision info those need. Not supported for
      a BigQuery source.
    - ``evolve_schema=True`` adds a column to the existing destination (as
      Nullable) when the source has one the destination lacks, instead of
      hard-erroring. ADD-only — never drops or retypes a column. Also needed
      for ``mode="full"`` against an *existing* destination to pick up a
      genuinely new source column: full-refresh staging now mirrors the
      destination's actual DDL (see the note under ``engine``/``order_by``
      below) rather than silently rebuilding it, so a new column is evolved
      in rather than included for free.
    - ``not_null=[...]`` forces these destination (post-``rename``) columns
      ``NOT NULL`` in DDL generated from scratch, regardless of the source's
      own resolved nullability. A ``key``/``order_by``/``primary_key`` column
      is already forced non-nullable (ClickHouse rejects a nullable sort key
      outright); this covers a column used *only* in ``partition_by``'s
      expression (e.g. ``toYYYYMM(create_date)``), which isn't otherwise
      covered — if the source reports it nullable (every BigQuery column not
      explicitly ``REQUIRED``), the generated ``Nullable(...)`` partition key
      is then rejected by ClickHouse. Only affects DDL generated *before* a
      destination table exists — see the ``engine``/``order_by`` note below
      for how an existing table's nullability is otherwise handled.
    - ``numeric_as_decimal="Decimal(38, 9)"`` decodes **every**
      arbitrary-precision decimal source column (PostgreSQL ``numeric``, MySQL
      ``DECIMAL``, BigQuery ``NUMERIC``) exactly, instead of through the default
      lossy ``Float64`` round-trip that turns a stored ``32.9`` into
      ``32.89999999999999``. Equivalent to writing
      ``type_overrides={col: "Decimal(38, 9)"}`` for each of them, which is the
      point: one argument instead of an audit, since a column you forget loses
      precision silently. A per-column ``type_overrides`` entry still wins.
      Not the default — it changes the destination column type, so against an
      already-created ``Float64`` column you'd be writing a decimal into a
      float. Pick the scale for the column's real range: a value that doesn't
      fit is coerced to ``NULL`` (counted and warned about). ``P > 38`` needs
      ``Decimal256``, which isn't supported yet.
    - ``tinyint1_as_bool=False`` (MySQL sources) reads a ``tinyint(1)`` column
      as the integer it is (``Int8``, or ``UInt8`` when UNSIGNED) rather than as
      a boolean. MySQL's ``BOOL`` is an alias for ``tinyint(1)``, so display
      width is the only signal available, and the default follows that
      convention — but schemas that don't (Odoo, for one) store genuine small
      integers there, and every non-zero value then lands as ``1``. Left at
      ``True``, any value outside ``{0, 1}`` is now counted and warned about at
      the end of the read. ``type_overrides`` cannot fix this after the fact:
      the value is flattened by the boolean decoder before the destination type
      matters.

    ``engine``/``order_by``/``partition_by``/``primary_key``/``key`` are
    interpreted per destination: for ClickHouse they drive `MergeTree`-family
    DDL as before; for BigQuery, ``engine`` is ignored, ``partition_by`` must
    be a bare date/timestamp column name, and ``order_by``/``key`` become
    clustering columns (at most 4 total — see ``BigQuery``'s doc comment).
    They only take effect building DDL *from scratch* (the destination table
    doesn't exist yet). Against an *already-existing* destination, staging
    (in ``mode="full"``, and in ``mode="incremental"`` for a BigQuery
    destination, which stages for its ``MERGE``) clones that table's actual
    engine/``ORDER BY``/``PARTITION BY``/nullability rather than recomputing
    them from these arguments — so omitting (or disagreeing with) them no
    longer silently changes an existing table's structure, or drifts staging
    away from types the table was actually created with. To deliberately
    change an existing table's DDL, drop it first (or run a manual migration)
    so quickhouse creates it fresh from these arguments.

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

    ``merge_prune_key_range`` (new in 0.14.0, default ``True``) additionally
    bounds that scan to the staging batch's ``[MIN, MAX]`` on the merge ``key``
    itself. Unlike the knob above this needs no immutability contract and is on
    by default, because it is a tautology rather than an assumption: a
    destination row can only match by holding a staging row's exact key value,
    which is inside that batch's own range by construction, so no configuration
    exists in which it changes which rows merge. It pays off when the table is
    clustered by the merge key — what quickhouse's own generated DDL does — and
    otherwise costs one small query against the staging table to resolve the
    bounds. Ignored when ``delete_stale_in_window`` is set, where narrowing the
    ``ON`` clause would quietly reduce "replace this window" to "replace this
    key range". Set ``False`` to restore the unbounded join.

    .. versionchanged:: 0.14.1
       Both prunes now paste the resolved ``[MIN, MAX]`` into the statement as
       literals. 0.14.0 emitted them as subqueries over the staging table, which
       BigQuery rejects inside a join predicate (``Unsupported subquery with
       table in join predicate``) when it analyses the query — so every
       BigQuery ``MERGE`` failed, whatever the data. If you worked around it
       with ``merge_prune_key_range=False``, the override is no longer needed.

    ``merge_prune_key_list_max`` (new in 0.15.0, default ``0`` = off) is the
    tighter form of the same tautology. When the staging batch holds at most
    this many distinct merge-key values, the bound becomes that exact key *list*
    (``T.k IN (v1, v2, ...)``) instead of a ``[MIN, MAX]`` range. Both are safe
    for the same reason and neither can change which rows merge; they differ in
    how well they *bind*. A range prunes in proportion to how tightly the
    changed keys cluster — narrow on an append-only table, useless on one whose
    rows are updated after insert (a user profile, an order status, a voucher
    redemption), where the delta is scattered across the whole key space and
    ``[MIN, MAX]`` covers nearly everything. A key list does not degrade that
    way. Costs one extra small query per merge; if the batch turns out to hold
    more distinct keys than the limit the list is abandoned and the range bound
    is used, so the ceiling really is a ceiling on statement size. Single-column
    ``key`` only, and ignored under ``delete_stale_in_window`` for the same
    reason the range bound is.

    quickhouse also **warns** (as a ``TransferWarning`` with kind
    ``"unclustered_merge_target"``, new in 0.15.0) when it MERGEs into a
    destination that is not clustered by the merge key — there the key bound
    cannot prune anything and every run scans the whole table, which is
    invisible from the caller's side until it shows up on a bill.

    ``delete_stale_in_window=True`` (incremental only) additionally DELETEs
    destination rows inside the merged window that are absent from the source
    pull — "replace this window", and a NULL merge key nets to a replace instead
    of duplicating. **This is the only way an ordinary sync converges with a
    source that hard-deletes rows**; without it a deleted row has no watermark
    to move, so it never reaches the destination again and stays there forever.
    It **requires** ``merge_prune_partition_by`` (the DELETE is scoped to that
    column's staging ``[MIN, MAX]`` range); without a window bound it would
    delete the entire destination history outside the batch, so it is a hard
    config error.

    .. versionchanged:: 0.15.0
       Supported for a **ClickHouse** destination as well as BigQuery. BigQuery
       expresses it as a ``WHEN NOT MATCHED BY SOURCE`` clause inside the same
       ``MERGE``, so it is atomic with the upsert but reports no separate
       deleted-row count. ClickHouse runs a lightweight ``DELETE FROM dest
       WHERE <window> AND key NOT IN (SELECT key FROM staging)`` just before the
       staged rows are promoted — which forces the run to stage (ClickHouse
       incremental otherwise inserts directly), is *not* atomic with the insert,
       and reports the exact count on ``TransferResult.rows_deleted``.

    To *measure* drift against a source without repairing it — including outside
    any sync, on its own schedule — see :func:`reconcile_keys`.

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
      batch is — the *decode* granularity.
    - ``insert_bytes`` (new in 0.14.0, default 32 MiB) controls how much is sent
      per insert: decoded batches accumulate until they reach it, then go out as
      one request. Previously every batch was its own insert, so ``batch_bytes``
      silently set both — at its 4 MiB default a 19.4M-row table meant 900+
      round-trips and 900+ new ClickHouse parts, against ClickHouse's own
      guidance of fewer, larger inserts (part count drives background merge
      work, which on Cloud competes with query memory). ``0`` restores
      one-insert-per-batch.
    - ``max_memory_bytes`` is the hard ceiling on *total* in-flight batch
      memory across all partitions and all uploads currently in flight,
      measured against each batch's real Arrow allocation. Decoding overlaps
      with concurrent uploads and blocks (backpressure) when this ceiling is
      reached, so peak RSS stays bounded regardless of ``parallelism`` or row
      width. Default 512 MiB; ``0`` disables the ceiling (unbounded). Raising
      ``insert_bytes`` does not raise this: batches still hold reservations while
      they accumulate, and the destination serializes its payload incrementally
      rather than buffering it whole.
    - ``max_memory_fraction`` (new in 0.14.0) sets that ceiling as a fraction of
      the memory this process is actually allowed — read from the cgroup limit
      where there is one, so four containers sharing a VM each size against
      their own 4 GiB rather than the host's 16. ``0.0`` (default) uses
      ``max_memory_bytes`` verbatim. If the host won't report a limit (notably
      off Linux), ``max_memory_bytes`` is kept as configured rather than
      silently becoming unbounded.
    - ``parallelism`` now defaults to ``0``, meaning "derive from the host"
      (CPUs available to this process, container quota included), instead of a
      fixed ``4``.

    Being gentle to a small source database:

    - ``read_max_rows_per_sec`` caps how many source rows are pulled per
      second, summed across *all* parallel partitions (a global limiter, not
      per-connection). After each batch is read the reader pauses to hold the
      aggregate rate at this ceiling; because ``COPY``/streaming results only
      produce as fast as the client consumes, that pause pushes back on the
      server-side scan itself, so the source does proportionally less work —
      not just quickhouse. ``None`` (default) reads as fast as possible.
      Applies to the PostgreSQL, MySQL and ClickHouse sources; ignored for a
      BigQuery source (its read path is a separately-metered managed API). For the lightest
      possible footprint on a small instance, combine a modest
      ``read_max_rows_per_sec`` with ``parallelism=1`` (one connection, one
      scan), ``mode="incremental"`` (reads only new rows, not the whole
      table), and a ``statement_timeout_secs`` on the ``Postgres``/``MySQL``
      connection. The Postgres connection also reports itself as
      ``application_name = 'quickhouse'`` so the export is visible (and
      killable) in ``pg_stat_activity``.

    Timeouts — which knob means what:

    - ``statement_timeout_secs`` (on the ``Postgres``/``MySQL`` descriptor) is a
      **server-side ceiling on the whole transfer**, despite its name. quickhouse
      streams the source result set straight into the destination, so the
      statement producing it stays open from the first row read to the last one
      written, and the server counts read + decode + destination write +
      backpressure against it. A sub-second source scan can be cancelled here
      purely because the *destination* was slow, and it is reported as a source
      error (``57014 canceling statement due to statement timeout``), which
      sends an operator hunting for a slow query and a missing index that do not
      exist. Retries cannot rescue it either: every attempt restarts from zero
      against the same ceiling, so a table that crosses the line fails every
      attempt.
    - ``read_idle_timeout_secs`` (new in 0.15.0, default ``0`` = off) is the
      thing the name above suggests: fail when **no source rows arrive** for
      that long. The timer wraps the awaits on the source stream and nothing
      else, so time spent decoding, inserting, or blocked on the memory budget
      does not count toward it — a slow destination cannot trip it, which is
      what makes it safe to set tightly. A genuinely hung source does trip it,
      and the resulting error is classified transient so ``retry_max_attempts``
      retries the whole transfer. Applies to the PostgreSQL, MySQL, BigQuery and
      ClickHouse source reads; API sources are paced by their own per-request
      HTTP timeouts. (For a ClickHouse source it measures the gap between
      response chunks rather than between rows — the same thing at any
      meaningful scale, since a block is flushed as it is produced.) Setting this lets ``statement_timeout_secs`` go back to being
      sized for the source database — a guard against a runaway scan — rather
      than for the destination's worst day.

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

def reconcile_keys(
    source: Union[Postgres, MySQL, ClickHouse],
    target: Union[ClickHouse, BigQuery],
    dest_table: str,
    *,
    key: str,
    source_table: Optional[str] = None,
    source_query: Optional[str] = None,
    window: Optional[str] = None,
    dest_window: Optional[str] = None,
    delete: bool = False,
    max_delete_keys: int = 0,
    sample_limit: int = 10,
) -> ReconcileResult:
    """Diff a source table's keyset against a destination's, and optionally
    delete the rows the source no longer has. New in 0.15.0.

    An incremental sync is insert-and-update only. It finds rows whose watermark
    moved and upserts them; a row the source *deleted* has no watermark to move,
    so nothing about it ever reaches the destination again and it stays there
    forever. For any source that hard-deletes as a matter of routine — an ERP
    cancelling a reservation, a queue draining, a "soft delete" that is really a
    ``DELETE`` — the destination drifts, one-directionally, without bound.

    The drift also hides well. It is lopsided: a measured production table
    carried +1.30% phantom rows against +0.0168% on a quantity sum, so every
    COUNT-based model over-reported materially while every SUM-based one looked
    fine. Row counts alone will not tell you your exposure.

    ``delete_stale_in_window`` on :func:`sync` prevents the drift going forward,
    inside a sync, for the window that sync touched. This is the other half: it
    answers "how far apart are these two right now?" for an arbitrary window, on
    its own schedule, and repairs the difference only when asked. Measuring is
    the part worth running continuously; deleting is the part worth approving.

    ::

        r = quickhouse.reconcile_keys(
            src, dst, dest_table="stock_move_line_fki",
            source_table="stock_move_line",
            key="id",
            window="create_date >= '2026-07-01' AND create_date < '2026-08-01'",
        )
        print(r.orphan_keys, r.missing_keys)   # measure
        if r.orphan_keys and r.orphan_keys < 50_000:
            quickhouse.reconcile_keys(..., delete=True, max_delete_keys=50_000)

    :param key: The identifying column, present on both sides under the same
        name. **Single column, and realistically an integer or string.** Both
        sides render their keys as text so the diff can be a set comparison, and
        the two renderings only agree for types with one obvious text form. An
        integer id round-trips exactly; a timestamp does not (PostgreSQL writes
        ``2026-08-30 12:00:00+00``, ClickHouse writes ``2026-08-30 12:00:00``)
        and every key would read as drift in both directions. That
        total-mismatch shape is detected and refused rather than acted on, but
        pick a sane key and it never arises.
    :param window: SQL predicate bounding the source read, in the source's
        dialect. ``None`` reads the whole table — fine for measuring a small one,
        but both keysets are held in memory, so bound anything large.
    :param dest_window: The same bound for the destination, in the
        *destination's* dialect. Defaults to ``window``, which is right whenever
        the destination mirrors the source's column names.
    :param delete: Delete the orphans. Default ``False`` — measure only.
        Requires a window: an unbounded delete would remove every destination
        key the source does not hold at this instant, which is a full refresh
        with a race in it, not a reconcile.
    :param max_delete_keys: Refuse to delete when the orphan count exceeds this
        (``0`` = no ceiling). A reconcile is only as good as its window; this is
        the blunt guard against a predicate that means something different on
        each side. Set it to a few times the drift you actually expect.
    :param sample_limit: How many orphan/missing keys to carry back as samples.
        The counts are exact regardless.

    Sources: PostgreSQL, MySQL and ClickHouse. A BigQuery source is rejected (it
    is normally itself a mirror rather than the system of record), as is an HTTP
    API source (no keyset query to diff against). Destinations: both ClickHouse
    and BigQuery.
    """
    ...

def version() -> str: ...
