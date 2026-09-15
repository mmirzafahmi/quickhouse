//! Configuration structs for a transfer. These are populated by the Python
//! binding (or constructed directly in Rust tests) and drive [`crate::sync`].

use std::collections::HashMap;

/// Where to read from.
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    /// libpq-style connection string, e.g. `postgresql://user:pw@host:5432/db`.
    pub dsn: String,
    /// Server-side statement timeout (seconds) set on each connection this
    /// transfer opens; `0` = leave the server default alone.
    ///
    /// **Read this as a ceiling on the whole transfer, not on the query.**
    /// quickhouse streams the source result set straight into the destination,
    /// so the statement that produces it stays open from the first row to the
    /// last one written. The server therefore counts read + decode +
    /// destination write + backpressure against this timeout, and cancels the
    /// statement — reporting a *source* error — when the total crosses it. A
    /// sub-second source scan can fail here purely because the destination was
    /// slow that day, and because every retry restarts from zero against the
    /// same ceiling, `retry_max_attempts` cannot rescue it.
    ///
    /// Size it for the transfer, then use
    /// [`TransferConfig::read_idle_timeout_secs`] for the thing this knob's
    /// name suggests: failing when the *source* stops producing rows.
    pub statement_timeout_secs: u64,
    /// Path to a PEM file with extra trusted CA certificate(s) (e.g. AWS RDS's
    /// regional bundle), trusted in addition to the public webpki-roots store.
    /// Needed whenever the server's certificate doesn't chain to a public CA.
    pub ca_cert_file: Option<String>,
    /// mTLS (client-certificate auth): path to the client certificate chain
    /// (PEM). Must be set together with `client_key_file`.
    pub client_cert_file: Option<String>,
    /// mTLS: path to the client private key (PEM). Must be set together with
    /// `client_cert_file`.
    pub client_key_file: Option<String>,
}

/// Where to read from, when the source is MySQL (e.g. AWS RDS for MySQL).
#[derive(Debug, Clone)]
pub struct MySqlConfig {
    /// MySQL connection string, e.g. `mysql://user:pw@host:3306/db`.
    pub dsn: String,
    /// Server-side statement timeout (seconds) set on each connection this
    /// transfer opens; `0` = leave the server default alone.
    ///
    /// **Read this as a ceiling on the whole transfer, not on the query.**
    /// quickhouse streams the source result set straight into the destination,
    /// so the statement that produces it stays open from the first row to the
    /// last one written. The server therefore counts read + decode +
    /// destination write + backpressure against this timeout, and cancels the
    /// statement — reporting a *source* error — when the total crosses it. A
    /// sub-second source scan can fail here purely because the destination was
    /// slow that day, and because every retry restarts from zero against the
    /// same ceiling, `retry_max_attempts` cannot rescue it.
    ///
    /// Size it for the transfer, then use
    /// [`TransferConfig::read_idle_timeout_secs`] for the thing this knob's
    /// name suggests: failing when the *source* stops producing rows.
    pub statement_timeout_secs: u64,
    /// Path to a PEM file with extra trusted CA certificate(s) (e.g. AWS RDS's
    /// regional bundle), trusted in addition to the public webpki-roots store.
    pub ca_cert_file: Option<String>,
    /// Require TLS for the connection (MySQL has no `sslmode` DSN parameter
    /// convention like libpq, so this is explicit).
    pub require_tls: bool,
    /// mTLS (client-certificate auth): path to the client certificate chain
    /// (DER or PEM). Must be set together with `client_key_file`.
    pub client_cert_file: Option<String>,
    /// mTLS: path to the client private key (DER or PEM). Must be set together
    /// with `client_cert_file`.
    pub client_key_file: Option<String>,
}

/// Where to read from, when the source is Google BigQuery.
#[derive(Debug, Clone)]
pub struct BigQueryConfig {
    /// GCP project ID. If `None`, resolved from the credentials (both ADC and
    /// service-account key files normally embed/resolve a project ID).
    pub project_id: Option<String>,
    /// Path to a service-account JSON key file. If `None`, falls back to
    /// Application Default Credentials (`GOOGLE_APPLICATION_CREDENTIALS`,
    /// `GOOGLE_APPLICATION_CREDENTIALS_JSON`, the metadata server, or the
    /// gcloud CLI's well-known ADC file).
    pub credentials_file: Option<String>,
    /// Inline service-account JSON key contents (an alternative to writing a
    /// file — e.g. loaded straight from a secrets manager). Takes precedence
    /// over `credentials_file` when both are set.
    pub credentials_json: Option<String>,
}

/// Where to read from, when the source is ClickHouse itself.
///
/// The same fields as [`ClickHouseConfig`] minus the write-path ones, so a
/// single `quickhouse.ClickHouse(...)` descriptor serves as either end of a
/// transfer: ClickHouse -> ClickHouse (a cross-cluster or cross-database copy),
/// or ClickHouse -> BigQuery (publishing a mart into a warehouse).
#[derive(Debug, Clone)]
pub struct ClickHouseSourceConfig {
    /// Base HTTP(S) URL of the ClickHouse server, e.g. `http://host:8123`.
    pub url: String,
    pub database: String,
    pub user: String,
    pub password: String,
    /// Server-side `max_execution_time` (seconds) applied to every request this
    /// source makes; `0` = leave the server default alone.
    ///
    /// The same whole-transfer ceiling the other SQL sources' statement
    /// timeouts are — read + decode + destination write all count against it,
    /// because the SELECT stays open from the first row to the last one
    /// written. See [`PostgresConfig::statement_timeout_secs`] for the longer
    /// version, and [`TransferConfig::read_idle_timeout_secs`] for the knob
    /// that fails only on a stalled *source*.
    pub statement_timeout_secs: u64,
    /// Arbitrary ClickHouse settings sent as URL query parameters on every
    /// request this source makes — the read-side twin of
    /// [`ClickHouseConfig::settings`]. Applied *after* the settings quickhouse
    /// chooses for the Arrow read path, so an explicit value always wins.
    pub settings: std::collections::BTreeMap<String, String>,
}

/// One declared output column for an HTTP API source. API responses have no
/// catalog to resolve a schema from, so the user declares the destination
/// column `name`, its BigQuery type (`bq_type` — a BigQuery type-name string:
/// `STRING`/`INTEGER`/`FLOAT`/`BOOLEAN`/`TIMESTAMP`/`DATETIME`/`DATE`/`TIME`/
/// `NUMERIC`/`BIGNUMERIC`/`BYTES`/`JSON`), and — for a nested-JSON source
/// (CleverTap) — an optional dotted `path` locating the value inside each
/// record (e.g. `"profile.identity"`, `"event_props.amount"`). `path=None`
/// looks the value up by `name` at the record's top level. For AppsFlyer CSV,
/// `path` (if set) is the CSV header to read from; else `name` is the header.
#[derive(Debug, Clone)]
pub struct ApiColumn {
    pub name: String,
    pub bq_type: String,
    pub path: Option<String>,
}

/// Read from CleverTap's Data Export API (events). Auth is Account ID +
/// Passcode; the host is region-specific (e.g. `https://sg1.api.clevertap.com`).
#[derive(Debug, Clone)]
pub struct CleverTapConfig {
    /// Region base URL, e.g. `https://sg1.api.clevertap.com`.
    pub base_url: String,
    pub account_id: String,
    pub passcode: String,
    /// Event to export (the `event_name` in the create-export request).
    pub event_name: String,
    /// `?batch_size=N` per page; `0` uses the client default.
    pub batch_size: u32,
    pub columns: Vec<ApiColumn>,
    /// Window start `"YYYY-MM-DD"` (full-mode start / incremental first-run floor).
    pub from_date: Option<String>,
    /// Window end `"YYYY-MM-DD"` (defaults to today).
    pub to_date: Option<String>,
    /// Incremental/append re-pull window: start `N` days *before* the committed
    /// watermark so late-arriving/restated events past the boundary day are
    /// re-fetched. `0` = only the boundary day. MERGE-on-key dedups the overlap
    /// (incremental); in append mode the overlap re-appends (downstream dedups).
    pub lookback_days: u32,
}

/// Read from AppsFlyer's raw-data Pull API. Auth is a V2.0 bearer token. The
/// Pull API has hard daily-call and row caps — for high volume, Data Locker
/// (files in a bucket) is the vendor-recommended path.
#[derive(Debug, Clone)]
pub struct AppsFlyerConfig {
    /// API host, default `https://hq1.appsflyer.com`.
    pub base_url: String,
    pub api_token: String,
    pub app_id: String,
    /// e.g. `installs_report`, `in_app_events_report`, `organic_installs_report`.
    pub report_type: String,
    /// Extra query params appended to the report URL (e.g. `timezone`, `maximum_rows`).
    pub extra_params: HashMap<String, String>,
    pub columns: Vec<ApiColumn>,
    pub from_date: Option<String>,
    pub to_date: Option<String>,
    /// See [`CleverTapConfig::lookback_days`].
    pub lookback_days: u32,
}

/// Response body shape for a generic [`HttpApiConfig`] source.
#[derive(Debug, Clone)]
pub enum HttpFormat {
    /// A JSON body. `records_path` is a dotted path to the array of record
    /// objects (e.g. `"data.rows"`); `None` means the body itself is the array
    /// (or a single object, treated as one record).
    Json { records_path: Option<String> },
    /// A CSV body (header row + data rows), parsed like the AppsFlyer report.
    Csv,
}

/// Read from a generic HTTP/REST or CSV endpoint. Auth is whatever `headers`
/// you supply (e.g. an `Authorization` header). The `{from}` and `{to}` tokens
/// in `url` and `body` are replaced with the window's date bounds.
#[derive(Debug, Clone)]
pub struct HttpApiConfig {
    pub url: String,
    /// `"GET"` (default) or `"POST"`.
    pub method: String,
    pub headers: HashMap<String, String>,
    /// Request body (for `POST`); `{from}`/`{to}` are substituted. `None` = none.
    pub body: Option<String>,
    pub format: HttpFormat,
    /// Cursor pagination: a dotted path to the next-cursor value in the response
    /// and the query-param name to send it back as. Both set ⇒ keep paging until
    /// the cursor is absent/empty; `None` ⇒ a single request.
    pub next_cursor_path: Option<String>,
    pub cursor_param: Option<String>,
    /// Stable identity for this source's incremental cursor in the state table
    /// (there's no `source_table`); defaults to the `url` when unset.
    pub state_id: Option<String>,
    pub columns: Vec<ApiColumn>,
    pub from_date: Option<String>,
    pub to_date: Option<String>,
    /// See [`CleverTapConfig::lookback_days`].
    pub lookback_days: u32,
}

/// Read from an in-memory DataFrame, handed in as a serialized Arrow IPC
/// stream. Backs `quickhouse.from_pandas`.
///
/// Nothing here knows about Python: the binding's Python layer converts
/// pandas/polars/pyarrow/DuckDB to a `pyarrow.Table`, normalises its schema to
/// the types this crate maps, and serializes it. That keeps the architecture's
/// standing invariant — no live Python object crosses `Python::allow_threads` —
/// intact for the first source that has one to begin with.
#[derive(Clone)]
pub struct ArrowFrameConfig {
    /// A complete Arrow IPC **stream**: schema message, record batches, then
    /// the end-of-stream marker.
    ///
    /// `Arc<[u8]>` rather than `Vec<u8>` because [`crate::sync::run_transfer`]
    /// clones the whole `SourceConfig` once per retry attempt — including the
    /// first. A `Vec` here would deep-copy the caller's entire frame on a code
    /// path that exists for source errors a frame source cannot produce.
    pub ipc: std::sync::Arc<[u8]>,
    /// Optional caller-supplied label, used for log lines and the persisted
    /// state key. `None` falls back to the destination table name.
    pub label: Option<String>,
}

/// Hand-written, never derived: `SourceConfig` is `Debug`-formatted into
/// tracing spans and error context, and a derived impl would dump the caller's
/// entire serialized frame — potentially gigabytes — into a single log line.
impl std::fmt::Debug for ArrowFrameConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowFrameConfig")
            .field("ipc_bytes", &self.ipc.len())
            .field("label", &self.label)
            .finish()
    }
}

/// Which engine/API to read from.
#[derive(Debug, Clone)]
pub enum SourceConfig {
    Postgres(PostgresConfig),
    MySql(MySqlConfig),
    BigQuery(BigQueryConfig),
    ClickHouse(ClickHouseSourceConfig),
    CleverTap(CleverTapConfig),
    AppsFlyer(AppsFlyerConfig),
    HttpApi(HttpApiConfig),
    Arrow(ArrowFrameConfig),
}

/// How a source is *read*, as opposed to which engine it is — the axis
/// [`TransferConfig::validate_impl`] and [`crate::sync`]'s dispatcher actually
/// branch on.
///
/// This was a `bool` (`is_api`) until the DataFrame source arrived and made it a
/// three-way question. An enum rather than a second bool on purpose: two bools
/// make a four-state truth table out of a three-state one, and the two
/// impossible states are exactly where a silent validation gap would live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceShape {
    /// Postgres/MySQL/BigQuery/ClickHouse: a connection, a schema to probe, and
    /// (for the first two and the last) partitions to fan out over.
    Db,
    /// CleverTap/AppsFlyer/HttpApi: a declared schema and a paginated date
    /// window, with no catalog to probe.
    Api,
    /// An in-memory Arrow frame: the schema arrives with the data, and there is
    /// no source to connect to, filter, partition or pace.
    Frame,
}

impl SourceShape {
    /// The noun to use in a validation error message about this shape.
    pub fn label(&self) -> &'static str {
        match self {
            SourceShape::Db => "a database source",
            SourceShape::Api => "an API source",
            SourceShape::Frame => "a DataFrame source",
        }
    }
}

impl SourceConfig {
    /// A short label identifying the source, used to persist watermark state
    /// under a source-qualified key (so the same table name in different
    /// engines doesn't collide) and in log lines.
    pub fn kind(&self) -> &'static str {
        match self {
            SourceConfig::Postgres(_) => "postgres",
            SourceConfig::MySql(_) => "mysql",
            SourceConfig::BigQuery(_) => "bigquery",
            SourceConfig::ClickHouse(_) => "clickhouse",
            SourceConfig::CleverTap(_) => "clevertap",
            SourceConfig::AppsFlyer(_) => "appsflyer",
            SourceConfig::HttpApi(_) => "http",
            SourceConfig::Arrow(_) => "arrow",
        }
    }

    /// How this source is read — see [`SourceShape`].
    pub fn shape(&self) -> SourceShape {
        match self {
            SourceConfig::CleverTap(_) | SourceConfig::AppsFlyer(_) | SourceConfig::HttpApi(_) => {
                SourceShape::Api
            }
            SourceConfig::Arrow(_) => SourceShape::Frame,
            SourceConfig::Postgres(_)
            | SourceConfig::MySql(_)
            | SourceConfig::BigQuery(_)
            | SourceConfig::ClickHouse(_) => SourceShape::Db,
        }
    }

    /// Whether this is an HTTP API source (CleverTap/AppsFlyer/HttpApi) —
    /// those take a declared schema + date window and bypass the DB
    /// schema-resolution / partition machinery. They write to either
    /// destination (BigQuery or ClickHouse) since `bc1ab45`.
    pub fn is_api(&self) -> bool {
        self.shape() == SourceShape::Api
    }

    /// A stable identity string for a source that has no `source_table` to key
    /// its `_quickhouse_state` cursor on — the API sources, and a DataFrame.
    /// `None` for a source that does have one.
    pub fn api_state_identity(&self) -> Option<String> {
        match self {
            SourceConfig::CleverTap(c) => Some(format!("clevertap:{}", c.event_name)),
            SourceConfig::AppsFlyer(a) => Some(format!("appsflyer:{}:{}", a.app_id, a.report_type)),
            SourceConfig::HttpApi(h) => Some(
                h.state_id
                    .clone()
                    .unwrap_or_else(|| format!("http:{}", h.url)),
            ),
            SourceConfig::Arrow(f) => f.label.as_ref().map(|l| format!("frame:{l}")),
            _ => None,
        }
    }
}

/// Where to write to.
#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    /// Base HTTP(S) URL of the ClickHouse server, e.g. `http://host:8123`.
    pub url: String,
    pub database: String,
    pub user: String,
    pub password: String,
    /// `"none" | "gzip" | "zstd"` — HTTP body compression for inserts.
    pub compression: Compression,
    /// Attach a generated `insert_deduplication_token` to every insert, making
    /// the retry path exactly-once instead of at-least-once. `false` (default).
    ///
    /// Inserts retry on transient failures, and a transient failure includes
    /// "the server committed the block but the ack was lost" — so a retry can
    /// duplicate one block's rows. Incremental mode hides this (a
    /// `ReplacingMergeTree` collapses by key), but a full refresh into a plain
    /// `MergeTree` keeps the duplicates. With this on, each insert carries a
    /// token that is unique per insert and identical across that insert's own
    /// retries, so the server recognises and discards the repeat.
    ///
    /// **Why it is opt-in.** ClickHouse deduplicates per *block*, not per
    /// request. A single insert large enough to be split into several blocks
    /// server-side shares one token across them, and if that makes blocks after
    /// the first look like duplicates of it, they are silently dropped — data
    /// loss, not a visible error. Coalesced inserts (see
    /// `TransferConfig::insert_bytes`) make multi-block inserts more likely, so
    /// this ships off by default until it has been verified against the
    /// ClickHouse version you actually run. Verify on a `Replicated*MergeTree`
    /// with a large insert, comparing row counts, before enabling it in
    /// production.
    ///
    /// Also note it only does anything on `Replicated*MergeTree` engines (or a
    /// `MergeTree` with `non_replicated_deduplication_window` set) — elsewhere
    /// ClickHouse ignores the token. An explicit
    /// `settings["insert_deduplication_token"]` always wins over this, though a
    /// hand-set constant token is a much worse idea than it looks: shared by
    /// every insert of the run, it would make ClickHouse discard all of them
    /// after the first.
    pub insert_dedup_token: bool,
    /// Arbitrary ClickHouse settings, sent as URL query parameters on **every**
    /// request this sink makes (DDL, inserts, reads, swaps) — the HTTP
    /// interface's own mechanism for per-request settings.
    ///
    /// This is deliberately an open passthrough rather than a fixed set of
    /// typed knobs. Server-side behavior that only ClickHouse can decide —
    /// `async_insert`, `max_insert_block_size`, `max_execution_time`,
    /// `insert_deduplication_token`, `select_sequential_consistency` — was
    /// previously unreachable from the client at any price, so tuning one meant
    /// waiting for a quickhouse release. One concrete case: on a ClickHouse
    /// Cloud service with lagging replicas, quickhouse's own post-swap row-count
    /// guard can read a stale replica, see 0 rows, and fail a run that in fact
    /// succeeded; `select_sequential_consistency=1` fixes it server-side and is
    /// now settable from the caller.
    ///
    /// A `BTreeMap` (not `HashMap`) so the generated query string is stable
    /// across runs — worth it for reproducible request logs.
    ///
    /// Names are passed through verbatim and unvalidated: ClickHouse rejects an
    /// unknown setting itself, with a better message than a client-side
    /// allowlist could give. Avoid `database`, which the sink already sends.
    pub settings: std::collections::BTreeMap<String, String>,
    /// Optional: also archive every synced batch as Parquet into S3 (or an
    /// S3-compatible store like MinIO) — a secondary, best-effort-free data
    /// lake for backup/historical analysis, independent of ClickHouse's own
    /// retention. `None` (default) disables this entirely; the ClickHouse
    /// write path is unaffected either way.
    pub s3_archive: Option<S3ArchiveConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Gzip,
    Zstd,
}

/// Parquet's own internal (column/page) compression for archived files —
/// distinct from `Compression` above, which is ClickHouse's HTTP transport
/// compression and has no bearing on the archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ParquetCompression {
    #[default]
    Zstd,
    Snappy,
    Uncompressed,
}

/// Optional S3 (or S3-compatible) data-lake archive for a ClickHouse
/// destination. Every batch synced into ClickHouse is also written as
/// Parquet to `s3://{bucket}/{prefix}/{dest_table}/dt=<date>/run=<id>/
/// part-<partition>.parquet` — one streamed file per parallel partition,
/// never fully buffered in memory (see `crate::archive`).
#[derive(Debug, Clone)]
pub struct S3ArchiveConfig {
    pub bucket: String,
    /// Key prefix within the bucket; empty string writes at the bucket root.
    pub prefix: String,
    /// `None` resolves the standard AWS credential chain (env vars, IAM
    /// role) via `AmazonS3Builder::from_env()` — set explicitly to override.
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    /// Custom endpoint for S3-compatible services (e.g. MinIO). When set,
    /// plain HTTP is allowed automatically (real AWS S3 always uses HTTPS).
    pub endpoint: Option<String>,
    pub compression: ParquetCompression,
}

/// How to write rows into BigQuery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BigQueryWriteMethod {
    /// `tabledata.insertAll` — a plain JSON POST over the REST API. The
    /// default: proven, no extra dependencies, but the older/lower-throughput
    /// path (and it bills, unlike the free Storage Write API).
    #[default]
    InsertAll,
    /// The BigQuery Storage Write API (gRPC, protobuf) — modern, free, and
    /// higher-throughput. Opt-in: rows are encoded to protobuf and appended to
    /// the table's `_default` stream. See `sink::bigquery_proto`.
    StorageWrite,
}

/// Where to write to, when the destination is Google BigQuery.
#[derive(Debug, Clone)]
pub struct BigQueryDestConfig {
    /// GCP project ID. If `None`, resolved from the credentials (both ADC and
    /// service-account key files normally embed/resolve a project ID).
    pub project_id: Option<String>,
    /// Path to a service-account JSON key file. If `None`, falls back to
    /// Application Default Credentials (`GOOGLE_APPLICATION_CREDENTIALS`,
    /// `GOOGLE_APPLICATION_CREDENTIALS_JSON`, the metadata server, or the
    /// gcloud CLI's well-known ADC file).
    pub credentials_file: Option<String>,
    /// Inline service-account JSON key contents (an alternative to writing a
    /// file — e.g. loaded straight from a secrets manager). Takes precedence
    /// over `credentials_file` when both are set.
    pub credentials_json: Option<String>,
    /// Destination dataset (BigQuery's equivalent of ClickHouse's `database`).
    /// `dest_table` names a bare table within it.
    pub dataset_id: String,
    /// How rows are written into BigQuery (default `InsertAll`). Only meaningful
    /// when BigQuery is the destination; ignored when it's the source.
    pub write_method: BigQueryWriteMethod,
}

/// Which destination engine to write to. Mirrors [`SourceConfig`]. `sync.rs`
/// builds the matching [`crate::sink::Sink`] from this and dispatches DDL,
/// inserts, atomic full-refresh swap, and incremental watermark state through
/// it uniformly regardless of which destination is chosen.
#[derive(Debug, Clone)]
pub enum DestinationConfig {
    ClickHouse(ClickHouseConfig),
    BigQuery(BigQueryDestConfig),
}

impl DestinationConfig {
    /// A short label identifying the destination, used in log lines.
    pub fn kind(&self) -> &'static str {
        match self {
            DestinationConfig::ClickHouse(_) => "clickhouse",
            DestinationConfig::BigQuery(_) => "bigquery",
        }
    }

    pub fn dest_kind(&self) -> DestKind {
        match self {
            DestinationConfig::ClickHouse(_) => DestKind::ClickHouse,
            DestinationConfig::BigQuery(_) => DestKind::BigQuery,
        }
    }
}

/// Which destination engine a transfer targets — a light discriminant threaded
/// into [`crate::transform::plan`] for destination-aware type decisions (e.g.
/// promoting a `NUMERIC`-overridden column to `Decimal128` only for BigQuery)
/// without carrying the whole [`DestinationConfig`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestKind {
    ClickHouse,
    BigQuery,
}

/// Full-refresh reloads everything; Incremental upserts rows past a watermark;
/// Append inserts rows past a watermark WITHOUT staging/merge/dedup (a
/// bronze-landing write — the caller de-duplicates downstream). Append is
/// currently supported only for HTTP API sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    Full,
    Incremental,
    Append,
}

/// How to seed the incremental cursor on the **first** run for a state
/// identity (i.e. when `_quickhouse_state` has no row yet). Once a real
/// watermark has been persisted this is ignored, so it self-retires after the
/// first successful run — a safe no-op thereafter.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum WatermarkSeed {
    /// First run reads the whole table (today's behavior).
    #[default]
    None,
    /// First run reads only rows past this explicit watermark floor.
    Value(String),
    /// First run seeds the cursor to the source's current MAX(watermark),
    /// reading (almost) nothing — for when the destination already holds
    /// complete data from a prior/legacy pipeline and a full first pull would
    /// be a doomed waste. Replaces the manual `INSERT INTO _quickhouse_state`
    /// hack.
    CurrentMax,
}

/// One table transfer.
#[derive(Debug, Clone)]
pub struct TransferConfig {
    /// Source table (schema-qualified allowed). Ignored if `source_query` is set.
    pub source_table: Option<String>,
    /// Custom SELECT to read from instead of a whole table.
    pub source_query: Option<String>,
    /// Destination table name (a bare name within the ClickHouse `database` or
    /// BigQuery `dataset_id`).
    pub dest_table: String,
    /// Stable identity for the persisted incremental cursor in
    /// `_quickhouse_state`. `None` (default) derives it from `source_table`,
    /// else the `source_query` text (byte-identical to pre-`state_key`
    /// behavior — so existing state is never orphaned). Set it to (a) keep the
    /// cursor stable when you edit a `source_query`'s WHERE/SELECT (whose text
    /// would otherwise change the derived key and silently reset the cursor),
    /// and (b) give two syncs that share a `dest_table` but track different
    /// `watermark` columns distinct cursors (which otherwise collide on one
    /// state row). See [`TransferConfig::effective_state_key`].
    pub state_key: Option<String>,

    pub mode: SyncMode,

    /// Column used for the incremental high-water mark (required for Incremental).
    /// `WHERE watermark > x` never matches a NULL value, so if this column is
    /// nullable, rows with a NULL watermark are silently excluded from every
    /// incremental run, forever (the transfer still reports success). A
    /// Postgres/MySQL source warns once per run (with the count) when this is
    /// detected — see `sync::warn_on_null_watermark`.
    pub watermark: Option<String>,
    /// Postgres/MySQL only: a raw SQL expression used in place of `watermark`
    /// when building the incremental filter and the boundary-max probe (the
    /// projected `watermark` output is left untouched). `None` (default) uses
    /// `watermark` itself, byte-identical to before.
    ///
    /// **Why this exists.** With `source_query`, the generated read is `SELECT
    /// ... FROM (<source_query>) AS _src WHERE <watermark> > $1` — an outer
    /// wrapper around whatever `source_query` projects. If `source_query`
    /// computes `watermark` from an expression (a cast, a timezone shift, a
    /// concatenation — anything other than a bare pass-through of an indexed
    /// base-table column), the filter binds to that *computed* value, not the
    /// underlying column, so no index on the base table can serve it — a full
    /// scan on every incremental run regardless of table size. Have
    /// `source_query` additionally project the raw, indexed column under a
    /// second name (e.g. `write_date AS write_date_raw`) and set
    /// `watermark_source_expr="write_date_raw"`: the filter and probe then
    /// bind to that bare pass-through column (which Postgres/MySQL can push
    /// down to the index), while `watermark`'s own projection keeps emitting
    /// the transformed value used everywhere else (DDL, dest column, persisted
    /// cursor comparison domain).
    pub watermark_source_expr: Option<String>,
    /// Widen the tracked watermark's lower bound by this many seconds before
    /// filtering, so a run re-includes a trailing window of already-synced
    /// rows (catches late-arriving/edited rows that don't monotonically bump
    /// the watermark). `0` disables this (default; byte-identical to the
    /// pre-lookback filter). Requires `key` or `order_by` to be set (relies
    /// on the destination's upsert/dedup to replace the overlap rather than
    /// duplicate them) and a `watermark` column that resolves to a date or
    /// timestamp type.
    pub lookback_seconds: u64,
    /// How to seed the incremental cursor on the first run for this state
    /// identity (default [`WatermarkSeed::None`] = read the whole table, as
    /// before). Only meaningful in incremental mode; ignored once a real
    /// watermark has been persisted. See [`WatermarkSeed`].
    pub seed_watermark: WatermarkSeed,
    /// Whether a successful incremental run persists (advances) the watermark.
    /// `true` (default) is today's behavior. Set `false` to read+merge a
    /// window WITHOUT moving the scheduled cursor — the primitive a bounded
    /// backfill needs so it doesn't rewind the regular schedule. Incremental
    /// mode only.
    pub advance_watermark: bool,
    /// Business/dedup key. ClickHouse: contributes to `ORDER BY` when no
    /// explicit `order_by` is given. BigQuery: contributes to `Clustering`
    /// alongside `order_by` (see its docs) — BigQuery has no dedicated key
    /// concept.
    pub key: Vec<String>,

    // ---- DDL / auto-create ----
    pub create_if_missing: bool,
    /// ClickHouse engine, e.g. `MergeTree` or `ReplacingMergeTree`. Ignored
    /// for a BigQuery destination (no engine concept there).
    /// When `None`, chosen by mode (Full -> MergeTree, Incremental -> ReplacingMergeTree).
    pub engine: Option<String>,
    /// ClickHouse: `ORDER BY` columns for generated DDL (falls back to `key`
    /// if empty). BigQuery: combined with `key` into `Clustering.fields` (at
    /// most 4 columns total — a clear config error if more are given, not a
    /// silent truncation).
    pub order_by: Vec<String>,
    /// ClickHouse: a `PARTITION BY` SQL expression (e.g. `toYYYYMM(date)`).
    /// BigQuery: must instead be a bare `DATE`/`TIMESTAMP`/`DATETIME` column
    /// name (BigQuery's time partitioning takes a column, not an expression)
    /// — mapped to `TimePartitioning`; a clear error if the name doesn't
    /// resolve to one of those types.
    pub partition_by: Option<String>,
    pub primary_key: Vec<String>,
    /// BigQuery-destination incremental only: prune the `MERGE`'s destination
    /// scan to the staging batch's range on this column, so BigQuery reads only
    /// the touched partitions instead of full-scanning the (possibly huge)
    /// destination table on every merge. `None` (default) full-scans, as before.
    ///
    /// **Correctness contract — read before setting.** This is ONLY safe when
    /// the named column is IMMUTABLE for a given merge `key` (its value never
    /// changes across updates to the same row), and it should be the table's
    /// partition column. A `create_date`/inserted-at column is safe: a row's
    /// value stays put, so the existing target row always lives in the
    /// partition the staging row implies. A `write_date`/updated-at column is
    /// **NOT** safe: an updated row's new `write_date` points at a different
    /// partition than where the old row lives, so pruning would miss it, fall
    /// through to `WHEN NOT MATCHED`, and INSERT A DUPLICATE KEY instead of
    /// updating. (This is the historical `merge_query_filter` duplicate-id bug;
    /// do not "optimize" a mutable partition column into this field.) quickhouse
    /// cannot detect mutability, so this is a deliberate per-table opt-in.
    ///
    /// The bound is expressed as literals resolved from the staging batch before
    /// the statement is built, not as subqueries over staging — see
    /// [`Self::merge_prune_key_range`] for why it has to be, and what that
    /// costs.
    pub merge_prune_partition_by: Option<String>,
    /// BigQuery-destination incremental only: bound the `MERGE`'s destination
    /// scan to the staging batch's `[MIN, MAX]` range on the merge `key` itself.
    /// `true` (default) emits the bound; `false` restores the unbounded
    /// `ON T.key = S.key`.
    ///
    /// **Why this is on by default**, unlike
    /// [`Self::merge_prune_partition_by`]. That knob bounds a *different*
    /// column than the one being joined, which is why it needs an immutability
    /// contract: an updated row whose `write_date` moved lives in a partition
    /// the staging row no longer implies, so pruning misses it and the merge
    /// inserts a duplicate key. Bounding on the join key has no such hazard,
    /// because it is tautological rather than an assumption — a destination row
    /// can only match a staging row by holding that row's exact key value, and
    /// that value is inside the staging batch's own `[MIN, MAX]` by
    /// construction. Nothing matchable can fall outside the bound, so there is
    /// no configuration in which this changes which rows merge; a NULL key is
    /// equally unmatchable with or without it.
    ///
    /// It pays off when `dest` is clustered by the merge key — which is what
    /// quickhouse's own generated DDL does (see [`Self::key`]) — since BigQuery
    /// can then skip whole blocks instead of scanning the full destination on
    /// every run. On an unclustered or differently-clustered table it costs one
    /// small query against the staging table (the probe below) plus a predicate
    /// evaluated during a scan that was happening anyway.
    ///
    /// **How the bound is expressed, and why it matters.** As literals:
    /// quickhouse probes the staging batch's `[MIN, MAX]` per key column, then
    /// pastes the values into the `ON` clause. It cannot be the obvious
    /// `BETWEEN (SELECT MIN(k) FROM staging) AND (SELECT MAX(k) FROM staging)`,
    /// because BigQuery rejects a subquery referencing a table inside a join
    /// predicate — `Unsupported subquery with table in join predicate` — while
    /// it *analyses* the statement, which means for every batch including an
    /// empty one. 0.14.0 shipped that form and every BigQuery `MERGE` failed;
    /// see the 0.14.1 changelog entry. BigQuery renders the literals itself
    /// (`FORMAT('%T', …)`), so quickhouse formats no types by hand and the
    /// `MIN`/`MAX` that picks a bound is evaluated by the same engine, over the
    /// same table, as the `BETWEEN` that uses it.
    ///
    /// An empty batch leaves `MIN`/`MAX` NULL, so no bound is emitted at all —
    /// always safe, since an absent bound only widens the scan.
    ///
    /// Ignored when [`Self::delete_stale_in_window`] is set: that feature's
    /// `WHEN NOT MATCHED BY SOURCE` clause deletes destination rows the source
    /// pull no longer has, and it can only see rows the `ON` clause admits — so
    /// a key-range bound there would quietly narrow "replace this window" to
    /// "replace this key range", leaving deleted-at-source rows outside the
    /// batch's key span alive. Those transfers keep the partition-scoped bound
    /// they already required.
    pub merge_prune_key_range: bool,
    /// BigQuery-destination incremental only: when the staging batch holds at
    /// most this many distinct merge-key values, bound the `MERGE`'s
    /// destination scan to that exact key *list* (`T.k IN (v1, v2, …)`) instead
    /// of the `[MIN, MAX]` range of [`Self::merge_prune_key_range`]. `0`
    /// (default) disables it and keeps the range bound alone.
    ///
    /// **Why a list, when a range is already emitted.** Both are the same
    /// tautology — a destination row can only match a key the batch contains —
    /// so neither needs an immutability contract and neither can change which
    /// rows merge. They differ entirely in how well they *bind*. A range bound
    /// prunes in proportion to how tightly the changed keys cluster: on a
    /// table whose rows are only ever appended, the delta sits at the top of
    /// the key space and `[MIN, MAX]` is narrow. On a table whose rows are
    /// updated after insert — a user profile, an order status, a voucher
    /// redemption — the changed keys are scattered across the whole key space,
    /// `[MIN, MAX]` covers nearly the entire table, and the prune is correct
    /// and useless at the same time. A key list does not degrade that way: it
    /// names exactly the keys in the batch, and BigQuery prunes an `IN` list
    /// against a clustered table as well as it prunes a range.
    ///
    /// **What it costs.** One extra query over the staging table per merge, to
    /// read up to `merge_prune_key_list_max + 1` distinct key values. If the
    /// batch turns out to hold more distinct keys than the limit, the list is
    /// abandoned and the run falls back to the range bound — so the ceiling is
    /// a real ceiling on statement size, not just a hint. Set it to a few
    /// thousand: large enough to cover a normal incremental delta, small
    /// enough that the generated `IN` list stays a sane statement.
    ///
    /// **Applies only to a single-column `key`.** A composite key would need
    /// an `IN UNNEST([STRUCT(…), …])` form whose pruning behaviour is not the
    /// same; those transfers keep the per-column range bound.
    ///
    /// Ignored — like [`Self::merge_prune_key_range`] — when
    /// [`Self::delete_stale_in_window`] is set, for the same reason: narrowing
    /// the `ON` clause would strand deleted-at-source rows outside the batch's
    /// keys.
    pub merge_prune_key_list_max: usize,
    /// Incremental only: additionally `DELETE` destination rows *inside the
    /// merged window* that are absent from the source pull, giving "replace
    /// this window" semantics and making a NULL merge key self-correct (net
    /// replace) instead of duplicating on re-runs. This is the only way an
    /// ordinary sync converges with a source that hard-deletes rows.
    ///
    /// **Requires `merge_prune_partition_by`** (a hard config error otherwise):
    /// the DELETE is scoped to that immutable column's `[MIN, MAX]` range in the
    /// staging batch — the SAME bound the prune uses. Without a window bound
    /// the delete would remove the ENTIRE destination history outside the
    /// delta, so it is never allowed unscoped. `false` (default) keeps the
    /// insert-or-update-only merge.
    ///
    /// **How each destination performs it.**
    /// - BigQuery: a `WHEN NOT MATCHED BY SOURCE AND <window>` clause inside
    ///   the same `MERGE`, so it is atomic with the upsert. BigQuery reports
    ///   one combined affected-row count for the statement, so
    ///   [`TransferResult::rows_deleted`] stays `0` here.
    /// - ClickHouse: a lightweight `DELETE FROM dest WHERE <window> AND key
    ///   NOT IN (SELECT key FROM staging)`, run just before the staged rows are
    ///   promoted. This forces the run to stage (ClickHouse incremental
    ///   otherwise inserts directly), because the delete needs a materialised
    ///   batch to subtract from. Not atomic with the insert: a reader between
    ///   the two statements sees the window with stale rows already removed and
    ///   the new ones not yet in. `rows_deleted` reports the exact count.
    ///
    /// To *measure* drift against a source without repairing it — including
    /// outside any sync — see [`crate::reconcile::reconcile_keys`].
    pub delete_stale_in_window: bool,

    /// Full-refresh only: allow the swap even when it would leave the
    /// destination with FEWER rows than it had before.
    ///
    /// `mode="full"` replaces the destination wholesale — ClickHouse
    /// `EXCHANGE TABLES`, BigQuery `TRUNCATE` + `INSERT ... SELECT`. **Neither
    /// is partition-aware.** A run that reads one partition's worth of data and
    /// swaps it in therefore destroys every *other* partition, atomically and
    /// with no error. That is not an API-source quirk: a Postgres full refresh
    /// scoped to one month, swapped into a monthly-partitioned destination,
    /// loses the other eleven months by exactly the same code path.
    ///
    /// `false` (the default) turns that into a hard error *before* the swap
    /// runs. Set `true` only when a full refresh is genuinely expected to
    /// shrink the destination — i.e. the source really did lose rows. To add
    /// to a destination rather than replace it, use `mode="incremental"` with
    /// `key=`, or `mode="append"`.
    pub allow_full_refresh_shrink: bool,

    // ---- parallelism / batching ----
    /// How many partitions read concurrently. `0` means "derive from the host"
    /// — [`crate::host::available_cpus`], which already reflects a container's
    /// CPU quota rather than the whole machine's core count. Resolved in
    /// [`Self::normalize`], so everything downstream sees a concrete number.
    ///
    /// Note this is a ceiling on *available* fan-out, not a promise of it: the
    /// read must also be partitionable for it to matter at all (see
    /// [`Self::partition_source_expr`], without which a `source_query` transfer
    /// runs single-stream no matter what this says).
    pub parallelism: usize,
    /// Per-batch granularity: flush a RecordBatch once it reaches this many
    /// rows. Controls how big each individual insert is (a throughput/overhead
    /// knob), NOT the overall memory ceiling — that's `max_memory_bytes`.
    pub batch_rows: usize,
    /// Per-batch granularity: also flush once a batch's accumulated (estimated)
    /// source bytes reach this many, even if `batch_rows` hasn't been hit yet,
    /// so a single batch of wide rows doesn't grow unbounded. `0` disables this
    /// per-batch byte cap (row count alone decides batch size). This bounds one
    /// *batch*; the total in-flight memory across all partitions and in-flight
    /// inserts is bounded separately by `max_memory_bytes`.
    pub batch_bytes: usize,
    /// Size `max_memory_bytes` as this fraction of the memory this process is
    /// actually allowed to use, instead of as an absolute byte count. `0.0`
    /// (default) keeps `max_memory_bytes` as given. Values above `1.0` are
    /// clamped.
    ///
    /// **Why a fraction is the better unit.** An absolute ceiling can't know how
    /// many peers it has. Four containers capped at 4 GiB each on one 16 GiB VM
    /// all want a different number from one container that owns the whole box,
    /// and only the container itself knows which it is. Resolved against the
    /// cgroup limit where there is one (so it reads *this* container's share,
    /// not the host's total), falling back to total system memory. If the host
    /// won't say — notably on non-Linux — `max_memory_bytes` is left exactly as
    /// configured, so a fraction never silently becomes "unbounded".
    ///
    /// Suggested starting point: `0.25`. The pipeline's measured peak RSS is
    /// ~100 MB at default batch sizes, so the ceiling normally has plenty of
    /// headroom; its job is to bound the pathological case, not the usual one.
    pub max_memory_fraction: f64,
    /// Target size of one insert to the destination, in bytes of real Arrow
    /// memory (measured the same way `batch_bytes` and `max_memory_bytes` are —
    /// not post-compression wire size). Decoded batches accumulate until they
    /// reach this, then go out as a single insert. `0` disables coalescing
    /// entirely: one insert per decoded batch, the pre-0.14 behavior.
    ///
    /// **Why this is separate from `batch_bytes`.** `batch_bytes` is decode
    /// granularity — how much Arrow to build before handing a batch onward — and
    /// that is all it was ever documented to be. But every insert call site
    /// passed exactly one batch, so it silently became the *insert* size too. At
    /// the 4 MiB default, a 19.4M-row table meant 900+ HTTP round-trips and 900+
    /// new ClickHouse parts, when ClickHouse's own guidance is fewer and larger
    /// (10k–100k rows minimum) precisely to hold down part count and the
    /// background merge load it creates — which on ClickHouse Cloud competes
    /// with query memory on the same service.
    ///
    /// Raising this does not raise peak memory: `max_memory_bytes` still bounds
    /// everything decoded-but-not-yet-landed (each buffered batch holds its own
    /// reservation), and the destination serializes its payload incrementally
    /// rather than materializing it, so a larger insert costs a larger *stream*,
    /// not a larger buffer.
    pub insert_bytes: usize,
    /// Hard ceiling on total in-flight Arrow batch memory across the whole
    /// transfer — every partition's decoded-but-not-yet-sent batches plus all
    /// batches currently being uploaded. Enforced against each batch's real
    /// `RecordBatch::get_array_memory_size()`, so it holds regardless of
    /// `parallelism`, row width, or partition skew. When the ceiling is
    /// reached, decoding blocks (backpressure) until in-flight inserts drain.
    /// `0` disables the ceiling (unbounded — memory then scales with
    /// parallelism and batch size, the pre-`max_memory_bytes` behavior).
    pub max_memory_bytes: usize,
    /// Column used to split the table into parallel range partitions.
    /// Defaults to the first `key` column, else the sync falls back to a single stream.
    pub partition_column: Option<String>,
    /// Postgres/MySQL only: a raw SQL expression that resolves the partition
    /// column *inside* a `source_query`, which is what makes range partitioning
    /// possible at all for a custom query. `None` (default) keeps the old
    /// behavior exactly: a `source_query` transfer runs single-stream.
    ///
    /// **Why this exists.** Range partitioning needs two things a bare
    /// `source_table` gives for free: a cheap `MIN`/`MAX` probe to find the key
    /// span, and a per-partition `key >= lo AND key <= hi` predicate an index
    /// can serve. With `source_query` set, the planner had no column it could
    /// trust for either, so it returned a single partition — meaning
    /// `parallelism` was silently inert for every custom-query transfer, however
    /// large. Since a `CAST` (or any other projection fix-up) can only live in
    /// `source_query`, that covered essentially every non-trivial table.
    ///
    /// Set this to the name `source_query` projects the raw, indexed key column
    /// under (e.g. `id AS id_raw` -> `partition_source_expr="id_raw"`). The
    /// planner then probes `SELECT MIN(<expr>), MAX(<expr>) FROM (<source_query>)
    /// AS _src` and builds its range predicates over `<expr>`, so both bind to a
    /// bare pass-through column the source can push down to the index — exactly
    /// the arrangement [`Self::watermark_source_expr`] establishes for the
    /// incremental filter. It may name the same column as `partition_column`
    /// when `source_query` passes that column through unchanged.
    ///
    /// **Two costs to know.** (1) The `MIN`/`MAX` probe now runs against the
    /// wrapped query, not a base table. A single-table `source_query` flattens
    /// and still resolves via index, but a query with joins or aggregation pays
    /// for one extra evaluation per run. (2) Unlike the implicit base-table path
    /// — which degrades quietly to one stream when the key isn't range-able —
    /// an expression set here that is missing or non-integer is a hard error,
    /// not a silent fallback: you asked for fan-out explicitly, so failing to
    /// deliver it silently would just recreate the bug this field fixes.
    pub partition_source_expr: Option<String>,
    /// Optional cap on how many source rows are pulled **per second**, summed
    /// across all parallel partitions (a global limiter, not per-connection).
    /// Deliberately paces the read so a small/production database isn't
    /// hammered by a bulk export: after each batch is read, the reader sleeps
    /// long enough to hold the aggregate rate at this ceiling, which — because
    /// `COPY TO STDOUT` streams only as fast as the client consumes — makes the
    /// server-side scan itself back off (TCP backpressure), not just the
    /// client. `None` (default) reads as fast as possible. Applies to the
    /// PostgreSQL and MySQL sources; ignored for a BigQuery source (its read
    /// path is a managed, separately-metered API).
    pub read_max_rows_per_sec: Option<u64>,
    /// Fail the transfer when **no source rows arrive** for this many seconds.
    /// `0` (default) disables it.
    ///
    /// **This is the knob that `statement_timeout_secs` is not.** A source-side
    /// `statement_timeout` looks like a cap on query duration, but quickhouse
    /// streams the source result set straight into the destination, so the
    /// source statement's cursor stays open for the *entire* transfer. That
    /// makes `statement_timeout_secs` a ceiling on read + decode + destination
    /// write + backpressure — a source query that takes 0.6 seconds server-side
    /// still trips it when the *destination* is throttling, and it reports the
    /// failure as a source error (`57014 canceling statement due to statement
    /// timeout`), pointing an operator at a query and an index that are fine.
    /// Worse, retries cannot rescue it: every attempt restarts from zero
    /// against the same ceiling, so a table that crosses the line fails every
    /// attempt, on a condition that has nothing to do with the source.
    ///
    /// This timer measures only the awaits on the source stream itself. Time
    /// spent decoding, inserting, or blocked on the memory budget does not
    /// count toward it, so a slow destination never trips it — which is what
    /// makes it safe to set tightly. A genuinely hung source does trip it,
    /// which is the condition an operator actually wants to detect.
    ///
    /// The resulting error is classified transient, so `retry_max_attempts`
    /// retries the whole transfer.
    ///
    /// Applies to the PostgreSQL, MySQL and BigQuery source reads. API sources
    /// are paced by their own per-request HTTP timeouts instead.
    ///
    /// Setting this lets `statement_timeout_secs` go back to meaning what its
    /// name says — a guard on the source database against a runaway scan — at
    /// a value sized for the source rather than for the destination's worst day.
    pub read_idle_timeout_secs: u64,

    // ---- 0.5.0 block (kept contiguous; append new fields here) ----
    /// Incremental + ClickHouse-destination only: read the source in
    /// keyset-ordered chunks of this many rows, committing the watermark per
    /// chunk so a mid-read failure resumes instead of restarting from the last
    /// run's watermark. `None` (default) = one unbounded read per partition, as
    /// before. Requires a keyset ordering column (see [`Self::keyset_column`])
    /// that is a **unique, NOT NULL integer** — ties or NULLs would silently
    /// skip rows. Chunked mode runs single-stream (range partitioning is off).
    pub chunk_rows: Option<usize>,
    /// Max total attempts for the whole transfer when it fails with a
    /// *transient source* error (PostgreSQL hot-standby recovery conflict /
    /// statement cancel; MySQL server-gone-away / lock-wait / deadlock).
    /// `1` (default) = no retry, byte-identical to before. Sink/write-side
    /// retries are separate and always on (see `sink::backoff_delay`).
    pub retry_max_attempts: u32,
    /// Per-column SQL value transforms applied in the source `SELECT`
    /// (source-column name -> expression, e.g. `"CAST(x AS TEXT)"`,
    /// `"col AT TIME ZONE 'UTC'"`, `"ROUND(amt, 9)"`). Applied over
    /// `source_table=` so range partitioning is preserved (unlike a
    /// `source_query`). Changes the *value*, not the resolved column type —
    /// combine with `type_overrides` if the destination type must change too.
    /// Not supported for a BigQuery source (use `source_query` there).
    pub column_transforms: HashMap<String, String>,
    /// Declares the Arrow decode type a `column_transforms` entry's SQL
    /// actually produces on the wire (source column name -> one of
    /// `"Boolean"`, `"Int16"`, `"Int32"`, `"Int64"`, `"UInt32"`, `"Float32"`,
    /// `"Float64"`, `"Utf8"`, `"Binary"`, `"Date32"`). Requires a matching
    /// `column_transforms` entry for the same column — a config error
    /// otherwise.
    ///
    /// **Why this exists (bug report B6).** `column_transforms`' own doc
    /// says "changes the value, not the resolved type — pair it with
    /// `type_overrides` if the type must change too", but that pairing
    /// doesn't actually work: `type_overrides` only changes the *declared
    /// destination* type string, not what this crate decodes the source's
    /// binary wire data as (still the untransformed source column's own
    /// type). So `column_transforms={"active": "CAST(active AS TEXT)"}`
    /// with `type_overrides={"active": "STRING"}` still decodes the wire
    /// bytes as the original `bool`, corrupting values (or, downstream,
    /// erroring as a bool-vs-STRING proto/type mismatch). Set
    /// `column_transform_types={"active": "Utf8"}` alongside the transform
    /// to fix the decode side too. A datetime or decimal type change should
    /// still go through `type_overrides`, which already carries the extra
    /// timezone/precision info those need and isn't affected by this field.
    pub column_transform_types: HashMap<String, String>,
    /// Opt-in schema evolution: when the source has a column the existing
    /// destination table lacks, `ALTER TABLE ADD COLUMN` (as Nullable) instead
    /// of hard-erroring. `false` (default) preserves today's behavior. Never
    /// drops or retypes a column.
    ///
    /// Full-refresh against an *existing* destination now also needs this to
    /// pick up a genuinely new source column (fixed alongside bug report B4:
    /// full-refresh staging mirrors the destination's actual DDL instead of
    /// silently rebuilding it from `cfg`, so a new column is no longer added
    /// to staging for free the way it used to be — it's evolved in, same as
    /// incremental).
    pub evolve_schema: bool,

    // ---- 0.9.0 block (configurable internal names; defaults preserve prior behavior) ----
    /// Name of quickhouse's internal watermark/chunk-cursor bookkeeping table,
    /// created inside the destination database/dataset. Default
    /// `_quickhouse_state`. Override for teams with table-naming policies (an
    /// incremental cursor persisted under the old name won't be found after a
    /// rename — treat a change as a first run).
    pub state_table_name: String,
    /// Suffix for the per-run staging table name (`{dest}{suffix}_{run_id}`).
    /// Default `_quickhouse_tmp`.
    pub staging_suffix: String,
    /// Client application name announced to the source server — PostgreSQL
    /// `application_name` (visible in `pg_stat_activity`, so a DBA can see/kill
    /// the export). Default `quickhouse`.
    pub application_name: String,

    // ---- transforms ----
    /// Per-column destination type overrides (column name -> the
    /// destination's own type name, e.g. ClickHouse `"Decimal(18, 2)"` or
    /// BigQuery `"NUMERIC"`/`"BIGNUMERIC"`).
    pub type_overrides: HashMap<String, String>,
    /// Source column -> destination column renames.
    pub rename: HashMap<String, String>,
    /// If non-empty, only these source columns are transferred.
    pub include: Vec<String>,
    /// Source columns to drop.
    pub exclude: Vec<String>,
    /// Destination (post-rename) column names to force `NOT NULL` in
    /// generated DDL, regardless of the source's own resolved nullability.
    ///
    /// **Why this exists (bug report B5).** Nullability is otherwise always
    /// taken from the source: a `key`/`order_by`/`primary_key` column is
    /// already forced non-nullable (ClickHouse rejects a nullable sort key
    /// outright), but a column used only in `partition_by`'s expression
    /// (e.g. `toYYYYMM(create_date)`) isn't covered by that check, so if the
    /// source reports it nullable — e.g. every BigQuery column not
    /// explicitly declared `REQUIRED` — the generated `Nullable(...)`
    /// partition key is then rejected by ClickHouse
    /// (`allow_nullable_key` is off by default). List such columns here.
    /// Only affects DDL generated *from scratch* (a table created fresh) —
    /// full-refresh against an *existing* destination now clones its actual
    /// DDL/nullability instead of regenerating it (see `Sink::clone_table_structure`),
    /// so this has nothing to add there.
    pub not_null: Vec<String>,
    /// Default destination type for **every** arbitrary-precision decimal
    /// column (PostgreSQL `numeric`, MySQL `DECIMAL`/`NEWDECIMAL`, BigQuery
    /// `NUMERIC`) that has no `type_overrides` entry of its own — e.g.
    /// `"Decimal(38, 9)"`. `None` (default) keeps the historical `Float64`
    /// mapping.
    ///
    /// **Why this exists (bug report B7b).** Those source types are exact
    /// decimals with no `f64` equivalent, so the default mapping round-trips
    /// them through IEEE-754 and reproduces the result: a stored `32.9` can
    /// arrive as `32.89999999999999`. Confirmed in production — 2,457 of
    /// 179,478 sampled rows of one Odoo `numeric` column already carry exactly
    /// this noise, and 7 of 734,047 rows of another.
    ///
    /// Exact decoding was always reachable per column
    /// (`type_overrides={col: "Decimal(P,S)"}`), which is the problem: it has to
    /// be remembered for every affected column in every table, and forgetting it
    /// is silent. Setting this once covers them all.
    ///
    /// Not the default, because it changes the *destination column type*:
    /// against a table that already exists with a `Float64`/`FLOAT64` column,
    /// switching the decode type to `Decimal128` would mean writing a decimal
    /// into a float column. Choose the precision and scale deliberately —
    /// a value that doesn't fit is coerced to NULL (counted and warned about,
    /// see `warn_coerced_decimals`), so pick a scale that covers the column's
    /// real range. P > 38 needs `Decimal256`, which isn't supported yet.
    pub numeric_as_decimal: Option<String>,
    /// MySQL only: whether a `tinyint(1)` column is mapped to Boolean
    /// (`Bool`/`BOOL`) rather than a small integer. Default `true`.
    ///
    /// **Why this exists (bug report B8).** MySQL doesn't have a boolean type;
    /// `BOOL` is an alias for `tinyint(1)`, so the *display width* of 1 is the
    /// only signal that a `tinyint` was meant as a flag. That convention holds
    /// for Rails/PHP-style ORMs, and this crate followed it unconditionally —
    /// but it does not hold universally: Odoo, for one, declares genuinely
    /// integer columns as `tinyint(1)`. For those, every non-zero value
    /// (`2`, `3`, `-5`, ...) decoded to `true` and landed as `1`, silently
    /// destroying the distinction between them — a real production incident.
    ///
    /// `type_overrides` can't repair this: it changes only the *declared
    /// destination* type, while the value is already flattened by the Boolean
    /// Arrow builder before that type is relevant. Set this to `false` to
    /// decode such columns as the integers they are (`Int8`, or `UInt8` when
    /// the column is UNSIGNED). Rows that were *already* written as
    /// `true`/`false` keep whatever the destination stored; only future reads
    /// change. When left `true`, any value outside `{0, 1}` is counted and
    /// warned about at the end of the read rather than passing unnoticed.
    pub tinyint1_as_bool: bool,
}

impl TransferConfig {
    pub fn effective_engine(&self) -> String {
        if let Some(e) = &self.engine {
            return e.clone();
        }
        match self.mode {
            // Append is API-only (BigQuery dest, no engine concept); a plain
            // MergeTree is the sensible fallback for the ClickHouse-DDL path.
            SyncMode::Full | SyncMode::Append => "MergeTree".to_string(),
            SyncMode::Incremental => "ReplacingMergeTree".to_string(),
        }
    }

    /// Clear fields that a given mode doesn't use, so the effective config
    /// matches what actually runs. In full-refresh mode the watermark is
    /// meaningless — there's no "since last run" filter and the generated DDL
    /// uses a plain `MergeTree` (not `ReplacingMergeTree(<watermark>)`) — so a
    /// watermark passed alongside `mode="full"` is dropped here, and the
    /// returned `new_watermark` is `None`.
    pub fn normalize(&mut self) {
        // Host-derived tuning, resolved once so every later reader — the
        // partition planner, `buffer_unordered`, `MemoryBudget` — sees a
        // concrete number rather than a sentinel.
        if self.parallelism == 0 {
            self.parallelism = crate::host::available_cpus();
            tracing::info!(
                "parallelism derived from the host: {} CPU(s) available",
                self.parallelism
            );
        }
        if let Some(bytes) = crate::host::memory_fraction_bytes(self.max_memory_fraction) {
            tracing::info!(
                "max_memory_bytes derived from the host: {bytes} bytes ({} of the memory limit)",
                self.max_memory_fraction
            );
            self.max_memory_bytes = bytes;
        } else if self.max_memory_fraction > 0.0 {
            tracing::warn!(
                "max_memory_fraction={} was requested but this host's memory limit could not be \
                 determined; keeping max_memory_bytes={}",
                self.max_memory_fraction,
                self.max_memory_bytes
            );
        }
        if self.mode == SyncMode::Full {
            self.watermark = None;
            self.watermark_source_expr = None;
            // The seed only meaningfully floors an incremental cursor; a
            // full refresh has none, so clear it (mirrors `watermark`).
            self.seed_watermark = WatermarkSeed::None;
            // Chunked resumable reads are incremental-only (validate rejects
            // this combo; clearing keeps the effective config honest).
            self.chunk_rows = None;
        }
    }

    /// Identity of the persisted watermark row in `_quickhouse_state`.
    /// Overridable via `state_key`; otherwise the source table name, else the
    /// query text (unchanged default — so existing state rows keep matching).
    pub fn effective_state_key(&self) -> String {
        self.state_key
            .clone()
            .or_else(|| self.source_table.clone())
            .or_else(|| self.source_query.clone())
            .unwrap_or_default()
    }

    /// The keyset ordering column for chunked resumable reads: `partition_column`
    /// if set, else the first `key` column. Same resolution the range-partition
    /// planner uses, so chunked and partitioned reads agree on the column.
    pub fn keyset_column(&self) -> Option<String> {
        self.partition_column
            .clone()
            .or_else(|| self.key.first().cloned())
    }

    /// Validation for a DB source (Postgres/MySQL/BigQuery/ClickHouse) —
    /// byte-identical to the original `validate`.
    pub fn validate(&self) -> crate::error::Result<()> {
        self.validate_impl(SourceShape::Db)
    }

    /// Validation for an HTTP API source (CleverTap/AppsFlyer): no
    /// `source_table`/`source_query` is expected (the "what to read" lives on
    /// the source descriptor), and a few DB-only knobs are rejected.
    pub fn validate_api(&self) -> crate::error::Result<()> {
        self.validate_impl(SourceShape::Api)
    }

    /// Validation for an in-memory DataFrame source. Shares the API source's
    /// "no table, no SQL" rules and adds its own: there is no source to
    /// connect to, so nothing about filtering, partitioning, pacing or
    /// resuming applies — and incremental upserts on `key` alone, with no
    /// watermark, because the frame you passed *is* the delta.
    pub fn validate_frame(&self) -> crate::error::Result<()> {
        self.validate_impl(SourceShape::Frame)
    }

    fn validate_impl(&self, shape: SourceShape) -> crate::error::Result<()> {
        use crate::error::EtlError;
        let noun = shape.label();
        if shape == SourceShape::Db && self.source_table.is_none() && self.source_query.is_none() {
            return Err(EtlError::config(
                "either source_table or source_query must be set",
            ));
        }
        // Everything that is not a database read shares these: there is no SQL
        // for a transform to live in, and no cursor to chunk or widen.
        if shape != SourceShape::Db {
            if !self.column_transforms.is_empty() {
                return Err(EtlError::config(format!(
                    "column_transforms is not supported for {noun} (there is no source SQL for the \
                     expression to live in)"
                )));
            }
            if !self.column_transform_types.is_empty() {
                return Err(EtlError::config(format!(
                    "column_transform_types is not supported for {noun} (it overrides the decode \
                     type for a column_transforms entry, which is itself unsupported here)"
                )));
            }
            if self.chunk_rows.is_some() {
                return Err(EtlError::config(format!(
                    "chunk_rows (keyset resumable reads) is not supported for {noun}"
                )));
            }
            if self.lookback_seconds > 0 {
                return Err(EtlError::config(format!(
                    "lookback_seconds is not supported for {noun}"
                )));
            }
        }
        // A DataFrame source is exempt: a watermark drives a *resumable read
        // window*, and a frame has no read to resume — the caller already holds
        // every row. Incremental from a frame upserts on `key` instead, which
        // the rule below makes mandatory in its place.
        if matches!(self.mode, SyncMode::Incremental | SyncMode::Append)
            && self.watermark.is_none()
            && shape != SourceShape::Frame
        {
            return Err(EtlError::config(
                "watermark column is required for incremental and append mode (it drives the \
                 resumable date window)",
            ));
        }
        if shape == SourceShape::Frame
            && self.mode == SyncMode::Incremental
            && self.key.is_empty()
            && self.watermark.is_none()
        {
            return Err(EtlError::config(
                "incremental from a DataFrame upserts on key= alone, so key is required: \
                 key=[\"id\"]. There is no watermark to resume from — the frame you passed IS \
                 the delta. (Pass watermark= as well if you want it used as the version column \
                 for dedup ordering.)",
            ));
        }
        if self.lookback_seconds > 0 && self.mode != SyncMode::Incremental {
            return Err(EtlError::config(
                "lookback_seconds only applies to incremental mode",
            ));
        }
        // Append is a bronze-landing write: no staging, no merge, no swap. It
        // makes sense wherever the caller already knows the rows are new — an
        // API window, or a frame they just built.
        if self.mode == SyncMode::Append && shape == SourceShape::Db {
            return Err(EtlError::config(
                "append mode is currently supported only for HTTP API sources \
                 (CleverTap/AppsFlyer) and DataFrame sources",
            ));
        }
        if shape == SourceShape::Frame {
            self.validate_frame_only()?;
        }
        // seed_watermark / advance_watermark drive the resumable cursor, which
        // both incremental and append use (append inserts instead of merging).
        let cursor_mode = matches!(self.mode, SyncMode::Incremental | SyncMode::Append);
        if self.seed_watermark != WatermarkSeed::None && !cursor_mode {
            return Err(EtlError::config(
                "seed_watermark only applies to incremental or append mode",
            ));
        }
        if !self.advance_watermark && !cursor_mode {
            return Err(EtlError::config(
                "advance_watermark=false only applies to incremental or append mode",
            ));
        }
        if self.lookback_seconds > 0 && self.key.is_empty() && self.order_by.is_empty() {
            return Err(EtlError::config(
                "lookback_seconds requires key or order_by (otherwise the re-synced \
                 overlap window produces duplicate rows instead of an upsert)",
            ));
        }
        // `parallelism == 0` is no longer an error but the "derive from the
        // host" request; `normalize` turns it into a concrete count before
        // anything reads it. Nothing to validate here.
        if self.max_memory_fraction < 0.0 || self.max_memory_fraction > 1.0 {
            return Err(EtlError::config(
                "max_memory_fraction must be between 0.0 (use max_memory_bytes as given) and 1.0",
            ));
        }
        if self.batch_rows == 0 {
            return Err(EtlError::config("batch_rows must be >= 1"));
        }
        // A non-zero ceiling must at least admit a single batch's worth of
        // rows-ish of memory; guard against pathologically tiny values that
        // would stall every transfer. (0 = unbounded, always allowed.)
        if self.max_memory_bytes != 0 && self.max_memory_bytes < 64 * 1024 {
            return Err(EtlError::config(
                "max_memory_bytes must be 0 (unbounded) or >= 65536",
            ));
        }
        if self.read_max_rows_per_sec == Some(0) {
            return Err(EtlError::config(
                "read_max_rows_per_sec must be None (unlimited) or >= 1",
            ));
        }
        if self.chunk_rows == Some(0) {
            return Err(EtlError::config(
                "chunk_rows must be None (one-shot) or >= 1",
            ));
        }
        if self.chunk_rows.is_some() && self.mode != SyncMode::Incremental {
            return Err(EtlError::config(
                "chunk_rows (keyset resumable reads) only applies to incremental mode",
            ));
        }
        if self.chunk_rows.is_some() && self.keyset_column().is_none() {
            return Err(EtlError::config(
                "chunk_rows requires a keyset ordering column: set partition_column or key \
                 (it must be a UNIQUE, NOT NULL integer column, or ties silently skip rows)",
            ));
        }
        if self.delete_stale_in_window {
            if self.mode != SyncMode::Incremental {
                return Err(EtlError::config(
                    "delete_stale_in_window only applies to incremental mode",
                ));
            }
            if self.merge_prune_partition_by.is_none() {
                return Err(EtlError::config(
                    "delete_stale_in_window requires merge_prune_partition_by (the immutable \
                     window column to scope the DELETE); without it, WHEN NOT MATCHED BY SOURCE \
                     would delete the ENTIRE destination history outside the current batch",
                ));
            }
        }
        Ok(())
    }

    /// The knobs that mean nothing when the source is a frame already sitting in
    /// memory. Every one of these would otherwise be silently ignored, which is
    /// the failure mode this crate spends the most effort avoiding — so each
    /// names the knob *and* why it cannot apply.
    ///
    /// Deliberately **not** rejected, and simply ignored instead:
    /// `parallelism` (the read is single-stream, but the *write* side still
    /// fans out through `SendCtx::flush`'s `JoinSet`), `application_name`
    /// (Postgres-only), `tinyint1_as_bool` (MySQL-only) and `batch_rows` (the
    /// frame's own batch boundaries decide). Rejecting those would break a
    /// caller passing one shared kwargs dict to several transfers.
    fn validate_frame_only(&self) -> crate::error::Result<()> {
        use crate::error::EtlError;
        let reject = |msg: &str| -> crate::error::Result<()> { Err(EtlError::config(msg)) };
        if self.source_table.is_some() || self.source_query.is_some() {
            return reject(
                "source_table/source_query do not apply to a DataFrame source: it reads the frame \
                 you passed, not a table or a query. Filter the frame itself before handing it \
                 over.",
            );
        }
        if self.partition_source_expr.is_some() || self.watermark_source_expr.is_some() {
            return reject(
                "partition_source_expr/watermark_source_expr do not apply to a DataFrame source: \
                 there is no source query for an expression to resolve against.",
            );
        }
        if self.partition_column.is_some() {
            return reject(
                "partition_column does not apply to a DataFrame source: the frame is decoded \
                 single-stream from its own Arrow batches, so there is no range to split.",
            );
        }
        if self.read_max_rows_per_sec.is_some() {
            return reject(
                "read_max_rows_per_sec does not apply to a DataFrame source: there is no source \
                 server to be gentle to — the rows are already in memory.",
            );
        }
        if self.read_idle_timeout_secs > 0 {
            return reject(
                "read_idle_timeout_secs does not apply to a DataFrame source: there is no source \
                 stream that can stall.",
            );
        }
        if self.seed_watermark != WatermarkSeed::None || !self.advance_watermark {
            return reject(
                "seed_watermark/advance_watermark do not apply to a DataFrame source: it has no \
                 resumable cursor to seed or advance.",
            );
        }
        if self.retry_max_attempts > 1 {
            return reject(
                "retry_max_attempts does not apply to a DataFrame source: a whole-transfer retry \
                 exists to re-read a source that failed transiently, and there is nothing to \
                 re-read — the bytes are already in memory, so a retry would only redo the DDL. \
                 Destination blips are already retried at the insert layer.",
            );
        }
        Ok(())
    }
}

/// What a [`TransferWarning`] is about. Every variant names a condition
/// quickhouse detects and recovers from on its own — the transfer still
/// succeeds — but which changes what the destination now contains. These used
/// to be `tracing::warn!` lines only, which meant an orchestrator could not act
/// on them: a Dagster asset reported success while a column quietly rotted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum WarningKind {
    /// A MySQL `tinyint(1)` value outside `{0, 1}` was flattened to a boolean,
    /// losing the difference between e.g. 2 and 3. See
    /// [`TransferConfig::tinyint1_as_bool`].
    CollapsedBool,
    /// A date/datetime was coerced to NULL: a zero-date, or a year outside
    /// ClickHouse's representable `[1900, 2299]` window.
    CoercedDate,
    /// A decimal was coerced to NULL: it exceeded the declared `Decimal(P,S)`
    /// precision, or was NaN/Infinity.
    CoercedDecimal,
    /// An API source's scalar (int/float/bool/bytes) failed to parse -> NULL.
    CoercedScalar,
    /// The incremental watermark column is nullable and rows currently hold a
    /// NULL there. `WHERE watermark > x` never matches NULL, so those rows are
    /// excluded from this and every future incremental run — silently, and
    /// permanently. The most dangerous condition in this list.
    NullWatermark,
    /// A full refresh left the destination with fewer rows than it had, and
    /// `allow_full_refresh_shrink` permitted it. (Without that flag the same
    /// condition is a hard error, not a warning.)
    FullRefreshShrink,
    /// A BigQuery `MERGE` ran against a destination that is not clustered by
    /// the merge key, so the key-range prune had nothing to prune with and the
    /// statement scanned the whole table. Not a data problem — a cost one.
    UnclusteredMergeTarget,
}

impl WarningKind {
    /// Stable machine-readable name, for a caller matching on the kind (and
    /// what the Python binding exposes as `warning.kind`). Never reworded
    /// without a deprecation cycle, unlike the human-facing `message`.
    pub fn as_str(&self) -> &'static str {
        match self {
            WarningKind::CollapsedBool => "collapsed_bool",
            WarningKind::CoercedDate => "coerced_date",
            WarningKind::CoercedDecimal => "coerced_decimal",
            WarningKind::CoercedScalar => "coerced_scalar",
            WarningKind::NullWatermark => "null_watermark",
            WarningKind::FullRefreshShrink => "full_refresh_shrink",
            WarningKind::UnclusteredMergeTarget => "unclustered_merge_target",
        }
    }
}

/// One structured warning from a transfer — the data form of what also goes to
/// the log. Collected on [`TransferResult::warnings`] so a scheduler can *fail*
/// a run on a signal quickhouse already computes, instead of scraping stderr.
///
/// Warnings are aggregated per `(kind, column)` across the whole run, not
/// emitted per row: `count` is how many values tripped it, so a badly-legacy
/// table yields one entry with a large count rather than millions of entries.
#[derive(Debug, Clone)]
pub struct TransferWarning {
    pub kind: WarningKind,
    /// The source column responsible, when the condition is attributable to
    /// one. `None` for table-level conditions (a full-refresh shrink, an
    /// unclustered merge target).
    pub column: Option<String>,
    /// How many values/rows tripped this condition. Table-level warnings use
    /// the count that makes the condition concrete (rows lost to a shrink,
    /// rows holding a NULL watermark); `0` where no count applies.
    pub count: u64,
    /// A representative offending value, where capturing one is free at the
    /// point of detection. `None` otherwise — an absent sample never means the
    /// count is uncertain.
    pub sample: Option<String>,
    /// Human-facing text, the same sentence written to the log. Format is not
    /// stable; match on `kind` instead.
    pub message: String,
}

/// Summary returned to the caller after a transfer.
///
/// Beyond the row/byte counters, this carries the two things a caller
/// previously had to reconstruct from logs and follow-up queries: the
/// structured [`warnings`](Self::warnings) quickhouse detected, and a
/// phase breakdown of where the time went.
#[derive(Debug, Clone, Default)]
pub struct TransferResult {
    pub rows_read: u64,
    pub rows_written: u64,
    pub bytes_written: u64,
    /// Destination rows deleted by this run: the window-scoped delete of
    /// [`TransferConfig::delete_stale_in_window`] on a ClickHouse destination,
    /// or a [`crate::reconcile::reconcile_keys`] repair. `0` everywhere else.
    ///
    /// **BigQuery caveat:** a BigQuery `delete_stale_in_window` performs its
    /// delete inside the `MERGE`'s `WHEN NOT MATCHED BY SOURCE` clause, and
    /// BigQuery reports only one combined `numDmlAffectedRows` for the whole
    /// statement — inserts, updates and deletes together. There is no way to
    /// attribute the delete portion, so this stays `0` for that path rather
    /// than reporting a number that is really the merge's total.
    pub rows_deleted: u64,
    pub duration_secs: f64,
    /// Cumulative time the source readers spent *waiting on source rows* — the
    /// awaits on the source stream itself, excluding decode, insert and any
    /// backpressure. Summed across parallel readers, so with `parallelism > 1`
    /// it can legitimately exceed [`stage_secs`](Self::stage_secs); compare
    /// `read_secs / parallelism` against `stage_secs` to judge whether the
    /// source or the write path is the bottleneck.
    pub read_secs: f64,
    /// Wall time of the streaming phase: reading, decoding and writing every
    /// row into the destination (or into this run's staging table). Ends when
    /// the last partition finishes.
    pub stage_secs: f64,
    /// Wall time of the promotion that follows the streaming phase — the
    /// full-refresh `EXCHANGE`/swap, the incremental `MERGE` or insert-select,
    /// the window-scoped delete, and the watermark persist. On a BigQuery
    /// destination this is usually most of the run.
    pub promote_secs: f64,
    pub new_watermark: Option<String>,
    /// Structured warnings, aggregated per `(kind, column)`. Empty on a clean
    /// run. See [`TransferWarning`].
    pub warnings: Vec<TransferWarning>,
}

/// A default `TransferConfig` for tests in other modules (e.g. `sync`), which
/// can't reach this module's private test `cfg()` helper. Full mode, single
/// stream, all optional features off.
#[cfg(test)]
pub(crate) fn default_test_config() -> TransferConfig {
    TransferConfig {
        source_table: Some("t".into()),
        source_query: None,
        dest_table: "t".into(),
        state_key: None,
        mode: SyncMode::Full,
        watermark: None,
        watermark_source_expr: None,
        lookback_seconds: 0,
        seed_watermark: WatermarkSeed::None,
        advance_watermark: true,
        key: vec![],
        create_if_missing: true,
        engine: None,
        order_by: vec![],
        partition_by: None,
        primary_key: vec![],
        merge_prune_partition_by: None,
        merge_prune_key_range: true,
        merge_prune_key_list_max: 0,
        delete_stale_in_window: false,
        allow_full_refresh_shrink: false,
        parallelism: 1,
        batch_rows: 1000,
        batch_bytes: 0,
        insert_bytes: 0,
        max_memory_bytes: 0,
        max_memory_fraction: 0.0,
        partition_column: None,
        partition_source_expr: None,
        read_max_rows_per_sec: None,
        read_idle_timeout_secs: 0,
        chunk_rows: None,
        retry_max_attempts: 1,
        column_transforms: HashMap::new(),
        column_transform_types: HashMap::new(),
        evolve_schema: false,
        state_table_name: "_quickhouse_state".into(),
        staging_suffix: "_quickhouse_tmp".into(),
        application_name: "quickhouse".into(),
        type_overrides: HashMap::new(),
        rename: HashMap::new(),
        include: vec![],
        exclude: vec![],
        not_null: vec![],
        tinyint1_as_bool: true,
        numeric_as_decimal: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_parallelism_is_resolved_from_the_host_not_rejected() {
        // The shipped default is now 0 = "derive", so validate must let it
        // through and normalize must turn it into a real count.
        let mut c = default_test_config();
        c.parallelism = 0;
        c.validate().expect("0 is a request, not an error");
        c.normalize();
        assert_eq!(c.parallelism, crate::host::available_cpus());
        assert!(c.parallelism >= 1, "downstream needs a concrete count");
    }

    #[test]
    fn an_explicit_parallelism_is_left_alone() {
        let mut c = default_test_config();
        c.parallelism = 3;
        c.normalize();
        assert_eq!(c.parallelism, 3);
    }

    #[test]
    fn a_memory_fraction_outside_zero_to_one_is_rejected() {
        let mut c = default_test_config();
        c.max_memory_fraction = 1.5;
        assert!(c
            .validate()
            .unwrap_err()
            .to_string()
            .contains("max_memory_fraction"));
        c.max_memory_fraction = -0.5;
        assert!(c.validate().is_err());
    }

    #[test]
    fn no_memory_fraction_leaves_the_byte_ceiling_as_configured() {
        let mut c = default_test_config();
        c.max_memory_bytes = 12_345;
        c.max_memory_fraction = 0.0;
        c.normalize();
        assert_eq!(c.max_memory_bytes, 12_345);
    }

    #[test]
    fn a_memory_fraction_only_lowers_the_ceiling_when_the_host_is_known() {
        let mut c = default_test_config();
        c.max_memory_bytes = 512 * 1024 * 1024;
        c.max_memory_fraction = 0.25;
        c.normalize();
        match crate::host::memory_limit_bytes() {
            // Where the host reports a limit, the ceiling is that share of it.
            Some(limit) => assert_eq!(c.max_memory_bytes, ((limit as f64) * 0.25) as usize),
            // Where it doesn't, the configured value must survive untouched —
            // a fraction must never silently become "unbounded".
            None => assert_eq!(c.max_memory_bytes, 512 * 1024 * 1024),
        }
    }

    fn cfg(mode: SyncMode, watermark: Option<&str>) -> TransferConfig {
        TransferConfig {
            source_table: Some("t".into()),
            source_query: None,
            dest_table: "t".into(),
            state_key: None,
            mode,
            watermark: watermark.map(str::to_string),
            watermark_source_expr: None,
            lookback_seconds: 0,
            seed_watermark: WatermarkSeed::None,
            advance_watermark: true,
            key: vec!["id".into()],
            create_if_missing: true,
            engine: None,
            order_by: vec![],
            partition_by: None,
            primary_key: vec![],
            merge_prune_partition_by: None,
            merge_prune_key_range: true,
            merge_prune_key_list_max: 0,
            delete_stale_in_window: false,
            allow_full_refresh_shrink: false,
            parallelism: 1,
            batch_rows: 1000,
            batch_bytes: 0,
            insert_bytes: 0,
            max_memory_bytes: 0,
            max_memory_fraction: 0.0,
            partition_column: None,
            partition_source_expr: None,
            read_max_rows_per_sec: None,
            read_idle_timeout_secs: 0,
            chunk_rows: None,
            retry_max_attempts: 1,
            column_transforms: HashMap::new(),
            column_transform_types: HashMap::new(),
            evolve_schema: false,
            state_table_name: "_quickhouse_state".into(),
            staging_suffix: "_quickhouse_tmp".into(),
            application_name: "quickhouse".into(),
            type_overrides: HashMap::new(),
            rename: HashMap::new(),
            include: vec![],
            exclude: vec![],
            not_null: vec![],
            tinyint1_as_bool: true,
            numeric_as_decimal: None,
        }
    }

    #[test]
    fn normalize_clears_watermark_in_full_mode() {
        let mut c = cfg(SyncMode::Full, Some("write_date"));
        c.normalize();
        assert_eq!(c.watermark, None, "watermark is unused in full mode");
    }

    #[test]
    fn normalize_keeps_watermark_in_incremental_mode() {
        let mut c = cfg(SyncMode::Incremental, Some("write_date"));
        c.normalize();
        assert_eq!(c.watermark.as_deref(), Some("write_date"));
    }

    #[test]
    fn validate_rejects_lookback_in_full_mode() {
        let mut c = cfg(SyncMode::Full, None);
        c.lookback_seconds = 60;
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains("lookback_seconds"), "got: {err}");
        assert!(err.contains("incremental"), "got: {err}");
    }

    #[test]
    fn validate_rejects_lookback_without_key_or_order_by() {
        let mut c = cfg(SyncMode::Incremental, Some("write_date"));
        c.key = vec![];
        c.order_by = vec![];
        c.lookback_seconds = 60;
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains("lookback_seconds"), "got: {err}");
        assert!(err.contains("key or order_by"), "got: {err}");
    }

    #[test]
    fn validate_accepts_lookback_with_order_by_but_no_key() {
        let mut c = cfg(SyncMode::Incremental, Some("write_date"));
        c.key = vec![];
        c.order_by = vec!["id".into()];
        c.lookback_seconds = 60;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_accepts_lookback_with_key_in_incremental_mode() {
        let mut c = cfg(SyncMode::Incremental, Some("write_date"));
        c.lookback_seconds = 60;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_read_rate() {
        let mut c = cfg(SyncMode::Full, None);
        c.read_max_rows_per_sec = Some(0);
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains("read_max_rows_per_sec"), "got: {err}");
    }

    #[test]
    fn validate_accepts_none_or_positive_read_rate() {
        let mut c = cfg(SyncMode::Full, None);
        c.read_max_rows_per_sec = None;
        assert!(c.validate().is_ok());
        c.read_max_rows_per_sec = Some(10_000);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn effective_state_key_prefers_override_then_table_then_query() {
        let mut c = cfg(SyncMode::Incremental, Some("write_date"));
        // Default: source_table.
        assert_eq!(c.effective_state_key(), "t");
        // source_query wins over nothing but loses to source_table.
        c.source_table = None;
        c.source_query = Some("SELECT * FROM t WHERE x".into());
        assert_eq!(c.effective_state_key(), "SELECT * FROM t WHERE x");
        // explicit override wins over both — and is stable across query edits.
        c.state_key = Some("orders:write_date".into());
        assert_eq!(c.effective_state_key(), "orders:write_date");
        c.source_query = Some("SELECT * FROM t WHERE y /* edited */".into());
        assert_eq!(c.effective_state_key(), "orders:write_date");
    }

    #[test]
    fn validate_rejects_seed_and_freeze_outside_incremental() {
        let mut c = cfg(SyncMode::Full, None);
        c.seed_watermark = WatermarkSeed::CurrentMax;
        assert!(c
            .validate()
            .unwrap_err()
            .to_string()
            .contains("seed_watermark"));
        c.seed_watermark = WatermarkSeed::None;
        c.advance_watermark = false;
        assert!(c
            .validate()
            .unwrap_err()
            .to_string()
            .contains("advance_watermark"));
    }

    #[test]
    fn validate_accepts_seed_and_freeze_in_incremental() {
        let mut c = cfg(SyncMode::Incremental, Some("write_date"));
        c.seed_watermark = WatermarkSeed::Value("2026-01-01".into());
        c.advance_watermark = false;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn normalize_clears_seed_in_full_mode() {
        let mut c = cfg(SyncMode::Full, Some("write_date"));
        c.seed_watermark = WatermarkSeed::CurrentMax;
        c.normalize();
        assert_eq!(c.seed_watermark, WatermarkSeed::None);
    }

    /// A config shaped the way `from_pandas` builds one: no source table, and
    /// (for incremental) a key but no watermark.
    fn frame_cfg(mode: SyncMode) -> TransferConfig {
        let mut c = cfg(mode, None);
        c.source_table = None;
        c
    }

    #[test]
    fn frame_incremental_needs_no_watermark_but_does_need_a_key() {
        // The whole point: the frame IS the delta, so there is no window to
        // resume and nothing for a watermark to mean.
        frame_cfg(SyncMode::Incremental).validate_frame().unwrap();
        frame_cfg(SyncMode::Append).validate_frame().unwrap();
        frame_cfg(SyncMode::Full).validate_frame().unwrap();

        // ...but `key` takes over the watermark's old job of making the
        // relaxation safe, so a keyless incremental is refused here rather than
        // much later, in DDL generation, with a vaguer message.
        let mut c = frame_cfg(SyncMode::Incremental);
        c.key = vec![];
        let err = c.validate_frame().unwrap_err().to_string();
        assert!(err.contains("upserts on key= alone"), "{err}");
        // A watermark is still accepted, as the dedup ordering column.
        c.watermark = Some("updated_at".into());
        c.validate_frame().unwrap();
    }

    #[test]
    fn frame_relaxations_do_not_leak_to_the_other_sources() {
        // The regression guard that matters: relaxing two rules for frames must
        // not weaken them for the seven sources that had them before.
        let mut db = cfg(SyncMode::Incremental, None);
        db.key = vec![];
        let err = db.validate().unwrap_err().to_string();
        assert!(err.contains("watermark column is required"), "{err}");

        let db = cfg(SyncMode::Append, Some("updated_at"));
        let err = db.validate().unwrap_err().to_string();
        assert!(err.contains("append mode"), "{err}");

        let mut api = cfg(SyncMode::Incremental, None);
        api.source_table = None;
        let err = api.validate_api().unwrap_err().to_string();
        assert!(err.contains("watermark column is required"), "{err}");
    }

    #[test]
    fn frame_rejects_the_knobs_that_cannot_apply_to_it() {
        // Each of these would otherwise be silently ignored — the failure mode
        // this crate works hardest to avoid.
        type Knob = (&'static str, Box<dyn Fn(&mut TransferConfig)>);
        let cases: Vec<Knob> = vec![
            (
                "source_table",
                Box::new(|c: &mut TransferConfig| c.source_table = Some("t".into())),
            ),
            (
                "source_query",
                Box::new(|c: &mut TransferConfig| c.source_query = Some("SELECT 1".into())),
            ),
            (
                "watermark_source_expr",
                Box::new(|c: &mut TransferConfig| c.watermark_source_expr = Some("x".into())),
            ),
            (
                "partition_source_expr",
                Box::new(|c: &mut TransferConfig| c.partition_source_expr = Some("x".into())),
            ),
            (
                "partition_column",
                Box::new(|c: &mut TransferConfig| c.partition_column = Some("id".into())),
            ),
            (
                "read_max_rows_per_sec",
                Box::new(|c: &mut TransferConfig| c.read_max_rows_per_sec = Some(100)),
            ),
            (
                "read_idle_timeout_secs",
                Box::new(|c: &mut TransferConfig| c.read_idle_timeout_secs = 30),
            ),
            (
                "seed_watermark",
                Box::new(|c: &mut TransferConfig| {
                    c.seed_watermark = WatermarkSeed::Value("x".into())
                }),
            ),
            (
                "retry_max_attempts",
                Box::new(|c: &mut TransferConfig| c.retry_max_attempts = 3),
            ),
            (
                "chunk_rows",
                Box::new(|c: &mut TransferConfig| c.chunk_rows = Some(1000)),
            ),
            (
                "lookback_seconds",
                Box::new(|c: &mut TransferConfig| c.lookback_seconds = 60),
            ),
            (
                "column_transforms",
                Box::new(|c: &mut TransferConfig| {
                    c.column_transforms.insert("a".into(), "lower(a)".into());
                }),
            ),
        ];
        for (name, apply) in cases {
            let mut c = frame_cfg(SyncMode::Incremental);
            apply(&mut c);
            let err = match c.validate_frame() {
                Err(e) => e.to_string(),
                Ok(()) => panic!("{name} should be rejected for a frame source, but passed"),
            };
            // The message has to name the knob, or the caller is left guessing
            // which of their arguments was the problem.
            assert!(
                err.contains(name),
                "{name}: message does not name it: {err}"
            );
        }
    }

    #[test]
    fn keyset_column_prefers_partition_column_then_key() {
        let mut c = cfg(SyncMode::Incremental, Some("write_date"));
        assert_eq!(c.keyset_column().as_deref(), Some("id")); // default key
        c.partition_column = Some("pk".into());
        assert_eq!(c.keyset_column().as_deref(), Some("pk"));
        c.partition_column = None;
        c.key = vec![];
        assert_eq!(c.keyset_column(), None);
    }

    #[test]
    fn validate_api_allows_no_table_and_rejects_db_only_knobs() {
        // No source_table/query is fine under the API rules.
        let mut c = cfg(SyncMode::Full, None);
        c.source_table = None;
        c.source_query = None;
        assert!(c.validate_api().is_ok());
        assert!(
            c.validate().is_err(),
            "DB validation still requires a table/query"
        );
        // API rejects column_transforms / chunk_rows / lookback.
        let mut c = cfg(SyncMode::Incremental, Some("ts"));
        c.column_transforms = HashMap::from([("x".to_string(), "y".to_string())]);
        assert!(c
            .validate_api()
            .unwrap_err()
            .to_string()
            .contains("column_transforms"));
        let mut c = cfg(SyncMode::Incremental, Some("ts"));
        c.chunk_rows = Some(1000);
        assert!(c
            .validate_api()
            .unwrap_err()
            .to_string()
            .contains("chunk_rows"));
    }

    #[test]
    fn validate_chunk_rows_rules() {
        // Zero rejected.
        let mut c = cfg(SyncMode::Incremental, Some("write_date"));
        c.chunk_rows = Some(0);
        assert!(c.validate().unwrap_err().to_string().contains("chunk_rows"));
        // Full mode rejected.
        let mut c = cfg(SyncMode::Full, None);
        c.chunk_rows = Some(1000);
        assert!(c
            .validate()
            .unwrap_err()
            .to_string()
            .contains("incremental"));
        // No keyset column rejected.
        let mut c = cfg(SyncMode::Incremental, Some("write_date"));
        c.chunk_rows = Some(1000);
        c.key = vec![];
        c.partition_column = None;
        assert!(c.validate().unwrap_err().to_string().contains("keyset"));
        // Valid: incremental + a key.
        let mut c = cfg(SyncMode::Incremental, Some("write_date"));
        c.chunk_rows = Some(1000);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn append_mode_is_api_only_and_needs_watermark() {
        // Append is rejected for a DB source, allowed for an API source.
        let c = cfg(SyncMode::Append, Some("ts"));
        assert!(c
            .validate()
            .unwrap_err()
            .to_string()
            .contains("append mode"));
        assert!(c.validate_api().is_ok());
        // Append without a watermark (the resume cursor) is rejected.
        let c2 = cfg(SyncMode::Append, None);
        assert!(c2
            .validate_api()
            .unwrap_err()
            .to_string()
            .contains("watermark"));
        // seed_watermark / advance_watermark are allowed in append mode.
        let mut c3 = cfg(SyncMode::Append, Some("ts"));
        c3.seed_watermark = WatermarkSeed::CurrentMax;
        c3.advance_watermark = false;
        assert!(c3.validate_api().is_ok());
    }

    #[test]
    fn delete_stale_requires_prune_and_incremental() {
        // Incremental without a prune column -> rejected (would nuke history).
        let mut c = cfg(SyncMode::Incremental, Some("write_date"));
        c.delete_stale_in_window = true;
        assert!(c
            .validate()
            .unwrap_err()
            .to_string()
            .contains("merge_prune_partition_by"));
        // With an immutable prune column -> ok.
        c.merge_prune_partition_by = Some("create_date".into());
        assert!(c.validate().is_ok());
        // Full mode -> rejected.
        let mut c2 = cfg(SyncMode::Full, None);
        c2.delete_stale_in_window = true;
        c2.merge_prune_partition_by = Some("create_date".into());
        assert!(c2
            .validate()
            .unwrap_err()
            .to_string()
            .contains("incremental"));
    }
}
