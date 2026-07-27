//! Configuration structs for a transfer. These are populated by the Python
//! binding (or constructed directly in Rust tests) and drive [`crate::sync`].

use std::collections::HashMap;

/// Where to read from.
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    /// libpq-style connection string, e.g. `postgresql://user:pw@host:5432/db`.
    pub dsn: String,
    /// Statement timeout hint (seconds) applied per connection; 0 = server default.
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
    /// Statement timeout hint (seconds) applied per connection; 0 = server default.
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

/// Which engine/API to read from.
#[derive(Debug, Clone)]
pub enum SourceConfig {
    Postgres(PostgresConfig),
    MySql(MySqlConfig),
    BigQuery(BigQueryConfig),
    CleverTap(CleverTapConfig),
    AppsFlyer(AppsFlyerConfig),
    HttpApi(HttpApiConfig),
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
            SourceConfig::CleverTap(_) => "clevertap",
            SourceConfig::AppsFlyer(_) => "appsflyer",
            SourceConfig::HttpApi(_) => "http",
        }
    }

    /// Whether this is an HTTP API source (CleverTap/AppsFlyer) — those take a
    /// declared schema + date window and bypass the DB schema-resolution /
    /// partition machinery, writing only to BigQuery.
    pub fn is_api(&self) -> bool {
        matches!(
            self,
            SourceConfig::CleverTap(_) | SourceConfig::AppsFlyer(_) | SourceConfig::HttpApi(_)
        )
    }

    /// A stable identity string for an API source's incremental cursor in
    /// `_quickhouse_state` (API sources have no `source_table`). `None` for a
    /// non-API source.
    pub fn api_state_identity(&self) -> Option<String> {
        match self {
            SourceConfig::CleverTap(c) => Some(format!("clevertap:{}", c.event_name)),
            SourceConfig::AppsFlyer(a) => Some(format!("appsflyer:{}:{}", a.app_id, a.report_type)),
            SourceConfig::HttpApi(h) => Some(
                h.state_id
                    .clone()
                    .unwrap_or_else(|| format!("http:{}", h.url)),
            ),
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
    pub watermark: Option<String>,
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
    pub merge_prune_partition_by: Option<String>,
    /// BigQuery-destination incremental only: additionally `DELETE` destination
    /// rows *inside the merged window* that are absent from the source pull
    /// (`WHEN NOT MATCHED BY SOURCE`), giving "replace this window" semantics
    /// and making a NULL merge key self-correct (net replace) instead of
    /// duplicating on re-runs.
    ///
    /// **Requires `merge_prune_partition_by`** (a hard config error otherwise):
    /// the DELETE is scoped to that immutable column's `[MIN, MAX]` range in the
    /// staging batch — the SAME bound the prune uses. Without a window bound a
    /// `WHEN NOT MATCHED BY SOURCE` clause would delete the ENTIRE destination
    /// history outside the delta, so it is never allowed unscoped. `false`
    /// (default) keeps the insert-or-update-only merge.
    pub delete_stale_in_window: bool,

    // ---- parallelism / batching ----
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
    /// Opt-in schema evolution: when the source has a column the existing
    /// destination table lacks, `ALTER TABLE ADD COLUMN` (as Nullable) instead
    /// of hard-erroring. `false` (default) preserves today's behavior. Never
    /// drops or retypes a column.
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
        if self.mode == SyncMode::Full {
            self.watermark = None;
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

    /// Validation for a DB source (Postgres/MySQL/BigQuery) — byte-identical to
    /// the original `validate`.
    pub fn validate(&self) -> crate::error::Result<()> {
        self.validate_impl(false)
    }

    /// Validation for an HTTP API source (CleverTap/AppsFlyer): no
    /// `source_table`/`source_query` is expected (the "what to read" lives on
    /// the source descriptor), and a few DB-only knobs are rejected.
    pub fn validate_api(&self) -> crate::error::Result<()> {
        self.validate_impl(true)
    }

    fn validate_impl(&self, is_api: bool) -> crate::error::Result<()> {
        use crate::error::EtlError;
        if !is_api && self.source_table.is_none() && self.source_query.is_none() {
            return Err(EtlError::config(
                "either source_table or source_query must be set",
            ));
        }
        if is_api {
            if !self.column_transforms.is_empty() {
                return Err(EtlError::config(
                    "column_transforms is not supported for an API source (declare the columns instead)",
                ));
            }
            if self.chunk_rows.is_some() {
                return Err(EtlError::config(
                    "chunk_rows (keyset resumable reads) is not supported for an API source",
                ));
            }
            if self.lookback_seconds > 0 {
                return Err(EtlError::config(
                    "lookback_seconds is not supported for an API source",
                ));
            }
        }
        if matches!(self.mode, SyncMode::Incremental | SyncMode::Append) && self.watermark.is_none()
        {
            return Err(EtlError::config(
                "watermark column is required for incremental and append mode (it drives the \
                 resumable date window)",
            ));
        }
        if self.lookback_seconds > 0 && self.mode != SyncMode::Incremental {
            return Err(EtlError::config(
                "lookback_seconds only applies to incremental mode",
            ));
        }
        // Append is a bronze-landing write for API sources only.
        if self.mode == SyncMode::Append && !is_api {
            return Err(EtlError::config(
                "append mode is currently supported only for HTTP API sources (CleverTap/AppsFlyer)",
            ));
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
        if self.parallelism == 0 {
            return Err(EtlError::config("parallelism must be >= 1"));
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
}

/// Summary returned to the caller after a transfer.
#[derive(Debug, Clone, Default)]
pub struct TransferResult {
    pub rows_read: u64,
    pub rows_written: u64,
    pub bytes_written: u64,
    pub duration_secs: f64,
    pub new_watermark: Option<String>,
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
        delete_stale_in_window: false,
        parallelism: 1,
        batch_rows: 1000,
        batch_bytes: 0,
        max_memory_bytes: 0,
        partition_column: None,
        read_max_rows_per_sec: None,
        chunk_rows: None,
        retry_max_attempts: 1,
        column_transforms: HashMap::new(),
        evolve_schema: false,
        state_table_name: "_quickhouse_state".into(),
        staging_suffix: "_quickhouse_tmp".into(),
        application_name: "quickhouse".into(),
        type_overrides: HashMap::new(),
        rename: HashMap::new(),
        include: vec![],
        exclude: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(mode: SyncMode, watermark: Option<&str>) -> TransferConfig {
        TransferConfig {
            source_table: Some("t".into()),
            source_query: None,
            dest_table: "t".into(),
            state_key: None,
            mode,
            watermark: watermark.map(str::to_string),
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
            delete_stale_in_window: false,
            parallelism: 1,
            batch_rows: 1000,
            batch_bytes: 0,
            max_memory_bytes: 0,
            partition_column: None,
            read_max_rows_per_sec: None,
            chunk_rows: None,
            retry_max_attempts: 1,
            column_transforms: HashMap::new(),
            evolve_schema: false,
            state_table_name: "_quickhouse_state".into(),
            staging_suffix: "_quickhouse_tmp".into(),
            application_name: "quickhouse".into(),
            type_overrides: HashMap::new(),
            rename: HashMap::new(),
            include: vec![],
            exclude: vec![],
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
