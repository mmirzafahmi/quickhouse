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
}

/// Which database engine to read from.
#[derive(Debug, Clone)]
pub enum SourceConfig {
    Postgres(PostgresConfig),
    MySql(MySqlConfig),
    BigQuery(BigQueryConfig),
}

impl SourceConfig {
    /// A short label identifying the source, used to persist watermark state
    /// under a source-qualified key (so the same table name in different
    /// engines doesn't collide).
    pub fn kind(&self) -> &'static str {
        match self {
            SourceConfig::Postgres(_) => "postgres",
            SourceConfig::MySql(_) => "mysql",
            SourceConfig::BigQuery(_) => "bigquery",
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Gzip,
    Zstd,
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
}

/// Full-refresh reloads everything; Incremental appends rows past a watermark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    Full,
    Incremental,
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
            SyncMode::Full => "MergeTree".to_string(),
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
        }
    }

    pub fn validate(&self) -> crate::error::Result<()> {
        use crate::error::EtlError;
        if self.source_table.is_none() && self.source_query.is_none() {
            return Err(EtlError::config(
                "either source_table or source_query must be set",
            ));
        }
        if self.mode == SyncMode::Incremental && self.watermark.is_none() {
            return Err(EtlError::config(
                "watermark column is required for incremental mode",
            ));
        }
        if self.lookback_seconds > 0 && self.mode != SyncMode::Incremental {
            return Err(EtlError::config(
                "lookback_seconds only applies to incremental mode",
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

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(mode: SyncMode, watermark: Option<&str>) -> TransferConfig {
        TransferConfig {
            source_table: Some("t".into()),
            source_query: None,
            dest_table: "t".into(),
            mode,
            watermark: watermark.map(str::to_string),
            lookback_seconds: 0,
            key: vec!["id".into()],
            create_if_missing: true,
            engine: None,
            order_by: vec![],
            partition_by: None,
            primary_key: vec![],
            parallelism: 1,
            batch_rows: 1000,
            batch_bytes: 0,
            max_memory_bytes: 0,
            partition_column: None,
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
}
