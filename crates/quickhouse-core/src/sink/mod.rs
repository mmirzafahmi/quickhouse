//! Destination sinks. The [`Sink`] trait is the destination-agnostic seam:
//! `sync.rs` builds one from a [`crate::config::DestinationConfig`] via
//! [`build_sink`] and drives it (DDL, inserts, the full-refresh atomic swap,
//! incremental watermark state) without knowing the concrete engine. Required
//! methods every destination must implement; the *capability* methods
//! (staging-merge, chunk-resume) have safe defaults, so a new destination
//! implements only what it supports.

pub mod bigquery;
mod bigquery_proto;
pub mod clickhouse;

pub use bigquery::BigQuerySink;
pub use clickhouse::ClickHouseSink;

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;

use crate::config::{DestinationConfig, TransferConfig};
use crate::error::{EtlError, Result};
use crate::types::ColumnType;

/// Max total attempts for one insert (1 initial + retries). Shared by every
/// sink so both destinations retry transient failures identically.
pub(crate) const MAX_INSERT_ATTEMPTS: u32 = 4;
/// Base backoff; attempt N waits `BASE * 2^(N-1)` (0.25s, 0.5s, 1s, ...).
pub(crate) const BACKOFF_BASE_MS: u64 = 250;

/// Exponential backoff for retry attempt `attempt` (1-based).
pub(crate) fn backoff_delay(attempt: u32) -> std::time::Duration {
    let mult = 1u64 << (attempt.saturating_sub(1)).min(6); // cap the shift
    std::time::Duration::from_millis(BACKOFF_BASE_MS.saturating_mul(mult))
}

/// The columns in `desired` whose names aren't already present in `existing`
/// — the set schema-evolution must `ADD COLUMN`. `case_insensitive` compares
/// lowercased (BigQuery treats column names case-insensitively; ClickHouse is
/// case-sensitive). Preserves `desired` order.
pub(crate) fn missing_columns<'a>(
    existing: &[String],
    desired: &'a [ColumnType],
    case_insensitive: bool,
) -> Vec<&'a ColumnType> {
    let norm = |s: &str| {
        if case_insensitive {
            s.to_ascii_lowercase()
        } else {
            s.to_string()
        }
    };
    let have: std::collections::HashSet<String> = existing.iter().map(|s| norm(s)).collect();
    desired
        .iter()
        .filter(|c| !have.contains(&norm(&c.name)))
        .collect()
}

/// Outcome of a single send attempt, telling the caller whether to retry.
/// Shared classification: transport failures and 5xx/429 are transient
/// (worth retrying with backoff); deterministic errors (4xx: bad request,
/// auth, schema mismatch) are permanent and returned immediately.
pub(crate) enum SendError {
    Transient(EtlError),
    Permanent(EtlError),
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendError::Transient(e) | SendError::Permanent(e) => write!(f, "{e}"),
        }
    }
}

/// A write destination, driven by `sync.rs` without knowing the concrete
/// engine. Build one with [`build_sink`]. The methods above the capability
/// section are required; the capability methods below have safe defaults so a
/// destination implements only what it supports (e.g. ClickHouse keeps the
/// default `merge_into`, since it dedups via `ReplacingMergeTree` rather than a
/// staged MERGE). Object-safe (`Arc<dyn Sink>`) via `#[async_trait]`.
#[async_trait]
pub trait Sink: Send + Sync {
    async fn table_exists(&self, table: &str) -> Result<bool>;

    /// Create `table` (auto-generated DDL/schema from `columns` + `cfg`'s
    /// key/order_by/partition_by/engine — interpreted per destination).
    async fn create_table(
        &self,
        table: &str,
        columns: &[ColumnType],
        cfg: &TransferConfig,
    ) -> Result<()>;

    /// Insert a group of Arrow batches into `table`. Returns an approximate
    /// wire-bytes-sent count (an accounting detail, not exact for either sink).
    async fn insert_batches(
        &self,
        table: &str,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<u64>;

    /// Atomically replace `dest`'s contents with `staging`'s (both must exist).
    async fn atomic_swap(&self, dest: &str, staging: &str, columns: &[ColumnType]) -> Result<()>;

    /// Current committed row count of `table`, or `None` if it doesn't exist.
    /// Best-effort/diagnostic — callers use it for warnings, not correctness.
    async fn current_row_count(&self, table: &str) -> Result<Option<u64>>;

    async fn drop_table(&self, table: &str) -> Result<()>;

    /// Create the internal watermark-tracking table (named `state_table`).
    async fn ensure_state_table(&self, state_table: &str) -> Result<()>;

    /// Read the last persisted watermark for this `(source, dest_table)` pair;
    /// `None` if this is the first incremental run.
    async fn read_last_watermark(&self, cfg: &TransferConfig) -> Result<Option<String>>;

    /// Persist a new watermark after a successful incremental run.
    async fn persist_watermark(
        &self,
        cfg: &TransferConfig,
        watermark: &str,
        rows: u64,
    ) -> Result<()>;

    /// `ALTER TABLE ADD COLUMN` (as Nullable) for every `columns` entry the
    /// existing destination table lacks; returns the names added. ADD-only.
    async fn add_missing_columns(
        &self,
        table: &str,
        columns: &[ColumnType],
        cfg: &TransferConfig,
    ) -> Result<Vec<String>>;

    /// Which destination engine this writes to — threaded into `transform::plan`
    /// for destination-aware type promotion. Pure/no I/O.
    fn dest_kind(&self) -> crate::config::DestKind;

    // ---- capability methods (safe defaults; override where supported) ----

    /// Whether incremental writes must be *staged then merged* rather than
    /// inserted directly. Default `false`: direct inserts (e.g. ClickHouse
    /// dedups lazily via `ReplacingMergeTree`). A destination with no
    /// engine-level dedup overrides this to `true` and implements
    /// [`Self::merge_into`]. Pure/no I/O.
    fn requires_staging_for_incremental(&self) -> bool {
        false
    }

    /// Whether a *full-refresh* swap references the destination's columns by
    /// name (so a schema drift must be evolved before the swap). Default
    /// `false` (e.g. ClickHouse's `EXCHANGE TABLES` absorbs drift). Pure.
    fn full_refresh_references_dest_columns(&self) -> bool {
        false
    }

    /// In-progress chunk-resume marker `(cursor, upper)`, or `None`. Default
    /// `None` — a destination without chunked-read support has nothing to resume.
    async fn read_chunk_state(&self, _cfg: &TransferConfig) -> Result<Option<(String, String)>> {
        Ok(None)
    }

    /// Persist a per-chunk resume marker. Default: unsupported — only a
    /// destination that actually supports chunked resumable reads is ever asked.
    async fn persist_chunk_cursor(
        &self,
        _cfg: &TransferConfig,
        _committed: Option<&str>,
        _cursor: &str,
        _upper: &str,
        _rows: u64,
    ) -> Result<()> {
        Err(EtlError::internal(
            "persist_chunk_cursor: this destination does not support chunked resumable reads",
        ))
    }

    /// Upsert `staging`'s rows into `dest`, matched on `key`. Default:
    /// unsupported — only a destination reporting
    /// [`Self::requires_staging_for_incremental`] performs a staged MERGE.
    async fn merge_into(
        &self,
        _dest: &str,
        _staging: &str,
        _key: &[String],
        _columns: &[ColumnType],
        _prune_partition: Option<&str>,
        _delete_stale: bool,
    ) -> Result<()> {
        Err(EtlError::internal(
            "merge_into: this destination does not use staged-merge incremental writes",
        ))
    }
}

/// Build the concrete sink for `dest`, boxed behind the [`Sink`] trait.
pub async fn build_sink(dest: DestinationConfig) -> Result<Arc<dyn Sink>> {
    Ok(match dest {
        DestinationConfig::ClickHouse(cfg) => Arc::new(ClickHouseSink::new(cfg)?),
        DestinationConfig::BigQuery(cfg) => Arc::new(BigQuerySink::new(cfg).await?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    fn col(name: &str) -> ColumnType {
        ColumnType {
            name: name.into(),
            type_id: 0,
            nullable: true,
            arrow: DataType::Int64,
            clickhouse_inner: "Int64".into(),
            arbitrary_precision_decimal: false,
        }
    }

    #[test]
    fn missing_columns_honors_case_sensitivity_and_order() {
        let existing = vec!["id".to_string(), "Name".to_string()];
        let desired = vec![col("id"), col("name"), col("amount")];
        // Case-sensitive (ClickHouse): "name" != "Name", so both are missing,
        // in `desired` order.
        let cs: Vec<&str> = missing_columns(&existing, &desired, false)
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(cs, vec!["name", "amount"]);
        // Case-insensitive (BigQuery): "name" matches "Name", only amount left.
        let ci: Vec<&str> = missing_columns(&existing, &desired, true)
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(ci, vec!["amount"]);
    }

    #[test]
    fn backoff_delay_doubles_and_caps() {
        assert_eq!(backoff_delay(1), std::time::Duration::from_millis(250));
        assert_eq!(backoff_delay(2), std::time::Duration::from_millis(500));
        assert_eq!(backoff_delay(3), std::time::Duration::from_millis(1000));
        // Shift is capped, so very high attempts don't overflow/explode.
        assert_eq!(backoff_delay(100), backoff_delay(7));
    }
}
