//! Destination sinks. [`Sink`] mirrors [`crate::source::Source`]: `sync.rs`
//! builds one from a [`crate::config::DestinationConfig`] and calls its
//! (destination-agnostic) methods for DDL, inserts, the full-refresh atomic
//! swap, and incremental watermark state — the orchestration in `sync.rs`
//! never needs to know which concrete destination it's talking to.

pub mod bigquery;
mod bigquery_proto;
pub mod clickhouse;

pub use bigquery::BigQuerySink;
pub use clickhouse::ClickHouseSink;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

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
    desired.iter().filter(|c| !have.contains(&norm(&c.name))).collect()
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

/// Which destination engine to write to. Every method delegates to whichever
/// concrete sink this instance wraps.
#[derive(Clone)]
pub enum Sink {
    ClickHouse(ClickHouseSink),
    BigQuery(BigQuerySink),
}

impl Sink {
    pub async fn new(dest: DestinationConfig) -> Result<Self> {
        match dest {
            DestinationConfig::ClickHouse(cfg) => Ok(Sink::ClickHouse(ClickHouseSink::new(cfg)?)),
            DestinationConfig::BigQuery(cfg) => Ok(Sink::BigQuery(BigQuerySink::new(cfg).await?)),
        }
    }

    pub async fn table_exists(&self, table: &str) -> Result<bool> {
        match self {
            Sink::ClickHouse(s) => s.table_exists(table).await,
            Sink::BigQuery(s) => s.table_exists(table).await,
        }
    }

    /// Create `table` (auto-generated DDL/schema from `columns` + `cfg`'s
    /// key/order_by/partition_by/engine — interpreted per destination, see
    /// `TransferConfig`'s field docs).
    pub async fn create_table(
        &self,
        table: &str,
        columns: &[ColumnType],
        cfg: &TransferConfig,
    ) -> Result<()> {
        match self {
            Sink::ClickHouse(s) => s.create_table(table, columns, cfg).await,
            Sink::BigQuery(s) => s.create_table(table, columns, cfg).await,
        }
    }

    /// Insert a group of Arrow batches into `table`. Returns an approximate
    /// wire-bytes-sent count (post-compression for ClickHouse; JSON payload
    /// size for BigQuery — an accounting detail, not exact for either).
    pub async fn insert_batches(
        &self,
        table: &str,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<u64> {
        match self {
            Sink::ClickHouse(s) => s.insert_batches(table, schema, batches).await,
            Sink::BigQuery(s) => s.insert_batches(table, schema, batches).await,
        }
    }

    /// Atomically replace `dest`'s contents with `staging`'s (both must
    /// exist): ClickHouse's `EXCHANGE TABLES`, or BigQuery's `TRUNCATE` +
    /// `INSERT ... SELECT` transaction (needs `columns` to build the
    /// `INSERT`/`SELECT` column list; ClickHouse's swap needs no column list).
    pub async fn atomic_swap(&self, dest: &str, staging: &str, columns: &[ColumnType]) -> Result<()> {
        match self {
            Sink::ClickHouse(s) => s.exchange_tables(dest, staging).await,
            Sink::BigQuery(s) => s.atomic_swap(dest, staging, columns).await,
        }
    }

    /// Current committed row count of `table`, or `None` if it doesn't exist.
    /// Best-effort/diagnostic (BigQuery reads free table metadata; the count may
    /// lag a streaming buffer) — callers use it for warnings, not correctness.
    pub async fn current_row_count(&self, table: &str) -> Result<Option<u64>> {
        match self {
            Sink::ClickHouse(s) => s.current_row_count(table).await,
            Sink::BigQuery(s) => s.current_row_count(table).await,
        }
    }

    pub async fn drop_table(&self, table: &str) -> Result<()> {
        match self {
            Sink::ClickHouse(s) => s.drop_table(table).await,
            Sink::BigQuery(s) => s.drop_table(table).await,
        }
    }

    /// Create the internal watermark-tracking table if it doesn't exist yet.
    pub async fn ensure_state_table(&self) -> Result<()> {
        match self {
            Sink::ClickHouse(s) => s.ensure_state_table().await,
            Sink::BigQuery(s) => s.ensure_state_table().await,
        }
    }

    /// Read the last persisted watermark for this `(source, dest_table)`
    /// pair; `None` if this is the first incremental run.
    pub async fn read_last_watermark(&self, cfg: &TransferConfig) -> Result<Option<String>> {
        match self {
            Sink::ClickHouse(s) => s.read_last_watermark(cfg).await,
            Sink::BigQuery(s) => s.read_last_watermark(cfg).await,
        }
    }

    /// Persist a new watermark after a successful incremental run.
    pub async fn persist_watermark(&self, cfg: &TransferConfig, watermark: &str, rows: u64) -> Result<()> {
        match self {
            Sink::ClickHouse(s) => s.persist_watermark(cfg, watermark, rows).await,
            Sink::BigQuery(s) => s.persist_watermark(cfg, watermark, rows).await,
        }
    }

    /// Whether this destination needs incremental writes staged (then
    /// merged) rather than inserted directly into the destination table.
    /// ClickHouse dedupes lazily at merge time via `ReplacingMergeTree`, so
    /// direct inserts are fine; BigQuery has no engine-level dedup, so an
    /// updated source row (same key, newer watermark) would otherwise land
    /// as a duplicate row — [`Self::merge_into`] is required there instead.
    /// Pure/no I/O, so callers can check it without an `await`.
    pub fn requires_staging_for_incremental(&self) -> bool {
        matches!(self, Sink::BigQuery(_))
    }

    /// Which destination engine this sink writes to — threaded into
    /// `transform::plan` for destination-aware type promotion. Pure/no I/O.
    pub fn dest_kind(&self) -> crate::config::DestKind {
        match self {
            Sink::ClickHouse(_) => crate::config::DestKind::ClickHouse,
            Sink::BigQuery(_) => crate::config::DestKind::BigQuery,
        }
    }

    /// Whether a *full-refresh* run's swap references the destination table's
    /// columns by name (so a schema drift must be evolved before the swap).
    /// BigQuery swaps via `INSERT ... SELECT` naming each column → yes;
    /// ClickHouse swaps via `exchange_tables` (the freshly-built staging table
    /// becomes the destination) → no, drift is absorbed transparently. Pure.
    pub fn full_refresh_references_dest_columns(&self) -> bool {
        matches!(self, Sink::BigQuery(_))
    }

    /// `ALTER TABLE ADD COLUMN` (as Nullable) for every `columns` entry the
    /// existing destination table lacks; returns the names added. ADD-only —
    /// never drops or retypes. Opt-in via `TransferConfig::evolve_schema`.
    pub async fn add_missing_columns(
        &self,
        table: &str,
        columns: &[ColumnType],
        cfg: &TransferConfig,
    ) -> Result<Vec<String>> {
        match self {
            Sink::ClickHouse(s) => s.add_missing_columns(table, columns, cfg).await,
            Sink::BigQuery(s) => s.add_missing_columns(table, columns, cfg).await,
        }
    }

    /// Read an in-progress chunk-resume marker `(cursor, upper)`, or `None`.
    /// ClickHouse-only (chunked reads are gated to a ClickHouse dest); the
    /// BigQuery arm always returns `None`.
    pub async fn read_chunk_state(&self, cfg: &TransferConfig) -> Result<Option<(String, String)>> {
        match self {
            Sink::ClickHouse(s) => s.read_chunk_state(cfg).await,
            Sink::BigQuery(_) => Ok(None),
        }
    }

    /// Persist a per-chunk resume marker. ClickHouse-only; calling it on a
    /// BigQuery sink is a logic bug (chunked reads never run there).
    pub async fn persist_chunk_cursor(
        &self,
        cfg: &TransferConfig,
        committed: Option<&str>,
        cursor: &str,
        upper: &str,
        rows: u64,
    ) -> Result<()> {
        match self {
            Sink::ClickHouse(s) => s.persist_chunk_cursor(cfg, committed, cursor, upper, rows).await,
            Sink::BigQuery(_) => Err(EtlError::internal(
                "persist_chunk_cursor on a BigQuery sink — chunked resumable reads are ClickHouse-only",
            )),
        }
    }

    /// Upsert `staging`'s rows into `dest`, matched on `key`. Only meaningful
    /// (and only ever called) when [`Self::requires_staging_for_incremental`]
    /// is `true`; calling it on a destination that doesn't need staging is a
    /// logic bug, not a real config error, so the ClickHouse arm returns
    /// [`EtlError::internal`] rather than silently doing nothing or panicking.
    pub async fn merge_into(
        &self,
        dest: &str,
        staging: &str,
        key: &[String],
        columns: &[ColumnType],
        prune_partition: Option<&str>,
        delete_stale: bool,
    ) -> Result<()> {
        match self {
            Sink::ClickHouse(_) => Err(EtlError::internal(
                "merge_into called on a ClickHouse sink — unreachable, since ClickHouse never \
                 reports requires_staging_for_incremental()",
            )),
            Sink::BigQuery(s) => s.merge_into(dest, staging, key, columns, prune_partition, delete_stale).await,
        }
    }
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
