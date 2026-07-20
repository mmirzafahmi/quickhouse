//! Destination sinks. [`Sink`] mirrors [`crate::source::Source`]: `sync.rs`
//! builds one from a [`crate::config::DestinationConfig`] and calls its
//! (destination-agnostic) methods for DDL, inserts, the full-refresh atomic
//! swap, and incremental watermark state — the orchestration in `sync.rs`
//! never needs to know which concrete destination it's talking to.

pub mod bigquery;
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
    /// exist): ClickHouse's `EXCHANGE TABLES`, BigQuery's `WRITE_TRUNCATE`
    /// copy job.
    pub async fn atomic_swap(&self, dest: &str, staging: &str) -> Result<()> {
        match self {
            Sink::ClickHouse(s) => s.exchange_tables(dest, staging).await,
            Sink::BigQuery(s) => s.atomic_swap(dest, staging).await,
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
    ) -> Result<()> {
        match self {
            Sink::ClickHouse(_) => Err(EtlError::internal(
                "merge_into called on a ClickHouse sink — unreachable, since ClickHouse never \
                 reports requires_staging_for_incremental()",
            )),
            Sink::BigQuery(s) => s.merge_into(dest, staging, key, columns).await,
        }
    }
}
