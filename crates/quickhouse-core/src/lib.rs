//! quickhouse-core — the Rust engine behind the `quickhouse` Python package.
//!
//! Streams large PostgreSQL, MySQL, BigQuery or ClickHouse tables into
//! ClickHouse or BigQuery: native wire protocol (or, for BigQuery, the Storage
//! Read API; for ClickHouse, `FORMAT ArrowStream` over HTTP) ->
//! Apache Arrow -> the destination's native ingestion path (ClickHouse
//! `FORMAT ArrowStream`, or BigQuery's `insertAll` streaming insert), with
//! parallel range partitioning, bounded memory, auto DDL, and full-refresh /
//! incremental sync modes.
//!
//! The public entry point is [`sync::run_transfer`] (async) or
//! [`run_transfer_blocking`] for callers without an async runtime.

mod archive;
pub mod config;
pub mod ddl;
mod decimal;
pub mod decode;
pub mod decode_api;
pub mod decode_bigquery;
pub mod decode_clickhouse;
pub mod decode_mysql;
pub mod error;
pub mod host;
pub mod memory;
pub mod reconcile;
pub mod sink;
pub mod source;
pub mod state;
pub mod sync;
pub mod transform;
pub mod types;

pub use config::{
    ApiColumn, AppsFlyerConfig, ArchiveConfig, ArrowFrameConfig, BigQueryConfig,
    BigQueryDestConfig, BigQueryWriteMethod, CleverTapConfig, ClickHouseConfig,
    ClickHouseSourceConfig, Compression, DestinationConfig, GcsArchiveConfig, HttpApiConfig,
    HttpFormat, MySqlConfig, ParquetCompression, PostgresConfig, S3ArchiveConfig, SourceConfig,
    SourceShape, SyncMode, TransferConfig, TransferResult, TransferWarning, WarningKind,
    WatermarkSeed,
};
pub use error::{EtlError, Result};
pub use reconcile::{reconcile_keys, ReconcileConfig, ReconcileResult};
pub use sink::{build_sink, BigQuerySink, ClickHouseSink, Sink, StateKey};
pub use state::{compact_state, state_keys};
pub use sync::{run_transfer, Progress, ProgressCb, StagedInfo, StagedValidationCb};

/// Run a transfer to completion on a dedicated multi-threaded Tokio runtime.
///
/// Convenient for synchronous callers such as the Python binding.
pub fn run_transfer_blocking(
    source_cfg: SourceConfig,
    dest: DestinationConfig,
    cfg: TransferConfig,
    progress: Option<ProgressCb>,
    on_staged: Option<StagedValidationCb>,
) -> Result<TransferResult> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(EtlError::from)?;
    runtime.block_on(run_transfer(source_cfg, dest, cfg, progress, on_staged))
}

/// [`compact_state`] on a dedicated Tokio runtime, for synchronous callers.
pub fn compact_state_blocking(dest: DestinationConfig, state_table: &str) -> Result<u64> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(EtlError::from)?;
    runtime.block_on(compact_state(dest, state_table))
}

/// [`state_keys`] on a dedicated Tokio runtime, for synchronous callers.
pub fn state_keys_blocking(
    dest: DestinationConfig,
    state_table: &str,
    idle_days: Option<u32>,
) -> Result<Vec<StateKey>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(EtlError::from)?;
    runtime.block_on(state_keys(dest, state_table, idle_days))
}

/// Run a keyset reconciliation to completion on a dedicated Tokio runtime.
///
/// The [`reconcile_keys`] counterpart to [`run_transfer_blocking`], for
/// synchronous callers such as the Python binding.
pub fn reconcile_keys_blocking(
    source_cfg: SourceConfig,
    dest: DestinationConfig,
    cfg: ReconcileConfig,
) -> Result<ReconcileResult> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(EtlError::from)?;
    runtime.block_on(reconcile_keys(source_cfg, dest, cfg))
}
