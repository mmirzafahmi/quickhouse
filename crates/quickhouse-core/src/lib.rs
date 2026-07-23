//! quickhouse-core — the Rust engine behind the `quickhouse` Python package.
//!
//! Streams large PostgreSQL, MySQL, or BigQuery tables into ClickHouse or
//! BigQuery: native wire protocol (or, for BigQuery, the Storage Read API) ->
//! Apache Arrow -> the destination's native ingestion path (ClickHouse
//! `FORMAT ArrowStream`, or BigQuery's `insertAll` streaming insert), with
//! parallel range partitioning, bounded memory, auto DDL, and full-refresh /
//! incremental sync modes.
//!
//! The public entry point is [`sync::run_transfer`] (async) or
//! [`run_transfer_blocking`] for callers without an async runtime.

mod archive;
pub mod config;
mod decimal;
pub mod ddl;
pub mod decode;
pub mod decode_bigquery;
pub mod decode_mysql;
pub mod error;
pub mod memory;
pub mod sink;
pub mod source;
pub mod sync;
pub mod transform;
pub mod types;

pub use config::{
    BigQueryConfig, BigQueryDestConfig, BigQueryWriteMethod, ClickHouseConfig, Compression,
    DestinationConfig, MySqlConfig, ParquetCompression, PostgresConfig, S3ArchiveConfig,
    SourceConfig, SyncMode, TransferConfig, TransferResult,
};
pub use error::{EtlError, Result};
pub use sync::{run_transfer, Progress, ProgressCb};

/// Run a transfer to completion on a dedicated multi-threaded Tokio runtime.
///
/// Convenient for synchronous callers such as the Python binding.
pub fn run_transfer_blocking(
    source_cfg: SourceConfig,
    dest: DestinationConfig,
    cfg: TransferConfig,
    progress: Option<ProgressCb>,
) -> Result<TransferResult> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(EtlError::from)?;
    runtime.block_on(run_transfer(source_cfg, dest, cfg, progress))
}
