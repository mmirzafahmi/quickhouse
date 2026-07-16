//! etlhouse-core — the Rust engine behind the `etlhouse` Python package.
//!
//! Streams large PostgreSQL tables into ClickHouse: binary `COPY` -> Apache
//! Arrow -> ClickHouse `FORMAT ArrowStream`, with parallel range partitioning,
//! bounded memory, auto DDL, and full-refresh / incremental sync modes.
//!
//! The public entry point is [`sync::run_transfer`] (async) or
//! [`run_transfer_blocking`] for callers without an async runtime.

pub mod config;
pub mod ddl;
pub mod decode;
pub mod error;
pub mod sink;
pub mod source;
pub mod sync;
pub mod transform;
pub mod types;

pub use config::{
    ClickHouseConfig, Compression, PostgresConfig, SyncMode, TransferConfig, TransferResult,
};
pub use error::{EtlError, Result};
pub use sync::{run_transfer, Progress, ProgressCb};

/// Run a transfer to completion on a dedicated multi-threaded Tokio runtime.
///
/// Convenient for synchronous callers such as the Python binding.
pub fn run_transfer_blocking(
    pg: PostgresConfig,
    ch: ClickHouseConfig,
    cfg: TransferConfig,
    progress: Option<ProgressCb>,
) -> Result<TransferResult> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(EtlError::from)?;
    runtime.block_on(run_transfer(pg, ch, cfg, progress))
}
