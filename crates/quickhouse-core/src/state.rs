//! Maintenance of the incremental cursor state table (`_quickhouse_state`).
//!
//! Every `persist_watermark` appends a row and nothing removes old ones. On
//! ClickHouse the table is a `ReplacingMergeTree` that merges them away in the
//! background; on BigQuery it only grows, and keys renamed long ago keep their
//! rows forever (96% of one production state table's bytes). These helpers
//! compact the table and list its keys, so the dead ones can be found.

use crate::config::DestinationConfig;
use crate::error::Result;
use crate::sink::{build_sink, StateKey};

/// Delete every state row that isn't the newest for its `(state_key,
/// dest_table)` key, leaving the values every cursor read returns unchanged.
/// Safe to run between syncs. Returns the rows deleted.
pub async fn compact_state(dest: DestinationConfig, state_table: &str) -> Result<u64> {
    // The rustls provider every entry point selects; see `sync::run_transfer_impl`.
    let _ = rustls::crypto::ring::default_provider().install_default();
    build_sink(dest).await?.compact_state(state_table).await
}

/// Every key in the state table with its newest cursor, oldest first; with
/// `idle_days`, only the keys with no write in that many days.
pub async fn state_keys(
    dest: DestinationConfig,
    state_table: &str,
    idle_days: Option<u32>,
) -> Result<Vec<StateKey>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    build_sink(dest)
        .await?
        .state_keys(state_table, idle_days)
        .await
}
