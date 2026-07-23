//! Optional S3 (or S3-compatible, e.g. MinIO) data-lake archival for a
//! ClickHouse destination — see [`crate::config::S3ArchiveConfig`].
//!
//! Every batch synced into ClickHouse is also streamed to S3 as Parquet, one
//! file per parallel partition, via [`S3ArchiveWriter`] — never fully
//! buffered in memory (the same bounded-memory guarantee as the rest of this
//! crate). This is a secondary, best-effort-free side channel: it has no
//! effect on the ClickHouse write path, and is entirely absent when
//! `s3_archive` is `None`.
//!
//! Deliberately built on the same Apache Arrow ecosystem already in this
//! crate's dependency tree (`arrow`/`arrow-array`) rather than the official
//! AWS SDK: `parquet` is pinned to the exact `arrow` 53.x release line so
//! `RecordBatch`es already flowing through the decoders pass straight into
//! the Parquet writer with zero conversion, and `parquet`'s own
//! `object_store` integration (`ParquetObjectWriter`) needs no hand-rolled
//! `AsyncWrite`-over-S3-multipart glue. `object_store`'s `AmazonS3Builder`
//! also has its own request-level retry (`RetryConfig`), so unlike the
//! ClickHouse/BigQuery sinks this path doesn't need to reuse
//! `sink::{SendError, backoff_delay}` — the crate already solves that.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::async_writer::ParquetObjectWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::config::{ParquetCompression, S3ArchiveConfig};
use crate::error::{EtlError, Result};

/// Build the S3 client for an archive config. `AmazonS3Builder::from_env()`
/// resolves the standard AWS credential chain (env vars, IAM role) as the
/// base, so `access_key_id`/`secret_access_key`/`region` are only needed when
/// overriding that — e.g. for MinIO, which also needs `endpoint` (plain HTTP
/// is allowed automatically whenever a custom endpoint is set; real AWS S3
/// always uses HTTPS).
pub(crate) fn build_s3_store(cfg: &S3ArchiveConfig) -> Result<Arc<dyn ObjectStore>> {
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(&cfg.bucket);
    if let Some(region) = &cfg.region {
        builder = builder.with_region(region);
    }
    if let Some(key) = &cfg.access_key_id {
        builder = builder.with_access_key_id(key);
    }
    if let Some(secret) = &cfg.secret_access_key {
        builder = builder.with_secret_access_key(secret);
    }
    if let Some(endpoint) = &cfg.endpoint {
        builder = builder.with_endpoint(endpoint).with_allow_http(true);
    }
    let store = builder
        .build()
        .map_err(|e| EtlError::config(format!("s3 archive: failed to build S3 client: {e}")))?;
    Ok(Arc::new(store))
}

/// Hive-style object key: `{prefix}/{dest_table}/dt={run_date}/run={run_id}/
/// part-{partition_label}.parquet` — one file per parallel partition per run,
/// partitioned by date so standard data-lake query engines (Athena, Spark,
/// DuckDB) can prune by `dt=` without reading a table manifest. `prefix` may
/// be empty (writes at the bucket root); a non-empty prefix's own leading/
/// trailing slashes are trimmed so callers don't need to worry about
/// double-slashes.
pub(crate) fn archive_object_key(prefix: &str, dest_table: &str, run_date: &str, run_id: &str, partition_label: &str) -> String {
    let trimmed = prefix.trim_matches('/');
    let mut parts = Vec::with_capacity(5);
    if !trimmed.is_empty() {
        parts.push(trimmed.to_string());
    }
    parts.push(dest_table.to_string());
    parts.push(format!("dt={run_date}"));
    parts.push(format!("run={run_id}"));
    parts.push(format!("part-{partition_label}.parquet"));
    parts.join("/")
}

fn parquet_compression(c: ParquetCompression) -> Compression {
    match c {
        ParquetCompression::Zstd => Compression::ZSTD(ZstdLevel::default()),
        ParquetCompression::Snappy => Compression::SNAPPY,
        ParquetCompression::Uncompressed => Compression::UNCOMPRESSED,
    }
}

/// Streams one Parquet file to S3 for the lifetime of one partition: each
/// call to [`Self::write`] appends a new row group without buffering prior
/// ones, and [`Self::close`] finalizes the footer and completes the
/// underlying multipart upload.
pub(crate) struct S3ArchiveWriter {
    inner: AsyncArrowWriter<ParquetObjectWriter>,
    key: String,
}

impl S3ArchiveWriter {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, key: String, schema: SchemaRef, compression: ParquetCompression) -> Result<Self> {
        let writer = ParquetObjectWriter::new(store, Path::from(key.as_str()));
        let props = WriterProperties::builder().set_compression(parquet_compression(compression)).build();
        let inner = AsyncArrowWriter::try_new(writer, schema, Some(props))
            .map_err(|e| EtlError::other(format!("s3 archive: failed to open parquet writer for '{key}': {e}")))?;
        Ok(Self { inner, key })
    }

    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.inner
            .write(batch)
            .await
            .map_err(|e| EtlError::other(format!("s3 archive: parquet write error for '{}': {e}", self.key)))
    }

    pub(crate) async fn close(self) -> Result<()> {
        let key = self.key.clone();
        self.inner
            .close()
            .await
            .map_err(|e| EtlError::other(format!("s3 archive: failed to finalize parquet file '{key}': {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_object_key_full_hive_style_path() {
        let key = archive_object_key("lake", "orders", "2026-07-23", "1753234567", "range-0");
        assert_eq!(key, "lake/orders/dt=2026-07-23/run=1753234567/part-range-0.parquet");
    }

    #[test]
    fn archive_object_key_empty_prefix_writes_at_bucket_root() {
        let key = archive_object_key("", "orders", "2026-07-23", "1", "all");
        assert_eq!(key, "orders/dt=2026-07-23/run=1/part-all.parquet");
    }

    #[test]
    fn archive_object_key_trims_slashes_from_prefix() {
        let key = archive_object_key("/lake/", "orders", "2026-07-23", "1", "all");
        assert_eq!(key, "lake/orders/dt=2026-07-23/run=1/part-all.parquet");
        let key = archive_object_key("///", "orders", "2026-07-23", "1", "all");
        assert_eq!(key, "orders/dt=2026-07-23/run=1/part-all.parquet");
    }

    #[test]
    fn parquet_compression_maps_every_variant() {
        assert_eq!(parquet_compression(ParquetCompression::Zstd), Compression::ZSTD(ZstdLevel::default()));
        assert_eq!(parquet_compression(ParquetCompression::Snappy), Compression::SNAPPY);
        assert_eq!(parquet_compression(ParquetCompression::Uncompressed), Compression::UNCOMPRESSED);
    }

    // `build_s3_store` itself does not validate that `bucket` is non-empty —
    // `object_store`'s builder happily accepts an empty string (it would only
    // surface as a rejected request against real S3/MinIO later). Requiring a
    // non-empty bucket is deliberately enforced one layer up, at the Python
    // boundary (`AnyDestination::into_config` in quickhouse-py), matching how
    // `BigQueryDestConfig::dataset_id` requiredness is checked the same way
    // rather than re-validated deep in core.

    #[test]
    fn build_s3_store_accepts_minio_style_config() {
        let cfg = S3ArchiveConfig {
            bucket: "test-bucket".to_string(),
            prefix: "lake".to_string(),
            region: Some("us-east-1".to_string()),
            access_key_id: Some("minioadmin".to_string()),
            secret_access_key: Some("minioadmin".to_string()),
            endpoint: Some("http://localhost:9000".to_string()),
            compression: ParquetCompression::Zstd,
        };
        assert!(build_s3_store(&cfg).is_ok());
    }
}
