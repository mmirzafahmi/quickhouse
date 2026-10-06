//! Optional cloud data-lake archival for either destination — S3 (or an
//! S3-compatible store like MinIO) and Google Cloud Storage. See
//! [`crate::config::ArchiveConfig`].
//!
//! Every batch synced to the destination is also streamed to object storage
//! as Parquet, one file per parallel partition (one per chunk for a
//! `chunk_rows` read), via [`ArchiveWriter`] — never fully buffered in memory
//! (the same bounded-memory guarantee as the rest of this crate). This is a
//! secondary, best-effort-free side channel: it has no effect on the
//! destination write path, and is entirely absent when `archive` is `None`.
//!
//! Deliberately built on the same Apache Arrow ecosystem already in this
//! crate's dependency tree (`arrow`/`arrow-array`) rather than a vendor SDK:
//! `parquet` is pinned to the exact `arrow` 53.x release line so
//! `RecordBatch`es already flowing through the decoders pass straight into
//! the Parquet writer with zero conversion, and `object_store`'s `BufWriter`
//! does the multipart upload, so there is no hand-rolled multipart glue — only
//! [`UploadWriter`], which feeds it exactly as `parquet`'s own
//! `ParquetObjectWriter` does while keeping a handle that can abort it.
//! `object_store`'s builders also have their own request-level retry
//! (`RetryConfig`), so unlike the ClickHouse/BigQuery sinks this path doesn't
//! need to reuse `sink::{SendError, backoff_delay}` — the crate already solves
//! that.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use futures::future::BoxFuture;
use object_store::aws::AmazonS3Builder;
use object_store::buffered::BufWriter;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use tokio::io::AsyncWriteExt;

use crate::config::{ArchiveConfig, GcsArchiveConfig, ParquetCompression, S3ArchiveConfig};
use crate::error::{EtlError, Result};

/// Build the object store for an archive config, whichever backend it names.
/// The only place in the crate that looks at the [`ArchiveConfig`] variant —
/// everything downstream holds an `Arc<dyn ObjectStore>`.
pub(crate) fn build_store(cfg: &ArchiveConfig) -> Result<Arc<dyn ObjectStore>> {
    match cfg {
        ArchiveConfig::S3(c) => build_s3_store(c),
        ArchiveConfig::Gcs(c) => build_gcs_store(c),
    }
}

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

/// An unauthenticated service-account key pointing `object_store` at an
/// alternate GCS base URL. `GoogleCloudStorageBuilder` has no endpoint setter
/// — the base URL is only readable out of the service-account JSON
/// (`gcs_base_url`, with `disable_oauth` to skip token minting), so an
/// `endpoint` is expressed by synthesizing that document. This is the shape
/// `object_store` documents for exactly this purpose.
///
/// Note this does NOT make `fake-gcs-server` usable: `object_store` writes
/// objects through Google's XML API (`PUT {base}/{bucket}/{object}`), and
/// that emulator implements only the JSON upload API, answering the XML path
/// with `400 invalid uploadType`. The knob is for a proxy or a private
/// endpoint that speaks the real XML API. See `tests/test_gcs_archive.py`.
fn unauthenticated_service_account_key(endpoint: &str) -> String {
    // `endpoint` lands inside a JSON string, so a quote or backslash in it
    // would otherwise produce a malformed document that surfaces as an
    // inscrutable parse error rather than a bad-URL one.
    let escaped = endpoint.replace('\\', "\\\\").replace('"', "\\\"");
    // `private_key_id` looks redundant next to the empty `private_key`, but
    // `ServiceAccountCredentials` declares it as a plain non-defaulted
    // `String` — only `gcs_base_url` and `disable_oauth` carry
    // `#[serde(default)]` — so omitting it fails deserialization with
    // "missing field `private_key_id`" before the base URL is ever read.
    format!(
        r#"{{"gcs_base_url":"{escaped}","disable_oauth":true,"client_email":"","private_key":"","private_key_id":""}}"#
    )
}

/// Build the GCS client for an archive config. With no credentials set at
/// all, `GoogleCloudStorageBuilder::from_env()` resolves the standard Google
/// chain (`SERVICE_ACCOUNT`, `GOOGLE_SERVICE_ACCOUNT*`, Application Default
/// Credentials, the metadata server). `credentials_json` wins over
/// `credentials_file`, matching `BigQueryDestConfig`; `endpoint` wins over
/// both, since it is only ever set to reach an emulator that has no real
/// credentials to check.
///
/// Note the asymmetry with [`build_s3_store`], which layers overrides on top
/// of `from_env()`: here an explicit credential must start from `new()`
/// instead. `from_env()` populates `service_account_path` from
/// `SERVICE_ACCOUNT`/`GOOGLE_SERVICE_ACCOUNT`, and `build()` rejects holding
/// both a path and a key at once with `"One of service account path or
/// service account key may be provided"`. So layering breaks precisely the
/// two arms that set a *key* — `credentials_json` and `endpoint` — for any
/// user who also has a service account exported, while `credentials_file`
/// survives because it writes the same field `from_env()` did. Verified both
/// ways against real GCS: with `SERVICE_ACCOUNT` set, the layered form fails
/// those two and the branched form passes all four. That failure is also
/// invisible in clean CI and only shows up on a configured machine, which is
/// the worst shape a bug can take. Starting from `new()` costs the `GOOGLE_*`
/// client options (proxy, timeout); that is the cheaper loss.
pub(crate) fn build_gcs_store(cfg: &GcsArchiveConfig) -> Result<Arc<dyn ObjectStore>> {
    let builder = match (&cfg.endpoint, &cfg.credentials_json, &cfg.credentials_file) {
        (Some(endpoint), _, _) => GoogleCloudStorageBuilder::new()
            .with_service_account_key(unauthenticated_service_account_key(endpoint)),
        (None, Some(json), _) => GoogleCloudStorageBuilder::new().with_service_account_key(json),
        (None, None, Some(path)) => {
            GoogleCloudStorageBuilder::new().with_service_account_path(path)
        }
        (None, None, None) => GoogleCloudStorageBuilder::from_env(),
    };
    let store = builder
        .with_bucket_name(&cfg.bucket)
        .build()
        .map_err(|e| EtlError::config(format!("gcs archive: failed to build GCS client: {e}")))?;
    Ok(Arc::new(store))
}

/// Hive-style object key: `{prefix}/{dest_table}/dt={run_date}/run={run_id}/
/// part-{partition_label}.parquet` — one file per parallel partition per run,
/// partitioned by date so standard data-lake query engines (Athena, Spark,
/// DuckDB) can prune by `dt=` without reading a table manifest. `prefix` may
/// be empty (writes at the bucket root); a non-empty prefix's own leading/
/// trailing slashes are trimmed so callers don't need to worry about
/// double-slashes.
pub(crate) fn archive_object_key(
    prefix: &str,
    dest_table: &str,
    run_date: &str,
    run_id: &str,
    partition_label: &str,
) -> String {
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

/// Every upload one transfer attempt opens, so the attempt can abort the ones
/// it never finished.
///
/// A file bigger than `BufWriter`'s 10 MiB buffer goes up as a multipart
/// upload and becomes an object only when the upload is completed. One that
/// never is — its transfer failed — is invisible but not gone: the store keeps
/// its parts, and bills for them, until the upload is aborted, and
/// `object_store` has no `Drop` that aborts one. A `Drop` here couldn't do it
/// reliably either: aborting is async, and `run_transfer_blocking` drops its
/// runtime as soon as the transfer returns, cancelling any task spawned to do
/// it. So the attempt awaits [`Self::abort_unfinished`] itself when it fails.
#[derive(Clone, Default)]
pub(crate) struct ArchiveUploads {
    started: Arc<std::sync::Mutex<Vec<(String, SharedUpload)>>>,
}

type SharedUpload = Arc<tokio::sync::Mutex<Upload>>;

/// One upload, shared by the [`UploadWriter`] feeding it and the
/// [`ArchiveUploads`] that may have to abort it.
enum Upload {
    Open(Box<BufWriter>),
    /// Completed, aborted, or past the point where it can still be aborted:
    /// `BufWriter::abort` panics once a shutdown has begun.
    Closed,
}

impl ArchiveUploads {
    /// Start an upload to `key`, keeping a handle on it.
    fn start(&self, store: Arc<dyn ObjectStore>, key: &str) -> UploadWriter {
        let writer = BufWriter::new(store, Path::from(key));
        let upload = Arc::new(tokio::sync::Mutex::new(Upload::Open(Box::new(writer))));
        self.started
            .lock()
            .unwrap()
            .push((key.to_string(), upload.clone()));
        UploadWriter(upload)
    }

    /// Abort one upload now, best effort, as [`Self::abort_unfinished`] does.
    async fn abort(key: &str, upload: &SharedUpload) {
        let state = std::mem::replace(&mut *upload.lock().await, Upload::Closed);
        if let Upload::Open(mut w) = state {
            match w.abort().await {
                Ok(()) => tracing::debug!("archive: aborted the unfinished upload of '{key}'"),
                Err(e) => tracing::warn!(
                    "archive: could not abort the unfinished upload of '{key}' ({e}); its \
                     parts stay in the bucket until a lifecycle rule removes them"
                ),
            }
        }
    }

    /// Abort every upload not yet completed. Best effort: a failure is logged,
    /// never returned, so it can't mask the error that failed the transfer. An
    /// upload still inside `BufWriter`'s buffer has sent nothing, and aborting
    /// it is a no-op.
    pub(crate) async fn abort_unfinished(&self) {
        let started = std::mem::take(&mut *self.started.lock().unwrap());
        for (key, upload) in started {
            Self::abort(&key, &upload).await;
        }
    }
}

/// `parquet`'s `ParquetObjectWriter`, except that the upload is shared with
/// the [`ArchiveUploads`] that started it, so a failed transfer can abort it.
struct UploadWriter(SharedUpload);

impl AsyncFileWriter for UploadWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            match &mut *self.0.lock().await {
                Upload::Open(w) => w
                    .put(bs)
                    .await
                    .map_err(|e| ParquetError::External(Box::new(e))),
                Upload::Closed => Err(ParquetError::General(
                    "archive upload is already closed".to_string(),
                )),
            }
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            // Out of the shared slot before the shutdown starts, since from
            // then on the upload can't be aborted.
            let state = std::mem::replace(&mut *self.0.lock().await, Upload::Closed);
            match state {
                Upload::Open(mut w) => w
                    .shutdown()
                    .await
                    .map_err(|e| ParquetError::External(Box::new(e))),
                Upload::Closed => Err(ParquetError::General(
                    "archive upload is already closed".to_string(),
                )),
            }
        })
    }
}

/// Streams one Parquet file to object storage for the lifetime of one
/// partition, or of one chunk of a `chunk_rows` read: each call to
/// [`Self::write`] appends a new row group without buffering prior ones, and
/// [`Self::close`] finalizes the footer and completes the underlying upload.
pub(crate) struct ArchiveWriter {
    inner: AsyncArrowWriter<UploadWriter>,
    /// The upload `inner` writes to, so [`Self::abort`] can reach it.
    upload: SharedUpload,
    key: String,
    /// `ArchiveConfig::kind()` — carried purely so an error names the store it
    /// failed against; by this point the config itself is long gone.
    kind: &'static str,
}

impl ArchiveWriter {
    /// Open a file at `key`. Its upload is registered with `uploads`, so a
    /// transfer that fails before [`Self::close`] can abort it.
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        key: String,
        schema: SchemaRef,
        compression: ParquetCompression,
        kind: &'static str,
        uploads: &ArchiveUploads,
    ) -> Result<Self> {
        let writer = uploads.start(store, &key);
        let upload = writer.0.clone();
        let props = WriterProperties::builder()
            .set_compression(parquet_compression(compression))
            .build();
        let inner = AsyncArrowWriter::try_new(writer, schema, Some(props)).map_err(|e| {
            EtlError::other(format!(
                "{kind} archive: failed to open parquet writer for '{key}': {e}"
            ))
        })?;
        Ok(Self {
            inner,
            upload,
            key,
            kind,
        })
    }

    /// Give up on this file: abort its upload now rather than leave it to the
    /// transfer's own failure, for a read that fails but is tried again in
    /// the same run (a window of a sweep). A run that then succeeds would
    /// otherwise leave the upload's parts in the bucket.
    pub(crate) async fn abort(self) {
        ArchiveUploads::abort(&self.key, &self.upload).await;
    }

    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.inner.write(batch).await.map_err(|e| {
            EtlError::other(format!(
                "{} archive: parquet write error for '{}': {e}",
                self.kind, self.key
            ))
        })
    }

    pub(crate) async fn close(self) -> Result<()> {
        let key = self.key.clone();
        let kind = self.kind;
        self.inner.close().await.map_err(|e| {
            EtlError::other(format!(
                "{kind} archive: failed to finalize parquet file '{key}': {e}"
            ))
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_object_key_full_hive_style_path() {
        let key = archive_object_key("lake", "orders", "2026-07-23", "1753234567", "range-0");
        assert_eq!(
            key,
            "lake/orders/dt=2026-07-23/run=1753234567/part-range-0.parquet"
        );
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
        assert_eq!(
            parquet_compression(ParquetCompression::Zstd),
            Compression::ZSTD(ZstdLevel::default())
        );
        assert_eq!(
            parquet_compression(ParquetCompression::Snappy),
            Compression::SNAPPY
        );
        assert_eq!(
            parquet_compression(ParquetCompression::Uncompressed),
            Compression::UNCOMPRESSED
        );
    }

    /// A GCS config with only the two required fields set; each test varies
    /// the one field it is about.
    fn gcs_cfg() -> GcsArchiveConfig {
        GcsArchiveConfig {
            bucket: "test-bucket".to_string(),
            prefix: "lake".to_string(),
            credentials_file: None,
            credentials_json: None,
            endpoint: None,
            compression: ParquetCompression::Zstd,
        }
    }

    // Passes with no GCP credentials anywhere, which is what CI has: with no
    // service account set, `ApplicationDefaultCredentials::read(None)` returns
    // `Ok(None)` when the gcloud well-known file is absent, and the builder
    // falls through to the metadata-server provider. Nothing here reaches the
    // network. Don't "fix" this into an `is_err()`.
    #[test]
    fn build_gcs_store_with_no_credentials_falls_back_to_the_default_chain() {
        assert!(build_gcs_store(&gcs_cfg()).is_ok());
    }

    // Regression: the synthesized key must satisfy
    // `ServiceAccountCredentials`, which declares `private_key_id` WITHOUT
    // `#[serde(default)]`. Omitting it fails with "missing field
    // `private_key_id`" before `gcs_base_url` is ever read.
    #[test]
    fn build_gcs_store_accepts_a_custom_endpoint() {
        let cfg = GcsArchiveConfig {
            endpoint: Some("http://localhost:4443".to_string()),
            ..gcs_cfg()
        };
        assert!(build_gcs_store(&cfg).is_ok());
    }

    #[test]
    fn synthesized_key_escapes_a_quote_in_the_endpoint() {
        let key = unauthenticated_service_account_key("http://h\"x");
        assert!(key.contains("\"gcs_base_url\":\"http://h\\\"x\""));
        assert!(key.contains("\"private_key_id\":\"\""));
    }

    // Regression, and the reason `build_gcs_store` starts from
    // `GoogleCloudStorageBuilder::new()` rather than layering onto
    // `from_env()`: `build()` rejects having both a service-account PATH and
    // a KEY ("One of service account path or service account key may be
    // provided"). Setting both here proves the explicit branch wins instead
    // of colliding — the same collision `SERVICE_ACCOUNT` in the environment
    // would otherwise cause, which would pass in clean CI and fail on a
    // developer machine with GCP credentials exported.
    #[test]
    fn build_gcs_store_prefers_credentials_json_over_credentials_file() {
        let cfg = GcsArchiveConfig {
            credentials_file: Some("/nonexistent/sa.json".to_string()),
            credentials_json: Some(unauthenticated_service_account_key("http://localhost:4443")),
            ..gcs_cfg()
        };
        assert!(build_gcs_store(&cfg).is_ok());
    }

    // Malformed inline credentials fail at build time — before any source
    // connection is opened — which is why `build_archive_run_info` builds the
    // store up front. Stronger fail-fast than the S3 path gets.
    #[test]
    fn build_gcs_store_rejects_malformed_credentials_json() {
        let cfg = GcsArchiveConfig {
            credentials_json: Some("{not json".to_string()),
            ..gcs_cfg()
        };
        let err = build_gcs_store(&cfg).unwrap_err().to_string();
        assert!(err.contains("gcs archive:"), "unlabelled error: {err}");
    }

    // Full write -> close -> read-back through a real `ObjectStore`, which
    // the S3/GCS builders' `is_ok()` checks cannot reach (they never touch
    // the network). `InMemory` is the same `dyn ObjectStore` the cloud
    // backends resolve to, so this covers everything above the transport:
    // Parquet framing, the row groups one `write` each produces, and the
    // footer `close` finalizes. Without it, "the file is valid Parquet" is
    // only checked by the MinIO suite, which needs Docker.
    #[tokio::test]
    async fn archive_writer_round_trips_parquet_through_an_object_store() {
        use arrow_array::{Int64Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use object_store::memory::InMemory;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let key = archive_object_key("lake", "orders", "2026-09-21", "42", "range-0");

        let mut w = ArchiveWriter::new(
            store.clone(),
            key.clone(),
            schema.clone(),
            ParquetCompression::Zstd,
            "s3",
            &ArchiveUploads::default(),
        )
        .expect("open writer");
        // Two writes, so the read-back also proves multiple row groups are
        // concatenated rather than the last one winning.
        for chunk in [[1_i64, 2], [3, 4]] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(chunk.to_vec())),
                    Arc::new(StringArray::from(
                        chunk.iter().map(|i| format!("row-{i}")).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            w.write(&batch).await.expect("write batch");
        }
        w.close().await.expect("finalize");

        let bytes = store
            .get(&Path::from(key.as_str()))
            .await
            .expect("archived object exists")
            .bytes()
            .await
            .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("a finalized archive must be readable Parquet")
            .build()
            .unwrap();
        let ids: Vec<i64> = reader
            .map(|b| b.unwrap())
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![1, 2, 3, 4]);
    }

    // An archive abandoned without `close` must leave nothing behind: a
    // half-written Parquet file with no footer would read as corrupt, and a
    // transfer that errors mid-run (a truncated Arrow frame, a failed source
    // read) drops the writer exactly this way.
    #[tokio::test]
    async fn dropping_a_writer_without_close_publishes_no_object() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};
        use object_store::memory::InMemory;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let key = "lake/orders/dt=2026-09-21/run=1/part-all.parquet".to_string();
        {
            let mut w = ArchiveWriter::new(
                store.clone(),
                key.clone(),
                schema.clone(),
                ParquetCompression::Snappy,
                "gcs",
                &ArchiveUploads::default(),
            )
            .unwrap();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
            )
            .unwrap();
            w.write(&batch).await.unwrap();
            // dropped here, deliberately without close()
        }
        assert!(store.get(&Path::from(key.as_str())).await.is_err());
    }

    /// `InMemory`, counting what becomes of its multipart uploads — the one
    /// thing `InMemory` can't tell a test, because an aborted upload and one
    /// left open alike just never become an object.
    #[derive(Debug)]
    struct CountingStore {
        inner: object_store::memory::InMemory,
        started: Arc<std::sync::atomic::AtomicUsize>,
        completed: Arc<std::sync::atomic::AtomicUsize>,
        aborted: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl CountingStore {
        fn new() -> Self {
            Self {
                inner: object_store::memory::InMemory::new(),
                started: Arc::default(),
                completed: Arc::default(),
                aborted: Arc::default(),
            }
        }

        fn count(n: &std::sync::atomic::AtomicUsize) -> usize {
            n.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOpts,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.started
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Box::new(CountingUpload {
                inner: self.inner.put_multipart_opts(location, opts).await?,
                completed: self.completed.clone(),
                aborted: self.aborted.clone(),
            }))
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'_, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    #[derive(Debug)]
    struct CountingUpload {
        inner: Box<dyn object_store::MultipartUpload>,
        completed: Arc<std::sync::atomic::AtomicUsize>,
        aborted: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl object_store::MultipartUpload for CountingUpload {
        fn put_part(&mut self, data: object_store::PutPayload) -> object_store::UploadPart {
            self.inner.put_part(data)
        }

        async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
            self.completed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.complete().await
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.aborted
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.abort().await
        }
    }

    /// Past `BufWriter`'s 10 MiB buffer, so an upload holding this much is a
    /// real multipart one with parts already sent.
    fn eleven_mib() -> Bytes {
        Bytes::from(vec![0_u8; 11 * 1024 * 1024])
    }

    // Regression: a transfer that failed after its archive began sending parts
    // left the multipart upload open in the bucket. It never became an object
    // and nothing ever aborted it, so its parts stayed, billed. A failed run on
    // GCS left one behind for every file it had open.
    #[tokio::test]
    async fn abort_unfinished_aborts_an_upload_that_sent_parts() {
        let store = Arc::new(CountingStore::new());
        let uploads = ArchiveUploads::default();
        let key = "lake/orders/dt=2026-10-06/run=1/part-keyset-00003.parquet";
        let mut w = uploads.start(store.clone(), key);
        w.write(eleven_mib()).await.unwrap();
        assert_eq!(
            CountingStore::count(&store.started),
            1,
            "expected a multipart upload"
        );

        uploads.abort_unfinished().await;

        assert_eq!(CountingStore::count(&store.aborted), 1);
        assert_eq!(CountingStore::count(&store.completed), 0);
        assert!(store.inner.get(&Path::from(key)).await.is_err());
        // An aborted upload takes no more bytes, and can't be completed.
        assert!(w.write(Bytes::from_static(b"late")).await.is_err());
        assert!(w.complete().await.is_err());
    }

    // The other half: a file the run finished is never touched, even though
    // the attempt failed later on — e.g. the chunks of a `chunk_rows` read that
    // committed before the chunk that failed.
    #[tokio::test]
    async fn abort_unfinished_leaves_a_completed_upload_alone() {
        let store = Arc::new(CountingStore::new());
        let uploads = ArchiveUploads::default();
        let key = "lake/orders/dt=2026-10-06/run=1/part-keyset-00000.parquet";
        let mut w = uploads.start(store.clone(), key);
        w.write(eleven_mib()).await.unwrap();
        w.complete().await.unwrap();

        uploads.abort_unfinished().await;

        assert_eq!(CountingStore::count(&store.completed), 1);
        assert_eq!(CountingStore::count(&store.aborted), 0);
        let got = store.inner.get(&Path::from(key)).await.unwrap();
        assert_eq!(got.bytes().await.unwrap().len(), 11 * 1024 * 1024);
    }

    /// Live GCS round trip, skipped unless `QUICKHOUSE_GCS_BUCKET` names a
    /// bucket you can write to. This is the only coverage of the real GCS
    /// transport: `object_store` writes objects through Google's XML API and
    /// fake-gcs-server implements only the JSON upload API, so no emulator
    /// can stand in (see `tests/test_gcs_archive.py`). Mirrors how the
    /// BigQuery sink gates its live tests on an env var.
    ///
    ///   QUICKHOUSE_GCS_BUCKET=my-bucket \
    ///   QUICKHOUSE_GCS_CREDENTIALS=/path/to/sa.json \
    ///     cargo test -p quickhouse-core live_gcs -- --ignored --nocapture
    ///
    /// `#[ignore]` as well as env-gated: it reaches the network and writes a
    /// real object, so it must never run as part of a plain `cargo test`.
    /// Everything it creates is deleted again, including on assertion
    /// failure paths that reach the cleanup.
    #[tokio::test]
    #[ignore = "writes to a real GCS bucket; set QUICKHOUSE_GCS_BUCKET"]
    async fn live_gcs_archive_round_trip() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let Ok(bucket) = std::env::var("QUICKHOUSE_GCS_BUCKET") else {
            eprintln!("skipped: QUICKHOUSE_GCS_BUCKET not set");
            return;
        };
        let cfg = GcsArchiveConfig {
            bucket: bucket.clone(),
            prefix: std::env::var("QUICKHOUSE_GCS_PREFIX")
                .unwrap_or_else(|_| "quickhouse-livetest".to_string()),
            credentials_file: std::env::var("QUICKHOUSE_GCS_CREDENTIALS").ok(),
            credentials_json: None,
            endpoint: None,
            compression: ParquetCompression::Zstd,
        };
        let store = build_gcs_store(&cfg).expect("build GCS client");

        // Unique per run, so a failed run never collides with a later one.
        let run_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .to_string();
        let key = archive_object_key(&cfg.prefix, "live_probe", "2026-09-21", &run_id, "all");
        eprintln!("writing gs://{bucket}/{key}");

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut w = ArchiveWriter::new(
            store.clone(),
            key.clone(),
            schema.clone(),
            cfg.compression,
            "gcs",
            &ArchiveUploads::default(),
        )
        .expect("open writer");
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5]))],
        )
        .unwrap();
        w.write(&batch).await.expect("write batch to GCS");
        w.close().await.expect("finalize object in GCS");

        let path = Path::from(key.as_str());
        let got = store.get(&path).await.expect("read the object back");
        let bytes = got.bytes().await.unwrap();
        let ids: Vec<i64> = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("object must be valid Parquet")
            .build()
            .unwrap()
            .map(|b| b.unwrap())
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();

        store
            .delete(&path)
            .await
            .expect("clean up the probe object");
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
        eprintln!("round trip OK, probe object deleted");
    }

    #[test]
    fn build_store_dispatches_on_the_configured_backend() {
        assert!(build_store(&ArchiveConfig::Gcs(gcs_cfg())).is_ok());
        assert!(build_store(&ArchiveConfig::S3(S3ArchiveConfig {
            bucket: "b".to_string(),
            prefix: String::new(),
            region: Some("us-east-1".to_string()),
            access_key_id: None,
            secret_access_key: None,
            endpoint: None,
            compression: ParquetCompression::Zstd,
        }))
        .is_ok());
    }

    // Neither builder validates that `bucket` is non-empty —
    // `object_store`'s builders happily accept an empty string (it would only
    // surface as a rejected request against real S3/GCS/MinIO later).
    // Requiring a non-empty bucket is deliberately enforced one layer up, at
    // the Python boundary (`archive_config` in quickhouse-py), matching how
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
