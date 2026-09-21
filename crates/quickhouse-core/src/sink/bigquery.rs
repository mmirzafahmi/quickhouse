//! BigQuery sink: auth, structured DDL (no SQL-string templating, unlike
//! ClickHouse — see `build_table`), and writes via either `tabledata.insertAll`
//! (default) or the Storage Write API (opt-in, `write_method="storage_write"`
//! — see `bigquery_proto` for the runtime protobuf descriptor/encoder).
//! Load jobs — BigQuery's own recommended bulk path, and free — are still not
//! implemented, but the reason recorded here previously (that they "would add a
//! hard Cloud Storage dependency and a bucket requirement just for this
//! destination") no longer holds: `object_store` and `parquet` are already
//! workspace dependencies, and `archive.rs` already streams `RecordBatch`es to
//! Parquet in object storage via `ParquetObjectWriter`, so a staging writer
//! would be reuse rather than new machinery. What actually blocks it is narrower
//! and worth stating: a Parquet load path needs its own verified type mapping
//! (this crate is deliberately exacting about decimal precision and timestamp
//! fidelity — see `decimal.rs` and `bigquery_proto`), and the `Sink` trait has
//! no "all writes are done" hook for a bulk load to fire from. Both are
//! tractable; neither is a detail to guess at.
//!
//! The atomic full-refresh swap (ClickHouse's `EXCHANGE TABLES`) has no
//! single-statement BigQuery equivalent. The obvious-looking option — a COPY
//! job with `WRITE_TRUNCATE` — is actually **wrong**: BigQuery copy jobs never
//! include rows still sitting in a table's streaming buffer (true for both
//! `insertAll` and the Storage Write API), so a copy run immediately after
//! writing `staging` can silently produce an empty `dest` while `sync()`
//! still reports success. Instead, `atomic_swap` runs `TRUNCATE TABLE dest` +
//! `INSERT INTO dest SELECT ... FROM staging` inside an explicit
//! multi-statement transaction (`build_swap_sql`) — a DML/DDL query job,
//! which (unlike a copy job) correctly reads all of `staging`'s rows whether
//! buffered or not, while `TRUNCATE` (unlike `CREATE OR REPLACE TABLE ... AS
//! SELECT`) preserves `dest`'s existing partitioning/clustering. This bills
//! for reading `staging` once per full refresh — the same cost the
//! incremental `MERGE` path already accepts — in exchange for actually being
//! correct.
//!
//! **Write idempotency.** Both write paths retry on a transient failure, and a
//! transient failure includes the case where the server committed the rows but
//! the client never saw the ack — so a retry must not be allowed to write them
//! twice. `insertAll` gets a deterministic per-row `insertId` so BigQuery's own
//! row-level dedup catches the repeat; the Storage Write path appends at an
//! explicit offset on a committed stream, which the server rejects as
//! `ALREADY_EXISTS` rather than re-appending. Independently, the incremental
//! `MERGE` deduplicates its staging input by `key` (see `build_merge_sql`), so a
//! duplicate that reaches staging by any route still can't reach the
//! destination. All three were absent before: `insertId` was `None`, appends
//! went to the offset-less `_default` stream, and the `MERGE` read staging
//! directly — the previously-documented claim that "idempotency comes from the
//! staging + MERGE flow" held only for keys already present in `dest`, and
//! never for the pure-insert case that a first-time-seen id always is.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use base64::Engine;
use google_cloud_bigquery::client::Client;
use google_cloud_bigquery::http::error::Error as BqError;
use google_cloud_bigquery::http::job::get::GetJobRequest;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::http::job::{
    Job, JobConfiguration, JobConfigurationQuery, JobReference, JobState, JobType,
};
use google_cloud_bigquery::http::table::{
    Clustering, Table, TableFieldMode, TableFieldSchema, TableFieldType, TableReference,
    TableSchema, TimePartitionType, TimePartitioning,
};
use google_cloud_bigquery::http::tabledata::insert_all::{InsertAllRequest, Row as InsertRow};
use google_cloud_bigquery::query::row::Row as QueryRow;
use google_cloud_bigquery::storage_write::stream::committed::CommittedStream;
use google_cloud_bigquery::storage_write::AppendRowsRequestBuilder;
use prost_types::DescriptorProto;
use serde_json::Value;

use async_trait::async_trait;

use crate::config::{BigQueryDestConfig, BigQueryWriteMethod, TransferConfig};
use crate::error::{EtlError, Result};
use crate::sink::bigquery_proto::{build_proto_descriptor, encode_row, resolve_fields};
use crate::sink::{backoff_delay, SendError, Sink, MAX_INSERT_ATTEMPTS};
use crate::types::bigquery::arrow_to_bigquery_type;
use crate::types::ColumnType;

/// insertAll accepts up to 10,000 rows / ~10MB per request; chunk well under
/// both (byte size isn't pre-computed, so the row-count cap alone is the
/// guard — conservative but simple for v1).
const INSERT_ALL_MAX_ROWS_PER_REQUEST: usize = 5_000;

/// The Storage Write API `AppendRows` request has a ~10MB limit. Protobuf rows
/// are far more compact than insertAll's JSON, so this row cap leaves ample
/// headroom for typical rows (byte size isn't pre-computed, same as insertAll).
const STORAGE_WRITE_MAX_ROWS_PER_APPEND: usize = 5_000;

#[derive(Clone)]
pub struct BigQuerySink {
    client: Client,
    project_id: String,
    dataset_id: String,
    write_method: BigQueryWriteMethod,
    /// Per-sink token making this sink's `insertId`s distinct from any other
    /// process/run writing the same table (a nanosecond timestamp, same
    /// approach as `unique_job_id`).
    run_token: String,
    /// Hands out a distinct id-space to each write call, so two concurrent
    /// partitions can't mint the same `insertId` for different rows. Only ever
    /// incremented, never reset — a retry reuses the value it already took.
    insert_id_epoch: Arc<AtomicU64>,
}

impl BigQuerySink {
    /// Authenticate and hold a client for the lifetime of the sink (unlike
    /// the read side's `BigQuerySource::connect`, which reconnects fresh
    /// each call — here we make far more calls per transfer: schema
    /// creation, many inserts, the swap job, watermark read/persist — so a
    /// held client amortizes the auth handshake).
    pub async fn new(cfg: BigQueryDestConfig) -> Result<Self> {
        let (config, resolved_project) = crate::source::bigquery::resolve_bq_client_config(
            &cfg.credentials_json,
            &cfg.credentials_file,
        )
        .await?;
        let project_id = cfg
            .project_id
            .clone()
            .or(resolved_project)
            .ok_or_else(|| EtlError::config("bigquery project_id could not be resolved from credentials; pass project_id explicitly"))?;
        let client = Client::new(config)
            .await
            .map_err(|e| EtlError::other(format!("bigquery client error: {e}")))?;
        Ok(Self {
            client,
            project_id,
            dataset_id: cfg.dataset_id,
            write_method: cfg.write_method,
            run_token: time::OffsetDateTime::now_utc()
                .unix_timestamp_nanos()
                .to_string(),
            insert_id_epoch: Arc::new(AtomicU64::new(0)),
        })
    }

    pub async fn table_exists(&self, table: &str) -> Result<bool> {
        match self
            .client
            .table()
            .get(&self.project_id, &self.dataset_id, table)
            .await
        {
            Ok(_) => Ok(true),
            Err(e) if is_not_found(&e) => Ok(false),
            Err(e) => Err(EtlError::other(format!("bigquery table get error: {e}"))),
        }
    }

    /// Committed row count from free table metadata (`numRows`); `None` if the
    /// table doesn't exist. May lag the streaming buffer — diagnostic only.
    pub async fn current_row_count(&self, table: &str) -> Result<Option<u64>> {
        match self
            .client
            .table()
            .get(&self.project_id, &self.dataset_id, table)
            .await
        {
            Ok(t) => Ok(Some(t.num_rows)),
            Err(e) if is_not_found(&e) => Ok(None),
            Err(e) => Err(EtlError::other(format!("bigquery table get error: {e}"))),
        }
    }

    /// Build and run this destination's own structured `Table` creation —
    /// no DDL string templating, unlike ClickHouse (BigQuery's REST API
    /// takes a schema object directly).
    pub async fn create_table(
        &self,
        table: &str,
        columns: &[ColumnType],
        cfg: &TransferConfig,
    ) -> Result<()> {
        let t = build_table(&self.project_id, &self.dataset_id, table, columns, cfg)?;
        tracing::debug!(
            "creating BigQuery table {}.{}.{}",
            t.table_reference.project_id,
            t.table_reference.dataset_id,
            t.table_reference.table_id
        );
        self.client
            .table()
            .create(&t)
            .await
            .map_err(|e| EtlError::other(format!("bigquery table create error: {e}")))?;
        Ok(())
    }

    /// `CREATE TABLE new_table LIKE like_table` — a structure-only clone
    /// (schema, partitioning, clustering; no data, and no row-level SELECT
    /// the way `CREATE TABLE ... AS SELECT` would run). For **full-refresh**,
    /// correctness doesn't depend on this for BigQuery specifically
    /// (`atomic_swap` above already preserves `dest`'s own DDL via TRUNCATE,
    /// unlike ClickHouse's swap — see the module docs), but a staging table
    /// built this way still means both destinations share the same "staging
    /// mirrors dest" contract in `sync::prepare_target` rather than one being
    /// a special case. For **incremental**, this one *is* load-bearing:
    /// BigQuery is the only destination whose incremental mode stages at all
    /// (`requires_staging_for_incremental`), and staging feeds a
    /// column-to-column `MERGE` — if staging's types were instead rebuilt
    /// fresh from this run's resolved source schema, they could drift from
    /// whatever `type_overrides`/`column_transform_types` the destination was
    /// actually created with on an earlier run.
    pub async fn clone_table_structure(&self, new_table: &str, like_table: &str) -> Result<()> {
        let query = format!(
            "CREATE TABLE `{}`.`{}`.`{}` LIKE `{}`.`{}`.`{}`",
            self.project_id,
            self.dataset_id,
            new_table,
            self.project_id,
            self.dataset_id,
            like_table,
        );
        let job = Job {
            job_reference: JobReference {
                project_id: self.project_id.clone(),
                job_id: unique_job_id("clone", new_table),
                location: None,
            },
            configuration: JobConfiguration {
                job: JobType::Query(JobConfigurationQuery {
                    query,
                    use_legacy_sql: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        };
        let created = self
            .client
            .job()
            .create(&job)
            .await
            .map_err(|e| EtlError::other(format!("bigquery clone-table job error: {e}")))?;
        self.poll_job_until_done(created).await?;
        Ok(())
    }

    /// Insert Arrow batches, dispatching on the configured write method:
    /// `insertAll` (default) or the Storage Write API (opt-in). Both share the
    /// transient-failure retry/backoff policy of the ClickHouse sink.
    pub async fn insert_batches(
        &self,
        table: &str,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<u64> {
        match self.write_method {
            BigQueryWriteMethod::InsertAll => self.insert_batches_insert_all(table, batches).await,
            BigQueryWriteMethod::StorageWrite => {
                self.insert_batches_storage_write(table, schema, batches)
                    .await
            }
        }
    }

    /// Insert Arrow batches via `tabledata.insertAll`, chunked to stay under
    /// its per-request row limit. Returns an approximate JSON-payload-bytes-sent
    /// count (an accounting detail, not exact).
    async fn insert_batches_insert_all(&self, table: &str, batches: &[RecordBatch]) -> Result<u64> {
        if batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(0);
        }
        // A distinct id-space for this call, so concurrent partitions never
        // mint the same `insertId` for different rows.
        let epoch = self.insert_id_epoch.fetch_add(1, Ordering::Relaxed);
        let mut row_ordinal = 0u64;
        let mut total_bytes = 0u64;
        for batch in batches {
            let mut start = 0usize;
            while start < batch.num_rows() {
                let end = (start + INSERT_ALL_MAX_ROWS_PER_REQUEST).min(batch.num_rows());
                // Give every row a deterministic `insertId`: stable across the
                // retries of *this* request (so BigQuery's own best-effort
                // row-level dedup discards a duplicate delivery from a retry
                // whose original the server had already committed) and distinct
                // between different rows (so nothing legitimate is dropped).
                // Previously `None`, which disabled that dedup entirely and
                // made a lost ack on a committed insert duplicate rows.
                let rows = (start..end)
                    .map(|r| {
                        let id = format!("{}-{epoch}-{row_ordinal}", self.run_token);
                        row_ordinal += 1;
                        batch_row_to_json(batch, r).map(|json| InsertRow {
                            insert_id: Some(id),
                            json: Value::Object(json),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let request = InsertAllRequest {
                    rows,
                    ..Default::default()
                };
                total_bytes += serde_json::to_vec(&request)
                    .map(|b| b.len() as u64)
                    .unwrap_or(0);

                let mut attempt = 0u32;
                loop {
                    attempt += 1;
                    match self.try_insert(table, &request).await {
                        Ok(()) => break,
                        Err(SendError::Permanent(e)) => return Err(e),
                        Err(SendError::Transient(e)) => {
                            if attempt >= MAX_INSERT_ATTEMPTS {
                                return Err(EtlError::other(format!(
                                    "bigquery insert into {}.{table} failed after {attempt} attempts: {e}",
                                    self.dataset_id
                                )));
                            }
                            let delay = backoff_delay(attempt);
                            tracing::warn!(
                                "bigquery insert into {}.{table} attempt {attempt} failed ({e}); retrying in {:?}",
                                self.dataset_id,
                                delay
                            );
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
                start = end;
            }
        }
        Ok(total_bytes)
    }

    /// One insertAll attempt: send the request and classify the outcome —
    /// transport/5xx/429 failures and per-row `insertErrors` (a schema
    /// mismatch BigQuery rejected outright) are both surfaced, the former
    /// retried, the latter not.
    async fn try_insert(
        &self,
        table: &str,
        request: &InsertAllRequest<Value>,
    ) -> std::result::Result<(), SendError> {
        let response = self
            .client
            .tabledata()
            .insert(&self.project_id, &self.dataset_id, table, request)
            .await
            .map_err(|e| classify_bq_error(e, "bigquery insertAll"))?;
        if let Some(errors) = response.insert_errors {
            if !errors.is_empty() {
                let detail = errors
                    .iter()
                    .map(|e| {
                        let reasons = e
                            .errors
                            .iter()
                            .map(|m| format!("{}: {}", m.reason, m.message))
                            .collect::<Vec<_>>()
                            .join("; ");
                        format!("row {}: {reasons}", e.index)
                    })
                    .collect::<Vec<_>>()
                    .join(" | ");
                return Err(SendError::Permanent(EtlError::other(format!(
                    "bigquery insertAll rejected {} row(s) into {}.{table}: {detail}",
                    errors.len(),
                    self.dataset_id
                ))));
            }
        }
        Ok(())
    }

    /// Insert Arrow batches via the BigQuery Storage Write API (opt-in). Each
    /// row is protobuf-encoded (see [`super::bigquery_proto`]) and appended with
    /// an explicit **offset** to a per-call *committed* stream, which is what
    /// makes the write exactly-once: an append carrying an offset the stream has
    /// already accepted is rejected as `ALREADY_EXISTS` instead of being written
    /// a second time, so retrying a request the server had already committed
    /// (but whose ack was lost) is a no-op rather than a duplicate.
    ///
    /// This used to append to the table's `_default` stream, which cannot carry
    /// offsets — so it was at-least-once at the row level, and a retry after a
    /// lost ack silently duplicated up to a whole append's worth of rows into
    /// staging. For an incremental sync those duplicates then reached the
    /// destination permanently (the `MERGE`'s `WHEN NOT MATCHED THEN INSERT`
    /// fires per source row, and BigQuery only rejects a *target* row matched
    /// more than once); for a full refresh they survived the swap. The module
    /// comment claiming "idempotency comes from the staging + MERGE flow" was
    /// only ever true for keys already present in the destination — never for
    /// the pure-insert case.
    ///
    /// One stream is created and finalized per call, so offsets are local to
    /// this call and strictly sequential, and concurrent uploads stay fully
    /// parallel (each gets its own stream — BigQuery's documented
    /// one-stream-per-writer pattern) rather than serializing behind a shared
    /// offset counter. At the default `batch_rows`, that's one stream per
    /// 100k rows written.
    ///
    /// Returns the total encoded protobuf bytes sent.
    async fn insert_batches_storage_write(
        &self,
        table: &str,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<u64> {
        if batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(0);
        }
        let fields = resolve_fields(&schema)?;
        let descriptor = build_proto_descriptor(&fields)?;
        // An application-created COMMITTED stream: rows are queryable as soon
        // as they're appended (like `_default`), but unlike `_default` it
        // accepts an explicit offset, which is the whole basis of the
        // exactly-once retry behaviour documented above.
        let resource = format!(
            "projects/{}/datasets/{}/tables/{table}",
            self.project_id, self.dataset_id
        );
        let stream = self
            .client
            .committed_storage_writer()
            .create_write_stream(&resource)
            .await
            .map_err(|e| {
                EtlError::other(format!(
                    "bigquery storage-write: open committed stream for {resource}: {e}"
                ))
            })?;

        let mut total_bytes = 0u64;
        // Next offset to write, in rows, relative to this stream's start.
        let mut offset = 0i64;
        for batch in batches {
            let mut start = 0usize;
            while start < batch.num_rows() {
                let end = (start + STORAGE_WRITE_MAX_ROWS_PER_APPEND).min(batch.num_rows());
                let mut serialized = Vec::with_capacity(end - start);
                for r in start..end {
                    let mut buf = Vec::new();
                    encode_row(batch, r, &fields, &mut buf)?;
                    total_bytes += buf.len() as u64;
                    serialized.push(buf);
                }
                let rows_in_append = (end - start) as i64;

                let mut attempt = 0u32;
                loop {
                    attempt += 1;
                    // Clone per attempt: retries are rare, and the builder
                    // consumes the row bytes. The offset is deliberately NOT
                    // advanced between attempts — resending the same rows at
                    // the same offset is exactly what lets the server reject
                    // the duplicate instead of appending it again.
                    match self
                        .try_append(&stream, &descriptor, serialized.clone(), table, offset)
                        .await
                    {
                        Ok(()) => break,
                        Err(SendError::Permanent(e)) => return Err(e),
                        Err(SendError::Transient(e)) => {
                            if attempt >= MAX_INSERT_ATTEMPTS {
                                return Err(EtlError::other(format!(
                                    "bigquery storage-write append into {}.{table} failed after {attempt} attempts: {e}",
                                    self.dataset_id
                                )));
                            }
                            let delay = backoff_delay(attempt);
                            tracing::warn!(
                                "bigquery storage-write append into {}.{table} attempt {attempt} failed ({e}); retrying in {:?}",
                                self.dataset_id,
                                delay
                            );
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
                offset += rows_in_append;
                start = end;
            }
        }
        // Close the stream. Not required for the rows to be visible (a
        // committed stream's appends are queryable immediately), but it releases
        // the handle instead of leaving it to expire, and it reports back how
        // many rows the stream actually holds — a free end-to-end check that
        // the offsets did what they're supposed to.
        match stream.finalize().await {
            Ok(row_count) if row_count != offset => tracing::warn!(
                "bigquery storage-write into {}.{table}: stream finalized with {row_count} row(s) \
                 but {offset} were appended — offsets may not have deduplicated a retry as \
                 expected; check the destination for duplicate or missing rows",
                self.dataset_id
            ),
            Ok(_) => {}
            // A finalize failure loses nothing: every append was already
            // committed, so this is cleanup, not correctness.
            Err(e) => tracing::warn!(
                "bigquery storage-write into {}.{table}: finalizing the write stream failed \
                 ({e}); rows were already committed, so this is harmless — the stream handle \
                 will expire on its own",
                self.dataset_id
            ),
        }
        Ok(total_bytes)
    }

    /// One `AppendRows` attempt: append the chunk at `offset` and drain the
    /// response stream, surfacing both transport failures and in-band append/row
    /// errors classified for retry (transient) vs. immediate failure
    /// (permanent).
    ///
    /// `ALREADY_EXISTS` is reported as success, not an error: it's precisely the
    /// signal that a previous attempt at this offset *did* land (its ack was
    /// lost in transit), so the rows are committed and re-appending them would
    /// be the duplication this offset exists to prevent.
    async fn try_append(
        &self,
        stream: &CommittedStream,
        descriptor: &DescriptorProto,
        rows: Vec<Vec<u8>>,
        table: &str,
        offset: i64,
    ) -> std::result::Result<(), SendError> {
        let builder = AppendRowsRequestBuilder::new(descriptor.clone(), rows).with_offset(offset);
        let mut resp = match stream.append_rows(vec![builder]).await {
            Ok(resp) => resp,
            Err(s) if s.code() == google_cloud_gax::grpc::Code::AlreadyExists => {
                tracing::info!(
                    "bigquery storage-write into {}.{table}: offset {offset} was already \
                     committed by an earlier attempt; not re-appending",
                    self.dataset_id
                );
                return Ok(());
            }
            Err(s) => return Err(classify_status(&s, "bigquery storage-write append")),
        };
        while let Some(msg) = resp
            .message()
            .await
            .map_err(|s| classify_status(&s, "bigquery storage-write stream"))?
        {
            if !msg.row_errors.is_empty() {
                let detail = msg
                    .row_errors
                    .iter()
                    .map(|e| format!("row {}: {}", e.index, e.message))
                    .collect::<Vec<_>>()
                    .join(" | ");
                return Err(SendError::Permanent(EtlError::other(format!(
                    "bigquery storage-write rejected {} row(s) into {}.{table}: {detail}",
                    msg.row_errors.len(),
                    self.dataset_id
                ))));
            }
            if let Some(response) = msg.response {
                use google_cloud_googleapis::cloud::bigquery::storage::v1::append_rows_response::Response;
                if let Response::Error(status) = response {
                    // Same reasoning as the transport-level arm above, for the
                    // in-band form of the code: these rows are already
                    // committed at this offset.
                    if status.code == RPC_CODE_ALREADY_EXISTS {
                        tracing::info!(
                            "bigquery storage-write into {}.{table}: offset {offset} was already \
                             committed by an earlier attempt; not re-appending",
                            self.dataset_id
                        );
                        return Ok(());
                    }
                    return Err(classify_rpc_status(&status, table, &self.dataset_id));
                }
            }
        }
        Ok(())
    }

    /// Atomically replace `dest`'s contents with `staging`'s via a
    /// `TRUNCATE TABLE` + `INSERT ... SELECT` wrapped in an explicit
    /// multi-statement transaction — see the module docs for why this, not a
    /// `WRITE_TRUNCATE` copy job (which silently drops rows still in
    /// `staging`'s streaming buffer) or `CREATE OR REPLACE TABLE ... AS
    /// SELECT` (which would drop `dest`'s partitioning/clustering).
    pub async fn atomic_swap(
        &self,
        dest: &str,
        staging: &str,
        columns: &[ColumnType],
    ) -> Result<()> {
        let query = build_swap_sql(&self.project_id, &self.dataset_id, dest, staging, columns);
        let job = Job {
            job_reference: JobReference {
                project_id: self.project_id.clone(),
                job_id: unique_job_id("swap", dest),
                location: None,
            },
            configuration: JobConfiguration {
                job: JobType::Query(JobConfigurationQuery {
                    query,
                    use_legacy_sql: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        };
        let created = self
            .client
            .job()
            .create(&job)
            .await
            .map_err(|e| EtlError::other(format!("bigquery swap job error: {e}")))?;
        self.poll_job_until_done(created).await?;
        Ok(())
    }

    /// Upsert `staging`'s rows into `dest`, matched on `key` — BigQuery's
    /// incremental-mode equivalent of ClickHouse's `ReplacingMergeTree`
    /// dedup (see the module docs: BigQuery has no engine-level merge-on-read,
    /// so an updated source row would otherwise land as a duplicate via a
    /// plain `insertAll`). Runs as a `MERGE` DML query job — bills for bytes
    /// scanned in both tables, unlike the free `insertAll` path, but is
    /// naturally idempotent: re-running the same merge (e.g. after a crash,
    /// before the watermark advances) re-applies the same key-matched rows
    /// rather than duplicating them.
    #[allow(clippy::too_many_arguments)]
    pub async fn merge_into(
        &self,
        dest: &str,
        staging: &str,
        key: &[String],
        columns: &[ColumnType],
        prune_partition: Option<&str>,
        prune_key_range: bool,
        prune_key_list_max: usize,
        delete_stale: bool,
        dedup_order: Option<&str>,
    ) -> Result<()> {
        // Optional exact key-list bound, resolved first because it supersedes
        // the range bound on the key columns — knowing whether it binds keeps
        // the range probe below from reading columns nothing will use.
        // Single-column keys only (see
        // `TransferConfig::merge_prune_key_list_max`), and never under
        // `delete_stale`, for the same reason the range bound is skipped there.
        let key_list = match (prune_key_list_max, key.len(), delete_stale) {
            (0, _, _) | (_, _, true) => None,
            (max, 1, false) => match self.probe_staging_key_list(staging, &key[0], max).await {
                Ok(list) => list,
                // Same degradation rule as the range probe below: an
                // optimization that cannot be resolved falls back to the wider
                // bound, it does not fail a run.
                Err(e) => {
                    tracing::warn!(
                        "could not resolve a staging key list for '{dest}' ({e}); \
                         merging with the key-range bound instead"
                    );
                    None
                }
            },
            (_, _, false) => {
                tracing::debug!(
                    "merge_prune_key_list_max is set but '{dest}' merges on a composite key; \
                     using the per-column range bound instead"
                );
                None
            }
        };
        if let Some(list) = &key_list {
            tracing::info!(
                "merging '{dest}' bounded to {} distinct key value(s) instead of a key range",
                list.len()
            );
        }
        // Resolve the prune bounds: they go into the statement as literals,
        // because BigQuery rejects a subquery that references a table inside a
        // join predicate (see `StagingBounds`). Only the columns actually
        // bounded are probed, so a transfer that prunes on nothing makes no
        // extra call at all.
        let bounded = bounded_columns(
            key,
            prune_partition,
            prune_key_range && key_list.is_none(),
            delete_stale,
        );
        let bounds = match self.probe_staging_bounds(staging, &bounded).await {
            Ok(b) => b,
            // Pruning is an optimization, so a failed probe degrades to the
            // unbounded join rather than failing the transfer — except under
            // `delete_stale`, where the bound is the only thing keeping the
            // DELETE inside the batch's window, and quietly dropping it would
            // change what the statement means.
            Err(e) if !delete_stale => {
                tracing::warn!(
                    "could not resolve staging prune bounds for '{dest}' ({e}); \
                     merging without them"
                );
                StagingBounds::default()
            }
            Err(e) => return Err(e),
        };
        let query = build_merge_sql(
            &self.project_id,
            &self.dataset_id,
            dest,
            staging,
            key,
            columns,
            prune_partition,
            prune_key_range,
            delete_stale,
            dedup_order,
            &bounds,
            key_list.as_deref(),
        )?;
        self.run_query_job(query, "merge", dest).await?;
        Ok(())
    }

    /// Resolve the staging batch's `[MIN, MAX]` on each of `columns`, as
    /// GoogleSQL literals ready to paste into the `MERGE`'s predicates — see
    /// [`StagingBounds`] for why the bound cannot be a subquery.
    ///
    /// One small query over the staging table (only the bounded columns are
    /// read, and staging holds just the delta). A column whose bound comes back
    /// `NULL` — an empty batch, or a column that is all-`NULL` in it — is left
    /// out of the map, and the caller emits no bound for it.
    async fn probe_staging_bounds(&self, staging: &str, columns: &[&str]) -> Result<StagingBounds> {
        if columns.is_empty() {
            return Ok(StagingBounds::default());
        }
        let query = build_staging_bounds_sql(&self.project_id, &self.dataset_id, staging, columns);
        let request = QueryRequest {
            query,
            ..Default::default()
        };
        let mut iter = self
            .client
            .query::<QueryRow>(&self.project_id, request)
            .await
            .map_err(|e| EtlError::other(format!("bigquery staging bounds query error: {e}")))?;
        // An un-grouped aggregate always returns exactly one row.
        let row = match iter
            .next()
            .await
            .map_err(|e| EtlError::other(format!("bigquery staging bounds row error: {e}")))?
        {
            Some(row) => row,
            None => return Ok(StagingBounds::default()),
        };
        let mut resolved = HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            let read = |idx: usize| {
                row.column::<Option<String>>(idx).map_err(|e| {
                    EtlError::other(format!("bigquery staging bounds column error: {e}"))
                })
            };
            match (read(i * 2)?, read(i * 2 + 1)?) {
                // `FORMAT('%T', NULL)` renders the bare string `NULL`, which is
                // unambiguous: a STRING value of "NULL" renders *with* quotes.
                (Some(lo), Some(hi)) if lo != "NULL" && hi != "NULL" => {
                    resolved.insert((*col).to_string(), (lo, hi));
                }
                _ => tracing::debug!(
                    "staging '{staging}' has no usable range on '{col}' (empty batch \
                     or all-NULL column); merging without a bound on it"
                ),
            }
        }
        Ok(StagingBounds(resolved))
    }

    /// Resolve the staging batch's distinct merge-key values as GoogleSQL
    /// literals, or `None` when a key *list* is not the right bound for this
    /// batch — see `TransferConfig::merge_prune_key_list_max`.
    ///
    /// `None` means "fall back to the range bound", and it covers three cases
    /// that are all normal rather than exceptional: an empty batch, a batch
    /// whose key column is entirely NULL, and a batch holding more distinct
    /// keys than `max`. The over-limit case is detected by reading `max + 1`
    /// rows and finding the extra one, so the limit bounds the generated
    /// statement's size for real instead of being a hint the probe might
    /// exceed.
    ///
    /// Like the range bounds, the values come back through `FORMAT('%T', …)`,
    /// so BigQuery renders every literal — quickhouse formats no types by hand
    /// and cannot get a timestamp precision or a string escape wrong.
    async fn probe_staging_key_list(
        &self,
        staging: &str,
        key: &str,
        max: usize,
    ) -> Result<Option<Vec<String>>> {
        let query = build_staging_key_list_sql(
            &self.project_id,
            &self.dataset_id,
            staging,
            key,
            max.saturating_add(1),
        );
        let request = QueryRequest {
            query,
            ..Default::default()
        };
        let mut iter = self
            .client
            .query::<QueryRow>(&self.project_id, request)
            .await
            .map_err(|e| EtlError::other(format!("bigquery staging key list query error: {e}")))?;
        let mut out = Vec::with_capacity(max.min(1024));
        while let Some(row) = iter
            .next()
            .await
            .map_err(|e| EtlError::other(format!("bigquery staging key list row error: {e}")))?
        {
            let v = row
                .column::<Option<String>>(0)
                .map_err(|e| {
                    EtlError::other(format!("bigquery staging key list column error: {e}"))
                })?
                .unwrap_or_else(|| "NULL".to_string());
            // `FORMAT('%T', NULL)` renders the bare word NULL. The probe
            // already filters NULL keys out, so seeing one means the column
            // is not what we think it is — drop the list rather than emit a
            // predicate containing an unmatchable literal.
            if v == "NULL" {
                return Ok(None);
            }
            out.push(v);
            if out.len() > max {
                tracing::debug!(
                    "staging '{staging}' holds more than {max} distinct '{key}' value(s); \
                     merging with the key-range bound instead of a key list"
                );
                return Ok(None);
            }
        }
        if out.is_empty() {
            return Ok(None);
        }
        Ok(Some(out))
    }

    /// The destination table's clustering columns, in clustering order, or
    /// `None` when it has none / is not visible. Diagnostic: it tells the
    /// caller whether the merge's key bound can prune anything at all.
    pub async fn clustering_columns(&self, table: &str) -> Result<Option<Vec<String>>> {
        let query = format!(
            "SELECT column_name FROM `{}`.`{}`.INFORMATION_SCHEMA.COLUMNS \
             WHERE table_name = '{}' AND clustering_ordinal_position IS NOT NULL \
             ORDER BY clustering_ordinal_position",
            self.project_id,
            self.dataset_id,
            escape_sql_string(table),
        );
        let cols = self.query_strings(&query, "clustering columns").await?;
        Ok(if cols.is_empty() { None } else { Some(cols) })
    }

    /// The declared BigQuery `data_type` of one column, from
    /// `INFORMATION_SCHEMA.COLUMNS`.
    async fn column_data_type(&self, table: &str, column: &str) -> Result<String> {
        let query = format!(
            "SELECT data_type FROM `{}`.`{}`.INFORMATION_SCHEMA.COLUMNS \
             WHERE table_name = '{}' AND column_name = '{}'",
            self.project_id,
            self.dataset_id,
            escape_sql_string(table),
            escape_sql_string(column),
        );
        self.query_strings(&query, "column data type")
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| {
                EtlError::config(format!(
                    "column '{column}' not found on {}.{}.{table}",
                    self.project_id, self.dataset_id
                ))
            })
    }

    /// Run `query` and collect its first column as strings. Used for the small
    /// metadata reads (clustering, column types, key sets) — not a general row
    /// reader.
    async fn query_strings(&self, query: &str, what: &str) -> Result<Vec<String>> {
        let request = QueryRequest {
            query: query.to_string(),
            ..Default::default()
        };
        let mut iter = self
            .client
            .query::<QueryRow>(&self.project_id, request)
            .await
            .map_err(|e| EtlError::other(format!("bigquery {what} query error: {e}")))?;
        let mut out = Vec::new();
        while let Some(row) = iter
            .next()
            .await
            .map_err(|e| EtlError::other(format!("bigquery {what} row error: {e}")))?
        {
            if let Some(v) = row
                .column::<Option<String>>(0)
                .map_err(|e| EtlError::other(format!("bigquery {what} column error: {e}")))?
            {
                out.push(v);
            }
        }
        Ok(out)
    }

    /// Distinct non-NULL values of `key_column`, as text — the destination half
    /// of a `reconcile_keys` diff. `CAST(... AS STRING)` so an INT64 key renders
    /// the same way the source's own text rendering does.
    pub async fn distinct_keys(
        &self,
        table: &str,
        key_column: &str,
        window: Option<&str>,
    ) -> Result<Vec<String>> {
        let query = format!(
            "SELECT DISTINCT CAST(`{key_column}` AS STRING) FROM `{}`.`{}`.`{table}` \
             WHERE {} AND `{key_column}` IS NOT NULL",
            self.project_id,
            self.dataset_id,
            window.unwrap_or("TRUE"),
        );
        self.query_strings(&query, "distinct keys").await
    }

    /// Delete rows whose `key_column` is one of `keys`, inside `window`.
    /// Chunked at [`crate::sink::DELETE_KEY_CHUNK`] keys per statement; each
    /// chunk's affected rows are counted first so the total is exact.
    pub async fn delete_keys(
        &self,
        table: &str,
        key_column: &str,
        keys: &[String],
        window: Option<&str>,
    ) -> Result<u64> {
        if keys.is_empty() {
            return Ok(0);
        }
        let data_type = self.column_data_type(table, key_column).await?;
        let mut deleted = 0u64;
        for chunk in keys.chunks(crate::sink::DELETE_KEY_CHUNK) {
            let lits = bq_key_literals(&data_type, key_column, chunk)?.join(", ");
            let predicate = format!(
                "{} AND `{key_column}` IN ({lits})",
                window.unwrap_or("TRUE")
            );
            let count_sql = format!(
                "SELECT CAST(COUNT(*) AS STRING) FROM `{}`.`{}`.`{table}` WHERE {predicate}",
                self.project_id, self.dataset_id,
            );
            deleted += self
                .query_strings(&count_sql, "delete key count")
                .await?
                .first()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let delete_sql = format!(
                "DELETE FROM `{}`.`{}`.`{table}` WHERE {predicate}",
                self.project_id, self.dataset_id,
            );
            self.run_query_job(delete_sql, "delete", table).await?;
        }
        Ok(deleted)
    }

    /// Submit `query` as a job and wait for it to finish. Shared by the
    /// `MERGE` and the `DELETE` paths so both get the same unique job id and
    /// the same terminal-state polling.
    async fn run_query_job(&self, query: String, prefix: &str, table: &str) -> Result<Job> {
        let job = Job {
            job_reference: JobReference {
                project_id: self.project_id.clone(),
                job_id: unique_job_id(prefix, table),
                location: None,
            },
            configuration: JobConfiguration {
                job: JobType::Query(JobConfigurationQuery {
                    query,
                    use_legacy_sql: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        };
        let created = self
            .client
            .job()
            .create(&job)
            .await
            .map_err(|e| EtlError::other(format!("bigquery {prefix} job error: {e}")))?;
        self.poll_job_until_done(created).await
    }

    /// Idempotent, matching ClickHouse's `DROP TABLE IF EXISTS`: a
    /// not-found is success, not an error.
    pub async fn drop_table(&self, table: &str) -> Result<()> {
        match self
            .client
            .table()
            .delete(&self.project_id, &self.dataset_id, table)
            .await
        {
            Ok(()) => Ok(()),
            Err(e) if is_not_found(&e) => Ok(()),
            Err(e) => Err(EtlError::other(format!("bigquery table delete error: {e}"))),
        }
    }

    /// Add each missing column as `Nullable` via a schema PATCH. Fetches the
    /// live table, mutates its schema in place, and patches it back — so
    /// partitioning, clustering, and the `etag` (optimistic-concurrency
    /// If-Match) all carry through untouched. Returns the names added.
    /// Case-insensitive column matching (BigQuery names are). ADD-only.
    pub async fn add_missing_columns(
        &self,
        table: &str,
        columns: &[ColumnType],
        cfg: &TransferConfig,
    ) -> Result<Vec<String>> {
        let mut t = self
            .client
            .table()
            .get(&self.project_id, &self.dataset_id, table)
            .await
            .map_err(|e| EtlError::other(format!("bigquery table get (evolve) error: {e}")))?;
        let existing = t.schema.take().map(|s| s.fields).unwrap_or_default();
        let (fields, added) = build_evolved_fields(existing, columns, cfg)?;
        t.schema = Some(TableSchema { fields });
        if added.is_empty() {
            return Ok(added);
        }
        self.client
            .table()
            .patch(&t)
            .await
            .map_err(|e| EtlError::other(format!("bigquery schema patch (evolve) error: {e}")))?;
        Ok(added)
    }

    /// Create the internal `_quickhouse_state` watermark-tracking table if
    /// it doesn't exist yet. Unlike ClickHouse's `CREATE TABLE IF NOT
    /// EXISTS`, BigQuery's table creation has no such clause, so the
    /// existence check happens here explicitly.
    pub async fn ensure_state_table(&self, state_table: &str) -> Result<()> {
        if self.table_exists(state_table).await? {
            return Ok(());
        }
        let field = |name: &str, data_type: TableFieldType| TableFieldSchema {
            name: name.to_string(),
            data_type,
            mode: Some(TableFieldMode::Required),
            ..Default::default()
        };
        let t = Table {
            table_reference: TableReference {
                project_id: self.project_id.clone(),
                dataset_id: self.dataset_id.clone(),
                table_id: state_table.to_string(),
            },
            schema: Some(TableSchema {
                fields: vec![
                    field("source_table", TableFieldType::String),
                    field("dest_table", TableFieldType::String),
                    field("last_watermark", TableFieldType::String),
                    field("rows", TableFieldType::Integer),
                    field("run_ts", TableFieldType::Timestamp),
                ],
            }),
            ..Default::default()
        };
        self.client
            .table()
            .create(&t)
            .await
            .map_err(|e| EtlError::other(format!("bigquery state table create error: {e}")))?;
        Ok(())
    }

    /// Read the last persisted watermark for this `(state_key, dest_table)` pair.
    pub async fn read_last_watermark(&self, cfg: &TransferConfig) -> Result<Option<String>> {
        if !self.table_exists(&cfg.state_table_name).await? {
            return Ok(None);
        }
        let source_id = cfg.effective_state_key();
        let query = format!(
            "SELECT last_watermark FROM `{}`.`{}`.`{}` \
             WHERE source_table = '{}' AND dest_table = '{}' \
             ORDER BY run_ts DESC LIMIT 1",
            self.project_id,
            self.dataset_id,
            cfg.state_table_name,
            escape_sql_string(&source_id),
            escape_sql_string(&cfg.dest_table),
        );
        let request = QueryRequest {
            query,
            ..Default::default()
        };
        let mut iter = self
            .client
            .query::<QueryRow>(&self.project_id, request)
            .await
            .map_err(|e| EtlError::other(format!("bigquery query error: {e}")))?;
        match iter
            .next()
            .await
            .map_err(|e| EtlError::other(format!("bigquery row error: {e}")))?
        {
            Some(row) => row
                .column::<Option<String>>(0)
                .map_err(|e| EtlError::other(format!("bigquery column error: {e}"))),
            None => Ok(None),
        }
    }

    /// Persist a new watermark after a successful incremental run, via a
    /// DML `INSERT` run as a query job (BigQuery executes DML through the
    /// same job mechanism as `SELECT`).
    pub async fn persist_watermark(
        &self,
        cfg: &TransferConfig,
        watermark: &str,
        rows: u64,
    ) -> Result<()> {
        let source_id = cfg.effective_state_key();
        let query = build_persist_watermark_sql(
            &self.project_id,
            &self.dataset_id,
            &cfg.state_table_name,
            &source_id,
            &cfg.dest_table,
            watermark,
            rows,
        );
        let job = Job {
            job_reference: JobReference {
                project_id: self.project_id.clone(),
                job_id: unique_job_id("persist_watermark", &cfg.dest_table),
                location: None,
            },
            configuration: JobConfiguration {
                job: JobType::Query(JobConfigurationQuery {
                    query,
                    use_legacy_sql: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        };
        let created =
            self.client.job().create(&job).await.map_err(|e| {
                EtlError::other(format!("bigquery persist_watermark job error: {e}"))
            })?;
        self.poll_job_until_done(created).await?;
        Ok(())
    }

    /// Poll a submitted job (copy or DML query) until it reaches `DONE`,
    /// then surface any job-level failure — same poll-until-done idiom
    /// already used by the read side (`source/bigquery.rs::run_query`).
    async fn poll_job_until_done(&self, mut job: Job) -> Result<Job> {
        while job.status.state != JobState::Done {
            tokio::time::sleep(Duration::from_millis(500)).await;
            job = self
                .client
                .job()
                .get(
                    &job.job_reference.project_id,
                    &job.job_reference.job_id,
                    &GetJobRequest {
                        location: job.job_reference.location.clone(),
                    },
                )
                .await
                .map_err(|e| EtlError::other(format!("bigquery job get error: {e}")))?;
        }
        if let Some(err) = &job.status.error_result {
            return Err(EtlError::other(format!(
                "bigquery job {} failed: {}",
                job.job_reference.job_id,
                err.message.as_deref().unwrap_or("unknown error")
            )));
        }
        Ok(job)
    }
}

/// Thin delegation to the inherent methods above. BigQuery overrides the
/// staging/merge capability (no engine-level dedup) and keeps the default
/// chunk-resume methods (chunked reads are ClickHouse-only, so BigQuery is
/// never asked to persist a chunk cursor).
#[async_trait]
impl Sink for BigQuerySink {
    async fn table_exists(&self, table: &str) -> Result<bool> {
        BigQuerySink::table_exists(self, table).await
    }
    async fn create_table(
        &self,
        table: &str,
        columns: &[ColumnType],
        cfg: &TransferConfig,
    ) -> Result<()> {
        BigQuerySink::create_table(self, table, columns, cfg).await
    }
    async fn clone_table_structure(&self, new_table: &str, like_table: &str) -> Result<()> {
        BigQuerySink::clone_table_structure(self, new_table, like_table).await
    }
    async fn insert_batches(
        &self,
        table: &str,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<u64> {
        BigQuerySink::insert_batches(self, table, schema, batches).await
    }
    async fn atomic_swap(&self, dest: &str, staging: &str, columns: &[ColumnType]) -> Result<()> {
        BigQuerySink::atomic_swap(self, dest, staging, columns).await
    }
    async fn current_row_count(&self, table: &str) -> Result<Option<u64>> {
        BigQuerySink::current_row_count(self, table).await
    }
    async fn drop_table(&self, table: &str) -> Result<()> {
        BigQuerySink::drop_table(self, table).await
    }
    async fn ensure_state_table(&self, state_table: &str) -> Result<()> {
        BigQuerySink::ensure_state_table(self, state_table).await
    }
    async fn read_last_watermark(&self, cfg: &TransferConfig) -> Result<Option<String>> {
        BigQuerySink::read_last_watermark(self, cfg).await
    }
    async fn persist_watermark(
        &self,
        cfg: &TransferConfig,
        watermark: &str,
        rows: u64,
    ) -> Result<()> {
        BigQuerySink::persist_watermark(self, cfg, watermark, rows).await
    }
    async fn add_missing_columns(
        &self,
        table: &str,
        columns: &[ColumnType],
        cfg: &TransferConfig,
    ) -> Result<Vec<String>> {
        BigQuerySink::add_missing_columns(self, table, columns, cfg).await
    }
    fn dest_kind(&self) -> crate::config::DestKind {
        crate::config::DestKind::BigQuery
    }
    fn namespace(&self) -> &str {
        &self.dataset_id
    }
    fn requires_staging_for_incremental(&self) -> bool {
        true
    }
    fn full_refresh_references_dest_columns(&self) -> bool {
        true
    }
    async fn merge_into(
        &self,
        dest: &str,
        staging: &str,
        key: &[String],
        columns: &[ColumnType],
        prune_partition: Option<&str>,
        prune_key_range: bool,
        prune_key_list_max: usize,
        delete_stale: bool,
        dedup_order: Option<&str>,
    ) -> Result<()> {
        BigQuerySink::merge_into(
            self,
            dest,
            staging,
            key,
            columns,
            prune_partition,
            prune_key_range,
            prune_key_list_max,
            delete_stale,
            dedup_order,
        )
        .await
    }
    fn supports_row_delete(&self) -> bool {
        true
    }
    /// BigQuery expresses the window-scoped delete as a
    /// `WHEN NOT MATCHED BY SOURCE` clause inside its own `MERGE`, so the sync
    /// path must not run a separate delete statement after it.
    fn deletes_stale_within_merge(&self) -> bool {
        true
    }
    async fn clustering_columns(&self, table: &str) -> Result<Option<Vec<String>>> {
        BigQuerySink::clustering_columns(self, table).await
    }
    async fn distinct_keys(
        &self,
        table: &str,
        key_column: &str,
        window: Option<&str>,
    ) -> Result<Vec<String>> {
        BigQuerySink::distinct_keys(self, table, key_column, window).await
    }
    async fn delete_keys(
        &self,
        table: &str,
        key_column: &str,
        keys: &[String],
        window: Option<&str>,
    ) -> Result<u64> {
        BigQuerySink::delete_keys(self, table, key_column, keys, window).await
    }
}

fn is_not_found(e: &BqError) -> bool {
    matches!(e, BqError::Response(r) if r.code == 404)
}

/// Classify an insertAll failure so the retry loop knows whether to retry:
/// transport failures and 5xx/429 are transient, everything else (4xx,
/// e.g. bad schema/auth) is deterministic.
fn classify_bq_error(e: BqError, context: &str) -> SendError {
    let transient = match &e {
        BqError::Response(r) => r.code >= 500 || r.code == 429,
        BqError::HttpClient(_) | BqError::HttpMiddleware(_) => true,
        BqError::TokenSource(_) => false,
    };
    let err = EtlError::other(format!("{context}: {e}"));
    if transient {
        SendError::Transient(err)
    } else {
        SendError::Permanent(err)
    }
}

/// Classify a gRPC transport `Status` (from `append_rows`/stream draining):
/// unavailable/internal/aborted/exhausted/deadline are transient (retry),
/// everything else (e.g. `INVALID_ARGUMENT`, auth) is deterministic.
fn classify_status(status: &google_cloud_gax::grpc::Status, context: &str) -> SendError {
    use google_cloud_gax::grpc::Code;
    let err = EtlError::other(format!(
        "{context}: {} ({:?})",
        status.message(),
        status.code()
    ));
    match status.code() {
        Code::Unavailable
        | Code::Internal
        | Code::Aborted
        | Code::DeadlineExceeded
        | Code::ResourceExhausted => SendError::Transient(err),
        _ => SendError::Permanent(err),
    }
}

/// Classify an in-band `google.rpc.Status` returned inside an
/// `AppendRowsResponse` (a per-append error, distinct from a transport
/// failure). Codes are `google.rpc.Code` integers: INTERNAL(13),
/// UNAVAILABLE(14), ABORTED(10), RESOURCE_EXHAUSTED(8), DEADLINE_EXCEEDED(4)
/// are transient; everything else (e.g. INVALID_ARGUMENT(3)) is deterministic.
/// `google.rpc.Code.ALREADY_EXISTS`. Returned by an `AppendRows` carrying an
/// offset the stream has already accepted — i.e. proof that an earlier attempt
/// committed, so it's handled as success rather than classified as an error.
const RPC_CODE_ALREADY_EXISTS: i32 = 6;

fn classify_rpc_status(
    status: &google_cloud_googleapis::rpc::Status,
    table: &str,
    dataset: &str,
) -> SendError {
    let err = EtlError::other(format!(
        "bigquery storage-write append error into {dataset}.{table}: code {} {}",
        status.code, status.message
    ));
    match status.code {
        13 | 14 | 10 | 8 | 4 => SendError::Transient(err),
        _ => SendError::Permanent(err),
    }
}

/// A job ID unique enough in practice (nanosecond timestamp + a sanitized
/// table name), matching this crate's own test-suite convention of deriving
/// job IDs from a timestamp. Job IDs may only contain letters/digits/`_`/`-`.
fn unique_job_id(prefix: &str, table: &str) -> String {
    let sanitized: String = table
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    format!(
        "quickhouse_{prefix}_{sanitized}_{}",
        time::OffsetDateTime::now_utc().unix_timestamp_nanos()
    )
}

/// GoogleSQL string literal escaping (used for the hand-built watermark
/// state queries — matches `sync.rs`'s `escape_bigquery_string`, kept in sync
/// deliberately). GoogleSQL only recognizes backslash-based escapes — unlike
/// ANSI SQL's doubled-quote convention (`''`), a doubled single quote is NOT
/// an escaped quote in BigQuery and is rejected as a syntax error (a
/// well-documented real-world gotcha porting ANSI-style SQL generation to
/// BigQuery, e.g. https://github.com/trinodb/trino/issues/7784). Backslash
/// must be escaped first, or a trailing backslash in a value would escape
/// the literal's closing quote instead of terminating the string.
///
/// Newlines and carriage returns must be escaped too: a *quoted* (non-triple)
/// GoogleSQL literal cannot contain a raw newline — it terminates the literal
/// mid-string and the statement fails to parse. This matters because the value
/// interpolated here is `effective_state_key()`, which falls back to the raw
/// `source_query` text when no explicit `state_key` is set, so a perfectly
/// ordinary multi-line user query would otherwise break every watermark
/// read/persist. (The failure is confusingly ordered: `read_last_watermark`
/// short-circuits to `Ok(None)` while the state table doesn't exist, so run 1
/// succeeds and only run 2 onward fails.)
fn escape_sql_string(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\'', "\\'")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

/// Build the `TRUNCATE` + `INSERT ... SELECT` transaction that atomically
/// replaces `dest`'s contents with `staging`'s — see `BigQuerySink::
/// atomic_swap`'s docs for why this, not a copy job. `TRUNCATE TABLE` is DDL
/// that preserves `dest`'s schema/partitioning/clustering (unlike `CREATE OR
/// REPLACE`) and is free (no bytes scanned); only the `INSERT ... SELECT`
/// bills, for reading `staging` once. Wrapping both in an explicit
/// transaction keeps the swap atomic: if the `INSERT` fails, `TRUNCATE`
/// rolls back too and `dest` is left untouched. A free function (not a
/// `&self` method) so it's unit-testable without a real authenticated
/// client, mirroring `build_merge_sql`/`build_table`.
fn build_swap_sql(
    project_id: &str,
    dataset_id: &str,
    dest: &str,
    staging: &str,
    columns: &[ColumnType],
) -> String {
    let dest_ref = format!("`{project_id}`.`{dataset_id}`.`{dest}`");
    let staging_ref = format!("`{project_id}`.`{dataset_id}`.`{staging}`");
    let col_list = columns
        .iter()
        .map(|c| format!("`{}`", c.name))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "BEGIN TRANSACTION; \
         TRUNCATE TABLE {dest_ref}; \
         INSERT INTO {dest_ref} ({col_list}) SELECT {col_list} FROM {staging_ref}; \
         COMMIT TRANSACTION;"
    )
}

/// Build the `INSERT` that records a new watermark in `_quickhouse_state` —
/// see `BigQuerySink::persist_watermark`'s docs. A free function (not a
/// `&self` method) so it's unit-testable without a real authenticated client,
/// mirroring `build_swap_sql`/`build_merge_sql`. `` `rows` `` is backtick-quoted
/// because it's a reserved GoogleSQL keyword (window-framing syntax, e.g.
/// `ROWS BETWEEN ... PRECEDING`) — every other bare identifier here is a
/// column name that happens not to collide with a keyword.
#[allow(clippy::too_many_arguments)]
fn build_persist_watermark_sql(
    project_id: &str,
    dataset_id: &str,
    state_table: &str,
    source_id: &str,
    dest_table: &str,
    watermark: &str,
    rows: u64,
) -> String {
    format!(
        "INSERT INTO `{project_id}`.`{dataset_id}`.`{state_table}` \
         (source_table, dest_table, last_watermark, `rows`, run_ts) \
         VALUES ('{}', '{}', '{}', {rows}, CURRENT_TIMESTAMP())",
        escape_sql_string(source_id),
        escape_sql_string(dest_table),
        escape_sql_string(watermark),
    )
}

/// Literal `[MIN, MAX]` bounds over a staging table, per column, each already
/// rendered as a GoogleSQL constant (`42`, `"abc"`, `DATE "2026-08-20"`,
/// `TIMESTAMP "2026-08-19 21:53:01.234567+00"`, …).
///
/// **Why literals.** Both `MERGE` prunes used to express their bound as
/// `T.c BETWEEN (SELECT MIN(c) FROM staging) AND (SELECT MAX(c) FROM staging)`.
/// BigQuery rejects that at *analysis* time — `Unsupported subquery with table
/// in join predicate` — so it failed every merge on every table regardless of
/// the data, including no-op runs that staged zero rows. A constant is legal
/// there, and is also the form BigQuery can actually prune partitions and
/// clustering blocks on, which was the point of the feature.
///
/// **Why BigQuery renders them, not us.** The bounds are produced by
/// `FORMAT('%T', …)` in [`build_staging_bounds_sql`], which returns each value
/// in valid constant syntax for its own type. That keeps two things out of
/// quickhouse: per-type literal formatting (timestamp precision, `NUMERIC`
/// scale, string escaping), and any assumption that Rust and GoogleSQL order
/// values the same way — the `MIN`/`MAX` that picks the bound is evaluated by
/// the same engine, over the same staging table, as the `BETWEEN` that uses it.
///
/// **Why resolving them in a separate query is sound.** The bound is read before
/// the `MERGE` runs, so a staging table still being written between the two
/// would leave it stale, and a matched destination row outside a stale bound
/// would fall through to `WHEN NOT MATCHED` and insert a duplicate key. Neither
/// is reachable: `sync::staging_name` gives every run its own staging table
/// (nanosecond run id), and that table is fully written before the promote step
/// calls `merge_into` — so no other run, and no later append from this one, can
/// move the range under the statement.
#[derive(Debug, Default, Clone)]
struct StagingBounds(HashMap<String, (String, String)>);

impl StagingBounds {
    /// The `(lo, hi)` literals for `column`, or `None` when the staging batch
    /// yielded no usable range on it — in which case the caller emits no bound,
    /// which is always safe (an absent bound only widens the scan).
    fn get(&self, column: &str) -> Option<&(String, String)> {
        self.0.get(column)
    }
}

/// Which columns the `MERGE` needs a staging range for, in probe order: the
/// immutable prune column if one is configured, plus every key column when
/// key-range pruning applies. Deduplicated, because the prune column is allowed
/// to also be a key column. An empty result means no probe is needed at all.
///
/// Mirrors the conditions [`build_merge_sql`] emits bounds under, so the two
/// cannot drift into probing a column that is never bounded (a wasted read) or
/// bounding one that was never probed (a silently dropped bound).
fn bounded_columns<'a>(
    key: &'a [String],
    prune_partition: Option<&'a str>,
    prune_key_range: bool,
    delete_stale: bool,
) -> Vec<&'a str> {
    let mut cols: Vec<&str> = Vec::new();
    if let Some(pcol) = prune_partition {
        cols.push(pcol);
    }
    if prune_key_range && !delete_stale {
        for k in key {
            if !cols.contains(&k.as_str()) {
                cols.push(k);
            }
        }
    }
    cols
}

/// Build the probe behind [`StagingBounds`]: `MIN`/`MAX` per bounded column,
/// wrapped in `FORMAT('%T', …)` so BigQuery hands back each bound already in
/// literal syntax. Aliases are positional (`lo_0`/`hi_0`, …) so results are
/// read back by index and an awkward column name can't collide with them. A
/// free function (not a `&self` method) so it's unit-testable without a real
/// authenticated client, mirroring `build_merge_sql`.
fn build_staging_bounds_sql(
    project_id: &str,
    dataset_id: &str,
    staging: &str,
    columns: &[&str],
) -> String {
    let projection = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            format!("FORMAT('%T', MIN(`{c}`)) AS lo_{i}, FORMAT('%T', MAX(`{c}`)) AS hi_{i}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {projection} FROM `{project_id}`.`{dataset_id}`.`{staging}`")
}

/// Build the probe behind the key-list prune: up to `limit` distinct non-NULL
/// values of `key` in the staging batch, each already rendered as a GoogleSQL
/// literal by `FORMAT('%T', …)` — the same mechanism the range bounds use, for
/// the same reason (BigQuery formats its own literals; quickhouse never has to
/// quote a value or pick a timestamp precision).
///
/// `limit` is the caller's `merge_prune_key_list_max + 1`: reading one row past
/// the ceiling is how the caller learns the batch exceeded it without counting
/// the whole table. The `DISTINCT` is inside the subquery so the `LIMIT` caps
/// distinct values, not raw rows. A free function (not a `&self` method) so
/// it's unit-testable without a real authenticated client, mirroring
/// `build_staging_bounds_sql`.
fn build_staging_key_list_sql(
    project_id: &str,
    dataset_id: &str,
    staging: &str,
    key: &str,
    limit: usize,
) -> String {
    format!(
        "SELECT FORMAT('%T', k) FROM (SELECT DISTINCT `{key}` AS k \
         FROM `{project_id}`.`{dataset_id}`.`{staging}` \
         WHERE `{key}` IS NOT NULL LIMIT {limit})"
    )
}

/// Render `values` — the text form `BigQuerySink::distinct_keys` produces — as
/// literals of a column whose `INFORMATION_SCHEMA` `data_type` is `data_type`.
///
/// Unlike the merge's prune bounds, these values did not come from BigQuery's
/// own `FORMAT('%T', …)`: a reconcile reads the destination's keys as strings,
/// diffs them against the source's, and has to write the survivors back into a
/// predicate. A numeric column takes the value bare, but only once it parses as
/// a number here — otherwise a key from anywhere else could carry SQL into the
/// statement. Types with no unambiguous literal form for a plain string are
/// rejected rather than guessed at.
fn bq_key_literals(data_type: &str, column: &str, values: &[String]) -> Result<Vec<String>> {
    let t = data_type.trim().to_ascii_uppercase();
    let numeric = matches!(
        t.as_str(),
        "INT64" | "INTEGER" | "NUMERIC" | "BIGNUMERIC" | "DECIMAL" | "BIGDECIMAL" | "FLOAT64"
    );
    // A typed literal prefix: `DATE '2026-08-30'` and friends, which is how a
    // string has to be written to compare against these columns.
    let prefix = match t.as_str() {
        "STRING" => Some(""),
        "DATE" => Some("DATE "),
        "DATETIME" => Some("DATETIME "),
        "TIMESTAMP" => Some("TIMESTAMP "),
        _ => None,
    };
    if !numeric && prefix.is_none() {
        return Err(EtlError::config(format!(
            "column '{column}' has BigQuery type {data_type}, which reconcile_keys cannot \
             write as a literal; use an INT64/NUMERIC/STRING/DATE/DATETIME/TIMESTAMP key"
        )));
    }
    values
        .iter()
        .map(|v| {
            if numeric {
                if v.parse::<f64>().is_ok() && !v.contains(char::is_whitespace) {
                    Ok(v.clone())
                } else {
                    Err(EtlError::config(format!(
                        "key value {v:?} is not a valid literal for the numeric column \
                         '{column}' ({data_type})"
                    )))
                }
            } else {
                Ok(format!(
                    "{}'{}'",
                    prefix.expect("checked above"),
                    escape_sql_string(v)
                ))
            }
        })
        .collect()
}

/// Build the `MERGE` statement that upserts `staging`'s rows into `dest`,
/// matched on `key` — see `BigQuerySink::merge_into`'s docs. A free function
/// (not a `&self` method) so it's unit-testable without a real authenticated
/// client, mirroring `build_table`.
#[allow(clippy::too_many_arguments)]
fn build_merge_sql(
    project_id: &str,
    dataset_id: &str,
    dest: &str,
    staging: &str,
    key: &[String],
    columns: &[ColumnType],
    prune_partition: Option<&str>,
    prune_key_range: bool,
    delete_stale: bool,
    dedup_order: Option<&str>,
    bounds: &StagingBounds,
    key_list: Option<&[String]>,
) -> Result<String> {
    if key.is_empty() {
        // Should already be caught by sync.rs's prepare_target validation
        // before staging is even created — defensive, not a real user-facing path.
        return Err(EtlError::internal(
            "build_merge_sql called with an empty key (should have been validated before staging began)",
        ));
    }
    let mut on_clause = key
        .iter()
        .map(|k| format!("T.`{k}` = S.`{k}`"))
        .collect::<Vec<_>>()
        .join(" AND ");
    // Optional partition pruning: bound the destination scan to the staging
    // batch's range on an IMMUTABLE partition column, so BigQuery reads only
    // the touched partitions instead of the whole table. The caller
    // guarantees the column is immutable-per-key (see the correctness contract
    // on `TransferConfig::merge_prune_partition_by` — a mutable column here
    // silently inserts duplicate keys).
    //
    // Both prunes below take their bound from `bounds` as LITERALS, resolved
    // from staging before this statement is built, and never as
    // `(SELECT MIN(c) FROM staging)` subqueries: BigQuery rejects a subquery
    // that references a table inside a join predicate, at analysis time, so
    // that form failed every merge regardless of the data. See
    // `StagingBounds`.
    //
    // A window bound over the staging batch's range on the IMMUTABLE prune
    // column, reused by both the ON-clause pruning and (if requested) the
    // scoped DELETE below.
    let window_bound = prune_partition
        .and_then(|pcol| bounds.get(pcol).map(|(lo, hi)| (pcol, lo, hi)))
        .map(|(pcol, lo, hi)| format!("T.`{pcol}` BETWEEN {lo} AND {hi}"));
    if let Some(bound) = &window_bound {
        on_clause.push_str(&format!(" AND {bound}"));
    }
    // Key pruning: bound the destination scan to what the staging batch
    // actually holds on the merge key. Safe with no immutability contract
    // (unlike the partition prune above) because it's a tautology, not an
    // assumption: a destination row can only match on `T.k = S.k`, so its key
    // is in the batch by construction.
    //
    // Two forms of the same tautology, and the tighter one wins when it is
    // available. An exact `IN` list names every key in the batch, so it prunes
    // just as well whether the changed keys are clustered at the top of the key
    // space or scattered across it. A `[MIN, MAX]` range only prunes in
    // proportion to how tightly they cluster — on a table whose rows are
    // updated after insert, the range covers nearly everything and the bound is
    // correct and useless at once. The list is only ever present for a
    // single-column key (see `merge_into`).
    //
    // Skipped entirely for `delete_stale`: `WHEN NOT MATCHED BY SOURCE` only
    // sees rows the ON clause admits, so narrowing it here would silently
    // reduce "replace this window" to "replace these keys" and strand
    // deleted-at-source rows outside the batch.
    let key_list = key_list.filter(|l| !l.is_empty() && key.len() == 1 && !delete_stale);
    match key_list {
        Some(list) => {
            let k = &key[0];
            on_clause.push_str(&format!(" AND T.`{k}` IN ({})", list.join(", ")));
        }
        None => {
            if prune_key_range && !delete_stale {
                // Every key column is bounded — the same argument holds per
                // column, and it gives BigQuery more clustering fields to prune
                // blocks on.
                for k in key {
                    if let Some((lo, hi)) = bounds.get(k) {
                        on_clause.push_str(&format!(" AND T.`{k}` BETWEEN {lo} AND {hi}"));
                    }
                }
            }
        }
    }

    let all_cols: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    let update_cols: Vec<&str> = all_cols
        .iter()
        .copied()
        .filter(|c| !key.iter().any(|k| k == c))
        .collect();

    let mut clauses = Vec::new();
    if !update_cols.is_empty() {
        let update_set = update_cols
            .iter()
            .map(|c| format!("`{c}` = S.`{c}`"))
            .collect::<Vec<_>>()
            .join(", ");
        clauses.push(format!("WHEN MATCHED THEN UPDATE SET {update_set}"));
    }
    // If every column is part of `key`, there's nothing left to update on a
    // match — this degrades to an insert-only merge (a de-duplicating
    // "insert if new"), which is still correct, just a no-op for existing rows.
    let insert_cols = all_cols
        .iter()
        .map(|c| format!("`{c}`"))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_vals = all_cols
        .iter()
        .map(|c| format!("S.`{c}`"))
        .collect::<Vec<_>>()
        .join(", ");
    clauses.push(format!(
        "WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    ));
    // Optional window-scoped delete: remove destination rows INSIDE the merged
    // window that are absent from the source pull ("replace this window", and a
    // NULL merge key nets to a replace instead of duplicating). Scoped to the
    // staging window bound so it never touches history outside the batch —
    // requires the prune column (enforced in config validation; internal error
    // here is purely defensive).
    if delete_stale {
        let pcol = prune_partition.ok_or_else(|| {
            EtlError::internal(
                "delete_stale requested without merge_prune_partition_by (should have been validated)",
            )
        })?;
        match &window_bound {
            Some(bound) => clauses.push(format!(
                "WHEN NOT MATCHED BY SOURCE AND {bound} THEN DELETE"
            )),
            // No resolvable range on the prune column means nothing was staged
            // (or the column is all-NULL across the batch), so there is no
            // window to replace and nothing in it to delete. Drop the clause
            // rather than emit it unscoped — an unscoped
            // `WHEN NOT MATCHED BY SOURCE` deletes the destination's entire
            // history. Same outcome the subquery form had on an empty batch,
            // where the bound evaluated to NULL and matched nothing.
            None => tracing::debug!(
                "delete_stale: staging '{staging}' has no '{pcol}' range; skipping the \
                 window-scoped DELETE (nothing staged to replace)"
            ),
        }
    }

    // Deduplicate staging by `key` before merging, keeping one row per key.
    //
    // Two reasons this is not optional:
    //
    // 1. Both write paths are at-least-once at the row level. A retried append
    //    whose original attempt the server had already committed leaves the
    //    same rows in staging twice. `WHEN NOT MATCHED THEN INSERT` fires once
    //    per unmatched source row, and BigQuery only rejects *target* rows
    //    matched more than once — so two identical rows for a key not yet in
    //    the destination were both inserted, permanently duplicating it. No
    //    later run could repair that: a subsequent merge updates both copies
    //    identically. (The offset/insert-id work on the write paths makes such
    //    a duplicate rare; this makes it harmless.)
    // 2. A source batch legitimately containing two rows for one key (two
    //    updates inside one watermark window) used to fail the whole MERGE
    //    with "UPDATE/MERGE must match at most one source row". Keeping the
    //    highest-watermark row instead mirrors what the ClickHouse sink's
    //    `ReplacingMergeTree(<watermark>)` does with the same input.
    //
    // Ordering: by the watermark descending when there is one, so "last write
    // wins"; otherwise by the key columns, which are constant within each
    // partition — an arbitrary but deterministic tie-break, and always valid
    // SQL (GoogleSQL's ROW_NUMBER needs an ORDER BY).
    let key_list = key
        .iter()
        .map(|k| format!("`{k}`"))
        .collect::<Vec<_>>()
        .join(", ");
    let dedup_order = match dedup_order.filter(|w| columns.iter().any(|c| &c.name == w)) {
        Some(w) => format!("`{w}` DESC"),
        None => key_list.clone(),
    };
    let using = format!(
        "(SELECT * EXCEPT (_qh_row_num) FROM (SELECT *, ROW_NUMBER() OVER \
         (PARTITION BY {key_list} ORDER BY {dedup_order}) AS _qh_row_num \
         FROM `{project_id}`.`{dataset_id}`.`{staging}`) WHERE _qh_row_num = 1)"
    );

    Ok(format!(
        "MERGE INTO `{project_id}`.`{dataset_id}`.`{dest}` T \
         USING {using} S \
         ON {on_clause} {}",
        clauses.join(" "),
    ))
}

/// Build this destination's own `CREATE TABLE`-equivalent (a structured
/// `Table`, not a DDL string — see the module docs). A free function (not a
/// `&self` method) so it's unit-testable without a real authenticated
/// client.
/// Resolve one column's BigQuery field type: an explicit `type_overrides`
/// entry (a BigQuery type name like `"NUMERIC"`) wins, else the Arrow-derived
/// mapping. Shared by table creation and schema evolution.
fn bq_field_data_type(c: &ColumnType, cfg: &TransferConfig) -> Result<TableFieldType> {
    match cfg.type_overrides.get(&c.name) {
        Some(ov) => serde_json::from_value(Value::String(ov.clone())).map_err(|_| {
            EtlError::config(format!(
                "invalid BigQuery type override '{ov}' for column '{}': expected a BigQuery type \
                 name like \"NUMERIC\" or \"BIGNUMERIC\"",
                c.name
            ))
        }),
        None => arrow_to_bigquery_type(&c.arrow).ok_or_else(|| EtlError::UnsupportedType {
            engine: "BigQuery",
            column: c.name.clone(),
            type_name: format!("{:?}", c.arrow),
        }),
    }
}

/// Append a `Nullable` field for every `desired` column missing from
/// `existing` (case-insensitive — BigQuery column names are), returning the
/// merged field list and the names added. Pure (no client), so it's
/// unit-testable. Added columns are forced `Nullable`: rows predating the
/// column read back as NULL. Existing fields are carried through untouched.
fn build_evolved_fields(
    mut existing: Vec<TableFieldSchema>,
    desired: &[ColumnType],
    cfg: &TransferConfig,
) -> Result<(Vec<TableFieldSchema>, Vec<String>)> {
    let names: Vec<String> = existing.iter().map(|f| f.name.clone()).collect();
    let mut added = Vec::new();
    for c in crate::sink::missing_columns(&names, desired, true) {
        existing.push(TableFieldSchema {
            name: c.name.clone(),
            data_type: bq_field_data_type(c, cfg)?,
            mode: Some(TableFieldMode::Nullable),
            ..Default::default()
        });
        added.push(c.name.clone());
    }
    Ok((existing, added))
}

fn build_table(
    project_id: &str,
    dataset_id: &str,
    table: &str,
    columns: &[ColumnType],
    cfg: &TransferConfig,
) -> Result<Table> {
    let mut fields = Vec::with_capacity(columns.len());
    for c in columns {
        fields.push(TableFieldSchema {
            name: c.name.clone(),
            data_type: bq_field_data_type(c, cfg)?,
            mode: Some(if c.nullable {
                TableFieldMode::Nullable
            } else {
                TableFieldMode::Required
            }),
            ..Default::default()
        });
    }

    let time_partitioning = match &cfg.partition_by {
        None => None,
        Some(col_name) => {
            let col = columns.iter().find(|c| &c.name == col_name).ok_or_else(|| {
                EtlError::config(format!(
                    "partition_by '{col_name}' does not match any destination column — for a BigQuery \
                     destination this must be a bare column name, not a SQL expression like ClickHouse's"
                ))
            })?;
            match arrow_to_bigquery_type(&col.arrow) {
                Some(TableFieldType::Date) | Some(TableFieldType::Datetime) | Some(TableFieldType::Timestamp) => {}
                _ => {
                    return Err(EtlError::config(format!(
                        "partition_by column '{col_name}' must be a DATE/DATETIME/TIMESTAMP column for a \
                         BigQuery destination"
                    )))
                }
            }
            Some(TimePartitioning {
                partition_type: TimePartitionType::Day,
                expiration_ms: None,
                field: Some(col_name.clone()),
            })
        }
    };

    let mut cluster_cols: Vec<String> = Vec::new();
    for c in cfg.order_by.iter().chain(cfg.key.iter()) {
        if !cluster_cols.contains(c) {
            cluster_cols.push(c.clone());
        }
    }
    if cluster_cols.len() > 4 {
        return Err(EtlError::config(format!(
            "BigQuery clustering supports at most 4 columns; order_by + key together supplied {} ({}) \
             — trim to at most 4",
            cluster_cols.len(),
            cluster_cols.join(", ")
        )));
    }
    let clustering = if cluster_cols.is_empty() {
        None
    } else {
        Some(Clustering {
            fields: cluster_cols,
        })
    };

    Ok(Table {
        table_reference: TableReference {
            project_id: project_id.to_string(),
            dataset_id: dataset_id.to_string(),
            table_id: table.to_string(),
        },
        schema: Some(TableSchema { fields }),
        time_partitioning,
        clustering,
        ..Default::default()
    })
}

/// Convert one Arrow row to a JSON object matching BigQuery's `insertAll`
/// row representation.
fn batch_row_to_json(batch: &RecordBatch, row: usize) -> Result<serde_json::Map<String, Value>> {
    let schema = batch.schema();
    let mut obj = serde_json::Map::with_capacity(batch.num_columns());
    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        let value = if col.is_null(row) {
            Value::Null
        } else {
            array_value_to_json(col.as_ref(), field.data_type(), row)?
        };
        obj.insert(field.name().clone(), value);
    }
    Ok(obj)
}

/// Convert one non-null Arrow cell to its BigQuery `insertAll` JSON
/// representation. Must cover every Arrow type any source's decoder
/// produces (see `types::bigquery::arrow_to_bigquery_type`).
fn array_value_to_json(col: &dyn Array, dt: &DataType, row: usize) -> Result<Value> {
    Ok(match dt {
        DataType::Boolean => Value::Bool(downcast::<BooleanArray>(col)?.value(row)),
        DataType::Int8 => Value::from(downcast::<Int8Array>(col)?.value(row)),
        DataType::Int16 => Value::from(downcast::<Int16Array>(col)?.value(row)),
        DataType::Int32 => Value::from(downcast::<Int32Array>(col)?.value(row)),
        DataType::Int64 => Value::from(downcast::<Int64Array>(col)?.value(row)),
        DataType::UInt8 => Value::from(downcast::<UInt8Array>(col)?.value(row)),
        DataType::UInt16 => Value::from(downcast::<UInt16Array>(col)?.value(row)),
        DataType::UInt32 => Value::from(downcast::<UInt32Array>(col)?.value(row)),
        DataType::UInt64 => Value::from(downcast::<UInt64Array>(col)?.value(row)),
        DataType::Float32 => json_float(downcast::<Float32Array>(col)?.value(row) as f64),
        DataType::Float64 => json_float(downcast::<Float64Array>(col)?.value(row)),
        DataType::Utf8 => Value::String(downcast::<StringArray>(col)?.value(row).to_string()),
        // BYTES columns take base64 text in insertAll's JSON representation.
        DataType::Binary => {
            let bytes = downcast::<BinaryArray>(col)?.value(row);
            Value::String(base64::engine::general_purpose::STANDARD.encode(bytes))
        }
        DataType::Date32 => Value::String(date32_to_iso(downcast::<Date32Array>(col)?.value(row))),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let micros = downcast::<TimestampMicrosecondArray>(col)?.value(row);
            Value::String(timestamp_micros_to_iso(micros, tz.is_some())?)
        }
        // BigQuery NUMERIC as exact decimal text — insertAll accepts a JSON
        // string for a NUMERIC column. REQUIRED companion to the Storage Write
        // proto arm: plan() promotes a NUMERIC override to Decimal128 for a
        // BigQuery dest unconditionally, so the DEFAULT insertAll path sees it
        // too and must encode it rather than hard-fail.
        DataType::Decimal128(_, scale) => Value::String(crate::decimal::decimal128_to_string(
            downcast::<Decimal128Array>(col)?.value(row),
            *scale,
        )),
        other => {
            return Err(EtlError::internal(format!(
                "no BigQuery JSON conversion implemented for Arrow type {other:?}"
            )))
        }
    })
}

fn downcast<T: 'static>(col: &dyn Array) -> Result<&T> {
    col.as_any().downcast_ref::<T>().ok_or_else(|| {
        EtlError::internal("Arrow array downcast failed (schema/builder type mismatch)")
    })
}

/// `NaN`/`Infinity` aren't representable in JSON — coerced to `null` rather
/// than erroring (matches this crate's general policy of degrading gracefully
/// on unrepresentable values rather than aborting the whole transfer).
fn json_float(v: f64) -> Value {
    serde_json::Number::from_f64(v)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

fn date32_to_iso(days: i32) -> String {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    (epoch + chrono::Duration::days(days as i64))
        .format("%Y-%m-%d")
        .to_string()
}

/// `has_tz` distinguishes BigQuery `DATETIME` (naive, no suffix) from
/// `TIMESTAMP` (UTC instant, trailing `Z`) — matching `arrow_to_bigquery_type`'s
/// `Timestamp(µs, None) -> Datetime` / `Timestamp(µs, Some(_)) -> Timestamp` split.
/// `pub(crate)` so the Storage Write proto encoder (`bigquery_proto`) reuses the
/// same civil-string format for `DATETIME` columns.
pub(crate) fn timestamp_micros_to_iso(micros: i64, has_tz: bool) -> Result<String> {
    let secs = micros.div_euclid(1_000_000);
    let nanos = (micros.rem_euclid(1_000_000) * 1000) as u32;
    let dt = chrono::DateTime::from_timestamp(secs, nanos).ok_or_else(|| {
        EtlError::internal(format!(
            "timestamp {micros} (µs) out of representable range"
        ))
    })?;
    Ok(if has_tz {
        dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
    } else {
        dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Float64Array, Int64Array};
    use arrow_schema::{Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn col(name: &str, arrow: DataType, nullable: bool) -> ColumnType {
        ColumnType {
            name: name.into(),
            type_id: 0,
            nullable,
            arrow,
            clickhouse_inner: "irrelevant".into(),
            arbitrary_precision_decimal: false,
        }
    }

    /// The literal `[MIN, MAX]` bounds a real run resolves from staging before
    /// building the statement, spelled the way BigQuery's `FORMAT('%T', ...)`
    /// spells them (see `StagingBounds`) — that is exactly what the probe reads
    /// back, so these tests assert against the real shape.
    fn bounds(pairs: &[(&str, &str, &str)]) -> StagingBounds {
        StagingBounds(
            pairs
                .iter()
                .map(|(c, lo, hi)| ((*c).to_string(), ((*lo).to_string(), (*hi).to_string())))
                .collect(),
        )
    }

    /// What a probe returns for an empty staging batch: `MIN`/`MAX` are NULL,
    /// so no column has a usable range and no bound is emitted.
    fn no_bounds() -> StagingBounds {
        StagingBounds::default()
    }

    fn base_cfg() -> TransferConfig {
        TransferConfig {
            source_archive_ignored: false,
            source_table: Some("t".into()),
            source_query: None,
            dest_table: "t".into(),
            mode: crate::config::SyncMode::Full,
            watermark: None,
            watermark_source_expr: None,
            lookback_seconds: 0,
            key: vec![],
            create_if_missing: true,
            engine: None,
            order_by: vec![],
            partition_by: None,
            primary_key: vec![],
            merge_prune_partition_by: None,
            merge_prune_key_range: true,
            merge_prune_key_list_max: 0,
            delete_stale_in_window: false,
            allow_full_refresh_shrink: false,
            parallelism: 1,
            batch_rows: 1000,
            batch_bytes: 0,
            insert_bytes: 0,
            max_memory_bytes: 0,
            max_memory_fraction: 0.0,
            partition_column: None,
            partition_source_expr: None,
            read_max_rows_per_sec: None,
            read_idle_timeout_secs: 0,
            chunk_rows: None,
            retry_max_attempts: 1,
            probe_max_cost: crate::source::DEFAULT_PROBE_MAX_COST,
            read_window_rows: None,
            window_target_secs: None,
            column_transforms: HashMap::new(),
            column_transform_types: HashMap::new(),
            evolve_schema: false,
            state_table_name: "_quickhouse_state".into(),
            staging_suffix: "_quickhouse_tmp".into(),
            application_name: "quickhouse".into(),
            state_key: None,
            seed_watermark: crate::config::WatermarkSeed::None,
            advance_watermark: true,
            type_overrides: HashMap::new(),
            rename: HashMap::new(),
            include: vec![],
            exclude: vec![],
            not_null: vec![],
            tinyint1_as_bool: true,
            numeric_as_decimal: None,
        }
    }

    fn lits(vals: &[&str]) -> Vec<String> {
        vals.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_key_list_bound_replaces_the_key_range_bound() {
        // The `fore_app.user` shape: correctly clustered on the merge key, but
        // the changed keys span the whole id space, so `[MIN, MAX]` covers
        // nearly the entire table and prunes almost nothing. A key list names
        // exactly what the batch holds and does not degrade that way.
        let cols = vec![
            col("id", DataType::Int64, false),
            col("v", DataType::Int64, true),
        ];
        let key = vec!["id".to_string()];
        let sql = build_merge_sql(
            "p",
            "d",
            "users",
            "users_tmp",
            &key,
            &cols,
            None,
            true,
            false,
            None,
            &bounds(&[("id", "1", "9000000")]),
            Some(&lits(&["7", "4210", "8999999"])),
        )
        .unwrap();
        assert!(sql.contains("T.`id` IN (7, 4210, 8999999)"), "{sql}");
        // ...and the range bound is gone: an IN list already implies it, so
        // emitting both would only make the predicate longer.
        assert!(!sql.contains("BETWEEN"), "{sql}");
    }

    #[test]
    fn a_key_list_is_ignored_where_it_would_change_what_merges() {
        let cols = vec![
            col("id", DataType::Int64, false),
            col("created_at", DataType::Date32, false),
            col("v", DataType::Int64, true),
        ];
        let list = lits(&["7", "8"]);
        // Under delete_stale the ON clause also gates
        // `WHEN NOT MATCHED BY SOURCE`, so narrowing it to the batch's keys
        // would strand deleted-at-source rows — exactly the rows the feature
        // exists to remove. Same reason the range bound is skipped there.
        let sql = build_merge_sql(
            "p",
            "d",
            "t",
            "s",
            &["id".to_string()],
            &cols,
            Some("created_at"),
            true,
            true,
            None,
            &bounds(&[("created_at", "DATE \"2026-08-01\"", "DATE \"2026-08-20\"")]),
            Some(&list),
        )
        .unwrap();
        assert!(!sql.contains("IN (7, 8)"), "{sql}");
        // The window bound it does depend on is still there.
        assert!(sql.contains("T.`created_at` BETWEEN"), "{sql}");

        // A composite key would need an IN UNNEST([STRUCT(...)]) form whose
        // pruning behaviour is not the same; those keep the range bound.
        let sql = build_merge_sql(
            "p",
            "d",
            "t",
            "s",
            &["id".to_string(), "v".to_string()],
            &cols,
            None,
            true,
            false,
            None,
            &bounds(&[("id", "1", "9"), ("v", "10", "99")]),
            Some(&list),
        )
        .unwrap();
        assert!(!sql.contains("IN (7, 8)"), "{sql}");
        assert!(sql.contains("T.`id` BETWEEN 1 AND 9"), "{sql}");

        // An empty list means "the probe found nothing", not "match nothing" —
        // emitting `IN ()` would merge zero rows.
        let sql = build_merge_sql(
            "p",
            "d",
            "t",
            "s",
            &["id".to_string()],
            &cols,
            None,
            true,
            false,
            None,
            &bounds(&[("id", "1", "9")]),
            Some(&[]),
        )
        .unwrap();
        assert!(!sql.contains("IN ()"), "{sql}");
        assert!(sql.contains("T.`id` BETWEEN 1 AND 9"), "{sql}");
    }

    #[test]
    fn the_key_list_probe_reads_one_row_past_the_ceiling() {
        // Reading `max + 1` distinct values is how the caller learns the batch
        // blew the ceiling without counting the whole staging table.
        let sql = build_staging_key_list_sql("p", "d", "users_tmp", "id", 1001);
        assert!(sql.contains("LIMIT 1001"), "{sql}");
        // DISTINCT inside the subquery, so the LIMIT caps distinct values
        // rather than raw rows.
        assert!(
            sql.contains("SELECT DISTINCT `id` AS k FROM `p`.`d`.`users_tmp`"),
            "{sql}"
        );
        assert!(sql.contains("`id` IS NOT NULL"), "{sql}");
        // BigQuery renders the literals, exactly as the range bounds do.
        assert!(sql.starts_with("SELECT FORMAT('%T', k)"), "{sql}");
    }

    #[test]
    fn reconcile_key_literals_render_per_destination_type() {
        assert_eq!(
            bq_key_literals("INT64", "id", &lits(&["1", "-42"])).unwrap(),
            lits(&["1", "-42"])
        );
        assert_eq!(
            bq_key_literals("STRING", "code", &lits(&["a-1", "it's"])).unwrap(),
            vec!["'a-1'".to_string(), "'it\\'s'".to_string()]
        );
        assert_eq!(
            bq_key_literals("DATE", "d", &lits(&["2026-08-30"])).unwrap(),
            vec!["DATE '2026-08-30'".to_string()]
        );
    }

    #[test]
    fn reconcile_key_literals_refuse_to_paste_a_non_number_into_a_numeric_column() {
        // These keys come back from a destination read and go straight into a
        // generated predicate, so a numeric column must not accept text.
        let err = bq_key_literals("INT64", "id", &lits(&["1 OR 1=1"]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("not a valid literal"), "{err}");
        // A type with no unambiguous literal form for a plain string is
        // rejected outright rather than guessed at.
        let err = bq_key_literals("BYTES", "b", &lits(&["x"]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("reconcile_keys cannot"), "{err}");
    }

    #[test]
    fn build_evolved_fields_appends_missing_as_nullable() {
        let existing = vec![TableFieldSchema {
            name: "id".into(),
            data_type: TableFieldType::Integer,
            mode: Some(TableFieldMode::Required),
            ..Default::default()
        }];
        let desired = vec![
            col("id", DataType::Int64, false),
            col("email", DataType::Utf8, true),
        ];
        let (fields, added) = build_evolved_fields(existing, &desired, &base_cfg()).unwrap();
        assert_eq!(
            added,
            vec!["email"],
            "only the genuinely-new column is added"
        );
        assert_eq!(fields.len(), 2);
        // The existing field is carried through untouched (still Required).
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[0].mode, Some(TableFieldMode::Required));
        // The added column is forced Nullable (rows predating it read as NULL).
        let email = fields.iter().find(|f| f.name == "email").unwrap();
        assert_eq!(email.mode, Some(TableFieldMode::Nullable));
        assert_eq!(email.data_type, TableFieldType::String);
    }

    #[test]
    fn build_evolved_fields_matches_column_names_case_insensitively() {
        // BigQuery treats column names case-insensitively: "ID" already exists.
        let existing = vec![TableFieldSchema {
            name: "ID".into(),
            data_type: TableFieldType::Integer,
            mode: Some(TableFieldMode::Nullable),
            ..Default::default()
        }];
        let desired = vec![col("id", DataType::Int64, false)];
        let (fields, added) = build_evolved_fields(existing, &desired, &base_cfg()).unwrap();
        assert!(
            added.is_empty(),
            "case-insensitive match must not re-add 'id'"
        );
        assert_eq!(fields.len(), 1);
    }

    #[test]
    fn build_table_maps_types_and_nullability() {
        let cols = vec![
            col("id", DataType::Int64, false),
            col("name", DataType::Utf8, true),
        ];
        let t = build_table("proj", "ds", "orders", &cols, &base_cfg()).unwrap();
        assert_eq!(t.table_reference.project_id, "proj");
        assert_eq!(t.table_reference.dataset_id, "ds");
        assert_eq!(t.table_reference.table_id, "orders");
        let fields = t.schema.unwrap().fields;
        assert_eq!(fields[0].data_type, TableFieldType::Integer);
        assert_eq!(fields[0].mode, Some(TableFieldMode::Required));
        assert_eq!(fields[1].data_type, TableFieldType::String);
        assert_eq!(fields[1].mode, Some(TableFieldMode::Nullable));
    }

    #[test]
    fn build_table_applies_type_override() {
        let cols = vec![col("amount", DataType::Float64, false)];
        let mut cfg = base_cfg();
        cfg.type_overrides.insert("amount".into(), "NUMERIC".into());
        let t = build_table("p", "d", "t", &cols, &cfg).unwrap();
        assert_eq!(
            t.schema.unwrap().fields[0].data_type,
            TableFieldType::Numeric
        );
    }

    #[test]
    fn build_table_rejects_invalid_type_override() {
        let cols = vec![col("amount", DataType::Float64, false)];
        let mut cfg = base_cfg();
        cfg.type_overrides
            .insert("amount".into(), "NOT_A_REAL_TYPE".into());
        assert!(build_table("p", "d", "t", &cols, &cfg).is_err());
    }

    #[test]
    fn build_table_clustering_from_order_by_and_key_deduped() {
        let cols = vec![
            col("a", DataType::Int64, false),
            col("b", DataType::Int64, false),
        ];
        let mut cfg = base_cfg();
        cfg.order_by = vec!["a".into(), "b".into()];
        cfg.key = vec!["a".into()]; // duplicate of order_by[0], must not double up
        let t = build_table("p", "d", "t", &cols, &cfg).unwrap();
        assert_eq!(t.clustering.unwrap().fields, vec!["a", "b"]);
    }

    #[test]
    fn build_table_rejects_more_than_four_clustering_columns() {
        let cols = (0..5)
            .map(|i| col(&format!("c{i}"), DataType::Int64, false))
            .collect::<Vec<_>>();
        let mut cfg = base_cfg();
        cfg.order_by = cols.iter().map(|c| c.name.clone()).collect();
        let err = build_table("p", "d", "t", &cols, &cfg)
            .unwrap_err()
            .to_string();
        assert!(err.contains("at most 4"), "{err}");
    }

    #[test]
    fn build_table_partition_by_requires_date_like_column() {
        let cols = vec![col("id", DataType::Int64, false)];
        let mut cfg = base_cfg();
        cfg.partition_by = Some("id".into());
        let err = build_table("p", "d", "t", &cols, &cfg)
            .unwrap_err()
            .to_string();
        assert!(err.contains("DATE/DATETIME/TIMESTAMP"), "{err}");
    }

    #[test]
    fn build_table_partition_by_missing_column_errors_clearly() {
        let cols = vec![col("id", DataType::Int64, false)];
        let mut cfg = base_cfg();
        cfg.partition_by = Some("nonexistent".into());
        let err = build_table("p", "d", "t", &cols, &cfg)
            .unwrap_err()
            .to_string();
        assert!(err.contains("nonexistent"), "{err}");
    }

    #[test]
    fn build_table_partition_by_valid_date_column() {
        let cols = vec![
            col("id", DataType::Int64, false),
            col("event_date", DataType::Date32, false),
        ];
        let mut cfg = base_cfg();
        cfg.partition_by = Some("event_date".into());
        let t = build_table("p", "d", "t", &cols, &cfg).unwrap();
        let tp = t.time_partitioning.unwrap();
        assert_eq!(tp.field.as_deref(), Some("event_date"));
        assert_eq!(tp.partition_type, TimePartitionType::Day);
    }

    #[test]
    fn build_swap_sql_wraps_truncate_and_insert_select_in_a_transaction() {
        let cols = vec![
            col("id", DataType::Int64, false),
            col("name", DataType::Utf8, true),
            col("amount", DataType::Float64, true),
        ];
        let sql = build_swap_sql("proj", "ds", "orders", "orders_quickhouse_tmp", &cols);

        assert!(sql.starts_with("BEGIN TRANSACTION;"), "{sql}");
        assert!(sql.trim_end().ends_with("COMMIT TRANSACTION;"), "{sql}");
        assert!(
            sql.contains("TRUNCATE TABLE `proj`.`ds`.`orders`;"),
            "{sql}"
        );
        assert!(
            sql.contains(
                "INSERT INTO `proj`.`ds`.`orders` (`id`, `name`, `amount`) \
                 SELECT `id`, `name`, `amount` FROM `proj`.`ds`.`orders_quickhouse_tmp`;"
            ),
            "{sql}"
        );
        // TRUNCATE must run before the INSERT, and both inside the transaction.
        let truncate_pos = sql.find("TRUNCATE TABLE").unwrap();
        let insert_pos = sql.find("INSERT INTO").unwrap();
        let commit_pos = sql.find("COMMIT TRANSACTION").unwrap();
        assert!(
            truncate_pos < insert_pos && insert_pos < commit_pos,
            "wrong statement order: {sql}"
        );
    }

    #[test]
    fn build_swap_sql_includes_every_column_not_just_a_key_subset() {
        // Unlike build_merge_sql there's no key/non-key split — every column
        // is both truncated away and re-inserted.
        let cols = vec![
            col("a", DataType::Int64, false),
            col("b", DataType::Utf8, true),
        ];
        let sql = build_swap_sql("p", "d", "dest", "staging", &cols);
        assert!(sql.contains("(`a`, `b`) SELECT `a`, `b` FROM"), "{sql}");
    }

    #[test]
    fn build_persist_watermark_sql_backtick_quotes_the_reserved_rows_column() {
        // Regression test: `rows` is a reserved GoogleSQL keyword (window-
        // framing syntax) and must be backtick-quoted as a column identifier,
        // or every persist_watermark call fails with a syntax error right
        // after a successful incremental run — 100% reproducible, since it's
        // independent of write_method, watermark value, or row count.
        let sql = build_persist_watermark_sql(
            "proj",
            "ds",
            "_quickhouse_state",
            "orders",
            "orders_dest",
            "2024-06-01",
            42,
        );
        assert!(
            sql.contains("(source_table, dest_table, last_watermark, `rows`, run_ts)"),
            "`rows` must be backtick-quoted: {sql}"
        );
        assert!(
            sql.starts_with("INSERT INTO `proj`.`ds`.`_quickhouse_state`"),
            "{sql}"
        );
        // A custom state-table name flows through (C1 configurable internals).
        let custom = build_persist_watermark_sql(
            "proj",
            "ds",
            "wh_state",
            "orders",
            "orders_dest",
            "2024-06-01",
            1,
        );
        assert!(
            custom.starts_with("INSERT INTO `proj`.`ds`.`wh_state`"),
            "{custom}"
        );
        assert!(
            sql.contains("VALUES ('orders', 'orders_dest', '2024-06-01', 42, CURRENT_TIMESTAMP())"),
            "{sql}"
        );
    }

    #[test]
    fn build_persist_watermark_sql_escapes_quotes_in_values() {
        // GoogleSQL doesn't recognize the ANSI doubled-quote escape ('') at
        // all — only a backslash-escaped quote (\') is valid, so this must
        // NOT double the quote like the Postgres/ClickHouse/MySQL sinks do.
        let sql = build_persist_watermark_sql(
            "p",
            "d",
            "_quickhouse_state",
            "o'brien",
            "dest",
            "it's here",
            1,
        );
        assert!(sql.contains(r"'o\'brien'"), "{sql}");
        assert!(sql.contains(r"'it\'s here'"), "{sql}");
    }

    #[test]
    fn build_persist_watermark_sql_escapes_backslash_before_quote() {
        // Regression test: a trailing backslash must not swallow the
        // literal's closing quote (`'ends_with_backslash\'` is an unclosed
        // string in GoogleSQL) — backslash has to be escaped first.
        let sql =
            build_persist_watermark_sql("p", "d", "_quickhouse_state", r"a\b", "dest", "wm", 1);
        assert!(sql.contains(r"'a\\b'"), "{sql}");
    }

    #[test]
    fn build_persist_watermark_sql_escapes_newlines_in_values() {
        // Regression test: `source_id` here is `effective_state_key()`, which
        // defaults to the raw `source_query` text — so an ordinary multi-line
        // user query lands inside a quoted GoogleSQL literal, which cannot
        // carry a raw newline (it terminates the literal and the statement
        // fails to parse). The bug was masked on run 1, because
        // `read_last_watermark` returns Ok(None) while the state table is
        // still absent; it surfaced only from run 2 onward, permanently.
        let multiline = "SELECT \"id\",\n  CAST(\"qty\" AS TEXT)\r\nFROM stock_move";
        let sql =
            build_persist_watermark_sql("p", "d", "_quickhouse_state", multiline, "dest", "wm", 1);
        assert!(
            !sql.contains('\n') && !sql.contains('\r'),
            "no raw newline may survive into the literal: {sql:?}"
        );
        assert!(sql.contains(r#"CAST("qty" AS TEXT)\r\nFROM"#), "{sql:?}");
    }

    #[test]
    fn escape_sql_string_escapes_backslash_before_newline() {
        // Order matters: escaping the newline first would leave the backslash
        // of `\n` to be doubled afterwards, producing a literal `\\n` (an
        // escaped backslash followed by `n`) instead of a newline escape.
        assert_eq!(escape_sql_string("a\nb"), r"a\nb");
        assert_eq!(escape_sql_string("a\r\nb"), r"a\r\nb");
        // A pre-existing literal backslash-n in the text stays distinct from a
        // real newline: it doubles to `\\n`, which GoogleSQL reads back as the
        // two characters `\` and `n`, not a newline.
        assert_eq!(escape_sql_string(r"a\nb"), r"a\\nb");
    }

    #[test]
    fn build_merge_sql_matches_on_key_and_updates_non_key_columns() {
        let cols = vec![
            col("id", DataType::Int64, false),
            col("name", DataType::Utf8, true),
            col("amount", DataType::Float64, true),
        ];
        let key = vec!["id".to_string()];
        let sql = build_merge_sql(
            "proj",
            "ds",
            "orders",
            "orders_quickhouse_tmp",
            &key,
            &cols,
            None,
            false,
            false,
            None,
            &no_bounds(),
            None,
        )
        .unwrap();

        assert!(
            sql.starts_with("MERGE INTO `proj`.`ds`.`orders` T USING "),
            "{sql}"
        );
        // Staging is read through a dedup-by-key subquery, never directly —
        // see the dedicated dedup tests below for why.
        assert!(
            sql.contains("FROM `proj`.`ds`.`orders_quickhouse_tmp`"),
            "{sql}"
        );
        assert!(sql.contains("ON T.`id` = S.`id`"));
        // No prune column and key-range pruning off -> no bound of either kind.
        // (Key-range pruning is on by default in real runs; it has its own
        // tests, and is passed `false` here to keep this one about the base
        // MERGE shape.)
        assert!(
            !sql.contains("BETWEEN"),
            "unexpected prune predicate without an immutable column: {sql}"
        );
        assert!(
            sql.contains("WHEN MATCHED THEN UPDATE SET `name` = S.`name`, `amount` = S.`amount`")
        );
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT (`id`, `name`, `amount`) VALUES (S.`id`, S.`name`, S.`amount`)"));
        // The key column must never appear in the UPDATE SET list.
        assert!(
            !sql.contains("`id` = S.`id`,"),
            "key column leaked into UPDATE SET: {sql}"
        );
    }

    #[test]
    fn build_merge_sql_composite_key() {
        let cols = vec![
            col("a", DataType::Int64, false),
            col("b", DataType::Int64, false),
        ];
        let key = vec!["a".to_string(), "b".to_string()];
        let sql = build_merge_sql(
            "p",
            "d",
            "t",
            "s",
            &key,
            &cols,
            None,
            false,
            false,
            None,
            &no_bounds(),
            None,
        )
        .unwrap();
        assert!(sql.contains("ON T.`a` = S.`a` AND T.`b` = S.`b`"));
    }

    #[test]
    fn build_merge_sql_bounds_the_destination_scan_to_the_staging_key_range() {
        // Default-on, and safe without any immutability contract: a row can only
        // match on the key, so its key is inside staging's own range already.
        let cols = vec![
            col("id", DataType::Int64, false),
            col("amount", DataType::Float64, true),
        ];
        let key = vec!["id".to_string()];
        let sql = build_merge_sql(
            "p",
            "d",
            "orders",
            "orders_tmp",
            &key,
            &cols,
            None,
            true,
            false,
            None,
            &bounds(&[("id", "1", "341")]),
            None,
        )
        .unwrap();
        assert!(sql.contains("ON T.`id` = S.`id`"), "{sql}");
        assert!(
            sql.contains("AND T.`id` BETWEEN 1 AND 341"),
            "missing key-range bound: {sql}"
        );
        // A literal, never a subquery over staging: BigQuery rejects
        // "Unsupported subquery with table in join predicate" when analysing
        // the statement, which failed every merge this feature touched.
        assert!(!sql.contains("BETWEEN (SELECT"), "{sql}");
    }

    #[test]
    fn build_merge_sql_bounds_every_column_of_a_composite_key() {
        // Per-column, the same tautology holds — a matched row carries the
        // staging row's exact value in each key column.
        let cols = vec![
            col("a", DataType::Int64, false),
            col("b", DataType::Int64, false),
            col("v", DataType::Float64, true),
        ];
        let key = vec!["a".to_string(), "b".to_string()];
        let sql = build_merge_sql(
            "p",
            "d",
            "t",
            "s",
            &key,
            &cols,
            None,
            true,
            false,
            None,
            &bounds(&[("a", "1", "9"), ("b", "10", "99")]),
            None,
        )
        .unwrap();
        assert!(sql.contains("AND T.`a` BETWEEN 1 AND 9"), "{sql}");
        assert!(sql.contains("AND T.`b` BETWEEN 10 AND 99"), "{sql}");
    }

    #[test]
    fn build_merge_sql_key_range_bound_is_opt_outable() {
        let cols = vec![
            col("id", DataType::Int64, false),
            col("amount", DataType::Float64, true),
        ];
        let key = vec!["id".to_string()];
        // Bounds are supplied, so it is the flag alone that suppresses them.
        let sql = build_merge_sql(
            "p",
            "d",
            "t",
            "s",
            &key,
            &cols,
            None,
            false,
            false,
            None,
            &bounds(&[("id", "1", "341")]),
            None,
        )
        .unwrap();
        assert!(sql.contains("ON T.`id` = S.`id`"), "{sql}");
        assert!(!sql.contains("BETWEEN"), "bound should be absent: {sql}");
    }

    #[test]
    fn build_merge_sql_key_range_bound_is_suppressed_by_delete_stale() {
        // `WHEN NOT MATCHED BY SOURCE` only sees rows the ON clause admits, so a
        // key-range bound would quietly turn "replace this window" into
        // "replace this key range" and strand deleted-at-source rows whose keys
        // fall outside the batch's span.
        let cols = vec![
            col("id", DataType::Int64, false),
            col(
                "create_date",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            col("amount", DataType::Float64, true),
        ];
        let key = vec!["id".to_string()];
        let sql = build_merge_sql(
            "p",
            "d",
            "orders",
            "orders_tmp",
            &key,
            &cols,
            Some("create_date"),
            true,
            true,
            None,
            &bounds(&[
                ("id", "1", "341"),
                (
                    "create_date",
                    r#"TIMESTAMP "2026-08-01 00:00:00+00""#,
                    r#"TIMESTAMP "2026-08-20 12:00:00+00""#,
                ),
            ]),
            None,
        )
        .unwrap();
        assert!(
            !sql.contains("T.`id` BETWEEN"),
            "key-range bound must not narrow a NOT MATCHED BY SOURCE delete: {sql}"
        );
        // The window bound it does require is still there.
        assert!(
            sql.contains(&format!(
                "T.`create_date` BETWEEN {} AND {}",
                r#"TIMESTAMP "2026-08-01 00:00:00+00""#, r#"TIMESTAMP "2026-08-20 12:00:00+00""#
            )),
            "{sql}"
        );
    }

    #[test]
    fn build_merge_sql_prunes_on_immutable_partition_column() {
        let cols = vec![
            col("id", DataType::Int64, false),
            col(
                "create_date",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            col("amount", DataType::Float64, true),
        ];
        let key = vec!["id".to_string()];
        let sql = build_merge_sql(
            "p",
            "d",
            "orders",
            "orders_tmp",
            &key,
            &cols,
            Some("create_date"),
            false,
            false,
            None,
            &bounds(&[(
                "create_date",
                r#"TIMESTAMP "2026-08-01 00:00:00+00""#,
                r#"TIMESTAMP "2026-08-20 12:00:00+00""#,
            )]),
            None,
        )
        .unwrap();
        // The join still matches on the key, AND the destination is bounded to
        // the staging batch's create_date range so BigQuery prunes partitions.
        assert!(sql.contains("ON T.`id` = S.`id`"), "{sql}");
        assert!(
            sql.contains(&format!(
                "AND T.`create_date` BETWEEN {} AND {}",
                r#"TIMESTAMP "2026-08-01 00:00:00+00""#, r#"TIMESTAMP "2026-08-20 12:00:00+00""#
            )),
            "missing partition-prune predicate: {sql}"
        );
        // Same dialect constraint as the key-range bound: a subquery here is
        // rejected when the statement is analysed, so it must be a literal.
        assert!(!sql.contains("BETWEEN (SELECT"), "{sql}");
        // The pruned column is still upserted like any other non-key column.
        assert!(sql.contains("`create_date` = S.`create_date`"), "{sql}");
    }

    #[test]
    fn build_merge_sql_delete_stale_scopes_to_the_staging_window() {
        let cols = vec![
            col("id", DataType::Int64, false),
            col(
                "create_date",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ];
        let key = vec!["id".to_string()];
        let sql = build_merge_sql(
            "p",
            "d",
            "orders",
            "orders_tmp",
            &key,
            &cols,
            Some("create_date"),
            false,
            true,
            None,
            &bounds(&[(
                "create_date",
                r#"TIMESTAMP "2026-08-01 00:00:00+00""#,
                r#"TIMESTAMP "2026-08-20 12:00:00+00""#,
            )]),
            None,
        )
        .unwrap();
        // The DELETE is scoped to the SAME staging window as the prune — it must
        // never delete outside the batch's create_date range.
        assert!(
            sql.contains(&format!(
                "WHEN NOT MATCHED BY SOURCE AND T.`create_date` BETWEEN {} AND {} THEN DELETE",
                r#"TIMESTAMP "2026-08-01 00:00:00+00""#, r#"TIMESTAMP "2026-08-20 12:00:00+00""#
            )),
            "missing window-scoped delete: {sql}"
        );
    }

    #[test]
    fn build_merge_sql_delete_stale_without_prune_is_internal_error() {
        // Defensive: config validation should reject this combo first, but the
        // builder must never emit an unscoped (history-nuking) DELETE.
        let cols = vec![
            col("id", DataType::Int64, false),
            col("v", DataType::Int64, true),
        ];
        let key = vec!["id".to_string()];
        let err = build_merge_sql(
            "p",
            "d",
            "t",
            "s",
            &key,
            &cols,
            None,
            false,
            true,
            None,
            &no_bounds(),
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("delete_stale"), "{err}");
    }

    #[test]
    fn build_merge_sql_all_columns_are_key_becomes_insert_only() {
        let cols = vec![col("id", DataType::Int64, false)];
        let key = vec!["id".to_string()];
        let sql = build_merge_sql(
            "p",
            "d",
            "t",
            "s",
            &key,
            &cols,
            None,
            false,
            false,
            None,
            &no_bounds(),
            None,
        )
        .unwrap();
        assert!(
            !sql.contains("WHEN MATCHED"),
            "no columns left to update: {sql}"
        );
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT (`id`) VALUES (S.`id`)"));
    }

    #[test]
    fn build_merge_sql_dedups_staging_by_key_newest_first() {
        // Both write paths are at-least-once per row: a retried append the
        // server had already committed leaves duplicate rows in staging, and
        // `WHEN NOT MATCHED THEN INSERT` fires per source row — so a key not
        // yet in the destination got inserted twice, permanently, with no
        // later run able to repair it. Deduping in the USING clause makes the
        // duplicate harmless regardless of which write path produced it.
        let cols = vec![
            col("id", DataType::Int64, false),
            col("write_date", DataType::Utf8, true),
            col("amount", DataType::Float64, true),
        ];
        let key = vec!["id".to_string()];
        let sql = build_merge_sql(
            "p",
            "d",
            "orders",
            "orders_tmp",
            &key,
            &cols,
            None,
            false,
            false,
            Some("write_date"),
            &no_bounds(),
            None,
        )
        .unwrap();
        assert!(
            sql.contains(
                "USING (SELECT * EXCEPT (_qh_row_num) FROM (SELECT *, ROW_NUMBER() OVER \
                 (PARTITION BY `id` ORDER BY `write_date` DESC) AS _qh_row_num \
                 FROM `p`.`d`.`orders_tmp`) WHERE _qh_row_num = 1) S"
            ),
            "{sql}"
        );
        // The merge still behaves the same otherwise.
        assert!(sql.contains("ON T.`id` = S.`id`"), "{sql}");
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"), "{sql}");
    }

    #[test]
    fn build_merge_sql_dedup_falls_back_to_key_order_without_a_watermark() {
        // GoogleSQL's ROW_NUMBER needs an ORDER BY. With no watermark to order
        // by, the key columns are constant within each partition — an
        // arbitrary but deterministic tie-break, which is all a retry-duplicate
        // (two byte-identical rows) needs.
        let cols = vec![
            col("a", DataType::Int64, false),
            col("b", DataType::Int64, false),
            col("v", DataType::Int64, true),
        ];
        let key = vec!["a".to_string(), "b".to_string()];
        let sql = build_merge_sql(
            "p",
            "d",
            "t",
            "s",
            &key,
            &cols,
            None,
            false,
            false,
            None,
            &no_bounds(),
            None,
        )
        .unwrap();
        assert!(
            sql.contains("PARTITION BY `a`, `b` ORDER BY `a`, `b`"),
            "{sql}"
        );
    }

    #[test]
    fn build_merge_sql_dedup_ignores_a_watermark_absent_from_staging() {
        // The watermark is a *source* column name; a rename (or an
        // include/exclude) can leave it out of the destination/staging columns.
        // Ordering by a column that isn't there would be a hard SQL error, so
        // fall back rather than emit invalid SQL.
        let cols = vec![
            col("id", DataType::Int64, false),
            col("v", DataType::Int64, true),
        ];
        let key = vec!["id".to_string()];
        let sql = build_merge_sql(
            "p",
            "d",
            "t",
            "s",
            &key,
            &cols,
            None,
            false,
            false,
            Some("not_a_staging_column"),
            &no_bounds(),
            None,
        )
        .unwrap();
        assert!(!sql.contains("not_a_staging_column"), "{sql}");
        assert!(sql.contains("PARTITION BY `id` ORDER BY `id`"), "{sql}");
    }

    #[test]
    fn build_merge_sql_rejects_empty_key() {
        let cols = vec![col("id", DataType::Int64, false)];
        let err = build_merge_sql(
            "p",
            "d",
            "t",
            "s",
            &[],
            &cols,
            None,
            false,
            false,
            None,
            &no_bounds(),
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("empty key"), "{err}");
        assert!(
            err.contains("quickhouse bug"),
            "must be framed as internal, not a config error: {err}"
        );
    }

    /// The text BigQuery analyses as this statement's join predicates:
    /// everything between `ON` and the first `WHEN`, plus the search condition
    /// of a `WHEN NOT MATCHED BY SOURCE` clause.
    fn merge_conditions(sql: &str) -> Vec<&str> {
        let on = sql.split(" ON ").nth(1).expect("a MERGE has an ON clause");
        let mut conds = vec![on.split(" WHEN ").next().unwrap()];
        if let Some(rest) = sql.split("WHEN NOT MATCHED BY SOURCE AND ").nth(1) {
            conds.push(rest.split(" THEN ").next().unwrap());
        }
        conds
    }

    #[test]
    fn build_merge_sql_never_puts_a_subquery_in_a_merge_condition() {
        // The regression this pins down. BigQuery analyses the `ON` clause (and
        // a `WHEN NOT MATCHED BY SOURCE` search condition) as join predicates,
        // and rejects a subquery referencing a table in one of them outright:
        //
        //     Unsupported subquery with table in join predicate.
        //
        // That happens before it looks at any data, so the previous
        // `BETWEEN (SELECT MIN(k) FROM staging) AND (SELECT MAX(k) FROM staging)`
        // form failed 100% of BigQuery merges — zero-row no-ops included — for
        // every transfer `merge_prune_key_range` (default on) or
        // `merge_prune_partition_by` applied to. Bounds must be literals; this
        // asserts it across every combination of the knobs that emit one.
        let cols = vec![
            col("id", DataType::Int64, false),
            col(
                "create_date",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            col("amount", DataType::Float64, true),
        ];
        let key = vec!["id".to_string()];
        let resolved = bounds(&[
            ("id", "1", "341"),
            (
                "create_date",
                r#"TIMESTAMP "2026-08-01 00:00:00+00""#,
                r#"TIMESTAMP "2026-08-20 12:00:00+00""#,
            ),
        ]);
        for prune_partition in [None, Some("create_date")] {
            for prune_key_range in [false, true] {
                for delete_stale in [false, true] {
                    // Validated as a config error long before here.
                    if delete_stale && prune_partition.is_none() {
                        continue;
                    }
                    // Both what a populated batch resolves and what an empty one
                    // does, since the empty case is what first broke in prod.
                    for staging_bounds in [&no_bounds(), &resolved] {
                        let sql = build_merge_sql(
                            "p",
                            "d",
                            "orders",
                            "orders_tmp",
                            &key,
                            &cols,
                            prune_partition,
                            prune_key_range,
                            delete_stale,
                            None,
                            staging_bounds,
                            None,
                        )
                        .unwrap();
                        for cond in merge_conditions(&sql) {
                            assert!(
                                !cond.contains("SELECT"),
                                "subquery in a merge condition ({cond}) of: {sql}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn build_merge_sql_bounds_a_string_key_with_the_literal_bigquery_rendered() {
        // A non-numeric key is bounded the same way, using the quoted form
        // `FORMAT('%T', ...)` returns — escaping included, so quickhouse never
        // has to quote a value itself.
        let cols = vec![
            col("code", DataType::Utf8, false),
            col("v", DataType::Int64, true),
        ];
        let key = vec!["code".to_string()];
        let sql = build_merge_sql(
            "p",
            "d",
            "t",
            "s",
            &key,
            &cols,
            None,
            true,
            false,
            None,
            &bounds(&[("code", r#""a-001""#, r#""it's z""#)]),
            None,
        )
        .unwrap();
        assert!(
            sql.contains(r#"AND T.`code` BETWEEN "a-001" AND "it's z""#),
            "{sql}"
        );
    }

    #[test]
    fn build_merge_sql_skips_the_bound_when_staging_has_no_range() {
        // An empty batch (or an all-NULL key column) leaves `MIN`/`MAX` NULL, so
        // there is no literal to bound with. Dropping the bound is always safe —
        // it only widens the scan — and the merge must still be emitted, since a
        // zero-row staging table is exactly the no-op case that has to succeed.
        let cols = vec![
            col("id", DataType::Int64, false),
            col("amount", DataType::Float64, true),
        ];
        let key = vec!["id".to_string()];
        let sql = build_merge_sql(
            "p",
            "d",
            "t",
            "s",
            &key,
            &cols,
            None,
            true,
            false,
            None,
            &no_bounds(),
            None,
        )
        .unwrap();
        assert!(!sql.contains("BETWEEN"), "{sql}");
        assert!(sql.contains("ON T.`id` = S.`id` WHEN MATCHED"), "{sql}");
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"), "{sql}");
    }

    #[test]
    fn build_merge_sql_delete_stale_drops_the_delete_when_staging_has_no_window() {
        // The DELETE is only ever allowed scoped. With no resolvable window
        // there is also nothing staged to replace, so the clause is dropped —
        // never emitted unscoped, which would delete the whole destination.
        let cols = vec![
            col("id", DataType::Int64, false),
            col(
                "create_date",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ];
        let key = vec!["id".to_string()];
        let sql = build_merge_sql(
            "p",
            "d",
            "orders",
            "orders_tmp",
            &key,
            &cols,
            Some("create_date"),
            false,
            true,
            None,
            &no_bounds(),
            None,
        )
        .unwrap();
        assert!(
            !sql.contains("WHEN NOT MATCHED BY SOURCE"),
            "an unscoped delete would wipe the destination: {sql}"
        );
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"), "{sql}");
    }

    #[test]
    fn build_staging_bounds_sql_reads_min_max_as_bigquery_rendered_literals() {
        let sql = build_staging_bounds_sql("p", "d", "orders_tmp", &["id", "create_date"]);
        assert_eq!(
            sql,
            "SELECT FORMAT('%T', MIN(`id`)) AS lo_0, FORMAT('%T', MAX(`id`)) AS hi_0, \
             FORMAT('%T', MIN(`create_date`)) AS lo_1, \
             FORMAT('%T', MAX(`create_date`)) AS hi_1 \
             FROM `p`.`d`.`orders_tmp`"
        );
    }

    #[test]
    fn bounded_columns_probes_only_what_gets_bounded() {
        let key = vec!["id".to_string(), "line".to_string()];
        // Nothing pruned -> nothing probed, so no extra query at all.
        assert!(bounded_columns(&key, None, false, false).is_empty());
        // Key-range pruning covers every key column.
        assert_eq!(bounded_columns(&key, None, true, false), vec!["id", "line"]);
        // The prune column comes first, and is not repeated when it is also a
        // key column.
        assert_eq!(
            bounded_columns(&key, Some("id"), true, false),
            vec!["id", "line"]
        );
        assert_eq!(
            bounded_columns(&key, Some("create_date"), true, false),
            vec!["create_date", "id", "line"]
        );
        // `delete_stale` suppresses the key-range bound, so only the window
        // column is worth probing.
        assert_eq!(
            bounded_columns(&key, Some("create_date"), true, true),
            vec!["create_date"]
        );
    }

    // ---- live BigQuery ----
    //
    // These are the checks 0.14.0 shipped without. Everything above asserts on
    // the *text* of the statement, and no amount of that could have caught the
    // bug: the bound it emitted was rejected while BigQuery ANALYSED the query
    // ("Unsupported subquery with table in join predicate"), so the only test
    // that fails is one that hands the statement to BigQuery. Executing it once
    // — even as a dry run — would have caught a default-on feature that broke
    // every BigQuery incremental transfer.

    /// Gate for the live tests: `QUICKHOUSE_BQ_PROJECT` and
    /// `QUICKHOUSE_BQ_DATASET` must name a dataset this process may create,
    /// write and drop tables in. Credentials resolve the way a real run's do
    /// (ADC, or `GOOGLE_APPLICATION_CREDENTIALS`). Unset means skip, matching
    /// how the Python integration tests treat an unreachable service.
    fn live_bq_dataset() -> Option<(String, String)> {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        match (var("QUICKHOUSE_BQ_PROJECT"), var("QUICKHOUSE_BQ_DATASET")) {
            (Some(project), Some(dataset)) => Some((project, dataset)),
            _ => {
                eprintln!(
                    "skipping live BigQuery MERGE test: set QUICKHOUSE_BQ_PROJECT and \
                     QUICKHOUSE_BQ_DATASET to run it"
                );
                None
            }
        }
    }

    async fn live_sink(project_id: String, dataset_id: String) -> BigQuerySink {
        // A real run gets its process-level rustls CryptoProvider from the
        // transfer entry points (`sync::run_transfer_impl`,
        // `reconcile::reconcile_keys`). These tests build the sink directly and
        // so bypass both — without this they panic inside rustls before a
        // single request leaves the process, which is not a product failure but
        // does make the whole live suite unrunnable. Idempotent; the error just
        // means someone else got here first.
        let _ = rustls::crypto::ring::default_provider().install_default();
        BigQuerySink::new(BigQueryDestConfig {
            project_id: Some(project_id),
            credentials_file: None,
            credentials_json: None,
            dataset_id,
            // Appends are committed, so staged rows are readable by the MERGE
            // immediately — and it is what a real run defaults to since 0.14.
            write_method: BigQueryWriteMethod::StorageWrite,
            archive: None,
        })
        .await
        .expect("authenticate against BigQuery")
    }

    fn temp_table(tag: &str) -> String {
        format!(
            "quickhouse_merge_it_{tag}_{}",
            time::OffsetDateTime::now_utc().unix_timestamp_nanos()
        )
    }

    /// Executes what `merge_prune_key_range` (default on) generates, including
    /// the zero-row case that failed first in production.
    ///
    /// Row counts are deliberately not asserted: a table's `num_rows` metadata
    /// is eventually consistent, and what broke — and what this pins — is
    /// whether BigQuery accepts and runs the statement at all.
    #[tokio::test]
    async fn live_merge_runs_with_key_range_pruning_on() {
        let Some((project, dataset)) = live_bq_dataset() else {
            return;
        };
        let sink = live_sink(project, dataset).await;
        let cols = vec![
            col("id", DataType::Int64, false),
            col("amount", DataType::Float64, true),
            col("name", DataType::Utf8, true),
        ];
        let key = vec!["id".to_string()];
        let dest = temp_table("keyrange");
        let staging = format!("{dest}_quickhouse_tmp");
        let empty = format!("{dest}_empty_tmp");
        let mut cfg = base_cfg();
        cfg.dest_table = dest.clone();
        cfg.key = key.clone();

        // Work first, cleanup second, assert last — so a failure cannot leave
        // scratch tables behind in the dataset.
        let outcome = async {
            sink.create_table(&dest, &cols, &cfg).await?;
            sink.create_table(&staging, &cols, &cfg).await?;
            sink.create_table(&empty, &cols, &cfg).await?;
            let batch = sample_batch();
            let staged = sink
                .insert_batches(&staging, batch.schema(), &[batch])
                .await?;
            // Keys not in the destination yet: the INSERT branch.
            sink.merge_into(&dest, &staging, &key, &cols, None, true, 0, false, None)
                .await?;
            // The same keys again: the UPDATE branch, which is what a bound on
            // the key range has to leave reachable.
            sink.merge_into(&dest, &staging, &key, &cols, None, true, 0, false, None)
                .await?;
            // Nothing staged at all — MIN/MAX are NULL, so there is no bound to
            // emit. This is the run that first failed in production ("partition
            // 'all' complete: 0 rows", then a failed merge job).
            sink.merge_into(&dest, &empty, &key, &cols, None, true, 0, false, None)
                .await?;
            Ok::<u64, EtlError>(staged)
        }
        .await;
        for t in [&dest, &staging, &empty] {
            let _ = sink.drop_table(t).await;
        }
        // `insert_batches` returns an approximate wire-BYTES count, not a row
        // count (see the `Sink` trait). This asserted `== 2` — a row count —
        // and that is precisely why every one of these live tests failed the
        // first time they were ever executed: the assertion was written
        // alongside a test nobody could run. What this pins is that BigQuery
        // accepted and ran every statement above; see the test docs for why
        // row counts are deliberately not asserted.
        let bytes = outcome.expect("merge with key-range pruning");
        assert!(bytes > 0, "the staging insert should have sent some bytes");
    }

    /// The 0.15 additions: the exact key-list bound
    /// (`merge_prune_key_list_max`) and the clustering probe that now rides
    /// alongside every merge.
    ///
    /// This is the test whose absence let 0.14.0 ship. `build_merge_sql`'s unit
    /// tests assert on statement *text*, and text cannot tell you whether
    /// BigQuery accepts the statement — 0.14.0's `BETWEEN (SELECT MIN(k) ...)`
    /// form was perfectly reasonable text that BigQuery rejects at analysis
    /// time, so every unit test passed while 100% of production merges failed.
    /// A key list is a new predicate shape in the `ON` clause and has to be
    /// handed to the real engine at least once.
    ///
    /// Row counts are deliberately not asserted, matching the tests above: a
    /// table's `num_rows` metadata is eventually consistent, and what this pins
    /// is whether BigQuery accepts and runs the statement at all.
    #[tokio::test]
    async fn live_merge_runs_with_a_key_list_bound() {
        let Some((project, dataset)) = live_bq_dataset() else {
            return;
        };
        let sink = live_sink(project, dataset).await;
        let cols = vec![
            col("id", DataType::Int64, false),
            col("amount", DataType::Float64, true),
            col("name", DataType::Utf8, true),
        ];
        let key = vec!["id".to_string()];
        let dest = temp_table("keylist");
        let staging = format!("{dest}_quickhouse_tmp");
        let empty = format!("{dest}_empty_tmp");
        let mut cfg = base_cfg();
        cfg.dest_table = dest.clone();
        cfg.key = key.clone();

        let outcome = async {
            sink.create_table(&dest, &cols, &cfg).await?;
            sink.create_table(&staging, &cols, &cfg).await?;
            sink.create_table(&empty, &cols, &cfg).await?;
            let batch = sample_batch();
            let staged = sink
                .insert_batches(&staging, batch.schema(), &[batch])
                .await?;
            // A ceiling comfortably above the batch, so the list actually
            // binds: this is the `T.id IN (…)` form reaching the engine.
            sink.merge_into(&dest, &staging, &key, &cols, None, true, 1_000, false, None)
                .await?;
            // Again, to exercise the UPDATE branch through the same bound — an
            // over-narrow key predicate would fall through to
            // WHEN NOT MATCHED and duplicate the key instead of updating it.
            sink.merge_into(&dest, &staging, &key, &cols, None, true, 1_000, false, None)
                .await?;
            // A ceiling BELOW the batch's distinct-key count: the probe reads
            // one row past the limit, abandons the list, and the run has to
            // degrade to the range bound rather than emit a truncated `IN`.
            sink.merge_into(&dest, &staging, &key, &cols, None, true, 1, false, None)
                .await?;
            // An empty batch resolves no list and no range — no bound at all.
            sink.merge_into(&dest, &empty, &key, &cols, None, true, 1_000, false, None)
                .await?;
            // The clustering probe runs concurrently with every merge above;
            // call it directly too, so a broken INFORMATION_SCHEMA query is a
            // failure here rather than a silently swallowed `debug!`.
            let clustering = sink.clustering_columns(&dest).await?;
            assert_eq!(
                clustering.as_deref(),
                Some(&["id".to_string()][..]),
                "quickhouse's own generated DDL must cluster by the merge key — \
                 if this drifts, every merge silently full-scans"
            );
            // And the destination is not reported as clustered by something
            // else, which is what would make the warning fire spuriously.
            Ok::<u64, EtlError>(staged)
        }
        .await;
        for t in [&dest, &staging, &empty] {
            let _ = sink.drop_table(t).await;
        }
        // Bytes, not rows — see `live_merge_runs_with_key_range_pruning_on`.
        let bytes = outcome.expect("merge with a key-list bound");
        assert!(bytes > 0, "the staging insert should have sent some bytes");
    }

    #[tokio::test]
    async fn live_reconcile_deletes_only_the_orphans_in_the_window() {
        // `reconcile_keys`' BigQuery half: `distinct_keys` must render keys the
        // same way a source does, and `delete_keys` must write them back as
        // correctly typed literals. Both are new in 0.15 and neither is
        // provable from statement text.
        let Some((project, dataset)) = live_bq_dataset() else {
            return;
        };
        let sink = live_sink(project, dataset).await;
        let cols = vec![
            col("id", DataType::Int64, false),
            col("amount", DataType::Float64, true),
            col("name", DataType::Utf8, true),
        ];
        let dest = temp_table("reconcile");
        let mut cfg = base_cfg();
        cfg.dest_table = dest.clone();
        cfg.key = vec!["id".to_string()];

        let outcome = async {
            sink.create_table(&dest, &cols, &cfg).await?;
            let batch = sample_batch();
            sink.insert_batches(&dest, batch.schema(), &[batch]).await?;
            // Text rendering: an INT64 key must come back as bare digits, or a
            // diff against a source's own text keys finds nothing in common.
            let mut keys = sink.distinct_keys(&dest, "id", None).await?;
            keys.sort();
            assert!(
                keys.iter().all(|k| k.parse::<i64>().is_ok()),
                "INT64 keys must render as plain integers, got {keys:?}"
            );
            assert!(!keys.is_empty(), "sample batch should have landed rows");
            // Delete exactly one key, scoped by a window, and check the count.
            let one = vec![keys[0].clone()];
            let deleted = sink.delete_keys(&dest, "id", &one, Some("TRUE")).await?;
            assert_eq!(deleted, 1, "one key, one row");
            let after = sink.distinct_keys(&dest, "id", None).await?;
            assert!(
                !after.contains(&keys[0]),
                "the deleted key must be gone; {after:?} still has {}",
                keys[0]
            );
            // A window that excludes the key deletes nothing.
            let untouched = sink.delete_keys(&dest, "id", &after, Some("FALSE")).await?;
            assert_eq!(untouched, 0, "a window matching no rows deletes nothing");
            Ok::<(), EtlError>(())
        }
        .await;
        let _ = sink.drop_table(&dest).await;
        outcome.expect("reconcile against BigQuery");
    }

    /// The same for `merge_prune_partition_by` + `delete_stale_in_window`: the
    /// opt-in pair that emitted the identical rejected form — in the `ON` clause
    /// and in the `WHEN NOT MATCHED BY SOURCE` condition — and so had never
    /// worked on BigQuery either, latent since 0.4.0 only because nobody had
    /// adopted it.
    #[tokio::test]
    async fn live_merge_runs_with_partition_pruning_and_stale_delete() {
        let Some((project, dataset)) = live_bq_dataset() else {
            return;
        };
        let sink = live_sink(project, dataset).await;
        let cols = vec![
            col("id", DataType::Int64, false),
            col(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            col("amount", DataType::Float64, true),
        ];
        let key = vec!["id".to_string()];
        let dest = temp_table("window");
        let staging = format!("{dest}_quickhouse_tmp");
        let empty = format!("{dest}_empty_tmp");
        let mut cfg = base_cfg();
        cfg.dest_table = dest.clone();
        cfg.key = key.clone();
        // A real partitioned + clustered destination, the shape both prunes
        // exist to exploit.
        cfg.partition_by = Some("created_at".into());

        let outcome = async {
            sink.create_table(&dest, &cols, &cfg).await?;
            sink.create_table(&staging, &cols, &cfg).await?;
            sink.create_table(&empty, &cols, &cfg).await?;
            let batch = partitioned_batch();
            let staged = sink
                .insert_batches(&staging, batch.schema(), &[batch])
                .await?;
            // `delete_stale` suppresses the key-range bound, so this exercises
            // the window bound in both of the places it appears.
            sink.merge_into(
                &dest,
                &staging,
                &key,
                &cols,
                Some("created_at"),
                true,
                0,
                true,
                None,
            )
            .await?;
            // No window at all: the DELETE clause has to be dropped rather than
            // emitted unscoped, which would wipe the destination.
            sink.merge_into(
                &dest,
                &empty,
                &key,
                &cols,
                Some("created_at"),
                true,
                0,
                true,
                None,
            )
            .await?;
            Ok::<u64, EtlError>(staged)
        }
        .await;
        for t in [&dest, &staging, &empty] {
            let _ = sink.drop_table(t).await;
        }
        // Bytes, not rows — see `live_merge_runs_with_key_range_pruning_on`.
        let bytes = outcome.expect("merge with a window-scoped delete");
        assert!(bytes > 0, "the staging insert should have sent some bytes");
    }

    fn partitioned_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("amount", DataType::Float64, true),
        ]));
        let id: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        // 2026-08-20T00:00:00Z and one hour later, in microseconds.
        let created_at: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![1_787_184_000_000_000, 1_787_187_600_000_000])
                .with_timezone("UTC"),
        );
        let amount: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.5), None]));
        RecordBatch::try_new(schema, vec![id, created_at, amount]).unwrap()
    }

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let id: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let amount: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.5), None]));
        let name: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
        RecordBatch::try_new(schema, vec![id, amount, name]).unwrap()
    }

    #[test]
    fn batch_row_to_json_covers_values_and_nulls() {
        let batch = sample_batch();
        let row0 = batch_row_to_json(&batch, 0).unwrap();
        assert_eq!(row0["id"], Value::from(1));
        assert_eq!(row0["amount"], Value::from(1.5));
        assert_eq!(row0["name"], Value::from("a"));

        let row1 = batch_row_to_json(&batch, 1).unwrap();
        assert_eq!(row1["id"], Value::from(2));
        assert_eq!(row1["amount"], Value::Null);
        assert_eq!(row1["name"], Value::Null);
    }

    #[test]
    fn date32_to_iso_epoch_and_offset() {
        assert_eq!(date32_to_iso(0), "1970-01-01");
        assert_eq!(date32_to_iso(19_723), "2024-01-01");
    }

    #[test]
    fn timestamp_micros_to_iso_naive_vs_tz_aware() {
        // 2024-01-01T00:00:00 UTC in microseconds since epoch.
        let micros = 1_704_067_200_000_000;
        assert_eq!(
            timestamp_micros_to_iso(micros, false).unwrap(),
            "2024-01-01T00:00:00.000000"
        );
        assert_eq!(
            timestamp_micros_to_iso(micros, true).unwrap(),
            "2024-01-01T00:00:00.000000Z"
        );
    }

    #[test]
    fn unique_job_id_sanitizes_table_name_and_stays_unique() {
        let a = unique_job_id("swap", "my.weird-table!");
        let b = unique_job_id("swap", "my.weird-table!");
        assert!(a.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
        assert_ne!(a, b, "two calls must not collide");
    }
}
