//! BigQuery sink: auth, structured DDL (no SQL-string templating, unlike
//! ClickHouse — see `build_table`), and writes via the `tabledata.insertAll`
//! streaming-insert API.
//!
//! Three ways exist to write into BigQuery; this is deliberately the
//! simplest one that needs zero new dependencies. The Storage Write API is
//! the modern, free, highest-throughput path, but requires hand-building a
//! dynamic protobuf encoder (no crate support for arbitrary runtime schemas)
//! — a large, separate undertaking, left as a documented future enhancement
//! (see the README). Load jobs are cheap and BigQuery's own recommended bulk
//! path, but this crate only supports them via a GCS `source_uris` staging
//! file, which would add a hard Cloud Storage dependency and a bucket
//! requirement just for this destination. `insertAll` needs neither: it's a
//! plain JSON POST over the REST API this crate already wraps.
//!
//! The atomic full-refresh swap (ClickHouse's `EXCHANGE TABLES`) has no
//! single-statement BigQuery equivalent; the nearest is a COPY job with
//! `WRITE_TRUNCATE`, which — like `EXCHANGE TABLES` — is atomic and (unlike
//! `CREATE OR REPLACE TABLE ... AS SELECT`) doesn't re-read/bill for
//! rewriting every row through a query.

use std::time::Duration;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use base64::Engine;
use google_cloud_bigquery::client::google_cloud_auth::credentials::CredentialsFile;
use google_cloud_bigquery::client::{Client, ClientConfig};
use google_cloud_bigquery::http::error::Error as BqError;
use google_cloud_bigquery::http::job::get::GetJobRequest;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::http::job::{
    CreateDisposition, Job, JobConfiguration, JobConfigurationQuery, JobConfigurationSourceTable,
    JobConfigurationTableCopy, JobReference, JobState, JobType, OperationType, WriteDisposition,
};
use google_cloud_bigquery::http::table::{
    Clustering, Table, TableFieldMode, TableFieldSchema, TableFieldType, TableReference, TableSchema,
    TimePartitionType, TimePartitioning,
};
use google_cloud_bigquery::http::tabledata::insert_all::{InsertAllRequest, Row as InsertRow};
use google_cloud_bigquery::query::row::Row as QueryRow;
use google_cloud_bigquery::storage_write::stream::default::DefaultStream;
use google_cloud_bigquery::storage_write::AppendRowsRequestBuilder;
use prost_types::DescriptorProto;
use serde_json::Value;

use crate::config::{BigQueryDestConfig, BigQueryWriteMethod, TransferConfig};
use crate::error::{EtlError, Result};
use crate::sink::bigquery_proto::{build_proto_descriptor, encode_row, resolve_fields};
use crate::sink::{backoff_delay, SendError, MAX_INSERT_ATTEMPTS};
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

const STATE_TABLE: &str = "_quickhouse_state";

#[derive(Clone)]
pub struct BigQuerySink {
    client: Client,
    project_id: String,
    dataset_id: String,
    write_method: BigQueryWriteMethod,
}

impl BigQuerySink {
    /// Authenticate and hold a client for the lifetime of the sink (unlike
    /// the read side's `BigQuerySource::connect`, which reconnects fresh
    /// each call — here we make far more calls per transfer: schema
    /// creation, many inserts, the swap job, watermark read/persist — so a
    /// held client amortizes the auth handshake).
    pub async fn new(cfg: BigQueryDestConfig) -> Result<Self> {
        let (config, resolved_project) = match &cfg.credentials_file {
            Some(path) => {
                let cred = CredentialsFile::new_from_file(path.clone())
                    .await
                    .map_err(|e| EtlError::other(format!("bigquery credentials error: {e}")))?;
                ClientConfig::new_with_credentials(cred)
                    .await
                    .map_err(|e| EtlError::other(format!("bigquery auth error: {e}")))?
            }
            None => ClientConfig::new_with_auth()
                .await
                .map_err(|e| EtlError::other(format!("bigquery auth error: {e}")))?,
        };
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
        })
    }

    pub async fn table_exists(&self, table: &str) -> Result<bool> {
        match self.client.table().get(&self.project_id, &self.dataset_id, table).await {
            Ok(_) => Ok(true),
            Err(e) if is_not_found(&e) => Ok(false),
            Err(e) => Err(EtlError::other(format!("bigquery table get error: {e}"))),
        }
    }

    /// Build and run this destination's own structured `Table` creation —
    /// no DDL string templating, unlike ClickHouse (BigQuery's REST API
    /// takes a schema object directly).
    pub async fn create_table(&self, table: &str, columns: &[ColumnType], cfg: &TransferConfig) -> Result<()> {
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

    /// Insert Arrow batches, dispatching on the configured write method:
    /// `insertAll` (default) or the Storage Write API (opt-in). Both share the
    /// transient-failure retry/backoff policy of the ClickHouse sink.
    pub async fn insert_batches(&self, table: &str, schema: SchemaRef, batches: &[RecordBatch]) -> Result<u64> {
        match self.write_method {
            BigQueryWriteMethod::InsertAll => self.insert_batches_insert_all(table, batches).await,
            BigQueryWriteMethod::StorageWrite => self.insert_batches_storage_write(table, schema, batches).await,
        }
    }

    /// Insert Arrow batches via `tabledata.insertAll`, chunked to stay under
    /// its per-request row limit. Returns an approximate JSON-payload-bytes-sent
    /// count (an accounting detail, not exact).
    async fn insert_batches_insert_all(&self, table: &str, batches: &[RecordBatch]) -> Result<u64> {
        if batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(0);
        }
        let mut total_bytes = 0u64;
        for batch in batches {
            let mut start = 0usize;
            while start < batch.num_rows() {
                let end = (start + INSERT_ALL_MAX_ROWS_PER_REQUEST).min(batch.num_rows());
                let rows = (start..end)
                    .map(|r| {
                        batch_row_to_json(batch, r)
                            .map(|json| InsertRow { insert_id: None, json: Value::Object(json) })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let request = InsertAllRequest { rows, ..Default::default() };
                total_bytes += serde_json::to_vec(&request).map(|b| b.len() as u64).unwrap_or(0);

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
    async fn try_insert(&self, table: &str, request: &InsertAllRequest<Value>) -> std::result::Result<(), SendError> {
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
    /// row is protobuf-encoded (see [`super::bigquery_proto`]) and appended to
    /// the table's `_default` stream — at-least-once, matching insertAll's
    /// semantics; idempotency for incremental syncs comes from the staging +
    /// MERGE flow, and for full-refresh from writing to staging then swapping.
    /// Returns the total encoded protobuf bytes sent.
    async fn insert_batches_storage_write(&self, table: &str, schema: SchemaRef, batches: &[RecordBatch]) -> Result<u64> {
        if batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(0);
        }
        let fields = resolve_fields(&schema)?;
        let descriptor = build_proto_descriptor(&fields)?;
        // `_default` is a persistent server-side stream; this fetches its
        // handle (a lightweight GetWriteStream), created once per call.
        let resource = format!("projects/{}/datasets/{}/tables/{table}", self.project_id, self.dataset_id);
        let stream = self
            .client
            .default_storage_writer()
            .create_write_stream(&resource)
            .await
            .map_err(|e| EtlError::other(format!("bigquery storage-write: open stream for {resource}: {e}")))?;

        let mut total_bytes = 0u64;
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

                let mut attempt = 0u32;
                loop {
                    attempt += 1;
                    // Clone per attempt: retries are rare, and the builder
                    // consumes the row bytes.
                    match self.try_append(&stream, &descriptor, serialized.clone(), table).await {
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
                start = end;
            }
        }
        Ok(total_bytes)
    }

    /// One `AppendRows` attempt: append the chunk and drain the response
    /// stream, surfacing both transport failures and in-band append/row errors
    /// classified for retry (transient) vs. immediate failure (permanent).
    async fn try_append(
        &self,
        stream: &DefaultStream,
        descriptor: &DescriptorProto,
        rows: Vec<Vec<u8>>,
        table: &str,
    ) -> std::result::Result<(), SendError> {
        let builder = AppendRowsRequestBuilder::new(descriptor.clone(), rows);
        let mut resp = stream
            .append_rows(vec![builder])
            .await
            .map_err(|s| classify_status(&s, "bigquery storage-write append"))?;
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
                    return Err(classify_rpc_status(&status, table, &self.dataset_id));
                }
            }
        }
        Ok(())
    }

    /// Atomically replace `dest`'s contents with `staging`'s via a
    /// `WRITE_TRUNCATE` copy job (see the module docs for why this, not
    /// `CREATE OR REPLACE TABLE ... AS SELECT`).
    pub async fn atomic_swap(&self, dest: &str, staging: &str) -> Result<()> {
        let job = Job {
            job_reference: JobReference {
                project_id: self.project_id.clone(),
                job_id: unique_job_id("swap", dest),
                location: None,
            },
            configuration: JobConfiguration {
                job: JobType::Copy(JobConfigurationTableCopy {
                    source_table: JobConfigurationSourceTable::SourceTable(TableReference {
                        project_id: self.project_id.clone(),
                        dataset_id: self.dataset_id.clone(),
                        table_id: staging.to_string(),
                    }),
                    destination_table: TableReference {
                        project_id: self.project_id.clone(),
                        dataset_id: self.dataset_id.clone(),
                        table_id: dest.to_string(),
                    },
                    create_disposition: Some(CreateDisposition::CreateIfNeeded),
                    write_disposition: Some(WriteDisposition::WriteTruncate),
                    operation_type: Some(OperationType::Copy),
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
            .map_err(|e| EtlError::other(format!("bigquery copy job error: {e}")))?;
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
    pub async fn merge_into(&self, dest: &str, staging: &str, key: &[String], columns: &[ColumnType]) -> Result<()> {
        let query = build_merge_sql(&self.project_id, &self.dataset_id, dest, staging, key, columns)?;
        let job = Job {
            job_reference: JobReference {
                project_id: self.project_id.clone(),
                job_id: unique_job_id("merge", dest),
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
            .map_err(|e| EtlError::other(format!("bigquery merge job error: {e}")))?;
        self.poll_job_until_done(created).await?;
        Ok(())
    }

    /// Idempotent, matching ClickHouse's `DROP TABLE IF EXISTS`: a
    /// not-found is success, not an error.
    pub async fn drop_table(&self, table: &str) -> Result<()> {
        match self.client.table().delete(&self.project_id, &self.dataset_id, table).await {
            Ok(()) => Ok(()),
            Err(e) if is_not_found(&e) => Ok(()),
            Err(e) => Err(EtlError::other(format!("bigquery table delete error: {e}"))),
        }
    }

    /// Create the internal `_quickhouse_state` watermark-tracking table if
    /// it doesn't exist yet. Unlike ClickHouse's `CREATE TABLE IF NOT
    /// EXISTS`, BigQuery's table creation has no such clause, so the
    /// existence check happens here explicitly.
    pub async fn ensure_state_table(&self) -> Result<()> {
        if self.table_exists(STATE_TABLE).await? {
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
                table_id: STATE_TABLE.to_string(),
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

    /// Read the last persisted watermark for this `(source, dest_table)` pair.
    pub async fn read_last_watermark(&self, cfg: &TransferConfig) -> Result<Option<String>> {
        if !self.table_exists(STATE_TABLE).await? {
            return Ok(None);
        }
        let source_id = cfg.source_table.clone().or_else(|| cfg.source_query.clone()).unwrap_or_default();
        let query = format!(
            "SELECT last_watermark FROM `{}`.`{}`.`{STATE_TABLE}` \
             WHERE source_table = '{}' AND dest_table = '{}' \
             ORDER BY run_ts DESC LIMIT 1",
            self.project_id,
            self.dataset_id,
            escape_sql_string(&source_id),
            escape_sql_string(&cfg.dest_table),
        );
        let request = QueryRequest { query, ..Default::default() };
        let mut iter = self
            .client
            .query::<QueryRow>(&self.project_id, request)
            .await
            .map_err(|e| EtlError::other(format!("bigquery query error: {e}")))?;
        match iter.next().await.map_err(|e| EtlError::other(format!("bigquery row error: {e}")))? {
            Some(row) => row
                .column::<Option<String>>(0)
                .map_err(|e| EtlError::other(format!("bigquery column error: {e}"))),
            None => Ok(None),
        }
    }

    /// Persist a new watermark after a successful incremental run, via a
    /// DML `INSERT` run as a query job (BigQuery executes DML through the
    /// same job mechanism as `SELECT`).
    pub async fn persist_watermark(&self, cfg: &TransferConfig, watermark: &str, rows: u64) -> Result<()> {
        let source_id = cfg.source_table.clone().or_else(|| cfg.source_query.clone()).unwrap_or_default();
        let query = format!(
            "INSERT INTO `{}`.`{}`.`{STATE_TABLE}` (source_table, dest_table, last_watermark, rows, run_ts) \
             VALUES ('{}', '{}', '{}', {rows}, CURRENT_TIMESTAMP())",
            self.project_id,
            self.dataset_id,
            escape_sql_string(&source_id),
            escape_sql_string(&cfg.dest_table),
            escape_sql_string(watermark),
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
        let created = self
            .client
            .job()
            .create(&job)
            .await
            .map_err(|e| EtlError::other(format!("bigquery persist_watermark job error: {e}")))?;
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
                    &GetJobRequest { location: job.job_reference.location.clone() },
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
    let err = EtlError::other(format!("{context}: {} ({:?})", status.message(), status.code()));
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
fn classify_rpc_status(status: &google_cloud_googleapis::rpc::Status, table: &str, dataset: &str) -> SendError {
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
/// state queries — matches the escaping already used for BigQuery in
/// `sync.rs`'s `build_watermark_filter_bigquery`, for consistency).
fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// Build the `MERGE` statement that upserts `staging`'s rows into `dest`,
/// matched on `key` — see `BigQuerySink::merge_into`'s docs. A free function
/// (not a `&self` method) so it's unit-testable without a real authenticated
/// client, mirroring `build_table`.
fn build_merge_sql(
    project_id: &str,
    dataset_id: &str,
    dest: &str,
    staging: &str,
    key: &[String],
    columns: &[ColumnType],
) -> Result<String> {
    if key.is_empty() {
        // Should already be caught by sync.rs's prepare_target validation
        // before staging is even created — defensive, not a real user-facing path.
        return Err(EtlError::internal(
            "build_merge_sql called with an empty key (should have been validated before staging began)",
        ));
    }
    let on_clause = key.iter().map(|k| format!("T.`{k}` = S.`{k}`")).collect::<Vec<_>>().join(" AND ");

    let all_cols: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    let update_cols: Vec<&str> = all_cols.iter().copied().filter(|c| !key.iter().any(|k| k == c)).collect();

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
    let insert_cols = all_cols.iter().map(|c| format!("`{c}`")).collect::<Vec<_>>().join(", ");
    let insert_vals = all_cols.iter().map(|c| format!("S.`{c}`")).collect::<Vec<_>>().join(", ");
    clauses.push(format!("WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"));

    Ok(format!(
        "MERGE INTO `{project_id}`.`{dataset_id}`.`{dest}` T \
         USING `{project_id}`.`{dataset_id}`.`{staging}` S \
         ON {on_clause} {}",
        clauses.join(" "),
    ))
}

/// Build this destination's own `CREATE TABLE`-equivalent (a structured
/// `Table`, not a DDL string — see the module docs). A free function (not a
/// `&self` method) so it's unit-testable without a real authenticated
/// client.
fn build_table(
    project_id: &str,
    dataset_id: &str,
    table: &str,
    columns: &[ColumnType],
    cfg: &TransferConfig,
) -> Result<Table> {
    let mut fields = Vec::with_capacity(columns.len());
    for c in columns {
        let data_type = match cfg.type_overrides.get(&c.name) {
            Some(ov) => serde_json::from_value(Value::String(ov.clone())).map_err(|_| {
                EtlError::config(format!(
                    "invalid BigQuery type override '{ov}' for column '{}': expected a BigQuery type \
                     name like \"NUMERIC\" or \"BIGNUMERIC\"",
                    c.name
                ))
            })?,
            None => arrow_to_bigquery_type(&c.arrow).ok_or_else(|| EtlError::UnsupportedType {
                engine: "BigQuery",
                column: c.name.clone(),
                type_name: format!("{:?}", c.arrow),
            })?,
        };
        fields.push(TableFieldSchema {
            name: c.name.clone(),
            data_type,
            mode: Some(if c.nullable { TableFieldMode::Nullable } else { TableFieldMode::Required }),
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
    let clustering = if cluster_cols.is_empty() { None } else { Some(Clustering { fields: cluster_cols }) };

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
        other => {
            return Err(EtlError::internal(format!(
                "no BigQuery JSON conversion implemented for Arrow type {other:?}"
            )))
        }
    })
}

fn downcast<T: 'static>(col: &dyn Array) -> Result<&T> {
    col.as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| EtlError::internal("Arrow array downcast failed (schema/builder type mismatch)"))
}

/// `NaN`/`Infinity` aren't representable in JSON — coerced to `null` rather
/// than erroring (matches this crate's general policy of degrading gracefully
/// on unrepresentable values rather than aborting the whole transfer).
fn json_float(v: f64) -> Value {
    serde_json::Number::from_f64(v).map(Value::Number).unwrap_or(Value::Null)
}

fn date32_to_iso(days: i32) -> String {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    (epoch + chrono::Duration::days(days as i64)).format("%Y-%m-%d").to_string()
}

/// `has_tz` distinguishes BigQuery `DATETIME` (naive, no suffix) from
/// `TIMESTAMP` (UTC instant, trailing `Z`) — matching `arrow_to_bigquery_type`'s
/// `Timestamp(µs, None) -> Datetime` / `Timestamp(µs, Some(_)) -> Timestamp` split.
/// `pub(crate)` so the Storage Write proto encoder (`bigquery_proto`) reuses the
/// same civil-string format for `DATETIME` columns.
pub(crate) fn timestamp_micros_to_iso(micros: i64, has_tz: bool) -> Result<String> {
    let secs = micros.div_euclid(1_000_000);
    let nanos = (micros.rem_euclid(1_000_000) * 1000) as u32;
    let dt = chrono::DateTime::from_timestamp(secs, nanos)
        .ok_or_else(|| EtlError::internal(format!("timestamp {micros} (µs) out of representable range")))?;
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
        }
    }

    fn base_cfg() -> TransferConfig {
        TransferConfig {
            source_table: Some("t".into()),
            source_query: None,
            dest_table: "t".into(),
            mode: crate::config::SyncMode::Full,
            watermark: None,
            lookback_seconds: 0,
            key: vec![],
            create_if_missing: true,
            engine: None,
            order_by: vec![],
            partition_by: None,
            primary_key: vec![],
            parallelism: 1,
            batch_rows: 1000,
            batch_bytes: 0,
            max_memory_bytes: 0,
            partition_column: None,
            type_overrides: HashMap::new(),
            rename: HashMap::new(),
            include: vec![],
            exclude: vec![],
        }
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
        assert_eq!(t.schema.unwrap().fields[0].data_type, TableFieldType::Numeric);
    }

    #[test]
    fn build_table_rejects_invalid_type_override() {
        let cols = vec![col("amount", DataType::Float64, false)];
        let mut cfg = base_cfg();
        cfg.type_overrides.insert("amount".into(), "NOT_A_REAL_TYPE".into());
        assert!(build_table("p", "d", "t", &cols, &cfg).is_err());
    }

    #[test]
    fn build_table_clustering_from_order_by_and_key_deduped() {
        let cols = vec![col("a", DataType::Int64, false), col("b", DataType::Int64, false)];
        let mut cfg = base_cfg();
        cfg.order_by = vec!["a".into(), "b".into()];
        cfg.key = vec!["a".into()]; // duplicate of order_by[0], must not double up
        let t = build_table("p", "d", "t", &cols, &cfg).unwrap();
        assert_eq!(t.clustering.unwrap().fields, vec!["a", "b"]);
    }

    #[test]
    fn build_table_rejects_more_than_four_clustering_columns() {
        let cols = (0..5).map(|i| col(&format!("c{i}"), DataType::Int64, false)).collect::<Vec<_>>();
        let mut cfg = base_cfg();
        cfg.order_by = cols.iter().map(|c| c.name.clone()).collect();
        let err = build_table("p", "d", "t", &cols, &cfg).unwrap_err().to_string();
        assert!(err.contains("at most 4"), "{err}");
    }

    #[test]
    fn build_table_partition_by_requires_date_like_column() {
        let cols = vec![col("id", DataType::Int64, false)];
        let mut cfg = base_cfg();
        cfg.partition_by = Some("id".into());
        let err = build_table("p", "d", "t", &cols, &cfg).unwrap_err().to_string();
        assert!(err.contains("DATE/DATETIME/TIMESTAMP"), "{err}");
    }

    #[test]
    fn build_table_partition_by_missing_column_errors_clearly() {
        let cols = vec![col("id", DataType::Int64, false)];
        let mut cfg = base_cfg();
        cfg.partition_by = Some("nonexistent".into());
        let err = build_table("p", "d", "t", &cols, &cfg).unwrap_err().to_string();
        assert!(err.contains("nonexistent"), "{err}");
    }

    #[test]
    fn build_table_partition_by_valid_date_column() {
        let cols = vec![col("id", DataType::Int64, false), col("event_date", DataType::Date32, false)];
        let mut cfg = base_cfg();
        cfg.partition_by = Some("event_date".into());
        let t = build_table("p", "d", "t", &cols, &cfg).unwrap();
        let tp = t.time_partitioning.unwrap();
        assert_eq!(tp.field.as_deref(), Some("event_date"));
        assert_eq!(tp.partition_type, TimePartitionType::Day);
    }

    #[test]
    fn build_merge_sql_matches_on_key_and_updates_non_key_columns() {
        let cols = vec![
            col("id", DataType::Int64, false),
            col("name", DataType::Utf8, true),
            col("amount", DataType::Float64, true),
        ];
        let key = vec!["id".to_string()];
        let sql = build_merge_sql("proj", "ds", "orders", "orders_quickhouse_tmp", &key, &cols).unwrap();

        assert!(sql.starts_with("MERGE INTO `proj`.`ds`.`orders` T USING `proj`.`ds`.`orders_quickhouse_tmp` S"));
        assert!(sql.contains("ON T.`id` = S.`id`"));
        assert!(sql.contains("WHEN MATCHED THEN UPDATE SET `name` = S.`name`, `amount` = S.`amount`"));
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT (`id`, `name`, `amount`) VALUES (S.`id`, S.`name`, S.`amount`)"));
        // The key column must never appear in the UPDATE SET list.
        assert!(!sql.contains("`id` = S.`id`,"), "key column leaked into UPDATE SET: {sql}");
    }

    #[test]
    fn build_merge_sql_composite_key() {
        let cols = vec![col("a", DataType::Int64, false), col("b", DataType::Int64, false)];
        let key = vec!["a".to_string(), "b".to_string()];
        let sql = build_merge_sql("p", "d", "t", "s", &key, &cols).unwrap();
        assert!(sql.contains("ON T.`a` = S.`a` AND T.`b` = S.`b`"));
    }

    #[test]
    fn build_merge_sql_all_columns_are_key_becomes_insert_only() {
        let cols = vec![col("id", DataType::Int64, false)];
        let key = vec!["id".to_string()];
        let sql = build_merge_sql("p", "d", "t", "s", &key, &cols).unwrap();
        assert!(!sql.contains("WHEN MATCHED"), "no columns left to update: {sql}");
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT (`id`) VALUES (S.`id`)"));
    }

    #[test]
    fn build_merge_sql_rejects_empty_key() {
        let cols = vec![col("id", DataType::Int64, false)];
        let err = build_merge_sql("p", "d", "t", "s", &[], &cols).unwrap_err().to_string();
        assert!(err.contains("empty key"), "{err}");
        assert!(err.contains("quickhouse bug"), "must be framed as internal, not a config error: {err}");
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
