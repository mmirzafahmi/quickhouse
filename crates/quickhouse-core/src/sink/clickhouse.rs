//! ClickHouse sink over the HTTP interface.
//!
//! Inserts stream Arrow batches serialized as an Arrow IPC stream, ingested by
//! ClickHouse's native `FORMAT ArrowStream`. DDL, reads, and table swaps go
//! through the same HTTP endpoint as plain SQL.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use reqwest::Client;

use crate::config::{ClickHouseConfig, Compression};
use crate::error::{EtlError, Result};

/// Max total attempts for one insert (1 initial + retries).
const MAX_INSERT_ATTEMPTS: u32 = 4;
/// Base backoff; attempt N waits `BASE * 2^(N-1)` (0.25s, 0.5s, 1s, ...).
const BACKOFF_BASE_MS: u64 = 250;

/// Exponential backoff for retry attempt `attempt` (1-based).
fn backoff_delay(attempt: u32) -> std::time::Duration {
    let mult = 1u64 << (attempt.saturating_sub(1)).min(6); // cap the shift
    std::time::Duration::from_millis(BACKOFF_BASE_MS.saturating_mul(mult))
}

/// Outcome of a single insert attempt, telling the caller whether to retry.
enum SendError {
    /// Retryable (dropped connection, timeout, 5xx/429).
    Transient(EtlError),
    /// Deterministic (4xx: bad SQL, auth) — do not retry.
    Permanent(EtlError),
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendError::Transient(e) | SendError::Permanent(e) => write!(f, "{e}"),
        }
    }
}

#[derive(Clone)]
pub struct ClickHouseSink {
    client: Client,
    cfg: Arc<ClickHouseConfig>,
}

impl ClickHouseSink {
    pub fn new(cfg: ClickHouseConfig) -> Result<Self> {
        let client = Client::builder()
            .build()
            .map_err(EtlError::from)?;
        Ok(Self {
            client,
            cfg: Arc::new(cfg),
        })
    }

    pub fn database(&self) -> &str {
        &self.cfg.database
    }

    fn base_request(&self) -> reqwest::RequestBuilder {
        self.client
            .post(&self.cfg.url)
            .header("X-ClickHouse-User", &self.cfg.user)
            .header("X-ClickHouse-Key", &self.cfg.password)
            .query(&[("database", &self.cfg.database)])
    }

    /// Execute a statement that returns no rows (DDL, TRUNCATE, EXCHANGE, ...).
    pub async fn execute(&self, sql: &str) -> Result<()> {
        let resp = self.base_request().body(sql.to_string()).send().await?;
        Self::check(resp).await.map(|_| ())
    }

    /// Run a query expected to return a single scalar; `None` if it returns no rows.
    pub async fn query_scalar(&self, sql: &str) -> Result<Option<String>> {
        let resp = self.base_request().body(sql.to_string()).send().await?;
        let body = Self::check(resp).await?;
        let trimmed = body.trim();
        if trimmed.is_empty() {
            Ok(None)
        } else {
            Ok(Some(trimmed.to_string()))
        }
    }

    pub async fn table_exists(&self, table: &str) -> Result<bool> {
        let sql = format!(
            "EXISTS TABLE {}.{}",
            ident(&self.cfg.database),
            ident(table)
        );
        Ok(self.query_scalar(&sql).await?.as_deref() == Some("1"))
    }

    /// Insert a group of Arrow batches into `table` via `FORMAT ArrowStream`.
    /// Returns the number of bytes sent on the wire (post-compression).
    ///
    /// The Arrow IPC bytes are serialized once into a `Vec`, then compressed
    /// and uploaded as a *stream*: the compressed body is produced and sent
    /// incrementally (chunked transfer encoding) rather than materializing a
    /// second full compressed buffer in memory. Net effect vs. the old path:
    /// the transient per-insert footprint drops from ~3 full copies (IPC Vec +
    /// gzip Vec + reqwest body) to ~1 (the IPC Vec plus a small streaming
    /// window), which keeps `MemoryBudget`'s accounting honest.
    pub async fn insert_batches(
        &self,
        table: &str,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<u64> {
        if batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(0);
        }
        // `Bytes` (not `Vec<u8>`) so each retry attempt re-wraps the payload
        // with a cheap Arc-clone instead of copying the serialized IPC again.
        let payload = Bytes::from(serialize_ipc(schema, batches)?);

        let query = format!(
            "INSERT INTO {}.{} FORMAT ArrowStream",
            ident(&self.cfg.database),
            ident(table)
        );

        // Retry transient failures (dropped/reset connections, timeouts, 5xx/429
        // from ClickHouse Cloud's LB) with exponential backoff — a single blip
        // over a long WAN transfer must not abort the whole run. Deterministic
        // errors (4xx, e.g. bad SQL) are returned immediately, never retried.
        //
        // Delivery is at-least-once: if a batch is fully received and committed
        // but its HTTP ack is lost, the retry re-sends it, duplicating that one
        // batch. Harmless for incremental mode (ReplacingMergeTree collapses by
        // key); for full-refresh into a plain MergeTree it can leave duplicate
        // rows from the single re-sent batch. Preferred over aborting the whole
        // transfer; dedupe downstream if exactness matters.
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let sent = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let mut req = self.base_request().query(&[("query", query.as_str())]);
            let body = match self.cfg.compression {
                Compression::None => counting_body(payload.clone(), sent.clone()),
                Compression::Gzip => {
                    req = req.header("Content-Encoding", "gzip");
                    gzip_body(payload.clone(), sent.clone())
                }
                Compression::Zstd => {
                    req = req.header("Content-Encoding", "zstd");
                    zstd_body(payload.clone(), sent.clone())
                }
            };

            match self.send_insert(req, body).await {
                Ok(()) => return Ok(sent.load(std::sync::atomic::Ordering::Relaxed)),
                Err(SendError::Permanent(e)) => return Err(e),
                Err(SendError::Transient(e)) => {
                    if attempt >= MAX_INSERT_ATTEMPTS {
                        return Err(EtlError::clickhouse(format!(
                            "insert failed after {attempt} attempts: {e}"
                        )));
                    }
                    let delay = backoff_delay(attempt);
                    tracing::warn!(
                        "insert into {}.{} attempt {attempt} failed ({e}); retrying in {:?}",
                        self.cfg.database,
                        table,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// One insert attempt: send the request and classify the outcome so the
    /// caller can decide whether to retry.
    async fn send_insert(
        &self,
        req: reqwest::RequestBuilder,
        body: reqwest::Body,
    ) -> std::result::Result<(), SendError> {
        // A transport-level failure means no response was received at all
        // (connection reset, timeout, DNS, TLS) — always worth retrying.
        let resp = req
            .body(body)
            .send()
            .await
            .map_err(|e| SendError::Transient(EtlError::from(e)))?;

        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        let text = resp.text().await.unwrap_or_default();
        let err = EtlError::clickhouse(format!("HTTP {status}: {text}"));
        // 5xx (server/LB) and 429 (too many requests) are transient; other
        // 4xx are deterministic (bad SQL, auth) and must not be retried.
        if status.is_server_error() || status.as_u16() == 429 {
            Err(SendError::Transient(err))
        } else {
            Err(SendError::Permanent(err))
        }
    }

    /// Atomically replace `dest` with `staging` (both must exist).
    pub async fn exchange_tables(&self, dest: &str, staging: &str) -> Result<()> {
        let sql = format!(
            "EXCHANGE TABLES {}.{} AND {}.{}",
            ident(&self.cfg.database),
            ident(dest),
            ident(&self.cfg.database),
            ident(staging)
        );
        self.execute(&sql).await
    }

    pub async fn drop_table(&self, table: &str) -> Result<()> {
        let sql = format!(
            "DROP TABLE IF EXISTS {}.{}",
            ident(&self.cfg.database),
            ident(table)
        );
        self.execute(&sql).await
    }

    async fn check(resp: reqwest::Response) -> Result<String> {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if status.is_success() {
            Ok(text)
        } else {
            Err(EtlError::clickhouse(format!("HTTP {status}: {text}")))
        }
    }
}

fn ident(name: &str) -> String {
    crate::ddl::quote_ident(name)
}

/// Serialize batches to an in-memory Arrow IPC stream.
fn serialize_ipc(schema: SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema)?;
        for b in batches {
            writer.write(b)?;
        }
        writer.finish()?;
    }
    Ok(buf)
}

use std::sync::atomic::{AtomicU64, Ordering};

use async_compression::tokio::bufread::{GzipEncoder, ZstdEncoder};
use bytes::Bytes;
use futures::TryStreamExt;
use tokio::io::BufReader;
use tokio_util::io::ReaderStream;

/// Wrap a byte stream so every chunk that flows through is tallied into
/// `counter` — used to report the actual wire size of a streamed body.
fn count_stream<S>(
    stream: S,
    counter: Arc<AtomicU64>,
) -> impl futures::Stream<Item = std::io::Result<Bytes>>
where
    S: futures::Stream<Item = std::io::Result<Bytes>>,
{
    stream.inspect_ok(move |chunk| {
        counter.fetch_add(chunk.len() as u64, Ordering::Relaxed);
    })
}

/// Uncompressed streamed body.
fn counting_body(payload: Bytes, counter: Arc<AtomicU64>) -> reqwest::Body {
    let stream = ReaderStream::new(std::io::Cursor::new(payload));
    reqwest::Body::wrap_stream(count_stream(stream, counter))
}

/// gzip-compressed streamed body (compression happens incrementally).
fn gzip_body(payload: Bytes, counter: Arc<AtomicU64>) -> reqwest::Body {
    let enc = GzipEncoder::new(BufReader::new(std::io::Cursor::new(payload)));
    let stream = ReaderStream::new(enc);
    reqwest::Body::wrap_stream(count_stream(stream, counter))
}

/// zstd-compressed streamed body (compression happens incrementally).
fn zstd_body(payload: Bytes, counter: Arc<AtomicU64>) -> reqwest::Body {
    let enc = ZstdEncoder::new(BufReader::new(std::io::Cursor::new(payload)));
    let stream = ReaderStream::new(enc);
    reqwest::Body::wrap_stream(count_stream(stream, counter))
}
