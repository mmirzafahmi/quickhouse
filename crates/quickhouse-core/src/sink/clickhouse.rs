//! ClickHouse sink over the HTTP interface.
//!
//! Inserts stream Arrow batches serialized as an Arrow IPC stream, ingested by
//! ClickHouse's native `FORMAT ArrowStream`. DDL, reads, and table swaps go
//! through the same HTTP endpoint as plain SQL.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use reqwest::Client;

use crate::config::{ClickHouseConfig, Compression, TransferConfig};
use crate::error::{EtlError, Result};
use crate::sink::{backoff_delay, SendError, Sink, MAX_INSERT_ATTEMPTS};
use crate::types::ColumnType;

/// The ClickHouse setting name this sink generates a value for when
/// `insert_dedup_token` is on — and the one an explicit caller setting can
/// override.
const DEDUP_TOKEN_SETTING: &str = "insert_deduplication_token";

#[derive(Clone)]
pub struct ClickHouseSink {
    client: Client,
    cfg: Arc<ClickHouseConfig>,
    /// Per-sink token making this run's dedup tokens distinct from any other
    /// process/run inserting into the same table (a nanosecond timestamp, the
    /// same approach the BigQuery sink takes for `insertId`).
    run_token: String,
    /// Hands each insert its own dedup token. Only ever incremented — a retry
    /// reuses the value it already took, which is what makes the retry
    /// idempotent rather than merely distinct.
    insert_epoch: Arc<std::sync::atomic::AtomicU64>,
}

impl ClickHouseSink {
    pub fn new(cfg: ClickHouseConfig) -> Result<Self> {
        let client = Client::builder().build().map_err(EtlError::from)?;
        Ok(Self {
            client,
            cfg: Arc::new(cfg),
            run_token: time::OffsetDateTime::now_utc()
                .unix_timestamp_nanos()
                .to_string(),
            insert_epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    /// A dedup token for one logical insert, or `None` when the feature is off
    /// or the caller set the setting themselves. Called once per
    /// `insert_batches` — outside the retry loop, so every attempt at the same
    /// insert presents the same token.
    fn next_dedup_token(&self) -> Option<String> {
        if !self.cfg.insert_dedup_token || self.cfg.settings.contains_key(DEDUP_TOKEN_SETTING) {
            return None;
        }
        let epoch = self
            .insert_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Some(format!("{}-{epoch}", self.run_token))
    }

    pub fn database(&self) -> &str {
        &self.cfg.database
    }

    fn base_request(&self) -> reqwest::RequestBuilder {
        let req = self
            .client
            .post(&self.cfg.url)
            .header("X-ClickHouse-User", &self.cfg.user)
            .header("X-ClickHouse-Key", &self.cfg.password)
            .query(&[("database", &self.cfg.database)]);
        // Caller-supplied ClickHouse settings ride along as query parameters on
        // every request — see `ClickHouseConfig::settings`. Applied here rather
        // than per-call site so DDL, inserts, reads and swaps all get them.
        if self.cfg.settings.is_empty() {
            req
        } else {
            req.query(&self.cfg.settings.iter().collect::<Vec<_>>())
        }
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

    /// Current row count of `table`, or `None` if it doesn't exist. Diagnostic.
    pub async fn current_row_count(&self, table: &str) -> Result<Option<u64>> {
        if !self.table_exists(table).await? {
            return Ok(None);
        }
        let sql = format!(
            "SELECT count() FROM {}.{}",
            ident(&self.cfg.database),
            ident(table)
        );
        Ok(self
            .query_scalar(&sql)
            .await?
            .and_then(|s| s.trim().parse::<u64>().ok()))
    }

    /// Run a query returning a single string column, one value per line (the
    /// HTTP interface's default TabSeparated). Blank lines are dropped.
    pub async fn query_column(&self, sql: &str) -> Result<Vec<String>> {
        let resp = self.base_request().body(sql.to_string()).send().await?;
        let body = Self::check(resp).await?;
        Ok(body
            .lines()
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect())
    }

    /// `ALTER TABLE ADD COLUMN` (Nullable) for each column missing from the
    /// existing table. Case-sensitive (ClickHouse column names are). Returns
    /// the names added. See [`crate::sink::Sink::add_missing_columns`].
    pub async fn add_missing_columns(
        &self,
        table: &str,
        columns: &[ColumnType],
        _cfg: &TransferConfig,
    ) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT name FROM system.columns WHERE database = '{}' AND table = '{}'",
            escape_sql_string(&self.cfg.database),
            escape_sql_string(table),
        );
        let existing = self.query_column(&sql).await?;
        let mut added = Vec::new();
        for c in crate::sink::missing_columns(&existing, columns, false) {
            self.execute(&crate::ddl::add_column(&self.cfg.database, table, c))
                .await?;
            added.push(c.name.clone());
        }
        Ok(added)
    }

    /// Generate and run this destination's own `CREATE TABLE` DDL for `table`.
    pub async fn create_table(
        &self,
        table: &str,
        columns: &[ColumnType],
        cfg: &TransferConfig,
    ) -> Result<()> {
        let sql = crate::ddl::create_table(&self.cfg.database, table, columns, cfg)?;
        tracing::debug!("DDL: {sql}");
        self.execute(&sql).await
    }

    /// `CREATE TABLE new_table AS like_table` — a structure-only clone (engine,
    /// ORDER BY, PARTITION BY, PRIMARY KEY, per-column nullability, all copied
    /// verbatim; no data). Used for staging against an *existing* destination
    /// in both full-refresh (bug report B4: staging must match what's already
    /// there, not a fresh `CREATE TABLE` regenerated from `cfg`, or a swap
    /// silently replaces the destination's engine/sort/partition key with
    /// whatever `cfg` happens to say) and incremental mode ("schema follows
    /// the destination" applies the same way there — though ClickHouse never
    /// actually reaches this path for incremental, since it inserts directly
    /// into the destination rather than staging).
    pub async fn clone_table_structure(&self, new_table: &str, like_table: &str) -> Result<()> {
        let db = ident(&self.cfg.database);
        let sql = format!(
            "CREATE TABLE {db}.{} AS {db}.{}",
            ident(new_table),
            ident(like_table),
        );
        tracing::debug!("DDL: {sql}");
        self.execute(&sql).await
    }

    /// Create the internal `_quickhouse_state` watermark-tracking table if it
    /// doesn't exist yet (`CREATE TABLE IF NOT EXISTS`, so no prior existence
    /// check is needed here, unlike BigQuery's sink), then migrate a pre-0.5
    /// table to the chunk-resume columns *only if it's actually missing them*.
    ///
    /// The migration `ALTER` used to run unconditionally on every call. That is
    /// a no-op for schema but not for cost: `ADD COLUMN IF NOT EXISTS` on an
    /// already-present column still bumps a replicated table's metadata version
    /// each time, so every `sync()` churned a cluster-wide counter and racing
    /// concurrent syncs aborted with `517 CANNOT_ASSIGN_ALTER`. Probing
    /// `system.columns` first (the same source the destination-column check
    /// uses) means the `ALTER` fires at most once per table, and never for a
    /// 0.5+ table — `create_state_table` already declares both columns.
    pub async fn ensure_state_table(&self, state_table: &str) -> Result<()> {
        self.execute(&crate::ddl::create_state_table(
            &self.cfg.database,
            state_table,
        ))
        .await?;
        let sql = format!(
            "SELECT name FROM system.columns WHERE database = '{}' AND table = '{}'",
            escape_sql_string(&self.cfg.database),
            escape_sql_string(state_table),
        );
        let existing = self.query_column(&sql).await?;
        if crate::ddl::state_table_needs_migration(&existing) {
            self.execute(&crate::ddl::migrate_state_table(
                &self.cfg.database,
                state_table,
            ))
            .await?;
        }
        Ok(())
    }

    /// Read the last persisted watermark for this `(state_key, dest_table)` pair.
    pub async fn read_last_watermark(&self, cfg: &TransferConfig) -> Result<Option<String>> {
        // The state table may not exist yet on the very first incremental run.
        if !self.table_exists(&cfg.state_table_name).await? {
            return Ok(None);
        }
        let source_id = cfg.effective_state_key();
        let sql = format!(
            "SELECT last_watermark FROM {}.{} FINAL \
             WHERE source_table = '{}' AND dest_table = '{}' \
             ORDER BY run_ts DESC LIMIT 1",
            crate::ddl::quote_ident(&self.cfg.database),
            crate::ddl::quote_ident(&cfg.state_table_name),
            escape_sql_string(&source_id),
            escape_sql_string(&cfg.dest_table),
        );
        self.query_scalar(&sql).await
    }

    /// Persist a new watermark after a successful incremental run. Writes empty
    /// `chunk_cursor`/`chunk_upper`, which clears any in-progress chunk-resume
    /// marker — a clean finish (chunked or not) leaves nothing to resume.
    pub async fn persist_watermark(
        &self,
        cfg: &TransferConfig,
        watermark: &str,
        rows: u64,
    ) -> Result<()> {
        let source_id = cfg.effective_state_key();
        let sql = format!(
            "INSERT INTO {}.{} \
             (source_table, dest_table, last_watermark, rows, chunk_cursor, chunk_upper) \
             VALUES ('{}', '{}', '{}', {}, '', '')",
            crate::ddl::quote_ident(&self.cfg.database),
            crate::ddl::quote_ident(&cfg.state_table_name),
            escape_sql_string(&source_id),
            escape_sql_string(&cfg.dest_table),
            escape_sql_string(watermark),
            rows,
        );
        self.execute(&sql).await
    }

    /// Read an in-progress chunk-resume marker `(cursor, upper)` for this
    /// `(state_key, dest_table)`, or `None` if there's nothing to resume (no
    /// state row, or `chunk_cursor` empty — the marker a clean run clears).
    /// `upper` is the frozen snapshot-max the interrupted run was reading up to,
    /// so resumption reads the same window rather than re-snapshotting.
    pub async fn read_chunk_state(&self, cfg: &TransferConfig) -> Result<Option<(String, String)>> {
        if !self.table_exists(&cfg.state_table_name).await? {
            return Ok(None);
        }
        let source_id = cfg.effective_state_key();
        let sql = format!(
            "SELECT chunk_cursor, chunk_upper FROM {}.{} FINAL \
             WHERE source_table = '{}' AND dest_table = '{}' \
             ORDER BY run_ts DESC LIMIT 1 FORMAT TabSeparated",
            crate::ddl::quote_ident(&self.cfg.database),
            crate::ddl::quote_ident(&cfg.state_table_name),
            escape_sql_string(&source_id),
            escape_sql_string(&cfg.dest_table),
        );
        let rows = self.query_column(&sql).await?;
        // One TSV line: "<cursor>\t<upper>". Empty cursor = no resume marker.
        match rows.first() {
            Some(line) => {
                let mut it = line.splitn(2, '\t');
                let cursor = it.next().unwrap_or("").to_string();
                let upper = it.next().unwrap_or("").to_string();
                if cursor.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some((cursor, upper)))
                }
            }
            None => Ok(None),
        }
    }

    /// Persist a per-chunk resume marker: the committed watermark stays put
    /// (`committed`, unchanged until the whole run finishes), while
    /// `chunk_cursor`/`chunk_upper` record how far this run has durably read so
    /// a crash resumes from here. `read_last_watermark` still returns
    /// `committed` throughout (empty reads back as `None`).
    pub async fn persist_chunk_cursor(
        &self,
        cfg: &TransferConfig,
        committed: Option<&str>,
        cursor: &str,
        upper: &str,
        rows: u64,
    ) -> Result<()> {
        let source_id = cfg.effective_state_key();
        let sql = format!(
            "INSERT INTO {}.{} \
             (source_table, dest_table, last_watermark, rows, chunk_cursor, chunk_upper) \
             VALUES ('{}', '{}', '{}', {}, '{}', '{}')",
            crate::ddl::quote_ident(&self.cfg.database),
            crate::ddl::quote_ident(&cfg.state_table_name),
            escape_sql_string(&source_id),
            escape_sql_string(&cfg.dest_table),
            escape_sql_string(committed.unwrap_or("")),
            rows,
            escape_sql_string(cursor),
            escape_sql_string(upper),
        );
        self.execute(&sql).await
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
        // Shared, not copied: each retry attempt re-encodes from these same
        // batches (an Arc clone per array) rather than holding a serialized
        // payload alive between attempts. See `ipc_stream`.
        let batches = Arc::new(batches.to_vec());

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
        // Taken once, before the retry loop: the point of the token is that all
        // attempts at this one insert present the *same* value.
        let dedup_token = self.next_dedup_token();

        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let sent = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let mut req = self.base_request().query(&[("query", query.as_str())]);
            if let Some(token) = &dedup_token {
                req = req.query(&[(DEDUP_TOKEN_SETTING, token.as_str())]);
            }
            let ipc = ipc_stream(schema.clone(), batches.clone());
            let body = match self.cfg.compression {
                Compression::None => counting_body(ipc, sent.clone()),
                Compression::Gzip => {
                    req = req.header("Content-Encoding", "gzip");
                    gzip_body(ipc, sent.clone())
                }
                Compression::Zstd => {
                    req = req.header("Content-Encoding", "zstd");
                    zstd_body(ipc, sent.clone())
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

    /// Append every row of `staging` into `dest`, column-for-column by name
    /// (`INSERT INTO dest (cols) SELECT cols FROM staging`). Both tables live in
    /// the configured database. Named columns (not `SELECT *`) so the copy is
    /// order-independent. `dest`'s `ReplacingMergeTree` dedups the appended rows
    /// lazily, exactly as a direct insert of the same rows would.
    pub async fn insert_select(
        &self,
        dest: &str,
        staging: &str,
        columns: &[ColumnType],
    ) -> Result<()> {
        let cols = columns
            .iter()
            .map(|c| ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ");
        let db = ident(&self.cfg.database);
        let sql = format!(
            "INSERT INTO {db}.{} ({cols}) SELECT {cols} FROM {db}.{}",
            ident(dest),
            ident(staging)
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

/// Thin delegation to the inherent methods above. ClickHouse keeps the default
/// `merge_into` (unsupported) — it dedups via `ReplacingMergeTree` rather than a
/// staged MERGE, so `requires_staging_for_incremental` stays `false`. It does
/// override `insert_select`, used to promote a staging table when an incremental
/// run stages only to interpose a data-quality gate (see `sync.rs`).
#[async_trait]
impl Sink for ClickHouseSink {
    async fn table_exists(&self, table: &str) -> Result<bool> {
        ClickHouseSink::table_exists(self, table).await
    }
    async fn create_table(
        &self,
        table: &str,
        columns: &[ColumnType],
        cfg: &TransferConfig,
    ) -> Result<()> {
        ClickHouseSink::create_table(self, table, columns, cfg).await
    }
    async fn clone_table_structure(&self, new_table: &str, like_table: &str) -> Result<()> {
        ClickHouseSink::clone_table_structure(self, new_table, like_table).await
    }
    async fn insert_batches(
        &self,
        table: &str,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<u64> {
        ClickHouseSink::insert_batches(self, table, schema, batches).await
    }
    async fn atomic_swap(&self, dest: &str, staging: &str, _columns: &[ColumnType]) -> Result<()> {
        self.exchange_tables(dest, staging).await
    }
    async fn current_row_count(&self, table: &str) -> Result<Option<u64>> {
        ClickHouseSink::current_row_count(self, table).await
    }
    async fn drop_table(&self, table: &str) -> Result<()> {
        ClickHouseSink::drop_table(self, table).await
    }
    async fn insert_select(&self, dest: &str, staging: &str, columns: &[ColumnType]) -> Result<()> {
        ClickHouseSink::insert_select(self, dest, staging, columns).await
    }
    async fn ensure_state_table(&self, state_table: &str) -> Result<()> {
        ClickHouseSink::ensure_state_table(self, state_table).await
    }
    async fn read_last_watermark(&self, cfg: &TransferConfig) -> Result<Option<String>> {
        ClickHouseSink::read_last_watermark(self, cfg).await
    }
    async fn persist_watermark(
        &self,
        cfg: &TransferConfig,
        watermark: &str,
        rows: u64,
    ) -> Result<()> {
        ClickHouseSink::persist_watermark(self, cfg, watermark, rows).await
    }
    async fn add_missing_columns(
        &self,
        table: &str,
        columns: &[ColumnType],
        cfg: &TransferConfig,
    ) -> Result<Vec<String>> {
        ClickHouseSink::add_missing_columns(self, table, columns, cfg).await
    }
    fn dest_kind(&self) -> crate::config::DestKind {
        crate::config::DestKind::ClickHouse
    }
    fn namespace(&self) -> &str {
        self.database()
    }
    async fn read_chunk_state(&self, cfg: &TransferConfig) -> Result<Option<(String, String)>> {
        ClickHouseSink::read_chunk_state(self, cfg).await
    }
    async fn persist_chunk_cursor(
        &self,
        cfg: &TransferConfig,
        committed: Option<&str>,
        cursor: &str,
        upper: &str,
        rows: u64,
    ) -> Result<()> {
        ClickHouseSink::persist_chunk_cursor(self, cfg, committed, cursor, upper, rows).await
    }
}

fn ident(name: &str) -> String {
    crate::ddl::quote_ident(name)
}

/// Escape a value for a single-quoted ClickHouse string literal. Backslash
/// must be escaped *first* — otherwise a value ending in a backslash (e.g. a
/// table/query name or watermark value with a trailing `\`) escapes the
/// literal's closing quote instead of terminating the string, breaking every
/// query built from it (independently verified against a real ClickHouse
/// server: `SELECT 'ends_with_backslash\'` fails with "Code: 62. Single
/// quoted string is not closed").
fn escape_sql_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "''")
}

use std::sync::atomic::{AtomicU64, Ordering};

use async_compression::tokio::bufread::{GzipEncoder, ZstdEncoder};
use bytes::Bytes;
use futures::TryStreamExt;
use tokio::io::BufReader;
use tokio_util::io::{ReaderStream, StreamReader};

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
fn counting_body<S>(source: S, counter: Arc<AtomicU64>) -> reqwest::Body
where
    S: futures::Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
{
    reqwest::Body::wrap_stream(count_stream(source, counter))
}

/// gzip-compressed streamed body (compression happens incrementally).
fn gzip_body<S>(source: S, counter: Arc<AtomicU64>) -> reqwest::Body
where
    S: futures::Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
{
    let enc = GzipEncoder::new(BufReader::new(StreamReader::new(source)));
    let stream = ReaderStream::new(enc);
    reqwest::Body::wrap_stream(count_stream(stream, counter))
}

/// zstd-compressed streamed body (compression happens incrementally).
fn zstd_body<S>(source: S, counter: Arc<AtomicU64>) -> reqwest::Body
where
    S: futures::Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
{
    let enc = ZstdEncoder::new(BufReader::new(StreamReader::new(source)));
    let stream = ReaderStream::new(enc);
    reqwest::Body::wrap_stream(count_stream(stream, counter))
}

/// How many IPC bytes accumulate before a chunk is handed to the HTTP body.
/// Small enough that peak per-insert overhead stays flat as insert size grows,
/// large enough that neither the channel nor the compressor sees tiny writes.
const IPC_CHUNK_BYTES: usize = 256 * 1024;

/// IPC chunks allowed in flight between the serializer and the socket. This,
/// times `IPC_CHUNK_BYTES`, is the whole steady-state serialization footprint
/// of an insert — independent of how many batches it carries.
const IPC_CHUNK_QUEUE: usize = 4;

/// Serialize `batches` as an Arrow IPC stream *incrementally*, as a stream of
/// byte chunks.
///
/// This is what lets insert size grow without RSS following it. Building the
/// whole IPC payload into a `Vec<u8>` first was harmless at 4 MiB batches, but
/// coalescing inserts to tens of MiB would have made that buffer the dominant
/// allocation of the entire pipeline — and `parallelism` multiplies it. Here
/// peak serialization memory is `IPC_CHUNK_BYTES * IPC_CHUNK_QUEUE` per
/// in-flight insert whatever the payload size, so `MemoryBudget`'s accounting
/// over the Arrow batches themselves stays the honest number it claims to be.
///
/// `StreamWriter` is synchronous, so it's driven on the blocking pool and hands
/// finished chunks over a bounded channel — which also keeps the serialization
/// CPU off the async reactor. A dropped receiver (abandoned attempt, aborted
/// request) makes the next send fail, which ends the task rather than leaking
/// it. Serialization restarts per retry attempt; the batches are `Arc`-shared,
/// so that costs nothing but the re-encode.
fn ipc_stream(
    schema: SchemaRef,
    batches: Arc<Vec<RecordBatch>>,
) -> impl futures::Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static {
    let (tx, rx) = tokio::sync::mpsc::channel::<std::io::Result<Bytes>>(IPC_CHUNK_QUEUE);
    tokio::task::spawn_blocking(move || {
        let sink = ChunkWriter {
            tx: tx.clone(),
            buf: Vec::with_capacity(IPC_CHUNK_BYTES),
        };
        let write_all = || -> std::io::Result<()> {
            let mut writer = arrow::ipc::writer::StreamWriter::try_new(sink, &schema)
                .map_err(std::io::Error::other)?;
            for b in batches.iter() {
                writer.write(b).map_err(std::io::Error::other)?;
            }
            writer.finish().map_err(std::io::Error::other)?;
            let mut done = writer.into_inner().map_err(std::io::Error::other)?;
            done.send_buffered()
        };
        if let Err(e) = write_all() {
            // The receiver going away is the normal "nobody wants this
            // any more" path, not an error worth reporting anywhere.
            let _ = tx.blocking_send(Err(e));
        }
    });
    futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|chunk| (chunk, rx))
    })
}

/// `std::io::Write` adapter that forwards to an async channel in
/// `IPC_CHUNK_BYTES`-sized pieces. Used only from inside `spawn_blocking`,
/// which is what makes `blocking_send` the right call here.
struct ChunkWriter {
    tx: tokio::sync::mpsc::Sender<std::io::Result<Bytes>>,
    buf: Vec<u8>,
}

impl ChunkWriter {
    /// Hand whatever is buffered to the consumer. A closed channel surfaces as
    /// a broken pipe, which aborts serialization instead of spinning.
    fn send_buffered(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let chunk = Bytes::from(std::mem::replace(
            &mut self.buf,
            Vec::with_capacity(IPC_CHUNK_BYTES),
        ));
        self.tx.blocking_send(Ok(chunk)).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "clickhouse insert body was dropped before serialization finished",
            )
        })
    }
}

impl std::io::Write for ChunkWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(data);
        if self.buf.len() >= IPC_CHUNK_BYTES {
            self.send_buffered()?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.send_buffered()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_sql_string_doubles_quotes() {
        assert_eq!(escape_sql_string("o'brien"), "o''brien");
    }

    fn sink_with_settings(settings: &[(&str, &str)]) -> ClickHouseSink {
        ClickHouseSink::new(ClickHouseConfig {
            url: "http://ch:8123".into(),
            database: "analytics".into(),
            user: "default".into(),
            password: String::new(),
            compression: Compression::None,
            insert_dedup_token: false,
            settings: settings
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            s3_archive: None,
        })
        .unwrap()
    }

    /// The URL every request is built from, so this covers DDL, inserts, reads
    /// and swaps at once — they all go through `base_request`.
    fn base_url(sink: &ClickHouseSink) -> String {
        sink.base_request().build().unwrap().url().to_string()
    }

    #[test]
    fn settings_ride_along_as_query_parameters() {
        let url = base_url(&sink_with_settings(&[
            ("select_sequential_consistency", "1"),
            ("async_insert", "1"),
        ]));
        assert!(url.contains("database=analytics"), "{url}");
        assert!(url.contains("select_sequential_consistency=1"), "{url}");
        assert!(url.contains("async_insert=1"), "{url}");
    }

    #[test]
    fn settings_are_emitted_in_a_stable_order() {
        // BTreeMap, not HashMap: the same config must produce the same URL every
        // run so request logs are reproducible.
        let s = &[("z_last", "1"), ("a_first", "2"), ("m_middle", "3")];
        assert_eq!(
            base_url(&sink_with_settings(s)),
            base_url(&sink_with_settings(s))
        );
        let url = base_url(&sink_with_settings(s));
        let a = url.find("a_first").unwrap();
        let m = url.find("m_middle").unwrap();
        let z = url.find("z_last").unwrap();
        assert!(a < m && m < z, "settings not sorted: {url}");
    }

    #[test]
    fn no_settings_leaves_the_request_url_untouched() {
        let url = base_url(&sink_with_settings(&[]));
        assert!(url.ends_with("database=analytics"), "{url}");
    }

    fn sink_with_dedup(enabled: bool, settings: &[(&str, &str)]) -> ClickHouseSink {
        ClickHouseSink::new(ClickHouseConfig {
            url: "http://ch:8123".into(),
            database: "analytics".into(),
            user: "default".into(),
            password: String::new(),
            compression: Compression::None,
            insert_dedup_token: enabled,
            settings: settings
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            s3_archive: None,
        })
        .unwrap()
    }

    #[test]
    fn dedup_token_is_absent_unless_enabled() {
        assert!(sink_with_dedup(false, &[]).next_dedup_token().is_none());
    }

    #[test]
    fn each_insert_gets_its_own_dedup_token() {
        // Distinctness is what keeps legitimate inserts from deduplicating each
        // other; a shared constant token would discard everything after the first.
        let sink = sink_with_dedup(true, &[]);
        let a = sink.next_dedup_token().expect("enabled");
        let b = sink.next_dedup_token().expect("enabled");
        assert_ne!(a, b);
        // Retries reuse the token their attempt already took, which is why the
        // value is minted once per insert rather than once per request — see
        // `insert_batches`. Both share the run token.
        let run = a.rsplit_once('-').unwrap().0;
        assert_eq!(run, b.rsplit_once('-').unwrap().0);
    }

    #[test]
    fn an_explicit_caller_setting_wins_over_the_generated_token() {
        let sink = sink_with_dedup(true, &[(DEDUP_TOKEN_SETTING, "mine")]);
        assert!(sink.next_dedup_token().is_none());
        // ...and the caller's value is what actually goes on the wire.
        assert!(base_url(&sink).contains("insert_deduplication_token=mine"));
    }

    fn ipc_test_batch(schema: &SchemaRef, from: i64, rows: i64) -> RecordBatch {
        let ids: Vec<i64> = (from..from + rows).collect();
        let names: Vec<String> = ids.iter().map(|i| format!("name-{i}")).collect();
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow_array::Int64Array::from(ids)),
                Arc::new(arrow_array::StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    fn ipc_test_schema() -> SchemaRef {
        Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
        ]))
    }

    /// The chunked, incrementally-serialized body must still be a byte-exact
    /// Arrow IPC stream — this is what ClickHouse's `FORMAT ArrowStream`
    /// parses, so a framing mistake here would corrupt every insert.
    #[tokio::test]
    async fn streamed_ipc_round_trips_through_an_arrow_reader() {
        use futures::StreamExt;

        let schema = ipc_test_schema();
        // Deliberately larger than IPC_CHUNK_BYTES so the writer really does
        // hand over multiple chunks rather than one buffered payload.
        let batches = vec![
            ipc_test_batch(&schema, 0, 20_000),
            ipc_test_batch(&schema, 20_000, 20_000),
        ];
        let expected_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        let mut stream = Box::pin(ipc_stream(schema.clone(), Arc::new(batches)));
        let mut payload: Vec<u8> = Vec::new();
        let mut chunks = 0;
        while let Some(chunk) = stream.next().await {
            payload.extend_from_slice(&chunk.expect("no serialization error"));
            chunks += 1;
        }
        assert!(
            chunks > 1,
            "expected the payload to arrive in multiple chunks, got {chunks}"
        );

        let reader =
            arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(payload), None).unwrap();
        assert_eq!(reader.schema(), schema);
        let decoded: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let decoded_rows: usize = decoded.iter().map(|b| b.num_rows()).sum();
        assert_eq!(decoded_rows, expected_rows);
    }

    /// Every retry attempt re-encodes from the shared batches, so the same
    /// input must serialize identically each time.
    #[tokio::test]
    async fn streamed_ipc_is_reproducible_across_attempts() {
        use futures::StreamExt;

        let schema = ipc_test_schema();
        let batches = Arc::new(vec![ipc_test_batch(&schema, 0, 500)]);
        let collect = || {
            let schema = schema.clone();
            let batches = batches.clone();
            async move {
                let mut s = Box::pin(ipc_stream(schema, batches));
                let mut out: Vec<u8> = Vec::new();
                while let Some(c) = s.next().await {
                    out.extend_from_slice(&c.unwrap());
                }
                out
            }
        };
        assert_eq!(collect().await, collect().await);
    }

    #[test]
    fn escape_sql_string_escapes_backslash_before_quote() {
        // Regression test: a trailing backslash used to escape the literal's
        // closing quote instead of terminating the string (verified against
        // a real ClickHouse server: `SELECT 'ends_with_backslash\'` fails
        // with "Code: 62. Single quoted string is not closed").
        assert_eq!(escape_sql_string(r"a\b"), r"a\\b");
        assert_eq!(
            escape_sql_string(r"ends_with_backslash\"),
            r"ends_with_backslash\\"
        );
    }
}
