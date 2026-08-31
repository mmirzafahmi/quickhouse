//! Transfer orchestration: schema resolution -> DDL -> parallel partitioned
//! stream/decode/insert -> (full) atomic swap or (incremental) watermark
//! persist. Each source engine (Postgres, MySQL, ...) plugs in via the
//! [`Source`] enum; everything downstream of "decode into Arrow batches" is
//! source-agnostic.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use bytes::Bytes;
use chrono::Utc;
use futures::StreamExt;
use mysql_async::prelude::*;
use object_store::ObjectStore;
use tokio::task::JoinSet;

use crate::archive::{archive_object_key, build_s3_store, S3ArchiveWriter};
use crate::config::{
    ApiColumn, DestinationConfig, ParquetCompression, S3ArchiveConfig, SourceConfig, SyncMode,
    TransferConfig, TransferResult, TransferWarning, WarningKind, WatermarkSeed,
};
use crate::decode::CopyDecoder;
use crate::decode_api::{resolve_api_columns, ApiBatcher};
use crate::decode_bigquery::BigQueryBatcher;
use crate::decode_mysql::MySqlBatcher;
use crate::error::{EtlError, Result};
use crate::memory::{MemoryBudget, Reservation};
use crate::sink::{build_sink, Sink};
use crate::source::appsflyer::AppsFlyerSource;
use crate::source::clevertap::CleverTapSource;
use crate::source::mysql::{quote_my, quote_my_table};
use crate::source::postgres::{quote_pg, quote_pg_table};
use crate::source::{BigQuerySource, Keyset, MySqlSource, Partition, PgSource, Source};
use crate::transform::{self, SelectPlan};
use crate::types::bigquery::arrow_to_bigquery_type;
use crate::types::ColumnType;
use google_cloud_bigquery::http::table::TableFieldType;

/// Live progress snapshot passed to the optional callback.
#[derive(Debug, Clone, Copy)]
pub struct Progress {
    pub rows_read: u64,
    pub rows_written: u64,
    pub bytes_written: u64,
    pub elapsed_secs: f64,
    pub rows_per_sec: f64,
}

pub type ProgressCb = Arc<dyn Fn(Progress) + Send + Sync>;

/// Context handed to a [`StagedValidationCb`]: the fully-loaded per-run staging
/// table that is about to be promoted (swapped in for full-refresh, or merged
/// for incremental), and where it lives. The callback validates this table and
/// vetoes the promotion by returning `Err`.
#[derive(Debug, Clone)]
pub struct StagedInfo {
    /// Bare name of the staging table (within `database`).
    pub staging_table: String,
    /// The ClickHouse database / BigQuery dataset the staging table lives in.
    pub database: String,
    /// Which destination engine this staging table is in.
    pub dest_kind: crate::config::DestKind,
    /// Rows loaded into staging so far this run (best-effort; the same counter
    /// the progress callback reports).
    pub rows_written: u64,
}

/// A pre-promotion data-quality gate. Fired once, after the per-run staging
/// table is fully loaded but **before** the atomic swap (full-refresh) or
/// `MERGE` (incremental). Returning `Err` aborts the promotion: the swap/merge
/// is skipped, the staging table is dropped by the existing cleanup path, and
/// the transfer fails with that error — so rejected data never reaches the
/// destination. Only fired on paths that stage (full-refresh for either
/// destination, and BigQuery incremental); the PyO3 binding wraps a Python
/// callback that runs a Great Expectations suite against `staging_table`.
pub type StagedValidationCb = Arc<dyn Fn(&StagedInfo) -> Result<()> + Send + Sync>;

/// Fire the optional staged-validation gate against the fully-loaded `staging`
/// table, just before it is promoted. A `None` callback is a no-op (unchanged
/// behavior). An `Err` from the callback vetoes the promotion — it propagates
/// out of the transfer's fallible tail, so the swap/merge is skipped and the
/// existing cleanup path drops `staging`.
fn run_staged_validation(
    on_staged: &Option<StagedValidationCb>,
    sink: &dyn Sink,
    staging: &str,
    rows_written: u64,
) -> Result<()> {
    let Some(cb) = on_staged else {
        return Ok(());
    };
    tracing::info!("running staged data-quality validation on '{staging}'");
    cb(&StagedInfo {
        staging_table: staging.to_string(),
        database: sink.namespace().to_string(),
        dest_kind: sink.dest_kind(),
        rows_written,
    })?;
    tracing::info!("staged data-quality validation passed for '{staging}'");
    Ok(())
}

/// Promote a fully-loaded staging table into the incremental destination: fire
/// the optional gate, run the window-scoped delete where the destination needs
/// it as a separate statement, then either `MERGE` (destinations that stage for
/// a real keyed upsert — BigQuery) or a plain insert-select (ClickHouse, which
/// stages to interpose the gate and/or the delete, and lets `ReplacingMergeTree`
/// dedup the promoted rows lazily, exactly as a direct insert would), then drop
/// staging. Shared by all three transfer flows. Only called when a staging table
/// was used. Returns the number of destination rows deleted.
#[allow(clippy::too_many_arguments)]
async fn promote_staged_incremental(
    sink: &dyn Sink,
    dest_table: &str,
    staging: &str,
    key: &[String],
    columns: &[ColumnType],
    merge_prune_partition_by: Option<&str>,
    prune_key_range: bool,
    prune_key_list_max: usize,
    delete_stale_in_window: bool,
    on_staged: &Option<StagedValidationCb>,
    rows_written: u64,
    dedup_order: Option<&str>,
    warnings: &Warnings,
) -> Result<u64> {
    run_staged_validation(on_staged, sink, staging, rows_written)?;
    let mut rows_deleted = 0u64;
    // Destinations that cannot express the delete inside their upsert run it as
    // its own statement, *before* the insert. Order is not load-bearing (the
    // predicate subtracts the staged keys either way), but delete-then-insert
    // keeps the window from momentarily holding both the old and new copy of an
    // updated row.
    if delete_stale_in_window && !sink.deletes_stale_within_merge() {
        let window_column = merge_prune_partition_by.ok_or_else(|| {
            EtlError::internal(
                "delete_stale_in_window without merge_prune_partition_by (should have been \
                 validated)",
            )
        })?;
        rows_deleted = sink
            .delete_stale_against_staging(dest_table, staging, key, window_column)
            .await?;
    }
    if sink.requires_staging_for_incremental() {
        tracing::info!("merging staged incremental rows into '{dest_table}'");
        // The clustering check is a metadata read that only ever produces a
        // warning, so it rides alongside the MERGE rather than in front of it:
        // a fleet running thousands of merges a week should not pay a round
        // trip each time for a diagnostic. Overlapped with a statement that
        // averages tens of seconds, it costs nothing at all.
        let (merged, ()) = tokio::join!(
            sink.merge_into(
                dest_table,
                staging,
                key,
                columns,
                merge_prune_partition_by,
                prune_key_range,
                prune_key_list_max,
                delete_stale_in_window,
                dedup_order,
            ),
            warn_unclustered_merge_target(sink, dest_table, key, warnings),
        );
        merged?;
    } else {
        tracing::info!("inserting validated staged rows into '{dest_table}'");
        sink.insert_select(dest_table, staging, columns).await?;
    }
    sink.drop_table(staging).await?;
    Ok(rows_deleted)
}

/// Warn when a staged `MERGE` is about to run against a destination that is not
/// clustered by the merge key.
///
/// The key bound quickhouse puts on every merge is a tautology — a destination
/// row can only match a key the batch holds — but a bound only *saves* anything
/// if the destination is physically organised by that column. On an unclustered
/// table it prunes nothing and the merge scans the whole thing, every run. That
/// is invisible from the caller's side and shows up as a billing line weeks
/// later: one production table here scanned ~10.4 GiB per run, 42 times in a
/// week, on an 11.3 GiB table.
///
/// quickhouse generates clustered DDL itself (see `TransferConfig::key`), so it
/// knows what good looks like; this only fires for a destination created some
/// other way. Purely diagnostic — a metadata read that fails, or a destination
/// that cannot report clustering at all, must never fail a transfer.
/// Whether a merge on `key` can prune against a destination clustered by
/// `clustering`.
///
/// True exactly when the merge key is a *prefix* of the clustering, in order.
/// A prefix is what block pruning needs: BigQuery skips blocks on the leading
/// clustering columns, so `CLUSTER BY id, created_at` prunes a merge on `id`
/// just as well as `CLUSTER BY id` does, while `CLUSTER BY created_at, id`
/// prunes it not at all. Compared case-insensitively, since BigQuery treats
/// column names that way and a case difference is not a real mismatch.
///
/// Split out as a pure function so the rule is testable without a live
/// destination — the same reason `full_refresh_shrink_verdict` is one.
fn clustering_binds(clustering: Option<&[String]>, key: &[String]) -> bool {
    match clustering {
        Some(cols) if !cols.is_empty() && !key.is_empty() => key
            .iter()
            .enumerate()
            .all(|(i, k)| cols.get(i).is_some_and(|c| c.eq_ignore_ascii_case(k))),
        _ => false,
    }
}

async fn warn_unclustered_merge_target(
    sink: &dyn Sink,
    dest_table: &str,
    key: &[String],
    warnings: &Warnings,
) {
    if key.is_empty() {
        return;
    }
    let clustering = match sink.clustering_columns(dest_table).await {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!("could not read clustering columns of '{dest_table}': {e}");
            return;
        }
    };
    if clustering_binds(clustering.as_deref(), key) {
        return;
    }
    let have = match &clustering {
        Some(cols) if !cols.is_empty() => format!("clustered by {}", cols.join(", ")),
        _ => "not clustered at all".to_string(),
    };
    let message = format!(
        "'{dest_table}' is {have}, but this run MERGEs on {} — so the merge's key bound cannot \
         prune anything and every run scans the whole destination. Recreate the destination \
         clustered by the merge key (quickhouse's own generated DDL does this) to make the \
         bound bite.",
        key.join(", "),
    );
    tracing::warn!("{message}");
    warnings.push(TransferWarning {
        kind: WarningKind::UnclusteredMergeTarget,
        column: None,
        count: 0,
        sample: clustering.map(|c| c.join(", ")),
        message,
    });
}

#[derive(Default)]
struct Counters {
    rows_read: AtomicU64,
    rows_written: AtomicU64,
    bytes_written: AtomicU64,
    /// Nanoseconds spent *awaiting source rows*, summed across every parallel
    /// reader. Recorded around the source-stream await alone — not around
    /// decode, insert, or the memory budget — which is what makes
    /// `TransferResult::read_secs` answer "is the source the bottleneck?"
    /// rather than restating the wall clock. See [`await_source`].
    read_nanos: AtomicU64,
}

/// Run-scoped collector for the structured warnings that end up on
/// [`TransferResult::warnings`].
///
/// Every condition here was already detected and already logged; what was
/// missing was a way for the caller to *act* on one. A Dagster asset cannot
/// fail on a `tracing::warn!`, so a transfer that flattened a column to a
/// boolean, or excluded every NULL-watermark row forever, reported success and
/// the damage surfaced weeks later in a completeness check.
///
/// Cloned into each partition task (it's an `Arc`), so concurrent readers all
/// report into the same collector. [`Self::drain`] folds the per-partition
/// entries into one entry per `(kind, column)`, which is the granularity a
/// caller wants: "column `x_state` lost 412 values", not one line per partition.
#[derive(Clone, Default)]
struct Warnings(Arc<Mutex<Vec<TransferWarning>>>);

impl Warnings {
    fn push(&self, w: TransferWarning) {
        self.0.lock().unwrap().push(w);
    }

    /// Take everything collected, folded per `(kind, column)` and ordered
    /// most-affected first so a caller reading only the head sees the worst.
    fn drain(&self) -> Vec<TransferWarning> {
        let raw = std::mem::take(&mut *self.0.lock().unwrap());
        let mut folded: Vec<TransferWarning> = Vec::new();
        for w in raw {
            match folded
                .iter_mut()
                .find(|f| f.kind == w.kind && f.column == w.column)
            {
                Some(f) => {
                    f.count += w.count;
                    if f.sample.is_none() {
                        f.sample = w.sample;
                    }
                }
                None => folded.push(w),
            }
        }
        folded.sort_by(|a, b| {
            b.count
                .cmp(&a.count)
                .then_with(|| a.kind.cmp(&b.kind))
                .then_with(|| a.column.cmp(&b.column))
        });
        folded
    }
}

/// Await one item from a source stream, recording how long that took and
/// enforcing [`TransferConfig::read_idle_timeout_secs`].
///
/// The timer wraps the source await and *only* the source await. That is the
/// whole point of the knob: a source-side `statement_timeout` is a ceiling on
/// the entire streamed transfer (the statement's cursor stays open from the
/// first row read to the last one written), so it fires on a slow destination
/// and reports it as a source error. This one cannot — while the destination is
/// throttling, the reader is blocked on the memory budget or the insert, not
/// here, and the clock is not running.
///
/// Note this stays accurate when the caller polls the read concurrently with a
/// decode (`tokio::join!`): the elapsed time is captured inside this future, at
/// the moment the item arrives, not when the surrounding join completes.
async fn await_source<F, T>(fut: F, counters: &Counters, idle_secs: u64, scope: &str) -> Result<T>
where
    F: std::future::Future<Output = T>,
{
    let started = Instant::now();
    let record = |counters: &Counters, started: Instant| {
        counters
            .read_nanos
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
    };
    if idle_secs == 0 {
        let out = fut.await;
        record(counters, started);
        return Ok(out);
    }
    match tokio::time::timeout(Duration::from_secs(idle_secs), fut).await {
        Ok(out) => {
            record(counters, started);
            Ok(out)
        }
        Err(_) => {
            record(counters, started);
            Err(EtlError::read_idle_timeout(scope, idle_secs))
        }
    }
}

/// Global source-read rate limiter, shared across every parallel partition so
/// the ceiling is an *aggregate* rows/sec (not per-connection). Uses a
/// virtual-scheduling (GCRA-style) clock: each `acquire(n)` reserves `n / rate`
/// seconds of read time by pushing a shared `next_available` instant forward,
/// and the caller sleeps until its reserved slot. Because reads are gated
/// *after* each batch, and `COPY TO STDOUT` (and MySQL's streaming result) only
/// produce as fast as the client consumes, pausing here applies TCP
/// backpressure that slows the server-side scan itself — the whole point of the
/// knob. No burst credit accumulates while idle (`next_available` is clamped
/// forward to `now`, never left in the past), which is the safe choice for
/// "be gentle to the source".
struct ReadThrottle {
    rate_per_sec: f64,
    next_available: Mutex<Instant>,
}

impl ReadThrottle {
    fn new(rows_per_sec: u64) -> Self {
        Self {
            rate_per_sec: rows_per_sec as f64,
            next_available: Mutex::new(Instant::now()),
        }
    }

    /// Reserve capacity for `rows` just-read rows and return how long the
    /// caller must wait before pulling more. Pure of any `.await` (so the
    /// `std::sync::Mutex` is never held across a suspension point, and the math
    /// is unit-testable without sleeping); `acquire` wraps it with the sleep.
    fn reserve(&self, rows: u64) -> Duration {
        // A zero rate can't happen via the public API (`validate` rejects
        // `Some(0)` before any throttle is built), but guard anyway so a Rust
        // caller constructing `TransferConfig` directly gets a safe no-op
        // instead of an `inf` `Duration` panic (`rows / 0.0`) in the read loop.
        // `rate_per_sec` is a `u64 as f64`, so it's always finite and >= 0.
        if rows == 0 || self.rate_per_sec <= 0.0 {
            return Duration::ZERO;
        }
        let mut next = self.next_available.lock().unwrap();
        let now = Instant::now();
        // Idle time grants no credit: never start earlier than `now`.
        let start = (*next).max(now);
        let cost = Duration::from_secs_f64(rows as f64 / self.rate_per_sec);
        *next = start + cost;
        start.saturating_duration_since(now)
    }

    async fn acquire(&self, rows: u64) {
        let wait = self.reserve(rows);
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

/// Shared handle for uploading finished batches. Cloned cheaply into each
/// partition task (all fields are `Arc`/`Copy`), so every partition spawns
/// sends against the *same* memory budget and counters.
#[derive(Clone)]
struct SendCtx {
    sink: Arc<dyn Sink>,
    budget: MemoryBudget,
    target_table: Arc<String>,
    counters: Arc<Counters>,
    progress: Option<ProgressCb>,
    started: Instant,
    /// `Some` only for a ClickHouse destination with `s3_archive` configured;
    /// `None` otherwise (including always for BigQuery). See
    /// `ArchiveRunInfo::writer_for`.
    archive: Option<Arc<ArchiveRunInfo>>,
    /// `Some` when `read_max_rows_per_sec` is set: a single limiter shared by
    /// all partition tasks, so the cap is an aggregate across the whole read.
    throttle: Option<Arc<ReadThrottle>>,
    /// Run-scoped warning collector, shared by every partition so per-column
    /// coercions from concurrent readers fold into one entry per column.
    warnings: Warnings,
}

/// One partition's accumulator of decoded batches, so an insert carries a
/// worthwhile amount of data instead of one batch per HTTP round-trip.
///
/// `insert_batches` always took a slice; every caller passed exactly one batch,
/// which at the default 4 MiB `batch_bytes` turned a 19.4M-row table into 900+
/// round-trips and 900+ new parts. ClickHouse's own guidance is the opposite —
/// fewer, larger inserts — because part count drives background merge work, and
/// on Cloud that merge pressure competes with query memory. Decode granularity
/// (`batch_bytes`) and insert granularity (`insert_bytes`) are now separate
/// knobs, which is what `batch_bytes` was always documented to be.
///
/// Each buffered batch holds its own [`MemoryBudget`] reservation, so the
/// ceiling still covers everything decoded-but-not-yet-landed rather than only
/// what's on the wire. That's also why the fill path must never *block* on the
/// budget while holding a group — see [`SendCtx::push_batch`].
struct InsertBuffer {
    batches: Vec<RecordBatch>,
    reservations: Vec<Reservation>,
    bytes: usize,
    /// Flush once the group reaches this many bytes of real Arrow memory
    /// (measured like `batch_bytes` and `max_memory_bytes`, not post-compression).
    target: usize,
}

impl InsertBuffer {
    fn new(target: usize) -> Self {
        InsertBuffer {
            batches: Vec::new(),
            reservations: Vec::new(),
            bytes: 0,
            target,
        }
    }

    fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Add a batch and its reservation; `true` once the group is worth sending.
    fn push(&mut self, batch: RecordBatch, reservation: Reservation, size: usize) -> bool {
        self.batches.push(batch);
        self.reservations.push(reservation);
        self.bytes += size;
        self.bytes >= self.target
    }

    /// Take everything buffered, leaving the buffer empty.
    fn take(&mut self) -> (Vec<RecordBatch>, Vec<Reservation>) {
        self.bytes = 0;
        (
            std::mem::take(&mut self.batches),
            std::mem::take(&mut self.reservations),
        )
    }
}

impl SendCtx {
    /// Buffer one decoded batch, uploading the accumulated group once it's big
    /// enough. This is the backpressure point: if the pipeline's memory ceiling
    /// is reached, the caller (decoder) stalls here until in-flight uploads
    /// drain.
    ///
    /// The reservation is taken with `try_reserve` first. Blocking outright
    /// would deadlock rather than throttle: a full budget can be full *of this
    /// buffer's own batches*, and nothing would ever release them. So on
    /// pressure we flush first — handing those reservations to a send task that
    /// will release them — and only then wait.
    async fn push_batch(
        &self,
        sends: &mut JoinSet<Result<()>>,
        buffer: &mut InsertBuffer,
        schema: SchemaRef,
        batch: RecordBatch,
    ) {
        let size = batch.get_array_memory_size();
        let reservation = match self.budget.try_reserve(size) {
            Some(r) => r,
            None => {
                if !buffer.is_empty() {
                    self.flush(sends, buffer, schema.clone()).await;
                }
                self.budget.reserve(size).await
            }
        };
        if buffer.push(batch, reservation, size) {
            self.flush(sends, buffer, schema).await;
        }
    }

    /// Upload whatever is buffered as one insert, as a background task so
    /// decoding keeps overlapping the network round-trip. No-op when empty.
    async fn flush(
        &self,
        sends: &mut JoinSet<Result<()>>,
        buffer: &mut InsertBuffer,
        schema: SchemaRef,
    ) {
        if buffer.is_empty() {
            return;
        }
        let (batches, reservations) = buffer.take();
        let ctx = self.clone();
        sends.spawn(async move {
            let _reservations = reservations; // released on task completion
            let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            let bytes = ctx
                .sink
                .insert_batches(&ctx.target_table, schema, &batches)
                .await?;
            ctx.counters.rows_written.fetch_add(rows, Ordering::Relaxed);
            ctx.counters
                .bytes_written
                .fetch_add(bytes, Ordering::Relaxed);
            // Progress fires on *completion*, so rows_written reflects rows
            // actually landed in the destination, not merely decoded.
            emit_progress(&ctx.counters, &ctx.progress, ctx.started);
            Ok(())
        });
    }
}

/// Static per-run info every partition needs to open its own S3 archive
/// writer — the S3 client and naming info are shared (built once per
/// transfer, mirroring `build_sink`); only the partition label varies.
struct ArchiveRunInfo {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    dest_table: String,
    run_date: String,
    run_id: String,
    compression: ParquetCompression,
}

impl ArchiveRunInfo {
    fn writer_for(&self, partition_label: &str, schema: SchemaRef) -> Result<S3ArchiveWriter> {
        let key = archive_object_key(
            &self.prefix,
            &self.dest_table,
            &self.run_date,
            &self.run_id,
            partition_label,
        );
        S3ArchiveWriter::new(self.store.clone(), key, schema, self.compression)
    }
}

/// Build the shared archive info for one transfer run, or `None` if S3
/// archival isn't configured. Building the S3 client here — before any
/// source connection is opened — means a bad archive config (e.g. a missing
/// bucket) fails fast rather than being discovered mid-transfer.
fn build_archive_run_info(
    s3_archive: Option<S3ArchiveConfig>,
    dest_table: &str,
) -> Result<Option<Arc<ArchiveRunInfo>>> {
    let Some(cfg) = s3_archive else {
        return Ok(None);
    };
    let store = build_s3_store(&cfg)?;
    let now = Utc::now();
    Ok(Some(Arc::new(ArchiveRunInfo {
        store,
        prefix: cfg.prefix,
        dest_table: dest_table.to_string(),
        run_date: now.format("%Y-%m-%d").to_string(),
        run_id: now.timestamp().to_string(),
        compression: cfg.compression,
    })))
}

/// Reap finished upload tasks, propagating the first error. With `block`,
/// awaits every remaining task (call once the source stream is exhausted, so
/// all uploads finish before a full-refresh swap / watermark persist); without
/// it, only drains already-finished tasks to surface errors promptly and keep
/// the `JoinSet` from accumulating completed handles.
async fn reap(sends: &mut JoinSet<Result<()>>, block: bool) -> Result<()> {
    if block {
        while let Some(res) = sends.join_next().await {
            join_result(res)?;
        }
    } else {
        while let Some(res) = sends.try_join_next() {
            join_result(res)?;
        }
    }
    Ok(())
}

fn join_result(res: std::result::Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
    match res {
        Ok(inner) => inner,
        Err(e) => Err(EtlError::other(format!("upload task failed: {e}"))),
    }
}

struct SourceSetup {
    source_cols: Vec<ColumnType>,
    snapshot_max: Option<String>,
    partitions: Vec<Partition>,
}

/// Run one table transfer end to end.
///
/// Thin wrapper around [`run_transfer_impl`] that prefixes any error it
/// returns with "which table" — e.g. `"orders -> analytics.orders: ..."` —
/// so a script syncing many tables in a loop (a common pattern; see the
/// README) can tell which one failed straight from the exception text, not
/// just from scrolling back through stderr logs.
pub async fn run_transfer(
    source_cfg: SourceConfig,
    dest: DestinationConfig,
    cfg: TransferConfig,
    progress: Option<ProgressCb>,
    on_staged: Option<StagedValidationCb>,
) -> Result<TransferResult> {
    let table_context = format!(
        "{} -> {}",
        cfg.source_table
            .as_deref()
            .or(cfg.source_query.as_deref())
            .unwrap_or_else(|| source_cfg.kind()),
        cfg.dest_table
    );
    let max_attempts = cfg.retry_max_attempts.max(1);
    if max_attempts <= 1 {
        // Fast path: byte-identical to the pre-retry behavior — one call, one
        // context wrap, no clones.
        return run_transfer_impl(source_cfg, dest, cfg, progress, on_staged)
            .await
            .map_err(|e| e.context(table_context));
    }
    // Retry the WHOLE transfer on a transient source error. Each attempt runs
    // from a clean slate (fresh per-run staging; the watermark advances only on
    // success), so a full refresh stays atomic and an incremental run re-reads
    // the same window rather than skipping rows. Sink/write blips are retried
    // separately at the insert layer, so those never re-read the source here.
    let mut attempt = 1u32;
    loop {
        let result = run_transfer_impl(
            source_cfg.clone(),
            dest.clone(),
            cfg.clone(),
            progress.clone(),
            on_staged.clone(),
        )
        .await;
        match result {
            Ok(r) => return Ok(r),
            Err(e) if attempt < max_attempts && e.is_transient_source() => {
                let delay = crate::sink::backoff_delay(attempt);
                tracing::warn!(
                    "{table_context}: attempt {attempt}/{max_attempts} failed with a transient \
                     source error ({e}); retrying the whole transfer in {delay:?}"
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
            Err(e) => return Err(e.context(table_context)),
        }
    }
}

async fn run_transfer_impl(
    source_cfg: SourceConfig,
    dest: DestinationConfig,
    cfg: TransferConfig,
    progress: Option<ProgressCb>,
    on_staged: Option<StagedValidationCb>,
) -> Result<TransferResult> {
    // Every source (Postgres, MySQL, BigQuery — directly or via reqwest/tonic's
    // own rustls-based transport) eventually needs a process-wide rustls
    // CryptoProvider selected. With both "ring" (this crate's explicit
    // feature) and "aws-lc-rs" (pulled in transitively by some dependency)
    // linked into the same binary, rustls refuses to guess and panics on
    // first use instead — and since this call is idempotent (ignore the
    // error; it just means some other crate got here first), doing it once
    // here, before any source-specific connection code runs, covers every
    // source uniformly instead of requiring each new source module to
    // remember it individually.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut cfg = cfg;
    // API sources have no source_table/source_query and reject a few DB-only
    // knobs; validate them under the API rules.
    if source_cfg.is_api() {
        cfg.validate_api()?;
    } else {
        cfg.validate()?;
    }
    // Drop mode-irrelevant fields (e.g. a watermark passed with mode="full")
    // so the config that runs matches what's effective — see normalize().
    cfg.normalize();

    // A staged-validation gate needs a staging table to validate before the
    // promotion. Full-refresh always stages; incremental stages either for its
    // MERGE (BigQuery) or is forced to stage below (ClickHouse — it would
    // otherwise insert directly). Append mode is the one path with no staging
    // option (a bronze-landing direct insert with no dedup), so reject it loudly
    // up front rather than silently skipping the validation the caller asked for.
    if on_staged.is_some() && cfg.mode == SyncMode::Append {
        return Err(EtlError::config(
            "data-quality validation is not supported for append mode (rows insert \
             directly with no staging table to gate)",
        ));
    }

    let started = Instant::now();
    // One collector for the whole attempt. `run_transfer`'s retry loop calls
    // this function afresh per attempt, so a run that eventually succeeds
    // reports only the successful attempt's warnings.
    let warnings = Warnings::default();

    let source_label = cfg
        .source_table
        .clone()
        .or_else(|| cfg.source_query.clone())
        .unwrap_or_else(|| source_cfg.kind().into());
    tracing::info!(
        "starting {} sync: {} -> {} (mode={:?})",
        source_cfg.kind(),
        source_label,
        cfg.dest_table,
        cfg.mode
    );

    // --- Optional S3 data-lake archival (ClickHouse destinations only). ---
    // Extracted (and cloned) before `build_sink(dest)` consumes `dest` below —
    // in either branch — and built once here so a bad archive config (e.g. a
    // missing bucket) fails fast rather than being discovered mid-transfer.
    let s3_archive_cfg = match &dest {
        DestinationConfig::ClickHouse(ch) => ch.s3_archive.clone(),
        DestinationConfig::BigQuery(_) => None,
    };
    let archive_info = build_archive_run_info(s3_archive_cfg, &cfg.dest_table)?;

    // Per-run-unique staging table name (see `staging_name`), computed once
    // and reused at every create/swap/merge/drop site this run.
    let run_id = new_run_id();
    let staging = staging_name(&cfg.dest_table, &cfg.staging_suffix, &run_id);

    // BigQuery has a genuinely different execution model (no discrete
    // range-partitions to fan out; a single read session that streams via
    // BigQuery-managed parallel streams, drained sequentially on our side —
    // see source/bigquery.rs's module docs). Handled as a fully separate
    // flow rather than contorting the partition-based abstraction below,
    // which was built for connection-oriented sources.
    if let SourceConfig::BigQuery(bq) = &source_cfg {
        let source = BigQuerySource::new(
            bq.project_id.clone(),
            bq.credentials_file.clone(),
            bq.credentials_json.clone(),
        );
        let sink = build_sink(dest).await?;
        return run_transfer_bigquery(
            &source,
            sink,
            cfg,
            progress,
            on_staged,
            started,
            archive_info,
            staging,
            warnings,
        )
        .await;
    }

    // HTTP API sources (CleverTap/AppsFlyer/HttpApi): a declared schema +
    // paginated fetch into either sink (BigQuery or ClickHouse, since
    // `bc1ab45`) — a separate flow from the DB partition machinery. archive is
    // always None: S3 archiving is wired for DB sources only.
    if source_cfg.is_api() {
        let sink = build_sink(dest).await?;
        return run_transfer_api(
            source_cfg, sink, cfg, progress, on_staged, started, staging, warnings,
        )
        .await;
    }

    let source = Arc::new(match &source_cfg {
        SourceConfig::Postgres(pg) => Source::Postgres(PgSource::new(
            pg.dsn.clone(),
            pg.statement_timeout_secs,
            pg.ca_cert_file.clone(),
            pg.client_cert_file.clone(),
            pg.client_key_file.clone(),
            cfg.application_name.clone(),
        )),
        SourceConfig::MySql(my) => Source::MySql(MySqlSource::new(
            my.dsn.clone(),
            my.statement_timeout_secs,
            my.ca_cert_file.clone(),
            my.require_tls,
            my.client_cert_file.clone(),
            my.client_key_file.clone(),
        )),
        SourceConfig::BigQuery(_) => unreachable!("handled via early return above"),
        SourceConfig::CleverTap(_) | SourceConfig::AppsFlyer(_) | SourceConfig::HttpApi(_) => {
            unreachable!("API sources handled via early return above")
        }
    });
    let sink = build_sink(dest).await?;

    // A data-quality gate on an incremental sink that would otherwise insert
    // directly (ClickHouse) forces a staging table so the gate has something to
    // validate before rows reach the destination; the promoted rows are then
    // deduped lazily by `ReplacingMergeTree`, exactly as a direct insert. This
    // is exactly the set of runs `requires_staging_for_incremental()` does NOT
    // already stage.
    // The same is true of a window-scoped delete on a destination whose upsert
    // cannot express one inline: the delete subtracts the staged keys, so it
    // needs the batch materialised in a table before any of it lands.
    let force_stage_incremental = cfg.mode == SyncMode::Incremental
        && !sink.requires_staging_for_incremental()
        && (on_staged.is_some()
            || (cfg.delete_stale_in_window && !sink.deletes_stale_within_merge()));
    if cfg.delete_stale_in_window && !sink.supports_row_delete() {
        return Err(EtlError::config(
            "delete_stale_in_window is not supported by this destination (it cannot delete \
             individual rows)",
        ));
    }

    // Keyset resumable reads (MVP) commit per chunk straight into the
    // destination, which only works where incremental inserts directly (no
    // staging + swap/merge) — i.e. ClickHouse.
    if cfg.chunk_rows.is_some() && sink.requires_staging_for_incremental() {
        return Err(EtlError::config(
            "chunk_rows (keyset resumable reads) is only supported for a ClickHouse destination \
             in this version",
        ));
    }
    // ...and chunked reads can't be gated: each chunk commits straight into the
    // destination as it's read, so there is no single staging table to validate.
    if cfg.chunk_rows.is_some() && force_stage_incremental {
        return Err(EtlError::config(
            "data-quality validation (validate=) is not supported together with chunk_rows: \
             chunked keyset reads commit each chunk directly into the destination, leaving no \
             single staging table to gate. Drop chunk_rows to validate, or validate downstream.",
        ));
    }

    let base_table = cfg.source_table.clone();
    let base_query = cfg.source_query.clone();
    let watermark = cfg.watermark.clone();

    // --- Resolve source schema, incremental snapshot max, and partitions,
    // all on one control connection. ---
    let setup = match source.as_ref() {
        Source::Postgres(s) => {
            setup_postgres(
                s,
                &cfg,
                base_table.as_deref(),
                base_query.as_deref(),
                watermark.as_deref(),
                &warnings,
            )
            .await?
        }
        Source::MySql(s) => {
            setup_mysql(
                s,
                &cfg,
                base_table.as_deref(),
                base_query.as_deref(),
                watermark.as_deref(),
                &warnings,
            )
            .await?
        }
        Source::BigQuery(_) => {
            unreachable!("BigQuery is handled via the early return in run_transfer")
        }
    };
    let SourceSetup {
        source_cols,
        snapshot_max,
        partitions,
    } = setup;
    tracing::info!(
        "resolved {} source column(s); computed {} partition(s) for parallel read",
        source_cols.len(),
        partitions.len()
    );

    let plan: SelectPlan = transform::plan(&source_cols, &cfg, sink.dest_kind())?;
    let plan = Arc::new(plan);

    // --- Incremental: read watermark state, build the "since last run" filter,
    // and (for chunked reads) the keyset resume plan. ---
    let (extra_filter, new_watermark, chunk_plan) = if cfg.mode == SyncMode::Incremental {
        let watermark = cfg.watermark.as_ref().unwrap();
        // The committed cursor from the last fully-successful run (None first run).
        let committed = sink.read_last_watermark(&cfg).await?;
        // An in-progress chunk-resume marker (frozen upper + last durable
        // cursor), only for chunked reads and only if a prior run was cut short.
        let resume = if cfg.chunk_rows.is_some() {
            sink.read_chunk_state(&cfg).await?
        } else {
            None
        };
        let (pinned_upper, start_cursor) = match &resume {
            Some((c, u)) => (Some(u.clone()), Some(c.clone())),
            None => (None, None),
        };
        // Resume freezes the upper bound to the interrupted run's snapshot so it
        // reads the same window; a fresh run uses the live source MAX. (For a
        // non-chunked run `pinned_upper` is always None, so this == snapshot_max
        // and behavior is unchanged.)
        let effective_upper = pinned_upper.or_else(|| snapshot_max.clone());
        // First run only: seed the lower bound.
        let last = committed
            .clone()
            .or_else(|| seed_value(&cfg.seed_watermark, effective_upper.as_deref()));
        tracing::info!(
            "incremental watermark on '{}': committed={:?}, upper={:?}, resuming={}",
            watermark,
            committed,
            effective_upper,
            resume.is_some()
        );
        let source_expr = cfg.watermark_source_expr.as_deref();
        if cfg.source_query.is_some() && source_expr.is_none() {
            tracing::info!(
                "incremental sync reads via source_query: the watermark filter binds to \
                 source_query's own '{watermark}' output column. If that column is anything \
                 other than a bare pass-through of an indexed base-table column (e.g. it's cast \
                 or otherwise transformed), this predicate cannot use an index and every \
                 incremental run will full-scan. See watermark_source_expr= to filter on a raw, \
                 indexed column while source_query still projects a transformed '{watermark}'."
            );
        }
        let filter = match source.as_ref() {
            Source::Postgres(_) => build_watermark_filter_pg(
                watermark,
                source_expr,
                last.as_deref(),
                effective_upper.as_deref(),
                cfg.lookback_seconds,
            ),
            Source::MySql(_) => build_watermark_filter_mysql(
                watermark,
                source_expr,
                last.as_deref(),
                effective_upper.as_deref(),
                cfg.lookback_seconds,
            ),
            Source::BigQuery(_) => {
                unreachable!("BigQuery is handled via the early return in run_transfer")
            }
        };
        let chunk_plan = match cfg.chunk_rows {
            Some(limit) => Some(build_chunk_plan(
                &cfg,
                &plan,
                &source_cols,
                limit,
                committed,
                effective_upper.clone(),
                start_cursor,
            )?),
            None => None,
        };
        (filter, effective_upper, chunk_plan)
    } else {
        (None, None, None)
    };

    // --- Ensure destination / staging tables exist. ---
    let target_table = prepare_target(
        &sink,
        &cfg,
        &plan.dest_columns,
        &staging,
        force_stage_incremental,
    )
    .await?;
    // Whether this run actually created a staging table (vs. inserting straight
    // into the destination, as ClickHouse incremental does) — decides whether
    // the error path below has anything to clean up.
    let used_staging = target_table != cfg.dest_table;
    let cleanup_sink = sink.clone();
    let cleanup_staging_name = staging.clone();

    // The whole fallible tail runs inside this block so that, on ANY error, we
    // best-effort drop the per-run staging table below — a unique-per-run name
    // is never reclaimed by a later run (unlike the old fixed name), so a
    // failed run would otherwise leak it forever.
    let outcome: Result<TransferResult> = async move {
        tracing::info!(
            "quickhouse: transferring into {} across {} partition(s), parallelism={}",
            target_table,
            partitions.len(),
            cfg.parallelism
        );

        // --- Fan out partitions with bounded concurrency. ---
        let counters = Arc::new(Counters::default());
        // One limiter shared across every partition, so `read_max_rows_per_sec`
        // caps the *aggregate* read rate regardless of `parallelism`.
        let throttle = cfg
            .read_max_rows_per_sec
            .map(|r| Arc::new(ReadThrottle::new(r)));
        let cfg = Arc::new(cfg);
        let extra_filter = Arc::new(extra_filter);
        let chunk_plan = Arc::new(chunk_plan);
        let target_table = Arc::new(target_table);
        let ctx = SendCtx {
            sink: sink.clone(),
            budget: MemoryBudget::new(cfg.max_memory_bytes),
            target_table: target_table.clone(),
            counters: counters.clone(),
            progress: progress.clone(),
            started,
            archive: archive_info.clone(),
            throttle,
            warnings: warnings.clone(),
        };
        let stage_started = Instant::now();

        let mut results = futures::stream::iter(partitions.into_iter().map(|part| {
            let source = source.clone();
            let plan = plan.clone();
            let cfg = cfg.clone();
            let ctx = ctx.clone();
            let extra_filter = extra_filter.clone();
            let chunk_plan = chunk_plan.clone();
            let base_table = base_table.clone();
            let base_query = base_query.clone();
            async move {
                match source.as_ref() {
                    Source::Postgres(s) => {
                        transfer_partition_postgres(
                            s,
                            &plan,
                            &cfg,
                            &ctx,
                            base_table.as_deref(),
                            base_query.as_deref(),
                            extra_filter.as_deref(),
                            part,
                            chunk_plan.as_ref().as_ref(),
                        )
                        .await
                    }
                    Source::MySql(s) => {
                        transfer_partition_mysql(
                            s,
                            &plan,
                            &cfg,
                            &ctx,
                            base_table.as_deref(),
                            base_query.as_deref(),
                            extra_filter.as_deref(),
                            part,
                            chunk_plan.as_ref().as_ref(),
                        )
                        .await
                    }
                    Source::BigQuery(_) => unreachable!("BigQuery is handled via the early return in run_transfer"),
                }
            }
        }))
        .buffer_unordered(cfg.parallelism);

        while let Some(r) = results.next().await {
            r?; // propagate the first partition error
        }
        // The streaming phase ends here: every row has been read, decoded and
        // written into the target (staging, or the destination itself).
        // Everything after this is promotion.
        let stage_secs = stage_started.elapsed().as_secs_f64();
        let promote_started = Instant::now();
        tracing::info!(
            "all partitions read: {} rows written",
            counters.rows_written.load(Ordering::Relaxed)
        );

        // --- Full refresh: validate staging, then atomically swap it into place. ---
        if cfg.mode == SyncMode::Full {
            run_staged_validation(
                &on_staged,
                sink.as_ref(),
                &staging,
                counters.rows_written.load(Ordering::Relaxed),
            )?;
            guard_full_refresh_shrink(
                sink.as_ref(),
                &cfg,
                counters.rows_written.load(Ordering::Relaxed),
                &warnings,
            )
            .await?;
            tracing::info!("swapping staging table into '{}'", cfg.dest_table);
            sink.atomic_swap(&cfg.dest_table, &staging, &plan.dest_columns).await?;
            sink.drop_table(&staging).await?;
        }

        // --- Incremental: validate + promote staged rows (MERGE for BigQuery,
        // insert-select for a gated ClickHouse run; direct-insert runs did not
        // stage and have nothing to promote), then persist the new watermark. ---
        let mut rows_deleted = 0u64;
        if cfg.mode == SyncMode::Incremental {
            if used_staging {
                rows_deleted = promote_staged_incremental(
                    sink.as_ref(),
                    &cfg.dest_table,
                    &staging,
                    &cfg.key,
                    &plan.dest_columns,
                    cfg.merge_prune_partition_by.as_deref(),
                    cfg.merge_prune_key_range,
                    cfg.merge_prune_key_list_max,
                    cfg.delete_stale_in_window,
                    &on_staged,
                    counters.rows_written.load(Ordering::Relaxed),
                    cfg.watermark.as_deref(),
                    &warnings,
                )
                .await?;
            }
            if let Some(w) = &new_watermark {
                if cfg.advance_watermark {
                    tracing::info!("persisting new watermark: {w}");
                    sink.persist_watermark(&cfg, w, counters.rows_written.load(Ordering::Relaxed))
                        .await?;
                } else {
                    tracing::info!(
                        "advance_watermark=false: computed watermark {w} NOT persisted (cursor left unchanged)"
                    );
                }
            }
        }

        let duration_secs = started.elapsed().as_secs_f64();
        tracing::info!(
            "transfer complete: {} rows in {:.2}s ({:.0} rows/s)",
            counters.rows_written.load(Ordering::Relaxed),
            duration_secs,
            counters.rows_written.load(Ordering::Relaxed) as f64 / duration_secs.max(0.001)
        );
        Ok(TransferResult {
            rows_read: counters.rows_read.load(Ordering::Relaxed),
            rows_written: counters.rows_written.load(Ordering::Relaxed),
            bytes_written: counters.bytes_written.load(Ordering::Relaxed),
            rows_deleted,
            duration_secs,
            read_secs: counters.read_nanos.load(Ordering::Relaxed) as f64 / 1e9,
            stage_secs,
            promote_secs: promote_started.elapsed().as_secs_f64(),
            new_watermark,
            warnings: warnings.drain(),
        })
    }
    .await;

    if outcome.is_err() && used_staging {
        cleanup_staging(&cleanup_sink, &cleanup_staging_name).await;
    }
    outcome
}

/// BigQuery's whole transfer flow: no discrete range-partitions (see the
/// early-return dispatch in [`run_transfer`]) — one read session, drained
/// sequentially, with `cfg.parallelism` passed through as BigQuery's own
/// `max_stream_count` hint for server-side parallel preparation.
#[allow(clippy::too_many_arguments)]
async fn run_transfer_bigquery(
    source: &BigQuerySource,
    sink: Arc<dyn Sink>,
    cfg: TransferConfig,
    progress: Option<ProgressCb>,
    on_staged: Option<StagedValidationCb>,
    started: Instant,
    archive_info: Option<Arc<ArchiveRunInfo>>,
    staging: String,
    warnings: Warnings,
) -> Result<TransferResult> {
    // column_transforms are injected into the SQL SELECT built for the
    // Postgres/MySQL COPY path; the BigQuery Storage Read API reads bare
    // columns with no SELECT to inject into, so it can't honor them. Reject
    // loudly rather than silently ignoring the transform.
    if !cfg.column_transforms.is_empty() {
        return Err(EtlError::config(
            "column_transforms is not supported for a BigQuery source — express the \
             transform in a source_query instead",
        ));
    }
    if !cfg.column_transform_types.is_empty() {
        return Err(EtlError::config(
            "column_transform_types is not supported for a BigQuery source (it overrides the \
             decode type for a column_transforms entry, which is itself unsupported here)",
        ));
    }
    if cfg.chunk_rows.is_some() {
        return Err(EtlError::config(
            "chunk_rows (keyset resumable reads) is not supported for a BigQuery source",
        ));
    }
    if cfg.watermark_source_expr.is_some() {
        return Err(EtlError::config(
            "watermark_source_expr is not supported for a BigQuery source (its Storage Read API \
             row_restriction always applies to the resolved table's own columns, not a nested \
             query, so the full-scan risk it addresses doesn't apply there)",
        ));
    }
    if cfg.partition_source_expr.is_some() {
        return Err(EtlError::config(
            "partition_source_expr is not supported for a BigQuery source (it reads through one \
             Storage Read session whose own stream count is the parallelism — there are no \
             discrete range partitions for an expression to bound)",
        ));
    }
    let (client, project_id) = source.connect().await?;
    tracing::info!("authenticated with bigquery, project_id={project_id}");

    let (source_cols, table_ref) = if let Some(t) = &cfg.source_table {
        let table_ref = source.parse_table_ref(t, &project_id)?;
        let cols = source.resolve_table_columns(&client, &table_ref).await?;
        (cols, table_ref)
    } else if let Some(q) = &cfg.source_query {
        tracing::info!("running bigquery query job to resolve source_query...");
        let (cols, dest) = source.run_query(&client, &project_id, q).await?;
        tracing::info!(
            "query job complete: destination table {}.{}.{}",
            dest.project_id,
            dest.dataset_id,
            dest.table_id
        );
        (cols, dest)
    } else {
        unreachable!("validated: source_table or source_query required");
    };
    tracing::info!("resolved {} source column(s)", source_cols.len());

    let plan: SelectPlan = transform::plan(&source_cols, &cfg, sink.dest_kind())?;

    let (row_restriction, new_watermark) = if cfg.mode == SyncMode::Incremental {
        let watermark = cfg.watermark.as_ref().unwrap();
        ensure_watermark_column(watermark, &source_cols)?;
        ensure_lookback_compatible(watermark, cfg.lookback_seconds, &source_cols)?;
        let last = sink.read_last_watermark(&cfg).await?;
        let table_sql = crate::source::bigquery::table_sql(&table_ref);
        let snapshot_max = source
            .max_watermark(&client, &project_id, &table_sql, watermark)
            .await?;
        // First run only: apply the seed (needs snapshot_max, computed above,
        // for the CurrentMax variant). Self-retires once a cursor is persisted.
        let last = last.or_else(|| seed_value(&cfg.seed_watermark, snapshot_max.as_deref()));
        tracing::info!(
            "incremental watermark on '{}': last synced={:?}, current source max={:?}",
            watermark,
            last,
            snapshot_max
        );
        let filter = build_watermark_filter_bigquery(
            watermark,
            last.as_deref(),
            snapshot_max.as_deref(),
            cfg.lookback_seconds,
            &source_cols,
        );
        (filter, snapshot_max)
    } else {
        (None, None)
    };

    // Force a staging table for a gated incremental run into a directly-inserting
    // destination (ClickHouse), so the gate has something to validate (see the
    // DB flow for the rationale).
    // The same is true of a window-scoped delete on a destination whose upsert
    // cannot express one inline: the delete subtracts the staged keys, so it
    // needs the batch materialised in a table before any of it lands.
    let force_stage_incremental = cfg.mode == SyncMode::Incremental
        && !sink.requires_staging_for_incremental()
        && (on_staged.is_some()
            || (cfg.delete_stale_in_window && !sink.deletes_stale_within_merge()));
    if cfg.delete_stale_in_window && !sink.supports_row_delete() {
        return Err(EtlError::config(
            "delete_stale_in_window is not supported by this destination (it cannot delete \
             individual rows)",
        ));
    }

    let target_table = prepare_target(
        &sink,
        &cfg,
        &plan.dest_columns,
        &staging,
        force_stage_incremental,
    )
    .await?;
    let used_staging = target_table != cfg.dest_table;
    let cleanup_sink = sink.clone();
    let cleanup_staging_name = staging.clone();

    // Fallible tail wrapped so any error triggers best-effort staging cleanup
    // below (a unique-per-run staging name is never reclaimed by a later run).
    let outcome: Result<TransferResult> = async move {
        tracing::info!(
            "quickhouse: transferring into {} via BigQuery Storage Read API, max_stream_count={}",
            target_table,
            cfg.parallelism
        );

        let counters = Arc::new(Counters::default());
        let ctx = SendCtx {
            sink: sink.clone(),
            budget: MemoryBudget::new(cfg.max_memory_bytes),
            target_table: Arc::new(target_table),
            counters: counters.clone(),
            progress: progress.clone(),
            started,
            archive: archive_info,
            // read_max_rows_per_sec is a Postgres/MySQL knob; the BigQuery
            // Storage Read API is a separately-metered managed service, so this
            // path never throttles.
            throttle: None,
            warnings: warnings.clone(),
        };
        let stage_started = Instant::now();

        let mut iter = source
            .read_table::<google_cloud_bigquery::storage::row::Row>(
                &client,
                &table_ref,
                &plan.source_columns,
                row_restriction.as_deref(),
                cfg.parallelism as i32,
            )
            .await?;

        let mut batcher = BigQueryBatcher::with_batch_bytes(&plan.dest_columns, cfg.batch_rows, cfg.batch_bytes)?;
        let schema = batcher.schema();
        let mut sends: JoinSet<Result<()>> = JoinSet::new();
        let mut insert_buf = InsertBuffer::new(cfg.insert_bytes);
        // No discrete partitions on this path (see the module docs) — "all" is
        // the only file this run will ever archive for this table.
        let mut archive_writer = match &ctx.archive {
            Some(info) => Some(info.writer_for("all", schema.clone())?),
            None => None,
        };
        while let Some(row) = await_source(
            iter.next(),
            &counters,
            cfg.read_idle_timeout_secs,
            "bigquery read",
        )
        .await?
        .map_err(|e| EtlError::other(format!("bigquery row error: {e}")))?
        {
            if let Some(batch) = batcher.append_row(&row)? {
                if let Some(w) = archive_writer.as_mut() {
                    w.write(&batch).await?;
                }
                ctx.push_batch(&mut sends, &mut insert_buf, schema.clone(), batch).await;
                reap(&mut sends, false).await?;
            }
        }
        if let Some(batch) = batcher.finish()? {
            if let Some(w) = archive_writer.as_mut() {
                w.write(&batch).await?;
            }
            ctx.push_batch(&mut sends, &mut insert_buf, schema.clone(), batch).await;
        }
        if let Some(w) = archive_writer.take() {
            w.close().await?;
        }
        ctx.flush(&mut sends, &mut insert_buf, schema.clone()).await;
        reap(&mut sends, true).await?;
        counters
            .rows_read
            .fetch_add(batcher.rows_total, Ordering::Relaxed);
        emit_progress(&counters, &progress, started);
        report_coercions("bigquery read", batcher.coercions(), &warnings);
        let stage_secs = stage_started.elapsed().as_secs_f64();
        let promote_started = Instant::now();
        tracing::info!(
            "bigquery read complete: {} rows written",
            counters.rows_written.load(Ordering::Relaxed)
        );

        if cfg.mode == SyncMode::Full {
            run_staged_validation(
                &on_staged,
                sink.as_ref(),
                &staging,
                counters.rows_written.load(Ordering::Relaxed),
            )?;
            guard_full_refresh_shrink(
                sink.as_ref(),
                &cfg,
                counters.rows_written.load(Ordering::Relaxed),
                &warnings,
            )
            .await?;
            tracing::info!("swapping staging table into '{}'", cfg.dest_table);
            sink.atomic_swap(&cfg.dest_table, &staging, &plan.dest_columns).await?;
            sink.drop_table(&staging).await?;
        }
        let mut rows_deleted = 0u64;
        if cfg.mode == SyncMode::Incremental {
            if used_staging {
                rows_deleted = promote_staged_incremental(
                    sink.as_ref(),
                    &cfg.dest_table,
                    &staging,
                    &cfg.key,
                    &plan.dest_columns,
                    cfg.merge_prune_partition_by.as_deref(),
                    cfg.merge_prune_key_range,
                    cfg.merge_prune_key_list_max,
                    cfg.delete_stale_in_window,
                    &on_staged,
                    counters.rows_written.load(Ordering::Relaxed),
                    cfg.watermark.as_deref(),
                    &warnings,
                )
                .await?;
            }
            if let Some(w) = &new_watermark {
                if cfg.advance_watermark {
                    tracing::info!("persisting new watermark: {w}");
                    sink.persist_watermark(&cfg, w, counters.rows_written.load(Ordering::Relaxed))
                        .await?;
                } else {
                    tracing::info!(
                        "advance_watermark=false: computed watermark {w} NOT persisted (cursor left unchanged)"
                    );
                }
            }
        }

        let duration_secs = started.elapsed().as_secs_f64();
        tracing::info!(
            "transfer complete: {} rows in {:.2}s ({:.0} rows/s)",
            counters.rows_written.load(Ordering::Relaxed),
            duration_secs,
            counters.rows_written.load(Ordering::Relaxed) as f64 / duration_secs.max(0.001)
        );
        Ok(TransferResult {
            rows_read: counters.rows_read.load(Ordering::Relaxed),
            rows_written: counters.rows_written.load(Ordering::Relaxed),
            bytes_written: counters.bytes_written.load(Ordering::Relaxed),
            rows_deleted,
            duration_secs,
            read_secs: counters.read_nanos.load(Ordering::Relaxed) as f64 / 1e9,
            stage_secs,
            promote_secs: promote_started.elapsed().as_secs_f64(),
            new_watermark,
            warnings: warnings.drain(),
        })
    }
    .await;

    if outcome.is_err() && used_staging {
        cleanup_staging(&cleanup_sink, &cleanup_staging_name).await;
    }
    outcome
}

// ---- HTTP API sources (CleverTap / AppsFlyer) ----

/// Refuse a full-refresh swap that would shrink the destination.
///
/// `atomic_swap` replaces the destination in its entirety — ClickHouse
/// `EXCHANGE TABLES`, BigQuery `TRUNCATE` + `INSERT ... SELECT` — and neither
/// is partition-aware. A run whose data covers less than the destination
/// already holds therefore destroys the remainder, atomically and silently,
/// then reports success. Comparing row counts is the one source-agnostic
/// signal that catches every shape of it: a one-day API pull swapped over a
/// year of history, or a one-month DB refresh swapped into a monthly-
/// partitioned table.
///
/// This used to be a `tracing::warn!` on the API path only, which is why it
/// never stopped anything: a warning does not prevent a swap, and the DB path
/// carried the identical footgun with no warning at all.
///
/// `current_row_count` is best-effort by contract, so a count we cannot read
/// must not fail the transfer — but it is logged at `warn`, because it means
/// the guard did not actually run.
async fn guard_full_refresh_shrink(
    sink: &dyn Sink,
    cfg: &TransferConfig,
    new_rows: u64,
    warnings: &Warnings,
) -> Result<()> {
    let existing = match sink.current_row_count(&cfg.dest_table).await {
        Ok(n) => n,
        Err(e) => {
            tracing::warn!(
                "full-refresh shrink guard could not read the current row count of '{}' ({e}); \
                 proceeding with the swap UNCHECKED.",
                cfg.dest_table
            );
            return Ok(());
        }
    };
    match full_refresh_shrink_verdict(existing, new_rows, cfg.allow_full_refresh_shrink) {
        ShrinkVerdict::Proceed => Ok(()),
        ShrinkVerdict::ProceedWithWarning { existing, lost } => {
            let message = format!(
                "full-refresh will REPLACE '{}' ({existing} row(s)) with only {new_rows} row(s) — \
                 the destination will SHRINK by {lost} row(s). Proceeding because \
                 allow_full_refresh_shrink=True.",
                cfg.dest_table,
            );
            tracing::warn!("{message}");
            warnings.push(TransferWarning {
                kind: WarningKind::FullRefreshShrink,
                column: None,
                // The rows the destination is about to lose — the number that
                // makes "it shrank" concrete enough for a scheduler to gate on.
                count: lost,
                sample: None,
                message,
            });
            Ok(())
        }
        ShrinkVerdict::Refuse { existing, lost } => Err(EtlError::config(format!(
            "refusing full-refresh: swapping staging into '{}' would REPLACE {existing} row(s) \
             with only {new_rows} row(s), shrinking the destination by {lost}. A full refresh \
             replaces the table wholesale and is NOT partition-aware, so if this run covers only \
             part of the destination the rest is destroyed. To add rows instead of replacing them \
             use mode=\"incremental\" with key=, or mode=\"append\". If this shrink is genuinely \
             intended, set allow_full_refresh_shrink=True.",
            cfg.dest_table,
        ))),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ShrinkVerdict {
    Proceed,
    ProceedWithWarning { existing: u64, lost: u64 },
    Refuse { existing: u64, lost: u64 },
}

/// The decision half of [`guard_full_refresh_shrink`], split out as a pure
/// function so it is unit-testable without an authenticated sink — the same
/// reason `build_swap_sql` and `build_merge_sql` are free functions.
///
/// `existing` is `None` when the destination does not exist yet or the sink
/// cannot report a count; neither is evidence of a shrink, so both proceed.
fn full_refresh_shrink_verdict(
    existing: Option<u64>,
    new_rows: u64,
    allow_shrink: bool,
) -> ShrinkVerdict {
    let Some(existing) = existing else {
        return ShrinkVerdict::Proceed;
    };
    let Some(lost) = existing.checked_sub(new_rows).filter(|n| *n > 0) else {
        return ShrinkVerdict::Proceed;
    };
    if allow_shrink {
        ShrinkVerdict::ProceedWithWarning { existing, lost }
    } else {
        ShrinkVerdict::Refuse { existing, lost }
    }
}

/// The declared output schema for an API source (API sources have no catalog
/// to resolve against, so the caller declares the columns).
///
/// NOTE: this doc comment used to read "API sources write only to BigQuery.
/// Reject any other destination up front with a clear config error." That gate
/// (`ensure_api_dest_supported`) was deliberately removed in `bc1ab45` when API
/// sources gained ClickHouse support; the comment outlived it and sat here
/// describing a rejection that no longer exists, attached to a function that
/// only returns a slice.
fn api_columns_of(source_cfg: &SourceConfig) -> &[ApiColumn] {
    match source_cfg {
        SourceConfig::CleverTap(c) => &c.columns,
        SourceConfig::AppsFlyer(a) => &a.columns,
        SourceConfig::HttpApi(h) => &h.columns,
        _ => &[],
    }
}

fn api_source_window(source_cfg: &SourceConfig) -> (Option<&str>, Option<&str>) {
    match source_cfg {
        SourceConfig::CleverTap(c) => (c.from_date.as_deref(), c.to_date.as_deref()),
        SourceConfig::AppsFlyer(a) => (a.from_date.as_deref(), a.to_date.as_deref()),
        SourceConfig::HttpApi(h) => (h.from_date.as_deref(), h.to_date.as_deref()),
        _ => (None, None),
    }
}

/// Seed `type_overrides` from each declared column's BigQuery type so the
/// destination table is created with the exact declared type (JSON/TIME/NUMERIC
/// etc.). A user-supplied override for the same column wins (escape hatch).
fn seed_api_type_overrides(cfg: &mut TransferConfig, cols: &[ApiColumn]) -> Result<()> {
    for c in cols {
        let canon = crate::decode_api::canonical_declared_type(c)?;
        cfg.type_overrides
            .entry(c.name.clone())
            .or_insert_with(|| canon.to_string());
    }
    Ok(())
}

fn api_lookback_days(source_cfg: &SourceConfig) -> u32 {
    match source_cfg {
        SourceConfig::CleverTap(c) => c.lookback_days,
        SourceConfig::AppsFlyer(a) => a.lookback_days,
        SourceConfig::HttpApi(h) => h.lookback_days,
        _ => 0,
    }
}

/// Shift a `"YYYY-MM-DD"` date back by `days`.
fn subtract_days(date: &str, days: u32) -> Result<String> {
    let d = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .map_err(|e| EtlError::config(format!("invalid lookback date '{date}': {e}")))?;
    let shifted = d
        .checked_sub_days(chrono::Days::new(days as u64))
        .ok_or_else(|| {
            EtlError::config(format!("lookback of {days} days underflows from '{date}'"))
        })?;
    Ok(shifted.format("%Y-%m-%d").to_string())
}

/// Resolve the `[from, to]` date window (`"YYYY-MM-DD"`) and the watermark to
/// persist. Full mode requires an explicit `from_date`. Incremental/append
/// derive `from` from the persisted cursor, else the seed, else `from_date`; on
/// a resume, `lookback_days` widens `from` back to re-pull late-arriving rows
/// (clamped to the `from_date` floor). `to` defaults to today; the window is
/// clamped to `from <= to`.
fn derive_api_window(
    cfg: &TransferConfig,
    source_cfg: &SourceConfig,
    committed: Option<String>,
) -> Result<(String, String, Option<String>)> {
    let (from_date, to_date) = api_source_window(source_cfg);
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let to = to_date.map(str::to_string).unwrap_or(today);
    if cfg.mode == SyncMode::Full {
        let from = from_date.map(str::to_string).ok_or_else(|| {
            EtlError::config("API full-refresh needs a from_date (window start, \"YYYY-MM-DD\")")
        })?;
        Ok((from, to, None))
    } else {
        let resuming = committed.is_some();
        let mut from = committed
            .or_else(|| seed_value(&cfg.seed_watermark, Some(&to)))
            .or_else(|| from_date.map(str::to_string))
            .ok_or_else(|| {
                EtlError::config(
                    "first incremental/append API run needs a start date: set from_date, seed_watermark=, or skip_to_max=True",
                )
            })?;
        // On a resume, re-pull a rolling lookback window before the cursor so
        // late-arriving/restated rows past the boundary day aren't missed.
        let lookback = api_lookback_days(source_cfg);
        if resuming && lookback > 0 {
            from = subtract_days(&from, lookback)?;
            // Never pull before the configured floor.
            if let Some(floor) = from_date {
                if from.as_str() < floor {
                    from = floor.to_string();
                }
            }
        }
        if from > to {
            from = to.clone(); // guard a clock-skew / stale-cursor inversion
        }
        Ok((from, to.clone(), Some(to)))
    }
}

/// Transfer from an HTTP API source (CleverTap/AppsFlyer) into any destination
/// (BigQuery or ClickHouse). Mirrors `run_transfer_bigquery`'s
/// staging/swap/merge/watermark tail; only the read side differs — a declared
/// schema + paginated fetch instead of a DB read.
#[allow(clippy::too_many_arguments)]
async fn run_transfer_api(
    source_cfg: SourceConfig,
    sink: Arc<dyn Sink>,
    mut cfg: TransferConfig,
    progress: Option<ProgressCb>,
    on_staged: Option<StagedValidationCb>,
    started: Instant,
    staging: String,
    warnings: Warnings,
) -> Result<TransferResult> {
    // Without a source_table, `effective_state_key()` is empty — give the
    // incremental cursor a stable identity. A user `state_key` still wins.
    if cfg.state_key.is_none() {
        cfg.state_key = source_cfg.api_state_identity();
    }
    let cols = api_columns_of(&source_cfg).to_vec();
    // The declared BigQuery type names only make sense for a BigQuery
    // destination (they seed its exact DDL type). A ClickHouse destination
    // instead takes its column types from the resolved Arrow/ClickHouse mapping,
    // so seeding BigQuery names there would produce invalid DDL — skip it.
    if matches!(sink.dest_kind(), crate::config::DestKind::BigQuery) {
        seed_api_type_overrides(&mut cfg, &cols)?;
    }
    let source_cols = resolve_api_columns(&cols)?;
    let plan: SelectPlan = transform::plan(&source_cols, &cfg, sink.dest_kind())?;

    // Per-output-column lookup path, aligned to `plan.source_columns` (which
    // survives include/exclude; keyed on the source name, unaffected by rename).
    let path_by_name: std::collections::HashMap<&str, &str> = cols
        .iter()
        .map(|c| {
            (
                c.name.as_str(),
                c.path.as_deref().unwrap_or(c.name.as_str()),
            )
        })
        .collect();
    let lookups: Vec<String> = plan
        .source_columns
        .iter()
        .map(|n| {
            path_by_name
                .get(n.as_str())
                .copied()
                .unwrap_or(n.as_str())
                .to_string()
        })
        .collect();

    // Incremental and append both resume from a persisted date cursor (append
    // inserts instead of merging); full mode has none.
    let committed = if matches!(cfg.mode, SyncMode::Incremental | SyncMode::Append) {
        let watermark = cfg.watermark.as_ref().unwrap();
        ensure_watermark_column(watermark, &source_cols)?;
        sink.read_last_watermark(&cfg).await?
    } else {
        None
    };
    let (from, to, new_watermark) = derive_api_window(&cfg, &source_cfg, committed)?;

    // Force a staging table for a gated incremental run into a directly-inserting
    // destination (ClickHouse), so the gate has something to validate (see the
    // DB flow for the rationale). Append mode has no staging option and is
    // rejected up front when a gate is attached.
    // The same is true of a window-scoped delete on a destination whose upsert
    // cannot express one inline: the delete subtracts the staged keys, so it
    // needs the batch materialised in a table before any of it lands.
    let force_stage_incremental = cfg.mode == SyncMode::Incremental
        && !sink.requires_staging_for_incremental()
        && (on_staged.is_some()
            || (cfg.delete_stale_in_window && !sink.deletes_stale_within_merge()));
    if cfg.delete_stale_in_window && !sink.supports_row_delete() {
        return Err(EtlError::config(
            "delete_stale_in_window is not supported by this destination (it cannot delete \
             individual rows)",
        ));
    }

    let target_table = prepare_target(
        &sink,
        &cfg,
        &plan.dest_columns,
        &staging,
        force_stage_incremental,
    )
    .await?;
    let used_staging = target_table != cfg.dest_table;
    let cleanup_sink = sink.clone();
    let cleanup_staging_name = staging.clone();

    let outcome: Result<TransferResult> = async move {
        tracing::info!(
            "quickhouse: {} API transfer into {} for [{from}, {to}]",
            source_cfg.kind(),
            target_table
        );
        let counters = Arc::new(Counters::default());
        let ctx = SendCtx {
            sink: sink.clone(),
            budget: MemoryBudget::new(cfg.max_memory_bytes),
            target_table: Arc::new(target_table),
            counters: counters.clone(),
            progress: progress.clone(),
            started,
            archive: None,
            throttle: None,
            warnings: warnings.clone(),
        };
        let stage_started = Instant::now();
        let mut batcher = ApiBatcher::new(&plan.dest_columns, &lookups, cfg.batch_rows, cfg.batch_bytes)?;
        let schema = batcher.schema();
        let mut sends: JoinSet<Result<()>> = JoinSet::new();
        let mut insert_buf = InsertBuffer::new(cfg.insert_bytes);

        match &source_cfg {
            SourceConfig::CleverTap(c) => {
                let src = CleverTapSource::new(c)?;
                let from_i = crate::source::clevertap::iso_to_yyyymmdd(&from)?;
                let to_i = crate::source::clevertap::iso_to_yyyymmdd(&to)?;
                let mut cursor = src.create_export(&c.event_name, from_i, to_i).await?;
                // The chain ends when a page carries no next cursor, and only
                // then. `status` is deliberately NOT a termination signal: the
                // live API sends "success" on every page, so the old rule —
                // stop on the first "success" — read one page of every export
                // and reported it clean (measured: 4,991 of 146,852 records,
                // 3.40%, for one event-day). See the module docs.
                let mut pages: u64 = 0;
                let mut records_total: u64 = 0;
                let stop: &str;
                loop {
                    let page = src.next_page(&cursor).await?;
                    pages += 1;
                    records_total += page.records.len() as u64;
                    tracing::debug!(
                        "clevertap '{}': page {pages} status={:?} records={} next_cursor={}",
                        c.event_name,
                        page.status,
                        page.records.len(),
                        if page.next_cursor.is_some() { "yes" } else { "no" },
                    );
                    for rec in &page.records {
                        if let Some(b) = batcher.append_record(rec)? {
                            ctx.push_batch(&mut sends, &mut insert_buf, schema.clone(), b).await;
                            reap(&mut sends, false).await?;
                        }
                    }
                    // Records are consumed above BEFORE this check, so the
                    // final page's rows are never dropped.
                    let Some(next) = page.next_cursor else {
                        stop = "no next_cursor (end of export)";
                        break;
                    };
                    // A cursor that does not advance would spin forever, and an
                    // export that genuinely ends says so by omitting the key
                    // rather than by repeating it. Treat a repeat as the end and
                    // say so loudly — it is the one stop reason that indicates a
                    // vendor-side anomaly rather than a normal finish.
                    if next == cursor {
                        tracing::warn!(
                            "clevertap '{}': page {pages} returned the SAME cursor it was fetched \
                             with, so the chain is not advancing; stopping after \
                             {records_total} record(s). This is a vendor-side anomaly — treat the \
                             result as incomplete.",
                            c.event_name,
                        );
                        stop = "cursor stopped advancing";
                        break;
                    }
                    cursor = next;
                }
                tracing::info!(
                    "clevertap '{}': read {records_total} record(s) across {pages} page(s) for \
                     [{from}, {to}]; stopped on {stop}",
                    c.event_name,
                );
                if pages == 1 {
                    tracing::warn!(
                        "clevertap '{}': the export ended after a SINGLE page ({records_total} \
                         record(s)). For a busy event that is the signature of a paging failure, \
                         not an empty day — verify against the vendor's own count before trusting \
                         this run.",
                        c.event_name,
                    );
                }
                tracing::info!(
                    "clevertap '{}': read {records_total} record(s) across {pages} page(s) for \
                     [{from}, {to}]; stopped on {stop}",
                    c.event_name,
                );
                if pages == 1 {
                    tracing::warn!(
                        "clevertap '{}': the export ended after a SINGLE page ({records_total} \
                         record(s)). For a busy event that is the signature of a paging failure, \
                         not an empty day — verify against the vendor's own count before trusting \
                         this run.",
                        c.event_name,
                    );
                }
            }
            SourceConfig::AppsFlyer(a) => {
                let src = AppsFlyerSource::new(a)?;
                let records = src.fetch_records(&from, &to, &lookups).await?;
                for rec in &records {
                    if let Some(b) = batcher.append_record(rec)? {
                        ctx.push_batch(&mut sends, &mut insert_buf, schema.clone(), b).await;
                        reap(&mut sends, false).await?;
                    }
                }
            }
            SourceConfig::HttpApi(h) => {
                let src = crate::source::http_api::HttpApiSource::new(h)?;
                let records = src.fetch_records(&from, &to, &lookups).await?;
                for rec in &records {
                    if let Some(b) = batcher.append_record(rec)? {
                        ctx.push_batch(&mut sends, &mut insert_buf, schema.clone(), b).await;
                        reap(&mut sends, false).await?;
                    }
                }
            }
            _ => unreachable!("run_transfer_api only handles API sources"),
        }
        if let Some(b) = batcher.finish()? {
            ctx.push_batch(&mut sends, &mut insert_buf, schema.clone(), b).await;
        }
        ctx.flush(&mut sends, &mut insert_buf, schema.clone()).await;
        reap(&mut sends, true).await?;
        counters.rows_read.fetch_add(batcher.rows_total, Ordering::Relaxed);
        emit_progress(&counters, &progress, started);
        report_coercions("api read", batcher.coercions(), &warnings);
        let stage_secs = stage_started.elapsed().as_secs_f64();
        let promote_started = Instant::now();
        // Distinct, actionable warning for a declared DATE/TIMESTAMP column that
        // came out NULL for EVERY source value — a unit/format mismatch (e.g. a
        // packed `yyyyMMddHHmmSS` `ts` mis-declared), not real nulls.
        for (col, n, sample) in batcher.fully_coerced_temporal_columns() {
            tracing::warn!(
                "api read: declared date/time column '{col}' was NULL for all {n} non-empty source \
                 value(s) (e.g. raw {sample:?}) — likely a unit/format mismatch, not genuine nulls. \
                 Verify the declared type matches the source field's format."
            );
        }
        tracing::info!(
            "api read complete: {} rows written",
            counters.rows_written.load(Ordering::Relaxed)
        );

        if cfg.mode == SyncMode::Full {
            // A full-refresh REPLACES the destination, and API sources are
            // naturally day/event-scoped, so a full run against an existing
            // large table swaps 100Ms of rows away for a handful. This used to
            // be a `warn!` here and nothing at all on the DB path, which is why
            // it never stopped anything; it is now a hard refusal shared by
            // every swap site (`guard_full_refresh_shrink`).
            run_staged_validation(
                &on_staged,
                sink.as_ref(),
                &staging,
                counters.rows_written.load(Ordering::Relaxed),
            )?;
            guard_full_refresh_shrink(
                sink.as_ref(),
                &cfg,
                counters.rows_written.load(Ordering::Relaxed),
                &warnings,
            )
            .await?;
            tracing::info!("swapping staging table into '{}'", cfg.dest_table);
            sink.atomic_swap(&cfg.dest_table, &staging, &plan.dest_columns).await?;
            sink.drop_table(&staging).await?;
        }
        let mut rows_deleted = 0u64;
        if cfg.mode == SyncMode::Incremental {
            if used_staging {
                rows_deleted = promote_staged_incremental(
                    sink.as_ref(),
                    &cfg.dest_table,
                    &staging,
                    &cfg.key,
                    &plan.dest_columns,
                    cfg.merge_prune_partition_by.as_deref(),
                    cfg.merge_prune_key_range,
                    cfg.merge_prune_key_list_max,
                    cfg.delete_stale_in_window,
                    &on_staged,
                    counters.rows_written.load(Ordering::Relaxed),
                    cfg.watermark.as_deref(),
                    &warnings,
                )
                .await?;
            }
            if let Some(w) = &new_watermark {
                if cfg.advance_watermark {
                    tracing::info!("persisting new watermark (window end): {w}");
                    sink.persist_watermark(&cfg, w, counters.rows_written.load(Ordering::Relaxed))
                        .await?;
                } else {
                    tracing::info!("advance_watermark=false: window end {w} NOT persisted");
                }
            }
        }
        if cfg.mode == SyncMode::Append {
            // Rows were inserted straight into the destination (target_table ==
            // dest_table): no staging, no merge, no swap. Only persist the
            // resume cursor.
            if let Some(w) = &new_watermark {
                if cfg.advance_watermark {
                    tracing::info!("append: persisting new watermark (window end): {w}");
                    sink.persist_watermark(&cfg, w, counters.rows_written.load(Ordering::Relaxed))
                        .await?;
                } else {
                    tracing::info!("append: advance_watermark=false: window end {w} NOT persisted");
                }
            }
        }

        let duration_secs = started.elapsed().as_secs_f64();
        tracing::info!(
            "transfer complete: {} rows in {:.2}s",
            counters.rows_written.load(Ordering::Relaxed),
            duration_secs
        );
        Ok(TransferResult {
            rows_read: counters.rows_read.load(Ordering::Relaxed),
            rows_written: counters.rows_written.load(Ordering::Relaxed),
            bytes_written: counters.bytes_written.load(Ordering::Relaxed),
            rows_deleted,
            duration_secs,
            read_secs: counters.read_nanos.load(Ordering::Relaxed) as f64 / 1e9,
            stage_secs,
            promote_secs: promote_started.elapsed().as_secs_f64(),
            new_watermark,
            warnings: warnings.drain(),
        })
    }
    .await;

    if outcome.is_err() && used_staging {
        cleanup_staging(&cleanup_sink, &cleanup_staging_name).await;
    }
    outcome
}

async fn setup_postgres(
    s: &PgSource,
    cfg: &TransferConfig,
    base_table: Option<&str>,
    base_query: Option<&str>,
    watermark: Option<&str>,
    warnings: &Warnings,
) -> Result<SourceSetup> {
    tracing::info!("connecting to postgres...");
    let control = s.connect().await?;
    tracing::debug!("postgres connection established");
    // `source_query` takes precedence over `source_table` — matching
    // `PgSource::copy_sql`, `PgSource::max_watermark`, and `source_table`'s own
    // documented contract ("Ignored if `source_query` is set"). The schema probe
    // used to invert exactly this one decision, so passing *both* resolved the
    // decode types from the bare table while the data was read from the query.
    // Postgres's binary `COPY` payload carries no per-field type tag, so the
    // wire bytes were then decoded against the table's OIDs: a widened field
    // (say `id::bigint` over an `int4` column) silently lost its value rather
    // than erroring, since the field readers take a byte-count prefix and
    // `read_i32` just takes the first 4 big-endian bytes of the 8 sent.
    //
    // When both are set the table is ignored for NOT NULL enrichment too, so
    // "both" behaves identically to "query only": the query may join or
    // outer-join in ways that make a NOT NULL base column nullable in the
    // result, and inheriting the table's NOT NULL set would then declare a
    // non-nullable destination column that real NULLs violate.
    let (schema_probe, not_null_from) = match (base_table, base_query) {
        (_, Some(q)) => (q.to_string(), None),
        (Some(t), None) => (format!("SELECT * FROM {}", quote_pg_table(t)), Some(t)),
        (None, None) => unreachable!("validated above"),
    };
    if base_table.is_some() && base_query.is_some() {
        tracing::warn!(
            "both source_table and source_query are set; source_table is ignored — \
             schema and data both come from source_query"
        );
    }
    let source_cols = s
        .resolve_columns(&control, &schema_probe, not_null_from)
        .await?;

    // Only needed for incremental mode (the value is discarded otherwise) —
    // skip it in full-refresh so a watermark column left set alongside
    // mode="full" can't add a spurious query or fail on an aggregate edge
    // case (e.g. an empty table's MAX() being NULL) for a result nothing uses.
    let snapshot_max = if cfg.mode == SyncMode::Incremental {
        if let Some(w) = watermark {
            ensure_watermark_column(w, &source_cols)?;
            ensure_lookback_compatible(w, cfg.lookback_seconds, &source_cols)?;
            if watermark_column_nullable(w, &source_cols) {
                let null_count = s
                    .count_null_watermark(
                        &control,
                        base_table,
                        base_query,
                        w,
                        cfg.watermark_source_expr.as_deref(),
                    )
                    .await?;
                warn_on_null_watermark(w, null_count, warnings);
            }
            s.max_watermark(
                &control,
                base_table,
                base_query,
                w,
                cfg.watermark_source_expr.as_deref(),
            )
            .await?
        } else {
            None
        }
    } else {
        None
    };

    let partitions = compute_partitions_pg(s, &control, cfg, &source_cols).await?;
    Ok(SourceSetup {
        source_cols,
        snapshot_max,
        partitions,
    })
}

async fn setup_mysql(
    s: &MySqlSource,
    cfg: &TransferConfig,
    base_table: Option<&str>,
    base_query: Option<&str>,
    watermark: Option<&str>,
    warnings: &Warnings,
) -> Result<SourceSetup> {
    tracing::info!("connecting to mysql...");
    let mut control = s.connect().await?;
    tracing::debug!("mysql connection established");
    // `source_query` wins, matching `MySqlSource::select_sql` and
    // `source_table`'s documented contract — see the equivalent comment in
    // `setup_postgres` for the mis-decode this inversion caused.
    let schema_probe = match (base_table, base_query) {
        (_, Some(q)) => q.to_string(),
        (Some(t), None) => format!("SELECT * FROM {}", quote_my_table(t)),
        (None, None) => unreachable!("validated above"),
    };
    if base_table.is_some() && base_query.is_some() {
        tracing::warn!(
            "both source_table and source_query are set; source_table is ignored — \
             schema and data both come from source_query"
        );
    }
    let source_cols = s
        .resolve_columns(&mut control, &schema_probe, cfg.tinyint1_as_bool)
        .await?;

    // Only needed for incremental mode (the value is discarded otherwise) —
    // skip it in full-refresh so a watermark column left set alongside
    // mode="full" can't add a spurious query or fail on an aggregate edge
    // case (e.g. an empty table's MAX() being NULL) for a result nothing uses.
    let snapshot_max = if cfg.mode == SyncMode::Incremental {
        if let Some(w) = watermark {
            ensure_watermark_column(w, &source_cols)?;
            ensure_lookback_compatible(w, cfg.lookback_seconds, &source_cols)?;
            if watermark_column_nullable(w, &source_cols) {
                let null_count = s
                    .count_null_watermark(
                        &mut control,
                        base_table,
                        base_query,
                        w,
                        cfg.watermark_source_expr.as_deref(),
                    )
                    .await?;
                warn_on_null_watermark(w, null_count, warnings);
            }
            s.max_watermark(
                &mut control,
                base_table,
                base_query,
                w,
                cfg.watermark_source_expr.as_deref(),
            )
            .await?
        } else {
            None
        }
    } else {
        None
    };

    let partitions = compute_partitions_mysql(s, &mut control, cfg, &source_cols).await?;
    Ok(SourceSetup {
        source_cols,
        snapshot_max,
        partitions,
    })
}

/// Resolved plan for a keyset-chunked resumable read (see
/// `TransferConfig::chunk_rows`). Built once per run in `run_transfer_impl`.
#[derive(Debug, Clone)]
struct ChunkPlan {
    /// Rows per chunk (`LIMIT`).
    limit: usize,
    /// The keyset ordering column (source name) — unique, NOT NULL, integer.
    keyset_col: String,
    /// Its index in `plan.source_columns` (== the decoded batch column index).
    keyset_idx: usize,
    /// The last fully-committed watermark (stays put across chunks; written as
    /// the per-chunk row's `last_watermark` so a concurrent read sees it).
    committed: Option<String>,
    /// The frozen upper bound this run reads up to (`chunk_upper`), so a resume
    /// re-reads the same window.
    effective_upper: Option<String>,
    /// The cursor to resume past (`None` = start from the beginning).
    start_cursor: Option<String>,
}

/// Validate and resolve the keyset plan for a chunked read. Enforces the
/// correctness contract: the keyset column must be selected, an integer type,
/// NOT NULL (a NULL key is silently skipped by `> cursor`), and not
/// value-transformed (the cursor is compared against the raw column in SQL, so
/// the decoded value must be the raw column value).
fn build_chunk_plan(
    cfg: &TransferConfig,
    plan: &SelectPlan,
    source_cols: &[ColumnType],
    limit: usize,
    committed: Option<String>,
    effective_upper: Option<String>,
    start_cursor: Option<String>,
) -> Result<ChunkPlan> {
    let keyset_col = cfg
        .keyset_column()
        .ok_or_else(|| EtlError::config("chunk_rows requires a keyset ordering column"))?;
    let keyset_idx = plan
        .source_columns
        .iter()
        .position(|c| c == &keyset_col)
        .ok_or_else(|| {
            EtlError::config(format!(
                "keyset column '{keyset_col}' must be selected (it was excluded from the transfer)"
            ))
        })?;
    if cfg.column_transforms.contains_key(&keyset_col) {
        return Err(EtlError::config(format!(
            "keyset column '{keyset_col}' cannot be in column_transforms — chunked reads order \
             and resume on the raw column value, not a transformed one"
        )));
    }
    let col = source_cols
        .iter()
        .find(|c| c.name == keyset_col)
        .ok_or_else(|| {
            EtlError::config(format!("keyset column '{keyset_col}' not found in source"))
        })?;
    if !matches!(
        col.arrow,
        DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::UInt32
    ) {
        return Err(EtlError::config(format!(
            "keyset column '{keyset_col}' must be an integer type for chunk_rows resumable reads \
             (found {:?}); pick a unique integer key",
            col.arrow
        )));
    }
    if col.nullable {
        return Err(EtlError::config(format!(
            "keyset column '{keyset_col}' must be NOT NULL for chunk_rows (a NULL key is silently \
             skipped by the cursor). With source_query, nullability can't be verified — use \
             source_table with a NOT NULL, unique integer key"
        )));
    }
    Ok(ChunkPlan {
        limit,
        keyset_col,
        keyset_idx,
        committed,
        effective_upper,
        start_cursor,
    })
}

/// Read the keyset column's value at the LAST row of `batch` (rows arrive in
/// ascending key order, so this is the chunk's max). `None` if the batch is
/// empty or that cell is NULL (a data-loss guard — a NULL key can't advance the
/// cursor). Supports the integer widths `build_chunk_plan` admits.
fn last_int_key(batch: &RecordBatch, idx: usize) -> Result<Option<i128>> {
    use arrow_array::{Int16Array, Int32Array, Int64Array, UInt32Array};
    let n = batch.num_rows();
    if n == 0 {
        return Ok(None);
    }
    let col = batch.column(idx);
    if col.is_null(n - 1) {
        return Ok(None);
    }
    let row = n - 1;
    let v: i128 = if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        a.value(row) as i128
    } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        a.value(row) as i128
    } else if let Some(a) = col.as_any().downcast_ref::<Int16Array>() {
        a.value(row) as i128
    } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
        a.value(row) as i128
    } else {
        return Err(EtlError::internal(
            "keyset column is not a supported integer array (build_chunk_plan gate should prevent this)",
        ));
    };
    Ok(Some(v))
}

/// Keyset-chunked resumable read for a Postgres source: a loop of bounded
/// `COPY (... WHERE key > cursor ORDER BY key LIMIT N) TO STDOUT` chunks, each
/// made durable (`reap(block=true)`) and its cursor persisted before advancing.
/// A crash resumes from the last persisted cursor within the frozen window.
#[allow(clippy::too_many_arguments)]
async fn transfer_keyset_postgres(
    source: &PgSource,
    plan: &SelectPlan,
    cfg: &TransferConfig,
    ctx: &SendCtx,
    base_table: Option<&str>,
    base_query: Option<&str>,
    extra_filter: Option<&str>,
    chunk: &ChunkPlan,
) -> Result<()> {
    let client = source.connect().await?;
    let col_quoted = quote_pg(&chunk.keyset_col);
    let mut cursor = chunk.start_cursor.clone();
    let partition = Partition {
        label: "keyset".into(),
        predicate: None,
    };
    let schema =
        CopyDecoder::with_batch_bytes(&plan.dest_columns, cfg.batch_rows, cfg.batch_bytes)?
            .schema();
    let mut archive_writer = match &ctx.archive {
        Some(info) => Some(info.writer_for("keyset", schema.clone())?),
        None => None,
    };
    tracing::info!(
        "keyset chunked read starting on '{}' (chunk_rows={}, resume_cursor={:?})",
        chunk.keyset_col,
        chunk.limit,
        cursor
    );

    loop {
        let keyset = Keyset {
            col_quoted: col_quoted.clone(),
            cursor: cursor.clone(),
            limit: chunk.limit,
        };
        let copy_sql = source.copy_sql(
            &plan.source_columns,
            &plan.source_select_exprs,
            base_table,
            base_query,
            &partition,
            extra_filter,
            Some(keyset),
        );
        tracing::debug!("keyset chunk: {copy_sql}");
        let stream = source.copy_stream(&client, &copy_sql).await?;
        futures::pin_mut!(stream);
        let mut decoder =
            CopyDecoder::with_batch_bytes(&plan.dest_columns, cfg.batch_rows, cfg.batch_bytes)?;
        let mut sends: JoinSet<Result<()>> = JoinSet::new();
        let mut insert_buf = InsertBuffer::new(cfg.insert_bytes);
        let mut cursor_candidate: Option<i128> = None;

        // Same read/parse overlap as the non-chunked path (see
        // `feed_off_reactor`): parse on the blocking pool, fetch the next chunk
        // of bytes concurrently.
        // The idle timer wraps the source await and nothing else: it is
        // recorded *inside* `await_source`, at the moment the chunk arrives, so
        // the concurrent decode below neither trips it nor inflates read_secs.
        let idle = cfg.read_idle_timeout_secs;
        let scope = format!("partition '{}'", partition.label);
        let mut pending = await_source(stream.next(), &ctx.counters, idle, &scope).await?;
        while let Some(bytes) = pending {
            let bytes = bytes?;
            let decoding = feed_off_reactor(decoder, bytes);
            let (next, joined) = tokio::join!(
                await_source(stream.next(), &ctx.counters, idle, &scope),
                decoding
            );
            pending = next?;
            let (returned, decoded) = joined.map_err(decode_task_failed)?;
            decoder = returned;
            for batch in decoded? {
                let rows = batch.num_rows() as u64;
                if let Some(k) = last_int_key(&batch, chunk.keyset_idx)? {
                    cursor_candidate = Some(cursor_candidate.map_or(k, |c| c.max(k)));
                }
                if let Some(w) = archive_writer.as_mut() {
                    w.write(&batch).await?;
                }
                ctx.push_batch(&mut sends, &mut insert_buf, schema.clone(), batch)
                    .await;
                if let Some(t) = &ctx.throttle {
                    t.acquire(rows).await;
                }
            }
            reap(&mut sends, false).await?;
        }
        if !decoder.saw_trailer() {
            return Err(EtlError::decode(
                "keyset COPY chunk ended without a trailer".to_string(),
            ));
        }
        if let Some(batch) = decoder.finish()? {
            if let Some(k) = last_int_key(&batch, chunk.keyset_idx)? {
                cursor_candidate = Some(cursor_candidate.map_or(k, |c| c.max(k)));
            }
            if let Some(w) = archive_writer.as_mut() {
                w.write(&batch).await?;
            }
            ctx.push_batch(&mut sends, &mut insert_buf, schema.clone(), batch)
                .await;
        }
        // Force this chunk's rows durable in the destination BEFORE advancing
        // the cursor — the invariant that makes a crash resumable.
        ctx.flush(&mut sends, &mut insert_buf, schema.clone()).await;
        reap(&mut sends, true).await?;

        let rows_this_chunk = decoder.rows_total;
        ctx.counters
            .rows_read
            .fetch_add(rows_this_chunk, Ordering::Relaxed);
        report_coercions("keyset read", decoder.coercions(), &ctx.warnings);

        if rows_this_chunk == 0 {
            break; // empty read — nothing (more) to sync
        }
        // Data-loss guard: rows were read but no key could be extracted (a NULL
        // key slipped through despite the NOT NULL gate) — refuse to advance.
        let next = cursor_candidate.ok_or_else(|| {
            EtlError::other(format!(
                "keyset column '{}' produced no usable value in a non-empty chunk (NULL key?); \
                 refusing to advance the cursor to avoid skipping rows",
                chunk.keyset_col
            ))
        })?;
        let cur = next.to_string();
        ctx.sink
            .persist_chunk_cursor(
                cfg,
                chunk.committed.as_deref(),
                &cur,
                chunk.effective_upper.as_deref().unwrap_or(""),
                ctx.counters.rows_written.load(Ordering::Relaxed),
            )
            .await?;
        cursor = Some(cur);
        emit_progress(&ctx.counters, &ctx.progress, ctx.started);

        if (rows_this_chunk as usize) < chunk.limit {
            break; // short chunk — the window is exhausted
        }
    }
    if let Some(w) = archive_writer.take() {
        w.close().await?;
    }
    tracing::info!(
        "keyset chunked read complete: {} rows read",
        ctx.counters.rows_read.load(Ordering::Relaxed)
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn transfer_partition_postgres(
    source: &PgSource,
    plan: &SelectPlan,
    cfg: &TransferConfig,
    ctx: &SendCtx,
    base_table: Option<&str>,
    base_query: Option<&str>,
    extra_filter: Option<&str>,
    partition: Partition,
    chunk: Option<&ChunkPlan>,
) -> Result<()> {
    if let Some(cp) = chunk {
        return transfer_keyset_postgres(
            source,
            plan,
            cfg,
            ctx,
            base_table,
            base_query,
            extra_filter,
            cp,
        )
        .await;
    }
    tracing::info!("partition '{}' starting", partition.label);
    let client = source.connect().await?;
    let copy_sql = source.copy_sql(
        &plan.source_columns,
        &plan.source_select_exprs,
        base_table,
        base_query,
        &partition,
        extra_filter,
        None,
    );
    tracing::debug!("partition {}: {copy_sql}", partition.label);

    let stream = source.copy_stream(&client, &copy_sql).await?;
    futures::pin_mut!(stream);

    let mut decoder =
        CopyDecoder::with_batch_bytes(&plan.dest_columns, cfg.batch_rows, cfg.batch_bytes)?;
    let schema = decoder.schema();
    let mut sends: JoinSet<Result<()>> = JoinSet::new();
    let mut insert_buf = InsertBuffer::new(cfg.insert_bytes);
    let mut archive_writer = match &ctx.archive {
        Some(info) => Some(info.writer_for(&partition.label, schema.clone())?),
        None => None,
    };

    // Read/parse overlap: each chunk's parse runs on the blocking pool while
    // the next chunk is pulled off the socket, so the COPY stream keeps draining
    // instead of idling for the duration of every parse.
    // The idle timer wraps the source await and nothing else: it is recorded
    // *inside* `await_source`, at the moment the chunk arrives, so the
    // concurrent decode below neither trips it nor inflates read_secs.
    let idle = cfg.read_idle_timeout_secs;
    let scope = format!("partition '{}'", partition.label);
    let mut pending = await_source(stream.next(), &ctx.counters, idle, &scope).await?;
    while let Some(chunk) = pending {
        let chunk = chunk?;
        let decoding = feed_off_reactor(decoder, chunk);
        let (next, joined) = tokio::join!(
            await_source(stream.next(), &ctx.counters, idle, &scope),
            decoding
        );
        pending = next?;
        let (returned, decoded) = joined.map_err(decode_task_failed)?;
        decoder = returned;
        for batch in decoded? {
            let rows = batch.num_rows() as u64;
            if let Some(w) = archive_writer.as_mut() {
                w.write(&batch).await?;
            }
            ctx.push_batch(&mut sends, &mut insert_buf, schema.clone(), batch)
                .await;
            // Pace the read: pausing here applies TCP backpressure to the COPY
            // stream, slowing the server-side scan. One chunk is already in
            // hand by this point (that's the overlap above), so the throttle
            // now bites a chunk later than it used to — it still bounds the
            // sustained rate, just with that much slack.
            if let Some(t) = &ctx.throttle {
                t.acquire(rows).await;
            }
        }
        reap(&mut sends, false).await?; // surface any upload error promptly
    }
    if !decoder.saw_trailer() {
        return Err(EtlError::decode(format!(
            "COPY stream for partition {} ended without a trailer",
            partition.label
        )));
    }
    if let Some(batch) = decoder.finish()? {
        if let Some(w) = archive_writer.as_mut() {
            w.write(&batch).await?;
        }
        ctx.push_batch(&mut sends, &mut insert_buf, schema.clone(), batch)
            .await;
    }
    if let Some(w) = archive_writer.take() {
        w.close().await?;
    }
    ctx.flush(&mut sends, &mut insert_buf, schema.clone()).await;
    reap(&mut sends, true).await?; // wait for all uploads before returning

    ctx.counters
        .rows_read
        .fetch_add(decoder.rows_total, Ordering::Relaxed);
    emit_progress(&ctx.counters, &ctx.progress, ctx.started);
    tracing::info!(
        "partition '{}' complete: {} rows",
        partition.label,
        decoder.rows_total
    );
    report_coercions(
        &format!("partition '{}'", partition.label),
        decoder.coercions(),
        &ctx.warnings,
    );
    Ok(())
}

/// Keyset-chunked resumable read for a MySQL source — the MySQL analogue of
/// [`transfer_keyset_postgres`]: bounded `SELECT ... WHERE key > cursor
/// ORDER BY key LIMIT N` chunks via the binary protocol, each made durable
/// before its cursor is persisted.
#[allow(clippy::too_many_arguments)]
async fn transfer_keyset_mysql(
    source: &MySqlSource,
    plan: &SelectPlan,
    cfg: &TransferConfig,
    ctx: &SendCtx,
    base_table: Option<&str>,
    base_query: Option<&str>,
    extra_filter: Option<&str>,
    chunk: &ChunkPlan,
) -> Result<()> {
    let mut conn = source.connect().await?;
    let col_quoted = quote_my(&chunk.keyset_col);
    let mut cursor = chunk.start_cursor.clone();
    let partition = Partition {
        label: "keyset".into(),
        predicate: None,
    };
    let schema =
        MySqlBatcher::with_batch_bytes(&plan.dest_columns, cfg.batch_rows, cfg.batch_bytes)?
            .schema();
    let mut archive_writer = match &ctx.archive {
        Some(info) => Some(info.writer_for("keyset", schema.clone())?),
        None => None,
    };
    tracing::info!(
        "keyset chunked read starting on '{}' (chunk_rows={}, resume_cursor={:?})",
        chunk.keyset_col,
        chunk.limit,
        cursor
    );

    loop {
        let keyset = Keyset {
            col_quoted: col_quoted.clone(),
            cursor: cursor.clone(),
            limit: chunk.limit,
        };
        let select_sql = source.select_sql(
            &plan.source_columns,
            &plan.source_select_exprs,
            base_table,
            base_query,
            &partition,
            extra_filter,
            Some(keyset),
        );
        tracing::debug!("keyset chunk: {select_sql}");
        let mut batcher =
            MySqlBatcher::with_batch_bytes(&plan.dest_columns, cfg.batch_rows, cfg.batch_bytes)?;
        let mut sends: JoinSet<Result<()>> = JoinSet::new();
        let mut insert_buf = InsertBuffer::new(cfg.insert_bytes);
        let mut cursor_candidate: Option<i128> = None;

        let stmt = conn
            .prep(select_sql)
            .await
            .map_err(|e| EtlError::from(e).context("preparing mysql keyset statement"))?;
        let mut result = conn
            .exec_iter(stmt, ())
            .await
            .map_err(|e| EtlError::from(e).context("executing mysql keyset query"))?;
        let stream = result
            .stream::<mysql_async::Row>()
            .await
            .map_err(|e| EtlError::from(e).context("streaming mysql keyset result"))?
            .ok_or_else(|| EtlError::other("mysql keyset query returned no result set"))?;
        futures::pin_mut!(stream);

        let idle = cfg.read_idle_timeout_secs;
        let scope = format!("partition '{}'", partition.label);
        while let Some(row) = await_source(stream.next(), &ctx.counters, idle, &scope).await? {
            let row = row.map_err(|e| EtlError::from(e).context("reading mysql row"))?;
            if let Some(batch) = batcher.append_row(row)? {
                let rows = batch.num_rows() as u64;
                if let Some(k) = last_int_key(&batch, chunk.keyset_idx)? {
                    cursor_candidate = Some(cursor_candidate.map_or(k, |c| c.max(k)));
                }
                if let Some(w) = archive_writer.as_mut() {
                    w.write(&batch).await?;
                }
                ctx.push_batch(&mut sends, &mut insert_buf, schema.clone(), batch)
                    .await;
                reap(&mut sends, false).await?;
                if let Some(t) = &ctx.throttle {
                    t.acquire(rows).await;
                }
            }
        }
        if let Some(batch) = batcher.finish()? {
            if let Some(k) = last_int_key(&batch, chunk.keyset_idx)? {
                cursor_candidate = Some(cursor_candidate.map_or(k, |c| c.max(k)));
            }
            if let Some(w) = archive_writer.as_mut() {
                w.write(&batch).await?;
            }
            ctx.push_batch(&mut sends, &mut insert_buf, schema.clone(), batch)
                .await;
        }
        ctx.flush(&mut sends, &mut insert_buf, schema.clone()).await;
        reap(&mut sends, true).await?;

        let rows_this_chunk = batcher.rows_total;
        ctx.counters
            .rows_read
            .fetch_add(rows_this_chunk, Ordering::Relaxed);
        report_coercions("keyset read", batcher.coercions(), &ctx.warnings);

        if rows_this_chunk == 0 {
            break;
        }
        let next = cursor_candidate.ok_or_else(|| {
            EtlError::other(format!(
                "keyset column '{}' produced no usable value in a non-empty chunk (NULL key?); \
                 refusing to advance the cursor to avoid skipping rows",
                chunk.keyset_col
            ))
        })?;
        let cur = next.to_string();
        ctx.sink
            .persist_chunk_cursor(
                cfg,
                chunk.committed.as_deref(),
                &cur,
                chunk.effective_upper.as_deref().unwrap_or(""),
                ctx.counters.rows_written.load(Ordering::Relaxed),
            )
            .await?;
        cursor = Some(cur);
        emit_progress(&ctx.counters, &ctx.progress, ctx.started);

        if (rows_this_chunk as usize) < chunk.limit {
            break;
        }
    }
    if let Some(w) = archive_writer.take() {
        w.close().await?;
    }
    tracing::info!(
        "keyset chunked read complete: {} rows read",
        ctx.counters.rows_read.load(Ordering::Relaxed)
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn transfer_partition_mysql(
    source: &MySqlSource,
    plan: &SelectPlan,
    cfg: &TransferConfig,
    ctx: &SendCtx,
    base_table: Option<&str>,
    base_query: Option<&str>,
    extra_filter: Option<&str>,
    partition: Partition,
    chunk: Option<&ChunkPlan>,
) -> Result<()> {
    if let Some(cp) = chunk {
        return transfer_keyset_mysql(
            source,
            plan,
            cfg,
            ctx,
            base_table,
            base_query,
            extra_filter,
            cp,
        )
        .await;
    }
    tracing::info!("partition '{}' starting", partition.label);
    let mut conn = source.connect().await?;
    let select_sql = source.select_sql(
        &plan.source_columns,
        &plan.source_select_exprs,
        base_table,
        base_query,
        &partition,
        extra_filter,
        None,
    );
    tracing::debug!("partition {}: {select_sql}", partition.label);

    let mut batcher =
        MySqlBatcher::with_batch_bytes(&plan.dest_columns, cfg.batch_rows, cfg.batch_bytes)?;
    let schema = batcher.schema();
    let mut sends: JoinSet<Result<()>> = JoinSet::new();
    let mut insert_buf = InsertBuffer::new(cfg.insert_bytes);
    let mut archive_writer = match &ctx.archive {
        Some(info) => Some(info.writer_for(&partition.label, schema.clone())?),
        None => None,
    };

    // Use the binary protocol (prepared statement) for actual row fetching,
    // not just for resolve_columns's schema probe: plain query_iter uses the
    // text protocol, which returns every value as Bytes (ASCII text) even
    // for integer/float columns, regardless of the column's real type.
    let stmt = conn
        .prep(select_sql)
        .await
        .map_err(|e| EtlError::from(e).context("preparing mysql statement"))?;
    let mut result = conn
        .exec_iter(stmt, ())
        .await
        .map_err(|e| EtlError::from(e).context("executing mysql query"))?;
    let stream = result
        .stream::<mysql_async::Row>()
        .await
        .map_err(|e| EtlError::from(e).context("streaming mysql result"))?
        .ok_or_else(|| EtlError::other("mysql query returned no result set"))?;
    futures::pin_mut!(stream);

    let idle = cfg.read_idle_timeout_secs;
    let scope = format!("partition '{}'", partition.label);
    while let Some(row) = await_source(stream.next(), &ctx.counters, idle, &scope).await? {
        let row = row.map_err(|e| EtlError::from(e).context("reading mysql row"))?;
        if let Some(batch) = batcher.append_row(row)? {
            let rows = batch.num_rows() as u64;
            if let Some(w) = archive_writer.as_mut() {
                w.write(&batch).await?;
            }
            ctx.push_batch(&mut sends, &mut insert_buf, schema.clone(), batch)
                .await;
            reap(&mut sends, false).await?; // surface any upload error promptly
                                            // Pace the read: pausing before fetching more rows applies
                                            // backpressure to the streaming result set, slowing the scan.
            if let Some(t) = &ctx.throttle {
                t.acquire(rows).await;
            }
        }
    }
    if let Some(batch) = batcher.finish()? {
        if let Some(w) = archive_writer.as_mut() {
            w.write(&batch).await?;
        }
        ctx.push_batch(&mut sends, &mut insert_buf, schema.clone(), batch)
            .await;
    }
    if let Some(w) = archive_writer.take() {
        w.close().await?;
    }
    ctx.flush(&mut sends, &mut insert_buf, schema.clone()).await;
    reap(&mut sends, true).await?; // wait for all uploads before returning

    ctx.counters
        .rows_read
        .fetch_add(batcher.rows_total, Ordering::Relaxed);
    emit_progress(&ctx.counters, &ctx.progress, ctx.started);
    tracing::info!(
        "partition '{}' complete: {} rows",
        partition.label,
        batcher.rows_total
    );
    report_coercions(
        &format!("partition '{}'", partition.label),
        batcher.coercions(),
        &ctx.warnings,
    );
    Ok(())
}

/// The sentence describing one coercion kind, naming the column it happened
/// in. Human-facing only — a caller matching behaviour matches on
/// [`WarningKind`], which is stable; this text is not.
fn coercion_message(kind: WarningKind, column: &str, n: u64) -> String {
    match kind {
        WarningKind::CoercedDate => format!(
            "column '{column}': {n} unrepresentable or out-of-range date/datetime value(s) \
             (zero-dates like '0000-00-00', or years outside ClickHouse's 1900-2299 window \
             like '9999-12-31') coerced to NULL"
        ),
        WarningKind::CoercedDecimal => format!(
            "column '{column}': {n} decimal value(s) coerced to NULL (value exceeded the \
             declared Decimal(P,S) precision, or was NaN/Infinity)"
        ),
        WarningKind::CoercedScalar => format!(
            "column '{column}': {n} value(s) coerced to NULL (a non-empty scalar that did not \
             parse as the declared type)"
        ),
        WarningKind::CollapsedBool => format!(
            "column '{column}': {n} tinyint(1) value(s) outside {{0, 1}} were flattened to a \
             boolean 0/1, losing their original value. MySQL's BOOL is an alias for \
             tinyint(1), so this column was mapped to Bool from its display width — if it \
             actually holds small integers, pass tinyint1_as_bool=False to read it as an \
             integer instead (note: rows already written keep the flattened value; re-sync \
             to repair them)"
        ),
        // Table-level kinds are raised directly by their own call sites, which
        // know the numbers that make them concrete.
        WarningKind::NullWatermark
        | WarningKind::FullRefreshShrink
        | WarningKind::UnclusteredMergeTarget => {
            format!("column '{column}': {n} affected row(s)")
        }
    }
}

/// Log and record every per-column coercion a decoder reported for one
/// partition/read.
///
/// Two audiences, one pass. The log keeps its old shape — one aggregated line
/// per kind, so a badly-legacy table cannot flood it — but now names the
/// columns responsible. The [`Warnings`] collector gets one structured entry
/// per `(kind, column)`, which is what a scheduler can actually branch on:
/// "9 columns across 8 tables were genuine small integers" is a statement you
/// can only make from per-column data.
fn report_coercions(scope: &str, entries: Vec<(WarningKind, String, u64)>, warnings: &Warnings) {
    if entries.is_empty() {
        return;
    }
    let mut by_kind: std::collections::BTreeMap<WarningKind, Vec<(String, u64)>> =
        std::collections::BTreeMap::new();
    for (kind, column, count) in entries {
        warnings.push(TransferWarning {
            kind,
            column: Some(column.clone()),
            count,
            sample: None,
            message: coercion_message(kind, &column, count),
        });
        by_kind.entry(kind).or_default().push((column, count));
    }
    for (kind, cols) in by_kind {
        let total: u64 = cols.iter().map(|(_, n)| n).sum();
        let list = cols
            .iter()
            .map(|(c, n)| format!("{c} ({n})"))
            .collect::<Vec<_>>()
            .join(", ");
        let what = match kind {
            WarningKind::CoercedDate => {
                "unrepresentable or out-of-range date/datetime value(s) \
                                         coerced to NULL"
            }
            WarningKind::CoercedDecimal => {
                "decimal value(s) coerced to NULL (exceeded the declared Decimal(P,S), or \
                 NaN/Infinity)"
            }
            WarningKind::CoercedScalar => {
                "value(s) coerced to NULL (a non-empty scalar that did not parse)"
            }
            WarningKind::CollapsedBool => {
                "tinyint(1) value(s) outside {0, 1} flattened to a boolean, losing their \
                 original value (pass tinyint1_as_bool=False to keep them)"
            }
            _ => "affected value(s)",
        };
        tracing::warn!("{scope}: {total} {what} — by column: {list}");
    }
}

/// Fail early (before any query touches it) if the incremental `watermark`
/// column isn't among the resolved source columns. Without this, the watermark
/// only surfaces deep in the setup as a cryptic driver error — e.g. MySQL 1054
/// `Unknown column '<w>' in 'field list'` from the `MAX(<w>)` probe — that
/// doesn't say the problem is the `watermark` argument. Common trigger: one
/// `watermark=...` reused across a batch of tables where one table lacks it.
fn ensure_watermark_column(watermark: &str, source_cols: &[ColumnType]) -> Result<()> {
    if source_cols.iter().any(|c| c.name == watermark) {
        return Ok(());
    }
    let available = source_cols
        .iter()
        .map(|c| c.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    Err(EtlError::config(format!(
        "watermark column '{watermark}' not found in source; available columns: {available}"
    )))
}

/// Whether the resolved `watermark` column allows NULL — see
/// `warn_on_null_watermark` for why this is checked at all.
fn watermark_column_nullable(watermark: &str, source_cols: &[ColumnType]) -> bool {
    source_cols
        .iter()
        .find(|c| c.name == watermark)
        .expect("ensure_watermark_column already validated the watermark column exists")
        .nullable
}

/// Bug report B3: a `WHERE watermark > x` predicate never matches a NULL
/// value, so when the watermark column is nullable, rows with a NULL there
/// are silently excluded from *every* incremental run, forever —
/// `rows_read` reports the filtered count and the transfer still reports
/// success. Surface it loudly instead of leaving the table quietly
/// incomplete (the report's reproduction: 322,318 of 383,500 rows in one
/// window, 0 errors, 0 warnings, until an independent completeness check
/// caught it).
fn warn_on_null_watermark(watermark: &str, null_count: i64, warnings: &Warnings) {
    if null_count <= 0 {
        return;
    }
    let message = format!(
        "watermark column '{watermark}' is nullable and {null_count} row(s) currently have a \
         NULL value there; a `WHERE {watermark} > x` predicate never matches NULL, so these \
         rows are silently excluded from this and every future incremental run on this \
         pipeline (this run will still report success). If they need to be synced, \
         backfill them separately — e.g. a source_query scoped to `{watermark} IS NULL`, or \
         a one-off mode=\"full\" load."
    );
    tracing::warn!("{message}");
    warnings.push(TransferWarning {
        kind: WarningKind::NullWatermark,
        column: Some(watermark.to_string()),
        count: null_count as u64,
        sample: None,
        message,
    });
}

/// Fail early (before any query touches it) if `lookback_seconds` is set but
/// the watermark column isn't a date/timestamp type. The lookback lower
/// bound is expressed as SQL date arithmetic (`... - INTERVAL n SECOND`),
/// which only makes sense for a temporal column; `TransferConfig::validate()`
/// can't catch this itself since it only sees config, not resolved source
/// columns. Reuses [`crate::types::may_coerce_to_null`]'s Date32/Timestamp
/// check as the "is this temporal" predicate — same two types this crate
/// already treats as its one temporal family.
fn ensure_lookback_compatible(
    watermark: &str,
    lookback_seconds: u64,
    source_cols: &[ColumnType],
) -> Result<()> {
    if lookback_seconds == 0 {
        return Ok(());
    }
    let col = source_cols
        .iter()
        .find(|c| c.name == watermark)
        .expect("ensure_watermark_column already validated the watermark column exists");
    if crate::types::may_coerce_to_null(&col.arrow) {
        Ok(())
    } else {
        Err(EtlError::config(format!(
            "lookback_seconds requires the watermark column '{watermark}' to be a date or \
             timestamp type (resolved as {:?})",
            col.arrow
        )))
    }
}

fn emit_progress(counters: &Counters, progress: &Option<ProgressCb>, started: Instant) {
    if let Some(cb) = progress {
        let elapsed = started.elapsed().as_secs_f64();
        let rows_written = counters.rows_written.load(Ordering::Relaxed);
        let p = Progress {
            rows_read: counters.rows_read.load(Ordering::Relaxed),
            rows_written,
            bytes_written: counters.bytes_written.load(Ordering::Relaxed),
            elapsed_secs: elapsed,
            rows_per_sec: if elapsed > 0.0 {
                rows_written as f64 / elapsed
            } else {
                0.0
            },
        };
        cb(p);
    }
}

/// Create/verify the destination table and return the table to write rows into.
/// For full refresh this is a fresh staging table; for incremental it's the
/// destination table itself. Destination-agnostic: `sink.create_table` builds
/// whichever native DDL/schema the concrete destination needs.
async fn prepare_target(
    sink: &Arc<dyn Sink>,
    cfg: &TransferConfig,
    dest_columns: &[ColumnType],
    staging: &str,
    // Stage an *incremental* run that wouldn't otherwise (ClickHouse) so a
    // data-quality gate has a staging table to validate before rows reach the
    // destination. No effect on full-refresh (always stages) or on a
    // destination that already stages for its MERGE (BigQuery).
    force_stage_incremental: bool,
) -> Result<String> {
    match cfg.mode {
        SyncMode::Full => {
            // Destination must exist for the atomic swap; create it empty if allowed.
            let dest_existed = sink.table_exists(&cfg.dest_table).await?;
            if !dest_existed {
                if cfg.create_if_missing {
                    tracing::info!(
                        "destination table '{}' does not exist; creating it",
                        cfg.dest_table
                    );
                    sink.create_table(&cfg.dest_table, dest_columns, cfg)
                        .await?;
                } else {
                    return Err(EtlError::config(format!(
                        "destination table {} does not exist and create_if_missing=false",
                        cfg.dest_table
                    )));
                }
            } else {
                tracing::debug!("destination table '{}' already exists", cfg.dest_table);
                // Full-refresh only needs to evolve a dest whose swap names its
                // columns (BigQuery INSERT...SELECT). ClickHouse's swap recreates
                // the table from staging, so drift is absorbed there instead (below).
                if cfg.evolve_schema && sink.full_refresh_references_dest_columns() {
                    let added = sink
                        .add_missing_columns(&cfg.dest_table, dest_columns, cfg)
                        .await?;
                    if !added.is_empty() {
                        tracing::info!(
                            "evolve_schema: added {} column(s) to '{}': {}",
                            added.len(),
                            cfg.dest_table,
                            added.join(", ")
                        );
                    }
                }
            }
            // Fresh, per-run-unique staging table. The unique name (see
            // `staging_name`) is why we can create it without first dropping a
            // prior one: a name that's never been used can't collide with a
            // crashed run's orphan, and — for BigQuery — never enters the
            // "recently deleted/recreated" state that blocks streaming inserts.
            tracing::info!("creating staging table '{staging}'");
            if dest_existed {
                // Bug report B4: a swap (ClickHouse's EXCHANGE TABLES) makes
                // staging's structure the new destination, so staging must
                // mirror the destination's *actual* engine/ORDER BY/PARTITION
                // BY/nullability — not a fresh CREATE TABLE recomputed from
                // `cfg`, which silently replaces them the moment `cfg` omits
                // (or disagrees with) whatever the table already has.
                sink.clone_table_structure(staging, &cfg.dest_table).await?;
                // The clone above copies dest's columns as they stood *before*
                // this function's own evolve_schema step ran (for BigQuery
                // that step already updated dest, so this is a no-op there;
                // for ClickHouse, which skips that step above, this is what
                // actually adds a new source column to staging).
                if cfg.evolve_schema {
                    let added = sink.add_missing_columns(staging, dest_columns, cfg).await?;
                    if !added.is_empty() {
                        tracing::info!(
                            "evolve_schema: added {} column(s) to staging '{}': {}",
                            added.len(),
                            staging,
                            added.join(", ")
                        );
                    }
                }
            } else {
                // First run for this destination: no existing DDL to preserve,
                // so build both dest (above) and staging fresh from `cfg`.
                sink.create_table(staging, dest_columns, cfg).await?;
            }
            Ok(staging.to_string())
        }
        SyncMode::Incremental => {
            // Checked first, before any network I/O: MERGE has nothing to
            // match rows on without a key — this destination has no
            // engine-level dedup the way ClickHouse's ReplacingMergeTree
            // does, so a key is mandatory here even though it's optional
            // everywhere else. A pure config check, so it fails fast and
            // cheaply rather than after establishing a connection.
            if sink.requires_staging_for_incremental() && cfg.key.is_empty() {
                return Err(EtlError::config(
                    "key is required for incremental mode with this destination (used as the \
                     MERGE match key; this destination has no engine-level dedup, unlike \
                     ClickHouse's ReplacingMergeTree)",
                ));
            }

            let dest_existed = sink.table_exists(&cfg.dest_table).await?;
            if !dest_existed {
                if cfg.create_if_missing {
                    tracing::info!(
                        "destination table '{}' does not exist; creating it",
                        cfg.dest_table
                    );
                    sink.create_table(&cfg.dest_table, dest_columns, cfg)
                        .await?;
                } else {
                    return Err(EtlError::config(format!(
                        "destination table {} does not exist and create_if_missing=false",
                        cfg.dest_table
                    )));
                }
            } else {
                tracing::debug!("destination table '{}' already exists", cfg.dest_table);
                // Incremental always evolves: ClickHouse inserts straight into
                // the dest, and BigQuery's MERGE references its columns — a new
                // source column would hard-error on either without this.
                if cfg.evolve_schema {
                    let added = sink
                        .add_missing_columns(&cfg.dest_table, dest_columns, cfg)
                        .await?;
                    if !added.is_empty() {
                        tracing::info!(
                            "evolve_schema: added {} column(s) to '{}': {}",
                            added.len(),
                            cfg.dest_table,
                            added.join(", ")
                        );
                    }
                }
            }
            sink.ensure_state_table(&cfg.state_table_name).await?;

            if sink.requires_staging_for_incremental() || force_stage_incremental {
                tracing::info!("creating staging table '{staging}' for incremental load");
                if dest_existed {
                    // Schema follows the destination here too (same fix as
                    // full-refresh, bug report B4): staging is only ever a
                    // transient MERGE source, so it must match what the
                    // destination *actually* has right now — including any
                    // `type_overrides`/`column_transform_types` from whatever
                    // run originally created it, which this run may not have
                    // repeated — not a fresh `create_table` re-derived from
                    // this run's `cfg`/source schema, which could silently
                    // diverge from the destination's real column types and
                    // break the MERGE's column-to-column assignment. Cloned
                    // *after* the evolve_schema step above, so a newly added
                    // column is already present on staging too.
                    sink.clone_table_structure(staging, &cfg.dest_table).await?;
                } else {
                    // First run for this destination: no existing schema to
                    // follow, so build both dest (above) and staging fresh
                    // from `cfg`/the resolved source schema.
                    sink.create_table(staging, dest_columns, cfg).await?;
                }
                Ok(staging.to_string())
            } else {
                Ok(cfg.dest_table.clone())
            }
        }
        SyncMode::Append => {
            // Bronze-landing: insert straight into the destination — no staging,
            // no merge, no swap, no key, no dedup. Create the dest if missing,
            // evolve it (inserts reference its columns), and ensure the state
            // table so the resume cursor can be persisted.
            if !sink.table_exists(&cfg.dest_table).await? {
                if cfg.create_if_missing {
                    tracing::info!(
                        "destination table '{}' does not exist; creating it",
                        cfg.dest_table
                    );
                    sink.create_table(&cfg.dest_table, dest_columns, cfg)
                        .await?;
                } else {
                    return Err(EtlError::config(format!(
                        "destination table {} does not exist and create_if_missing=false",
                        cfg.dest_table
                    )));
                }
            } else if cfg.evolve_schema {
                let added = sink
                    .add_missing_columns(&cfg.dest_table, dest_columns, cfg)
                    .await?;
                if !added.is_empty() {
                    tracing::info!(
                        "evolve_schema: added {} column(s) to '{}': {}",
                        added.len(),
                        cfg.dest_table,
                        added.join(", ")
                    );
                }
            }
            sink.ensure_state_table(&cfg.state_table_name).await?;
            Ok(cfg.dest_table.clone())
        }
    }
}

async fn compute_partitions_pg(
    source: &PgSource,
    client: &tokio_postgres::Client,
    cfg: &TransferConfig,
    source_cols: &[ColumnType],
) -> Result<Vec<Partition>> {
    let single = vec![Partition {
        label: "all".into(),
        predicate: None,
    }];

    // Chunked resumable reads drive a single keyset stream (the chunk loop is
    // the parallelism) — never range-fan-out.
    if cfg.chunk_rows.is_some() {
        return Ok(single);
    }
    if cfg.parallelism <= 1 {
        return Ok(single);
    }

    let (from_table, base_query) = match partition_target(cfg) {
        Some(t) => t,
        None => return Ok(single),
    };

    let part_col = cfg
        .partition_column
        .clone()
        .or_else(|| cfg.key.first().cloned());
    let part_col = match part_col {
        Some(c) => c,
        None => return Ok(single),
    };
    let source_expr = cfg.partition_source_expr.as_deref();
    // Without an override the partition column must be a resolvable source
    // column; with one, `range_partitions` probes the expression instead (and
    // `partition_key_type` has already rejected a non-integer named column).
    let (type_id, nullable) = match partition_key_type(
        source_cols,
        &part_col,
        source_expr,
        crate::source::postgres::is_range_partitionable,
    )? {
        Some(t) => t,
        None => return Ok(single),
    };

    source
        .range_partitions(
            client,
            from_table,
            base_query,
            &part_col,
            source_expr,
            type_id,
            cfg.parallelism,
            nullable,
        )
        .await
}

/// Which relation the range-partition probe reads: `(Some(table), None)` for a
/// base table, `(None, Some(query))` for a `source_query` that opted in via
/// `partition_source_expr`. `None` means this transfer can't be range
/// partitioned at all and should run single-stream.
///
/// A `source_query` without `partition_source_expr` is the case that used to be
/// silently unpartitionable for every custom query — it still runs single-stream
/// (that's the compatible default), but now says so and names the way out.
fn partition_target(cfg: &TransferConfig) -> Option<(Option<&str>, Option<&str>)> {
    match (cfg.source_table.as_deref(), cfg.source_query.as_deref()) {
        (Some(t), None) => Some((Some(t), None)),
        (_, Some(q)) => match cfg.partition_source_expr.as_deref() {
            Some(_) => Some((None, Some(q))),
            None => {
                tracing::info!(
                    "parallelism={} is inert for this transfer: range partitioning needs a key \
                     column it can probe MIN/MAX on and bound with an indexable predicate, and \
                     source_query hides that column behind its own projection, so the read runs \
                     single-stream. Have source_query additionally project the raw, indexed key \
                     column (e.g. `id AS id_raw`) and set partition_source_expr=\"id_raw\" to fan \
                     out.",
                    cfg.parallelism
                );
                None
            }
        },
        (None, None) => None,
    }
}

/// Resolve the partition key's `(type_id, nullable)` for the range probe.
/// `is_int` is the reading engine's own range-partitionable type gate.
///
/// With no `partition_source_expr`, this is just the named source column (and
/// an unresolvable name means "not partitionable", as before). With one, the
/// expression is authoritative: if it happens to name a projected column we
/// type-gate on that column and reject a non-integer outright — an explicitly
/// requested fan-out that silently collapses to one stream is the exact bug
/// `partition_source_expr` exists to fix. If it names no column we can type
/// (a real expression), the source's own `MIN`/`MAX` probe is the gate, and the
/// key is assumed nullable so NULL-keyed rows still get their own partition
/// rather than being dropped.
fn partition_key_type(
    source_cols: &[ColumnType],
    part_col: &str,
    source_expr: Option<&str>,
    is_int: impl Fn(u32) -> bool,
) -> Result<Option<(u32, bool)>> {
    let expr = match source_expr {
        None => {
            return Ok(source_cols
                .iter()
                .find(|c| c.name == part_col)
                .map(|c| (c.type_id, c.nullable)))
        }
        Some(e) => e,
    };
    match source_cols.iter().find(|c| c.name == expr) {
        Some(c) if !is_int(c.type_id) => Err(EtlError::config(format!(
            "partition_source_expr='{expr}' resolves to a non-integer column, which cannot be \
             split into numeric ranges. Point it at the raw integer key column source_query \
             projects, or unset it to read single-stream."
        ))),
        Some(c) => Ok(Some((c.type_id, c.nullable))),
        None => Ok(Some((0, true))),
    }
}

async fn compute_partitions_mysql(
    source: &MySqlSource,
    conn: &mut mysql_async::Conn,
    cfg: &TransferConfig,
    source_cols: &[ColumnType],
) -> Result<Vec<Partition>> {
    let single = vec![Partition {
        label: "all".into(),
        predicate: None,
    }];

    if cfg.chunk_rows.is_some() {
        return Ok(single);
    }
    if cfg.parallelism <= 1 {
        return Ok(single);
    }

    let (from_table, base_query) = match partition_target(cfg) {
        Some(t) => t,
        None => return Ok(single),
    };

    let part_col = cfg
        .partition_column
        .clone()
        .or_else(|| cfg.key.first().cloned());
    let part_col = match part_col {
        Some(c) => c,
        None => return Ok(single),
    };
    let source_expr = cfg.partition_source_expr.as_deref();
    let (type_id, nullable) = match partition_key_type(
        source_cols,
        &part_col,
        source_expr,
        crate::source::mysql::is_range_partitionable,
    )? {
        Some(t) => t,
        None => return Ok(single),
    };

    source
        .range_partitions(
            conn,
            from_table,
            base_query,
            &part_col,
            source_expr,
            type_id,
            cfg.parallelism,
            nullable,
        )
        .await
}

/// Hand one `COPY` chunk to `CopyDecoder::feed` on Tokio's blocking pool,
/// returning the decoder along with the result.
///
/// `feed` is a two-pass per-tuple parse into Arrow builders — the CPU-heaviest
/// step in the pipeline — and it used to run inline on the async worker that
/// owns the socket. That had two costs: the worker couldn't poll anything else
/// (other partitions' sockets, in-flight upload futures) for the duration of
/// every parse, and the `COPY` stream sat idle between chunks instead of
/// draining while the previous chunk was decoded. Moving the parse to the
/// blocking pool fixes the first; polling the next chunk concurrently with this
/// future (see the call sites) fixes the second.
///
/// The decoder is passed by value and handed back rather than borrowed: it
/// carries builder state across chunks, so it can't be shared with a
/// `'static` task any other way.
fn feed_off_reactor(
    mut decoder: CopyDecoder,
    chunk: Bytes,
) -> tokio::task::JoinHandle<(CopyDecoder, Result<Vec<RecordBatch>>)> {
    tokio::task::spawn_blocking(move || {
        let decoded = decoder.feed(&chunk);
        (decoder, decoded)
    })
}

/// A panic (or cancellation) inside the off-reactor decode task. Not reachable
/// through normal decode failures — those come back as the inner `Result`.
fn decode_task_failed(e: tokio::task::JoinError) -> EtlError {
    EtlError::other(format!("COPY decode task failed: {e}"))
}

/// Per-run-unique staging table name: `{dest}_quickhouse_tmp_{run_id}`.
///
/// The `run_id` makes the name **never reused across runs** — this is a
/// correctness requirement, not cosmetic. BigQuery blocks streaming inserts
/// into a table that was dropped and recreated under the same name within a
/// (minutes-long, eventually-consistent) metadata window; a fixed staging
/// name made every rapid re-run / whole-call retry recreate-then-stream and
/// hit that block. A never-before-used name can't be in the "recently
/// recreated" state, so the window never applies. It also means a crashed
/// run's orphaned staging table can't poison the next run (which uses a
/// different name) — at the cost that orphans are no longer auto-reclaimed by
/// the next run, so callers drop staging on the error path too.
fn staging_name(dest: &str, suffix: &str, run_id: &str) -> String {
    format!("{dest}{suffix}_{run_id}")
}

/// A run id unique enough that a staging table name built from it is never
/// reused across runs (including seconds-apart retries) — nanosecond wall
/// clock, matching `sink::bigquery`'s `unique_job_id` idiom.
fn new_run_id() -> String {
    time::OffsetDateTime::now_utc()
        .unix_timestamp_nanos()
        .to_string()
}

/// Best-effort drop of a per-run staging table after a failed transfer. A
/// unique-per-run name isn't reclaimed by any later run, so without this a
/// failed run would leak its staging table (a full data copy, for
/// full-refresh). Deliberately swallows its own error (logs a warning) so it
/// never masks the real transfer error being propagated.
async fn cleanup_staging(sink: &Arc<dyn Sink>, staging: &str) {
    if let Err(e) = sink.drop_table(staging).await {
        tracing::warn!("failed to drop staging table '{staging}' after a failed transfer: {e}");
    }
}

/// Resolve the first-run seed into the effective `last` watermark. Only ever
/// consulted when no cursor is persisted yet (`read_last_watermark` returned
/// `None`), so it self-retires after the first successful run. `CurrentMax`
/// seeds to the source's current MAX (reading ~nothing — for a destination
/// already backfilled by a prior pipeline).
fn seed_value(seed: &WatermarkSeed, snapshot_max: Option<&str>) -> Option<String> {
    match seed {
        WatermarkSeed::None => None,
        WatermarkSeed::Value(v) => Some(v.clone()),
        WatermarkSeed::CurrentMax => snapshot_max.map(str::to_string),
    }
}

fn build_watermark_filter_pg(
    watermark: &str,
    source_expr: Option<&str>,
    last: Option<&str>,
    snapshot_max: Option<&str>,
    lookback_seconds: u64,
) -> Option<String> {
    let col = source_expr
        .map(str::to_string)
        .unwrap_or_else(|| format!("\"{}\"", watermark.replace('"', "\"\"")));
    let lower = last.map(|l| lookback_lower_bound_pg(l, lookback_seconds));
    let upper = snapshot_max.map(quote_sql_literal);
    build_watermark_filter(&col, lower, upper)
}

fn build_watermark_filter_mysql(
    watermark: &str,
    source_expr: Option<&str>,
    last: Option<&str>,
    snapshot_max: Option<&str>,
    lookback_seconds: u64,
) -> Option<String> {
    let col = source_expr
        .map(str::to_string)
        .unwrap_or_else(|| quote_my(watermark));
    let lower = last.map(|l| lookback_lower_bound_mysql(l, lookback_seconds));
    let upper = snapshot_max.map(quote_mysql_literal);
    build_watermark_filter(&col, lower, upper)
}

/// Postgres, under the default `standard_conforming_strings = on`, treats
/// backslash as a plain literal character inside a `'...'` string — only the
/// doubled-quote convention is active, so this must NOT also escape
/// backslash (doing so would double every literal backslash in the actual
/// compared value, corrupting the match instead of protecting it).
fn quote_sql_literal(m: &str) -> String {
    format!("'{}'", m.replace('\'', "''"))
}

/// Unlike Postgres, MySQL treats backslash as an active escape character in
/// string literals by default (`NO_BACKSLASH_ESCAPES` is off unless a user
/// opts in), so — same failure mode as bigquery.rs's `escape_sql_string` — a
/// trailing backslash in a value would escape the literal's closing quote
/// instead of terminating the string unless backslash is escaped first.
fn quote_mysql_literal(m: &str) -> String {
    format!("'{}'", m.replace('\\', "\\\\").replace('\'', "''"))
}

/// BigQuery Standard SQL uses the same backtick identifier quoting as MySQL.
/// `source_cols` resolves the watermark column's BigQuery type, needed for
/// two independent reasons, both applying to *either* bound regardless of
/// `lookback_seconds`: (1) the lookback lower bound's `_SUB` function
/// (DATE/DATETIME/TIMESTAMP each need their own — BigQuery won't implicitly
/// compare across them) when `lookback_seconds > 0` (`ensure_lookback_compatible`
/// has already validated a temporal type by the time this runs); (2) the
/// CAST every literal needs otherwise — BigQuery only implicitly coerces an
/// untyped STRING literal against DATE/DATETIME/TIME/TIMESTAMP, NOT against
/// INT64/NUMERIC/FLOAT64/BOOL, so a plain (non-lookback) incremental sync
/// against a numeric watermark column needs the type resolved
/// unconditionally, not just when lookback is active. Both bounds share this
/// unconditionally: the *lower* bound is exactly the persisted cursor from a
/// prior run, in the same domain as the upper bound, so leaving it
/// bare-quoted while the upper bound is CAST-typed (as a stale fix here once
/// did) reintroduces the same `INT64 > STRING` failure one bound over, on
/// the very first run that has a persisted cursor to compare against.
fn build_watermark_filter_bigquery(
    watermark: &str,
    last: Option<&str>,
    snapshot_max: Option<&str>,
    lookback_seconds: u64,
    source_cols: &[ColumnType],
) -> Option<String> {
    let bq_type = source_cols
        .iter()
        .find(|c| c.name == watermark)
        .and_then(|c| arrow_to_bigquery_type(&c.arrow));
    let lower = last.map(|l| lookback_lower_bound_bigquery(l, lookback_seconds, bq_type.clone()));
    let upper = snapshot_max.map(|m| bigquery_typed_upper_bound(m, bq_type));
    build_watermark_filter(&quote_my(watermark), lower, upper)
}

/// CAST-wrap the upper-bound literal in the watermark column's own resolved
/// BigQuery type rather than emitting a bare quoted STRING — sidesteps every
/// per-type literal-syntax quirk (e.g. BOOL's `TRUE`/`FALSE` keywords) with
/// one mechanism that GoogleSQL supports uniformly from a STRING literal.
fn bigquery_typed_upper_bound(value: &str, bq_type: Option<TableFieldType>) -> String {
    let escaped = escape_bigquery_string(value);
    match bq_type {
        Some(t) => format!("CAST('{escaped}' AS {})", bq_cast_type_name(&t)),
        None => format!("'{escaped}'"),
    }
}

/// GoogleSQL string literals only recognize backslash-based escapes — unlike
/// ANSI SQL's doubled-quote convention (`''`), a doubled single quote is NOT
/// an escaped quote in BigQuery and is rejected as a syntax error (this is a
/// well-documented real-world gotcha when porting ANSI-style SQL generation
/// to BigQuery, e.g. https://github.com/trinodb/trino/issues/7784). Backslash
/// must be escaped first, same reasoning as `sink/bigquery.rs`'s
/// `escape_sql_string` (kept in sync with that function deliberately) — as
/// must newlines/carriage returns, which a quoted (non-triple) GoogleSQL
/// literal cannot carry raw: an unescaped one terminates the literal early and
/// the statement fails to parse. See that function's docs for why a raw
/// newline reaches this code path at all (a multi-line `source_query` becoming
/// the default state key).
fn escape_bigquery_string(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\'', "\\'")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn bq_cast_type_name(t: &TableFieldType) -> &'static str {
    match t {
        TableFieldType::Boolean | TableFieldType::Bool => "BOOL",
        TableFieldType::Integer | TableFieldType::Int64 => "INT64",
        TableFieldType::Float | TableFieldType::Float64 => "FLOAT64",
        TableFieldType::Bytes => "BYTES",
        TableFieldType::Date => "DATE",
        TableFieldType::Datetime => "DATETIME",
        TableFieldType::Timestamp => "TIMESTAMP",
        TableFieldType::Time => "TIME",
        TableFieldType::Numeric => "NUMERIC",
        TableFieldType::Bignumeric | TableFieldType::Decimal | TableFieldType::Bigdecimal => {
            "BIGNUMERIC"
        }
        TableFieldType::String
        | TableFieldType::Json
        | TableFieldType::Record
        | TableFieldType::Struct
        | TableFieldType::Interval => "STRING",
    }
}

/// Widen `last`'s lower bound by `lookback_seconds` using Postgres's own
/// cast-and-subtract syntax — a same-engine round trip (the tracked watermark
/// string is parsed by the same engine that produced it via `CAST(MAX(col)
/// AS ...)`, not guessed at in Rust). `lookback_seconds == 0` returns the
/// plain quoted literal, byte-identical to the pre-lookback filter.
fn lookback_lower_bound_pg(last: &str, lookback_seconds: u64) -> String {
    let l = last.replace('\'', "''");
    if lookback_seconds == 0 {
        return format!("'{l}'");
    }
    format!("('{l}'::timestamp - interval '{lookback_seconds} seconds')")
}

fn lookback_lower_bound_mysql(last: &str, lookback_seconds: u64) -> String {
    let l = last.replace('\\', "\\\\").replace('\'', "''");
    if lookback_seconds == 0 {
        return format!("'{l}'");
    }
    format!("(CAST('{l}' AS DATETIME) - INTERVAL {lookback_seconds} SECOND)")
}

/// `DATE_SUB` has no sub-day granularity, so a sub-day `lookback_seconds`
/// against a `DATE` watermark rounds *up* to whole days via `div_ceil` —
/// documented behavior, not silently wrong (a 1-hour lookback on a DATE
/// column re-includes the whole prior day, not nothing).
///
/// With `lookback_seconds == 0`, this is just the lower bound's CAST-typed
/// literal — the same treatment `bigquery_typed_upper_bound` gives the upper
/// bound (bug report B2: leaving *this* bound bare-quoted made every
/// non-lookback incremental sync against a numeric/boolean watermark fail
/// with `INT64 > STRING` as soon as a cursor was persisted).
fn lookback_lower_bound_bigquery(
    last: &str,
    lookback_seconds: u64,
    bq_type: Option<TableFieldType>,
) -> String {
    if lookback_seconds == 0 {
        return bigquery_typed_upper_bound(last, bq_type);
    }
    let l = escape_bigquery_string(last);
    match bq_type.expect("ensure_lookback_compatible already validated a temporal watermark type") {
        TableFieldType::Date => {
            format!(
                "DATE_SUB(DATE '{l}', INTERVAL {} DAY)",
                lookback_seconds.div_ceil(86400)
            )
        }
        TableFieldType::Datetime => {
            format!("DATETIME_SUB(DATETIME '{l}', INTERVAL {lookback_seconds} SECOND)")
        }
        TableFieldType::Timestamp => {
            format!("TIMESTAMP_SUB(TIMESTAMP '{l}', INTERVAL {lookback_seconds} SECOND)")
        }
        other => unreachable!(
            "ensure_lookback_compatible only allows Date32/Timestamp Arrow types, which map to \
             BigQuery Date/Datetime/Timestamp — got {other:?}"
        ),
    }
}

fn build_watermark_filter(
    col: &str,
    lower_bound: Option<String>,
    upper_bound: Option<String>,
) -> Option<String> {
    let mut clauses = Vec::new();
    if let Some(lb) = lower_bound {
        clauses.push(format!("{col} > {lb}"));
    }
    if let Some(ub) = upper_bound {
        clauses.push(format!("{col} <= {ub}"));
    }
    if clauses.is_empty() {
        None
    } else {
        Some(clauses.join(" AND "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::oid;
    use arrow_schema::DataType;

    /// A source column carrying just the fields the partition planner reads.
    fn pcol(name: &str, type_id: u32, nullable: bool) -> ColumnType {
        ColumnType {
            name: name.into(),
            type_id,
            nullable,
            arrow: DataType::Int64,
            clickhouse_inner: "Int64".into(),
            arbitrary_precision_decimal: false,
        }
    }

    /// A batch of `bytes` nominal size, for exercising the coalescer's arithmetic
    /// (the buffer is told the size, so the contents don't matter).
    fn sized_batch() -> RecordBatch {
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int64Array::from(vec![1i64]))],
        )
        .unwrap()
    }

    fn free_reservation() -> Reservation {
        MemoryBudget::new(0)
            .try_reserve(1)
            .expect("an unbounded budget always reserves")
    }

    #[test]
    fn insert_buffer_holds_batches_until_the_target_is_reached() {
        let mut buf = InsertBuffer::new(1000);
        assert!(buf.is_empty());
        assert!(!buf.push(sized_batch(), free_reservation(), 400));
        assert!(!buf.push(sized_batch(), free_reservation(), 400));
        // Crossing the target is the flush signal.
        assert!(buf.push(sized_batch(), free_reservation(), 400));
        let (batches, reservations) = buf.take();
        assert_eq!(batches.len(), 3, "all three batches go out as one insert");
        assert_eq!(reservations.len(), 3, "their reservations travel with them");
        assert!(buf.is_empty(), "taking leaves the buffer reusable");
    }

    #[test]
    fn insert_buffer_with_a_zero_target_sends_every_batch_immediately() {
        // insert_bytes=0 is the documented opt-out: one insert per decoded
        // batch, exactly the pre-0.14 behavior.
        let mut buf = InsertBuffer::new(0);
        assert!(buf.push(sized_batch(), free_reservation(), 1));
        assert_eq!(buf.take().0.len(), 1);
    }

    #[test]
    fn insert_buffer_take_on_empty_yields_nothing() {
        // The tail flush before every durability barrier hits this whenever the
        // last batch already triggered a send.
        let mut buf = InsertBuffer::new(1000);
        let (batches, reservations) = buf.take();
        assert!(batches.is_empty() && reservations.is_empty());
    }

    #[test]
    fn source_query_without_partition_expr_still_runs_single_stream() {
        // The compatible default: parallelism stays inert for a custom query
        // until the caller names the raw key column.
        let mut cfg = crate::config::default_test_config();
        cfg.source_table = None;
        cfg.source_query = Some("SELECT id, CAST(amt AS numeric) AS amt FROM orders".into());
        assert!(partition_target(&cfg).is_none());
    }

    #[test]
    fn partition_source_expr_makes_a_source_query_partitionable() {
        // The whole point of the knob: the planner now targets the wrapped
        // query instead of refusing, so `parallelism` fans out.
        let mut cfg = crate::config::default_test_config();
        cfg.source_table = None;
        cfg.source_query = Some("SELECT id AS id_raw, CAST(amt AS numeric) AS amt FROM o".into());
        cfg.partition_source_expr = Some("id_raw".into());
        let (from_table, base_query) = partition_target(&cfg).expect("partitionable");
        assert!(from_table.is_none());
        assert_eq!(
            base_query,
            Some("SELECT id AS id_raw, CAST(amt AS numeric) AS amt FROM o")
        );
    }

    #[test]
    fn base_table_partition_target_is_unchanged_by_the_new_knob() {
        let cfg = crate::config::default_test_config();
        let (from_table, base_query) = partition_target(&cfg).expect("partitionable");
        assert_eq!(from_table, Some("t"));
        assert!(base_query.is_none());
    }

    #[test]
    fn partition_key_type_reads_the_named_column_when_no_expr_is_set() {
        let cols = vec![pcol("id", oid::INT8, false), pcol("name", oid::TEXT, true)];
        let got = partition_key_type(&cols, "id", None, is_pg_int).unwrap();
        assert_eq!(got, Some((oid::INT8, false)));
        // An unresolvable column means "not partitionable" — a fallback, not an
        // error, because this path is implicit (derived from `key`).
        assert_eq!(
            partition_key_type(&cols, "missing", None, is_pg_int).unwrap(),
            None
        );
    }

    #[test]
    fn partition_source_expr_pointing_at_a_non_integer_column_is_a_hard_error() {
        // Explicit fan-out that silently collapses to one stream is the bug
        // this knob fixes, so a non-range-able key fails loudly instead.
        let cols = vec![
            pcol("id", oid::INT8, false),
            pcol("wm", oid::TIMESTAMPTZ, true),
        ];
        let err = partition_key_type(&cols, "id", Some("wm"), is_pg_int).unwrap_err();
        assert!(
            err.to_string().contains("partition_source_expr"),
            "error should name the knob: {err}"
        );
    }

    #[test]
    fn partition_source_expr_naming_a_projected_int_column_uses_its_type() {
        let cols = vec![pcol("id_raw", oid::INT4, false)];
        let got = partition_key_type(&cols, "id", Some("id_raw"), is_pg_int).unwrap();
        assert_eq!(got, Some((oid::INT4, false)));
    }

    #[test]
    fn a_real_expression_defers_to_the_probe_and_assumes_nullable() {
        // Not a projected column, so there's no type to gate on — the source's
        // own MIN/MAX probe decides. Nullable is assumed so NULL-keyed rows
        // still get their own partition instead of being silently dropped.
        let cols = vec![pcol("id", oid::INT8, false)];
        let got = partition_key_type(&cols, "id", Some("COALESCE(a, b)"), is_pg_int).unwrap();
        assert_eq!(got, Some((0, true)));
    }

    fn is_pg_int(t: u32) -> bool {
        crate::source::postgres::is_range_partitionable(t)
    }

    #[test]
    fn staging_name_includes_dest_suffix_and_run_id() {
        let name = staging_name("orders", "_quickhouse_tmp", "12345");
        assert_eq!(name, "orders_quickhouse_tmp_12345");
        assert!(name.starts_with("orders"));
        // A custom suffix flows through (C1 configurable internals).
        assert_eq!(staging_name("orders", "_stg", "12345"), "orders_stg_12345");
    }

    #[test]
    fn staging_name_is_distinct_per_run_id() {
        // The core property that defeats bug 10: the same destination table
        // never yields the same staging name across runs, so BigQuery can't
        // see a drop+recreate of a recently-used name. (run_id itself comes
        // from a nanosecond wall clock — not asserted here to avoid a
        // clock-resolution-dependent flaky test; the naming logic is what
        // matters and is deterministic given distinct run_ids.)
        assert_ne!(
            staging_name("orders", "_quickhouse_tmp", "1"),
            staging_name("orders", "_quickhouse_tmp", "2")
        );
    }

    #[test]
    fn seed_value_resolves_first_run_floor() {
        // None seed -> no floor (whole-table first pull).
        assert_eq!(seed_value(&WatermarkSeed::None, Some("2026-01-01")), None);
        // Explicit floor is used verbatim.
        assert_eq!(
            seed_value(
                &WatermarkSeed::Value("2026-01-01".into()),
                Some("2026-06-01")
            ),
            Some("2026-01-01".to_string())
        );
        // CurrentMax seeds to the source's snapshot max (skip the first pull).
        assert_eq!(
            seed_value(&WatermarkSeed::CurrentMax, Some("2026-06-01")),
            Some("2026-06-01".to_string())
        );
        // CurrentMax on an empty source (no max) is a safe no-op.
        assert_eq!(seed_value(&WatermarkSeed::CurrentMax, None), None);
    }

    #[test]
    fn throttle_reserve_zero_is_immediate() {
        let t = ReadThrottle::new(1000);
        assert!(t.reserve(0).is_zero());
    }

    /// `validate=` combined with `chunk_rows` on a ClickHouse incremental sync
    /// is rejected up front: chunked keyset reads commit each chunk straight
    /// into the destination, so there's no single staging table to gate. The
    /// check fires before any network I/O (right after `build_sink`, before the
    /// source connection), so this test needs no live source or destination.
    /// (ClickHouse incremental *without* chunk_rows is now supported via forced
    /// staging — covered by the live Python suite.)
    fn cols(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn clustering_binds_when_the_key_leads_the_clustering() {
        // Exactly clustered by the key.
        assert!(clustering_binds(Some(&cols(&["id"])), &cols(&["id"])));
        // A trailing clustering column costs nothing — BigQuery still prunes
        // blocks on the leading field.
        assert!(clustering_binds(
            Some(&cols(&["id", "created_at"])),
            &cols(&["id"])
        ));
        // Composite key matching the leading clustering fields, in order.
        assert!(clustering_binds(
            Some(&cols(&["a", "b", "c"])),
            &cols(&["a", "b"])
        ));
        // Case is not a real mismatch.
        assert!(clustering_binds(Some(&cols(&["ID"])), &cols(&["id"])));
    }

    #[test]
    fn clustering_does_not_bind_when_the_key_is_not_the_prefix() {
        // The production shape this warning exists for: no clustering at all,
        // so the key bound prunes nothing and every merge full-scans.
        assert!(!clustering_binds(None, &cols(&["id"])));
        assert!(!clustering_binds(Some(&[]), &cols(&["id"])));
        // Clustered by something else entirely.
        assert!(!clustering_binds(
            Some(&cols(&["created_at"])),
            &cols(&["id"])
        ));
        // Right columns, wrong order: pruning is prefix-based, so a key that
        // trails the clustering cannot skip blocks.
        assert!(!clustering_binds(
            Some(&cols(&["created_at", "id"])),
            &cols(&["id"])
        ));
        // Key longer than the clustering: the tail is unpruned.
        assert!(!clustering_binds(Some(&cols(&["a"])), &cols(&["a", "b"])));
        // No key at all is not a binding prune.
        assert!(!clustering_binds(Some(&cols(&["id"])), &[]));
    }

    #[test]
    fn warnings_fold_per_kind_and_column_across_partitions() {
        // Each partition reports its own per-column counts; a caller wants one
        // entry per column for the run, not one per partition.
        let w = Warnings::default();
        for (col, n) in [("state", 3u64), ("state", 4), ("qty", 1)] {
            w.push(TransferWarning {
                kind: WarningKind::CollapsedBool,
                column: Some(col.into()),
                count: n,
                sample: None,
                message: col.to_string(),
            });
        }
        // A different kind on the same column stays a separate entry.
        w.push(TransferWarning {
            kind: WarningKind::CoercedDate,
            column: Some("state".into()),
            count: 2,
            sample: None,
            message: "d".into(),
        });
        let out = w.drain();
        assert_eq!(out.len(), 3, "{out:?}");
        // Ordered most-affected first.
        assert_eq!(out[0].kind, WarningKind::CollapsedBool);
        assert_eq!(out[0].column.as_deref(), Some("state"));
        assert_eq!(out[0].count, 7);
        assert_eq!(out[1].count, 2);
        assert_eq!(out[2].count, 1);
        // Draining empties the collector, so a second call can't double-report.
        assert!(w.drain().is_empty());
    }

    #[test]
    fn a_clean_run_reports_no_warnings() {
        let w = Warnings::default();
        report_coercions("partition 'all'", Vec::new(), &w);
        assert!(w.drain().is_empty());
    }

    #[test]
    fn report_coercions_records_one_entry_per_kind_and_column() {
        let w = Warnings::default();
        report_coercions(
            "partition 'all'",
            vec![
                (WarningKind::CollapsedBool, "x_state".to_string(), 412),
                (WarningKind::CoercedDecimal, "amount".to_string(), 3),
            ],
            &w,
        );
        let out = w.drain();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].kind, WarningKind::CollapsedBool);
        assert_eq!(out[0].column.as_deref(), Some("x_state"));
        assert_eq!(out[0].count, 412);
        // The message names the column — the thing a per-table total could not.
        assert!(out[0].message.contains("x_state"), "{}", out[0].message);
        // ...and points at the fix.
        assert!(
            out[0].message.contains("tinyint1_as_bool=False"),
            "{}",
            out[0].message
        );
    }

    #[tokio::test]
    async fn read_idle_timeout_fires_only_when_the_source_stalls() {
        let counters = Counters::default();
        // A source that never produces trips the timer...
        let err = await_source(
            tokio::time::sleep(Duration::from_secs(30)),
            &counters,
            1,
            "partition 'all'",
        )
        .await
        .unwrap_err();
        assert!(matches!(err, EtlError::ReadIdleTimeout { .. }), "{err}");
        // ...and the message says which subsystem it is NOT, because the whole
        // point of this knob is that a statement_timeout blamed the wrong one.
        let msg = err.to_string();
        assert!(msg.contains("read_idle_timeout_secs"), "{msg}");
        assert!(msg.contains("not a destination-side stall"), "{msg}");
        // A source that produces in time does not, and its wait is counted.
        let before = counters.read_nanos.load(Ordering::Relaxed);
        let v = await_source(async { 7u8 }, &counters, 1, "partition 'all'")
            .await
            .unwrap();
        assert_eq!(v, 7);
        assert!(counters.read_nanos.load(Ordering::Relaxed) >= before);
        // Zero disables it entirely: the same never-ready future must not fail
        // fast, so a short race against it has to time out on our side.
        let never = await_source(
            tokio::time::sleep(Duration::from_secs(30)),
            &counters,
            0,
            "partition 'all'",
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), never)
                .await
                .is_err(),
            "read_idle_timeout_secs=0 must not impose any deadline"
        );
    }
    #[tokio::test]
    async fn validate_with_chunk_rows_is_rejected_up_front() {
        let mut cfg = crate::config::default_test_config();
        cfg.mode = SyncMode::Incremental;
        cfg.watermark = Some("ts".into());
        cfg.key = vec!["id".into()];
        cfg.chunk_rows = Some(1000);
        let src = SourceConfig::Postgres(crate::config::PostgresConfig {
            dsn: "postgresql://user:pw@127.0.0.1:1/db".into(),
            statement_timeout_secs: 0,
            ca_cert_file: None,
            client_cert_file: None,
            client_key_file: None,
        });
        let dst = DestinationConfig::ClickHouse(crate::config::ClickHouseConfig {
            url: "http://127.0.0.1:1".into(),
            database: "default".into(),
            user: "default".into(),
            password: String::new(),
            compression: crate::config::Compression::None,
            insert_dedup_token: false,
            settings: Default::default(),
            s3_archive: None,
        });
        let cb: StagedValidationCb = Arc::new(|_info: &StagedInfo| Ok(()));
        let err = run_transfer_impl(src, dst, cfg, None, Some(cb))
            .await
            .expect_err("validate= together with chunk_rows must be rejected")
            .to_string();
        assert!(err.contains("chunk_rows"), "got: {err}");
        assert!(err.contains("validate="), "got: {err}");
    }

    #[test]
    fn throttle_zero_rate_is_safe_noop_not_panic() {
        // `validate` prevents this via the public API, but a direct Rust
        // caller must not trip an `inf` Duration panic (rows / 0.0).
        let t = ReadThrottle::new(0);
        assert!(t.reserve(1_000_000).is_zero());
    }

    #[test]
    fn throttle_first_reserve_does_not_wait() {
        // The limiter starts unfilled: the first read is never delayed
        // (`next_available` begins at construction time, already in the past by
        // the time we reserve, so `start` clamps to `now` and the wait is 0).
        let t = ReadThrottle::new(1000);
        assert!(t.reserve(500).is_zero());
    }

    #[test]
    fn throttle_paces_subsequent_reads_by_rate() {
        // At 1000 rows/s, reserving 1000 rows pushes the schedule ~1s forward,
        // so the very next reservation must wait close to a full second. Bounds
        // are wide (>0.5s, <1s) so only genuine pacing — not scheduling jitter
        // in the microseconds between two calls — can satisfy them.
        let t = ReadThrottle::new(1000);
        assert!(t.reserve(1000).is_zero());
        let wait = t.reserve(1000);
        assert!(
            wait > Duration::from_millis(500) && wait < Duration::from_secs(1),
            "expected ~1s pacing wait, got {wait:?}"
        );
    }

    #[test]
    fn throttle_is_shared_across_handles() {
        // Two clones of the same Arc share one schedule, so the cap is an
        // aggregate across all partitions rather than per-connection: after one
        // handle consumes a full second of budget, the other must wait.
        let t = Arc::new(ReadThrottle::new(1000));
        let t2 = t.clone();
        assert!(t.reserve(1000).is_zero());
        let wait = t2.reserve(1000);
        assert!(
            wait > Duration::from_millis(500),
            "a second handle should be paced by the first's reservation, got {wait:?}"
        );
    }

    fn col(name: &str) -> ColumnType {
        col_typed(name, DataType::Int64)
    }

    fn col_typed(name: &str, arrow: DataType) -> ColumnType {
        ColumnType {
            name: name.into(),
            type_id: 0,
            nullable: true,
            arrow,
            clickhouse_inner: "Int64".into(),
            arbitrary_precision_decimal: false,
        }
    }

    #[test]
    fn watermark_column_present_ok() {
        let cols = vec![col("id"), col("write_date")];
        assert!(ensure_watermark_column("write_date", &cols).is_ok());
    }

    #[test]
    fn watermark_column_missing_errors_and_lists_available() {
        let cols = vec![col("id"), col("name")];
        let msg = ensure_watermark_column("created_date", &cols)
            .unwrap_err()
            .to_string();
        assert!(
            msg.contains("created_date"),
            "names the missing column: {msg}"
        );
        assert!(
            msg.contains("id") && msg.contains("name"),
            "lists available: {msg}"
        );
    }

    #[test]
    fn lookback_disabled_is_a_noop_regardless_of_watermark_type() {
        let cols = vec![col("id")]; // Int64 — not temporal
        assert!(ensure_lookback_compatible("id", 0, &cols).is_ok());
    }

    #[test]
    fn lookback_accepts_date32_and_timestamp_watermarks() {
        let cols = vec![
            col_typed("d", DataType::Date32),
            col_typed(
                "ts",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            ),
        ];
        assert!(ensure_lookback_compatible("d", 60, &cols).is_ok());
        assert!(ensure_lookback_compatible("ts", 60, &cols).is_ok());
    }

    #[test]
    fn lookback_rejects_non_temporal_watermark() {
        let cols = vec![col("id")];
        let msg = ensure_lookback_compatible("id", 60, &cols)
            .unwrap_err()
            .to_string();
        assert!(msg.contains("lookback_seconds"), "got: {msg}");
        assert!(msg.contains("id"), "got: {msg}");
    }

    #[test]
    fn watermark_filter_unchanged_when_lookback_disabled() {
        // Regression guard: lookback_seconds=0 must produce byte-identical
        // SQL to the pre-lookback filter.
        assert_eq!(
            build_watermark_filter_pg(
                "write_date",
                None,
                Some("2024-01-01"),
                Some("2024-06-01"),
                0
            ),
            Some("\"write_date\" > '2024-01-01' AND \"write_date\" <= '2024-06-01'".to_string())
        );
        assert_eq!(
            build_watermark_filter_mysql(
                "write_date",
                None,
                Some("2024-01-01"),
                Some("2024-06-01"),
                0
            ),
            Some("`write_date` > '2024-01-01' AND `write_date` <= '2024-06-01'".to_string())
        );
        // BigQuery's bounds are CAST-typed regardless of lookback state (a
        // separate, unconditional fix — see
        // watermark_filter_bigquery_casts_both_bounds_by_type below), so this
        // one isn't byte-identical to the plain-quoted pg/mysql shape above.
        let cols = vec![col_typed("write_date", DataType::Date32)];
        assert_eq!(
            build_watermark_filter_bigquery(
                "write_date",
                Some("2024-01-01"),
                Some("2024-06-01"),
                0,
                &cols
            ),
            Some(
                "`write_date` > CAST('2024-01-01' AS DATE) AND \
                 `write_date` <= CAST('2024-06-01' AS DATE)"
                    .to_string()
            )
        );
    }

    #[test]
    fn watermark_filter_source_expr_overrides_the_bare_column() {
        // Regression test (bug report B1): with `source_query`, the generated
        // read is `SELECT ... FROM (<source_query>) AS _src WHERE <watermark>
        // > $1` — if source_query computes `watermark` via a transform (a
        // cast, a timezone shift, ...) rather than a bare pass-through of an
        // indexed base column, that WHERE binds to the *computed* value, so
        // no index on the base table can serve it (full scan on every
        // incremental run, confirmed live via EXPLAIN in the report).
        // `watermark_source_expr` lets the filter target a different
        // expression (typically a second, untransformed pass-through column
        // projected by source_query) while `watermark`'s own output keeps
        // emitting the transformed value.
        assert_eq!(
            build_watermark_filter_pg(
                "write_date",
                Some("\"write_date_raw\""),
                Some("2024-01-01"),
                Some("2024-06-01"),
                0
            ),
            Some(
                "\"write_date_raw\" > '2024-01-01' AND \"write_date_raw\" <= '2024-06-01'"
                    .to_string()
            )
        );
        assert_eq!(
            build_watermark_filter_mysql(
                "write_date",
                Some("`write_date_raw`"),
                Some("2024-01-01"),
                Some("2024-06-01"),
                0
            ),
            Some(
                "`write_date_raw` > '2024-01-01' AND `write_date_raw` <= '2024-06-01'".to_string()
            )
        );
    }

    #[test]
    fn watermark_filter_bigquery_casts_both_bounds_by_type() {
        // Regression test (bug report B2): a numeric (INT64) watermark
        // column's *upper* bound (this run's fresh snapshot max) was CAST-typed,
        // but its *lower* bound (the persisted cursor from the prior run) was
        // still emitted as a bare quoted STRING literal — BigQuery rejects
        // comparing that against INT64 with "No matching signature for
        // operator > for argument types: INT64, STRING", reproduced live
        // against real BigQuery in the report. Since the lower bound is only
        // populated once a cursor exists, this passed on a table's very first
        // incremental run and failed on every run after — every plain
        // (non-lookback) incremental sync with a numeric watermark, on the
        // second run onward.
        let cols = vec![col_typed("uor_id", DataType::Int64)];
        let f =
            build_watermark_filter_bigquery("uor_id", Some("100"), Some("500"), 0, &cols).unwrap();
        assert_eq!(
            f,
            "`uor_id` > CAST('100' AS INT64) AND `uor_id` <= CAST('500' AS INT64)"
        );
    }

    #[test]
    fn watermark_filter_bigquery_escapes_backslash_before_quote() {
        // Regression test: GoogleSQL doesn't support the ANSI doubled-quote
        // escape ('') at all, and treats backslash as an active escape
        // character — so a value with a quote or a trailing backslash needs
        // backslash-first escaping, not quote-doubling.
        let cols = vec![col_typed("note", DataType::Utf8)];
        let f = build_watermark_filter_bigquery("note", None, Some(r"a'b\"), 0, &cols).unwrap();
        assert_eq!(f, r"`note` <= CAST('a\'b\\' AS STRING)");
    }

    #[test]
    fn watermark_filter_bigquery_escapes_newlines() {
        // A quoted GoogleSQL literal cannot carry a raw newline — it ends the
        // literal early and the whole predicate fails to parse. Kept in sync
        // with sink/bigquery.rs's `escape_sql_string` test of the same name.
        let cols = vec![col_typed("note", DataType::Utf8)];
        let f = build_watermark_filter_bigquery("note", None, Some("a\nb\r\nc"), 0, &cols).unwrap();
        assert_eq!(f, r"`note` <= CAST('a\nb\r\nc' AS STRING)");
        assert!(!f.contains('\n') && !f.contains('\r'), "{f:?}");
    }

    #[test]
    fn watermark_filter_mysql_escapes_backslash_before_quote() {
        // Regression test: MySQL (unlike Postgres) treats backslash as an
        // active string-literal escape by default, so the same
        // trailing-backslash-swallows-the-quote failure mode applies here.
        let f = build_watermark_filter_mysql("note", None, None, Some(r"a'b\"), 0).unwrap();
        assert_eq!(f, r"`note` <= 'a''b\\'");
    }

    #[test]
    fn watermark_filter_pg_widens_lower_bound_with_lookback() {
        let f = build_watermark_filter_pg(
            "write_date",
            None,
            Some("2024-06-10"),
            Some("2024-06-15"),
            3600,
        )
        .unwrap();
        assert!(
            f.contains("'2024-06-10'::timestamp - interval '3600 seconds'"),
            "got: {f}"
        );
        assert!(
            f.contains("<= '2024-06-15'"),
            "upper bound stays exact: {f}"
        );
    }

    #[test]
    fn watermark_filter_mysql_widens_lower_bound_with_lookback() {
        let f = build_watermark_filter_mysql(
            "write_date",
            None,
            Some("2024-06-10 00:00:00"),
            None,
            3600,
        )
        .unwrap();
        assert!(
            f.contains("CAST('2024-06-10 00:00:00' AS DATETIME) - INTERVAL 3600 SECOND"),
            "got: {f}"
        );
    }

    #[test]
    fn watermark_filter_bigquery_dispatches_by_resolved_type() {
        let date_cols = vec![col_typed("d", DataType::Date32)];
        let f = build_watermark_filter_bigquery("d", Some("2024-06-10"), None, 3600, &date_cols)
            .unwrap();
        assert!(
            f.contains("DATE_SUB(DATE '2024-06-10', INTERVAL 1 DAY)"),
            "got: {f}"
        );

        let datetime_cols = vec![col_typed(
            "dt",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        )];
        let f = build_watermark_filter_bigquery(
            "dt",
            Some("2024-06-10 00:00:00"),
            None,
            3600,
            &datetime_cols,
        )
        .unwrap();
        assert!(
            f.contains("DATETIME_SUB(DATETIME '2024-06-10 00:00:00', INTERVAL 3600 SECOND)"),
            "got: {f}"
        );

        let ts_cols = vec![col_typed(
            "ts",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
        )];
        let f = build_watermark_filter_bigquery(
            "ts",
            Some("2024-06-10 00:00:00"),
            None,
            3600,
            &ts_cols,
        )
        .unwrap();
        assert!(
            f.contains("TIMESTAMP_SUB(TIMESTAMP '2024-06-10 00:00:00', INTERVAL 3600 SECOND)"),
            "got: {f}"
        );
    }

    #[test]
    fn lookback_bigquery_date_rounds_up_to_whole_days() {
        // 1 hour of lookback against a DATE-typed watermark can't be expressed
        // in sub-day granularity, so it rounds up to 1 whole day rather than
        // silently rounding down to 0 (which would disable lookback entirely).
        let f = lookback_lower_bound_bigquery("2024-06-10", 3600, Some(TableFieldType::Date));
        assert_eq!(f, "DATE_SUB(DATE '2024-06-10', INTERVAL 1 DAY)");
        let f = lookback_lower_bound_bigquery("2024-06-10", 86400 * 2, Some(TableFieldType::Date));
        assert_eq!(f, "DATE_SUB(DATE '2024-06-10', INTERVAL 2 DAY)");
    }

    fn int64_batch(name: &str, vals: &[Option<i64>]) -> RecordBatch {
        use arrow_array::Int64Array;
        use arrow_schema::{Field, Schema};
        let arr = Int64Array::from(vals.to_vec());
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    #[test]
    fn last_int_key_reads_last_row_or_none() {
        // Ascending order -> last row is the chunk max cursor.
        let b = int64_batch("id", &[Some(1), Some(5), Some(9)]);
        assert_eq!(last_int_key(&b, 0).unwrap(), Some(9));
        // Empty batch -> None (no cursor to advance).
        let b = int64_batch("id", &[]);
        assert_eq!(last_int_key(&b, 0).unwrap(), None);
        // NULL last key -> None (data-loss guard: caller refuses to advance).
        let b = int64_batch("id", &[Some(1), None]);
        assert_eq!(last_int_key(&b, 0).unwrap(), None);
    }

    /// Minimal cfg + plan + source_cols for build_chunk_plan gate tests.
    fn chunk_inputs(
        keyset: &str,
        arrow: DataType,
        nullable: bool,
    ) -> (TransferConfig, SelectPlan, Vec<ColumnType>) {
        let mut cfg = crate::config::default_test_config();
        cfg.mode = SyncMode::Incremental;
        cfg.watermark = Some("wm".into());
        cfg.key = vec![keyset.to_string()];
        cfg.chunk_rows = Some(1000);
        let src = vec![
            ColumnType {
                name: keyset.into(),
                type_id: 0,
                nullable,
                arrow: arrow.clone(),
                clickhouse_inner: "x".into(),
                arbitrary_precision_decimal: false,
            },
            col_typed(
                "wm",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
            ),
        ];
        let plan = SelectPlan {
            source_columns: vec![keyset.to_string(), "wm".to_string()],
            source_select_exprs: vec![None, None],
            dest_columns: src.clone(),
        };
        (cfg, plan, src)
    }

    #[test]
    fn build_chunk_plan_accepts_unique_integer_notnull_key() {
        let (cfg, plan, src) = chunk_inputs("id", DataType::Int64, false);
        let cp = build_chunk_plan(&cfg, &plan, &src, 1000, None, Some("100".into()), None).unwrap();
        assert_eq!(cp.keyset_col, "id");
        assert_eq!(cp.keyset_idx, 0);
        assert_eq!(cp.limit, 1000);
    }

    fn ct_source() -> SourceConfig {
        SourceConfig::CleverTap(crate::config::CleverTapConfig {
            base_url: "https://sg1.api.clevertap.com".into(),
            account_id: "a".into(),
            passcode: "p".into(),
            event_name: "App Launched".into(),
            batch_size: 0,
            columns: vec![ApiColumn {
                name: "ts".into(),
                bq_type: "TIMESTAMP".into(),
                path: None,
            }],
            from_date: None,
            to_date: None,
            lookback_days: 0,
        })
    }

    #[test]
    fn full_refresh_refuses_to_shrink_the_destination_by_default() {
        // The audited failure: a one-day API pull swapped over a year of
        // history. `atomic_swap` is EXCHANGE TABLES / TRUNCATE+INSERT in the two
        // sinks — neither is partition-aware — so the other 364 days are gone,
        // atomically, with a success exit. Row counts are the one
        // source-agnostic signal that sees it coming.
        assert_eq!(
            full_refresh_shrink_verdict(Some(370_900_000), 1_598_163, false),
            ShrinkVerdict::Refuse {
                existing: 370_900_000,
                lost: 369_301_837
            }
        );
        // Not an API-source quirk: one month refreshed into a monthly-
        // partitioned table loses the other eleven the same way.
        assert_eq!(
            full_refresh_shrink_verdict(Some(1_200_000), 100_000, false),
            ShrinkVerdict::Refuse {
                existing: 1_200_000,
                lost: 1_100_000
            }
        );
        // Losing a single row is still losing a row.
        assert_eq!(
            full_refresh_shrink_verdict(Some(101), 100, false),
            ShrinkVerdict::Refuse {
                existing: 101,
                lost: 1
            }
        );
    }

    #[test]
    fn full_refresh_proceeds_when_it_is_not_a_shrink() {
        // Growing, or replacing exactly, is the normal case and must stay quiet.
        assert_eq!(
            full_refresh_shrink_verdict(Some(100), 500, false),
            ShrinkVerdict::Proceed
        );
        assert_eq!(
            full_refresh_shrink_verdict(Some(100), 100, false),
            ShrinkVerdict::Proceed
        );
        // A destination that does not exist yet, or a sink that cannot report a
        // count, is not evidence of a shrink — a first run must not be blocked.
        assert_eq!(
            full_refresh_shrink_verdict(None, 0, false),
            ShrinkVerdict::Proceed
        );
        // Replacing an empty table is fine even with zero rows read.
        assert_eq!(
            full_refresh_shrink_verdict(Some(0), 0, false),
            ShrinkVerdict::Proceed
        );
        // Zero rows over a populated table is the worst case, and is refused.
        assert_eq!(
            full_refresh_shrink_verdict(Some(1), 0, false),
            ShrinkVerdict::Refuse {
                existing: 1,
                lost: 1
            }
        );
    }

    #[test]
    fn allow_full_refresh_shrink_is_an_opt_in_escape_hatch_not_a_silence() {
        // Opting in still warns: a legitimate shrink is worth a line in the log,
        // and this is the flag that used to be the *only* behaviour.
        assert_eq!(
            full_refresh_shrink_verdict(Some(1_000), 10, true),
            ShrinkVerdict::ProceedWithWarning {
                existing: 1_000,
                lost: 990
            }
        );
        // The flag does not invent a warning where there is no shrink.
        assert_eq!(
            full_refresh_shrink_verdict(Some(10), 1_000, true),
            ShrinkVerdict::Proceed
        );
    }

    #[test]
    fn derive_api_window_full_and_incremental() {
        // Full: from_date required.
        let mut src = ct_source();
        let mut cfg = crate::config::default_test_config();
        cfg.mode = SyncMode::Full;
        assert!(
            derive_api_window(&cfg, &src, None).is_err(),
            "full needs from_date"
        );
        if let SourceConfig::CleverTap(c) = &mut src {
            c.from_date = Some("2026-07-01".into());
            c.to_date = Some("2026-07-10".into());
        }
        let (from, to, wm) = derive_api_window(&cfg, &src, None).unwrap();
        assert_eq!(
            (from.as_str(), to.as_str(), wm),
            ("2026-07-01", "2026-07-10", None)
        );
        // Incremental: committed cursor wins as `from`; window end is persisted.
        cfg.mode = SyncMode::Incremental;
        let (from, _to, wm) = derive_api_window(&cfg, &src, Some("2026-07-05".into())).unwrap();
        assert_eq!(from, "2026-07-05");
        assert_eq!(wm, Some("2026-07-10".to_string()));
        // Inversion (committed > to) clamps `from` to `to`.
        let (from, to, _) = derive_api_window(&cfg, &src, Some("2026-08-01".into())).unwrap();
        assert_eq!(from, to, "from clamped to to on inversion");
    }

    #[test]
    fn derive_api_window_lookback_widens_from_on_resume_clamped_to_floor() {
        let mut src = ct_source();
        if let SourceConfig::CleverTap(c) = &mut src {
            c.from_date = Some("2026-07-01".into());
            c.to_date = Some("2026-07-31".into());
            c.lookback_days = 3;
        }
        let mut cfg = crate::config::default_test_config();
        cfg.mode = SyncMode::Incremental;
        // Resume from a committed cursor: `from` is pulled back by lookback_days.
        let (from, _to, _) = derive_api_window(&cfg, &src, Some("2026-07-20".into())).unwrap();
        assert_eq!(from, "2026-07-17", "resume from - 3 days");
        // Lookback never pulls before the configured floor.
        let (from, _to, _) = derive_api_window(&cfg, &src, Some("2026-07-02".into())).unwrap();
        assert_eq!(from, "2026-07-01", "clamped to from_date floor");
        // First run (no committed cursor): lookback does NOT apply.
        let (from, _to, _) = derive_api_window(&cfg, &src, None).unwrap();
        assert_eq!(
            from, "2026-07-01",
            "first run starts at the floor, no lookback"
        );
    }

    #[test]
    fn derive_api_window_append_uses_incremental_windowing() {
        let mut src = ct_source();
        if let SourceConfig::CleverTap(c) = &mut src {
            c.from_date = Some("2026-07-01".into());
            c.to_date = Some("2026-07-10".into());
        }
        let mut cfg = crate::config::default_test_config();
        cfg.mode = SyncMode::Append;
        // Append resumes from the committed cursor and persists the window end,
        // exactly like incremental (it just inserts instead of merging).
        let (from, _to, wm) = derive_api_window(&cfg, &src, Some("2026-07-05".into())).unwrap();
        assert_eq!(from, "2026-07-05");
        assert_eq!(wm, Some("2026-07-10".to_string()));
    }

    #[test]
    fn build_chunk_plan_rejects_nullable_non_integer_and_transformed_keys() {
        // Nullable key (NULLs silently skipped) -> reject.
        let (cfg, plan, src) = chunk_inputs("id", DataType::Int64, true);
        assert!(build_chunk_plan(&cfg, &plan, &src, 1000, None, None, None)
            .unwrap_err()
            .to_string()
            .contains("NOT NULL"));
        // Non-integer key -> reject.
        let (cfg, plan, src) = chunk_inputs("id", DataType::Utf8, false);
        assert!(build_chunk_plan(&cfg, &plan, &src, 1000, None, None, None)
            .unwrap_err()
            .to_string()
            .contains("integer"));
        // Transformed key (decoded value != raw column) -> reject.
        let (mut cfg, plan, src) = chunk_inputs("id", DataType::Int64, false);
        cfg.column_transforms =
            std::collections::HashMap::from([("id".to_string(), "id + 1".to_string())]);
        assert!(build_chunk_plan(&cfg, &plan, &src, 1000, None, None, None)
            .unwrap_err()
            .to_string()
            .contains("column_transforms"));
    }
}
