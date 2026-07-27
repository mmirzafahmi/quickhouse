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
use chrono::Utc;
use futures::StreamExt;
use mysql_async::prelude::*;
use object_store::ObjectStore;
use tokio::task::JoinSet;

use crate::archive::{archive_object_key, build_s3_store, S3ArchiveWriter};
use crate::config::{
    ApiColumn, DestinationConfig, ParquetCompression, S3ArchiveConfig, SourceConfig, SyncMode,
    TransferConfig, TransferResult, WatermarkSeed,
};
use crate::decode::CopyDecoder;
use crate::decode_api::{resolve_api_columns, ApiBatcher};
use crate::decode_bigquery::BigQueryBatcher;
use crate::decode_mysql::MySqlBatcher;
use crate::error::{EtlError, Result};
use crate::memory::MemoryBudget;
use crate::sink::Sink;
use crate::source::appsflyer::AppsFlyerSource;
use crate::source::clevertap::{CleverTapSource, PageStatus};
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

#[derive(Default)]
struct Counters {
    rows_read: AtomicU64,
    rows_written: AtomicU64,
    bytes_written: AtomicU64,
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
    sink: Sink,
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
}

impl SendCtx {
    /// Reserve budget for `batch` — awaiting here is the backpressure point:
    /// if the pipeline's memory ceiling is reached, the caller (decoder) stalls
    /// until in-flight uploads drain — then spawn its upload as a background
    /// task and return immediately so decoding can continue overlapping the
    /// network round-trip. The reservation is held until the upload completes.
    async fn spawn_upload(
        &self,
        sends: &mut JoinSet<Result<()>>,
        schema: SchemaRef,
        batch: RecordBatch,
    ) {
        let reservation = self.budget.reserve(batch.get_array_memory_size()).await;
        let ctx = self.clone();
        sends.spawn(async move {
            let _reservation = reservation; // released on task completion
            let rows = batch.num_rows() as u64;
            let bytes = ctx
                .sink
                .insert_batches(&ctx.target_table, schema, std::slice::from_ref(&batch))
                .await?;
            ctx.counters.rows_written.fetch_add(rows, Ordering::Relaxed);
            ctx.counters
                .bytes_written
                .fetch_add(bytes, Ordering::Relaxed);
            // Progress fires on *completion*, so rows_written reflects rows
            // actually landed in ClickHouse, not merely decoded.
            emit_progress(&ctx.counters, &ctx.progress, ctx.started);
            Ok(())
        });
    }
}

/// Static per-run info every partition needs to open its own S3 archive
/// writer — the S3 client and naming info are shared (built once per
/// transfer, mirroring `Sink::new`); only the partition label varies.
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
        return run_transfer_impl(source_cfg, dest, cfg, progress)
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
    let started = Instant::now();

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
    // Extracted (and cloned) before `Sink::new(dest)` consumes `dest` below —
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
        let sink = Sink::new(dest).await?;
        return run_transfer_bigquery(&source, sink, cfg, progress, started, archive_info, staging)
            .await;
    }

    // HTTP API sources (CleverTap/AppsFlyer): a declared schema + paginated
    // fetch into the BigQuery sink — a separate flow from the DB partition
    // machinery. archive is always None (BigQuery-only).
    if source_cfg.is_api() {
        let sink = Sink::new(dest).await?;
        return run_transfer_api(source_cfg, sink, cfg, progress, started, staging).await;
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
        SourceConfig::CleverTap(_) | SourceConfig::AppsFlyer(_) => {
            unreachable!("API sources handled via early return above")
        }
    });
    let sink = Sink::new(dest).await?;

    // Keyset resumable reads (MVP) commit per chunk straight into the
    // destination, which only works where incremental inserts directly (no
    // staging + swap/merge) — i.e. ClickHouse.
    if cfg.chunk_rows.is_some() && sink.requires_staging_for_incremental() {
        return Err(EtlError::config(
            "chunk_rows (keyset resumable reads) is only supported for a ClickHouse destination \
             in this version",
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
        let filter = match source.as_ref() {
            Source::Postgres(_) => build_watermark_filter_pg(
                watermark,
                last.as_deref(),
                effective_upper.as_deref(),
                cfg.lookback_seconds,
            ),
            Source::MySql(_) => build_watermark_filter_mysql(
                watermark,
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
    let target_table = prepare_target(&sink, &cfg, &plan.dest_columns, &staging).await?;
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
        };

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
        tracing::info!(
            "all partitions read: {} rows written",
            counters.rows_written.load(Ordering::Relaxed)
        );

        // --- Full refresh: atomically swap staging into place. ---
        if cfg.mode == SyncMode::Full {
            tracing::info!("swapping staging table into '{}'", cfg.dest_table);
            sink.atomic_swap(&cfg.dest_table, &staging, &plan.dest_columns).await?;
            sink.drop_table(&staging).await?;
        }

        // --- Incremental: merge staged rows into the destination (destinations
        // with no engine-level dedup), then persist the new watermark. ---
        if cfg.mode == SyncMode::Incremental {
            if sink.requires_staging_for_incremental() {
                tracing::info!("merging staged incremental rows into '{}'", cfg.dest_table);
                sink.merge_into(&cfg.dest_table, &staging, &cfg.key, &plan.dest_columns, cfg.merge_prune_partition_by.as_deref(), cfg.delete_stale_in_window).await?;
                sink.drop_table(&staging).await?;
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
            duration_secs,
            new_watermark,
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
async fn run_transfer_bigquery(
    source: &BigQuerySource,
    sink: Sink,
    cfg: TransferConfig,
    progress: Option<ProgressCb>,
    started: Instant,
    archive_info: Option<Arc<ArchiveRunInfo>>,
    staging: String,
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
    if cfg.chunk_rows.is_some() {
        return Err(EtlError::config(
            "chunk_rows (keyset resumable reads) is not supported for a BigQuery source",
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

    let target_table = prepare_target(&sink, &cfg, &plan.dest_columns, &staging).await?;
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
        };

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
        // No discrete partitions on this path (see the module docs) — "all" is
        // the only file this run will ever archive for this table.
        let mut archive_writer = match &ctx.archive {
            Some(info) => Some(info.writer_for("all", schema.clone())?),
            None => None,
        };
        while let Some(row) = iter
            .next()
            .await
            .map_err(|e| EtlError::other(format!("bigquery row error: {e}")))?
        {
            if let Some(batch) = batcher.append_row(&row)? {
                if let Some(w) = archive_writer.as_mut() {
                    w.write(&batch).await?;
                }
                ctx.spawn_upload(&mut sends, schema.clone(), batch).await;
                reap(&mut sends, false).await?;
            }
        }
        if let Some(batch) = batcher.finish()? {
            if let Some(w) = archive_writer.as_mut() {
                w.write(&batch).await?;
            }
            ctx.spawn_upload(&mut sends, schema.clone(), batch).await;
        }
        if let Some(w) = archive_writer.take() {
            w.close().await?;
        }
        reap(&mut sends, true).await?;
        counters
            .rows_read
            .fetch_add(batcher.rows_total, Ordering::Relaxed);
        emit_progress(&counters, &progress, started);
        warn_coerced_dates("bigquery read", batcher.invalid_dates_total);
        warn_coerced_decimals("bigquery read", batcher.invalid_decimals_total);
        tracing::info!(
            "bigquery read complete: {} rows written",
            counters.rows_written.load(Ordering::Relaxed)
        );

        if cfg.mode == SyncMode::Full {
            tracing::info!("swapping staging table into '{}'", cfg.dest_table);
            sink.atomic_swap(&cfg.dest_table, &staging, &plan.dest_columns).await?;
            sink.drop_table(&staging).await?;
        }
        if cfg.mode == SyncMode::Incremental {
            if sink.requires_staging_for_incremental() {
                tracing::info!("merging staged incremental rows into '{}'", cfg.dest_table);
                sink.merge_into(&cfg.dest_table, &staging, &cfg.key, &plan.dest_columns, cfg.merge_prune_partition_by.as_deref(), cfg.delete_stale_in_window).await?;
                sink.drop_table(&staging).await?;
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
            duration_secs,
            new_watermark,
        })
    }
    .await;

    if outcome.is_err() && used_staging {
        cleanup_staging(&cleanup_sink, &cleanup_staging_name).await;
    }
    outcome
}

// ---- HTTP API sources (CleverTap / AppsFlyer) ----

/// API sources write only to BigQuery. Reject any other destination up front
/// with a clear config error.
fn api_columns_of(source_cfg: &SourceConfig) -> &[ApiColumn] {
    match source_cfg {
        SourceConfig::CleverTap(c) => &c.columns,
        SourceConfig::AppsFlyer(a) => &a.columns,
        _ => &[],
    }
}

fn api_source_window(source_cfg: &SourceConfig) -> (Option<&str>, Option<&str>) {
    match source_cfg {
        SourceConfig::CleverTap(c) => (c.from_date.as_deref(), c.to_date.as_deref()),
        SourceConfig::AppsFlyer(a) => (a.from_date.as_deref(), a.to_date.as_deref()),
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

fn warn_coerced_scalars(scope: &str, n: u64) {
    if n > 0 {
        tracing::warn!(
            "{scope}: {n} value(s) couldn't be parsed to their declared type and were set to NULL"
        );
    }
}

/// Transfer from an HTTP API source (CleverTap/AppsFlyer) into any destination
/// (BigQuery or ClickHouse). Mirrors `run_transfer_bigquery`'s
/// staging/swap/merge/watermark tail; only the read side differs — a declared
/// schema + paginated fetch instead of a DB read.
async fn run_transfer_api(
    source_cfg: SourceConfig,
    sink: Sink,
    mut cfg: TransferConfig,
    progress: Option<ProgressCb>,
    started: Instant,
    staging: String,
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

    let target_table = prepare_target(&sink, &cfg, &plan.dest_columns, &staging).await?;
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
        };
        let mut batcher = ApiBatcher::new(&plan.dest_columns, &lookups, cfg.batch_rows, cfg.batch_bytes)?;
        let schema = batcher.schema();
        let mut sends: JoinSet<Result<()>> = JoinSet::new();

        match &source_cfg {
            SourceConfig::CleverTap(c) => {
                let src = CleverTapSource::new(c)?;
                let from_i = crate::source::clevertap::iso_to_yyyymmdd(&from)?;
                let to_i = crate::source::clevertap::iso_to_yyyymmdd(&to)?;
                let mut cursor = src.create_export(&c.event_name, from_i, to_i).await?;
                loop {
                    let page = src.next_page(&cursor).await?;
                    for rec in &page.records {
                        if let Some(b) = batcher.append_record(rec)? {
                            ctx.spawn_upload(&mut sends, schema.clone(), b).await;
                            reap(&mut sends, false).await?;
                        }
                    }
                    // Records are processed above BEFORE this check, so the
                    // terminal `success` page is never dropped.
                    if matches!(page.status, PageStatus::Success) || page.records.is_empty() {
                        break;
                    }
                    match page.cursor {
                        Some(next) => cursor = next,
                        None => break,
                    }
                }
            }
            SourceConfig::AppsFlyer(a) => {
                let src = AppsFlyerSource::new(a)?;
                let records = src.fetch_records(&from, &to, &lookups).await?;
                for rec in &records {
                    if let Some(b) = batcher.append_record(rec)? {
                        ctx.spawn_upload(&mut sends, schema.clone(), b).await;
                        reap(&mut sends, false).await?;
                    }
                }
            }
            _ => unreachable!("run_transfer_api only handles API sources"),
        }
        if let Some(b) = batcher.finish()? {
            ctx.spawn_upload(&mut sends, schema.clone(), b).await;
        }
        reap(&mut sends, true).await?;
        counters.rows_read.fetch_add(batcher.rows_total, Ordering::Relaxed);
        emit_progress(&counters, &progress, started);
        warn_coerced_scalars("api read", batcher.invalid_scalars_total);
        warn_coerced_dates("api read", batcher.invalid_dates_total);
        warn_coerced_decimals("api read", batcher.invalid_decimals_total);
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
            // A full-refresh REPLACES the destination. API sources are naturally
            // day/event-scoped, so a full run against an existing large table can
            // silently swap 100Ms of rows away for a handful. Warn on any shrink;
            // best-effort (a metadata lookup failure must not fail the transfer).
            let new_rows = counters.rows_written.load(Ordering::Relaxed);
            if let Ok(Some(existing)) = sink.current_row_count(&cfg.dest_table).await {
                if existing > new_rows {
                    tracing::warn!(
                        "full-refresh will REPLACE '{}' ({existing} row(s)) with only {new_rows} row(s) \
                         from window [{from}, {to}] — the destination will SHRINK. If you meant to \
                         append/upsert rather than replace, use mode=\"incremental\" with key=.",
                        cfg.dest_table
                    );
                }
            }
            tracing::info!("swapping staging table into '{}'", cfg.dest_table);
            sink.atomic_swap(&cfg.dest_table, &staging, &plan.dest_columns).await?;
            sink.drop_table(&staging).await?;
        }
        if cfg.mode == SyncMode::Incremental {
            if sink.requires_staging_for_incremental() {
                tracing::info!("merging staged incremental rows into '{}'", cfg.dest_table);
                sink.merge_into(
                    &cfg.dest_table,
                    &staging,
                    &cfg.key,
                    &plan.dest_columns,
                    cfg.merge_prune_partition_by.as_deref(),
                    cfg.delete_stale_in_window,
                )
                .await?;
                sink.drop_table(&staging).await?;
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
            duration_secs,
            new_watermark,
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
) -> Result<SourceSetup> {
    tracing::info!("connecting to postgres...");
    let control = s.connect().await?;
    tracing::debug!("postgres connection established");
    let schema_probe = match (base_table, base_query) {
        (Some(t), _) => format!("SELECT * FROM {}", quote_pg_table(t)),
        (_, Some(q)) => q.to_string(),
        _ => unreachable!("validated above"),
    };
    let source_cols = s
        .resolve_columns(&control, &schema_probe, base_table)
        .await?;

    // Only needed for incremental mode (the value is discarded otherwise) —
    // skip it in full-refresh so a watermark column left set alongside
    // mode="full" can't add a spurious query or fail on an aggregate edge
    // case (e.g. an empty table's MAX() being NULL) for a result nothing uses.
    let snapshot_max = if cfg.mode == SyncMode::Incremental {
        if let Some(w) = watermark {
            ensure_watermark_column(w, &source_cols)?;
            ensure_lookback_compatible(w, cfg.lookback_seconds, &source_cols)?;
            s.max_watermark(&control, base_table, base_query, w).await?
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
) -> Result<SourceSetup> {
    tracing::info!("connecting to mysql...");
    let mut control = s.connect().await?;
    tracing::debug!("mysql connection established");
    let schema_probe = match (base_table, base_query) {
        (Some(t), _) => format!("SELECT * FROM {}", quote_my_table(t)),
        (_, Some(q)) => q.to_string(),
        _ => unreachable!("validated above"),
    };
    let source_cols = s.resolve_columns(&mut control, &schema_probe).await?;

    // Only needed for incremental mode (the value is discarded otherwise) —
    // skip it in full-refresh so a watermark column left set alongside
    // mode="full" can't add a spurious query or fail on an aggregate edge
    // case (e.g. an empty table's MAX() being NULL) for a result nothing uses.
    let snapshot_max = if cfg.mode == SyncMode::Incremental {
        if let Some(w) = watermark {
            ensure_watermark_column(w, &source_cols)?;
            ensure_lookback_compatible(w, cfg.lookback_seconds, &source_cols)?;
            s.max_watermark(&mut control, base_table, base_query, w)
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
        let mut cursor_candidate: Option<i128> = None;

        while let Some(bytes) = stream.next().await {
            let bytes = bytes?;
            for batch in decoder.feed(&bytes)? {
                let rows = batch.num_rows() as u64;
                if let Some(k) = last_int_key(&batch, chunk.keyset_idx)? {
                    cursor_candidate = Some(cursor_candidate.map_or(k, |c| c.max(k)));
                }
                if let Some(w) = archive_writer.as_mut() {
                    w.write(&batch).await?;
                }
                ctx.spawn_upload(&mut sends, schema.clone(), batch).await;
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
            ctx.spawn_upload(&mut sends, schema.clone(), batch).await;
        }
        // Force this chunk's rows durable in the destination BEFORE advancing
        // the cursor — the invariant that makes a crash resumable.
        reap(&mut sends, true).await?;

        let rows_this_chunk = decoder.rows_total;
        ctx.counters
            .rows_read
            .fetch_add(rows_this_chunk, Ordering::Relaxed);
        warn_coerced_dates("keyset read", decoder.invalid_dates_total);
        warn_coerced_decimals("keyset read", decoder.invalid_decimals_total);

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
    let mut archive_writer = match &ctx.archive {
        Some(info) => Some(info.writer_for(&partition.label, schema.clone())?),
        None => None,
    };

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let batches = decoder.feed(&chunk)?;
        for batch in batches {
            let rows = batch.num_rows() as u64;
            if let Some(w) = archive_writer.as_mut() {
                w.write(&batch).await?;
            }
            ctx.spawn_upload(&mut sends, schema.clone(), batch).await;
            // Pace the read: pausing before pulling the next chunk applies TCP
            // backpressure to the COPY stream, slowing the server-side scan.
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
        ctx.spawn_upload(&mut sends, schema.clone(), batch).await;
    }
    if let Some(w) = archive_writer.take() {
        w.close().await?;
    }
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
    warn_coerced_dates(
        &format!("partition '{}'", partition.label),
        decoder.invalid_dates_total,
    );
    warn_coerced_decimals(
        &format!("partition '{}'", partition.label),
        decoder.invalid_decimals_total,
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

        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| EtlError::from(e).context("reading mysql row"))?;
            if let Some(batch) = batcher.append_row(row)? {
                let rows = batch.num_rows() as u64;
                if let Some(k) = last_int_key(&batch, chunk.keyset_idx)? {
                    cursor_candidate = Some(cursor_candidate.map_or(k, |c| c.max(k)));
                }
                if let Some(w) = archive_writer.as_mut() {
                    w.write(&batch).await?;
                }
                ctx.spawn_upload(&mut sends, schema.clone(), batch).await;
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
            ctx.spawn_upload(&mut sends, schema.clone(), batch).await;
        }
        reap(&mut sends, true).await?;

        let rows_this_chunk = batcher.rows_total;
        ctx.counters
            .rows_read
            .fetch_add(rows_this_chunk, Ordering::Relaxed);
        warn_coerced_dates("keyset read", batcher.invalid_dates_total);
        warn_coerced_decimals("keyset read", batcher.invalid_decimals_total);

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

    while let Some(row) = stream.next().await {
        let row = row.map_err(|e| EtlError::from(e).context("reading mysql row"))?;
        if let Some(batch) = batcher.append_row(row)? {
            let rows = batch.num_rows() as u64;
            if let Some(w) = archive_writer.as_mut() {
                w.write(&batch).await?;
            }
            ctx.spawn_upload(&mut sends, schema.clone(), batch).await;
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
        ctx.spawn_upload(&mut sends, schema.clone(), batch).await;
    }
    if let Some(w) = archive_writer.take() {
        w.close().await?;
    }
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
    warn_coerced_dates(
        &format!("partition '{}'", partition.label),
        batcher.invalid_dates_total,
    );
    warn_coerced_decimals(
        &format!("partition '{}'", partition.label),
        batcher.invalid_decimals_total,
    );
    Ok(())
}

/// Warn (once per partition / read) when a decoder coerced date/datetime values
/// to NULL — either zero-dates (`0000-00-00`) or valid dates outside ClickHouse's
/// representable `[1900-01-01, 2299-12-31]` window (e.g. `9999-12-31` sentinels).
/// Aggregated, not per-row, so a badly-legacy table can't flood the log.
fn warn_coerced_dates(scope: &str, n: u64) {
    if n > 0 {
        tracing::warn!(
            "{scope}: {n} unrepresentable or out-of-range date/datetime value(s) \
             (zero-dates like '0000-00-00', or years outside ClickHouse's 1900–2299 \
             window like '9999-12-31') coerced to NULL"
        );
    }
}

/// Warn (once per partition / read) when a decoder coerced a NUMERIC/DECIMAL
/// value to NULL because it overflowed a `Decimal(P,S)` `type_overrides`
/// entry's precision, or (Postgres only) was NaN/Infinity. Separate from
/// [`warn_coerced_dates`] — same aggregation rationale, but a distinct
/// message so the two coercion reasons aren't conflated under one count.
fn warn_coerced_decimals(scope: &str, n: u64) {
    if n > 0 {
        tracing::warn!(
            "{scope}: {n} decimal value(s) coerced to NULL (value exceeded the declared \
             Decimal(P,S) precision, or was NaN/Infinity)"
        );
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
    sink: &Sink,
    cfg: &TransferConfig,
    dest_columns: &[ColumnType],
    staging: &str,
) -> Result<String> {
    match cfg.mode {
        SyncMode::Full => {
            // Destination must exist for the atomic swap; create it empty if allowed.
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
            } else {
                tracing::debug!("destination table '{}' already exists", cfg.dest_table);
                // Full-refresh only needs to evolve a dest whose swap names its
                // columns (BigQuery INSERT...SELECT). ClickHouse's swap recreates
                // the table from staging, so drift is absorbed transparently.
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
            sink.create_table(staging, dest_columns, cfg).await?;
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

            if sink.requires_staging_for_incremental() {
                tracing::info!("creating staging table '{staging}' for incremental merge");
                sink.create_table(staging, dest_columns, cfg).await?;
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

    // Only base tables (not custom queries) support cheap range partitioning.
    let table = match &cfg.source_table {
        Some(t) if cfg.source_query.is_none() => t,
        _ => return Ok(single),
    };
    if cfg.parallelism <= 1 {
        return Ok(single);
    }

    let part_col = cfg
        .partition_column
        .clone()
        .or_else(|| cfg.key.first().cloned());
    let part_col = match part_col {
        Some(c) => c,
        None => return Ok(single),
    };
    let col = match source_cols.iter().find(|c| c.name == part_col) {
        Some(c) => c,
        None => return Ok(single),
    };

    source
        .range_partitions(
            client,
            table,
            &part_col,
            col.type_id,
            cfg.parallelism,
            col.nullable,
        )
        .await
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

    let table = match &cfg.source_table {
        Some(t) if cfg.source_query.is_none() => t,
        _ => return Ok(single),
    };
    if cfg.parallelism <= 1 {
        return Ok(single);
    }

    let part_col = cfg
        .partition_column
        .clone()
        .or_else(|| cfg.key.first().cloned());
    let part_col = match part_col {
        Some(c) => c,
        None => return Ok(single),
    };
    let col = match source_cols.iter().find(|c| c.name == part_col) {
        Some(c) => c,
        None => return Ok(single),
    };

    source
        .range_partitions(
            conn,
            table,
            &part_col,
            col.type_id,
            cfg.parallelism,
            col.nullable,
        )
        .await
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
async fn cleanup_staging(sink: &Sink, staging: &str) {
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
    last: Option<&str>,
    snapshot_max: Option<&str>,
    lookback_seconds: u64,
) -> Option<String> {
    let col = format!("\"{}\"", watermark.replace('"', "\"\""));
    let lower = last.map(|l| lookback_lower_bound_pg(l, lookback_seconds));
    let upper = snapshot_max.map(quote_sql_literal);
    build_watermark_filter(&col, lower, upper)
}

fn build_watermark_filter_mysql(
    watermark: &str,
    last: Option<&str>,
    snapshot_max: Option<&str>,
    lookback_seconds: u64,
) -> Option<String> {
    let lower = last.map(|l| lookback_lower_bound_mysql(l, lookback_seconds));
    let upper = snapshot_max.map(quote_mysql_literal);
    build_watermark_filter(&quote_my(watermark), lower, upper)
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
/// two independent reasons: (1) the lookback lower bound's `_SUB` function
/// (DATE/DATETIME/TIMESTAMP each need their own — BigQuery won't implicitly
/// compare across them), only relevant when `lookback_seconds > 0`
/// (`ensure_lookback_compatible` has already validated a temporal type by the
/// time this runs); (2) the upper bound's CAST — BigQuery only implicitly
/// coerces an untyped STRING literal against DATE/DATETIME/TIME/TIMESTAMP,
/// NOT against INT64/NUMERIC/FLOAT64/BOOL, so a plain (non-lookback)
/// incremental sync against a numeric watermark column needs the type
/// resolved unconditionally, not just when lookback is active.
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
    let lower = last.map(|l| {
        let lb_type = if lookback_seconds > 0 {
            bq_type.clone()
        } else {
            None
        };
        lookback_lower_bound_bigquery(l, lookback_seconds, lb_type)
    });
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
/// `escape_sql_string` (kept in sync with that function deliberately).
fn escape_bigquery_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
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
fn lookback_lower_bound_bigquery(
    last: &str,
    lookback_seconds: u64,
    bq_type: Option<TableFieldType>,
) -> String {
    let l = escape_bigquery_string(last);
    if lookback_seconds == 0 {
        return format!("'{l}'");
    }
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
    use arrow_schema::DataType;

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
            build_watermark_filter_pg("write_date", Some("2024-01-01"), Some("2024-06-01"), 0),
            Some("\"write_date\" > '2024-01-01' AND \"write_date\" <= '2024-06-01'".to_string())
        );
        assert_eq!(
            build_watermark_filter_mysql("write_date", Some("2024-01-01"), Some("2024-06-01"), 0),
            Some("`write_date` > '2024-01-01' AND `write_date` <= '2024-06-01'".to_string())
        );
        // BigQuery's upper bound is CAST-typed regardless of lookback state
        // (a separate, unconditional fix — see
        // watermark_filter_bigquery_casts_numeric_upper_bound below), so this
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
                "`write_date` > '2024-01-01' AND `write_date` <= CAST('2024-06-01' AS DATE)"
                    .to_string()
            )
        );
    }

    #[test]
    fn watermark_filter_bigquery_casts_numeric_upper_bound() {
        // Regression test (bug report): a numeric (INT64) watermark column's
        // upper bound was always emitted as a bare quoted STRING literal,
        // which BigQuery rejects for INT64 with "No matching signature for
        // operator <= for argument types: INT64, STRING" — reproduced live
        // against real BigQuery in the report. Every plain (non-lookback)
        // incremental sync with an id-based watermark hit this on every run.
        let cols = vec![col_typed("uor_id", DataType::Int64)];
        let f =
            build_watermark_filter_bigquery("uor_id", Some("100"), Some("500"), 0, &cols).unwrap();
        assert_eq!(f, "`uor_id` > '100' AND `uor_id` <= CAST('500' AS INT64)");
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
    fn watermark_filter_mysql_escapes_backslash_before_quote() {
        // Regression test: MySQL (unlike Postgres) treats backslash as an
        // active string-literal escape by default, so the same
        // trailing-backslash-swallows-the-quote failure mode applies here.
        let f = build_watermark_filter_mysql("note", None, Some(r"a'b\"), 0).unwrap();
        assert_eq!(f, r"`note` <= 'a''b\\'");
    }

    #[test]
    fn watermark_filter_pg_widens_lower_bound_with_lookback() {
        let f =
            build_watermark_filter_pg("write_date", Some("2024-06-10"), Some("2024-06-15"), 3600)
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
        let f = build_watermark_filter_mysql("write_date", Some("2024-06-10 00:00:00"), None, 3600)
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
