//! Transfer orchestration: schema resolution -> DDL -> parallel partitioned
//! stream/decode/insert -> (full) atomic swap or (incremental) watermark
//! persist. Each source engine (Postgres, MySQL, ...) plugs in via the
//! [`Source`] enum; everything downstream of "decode into Arrow batches" is
//! source-agnostic.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::StreamExt;
use mysql_async::prelude::*;
use tokio::task::JoinSet;

use crate::config::{DestinationConfig, SourceConfig, SyncMode, TransferConfig, TransferResult};
use crate::decode::CopyDecoder;
use crate::decode_bigquery::BigQueryBatcher;
use crate::decode_mysql::MySqlBatcher;
use crate::error::{EtlError, Result};
use crate::memory::MemoryBudget;
use crate::sink::Sink;
use crate::source::mysql::{quote_my, quote_my_table};
use crate::source::{BigQuerySource, MySqlSource, PgSource, Partition, Source};
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
}

impl SendCtx {
    /// Reserve budget for `batch` — awaiting here is the backpressure point:
    /// if the pipeline's memory ceiling is reached, the caller (decoder) stalls
    /// until in-flight uploads drain — then spawn its upload as a background
    /// task and return immediately so decoding can continue overlapping the
    /// network round-trip. The reservation is held until the upload completes.
    async fn spawn_upload(&self, sends: &mut JoinSet<Result<()>>, schema: SchemaRef, batch: RecordBatch) {
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
            ctx.counters.bytes_written.fetch_add(bytes, Ordering::Relaxed);
            // Progress fires on *completion*, so rows_written reflects rows
            // actually landed in ClickHouse, not merely decoded.
            emit_progress(&ctx.counters, &ctx.progress, ctx.started);
            Ok(())
        });
    }
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

const STAGING_SUFFIX: &str = "_quickhouse_tmp";

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
            .unwrap_or(""),
        cfg.dest_table
    );
    run_transfer_impl(source_cfg, dest, cfg, progress)
        .await
        .map_err(|e| e.context(table_context))
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
    cfg.validate()?;
    // Drop mode-irrelevant fields (e.g. a watermark passed with mode="full")
    // so the config that runs matches what's effective — see normalize().
    cfg.normalize();
    let started = Instant::now();

    let source_label = cfg
        .source_table
        .clone()
        .or_else(|| cfg.source_query.clone())
        .unwrap_or_default();
    tracing::info!(
        "starting {} sync: {} -> {} (mode={:?})",
        source_cfg.kind(),
        source_label,
        cfg.dest_table,
        cfg.mode
    );

    // BigQuery has a genuinely different execution model (no discrete
    // range-partitions to fan out; a single read session that streams via
    // BigQuery-managed parallel streams, drained sequentially on our side —
    // see source/bigquery.rs's module docs). Handled as a fully separate
    // flow rather than contorting the partition-based abstraction below,
    // which was built for connection-oriented sources.
    if let SourceConfig::BigQuery(bq) = &source_cfg {
        let source = BigQuerySource::new(bq.project_id.clone(), bq.credentials_file.clone());
        let sink = Sink::new(dest).await?;
        return run_transfer_bigquery(&source, sink, cfg, progress, started).await;
    }

    let source = Arc::new(match &source_cfg {
        SourceConfig::Postgres(pg) => Source::Postgres(PgSource::new(
            pg.dsn.clone(),
            pg.statement_timeout_secs,
            pg.ca_cert_file.clone(),
        )),
        SourceConfig::MySql(my) => Source::MySql(MySqlSource::new(
            my.dsn.clone(),
            my.statement_timeout_secs,
            my.ca_cert_file.clone(),
            my.require_tls,
        )),
        SourceConfig::BigQuery(_) => unreachable!("handled via early return above"),
    });
    let sink = Sink::new(dest).await?;

    let base_table = cfg.source_table.clone();
    let base_query = cfg.source_query.clone();
    let watermark = cfg.watermark.clone();

    // --- Resolve source schema, incremental snapshot max, and partitions,
    // all on one control connection. ---
    let setup = match source.as_ref() {
        Source::Postgres(s) => {
            setup_postgres(s, &cfg, base_table.as_deref(), base_query.as_deref(), watermark.as_deref())
                .await?
        }
        Source::MySql(s) => {
            setup_mysql(s, &cfg, base_table.as_deref(), base_query.as_deref(), watermark.as_deref())
                .await?
        }
        Source::BigQuery(_) => unreachable!("BigQuery is handled via the early return in run_transfer"),
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

    let plan: SelectPlan = transform::plan(&source_cols, &cfg)?;
    let plan = Arc::new(plan);

    // --- Incremental: read last watermark, build the "since last run" filter. ---
    let (extra_filter, new_watermark) = if cfg.mode == SyncMode::Incremental {
        let watermark = cfg.watermark.as_ref().unwrap();
        let last = sink.read_last_watermark(&cfg).await?;
        tracing::info!(
            "incremental watermark on '{}': last synced={:?}, current source max={:?}",
            watermark,
            last,
            snapshot_max
        );
        let filter = match source.as_ref() {
            Source::Postgres(_) => {
                build_watermark_filter_pg(watermark, last.as_deref(), snapshot_max.as_deref(), cfg.lookback_seconds)
            }
            Source::MySql(_) => {
                build_watermark_filter_mysql(watermark, last.as_deref(), snapshot_max.as_deref(), cfg.lookback_seconds)
            }
            Source::BigQuery(_) => unreachable!("BigQuery is handled via the early return in run_transfer"),
        };
        (filter, snapshot_max)
    } else {
        (None, None)
    };

    // --- Ensure destination / staging tables exist. ---
    let target_table = prepare_target(&sink, &cfg, &plan.dest_columns).await?;

    tracing::info!(
        "quickhouse: transferring into {} across {} partition(s), parallelism={}",
        target_table,
        partitions.len(),
        cfg.parallelism
    );

    // --- Fan out partitions with bounded concurrency. ---
    let counters = Arc::new(Counters::default());
    let cfg = Arc::new(cfg);
    let extra_filter = Arc::new(extra_filter);
    let target_table = Arc::new(target_table);
    let ctx = SendCtx {
        sink: sink.clone(),
        budget: MemoryBudget::new(cfg.max_memory_bytes),
        target_table: target_table.clone(),
        counters: counters.clone(),
        progress: progress.clone(),
        started,
    };

    let mut results = futures::stream::iter(partitions.into_iter().map(|part| {
        let source = source.clone();
        let plan = plan.clone();
        let cfg = cfg.clone();
        let ctx = ctx.clone();
        let extra_filter = extra_filter.clone();
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
        sink.atomic_swap(&cfg.dest_table, &staging_name(&cfg.dest_table), &plan.dest_columns)
            .await?;
        sink.drop_table(&staging_name(&cfg.dest_table)).await?;
    }

    // --- Incremental: merge staged rows into the destination (destinations
    // with no engine-level dedup), then persist the new watermark. ---
    if cfg.mode == SyncMode::Incremental {
        if sink.requires_staging_for_incremental() {
            tracing::info!("merging staged incremental rows into '{}'", cfg.dest_table);
            sink.merge_into(
                &cfg.dest_table,
                &staging_name(&cfg.dest_table),
                &cfg.key,
                &plan.dest_columns,
            )
            .await?;
            sink.drop_table(&staging_name(&cfg.dest_table)).await?;
        }
        if let Some(w) = &new_watermark {
            tracing::info!("persisting new watermark: {w}");
            sink.persist_watermark(&cfg, w, counters.rows_written.load(Ordering::Relaxed))
                .await?;
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
) -> Result<TransferResult> {
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

    let plan: SelectPlan = transform::plan(&source_cols, &cfg)?;

    let (row_restriction, new_watermark) = if cfg.mode == SyncMode::Incremental {
        let watermark = cfg.watermark.as_ref().unwrap();
        ensure_watermark_column(watermark, &source_cols)?;
        ensure_lookback_compatible(watermark, cfg.lookback_seconds, &source_cols)?;
        let last = sink.read_last_watermark(&cfg).await?;
        let table_sql = crate::source::bigquery::table_sql(&table_ref);
        let snapshot_max = source
            .max_watermark(&client, &project_id, &table_sql, watermark)
            .await?;
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

    let target_table = prepare_target(&sink, &cfg, &plan.dest_columns).await?;

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
    while let Some(row) = iter
        .next()
        .await
        .map_err(|e| EtlError::other(format!("bigquery row error: {e}")))?
    {
        if let Some(batch) = batcher.append_row(&row)? {
            ctx.spawn_upload(&mut sends, schema.clone(), batch).await;
            reap(&mut sends, false).await?;
        }
    }
    if let Some(batch) = batcher.finish()? {
        ctx.spawn_upload(&mut sends, schema.clone(), batch).await;
    }
    reap(&mut sends, true).await?;
    counters
        .rows_read
        .fetch_add(batcher.rows_total, Ordering::Relaxed);
    emit_progress(&counters, &progress, started);
    warn_coerced_dates("bigquery read", batcher.invalid_dates_total);
    tracing::info!(
        "bigquery read complete: {} rows written",
        counters.rows_written.load(Ordering::Relaxed)
    );

    if cfg.mode == SyncMode::Full {
        tracing::info!("swapping staging table into '{}'", cfg.dest_table);
        sink.atomic_swap(&cfg.dest_table, &staging_name(&cfg.dest_table), &plan.dest_columns)
            .await?;
        sink.drop_table(&staging_name(&cfg.dest_table)).await?;
    }
    if cfg.mode == SyncMode::Incremental {
        if sink.requires_staging_for_incremental() {
            tracing::info!("merging staged incremental rows into '{}'", cfg.dest_table);
            sink.merge_into(
                &cfg.dest_table,
                &staging_name(&cfg.dest_table),
                &cfg.key,
                &plan.dest_columns,
            )
            .await?;
            sink.drop_table(&staging_name(&cfg.dest_table)).await?;
        }
        if let Some(w) = &new_watermark {
            tracing::info!("persisting new watermark: {w}");
            sink.persist_watermark(&cfg, w, counters.rows_written.load(Ordering::Relaxed))
                .await?;
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
        (Some(t), _) => format!("SELECT * FROM {}", quote_table(t)),
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
) -> Result<()> {
    tracing::info!("partition '{}' starting", partition.label);
    let client = source.connect().await?;
    let copy_sql = source.copy_sql(
        &plan.source_columns,
        base_table,
        base_query,
        &partition,
        extra_filter,
    );
    tracing::debug!("partition {}: {copy_sql}", partition.label);

    let stream = source.copy_stream(&client, &copy_sql).await?;
    futures::pin_mut!(stream);

    let mut decoder = CopyDecoder::with_batch_bytes(&plan.dest_columns, cfg.batch_rows, cfg.batch_bytes)?;
    let schema = decoder.schema();
    let mut sends: JoinSet<Result<()>> = JoinSet::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let batches = decoder.feed(&chunk)?;
        for batch in batches {
            ctx.spawn_upload(&mut sends, schema.clone(), batch).await;
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
        ctx.spawn_upload(&mut sends, schema.clone(), batch).await;
    }
    reap(&mut sends, true).await?; // wait for all uploads before returning

    ctx.counters
        .rows_read
        .fetch_add(decoder.rows_total, Ordering::Relaxed);
    emit_progress(&ctx.counters, &ctx.progress, ctx.started);
    tracing::info!("partition '{}' complete: {} rows", partition.label, decoder.rows_total);
    warn_coerced_dates(&format!("partition '{}'", partition.label), decoder.invalid_dates_total);
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
) -> Result<()> {
    tracing::info!("partition '{}' starting", partition.label);
    let mut conn = source.connect().await?;
    let select_sql = source.select_sql(
        &plan.source_columns,
        base_table,
        base_query,
        &partition,
        extra_filter,
    );
    tracing::debug!("partition {}: {select_sql}", partition.label);

    let mut batcher = MySqlBatcher::with_batch_bytes(&plan.dest_columns, cfg.batch_rows, cfg.batch_bytes)?;
    let schema = batcher.schema();
    let mut sends: JoinSet<Result<()>> = JoinSet::new();

    // Use the binary protocol (prepared statement) for actual row fetching,
    // not just for resolve_columns's schema probe: plain query_iter uses the
    // text protocol, which returns every value as Bytes (ASCII text) even
    // for integer/float columns, regardless of the column's real type.
    let stmt = conn
        .prep(select_sql)
        .await
        .map_err(|e| EtlError::other(format!("mysql prepare error: {e}")))?;
    let mut result = conn
        .exec_iter(stmt, ())
        .await
        .map_err(|e| EtlError::other(format!("mysql query error: {e}")))?;
    let stream = result
        .stream::<mysql_async::Row>()
        .await
        .map_err(|e| EtlError::other(format!("mysql stream error: {e}")))?
        .ok_or_else(|| EtlError::other("mysql query returned no result set"))?;
    futures::pin_mut!(stream);

    while let Some(row) = stream.next().await {
        let row = row.map_err(|e| EtlError::other(format!("mysql row error: {e}")))?;
        if let Some(batch) = batcher.append_row(row)? {
            ctx.spawn_upload(&mut sends, schema.clone(), batch).await;
            reap(&mut sends, false).await?; // surface any upload error promptly
        }
    }
    if let Some(batch) = batcher.finish()? {
        ctx.spawn_upload(&mut sends, schema.clone(), batch).await;
    }
    reap(&mut sends, true).await?; // wait for all uploads before returning

    ctx.counters
        .rows_read
        .fetch_add(batcher.rows_total, Ordering::Relaxed);
    emit_progress(&ctx.counters, &ctx.progress, ctx.started);
    tracing::info!("partition '{}' complete: {} rows", partition.label, batcher.rows_total);
    warn_coerced_dates(&format!("partition '{}'", partition.label), batcher.invalid_dates_total);
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
fn ensure_lookback_compatible(watermark: &str, lookback_seconds: u64, source_cols: &[ColumnType]) -> Result<()> {
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
) -> Result<String> {
    match cfg.mode {
        SyncMode::Full => {
            // Destination must exist for the atomic swap; create it empty if allowed.
            if !sink.table_exists(&cfg.dest_table).await? {
                if cfg.create_if_missing {
                    tracing::info!("destination table '{}' does not exist; creating it", cfg.dest_table);
                    sink.create_table(&cfg.dest_table, dest_columns, cfg).await?;
                } else {
                    return Err(EtlError::config(format!(
                        "destination table {} does not exist and create_if_missing=false",
                        cfg.dest_table
                    )));
                }
            } else {
                tracing::debug!("destination table '{}' already exists", cfg.dest_table);
            }
            // Fresh staging table (dropped first in case a prior run crashed).
            let staging = staging_name(&cfg.dest_table);
            tracing::info!("creating staging table '{staging}'");
            sink.drop_table(&staging).await?;
            sink.create_table(&staging, dest_columns, cfg).await?;
            Ok(staging)
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
                    tracing::info!("destination table '{}' does not exist; creating it", cfg.dest_table);
                    sink.create_table(&cfg.dest_table, dest_columns, cfg).await?;
                } else {
                    return Err(EtlError::config(format!(
                        "destination table {} does not exist and create_if_missing=false",
                        cfg.dest_table
                    )));
                }
            } else {
                tracing::debug!("destination table '{}' already exists", cfg.dest_table);
            }
            sink.ensure_state_table().await?;

            if sink.requires_staging_for_incremental() {
                let staging = staging_name(&cfg.dest_table);
                tracing::info!("creating staging table '{staging}' for incremental merge");
                sink.drop_table(&staging).await?;
                sink.create_table(&staging, dest_columns, cfg).await?;
                Ok(staging)
            } else {
                Ok(cfg.dest_table.clone())
            }
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

fn staging_name(dest: &str) -> String {
    format!("{dest}{STAGING_SUFFIX}")
}

fn quote_table(table: &str) -> String {
    match table.split_once('.') {
        Some((s, t)) => format!("\"{}\".\"{}\"", s.trim_matches('"'), t.trim_matches('"')),
        None => format!("\"{}\"", table.trim_matches('"')),
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
    build_watermark_filter(&col, lower, snapshot_max)
}

fn build_watermark_filter_mysql(
    watermark: &str,
    last: Option<&str>,
    snapshot_max: Option<&str>,
    lookback_seconds: u64,
) -> Option<String> {
    let lower = last.map(|l| lookback_lower_bound_mysql(l, lookback_seconds));
    build_watermark_filter(&quote_my(watermark), lower, snapshot_max)
}

/// BigQuery Standard SQL uses the same backtick identifier quoting as MySQL.
/// `source_cols` resolves the watermark column's BigQuery type
/// (DATE/DATETIME/TIMESTAMP each need their own `_SUB` function and typed
/// literal — BigQuery won't implicitly compare across them); only looked up
/// when `lookback_seconds > 0` (`ensure_lookback_compatible` has already run
/// by the time this is called, so the lookup is guaranteed to succeed).
fn build_watermark_filter_bigquery(
    watermark: &str,
    last: Option<&str>,
    snapshot_max: Option<&str>,
    lookback_seconds: u64,
    source_cols: &[ColumnType],
) -> Option<String> {
    let lower = last.map(|l| {
        let bq_type = if lookback_seconds > 0 {
            source_cols
                .iter()
                .find(|c| c.name == watermark)
                .and_then(|c| arrow_to_bigquery_type(&c.arrow))
        } else {
            None
        };
        lookback_lower_bound_bigquery(l, lookback_seconds, bq_type)
    });
    build_watermark_filter(&quote_my(watermark), lower, snapshot_max)
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
    let l = last.replace('\'', "''");
    if lookback_seconds == 0 {
        return format!("'{l}'");
    }
    format!("(CAST('{l}' AS DATETIME) - INTERVAL {lookback_seconds} SECOND)")
}

/// `DATE_SUB` has no sub-day granularity, so a sub-day `lookback_seconds`
/// against a `DATE` watermark rounds *up* to whole days via `div_ceil` —
/// documented behavior, not silently wrong (a 1-hour lookback on a DATE
/// column re-includes the whole prior day, not nothing).
fn lookback_lower_bound_bigquery(last: &str, lookback_seconds: u64, bq_type: Option<TableFieldType>) -> String {
    let l = last.replace('\'', "''");
    if lookback_seconds == 0 {
        return format!("'{l}'");
    }
    match bq_type.expect("ensure_lookback_compatible already validated a temporal watermark type") {
        TableFieldType::Date => {
            format!("DATE_SUB(DATE '{l}', INTERVAL {} DAY)", lookback_seconds.div_ceil(86400))
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

fn build_watermark_filter(col: &str, lower_bound: Option<String>, snapshot_max: Option<&str>) -> Option<String> {
    let mut clauses = Vec::new();
    if let Some(lb) = lower_bound {
        clauses.push(format!("{col} > {lb}"));
    }
    if let Some(m) = snapshot_max {
        clauses.push(format!("{col} <= '{}'", m.replace('\'', "''")));
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
        assert!(msg.contains("created_date"), "names the missing column: {msg}");
        assert!(msg.contains("id") && msg.contains("name"), "lists available: {msg}");
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
        let cols = vec![col_typed("write_date", DataType::Date32)];
        assert_eq!(
            build_watermark_filter_bigquery("write_date", Some("2024-01-01"), Some("2024-06-01"), 0, &cols),
            Some("`write_date` > '2024-01-01' AND `write_date` <= '2024-06-01'".to_string())
        );
    }

    #[test]
    fn watermark_filter_pg_widens_lower_bound_with_lookback() {
        let f = build_watermark_filter_pg("write_date", Some("2024-06-10"), Some("2024-06-15"), 3600).unwrap();
        assert!(
            f.contains("'2024-06-10'::timestamp - interval '3600 seconds'"),
            "got: {f}"
        );
        assert!(f.contains("<= '2024-06-15'"), "upper bound stays exact: {f}");
    }

    #[test]
    fn watermark_filter_mysql_widens_lower_bound_with_lookback() {
        let f = build_watermark_filter_mysql("write_date", Some("2024-06-10 00:00:00"), None, 3600).unwrap();
        assert!(
            f.contains("CAST('2024-06-10 00:00:00' AS DATETIME) - INTERVAL 3600 SECOND"),
            "got: {f}"
        );
    }

    #[test]
    fn watermark_filter_bigquery_dispatches_by_resolved_type() {
        let date_cols = vec![col_typed("d", DataType::Date32)];
        let f = build_watermark_filter_bigquery("d", Some("2024-06-10"), None, 3600, &date_cols).unwrap();
        assert!(f.contains("DATE_SUB(DATE '2024-06-10', INTERVAL 1 DAY)"), "got: {f}");

        let datetime_cols = vec![col_typed(
            "dt",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        )];
        let f = build_watermark_filter_bigquery("dt", Some("2024-06-10 00:00:00"), None, 3600, &datetime_cols)
            .unwrap();
        assert!(
            f.contains("DATETIME_SUB(DATETIME '2024-06-10 00:00:00', INTERVAL 3600 SECOND)"),
            "got: {f}"
        );

        let ts_cols = vec![col_typed(
            "ts",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
        )];
        let f = build_watermark_filter_bigquery("ts", Some("2024-06-10 00:00:00"), None, 3600, &ts_cols).unwrap();
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
}
