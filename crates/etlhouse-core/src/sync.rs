//! Transfer orchestration: schema resolution -> DDL -> parallel partitioned
//! COPY/decode/insert -> (full) atomic swap or (incremental) watermark persist.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;

use crate::config::{ClickHouseConfig, PostgresConfig, SyncMode, TransferConfig, TransferResult};
use crate::decode::CopyDecoder;
use crate::error::{EtlError, Result};
use crate::sink::ClickHouseSink;
use crate::source::{PgSource, Partition};
use crate::transform::{self, SelectPlan};
use crate::ddl;
use crate::types::ColumnType;

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

const STAGING_SUFFIX: &str = "_etlhouse_tmp";

/// Run one table transfer end to end.
pub async fn run_transfer(
    pg: PostgresConfig,
    ch: ClickHouseConfig,
    cfg: TransferConfig,
    progress: Option<ProgressCb>,
) -> Result<TransferResult> {
    cfg.validate()?;
    let started = Instant::now();

    let source = Arc::new(PgSource::new(pg.dsn.clone(), pg.statement_timeout_secs));
    let sink = ClickHouseSink::new(ch)?;

    // --- Resolve source schema on a control connection. ---
    let control = source.connect().await?;
    let base_table = cfg.source_table.clone();
    let base_query = cfg.source_query.clone();
    let schema_probe = match (&base_table, &base_query) {
        (Some(t), _) => format!("SELECT * FROM {}", quote_table(t)),
        (_, Some(q)) => q.clone(),
        _ => unreachable!("validated above"),
    };
    let source_cols = source
        .resolve_columns(&control, &schema_probe, base_table.as_deref())
        .await?;

    let plan: SelectPlan = transform::plan(&source_cols, &cfg)?;
    let plan = Arc::new(plan);

    // --- Incremental: read last watermark + snapshot the current max. ---
    let (extra_filter, new_watermark) = if cfg.mode == SyncMode::Incremental {
        let watermark = cfg.watermark.as_ref().unwrap();
        let last = read_last_watermark(&sink, &cfg).await?;
        let snapshot_max = source
            .max_watermark(&control, base_table.as_deref(), base_query.as_deref(), watermark)
            .await?;
        let filter = build_watermark_filter(watermark, last.as_deref(), snapshot_max.as_deref());
        (filter, snapshot_max)
    } else {
        (None, None)
    };

    // --- Ensure destination / staging tables exist. ---
    let target_table = prepare_target(&sink, &cfg, &plan.dest_columns).await?;

    // --- Compute partitions. ---
    let partitions = compute_partitions(&source, &control, &cfg, &source_cols).await?;
    drop(control);

    tracing::info!(
        "etlhouse: transferring into {} across {} partition(s), parallelism={}",
        target_table,
        partitions.len(),
        cfg.parallelism
    );

    // --- Fan out partitions with bounded concurrency. ---
    let counters = Arc::new(Counters::default());
    let cfg = Arc::new(cfg);
    let extra_filter = Arc::new(extra_filter);
    let target_table = Arc::new(target_table);

    let mut results = futures::stream::iter(partitions.into_iter().map(|part| {
        let source = source.clone();
        let sink = sink.clone();
        let plan = plan.clone();
        let cfg = cfg.clone();
        let counters = counters.clone();
        let progress = progress.clone();
        let extra_filter = extra_filter.clone();
        let target_table = target_table.clone();
        let base_table = base_table.clone();
        let base_query = base_query.clone();
        async move {
            transfer_partition(
                source,
                sink,
                plan,
                cfg,
                counters,
                progress,
                &target_table,
                base_table.as_deref(),
                base_query.as_deref(),
                extra_filter.as_deref(),
                part,
                started,
            )
            .await
        }
    }))
    .buffer_unordered(cfg.parallelism);

    while let Some(r) = results.next().await {
        r?; // propagate the first partition error
    }

    // --- Full refresh: atomically swap staging into place. ---
    if cfg.mode == SyncMode::Full {
        sink.exchange_tables(&cfg.dest_table, &staging_name(&cfg.dest_table))
            .await?;
        sink.drop_table(&staging_name(&cfg.dest_table)).await?;
    }

    // --- Incremental: persist the new watermark. ---
    if cfg.mode == SyncMode::Incremental {
        if let Some(w) = &new_watermark {
            persist_watermark(
                &sink,
                &cfg,
                w,
                counters.rows_written.load(Ordering::Relaxed),
            )
            .await?;
        }
    }

    let duration_secs = started.elapsed().as_secs_f64();
    Ok(TransferResult {
        rows_read: counters.rows_read.load(Ordering::Relaxed),
        rows_written: counters.rows_written.load(Ordering::Relaxed),
        bytes_written: counters.bytes_written.load(Ordering::Relaxed),
        duration_secs,
        new_watermark,
    })
}

#[allow(clippy::too_many_arguments)]
async fn transfer_partition(
    source: Arc<PgSource>,
    sink: ClickHouseSink,
    plan: Arc<SelectPlan>,
    cfg: Arc<TransferConfig>,
    counters: Arc<Counters>,
    progress: Option<ProgressCb>,
    target_table: &str,
    base_table: Option<&str>,
    base_query: Option<&str>,
    extra_filter: Option<&str>,
    partition: Partition,
    started: Instant,
) -> Result<()> {
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

    let mut decoder = CopyDecoder::new(&plan.dest_columns, cfg.batch_rows)?;
    let schema = decoder.schema();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let batches = decoder.feed(&chunk)?;
        for batch in batches {
            flush(&sink, target_table, &schema, &batch, &counters).await?;
            emit_progress(&counters, &progress, started);
        }
    }
    if let Some(batch) = decoder.finish()? {
        flush(&sink, target_table, &schema, &batch, &counters).await?;
    }
    if !decoder.saw_trailer() {
        return Err(EtlError::decode(format!(
            "COPY stream for partition {} ended without a trailer",
            partition.label
        )));
    }

    counters
        .rows_read
        .fetch_add(decoder.rows_total, Ordering::Relaxed);
    emit_progress(&counters, &progress, started);
    Ok(())
}

async fn flush(
    sink: &ClickHouseSink,
    table: &str,
    schema: &arrow_schema::SchemaRef,
    batch: &arrow_array::RecordBatch,
    counters: &Counters,
) -> Result<()> {
    let rows = batch.num_rows() as u64;
    let bytes = sink
        .insert_batches(table, schema.clone(), std::slice::from_ref(batch))
        .await?;
    counters.rows_written.fetch_add(rows, Ordering::Relaxed);
    counters.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    Ok(())
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
/// destination table itself.
async fn prepare_target(
    sink: &ClickHouseSink,
    cfg: &TransferConfig,
    dest_columns: &[ColumnType],
) -> Result<String> {
    match cfg.mode {
        SyncMode::Full => {
            // Destination must exist for EXCHANGE; create it empty if allowed.
            if !sink.table_exists(&cfg.dest_table).await? {
                if cfg.create_if_missing {
                    let sql = ddl::create_table(
                        sink.database(),
                        &cfg.dest_table,
                        dest_columns,
                        cfg,
                    )?;
                    sink.execute(&sql).await?;
                } else {
                    return Err(EtlError::config(format!(
                        "destination table {} does not exist and create_if_missing=false",
                        cfg.dest_table
                    )));
                }
            }
            // Fresh staging table (dropped first in case a prior run crashed).
            let staging = staging_name(&cfg.dest_table);
            sink.drop_table(&staging).await?;
            let sql = ddl::create_table(sink.database(), &staging, dest_columns, cfg)?;
            sink.execute(&sql).await?;
            Ok(staging)
        }
        SyncMode::Incremental => {
            if !sink.table_exists(&cfg.dest_table).await? {
                if cfg.create_if_missing {
                    let sql = ddl::create_table(
                        sink.database(),
                        &cfg.dest_table,
                        dest_columns,
                        cfg,
                    )?;
                    sink.execute(&sql).await?;
                } else {
                    return Err(EtlError::config(format!(
                        "destination table {} does not exist and create_if_missing=false",
                        cfg.dest_table
                    )));
                }
            }
            sink.execute(&ddl::create_state_table(sink.database())).await?;
            Ok(cfg.dest_table.clone())
        }
    }
}

async fn compute_partitions(
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
            col.pg_oid,
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

fn build_watermark_filter(
    watermark: &str,
    last: Option<&str>,
    snapshot_max: Option<&str>,
) -> Option<String> {
    let col = format!("\"{}\"", watermark.replace('"', "\"\""));
    let mut clauses = Vec::new();
    if let Some(l) = last {
        clauses.push(format!("{col} > '{}'", l.replace('\'', "''")));
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

async fn read_last_watermark(sink: &ClickHouseSink, cfg: &TransferConfig) -> Result<Option<String>> {
    // The state table may not exist yet on the very first incremental run.
    if !sink.table_exists("_etlhouse_state").await? {
        return Ok(None);
    }
    let source_id = cfg
        .source_table
        .clone()
        .or_else(|| cfg.source_query.clone())
        .unwrap_or_default();
    let sql = format!(
        "SELECT last_watermark FROM {}.`_etlhouse_state` FINAL \
         WHERE source_table = '{}' AND dest_table = '{}' \
         ORDER BY run_ts DESC LIMIT 1",
        crate::ddl::quote_ident(sink.database()),
        source_id.replace('\'', "''"),
        cfg.dest_table.replace('\'', "''"),
    );
    sink.query_scalar(&sql).await
}

async fn persist_watermark(
    sink: &ClickHouseSink,
    cfg: &TransferConfig,
    watermark: &str,
    rows: u64,
) -> Result<()> {
    let source_id = cfg
        .source_table
        .clone()
        .or_else(|| cfg.source_query.clone())
        .unwrap_or_default();
    let sql = format!(
        "INSERT INTO {}.`_etlhouse_state` (source_table, dest_table, last_watermark, rows) \
         VALUES ('{}', '{}', '{}', {})",
        crate::ddl::quote_ident(sink.database()),
        source_id.replace('\'', "''"),
        cfg.dest_table.replace('\'', "''"),
        watermark.replace('\'', "''"),
        rows,
    );
    sink.execute(&sql).await
}
