//! Transfer orchestration: schema resolution -> DDL -> parallel partitioned
//! stream/decode/insert -> (full) atomic swap or (incremental) watermark
//! persist. Each source engine (Postgres, MySQL, ...) plugs in via the
//! [`Source`] enum; everything downstream of "decode into Arrow batches" is
//! source-agnostic.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use mysql_async::prelude::*;

use crate::config::{ClickHouseConfig, SourceConfig, SyncMode, TransferConfig, TransferResult};
use crate::ddl;
use crate::decode::CopyDecoder;
use crate::decode_bigquery::BigQueryBatcher;
use crate::decode_mysql::MySqlBatcher;
use crate::error::{EtlError, Result};
use crate::sink::ClickHouseSink;
use crate::source::mysql::{quote_my, quote_my_table};
use crate::source::{BigQuerySource, MySqlSource, PgSource, Partition, Source};
use crate::transform::{self, SelectPlan};
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

struct SourceSetup {
    source_cols: Vec<ColumnType>,
    snapshot_max: Option<String>,
    partitions: Vec<Partition>,
}

/// Run one table transfer end to end.
pub async fn run_transfer(
    source_cfg: SourceConfig,
    ch: ClickHouseConfig,
    cfg: TransferConfig,
    progress: Option<ProgressCb>,
) -> Result<TransferResult> {
    cfg.validate()?;
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
        let sink = ClickHouseSink::new(ch)?;
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
    let sink = ClickHouseSink::new(ch)?;

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
        let last = read_last_watermark(&sink, &cfg).await?;
        tracing::info!(
            "incremental watermark on '{}': last synced={:?}, current source max={:?}",
            watermark,
            last,
            snapshot_max
        );
        let filter = match source.as_ref() {
            Source::Postgres(_) => build_watermark_filter_pg(watermark, last.as_deref(), snapshot_max.as_deref()),
            Source::MySql(_) => build_watermark_filter_mysql(watermark, last.as_deref(), snapshot_max.as_deref()),
            Source::BigQuery(_) => unreachable!("BigQuery is handled via the early return in run_transfer"),
        };
        (filter, snapshot_max)
    } else {
        (None, None)
    };

    // --- Ensure destination / staging tables exist. ---
    let target_table = prepare_target(&sink, &cfg, &plan.dest_columns).await?;

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
            match source.as_ref() {
                Source::Postgres(s) => {
                    transfer_partition_postgres(
                        s,
                        &sink,
                        &plan,
                        &cfg,
                        &counters,
                        &progress,
                        &target_table,
                        base_table.as_deref(),
                        base_query.as_deref(),
                        extra_filter.as_deref(),
                        part,
                        started,
                    )
                    .await
                }
                Source::MySql(s) => {
                    transfer_partition_mysql(
                        s,
                        &sink,
                        &plan,
                        &cfg,
                        &counters,
                        &progress,
                        &target_table,
                        base_table.as_deref(),
                        base_query.as_deref(),
                        extra_filter.as_deref(),
                        part,
                        started,
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
        sink.exchange_tables(&cfg.dest_table, &staging_name(&cfg.dest_table))
            .await?;
        sink.drop_table(&staging_name(&cfg.dest_table)).await?;
    }

    // --- Incremental: persist the new watermark. ---
    if cfg.mode == SyncMode::Incremental {
        if let Some(w) = &new_watermark {
            tracing::info!("persisting new watermark: {w}");
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
    sink: ClickHouseSink,
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
        let last = read_last_watermark(&sink, &cfg).await?;
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
        let filter = build_watermark_filter_bigquery(watermark, last.as_deref(), snapshot_max.as_deref());
        (filter, snapshot_max)
    } else {
        (None, None)
    };

    let target_table = prepare_target(&sink, &cfg, &plan.dest_columns).await?;

    tracing::info!(
        "etlhouse: transferring into {} via BigQuery Storage Read API, max_stream_count={}",
        target_table,
        cfg.parallelism
    );

    let counters = Counters::default();
    let mut iter = source
        .read_table::<google_cloud_bigquery::storage::row::Row>(
            &client,
            &table_ref,
            &plan.source_columns,
            row_restriction.as_deref(),
            cfg.parallelism as i32,
        )
        .await?;

    let mut batcher = BigQueryBatcher::new(&plan.dest_columns, cfg.batch_rows)?;
    let schema = batcher.schema();
    while let Some(row) = iter
        .next()
        .await
        .map_err(|e| EtlError::other(format!("bigquery row error: {e}")))?
    {
        if let Some(batch) = batcher.append_row(&row)? {
            flush(&sink, &target_table, &schema, &batch, &counters).await?;
            emit_progress(&counters, &progress, started);
        }
    }
    if let Some(batch) = batcher.finish()? {
        flush(&sink, &target_table, &schema, &batch, &counters).await?;
    }
    counters
        .rows_read
        .fetch_add(batcher.rows_total, Ordering::Relaxed);
    emit_progress(&counters, &progress, started);
    tracing::info!(
        "bigquery read complete: {} rows written",
        counters.rows_written.load(Ordering::Relaxed)
    );

    if cfg.mode == SyncMode::Full {
        tracing::info!("swapping staging table into '{}'", cfg.dest_table);
        sink.exchange_tables(&cfg.dest_table, &staging_name(&cfg.dest_table))
            .await?;
        sink.drop_table(&staging_name(&cfg.dest_table)).await?;
    }
    if cfg.mode == SyncMode::Incremental {
        if let Some(w) = &new_watermark {
            tracing::info!("persisting new watermark: {w}");
            persist_watermark(&sink, &cfg, w, counters.rows_written.load(Ordering::Relaxed)).await?;
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

    let snapshot_max = if let Some(w) = watermark {
        s.max_watermark(&control, base_table, base_query, w).await?
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

    let snapshot_max = if let Some(w) = watermark {
        s.max_watermark(&mut control, base_table, base_query, w)
            .await?
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
    sink: &ClickHouseSink,
    plan: &SelectPlan,
    cfg: &TransferConfig,
    counters: &Counters,
    progress: &Option<ProgressCb>,
    target_table: &str,
    base_table: Option<&str>,
    base_query: Option<&str>,
    extra_filter: Option<&str>,
    partition: Partition,
    started: Instant,
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

    let mut decoder = CopyDecoder::new(&plan.dest_columns, cfg.batch_rows)?;
    let schema = decoder.schema();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let batches = decoder.feed(&chunk)?;
        for batch in batches {
            flush(sink, target_table, &schema, &batch, counters).await?;
            emit_progress(counters, progress, started);
        }
    }
    if let Some(batch) = decoder.finish()? {
        flush(sink, target_table, &schema, &batch, counters).await?;
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
    emit_progress(counters, progress, started);
    tracing::info!("partition '{}' complete: {} rows", partition.label, decoder.rows_total);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn transfer_partition_mysql(
    source: &MySqlSource,
    sink: &ClickHouseSink,
    plan: &SelectPlan,
    cfg: &TransferConfig,
    counters: &Counters,
    progress: &Option<ProgressCb>,
    target_table: &str,
    base_table: Option<&str>,
    base_query: Option<&str>,
    extra_filter: Option<&str>,
    partition: Partition,
    started: Instant,
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

    let mut batcher = MySqlBatcher::new(&plan.dest_columns, cfg.batch_rows)?;
    let schema = batcher.schema();

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
            flush(sink, target_table, &schema, &batch, counters).await?;
            emit_progress(counters, progress, started);
        }
    }
    if let Some(batch) = batcher.finish()? {
        flush(sink, target_table, &schema, &batch, counters).await?;
    }

    counters
        .rows_read
        .fetch_add(batcher.rows_total, Ordering::Relaxed);
    emit_progress(counters, progress, started);
    tracing::info!("partition '{}' complete: {} rows", partition.label, batcher.rows_total);
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
                    tracing::info!("destination table '{}' does not exist; creating it", cfg.dest_table);
                    tracing::debug!("DDL: {sql}");
                    sink.execute(&sql).await?;
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
            let sql = ddl::create_table(sink.database(), &staging, dest_columns, cfg)?;
            tracing::debug!("DDL: {sql}");
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
                    tracing::info!("destination table '{}' does not exist; creating it", cfg.dest_table);
                    tracing::debug!("DDL: {sql}");
                    sink.execute(&sql).await?;
                } else {
                    return Err(EtlError::config(format!(
                        "destination table {} does not exist and create_if_missing=false",
                        cfg.dest_table
                    )));
                }
            } else {
                tracing::debug!("destination table '{}' already exists", cfg.dest_table);
            }
            sink.execute(&ddl::create_state_table(sink.database())).await?;
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
) -> Option<String> {
    let col = format!("\"{}\"", watermark.replace('"', "\"\""));
    build_watermark_filter(&col, last, snapshot_max)
}

fn build_watermark_filter_mysql(
    watermark: &str,
    last: Option<&str>,
    snapshot_max: Option<&str>,
) -> Option<String> {
    build_watermark_filter(&quote_my(watermark), last, snapshot_max)
}

/// BigQuery Standard SQL uses the same backtick identifier quoting as MySQL.
fn build_watermark_filter_bigquery(
    watermark: &str,
    last: Option<&str>,
    snapshot_max: Option<&str>,
) -> Option<String> {
    build_watermark_filter(&quote_my(watermark), last, snapshot_max)
}

fn build_watermark_filter(col: &str, last: Option<&str>, snapshot_max: Option<&str>) -> Option<String> {
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
