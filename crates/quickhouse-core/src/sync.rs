//! Transfer orchestration: schema resolution -> DDL -> parallel partitioned
//! stream/decode/insert -> (full) atomic swap or (incremental) watermark
//! persist. Each source engine (Postgres, MySQL, ClickHouse, ...) plugs in via the
//! [`Source`] enum; everything downstream of "decode into Arrow batches" is
//! source-agnostic.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
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

use crate::archive::{archive_object_key, build_store, ArchiveUploads, ArchiveWriter};
use crate::config::{
    ApiColumn, ArchiveConfig, DestinationConfig, ParquetCompression, SourceConfig, SourceShape,
    SyncMode, TransferConfig, TransferResult, TransferWarning, WarningKind, WatermarkSeed,
};
use crate::decode::CopyDecoder;
use crate::decode_api::{resolve_api_columns, ApiBatcher};
use crate::decode_bigquery::BigQueryBatcher;
use crate::decode_clickhouse::ChArrowDecoder;
use crate::decode_mysql::MySqlBatcher;
use crate::error::{EtlError, Result};
use crate::memory::{MemoryBudget, Reservation};
use crate::sink::{build_sink, Sink};
use crate::source::appsflyer::AppsFlyerSource;
use crate::source::clevertap::CleverTapSource;
use crate::source::clickhouse::quote_ch_table;
use crate::source::mysql::{quote_my, quote_my_table};
use crate::source::postgres::{quote_pg, quote_pg_table};
use crate::source::{
    BigQuerySource, ClickHouseSource, Keyset, MySqlSource, Partition, PgSource, Source,
};
use crate::source::{KeysetBound, ProbeCost};
use crate::transform::{self, SelectPlan};
use crate::types::bigquery::arrow_to_bigquery_type;
use crate::types::ColumnType;
use arrow_array::Array;
use arrow_schema::TimeUnit;
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

/// Per-chunk staging for a keyset-chunked read (`chunk_rows`) into a
/// destination that stages incremental loads for a `MERGE` (BigQuery).
///
/// A chunked read resumes correctly only if each chunk is *in the destination*
/// before its cursor is committed. On a direct-insert destination (ClickHouse)
/// the flush alone does that. Here each chunk is loaded into its own staging
/// table, merged, and the table dropped, and only then does the read loop
/// persist the cursor. A crash between the merge and the cursor re-reads that
/// one chunk, and merging it again is idempotent.
///
/// Each chunk's table gets a name never used before, cloned from the run's own
/// (empty) staging table. BigQuery can refuse streaming inserts into a table
/// recreated under a recently deleted name (see [`staging_name`]), so one
/// table truncated or recreated per chunk is not an option.
struct ChunkStager {
    sink: Arc<dyn Sink>,
    dest_table: String,
    /// The run's staging table. Never written: each chunk's table is cloned
    /// from it, and it is dropped with the run.
    template: String,
    key: Vec<String>,
    columns: Vec<ColumnType>,
    merge_prune_partition_by: Option<String>,
    merge_prune_key_range: bool,
    merge_prune_key_list_max: usize,
    dedup_order: Option<String>,
    warnings: Warnings,
    next: AtomicU64,
    /// The chunk table currently open, so a failed run can drop it too.
    open: Mutex<Option<String>>,
    /// Counts the rows each commit lands in the destination, where a commit
    /// run again would land them twice; see [`SendCtx::landed`]. `None` for a
    /// `MERGE`, which upserts.
    landed: Option<Arc<AtomicU64>>,
}

impl ChunkStager {
    /// Create the next chunk's staging table and return its name.
    async fn open(&self) -> Result<String> {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        let table = format!("{}_c{n}", self.template);
        self.sink
            .clone_table_structure(&table, &self.template)
            .await?;
        *self.open.lock().unwrap() = Some(table.clone());
        Ok(table)
    }

    /// Merge a loaded chunk into the destination and drop its table. A chunk
    /// that read nothing skips the merge, which would still scan the
    /// destination.
    async fn commit(&self, table: &str, rows: u64) -> Result<()> {
        if rows > 0 {
            promote_staged_incremental(
                self.sink.as_ref(),
                &self.dest_table,
                table,
                &self.key,
                &self.columns,
                self.merge_prune_partition_by.as_deref(),
                self.merge_prune_key_range,
                self.merge_prune_key_list_max,
                false,
                &None,
                rows,
                self.dedup_order.as_deref(),
                &self.warnings,
            )
            .await?;
            if let Some(landed) = &self.landed {
                landed.fetch_add(rows, Ordering::Relaxed);
            }
        } else {
            self.sink.drop_table(table).await?;
        }
        *self.open.lock().unwrap() = None;
        Ok(())
    }

    /// Best-effort drop of the chunk table a failed run left open.
    async fn cleanup(&self) {
        let open = self.open.lock().unwrap().take();
        if let Some(table) = open {
            cleanup_staging(&self.sink, &table).await;
        }
    }
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

/// Accumulates the largest watermark value actually read, across every
/// partition, so the cursor can be taken from the stream instead of from a
/// `MAX(watermark)` probe that would sequentially scan the whole table.
///
/// The maximum is folded as the raw Arrow integer (microseconds since epoch, or
/// days for a `Date32`) rather than as text. Comparing the rendered strings
/// would be subtly wrong: PostgreSQL's own `timestamp::text` drops trailing
/// zeros (`.100000` renders as `.1`), so lexicographic order over mixed-width
/// renderings does not match chronological order. Folding integers and
/// rendering once at the end avoids the question entirely.
struct WatermarkTracker {
    /// Index of the watermark column in each decoded batch. `SelectPlan`'s
    /// `source_columns` and `dest_columns` are parallel, so this position is
    /// valid even when `rename` gives the destination column another name.
    idx: usize,
    unit: WatermarkUnit,
    /// `i64::MIN` is the "nothing seen yet" sentinel. A real watermark can
    /// never be that value: as microseconds it is ~292,000 years before the
    /// epoch, outside every date type this crate can decode.
    max: AtomicI64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WatermarkUnit {
    /// `Timestamp(_, None)` — rendered as a naive `YYYY-MM-DD HH:MM:SS.ffffff`.
    NaiveMicros,
    /// `Timestamp(_, Some(tz))` on PostgreSQL — rendered with a `+00` offset,
    /// matching what `timestamptz::text` produces for a UTC session. Never used
    /// for MySQL, whose `DATETIME` literal can't carry that offset (see
    /// [`WatermarkTracker::new`]).
    UtcMicros,
    /// `Date32` — days since epoch, rendered `YYYY-MM-DD`.
    Days,
    /// A signed integer, or an unsigned one narrower than 64 bits, rendered as
    /// its decimal digits. Only ever bounded by a `MAX` from `source_table`
    /// (see [`table_max_eligible`]): a stream cursor needs a lookback, which
    /// only a temporal watermark takes.
    Int,
}

impl WatermarkUnit {
    /// The unit a column of type `arrow` folds in, if it can be folded. See
    /// [`WatermarkTracker::new`] for `utc_offset`, which follows the *source*
    /// column: a PostgreSQL `timestamptz` overridden to a naive destination
    /// type still compares as an instant, and a cursor without its `+00` is
    /// read in the session's own TimeZone, which quickhouse never sets.
    fn of(arrow: &DataType, utc_offset: bool) -> Option<Self> {
        Some(match arrow {
            DataType::Timestamp(TimeUnit::Microsecond, _) if utc_offset => WatermarkUnit::UtcMicros,
            DataType::Timestamp(TimeUnit::Microsecond, _) => WatermarkUnit::NaiveMicros,
            DataType::Date32 => WatermarkUnit::Days,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32 => WatermarkUnit::Int,
            _ => return None,
        })
    }
}

impl WatermarkUnit {
    /// A cursor literal in this unit. `None` when it doesn't parse, or carries
    /// a UTC offset where the unit has none (or the reverse).
    fn parse(self, cursor: &str) -> Option<i64> {
        if self == WatermarkUnit::Int {
            return cursor.trim().parse().ok();
        }
        let (micros, zoned) = parse_temporal_micros(cursor)?;
        match self {
            WatermarkUnit::NaiveMicros => (!zoned).then_some(micros),
            WatermarkUnit::UtcMicros => zoned.then_some(micros),
            WatermarkUnit::Days => (!zoned).then_some(micros.div_euclid(86_400_000_000)),
            WatermarkUnit::Int => unreachable!("returned above"),
        }
    }

    /// [`Self::parse`] for the cursor a run started from, which only ever
    /// floors a cursor moved back. A `timestamptz` watermark also takes a
    /// committed cursor without an offset as UTC: what 0.20.6 saved for one
    /// overridden to a naive type, rendered from the UTC instant. A seed is
    /// never passed here unless it parses in the unit itself (see
    /// [`cursor_floor`]): PostgreSQL reads an offset-less one in the session's
    /// zone.
    fn parse_floor(self, cursor: &str) -> Option<i64> {
        match self {
            WatermarkUnit::UtcMicros => parse_temporal_micros(cursor).map(|(micros, _)| micros),
            _ => self.parse(cursor),
        }
    }
}

/// The largest non-NULL value of an integer column, if it is one of `$t`.
macro_rules! fold_int_max {
    ($col:expr, $($t:ty),+) => {{
        let mut local = i64::MIN;
        $(
            if let Some(a) = $col.as_any().downcast_ref::<$t>() {
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        local = local.max(a.value(i) as i64);
                    }
                }
            }
        )+
        local
    }};
}

impl WatermarkTracker {
    /// `None` when the watermark is not a type whose maximum can be folded and
    /// rendered back into a comparable SQL literal, or is not in the projection.
    ///
    /// `utc_offset` says whether the source parses a tz-aware cursor's `+00`
    /// suffix. PostgreSQL's `timestamptz` does. MySQL doesn't: its `DATETIME`
    /// literal takes an offset only as `+hh:mm`, so `+00` makes the next run's
    /// bound NULL (non-strict `sql_mode`) or fails with 1525 (strict), and even
    /// `+00:00` is converted into the session time zone, shifting the cursor.
    /// The MySQL decoder reads a naive `DATETIME` as UTC, so the bare digits
    /// are the exact round trip — the same form `CAST(MAX(w) AS CHAR)` gives
    /// the MAX-probe path.
    fn new(watermark: &str, plan: &SelectPlan, utc_offset: bool) -> Option<Self> {
        let idx = plan.source_columns.iter().position(|c| c == watermark)?;
        let unit = WatermarkUnit::of(&plan.dest_columns.get(idx)?.arrow, utc_offset)?;
        Some(Self {
            idx,
            unit,
            max: AtomicI64::new(i64::MIN),
        })
    }

    /// Fold this batch's watermark column into the running maximum. NULLs are
    /// skipped — they are exactly the rows a `>` predicate never matches, and
    /// letting one influence the cursor would be meaningless.
    fn observe(&self, batch: &RecordBatch) {
        let Some(col) = batch.columns().get(self.idx) else {
            return;
        };
        let mut local = i64::MIN;
        match self.unit {
            WatermarkUnit::NaiveMicros | WatermarkUnit::UtcMicros => {
                let Some(a) = col
                    .as_any()
                    .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                else {
                    return;
                };
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        local = local.max(a.value(i));
                    }
                }
            }
            WatermarkUnit::Days => {
                let Some(a) = col.as_any().downcast_ref::<arrow_array::Date32Array>() else {
                    return;
                };
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        local = local.max(a.value(i) as i64);
                    }
                }
            }
            WatermarkUnit::Int => {
                use arrow_array::{
                    Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array, UInt32Array,
                    UInt8Array,
                };
                local = fold_int_max!(
                    col,
                    Int8Array,
                    Int16Array,
                    Int32Array,
                    Int64Array,
                    UInt8Array,
                    UInt16Array,
                    UInt32Array
                );
            }
        }
        if local > i64::MIN {
            self.max.fetch_max(local, Ordering::Relaxed);
        }
    }

    /// Whether any non-NULL watermark has been read.
    fn seen(&self) -> bool {
        self.max.load(Ordering::Relaxed) != i64::MIN
    }

    /// The cursor to save after a read bounded by `source_table`'s unfiltered
    /// MAX: the largest watermark actually read. `None` when nothing was read
    /// past `floor`, the cursor the run started from, so the cursor stays put
    /// rather than moving back into the lookback band it re-read.
    ///
    /// `rewind_secs` moves it back first, as [`Self::render_rewound`] does.
    fn advance_from(&self, floor: Option<&str>, rewind_secs: u64) -> Option<String> {
        let max = self.max.load(Ordering::Relaxed);
        if max == i64::MIN {
            return None;
        }
        let v = max.saturating_sub(self.rewind_step(rewind_secs));
        match floor.and_then(|f| self.unit.parse_floor(f)) {
            Some(f) if f >= v => None,
            _ => self.render_value(v),
        }
    }

    /// `rewind_secs` in the tracker's own unit: whole days for a `Date32`,
    /// rounded up, and nothing for an integer.
    fn rewind_step(&self, rewind_secs: u64) -> i64 {
        match self.unit {
            WatermarkUnit::NaiveMicros | WatermarkUnit::UtcMicros => i64::try_from(rewind_secs)
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000_000),
            WatermarkUnit::Days => i64::try_from(rewind_secs.div_ceil(86_400)).unwrap_or(i64::MAX),
            WatermarkUnit::Int => 0,
        }
    }

    /// Render the observed maximum as the SQL literal the next run's filter
    /// will compare against. `None` when no non-NULL row was read, which
    /// correctly leaves the cursor where it was.
    #[cfg(test)]
    fn render(&self) -> Option<String> {
        self.render_rewound(0, None)
    }

    /// [`Self::render`], moved back by `rewind_secs` (whole days for a
    /// `Date32` watermark, rounded up), but never below `floor`, the cursor
    /// this run started from, when that parses in the tracker's own unit.
    /// The floor never lifts the result above the observed maximum.
    fn render_rewound(&self, rewind_secs: u64, floor: Option<&str>) -> Option<String> {
        self.rewound(rewind_secs, floor)
            .and_then(|v| self.render_value(v))
    }

    /// [`stream_cursor_rewind_secs`] for a cursor in this tracker's unit. A
    /// `DATE` is the day of a change, up to a day before the change itself,
    /// so the lookback covers a day less of the read: with a lookback of a
    /// day or less, a read that crossed midnight is moved back a day (see
    /// [`Self::rewind_step`]). Otherwise a row read before midnight and
    /// changed again that day would sit below the next run's lower bound
    /// once a row changed after midnight set the cursor to the next day.
    fn stream_rewind_secs(&self, read_secs: f64, lookback_seconds: u64) -> u64 {
        let covered = match self.unit {
            WatermarkUnit::Days => lookback_seconds.saturating_sub(86_400),
            _ => lookback_seconds,
        };
        stream_cursor_rewind_secs(read_secs, covered)
    }

    /// [`Self::render_rewound`] before it is rendered.
    fn rewound(&self, rewind_secs: u64, floor: Option<&str>) -> Option<i64> {
        let max = self.max.load(Ordering::Relaxed);
        if max == i64::MIN {
            return None;
        }
        let mut v = max;
        if rewind_secs > 0 {
            v = v.saturating_sub(self.rewind_step(rewind_secs));
            if let Some(f) = floor.and_then(|f| self.unit.parse_floor(f)) {
                v = v.max(f.min(max));
            }
        }
        Some(v)
    }

    /// A cursor literal in the tracker's unit. `None` when it doesn't parse,
    /// or carries a UTC offset where the unit has none (or the reverse).
    fn parse(&self, cursor: &str) -> Option<i64> {
        self.unit.parse(cursor)
    }

    fn render_value(&self, v: i64) -> Option<String> {
        match self.unit {
            WatermarkUnit::NaiveMicros => chrono::DateTime::from_timestamp_micros(v)
                .map(|dt| dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
            WatermarkUnit::UtcMicros => chrono::DateTime::from_timestamp_micros(v).map(|dt| {
                dt.naive_utc()
                    .format("%Y-%m-%d %H:%M:%S%.6f+00")
                    .to_string()
            }),
            WatermarkUnit::Days => chrono::DateTime::from_timestamp(v * 86_400, 0)
                .map(|dt| dt.naive_utc().date().format("%Y-%m-%d").to_string()),
            WatermarkUnit::Int => Some(v.to_string()),
        }
    }
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

    /// The first collected warning of a kind in `kinds`, folded over its
    /// `(kind, column)` as [`Self::drain`] folds it, without taking anything.
    fn first_of(&self, kinds: &[WarningKind]) -> Option<TransferWarning> {
        if kinds.is_empty() {
            return None;
        }
        let raw = self.0.lock().unwrap();
        let first = raw.iter().find(|w| kinds.contains(&w.kind))?;
        let mut folded = first.clone();
        folded.count = raw
            .iter()
            .filter(|w| w.kind == first.kind && w.column == first.column)
            .map(|w| w.count)
            .sum();
        Some(folded)
    }

    /// Whether a warning of `kind` about `column` was already collected.
    fn contains(&self, kind: WarningKind, column: Option<&str>) -> bool {
        self.0
            .lock()
            .unwrap()
            .iter()
            .any(|w| w.kind == kind && w.column.as_deref() == column)
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

/// Which `fail_on_warnings` checkpoint a run is at: what it has made
/// permanent so far, which the error has to say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Before {
    /// Anything is written.
    Write,
    /// A full refresh's swap.
    Swap,
    /// An incremental run's `MERGE` or insert-select from staging.
    Merge,
    /// The cursor is saved, with the rows already in the destination.
    Cursor,
    /// A `chunk_rows` chunk is committed.
    Chunk,
    /// A chunked run's cursor is saved, its chunks already committed.
    CursorAfterChunks,
    /// Nothing: a run that writes straight into the destination and keeps no
    /// cursor it could leave unsaved (a frame), or one whose rows a later run
    /// would only write again (append, which saves its cursor first).
    Done,
}

impl Before {
    fn describe(self) -> &'static str {
        match self {
            Before::Write => {
                "Nothing was written to the destination, and the cursor was not saved."
            }
            Before::Swap => {
                "The destination is untouched: the staged rows were dropped before the swap."
            }
            Before::Merge => {
                "The destination is untouched (the staged rows were dropped before the merge), \
                 and the cursor was not saved."
            }
            Before::Cursor => {
                "The rows are already in the destination, but the cursor was not saved, so once \
                 the cause is fixed the next run reads the same range again (a \
                 ReplacingMergeTree or a MERGE converges on them)."
            }
            Before::Chunk => {
                "This chunk was not committed: its cursor was not saved, so the next run resumes \
                 at it (written straight into the destination, its rows are already there). \
                 Earlier chunks are committed."
            }
            Before::CursorAfterChunks => {
                "Every chunk is already in the destination with its resume marker saved, so the \
                 next run carries on past them; only the cursor that finishes the run was not \
                 saved."
            }
            Before::Done => {
                "The rows are already in the destination, which this run writes straight into, \
                 and its cursor (if it keeps one) is saved: reading them again would only write \
                 them twice."
            }
        }
    }
}

/// Fail the run if a warning kind `fail_on_warnings` names has been raised.
///
/// Called before every step that makes something permanent, because that is
/// the only place the documented "fail on this warning" can mean anything: a
/// caller raising after `sync()` returns gets one red run, while the cursor
/// already saved sends the retry past the rows the warning was about.
fn check_fatal_warnings(cfg: &TransferConfig, warnings: &Warnings, at: Before) -> Result<()> {
    match warnings.first_of(&cfg.fail_on_warnings) {
        Some(w) => Err(fatal_warning_error(&w, at, true)),
        None => Ok(()),
    }
}

/// [`check_fatal_warnings`] at a checkpoint of a run writing through `sink`,
/// with what the writes themselves raised taken in first. A Storage Write
/// stream that finalized with the wrong row count is fatal whatever
/// `fail_on_warnings` says when its rows are about to be merged or swapped in:
/// they are suspect, and still only in a staging table.
fn check_fatal(
    cfg: &TransferConfig,
    sink: &dyn Sink,
    warnings: &Warnings,
    at: Before,
) -> Result<()> {
    for w in sink.take_write_warnings() {
        warnings.push(w);
    }
    if matches!(at, Before::Swap | Before::Merge | Before::Chunk) {
        if let Some(w) = warnings.first_of(&[WarningKind::StorageWriteCountMismatch]) {
            return Err(fatal_warning_error(&w, at, false));
        }
    }
    check_fatal_warnings(cfg, warnings, at)
}

/// The error a fatal warning stops the run with. `requested` says the caller
/// named the kind in `fail_on_warnings`.
fn fatal_warning_error(w: &TransferWarning, at: Before, requested: bool) -> EtlError {
    let column = w
        .column
        .as_deref()
        .map(|c| format!(" on column '{c}'"))
        .unwrap_or_default();
    // Carried over from the attempts before this one, it describes what they
    // wrote, which stays where it is.
    let state = if w.kind == WarningKind::RetriedAfterPartialWrite {
        "This attempt has written nothing; what the failed attempts before it wrote stays in \
         the destination."
    } else {
        at.describe()
    };
    EtlError::other(format!(
        "{by}{kind}{column} (count {count}) stopped the run. {state} The warning: {message}",
        by = if requested { "fail_on_warnings: " } else { "" },
        kind = w.kind.as_str(),
        count = w.count,
        message = w.message,
    ))
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
    /// `Some` whenever the destination — ClickHouse or BigQuery — has an
    /// `archive` configured, for every source shape. See
    /// `ArchiveRunInfo::writer_for`.
    archive: Option<Arc<ArchiveRunInfo>>,
    /// `Some` when `read_max_rows_per_sec` is set: a single limiter shared by
    /// all partition tasks, so the cap is an aggregate across the whole read.
    throttle: Option<Arc<ReadThrottle>>,
    /// Run-scoped warning collector, shared by every partition so per-column
    /// coercions from concurrent readers fold into one entry per column.
    warnings: Warnings,
    /// `Some` when the incremental cursor is taken from the read stream rather
    /// than from a `MAX(watermark)` probe. Shared by every partition, so the
    /// maximum folds across all of them.
    watermark_max: Option<Arc<WatermarkTracker>>,
    /// `Some` for a chunked read into a destination that merges staged
    /// incremental loads: each chunk then goes through its own staging table.
    chunk_stager: Option<Arc<ChunkStager>>,
    /// `Some` when `target_table` is the destination itself: counts the rows
    /// that land there, which a retried attempt writes again (see
    /// [`run_transfer`]).
    landed: Option<Arc<AtomicU64>>,
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
    /// Parallel to `batches`: each one's size, so [`Self::truncate`] can give
    /// back exactly what it drops.
    sizes: Vec<usize>,
    bytes: usize,
    /// Flush once the group reaches this many bytes of real Arrow memory
    /// (measured like `batch_bytes` and `max_memory_bytes`, not post-compression).
    target: usize,
    /// How many times [`Self::take`] emptied the buffer: what tells a
    /// [`BufferMark`] whether the batches it counted are still here.
    takes: u64,
    /// Don't ask to be sent when full: the owner sends between statements
    /// instead ([`ReadOut::between_windows`]). Only memory pressure still
    /// sends mid-statement, which [`SendCtx::push_batch`] needs to avoid a
    /// deadlock.
    deferred: bool,
}

/// A point in an [`InsertBuffer`] to roll back to: see [`InsertBuffer::truncate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BufferMark {
    takes: u64,
    len: usize,
}

impl InsertBuffer {
    fn new(target: usize) -> Self {
        InsertBuffer {
            batches: Vec::new(),
            reservations: Vec::new(),
            sizes: Vec::new(),
            bytes: 0,
            target,
            takes: 0,
            deferred: false,
        }
    }

    /// Whether the buffer holds enough to be worth an insert.
    fn full(&self) -> bool {
        !self.batches.is_empty() && self.bytes >= self.target
    }

    fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Add a batch and its reservation; `true` once the group is worth sending.
    fn push(&mut self, batch: RecordBatch, reservation: Reservation, size: usize) -> bool {
        self.batches.push(batch);
        self.reservations.push(reservation);
        self.sizes.push(size);
        self.bytes += size;
        !self.deferred && self.bytes >= self.target
    }

    /// Take everything buffered, leaving the buffer empty.
    fn take(&mut self) -> (Vec<RecordBatch>, Vec<Reservation>) {
        self.bytes = 0;
        self.takes += 1;
        self.sizes.clear();
        (
            std::mem::take(&mut self.batches),
            std::mem::take(&mut self.reservations),
        )
    }

    fn mark(&self) -> BufferMark {
        BufferMark {
            takes: self.takes,
            len: self.batches.len(),
        }
    }

    /// Drop everything buffered since `mark`, releasing its memory: the rows
    /// of a window that failed and is read again. When the buffer was sent
    /// since, everything in it now came after `mark`. What was sent can't be
    /// taken back, which is no different from a window with a buffer of its
    /// own.
    fn truncate(&mut self, mark: BufferMark) {
        let keep = if mark.takes == self.takes {
            mark.len.min(self.batches.len())
        } else {
            0
        };
        self.batches.truncate(keep);
        self.reservations.truncate(keep);
        self.sizes.truncate(keep);
        self.bytes = self.sizes.iter().sum();
    }
}

/// Where one source statement's rows go, and the connection it reads on.
///
/// A partition read in one pass has its own. A windowed sweep shares one
/// across all its windows: before, each window opened its own connection (a
/// TLS handshake against a remote source) and flushed its own buffer, so a
/// sweep of N windows cost N connections and N inserts, which on ClickHouse
/// is N new parts for merges to clean up even when the windows return a
/// handful of rows between them (measured: 18 parts and 20 MySQL connections
/// for a sweep a single pass did with 2 and 4). The buffer now flushes on its
/// usual size limit only, and the connection is replaced only after a window
/// fails.
struct ReadOut<C> {
    conn: Option<C>,
    sends: JoinSet<Result<()>>,
    insert_buf: InsertBuffer,
    /// The decoded schema, once a statement has run: what the final flush
    /// sends under.
    schema: Option<SchemaRef>,
}

impl<C> ReadOut<C> {
    fn new(cfg: &TransferConfig) -> Self {
        ReadOut {
            conn: None,
            sends: JoinSet::new(),
            insert_buf: InsertBuffer::new(cfg.insert_bytes),
            schema: None,
        }
    }

    /// Between two windows of a sweep: send the buffer if it is full, and
    /// surface an insert that failed, rather than at the end of the sweep.
    async fn between_windows(&mut self, ctx: &SendCtx) -> Result<()> {
        if self.insert_buf.full() {
            if let Some(schema) = self.schema.clone() {
                ctx.flush(&mut self.sends, &mut self.insert_buf, schema)
                    .await;
            }
        }
        reap(&mut self.sends, false).await
    }

    /// Send what is still buffered and wait for every insert to land.
    async fn finish(mut self, ctx: &SendCtx) -> Result<()> {
        if let Some(schema) = self.schema.take() {
            ctx.flush(&mut self.sends, &mut self.insert_buf, schema)
                .await;
        }
        reap(&mut self.sends, true).await
    }
}

impl SendCtx {
    /// The context one keyset chunk writes through: this one, or for a staged
    /// destination a copy aimed at a fresh per-chunk staging table.
    async fn begin_chunk(&self) -> Result<SendCtx> {
        let Some(stager) = &self.chunk_stager else {
            return Ok(self.clone());
        };
        let table = stager.open().await?;
        Ok(SendCtx {
            target_table: Arc::new(table),
            // Staged: the chunk lands when the stager commits it.
            landed: None,
            ..self.clone()
        })
    }

    /// A chunk's cursor is committed: what landed up to here isn't written
    /// again by a retry, which resumes past it, so it stops counting.
    fn chunk_committed(&self) {
        if let Some(landed) = &self.landed {
            landed.store(0, Ordering::Relaxed);
        }
        if let Some(landed) = self.chunk_stager.as_ref().and_then(|s| s.landed.as_ref()) {
            landed.store(0, Ordering::Relaxed);
        }
    }

    /// Land a chunk written through `chunk` (from [`Self::begin_chunk`]) in
    /// the destination, once its inserts are durable and before its cursor is
    /// committed. Nothing to do for a direct-insert destination.
    async fn end_chunk(&self, chunk: &SendCtx, rows: u64) -> Result<()> {
        match &self.chunk_stager {
            Some(stager) => stager.commit(&chunk.target_table, rows).await,
            None => Ok(()),
        }
    }

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
        // Fold the watermark before the batch is handed on: this is the one
        // point every decoded batch from every partition passes through.
        if let Some(t) = &self.watermark_max {
            t.observe(&batch);
        }
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
            if let Some(landed) = &ctx.landed {
                landed.fetch_add(rows, Ordering::Relaxed);
            }
            // Progress fires on *completion*, so rows_written reflects rows
            // actually landed in the destination, not merely decoded.
            emit_progress(&ctx.counters, &ctx.progress, ctx.started);
            Ok(())
        });
    }
}

/// Static per-run info every partition needs to open its own archive writer —
/// the object-store client and naming info are shared (built once per
/// transfer, mirroring `build_sink`); only the partition label varies.
struct ArchiveRunInfo {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    dest_table: String,
    run_date: String,
    run_id: String,
    compression: ParquetCompression,
    /// Backend label ("s3"/"gcs") for error messages only — the store itself
    /// is a `dyn ObjectStore` and no longer says which cloud it talks to.
    kind: &'static str,
    /// Every upload this attempt starts; see [`run_transfer_attempt`].
    uploads: ArchiveUploads,
}

impl ArchiveRunInfo {
    fn writer_for(&self, partition_label: &str, schema: SchemaRef) -> Result<ArchiveWriter> {
        let key = archive_object_key(
            &self.prefix,
            &self.dest_table,
            &self.run_date,
            &self.run_id,
            partition_label,
        );
        ArchiveWriter::new(
            self.store.clone(),
            key,
            schema,
            self.compression,
            self.kind,
            &self.uploads,
        )
    }
}

/// The archive side of a keyset-chunked (`chunk_rows`) read: one Parquet file
/// per chunk, finished before that chunk's cursor is committed.
///
/// A chunked read commits its cursor after every chunk, and a run that fails
/// part-way resumes at the next one. With one file for the whole run, that
/// lost rows: the file was finished only after the last chunk, so a failed
/// run left no object, and the run that resumed archived only the chunks it
/// read itself. The destination held every row and the archive silently did
/// not — measured on GCS, a run killed after 4 of 20 chunks and resumed left
/// 2,000,000 of 10,000,000 rows out of the backup. A file per chunk keeps the
/// archive whole across a resume: every committed chunk is already an object.
struct ChunkArchive {
    info: Option<Arc<ArchiveRunInfo>>,
    schema: SchemaRef,
    /// Index of the current chunk within this run, which names its file.
    chunk: usize,
    writer: Option<ArchiveWriter>,
}

impl ChunkArchive {
    fn new(info: Option<Arc<ArchiveRunInfo>>, schema: SchemaRef) -> Self {
        Self {
            info,
            schema,
            chunk: 0,
            writer: None,
        }
    }

    /// Append `batch` to the current chunk's file, opening the file on the
    /// chunk's first row, so a chunk that reads nothing writes nothing.
    async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let Some(info) = &self.info else {
            return Ok(());
        };
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let writer = match self.writer.take() {
            Some(w) => w,
            None => {
                let label = format!("keyset-{:05}", self.chunk);
                info.writer_for(&label, self.schema.clone())?
            }
        };
        self.writer.insert(writer).write(batch).await
    }

    /// Finish the current chunk's file. Call once the chunk has landed in the
    /// destination and before its cursor is committed, so a chunk the cursor
    /// counts as done is always in the archive too.
    async fn finish_chunk(&mut self) -> Result<()> {
        if let Some(w) = self.writer.take() {
            w.close().await?;
        }
        self.chunk += 1;
        Ok(())
    }
}

/// Build the shared archive info for one transfer run, or `None` if archival
/// isn't configured. Building the object-store client here — before any
/// source connection is opened — means a bad archive config (e.g. a missing
/// bucket, or unparseable service-account JSON) fails fast rather than being
/// discovered mid-transfer.
fn build_archive_run_info(
    archive: Option<ArchiveConfig>,
    dest_table: &str,
    uploads: &ArchiveUploads,
) -> Result<Option<Arc<ArchiveRunInfo>>> {
    let Some(cfg) = archive else {
        return Ok(None);
    };
    let store = build_store(&cfg)?;
    let now = Utc::now();
    Ok(Some(Arc::new(ArchiveRunInfo {
        store,
        prefix: cfg.prefix().to_string(),
        dest_table: dest_table.to_string(),
        run_date: now.format("%Y-%m-%d").to_string(),
        // Whole-second resolution (`now.timestamp()`) let two runs into the
        // same `dest_table` starting in the same second — a quick backfill
        // loop, or two concurrent transfers — write the identical object key
        // (`.../run={run_id}/part-{label}.parquet`, and every run's part
        // labels are deterministic). `object_store`'s `put` overwrites with
        // no error, so the loser's whole backup silently vanished. `new_run_id`
        // (nanosecond wall clock) is what the staging table name already uses
        // for exactly this reason.
        run_id: new_run_id(),
        compression: cfg.compression(),
        kind: cfg.kind(),
        uploads: uploads.clone(),
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
    /// `snapshot_max` is `source_table`'s own, unfiltered MAX rather than
    /// `source_query`'s: see [`table_max_eligible`]. The read is bounded by
    /// it, and the cursor saved is the largest watermark the read returned.
    max_from_table: bool,
    partitions: Vec<Partition>,
    /// The `MAX(watermark)` probe was too costly to run, so this run reads with
    /// no frozen upper bound and takes its cursor from the stream instead. See
    /// [`plan_watermark_probes`].
    stream_max_cursor: bool,
    /// `Some` when the read's own filter plans as a sequential scan, so it must
    /// be swept in bounded key windows to survive a standby's conflict window.
    window: Option<WindowPlan>,
    /// ClickHouse only: the watermark's ClickHouse type, which types the
    /// filter's cursor literals (see `build_watermark_filter_clickhouse`).
    watermark_type: Option<String>,
    /// PostgreSQL only: the `chunk_rows` keyset column resolved as nullable
    /// (every `source_query` column does) but the query proves it NOT NULL
    /// (see `PgSource::result_column_not_null`).
    keyset_not_null: bool,
    /// The connection setup probed on, for the checks that need the cursor.
    control: Option<ControlConn>,
}

/// The connection setup probes the source on, kept for the checks that run
/// once the cursor is known (the lower-bound guard, the cursor-ahead check)
/// so they don't each open their own: a connection is a TLS handshake over a
/// WAN. Dropped before the read starts.
enum ControlConn {
    Postgres(tokio_postgres::Client),
    MySql(mysql_async::Conn),
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
    let landed = Arc::new(AtomicU64::new(0));
    let first_started = Instant::now();
    if max_attempts <= 1 {
        // Fast path: byte-identical to the pre-retry behavior — one call, one
        // context wrap, no clones.
        let attempt = Attempt {
            landed,
            carried: vec![],
            first_started,
        };
        return run_transfer_attempt(source_cfg, dest, cfg, progress, on_staged, attempt)
            .await
            .map_err(|e| e.context(table_context));
    }
    // Retry the WHOLE transfer on a transient source error. Each attempt runs
    // from a clean slate (fresh per-run staging; the watermark advances only on
    // success), so a full refresh stays atomic and an incremental run re-reads
    // the same window rather than skipping rows. Sink/write blips are retried
    // separately at the insert layer, so those never re-read the source here.
    //
    // What a failed attempt already wrote into the destination is written
    // again, so it is counted, and the attempts after it carry a
    // `retried_after_partial_write` warning (one a `fail_on_warnings` can stop
    // them on).
    let mut attempt = 1u32;
    let mut partial = 0u64;
    let mut carried = Vec::new();
    loop {
        landed.store(0, Ordering::Relaxed);
        let result = run_transfer_attempt(
            source_cfg.clone(),
            dest.clone(),
            cfg.clone(),
            progress.clone(),
            on_staged.clone(),
            Attempt {
                landed: landed.clone(),
                carried: carried.clone(),
                first_started,
            },
        )
        .await;
        match result {
            Ok(mut r) => {
                r.rows_written_failed_attempts = partial;
                return Ok(r);
            }
            Err(e) if attempt < max_attempts && e.is_transient_source() => {
                let delay = crate::sink::backoff_delay(attempt);
                tracing::warn!(
                    "{table_context}: attempt {attempt}/{max_attempts} failed with a transient \
                     source error ({e}); retrying the whole transfer in {delay:?}"
                );
                let written = landed.load(Ordering::Relaxed);
                if written > 0 {
                    partial += written;
                    carried = vec![partial_write_warning(&cfg, attempt, partial)];
                }
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
            Err(e) => return Err(e.context(table_context)),
        }
    }
}

/// `retried_after_partial_write`: attempts up to `attempt` failed after
/// writing `rows` rows into the destination, and the retry writes them again.
fn partial_write_warning(cfg: &TransferConfig, attempt: u32, rows: u64) -> TransferWarning {
    let message = format!(
        "attempt {attempt} of the transfer into '{dest}' failed after writing {rows} row(s) \
         into it, and retry_max_attempts runs the transfer again from the start, so those rows \
         are written a second time. A ReplacingMergeTree collapses the copies at its next \
         merge (count() runs high until then); an engine that keeps duplicates keeps them. \
         Retries stage each attempt instead for a ClickHouse MergeTree that keeps duplicates, \
         or set chunk_rows to resume rather than restart.",
        dest = cfg.dest_table,
    );
    tracing::warn!("{message}");
    TransferWarning {
        kind: WarningKind::RetriedAfterPartialWrite,
        column: None,
        count: rows,
        sample: None,
        message,
    }
}

/// One attempt at a transfer. When it fails, every archive upload it started
/// and never finished is aborted before the error is returned, so a failed run
/// leaves no incomplete multipart upload behind in the bucket. See
/// [`ArchiveUploads`] for why this is awaited here rather than left to a `Drop`.
async fn run_transfer_attempt(
    source_cfg: SourceConfig,
    dest: DestinationConfig,
    cfg: TransferConfig,
    progress: Option<ProgressCb>,
    on_staged: Option<StagedValidationCb>,
    attempt: Attempt,
) -> Result<TransferResult> {
    let uploads = ArchiveUploads::default();
    let result = run_transfer_impl(
        source_cfg, dest, cfg, progress, on_staged, &uploads, attempt,
    )
    .await;
    if result.is_err() {
        uploads.abort_unfinished().await;
    }
    result
}

/// What one attempt of [`run_transfer`] shares with the others.
struct Attempt {
    /// Counts the rows this attempt writes into the destination itself (not a
    /// staging table).
    landed: Arc<AtomicU64>,
    /// Warnings from the attempts before it.
    carried: Vec<TransferWarning>,
    /// When the first attempt started, which a chunked read's resume markers
    /// count from: see [`ChunkPlan::marker_upper`].
    first_started: Instant,
}

async fn run_transfer_impl(
    source_cfg: SourceConfig,
    dest: DestinationConfig,
    cfg: TransferConfig,
    progress: Option<ProgressCb>,
    on_staged: Option<StagedValidationCb>,
    uploads: &ArchiveUploads,
    attempt: Attempt,
) -> Result<TransferResult> {
    let Attempt {
        landed,
        carried,
        first_started,
    } = attempt;
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
    // Each source shape has its own rules: an API source has no
    // source_table/source_query and rejects a few DB-only knobs, and a frame
    // additionally has no source to filter, partition, pace or resume against.
    match source_cfg.shape() {
        SourceShape::Api => cfg.validate_api()?,
        SourceShape::Frame => cfg.validate_frame()?,
        SourceShape::Db => cfg.validate()?,
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
    // reports only the successful attempt's warnings, plus what the retry loop
    // carries over from the failed ones.
    let warnings = Warnings::default();
    for w in carried {
        warnings.push(w);
    }

    // A backup configured on the source descriptor is a backup that never
    // happens: `archive` is read off the destination only. Warn rather than
    // error, because one descriptor legitimately serves as both source and
    // destination in a self-copy — but never let it pass in silence.
    if cfg.source_archive_ignored {
        let message = format!(
            "archive= was set on the {} source descriptor, where it does nothing — archiving \
             is a write-path option, read from the destination only. NO BACKUP WILL BE WRITTEN \
             for this run unless archive= is also set on the destination descriptor.",
            source_cfg.kind(),
        );
        tracing::warn!("{message}");
        warnings.push(TransferWarning {
            kind: WarningKind::IgnoredSourceArchive,
            column: None,
            count: 0,
            sample: None,
            message,
        });
    }

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

    // --- Optional Parquet data-lake archival (either destination). ---
    // Extracted (and cloned) before `build_sink(dest)` consumes `dest` below —
    // in either branch — and built once here so a bad archive config (e.g. a
    // missing bucket) fails fast rather than being discovered mid-transfer.
    let archive_cfg = dest.archive().cloned();
    let archive_info = build_archive_run_info(archive_cfg, &cfg.dest_table, uploads)?;

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
            landed,
        )
        .await;
    }

    // HTTP API sources (CleverTap/AppsFlyer/HttpApi): a declared schema +
    // paginated fetch into either sink (BigQuery or ClickHouse, since
    // `bc1ab45`) — a separate flow from the DB partition machinery. Like the
    // BigQuery-source flow it has no discrete partitions, so it archives a
    // single "all" file per run.
    if source_cfg.is_api() {
        let sink = build_sink(dest).await?;
        return run_transfer_api(
            source_cfg,
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

    // An in-memory Arrow frame: the schema arrives with the data and there is
    // nothing to connect to, so — like the API flow above — this returns before
    // any of the partition machinery is built, and archives one "all" file.
    if let SourceConfig::Arrow(frame) = &source_cfg {
        let frame = frame.clone();
        let sink = build_sink(dest).await?;
        return run_transfer_frame(
            frame,
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
            my.utc_session,
        )),
        SourceConfig::ClickHouse(ch) => Source::ClickHouse(ClickHouseSource::new(ch)?),
        SourceConfig::BigQuery(_) => unreachable!("handled via early return above"),
        SourceConfig::CleverTap(_) | SourceConfig::AppsFlyer(_) | SourceConfig::HttpApi(_) => {
            unreachable!("API sources handled via early return above")
        }
        SourceConfig::Arrow(_) => unreachable!("frame sources handled via early return above"),
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
    // So is a run whose retries would duplicate what a failed attempt wrote
    // (see `Sink::stage_for_retries`): each attempt then writes its own
    // staging table, dropped when it fails.
    let stage_for_retries = cfg.mode == SyncMode::Incremental
        && cfg.retry_max_attempts > 1
        && !sink.requires_staging_for_incremental()
        && sink.stage_for_retries(&cfg.dest_table, &cfg).await?;
    if stage_for_retries {
        tracing::info!(
            "'{}' keeps duplicate rows, so with retry_max_attempts={} each attempt is staged and \
             moved into it at the end: a failed attempt leaves nothing behind",
            cfg.dest_table,
            cfg.retry_max_attempts
        );
    }
    let force_stage_incremental = cfg.mode == SyncMode::Incremental
        && !sink.requires_staging_for_incremental()
        && (on_staged.is_some()
            || stage_for_retries
            || (cfg.delete_stale_in_window && !sink.deletes_stale_within_merge()));
    if cfg.delete_stale_in_window && !sink.supports_row_delete() {
        return Err(EtlError::config(
            "delete_stale_in_window is not supported by this destination (it cannot delete \
             individual rows)",
        ));
    }

    // Keyset resumable reads land each chunk in the destination before reading
    // the next: straight in, or through its own staging table and MERGE on a
    // destination that stages incremental loads (see `ChunkStager`). Either
    // way no staging table ever holds the whole run, so it can't be gated...
    if cfg.chunk_rows.is_some() && on_staged.is_some() {
        return Err(EtlError::config(
            "data-quality validation (validate=) is not supported together with chunk_rows: \
             each chunk lands in the destination before the next is read, so no single staging \
             table holds the run to gate, and a gate failing on a later chunk would leave the \
             earlier ones applied. Drop chunk_rows to validate, or validate downstream.",
        ));
    }
    // (Nor can the window-scoped delete be split across chunks: see
    // `TransferConfig::validate`.)

    let base_table = cfg.source_table.clone();
    let base_query = cfg.source_query.clone();
    let watermark = cfg.watermark.clone();

    // The committed cursor from the last fully-successful run (None on a first
    // run), and an in-progress chunk-resume marker (frozen upper + last durable
    // cursor; chunked reads only, and only if a prior run was cut short). Read
    // before the source is probed, because the probes can use the cursor: see
    // `partitions_above` and `watermark_bounds_the_key`.
    let (committed, resume) = if cfg.mode == SyncMode::Incremental {
        let committed = sink.read_last_watermark(&cfg).await?;
        let resume = if cfg.chunk_rows.is_some() {
            sink.read_chunk_state(&cfg).await?
        } else {
            None
        };
        (committed, resume)
    } else {
        (None, None)
    };
    // The lower bound the read starts from, where it is known before the MAX
    // probe: a `skip_to_max` seed is the MAX itself.
    let cursor_hint = committed.clone().or_else(|| match &cfg.seed_watermark {
        WatermarkSeed::Value(v) => Some(v.clone()),
        _ => None,
    });

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
                cursor_hint.as_deref(),
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
                cursor_hint.as_deref(),
                &warnings,
            )
            .await?
        }
        Source::ClickHouse(s) => {
            setup_clickhouse(
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
        mut snapshot_max,
        mut max_from_table,
        partitions,
        stream_max_cursor,
        window: window_plan,
        watermark_type,
        keyset_not_null,
        mut control,
    } = setup;
    tracing::info!(
        "resolved {} source column(s); computed {} partition(s) for parallel read",
        source_cols.len(),
        partitions.len()
    );

    // The ClickHouse source is the only one whose decode path cannot invent a
    // NULL (see `transform::plan_with`), so it is the only one whose NOT NULL
    // date/decimal columns stay NOT NULL at the destination.
    let source_may_coerce = !matches!(source.as_ref(), Source::ClickHouse(_));
    // A column that declares its precision defaults to that exact decimal, but
    // never against a destination column that already holds something else —
    // so the destination is only consulted when such a column exists.
    let existing = if source_cols.iter().any(|c| c.declared_decimal.is_some()) {
        existing_columns(sink.as_ref(), &cfg.dest_table).await?
    } else {
        transform::ExistingColumns::NoTable
    };
    let mut plan: SelectPlan = transform::plan_for_destination(
        &source_cols,
        &cfg,
        sink.dest_kind(),
        source_may_coerce,
        &existing,
    )?;
    if let transform::ExistingDecimals::Mixed { decimal, float } =
        transform::existing_decimals(&source_cols, &cfg, sink.dest_kind(), &existing)
    {
        warn_mixed_decimals(&cfg.dest_table, &decimal, &float, &warnings);
    }
    if matches!(source.as_ref(), Source::Postgres(_)) {
        crate::source::postgres::pin_transformed_wire_types(&mut plan);
    }
    let plan = Arc::new(plan);

    // When the `MAX(watermark)` probe was skipped as too costly, the cursor has
    // to come from the rows this run actually reads. Built here because it
    // needs the resolved plan (for the watermark's column position and type),
    // and consumed after the streaming phase.
    //
    // The tracker folds the *decoded, projected* values — whatever the SELECT
    // actually emits as `watermark`, which is the `column_transforms` expression
    // when one is registered for this column. The incremental filter, on the
    // other hand, always binds to the *raw* column (or `watermark_source_expr`,
    // itself another raw expression) — never to `column_transforms`, because a
    // transform can be arbitrary SQL a WHERE cannot generally invert. So a
    // stream-derived cursor for a transformed watermark would be persisted in
    // one value domain and compared against the other on the next run,
    // silently skipping whatever the transform's range doesn't overlap between
    // runs. `build_chunk_plan` already refuses this same combination for the
    // keyset column; the stream watermark needs the identical guard.
    if stream_max_cursor
        && cfg
            .watermark
            .as_deref()
            .is_some_and(|w| cfg.column_transforms.contains_key(w))
    {
        return Err(EtlError::config(format!(
            "watermark column '{}' cannot be in column_transforms together with a stream-derived \
             cursor: the MAX(watermark) probe was skipped as too costly (see the \
             unindexed_watermark warning above), so the cursor is taken from the transformed \
             values this run reads, but the incremental filter always compares against the raw \
             column (or watermark_source_expr, itself a raw expression) — never against \
             column_transforms, which can be arbitrary SQL a WHERE cannot generally invert. Add \
             an index on the raw watermark column so the MAX probe runs and this path isn't \
             needed, or set probe_max_cost=0 to force it unconditionally.",
            cfg.watermark.as_deref().unwrap_or("?"),
        )));
    }
    // `watermark_source_expr` is the other way the filter and the projection
    // part: the filter reads the raw expression, while a stream cursor folds
    // the projected column, which is whatever `source_query` makes of it.
    if stream_max_cursor {
        if let Some(expr) = cfg.watermark_source_expr.as_deref() {
            return Err(EtlError::config(format!(
                "watermark_source_expr cannot be used with a stream-derived cursor: the \
                 MAX(watermark) probe was skipped as too costly (see the unindexed_watermark \
                 warning above), so the cursor would be the largest '{}' value this run reads, \
                 while the incremental filter compares against watermark_source_expr ({expr}). \
                 Whenever the projection transforms the column (a time-zone shift, a cast) \
                 those are different values, and a cursor from one compared against the other \
                 silently skips or re-reads rows. Add an index on what watermark_source_expr \
                 reads so the MAX probe runs, or set probe_max_cost=0 to force it \
                 unconditionally.",
                cfg.watermark.as_deref().unwrap_or("?"),
            )));
        }
    }
    let watermark_tracker = match (
        stream_max_cursor || max_from_table,
        cfg.watermark.as_deref(),
    ) {
        (true, Some(w)) => {
            // PostgreSQL parses a `+00` cursor, and a `timestamptz` cursor
            // needs one whatever its destination type: see `WatermarkUnit::of`.
            let utc_offset = matches!(source.as_ref(), Source::Postgres(_))
                && watermark_pg_is_tz_aware(w, &source_cols);
            WatermarkTracker::new(w, &plan, utc_offset).map(Arc::new)
        }
        _ => None,
    };
    if max_from_table && watermark_tracker.is_none() {
        // The watermark can't be folded from the rows read (left out of the
        // projection, or overridden to a type with no order to fold), and the
        // table's MAX is no cursor: a row the filter excludes could carry it
        // past rows not read yet. Probe source_query's own MAX instead.
        tracing::info!(
            "watermark '{}' can't be folded from the rows read, so source_query's own MAX is \
             probed for the cursor instead of source_table's",
            cfg.watermark.as_deref().unwrap_or("?")
        );
        snapshot_max = query_max_watermark(source.as_ref(), control.as_mut(), &cfg).await?;
        max_from_table = false;
    }
    if stream_max_cursor && watermark_tracker.is_none() {
        // Nothing can fold this watermark into a cursor (excluded from the
        // projection, or a type with no orderable Arrow representation). The
        // planner said MAX is expensive, but running with no cursor at all
        // would stall the pipeline permanently, so pay for it.
        return Err(EtlError::config(format!(
            "watermark column '{}' could not be tracked through the read stream, and the \
             MAX(watermark) probe was skipped as too costly. Either include the watermark \
             column in the transfer, or set probe_max_cost=0 to probe unconditionally.",
            cfg.watermark.as_deref().unwrap_or("?")
        )));
    }

    // --- Incremental: build the "since last run" filter from the watermark
    // state read above, and (for chunked reads) the keyset resume plan. ---
    let mut cursor_plan = CursorPlan::default();
    let (extra_filter, mut new_watermark, chunk_plan, cursor_check) = if cfg.mode
        == SyncMode::Incremental
    {
        let watermark = cfg.watermark.as_ref().unwrap();
        let (pinned_upper, start_cursor) = resume_bounds(resume.as_ref());
        cursor_plan.pinned = pinned_upper.is_some();
        // Resume freezes the upper bound to the interrupted run's snapshot so it
        // reads the same window; a fresh run uses the live source MAX. (For a
        // non-chunked run `pinned_upper` is always None, so this == snapshot_max
        // and behavior is unchanged.)
        let effective_upper = pinned_upper.or_else(|| snapshot_max.clone());
        // `skip_to_max` (WatermarkSeed::CurrentMax) exists precisely for a
        // table where a full first pull "would be a doomed waste" (its own
        // doc comment) — almost always because it is too big to probe
        // cheaply, which is exactly the condition that makes `stream_max_cursor`
        // true and leaves `effective_upper` unresolved here. Silently falling
        // through to `seed_value(_, None) == None` would run that doomed full
        // read instead of skipping it, with nothing to say so. Refuse instead:
        // the fix is `probe_max_cost=0` (pay for one MAX probe) or an index on
        // the watermark column, either of which resolves `effective_upper` and
        // clears this. Does not apply to a chunk-resume (`pinned_upper` is
        // already `Some`) or a genuinely empty source (`stream_max_cursor` is
        // only set when `lookback_seconds > 0`, so an empty table's `None` max
        // never sets it — see `plan_watermark_probes`).
        if cfg.seed_watermark == WatermarkSeed::CurrentMax
            && committed.is_none()
            && effective_upper.is_none()
            && stream_max_cursor
        {
            return Err(EtlError::config(format!(
                "seed_watermark=skip_to_max on '{watermark}' needs the source's current MAX, but \
                 the MAX(watermark) probe was skipped as too costly (see the unindexed_watermark \
                 warning above). Reading with no seed would run the full-table first pull \
                 skip_to_max exists to avoid, so this run is refused instead. Add an index on \
                 '{watermark}', or set probe_max_cost=0 to pay for one MAX probe regardless of \
                 estimated cost."
            )));
        }
        // First run only: seed the lower bound.
        let last = committed
            .clone()
            .or_else(|| seed_value(&cfg.seed_watermark, effective_upper.as_deref()));
        cursor_plan.seeded = committed.is_none();
        cursor_plan.keep_committed = resume.is_some() && !cursor_plan.pinned && committed.is_some();
        cursor_plan.floor = cursor_floor(
            committed.as_deref(),
            last.as_deref(),
            watermark_tracker.as_deref(),
        );
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
                watermark_pg_is_tz_aware(watermark, &source_cols),
            ),
            Source::MySql(_) => build_watermark_filter_mysql(
                watermark,
                source_expr,
                last.as_deref(),
                effective_upper.as_deref(),
                cfg.lookback_seconds,
            ),
            Source::ClickHouse(_) => build_watermark_filter_clickhouse(
                watermark,
                source_expr,
                last.as_deref(),
                effective_upper.as_deref(),
                cfg.lookback_seconds,
                watermark_type.as_deref(),
            ),
            Source::BigQuery(_) => {
                unreachable!("BigQuery is handled via the early return in run_transfer")
            }
        };
        // A first read whose cursor comes from the rows read has no bound
        // at all, so it reads rows whose watermark is NULL too. Resumed, it
        // is held to its marker's bound, which no NULL is under: read them
        // all the same, as the read it finishes would have. Unless the
        // destination can't hold them: a ClickHouse ReplacingMergeTree's
        // version column is the watermark, never NULL, and the first such
        // row would fail the insert. Such a read leaves them out, as a read
        // bounded by the MAX does, and the NULL-count probe reports them
        // (`null_watermark`, or `null_check_skipped` when it costs too much).
        let null_watermark_lands = null_watermark_lands(&plan, watermark);
        let col = watermark_column_sql(source.as_ref(), watermark, source_expr);
        let filter = match filter {
            Some(f)
                if last.is_none()
                    && cursor_plan.pinned
                    && stream_max_cursor
                    && null_watermark_lands =>
            {
                Some(format!("({f} OR {col} IS NULL)"))
            }
            None if !null_watermark_lands => {
                tracing::info!(
                    "the destination's '{watermark}' can't be NULL (a ReplacingMergeTree's \
                     version column, or a key), so this unbounded read leaves out rows whose \
                     '{watermark}' is NULL, as every bounded one does"
                );
                Some(format!("{col} IS NOT NULL"))
            }
            other => other,
        };
        if let Some(l) = last.as_deref() {
            let bound = lower_bound_sql(
                source.as_ref(),
                l,
                cfg.lookback_seconds,
                watermark,
                &source_cols,
                watermark_type.as_deref(),
            );
            ensure_lower_bound_not_null(
                source.as_ref(),
                control.as_mut(),
                &bound,
                &cfg,
                l,
                committed.is_some(),
            )
            .await?;
        }
        // Only a fresh probe describes the source now: a chunk resume reads up
        // to the interrupted run's frozen bound, and a stream-derived cursor
        // had no probe at all.
        let cursor_check = match (last.as_deref(), snapshot_max.as_deref()) {
            (Some(l), Some(m)) if resume.is_none() => {
                check_cursor_against_max(watermark, l, m, cfg.lookback_seconds, &source_cols)
            }
            _ => None,
        };
        if let Some(c) = cursor_check.as_ref().filter(|c| c.cursor_ahead) {
            // A MAX from the table itself is unfiltered already.
            let verdict = if cfg.source_query.is_some() && !max_from_table {
                judge_cursor_ahead_of_query(
                    source.as_ref(),
                    control.as_mut(),
                    &cfg,
                    c,
                    &source_cols,
                )
                .await
            } else {
                CursorAhead::Real
            };
            cursor_plan.rewind_to_max = verdict == CursorAhead::Real;
            match verdict {
                CursorAhead::Real => c.warn_if_cursor_ahead(&cfg, None, &warnings),
                CursorAhead::WithinTable => {}
                CursorAhead::Unexplained(why) => {
                    c.warn_if_cursor_ahead(&cfg, Some(&why), &warnings)
                }
            }
        }
        let chunk_plan = match cfg.chunk_rows {
            Some(limit) => {
                let mut chunk = build_chunk_plan(
                    &cfg,
                    &plan,
                    &source_cols,
                    limit,
                    committed,
                    effective_upper.clone(),
                    start_cursor,
                    keyset_not_null,
                )?;
                chunk.marker = if cursor_plan.keep_committed {
                    MarkerBound::Unbounded
                } else if chunk.effective_upper.is_some() {
                    MarkerBound::Frozen
                } else {
                    MarkerBound::Stream {
                        started: first_started,
                        floor: cursor_plan.floor.clone(),
                        best: Arc::new(AtomicI64::new(i64::MIN)),
                    }
                };
                Some(chunk)
            }
            None => None,
        };
        cursor_plan.last = last;
        (filter, effective_upper, chunk_plan, cursor_check)
    } else {
        (None, None, None, None)
    };
    // The checks that needed the setup connection are done; don't hold it open
    // through the read.
    drop(control);
    // A read of several statements (windows, partitions, chunks) sees no single
    // snapshot, so a stream-derived cursor has to allow for how long it took.
    let read_spans_statements =
        window_plan.is_some() || partitions.len() > 1 || chunk_plan.is_some();
    // Setup raised its warnings already: one that is fatal stops the run here,
    // before any table is created or written.
    check_fatal(&cfg, sink.as_ref(), &warnings, Before::Write)?;

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
    let chunk_stager = (chunk_plan.is_some() && used_staging).then(|| {
        Arc::new(ChunkStager {
            sink: sink.clone(),
            dest_table: cfg.dest_table.clone(),
            template: staging.clone(),
            key: cfg.key.clone(),
            columns: plan.dest_columns.clone(),
            merge_prune_partition_by: cfg.merge_prune_partition_by.clone(),
            merge_prune_key_range: cfg.merge_prune_key_range,
            merge_prune_key_list_max: cfg.merge_prune_key_list_max,
            dedup_order: cfg.watermark.clone(),
            warnings: warnings.clone(),
            next: AtomicU64::new(0),
            open: Mutex::new(None),
            landed: (!sink.requires_staging_for_incremental()).then(|| landed.clone()),
        })
    });
    let cleanup_stager = chunk_stager.clone();
    let staged_per_chunk = chunk_stager.is_some();

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
        let window_plan = Arc::new(window_plan);
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
            watermark_max: watermark_tracker.clone(),
            chunk_stager,
            landed: (!used_staging).then(|| landed.clone()),
        };
        let stage_started = Instant::now();

        let mut results = futures::stream::iter(partitions.into_iter().map(|part| {
            let source = source.clone();
            let plan = plan.clone();
            let cfg = cfg.clone();
            let ctx = ctx.clone();
            let extra_filter = extra_filter.clone();
            let chunk_plan = chunk_plan.clone();
            let window_plan = window_plan.clone();
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
                            window_plan.as_ref().as_ref(),
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
                            window_plan.as_ref().as_ref(),
                        )
                        .await
                    }
                    Source::ClickHouse(s) => {
                        transfer_partition_clickhouse(
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
            check_fatal(&cfg, sink.as_ref(), &warnings, Before::Swap)?;
            tracing::info!("swapping staging table into '{}'", cfg.dest_table);
            sink.atomic_swap(&cfg.dest_table, &staging, &plan.dest_columns).await?;
            sink.drop_table(&staging).await?;
        }

        // --- Incremental: validate + promote staged rows (MERGE for BigQuery,
        // insert-select for a gated ClickHouse run; direct-insert runs did not
        // stage and have nothing to promote), then persist the new watermark. ---
        let mut rows_deleted = 0u64;
        let rows_read = counters.rows_read.load(Ordering::Relaxed);
        if cfg.mode == SyncMode::Incremental {
            // Before the promotion, so a fail_on_warnings that names it stops
            // the MERGE too.
            let watermark_read = match &watermark_tracker {
                Some(t) => t.seen(),
                // A MAX-bounded read takes the MAX as its cursor, and that MAX
                // is NULL exactly when no row holds a watermark.
                None => new_watermark.is_some(),
            };
            if rows_read == 0 {
                if new_watermark.is_none() {
                    tracing::info!("no rows read, so no watermark to advance to (cursor unchanged)");
                }
            } else if !watermark_read && cursor_plan.last.is_none() && !cursor_plan.pinned {
                // With a lower bound the read can't return a NULL watermark; a
                // value decoded to NULL is a coerced_date of its own.
                warn_on_unwatermarked_read(&cfg, rows_read, &warnings);
            }
            if staged_per_chunk {
                // Every chunk was merged as it was read; only the empty
                // template its tables were cloned from is left.
                sink.drop_table(&staging).await?;
            } else if used_staging {
                check_fatal(&cfg, sink.as_ref(), &warnings, Before::Merge)?;
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
                // A MERGE run again upserts the same keys: nothing to count.
                if !sink.requires_staging_for_incremental() {
                    landed.fetch_add(counters.rows_written.load(Ordering::Relaxed), Ordering::Relaxed);
                }
            }
            // With no frozen upper bound, the cursor is the largest watermark
            // this run actually read. `None` means no non-NULL row was read, so
            // the cursor correctly stays where it was. Assigned to the outer
            // binding, not shadowed, so `TransferResult::new_watermark` reports
            // the value that was actually persisted.
            // A read of several statements can outlast the lookback: see
            // `stream_cursor_rewind_secs`. Under source_table's MAX, a DATE
            // needs no extra day: that bound keeps out rows changed after
            // the read began, a later day's included.
            let rewind = if read_spans_statements {
                stream_cursor_rewind_secs(stage_secs, cfg.lookback_seconds)
            } else {
                0
            };
            // A resumed chunked read saw only the chunks after its marker, so
            // its cursor isn't taken from what it read: a row in an earlier
            // chunk that changed since would sit below it (see `MarkerBound`).
            if cursor_plan.pinned {
                // The bound the marker froze, which held the read, and is in
                // `new_watermark` already.
                tracing::info!(
                    "resumed a chunked read: the cursor is the bound its marker recorded, {:?}",
                    new_watermark
                );
            } else if cursor_plan.keep_committed {
                new_watermark = None;
                tracing::info!(
                    "resumed a chunked read whose marker recorded no upper bound (written by \
                     0.20.6, say): the committed cursor is kept, and the next run reads every \
                     row changed since"
                );
            } else if let (Some(t), true) = (&watermark_tracker, max_from_table) {
                // Bounded by the table's MAX, the cursor is what the read
                // actually returned under it: see `table_max_eligible`. A
                // cursor that was really ahead of the table is moved back to
                // that MAX instead, as through a query. The rewind is a
                // margin here: the bound already keeps out rows changed
                // mid-read, unless source_query converts the watermark.
                if !cursor_plan.rewind_to_max {
                    new_watermark = t
                        .advance_from(cursor_plan.floor.as_deref(), rewind)
                        .or_else(|| {
                            // A seed is kept, as a first run's MAX would have been.
                            cursor_plan.seeded.then(|| cursor_plan.last.clone()).flatten()
                        });
                    tracing::info!(
                        "watermark taken from the rows read under source_table's MAX: {:?}",
                        new_watermark
                    );
                }
            } else if let Some(t) = &watermark_tracker {
                let rewind = if read_spans_statements {
                    t.stream_rewind_secs(stage_secs, cfg.lookback_seconds)
                } else {
                    0
                };
                new_watermark = t.render_rewound(rewind, cursor_plan.floor.as_deref());
                if let Some(w) = &new_watermark {
                    if rewind > 0 && t.unit == WatermarkUnit::Days {
                        tracing::info!(
                            "watermark taken from the read stream: {w}, moved back a day or \
                             more: a DATE is the day of a change, so a read of several \
                             statements that crossed midnight would otherwise leave a row read \
                             and changed again that day below the next run's lower bound"
                        );
                    } else if rewind > 0 {
                        tracing::info!(
                            "watermark taken from the read stream: {w}, moved back {rewind}s \
                             because the read took {stage_secs:.0}s, longer than \
                             lookback_seconds={}, so rows changed in a window already read are \
                             read again next run",
                            cfg.lookback_seconds
                        );
                    } else {
                        tracing::info!("watermark taken from the read stream: {w}");
                    }
                }
            }
            // The table's own MAX can sit above every row source_query returns,
            // so a read of nothing contradicts nothing there.
            if let Some(c) = cursor_check.as_ref().filter(|_| !max_from_table) {
                c.warn_if_not_advanced(&cfg, rows_read, &warnings);
            }
            // Last stop before the cursor moves, after every warning the read
            // and the promotion raise.
            let at = if chunk_plan.is_some() {
                Before::CursorAfterChunks
            } else {
                Before::Cursor
            };
            check_fatal(&cfg, sink.as_ref(), &warnings, at)?;
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
            // A chunked run that persists no new watermark (nothing to advance
            // to, or advance_watermark=false) must still clear the resume
            // marker it, or the interrupted run it resumed, left behind.
            // Otherwise every later run resumes past that key cursor and never
            // re-reads lower keys, whatever changes in them. Re-writing the
            // committed watermark clears the marker without moving the cursor.
            if let Some(chunk) = chunk_plan.as_ref() {
                if new_watermark.is_none() || !cfg.advance_watermark {
                    sink.persist_watermark(
                        &cfg,
                        chunk.committed.as_deref().unwrap_or(""),
                        counters.rows_written.load(Ordering::Relaxed),
                    )
                    .await?;
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
            rows_written_failed_attempts: 0,
        })
    }
    .await;

    if outcome.is_err() {
        if let Some(stager) = &cleanup_stager {
            stager.cleanup().await;
        }
        if used_staging {
            cleanup_staging(&cleanup_sink, &cleanup_staging_name).await;
        }
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
    // See `Attempt::landed`: a stalled read (read_idle_timeout_secs) is
    // retried like a database source's transient error.
    landed: Arc<AtomicU64>,
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

    let (row_restriction, new_watermark, cursor_check) = if cfg.mode == SyncMode::Incremental {
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
        // Unbounded only on a first run whose MAX is NULL: leave out the rows
        // with no watermark where the destination can't hold them, as any
        // bound does (see `null_watermark_lands`). With the MAX NULL, that is
        // every row the table has, which BigQuery counts for nothing, so
        // leaving them out is never silent.
        let filter = match filter {
            None if !null_watermark_lands(&plan, watermark) => {
                let rows = source.count_rows(&client, &project_id, &table_sql).await?;
                warn_on_null_watermark(watermark, rows, &warnings);
                Some(format!("{} IS NOT NULL", quote_my(watermark)))
            }
            other => other,
        };
        // No lower-bound NULL check here: every BigQuery bound is a typed
        // literal (`CAST('...' AS DATETIME)`, `TIMESTAMP '...'`), which fails
        // the query outright on a value it can't parse rather than yielding
        // NULL.
        let cursor_check = match (last.as_deref(), snapshot_max.as_deref()) {
            (Some(l), Some(m)) => {
                check_cursor_against_max(watermark, l, m, cfg.lookback_seconds, &source_cols)
            }
            _ => None,
        };
        // source_table wins over source_query here, so a MAX through the
        // query has no unfiltered table to be checked against: see
        // `judge_cursor_ahead_of_query`.
        if let Some(c) = &cursor_check {
            let through_query = cfg.source_table.is_none() && cfg.source_query.is_some();
            let why = "A BigQuery source reads source_query alone, so there is no \
                           unfiltered table to check it against.";
            c.warn_if_cursor_ahead(&cfg, through_query.then_some(why), &warnings);
        }
        (filter, snapshot_max, cursor_check)
    } else {
        (None, None, None)
    };

    // Force a staging table for a gated incremental run into a directly-inserting
    // destination (ClickHouse), so the gate has something to validate (see the
    // DB flow for the rationale).
    // The same is true of a window-scoped delete on a destination whose upsert
    // cannot express one inline: the delete subtracts the staged keys, so it
    // needs the batch materialised in a table before any of it lands.
    // Staged per attempt for a destination a retry would duplicate rows in,
    // as in the database flow (see `Sink::stage_for_retries`).
    let stage_for_retries = cfg.mode == SyncMode::Incremental
        && cfg.retry_max_attempts > 1
        && !sink.requires_staging_for_incremental()
        && sink.stage_for_retries(&cfg.dest_table, &cfg).await?;
    let force_stage_incremental = cfg.mode == SyncMode::Incremental
        && !sink.requires_staging_for_incremental()
        && (on_staged.is_some()
            || stage_for_retries
            || (cfg.delete_stale_in_window && !sink.deletes_stale_within_merge()));
    if cfg.delete_stale_in_window && !sink.supports_row_delete() {
        return Err(EtlError::config(
            "delete_stale_in_window is not supported by this destination (it cannot delete \
             individual rows)",
        ));
    }

    check_fatal(&cfg, sink.as_ref(), &warnings, Before::Write)?;
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
            watermark_max: None,
            chunk_stager: None,
            landed: (!used_staging).then(|| landed.clone()),
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
            check_fatal(&cfg, sink.as_ref(), &warnings, Before::Swap)?;
            tracing::info!("swapping staging table into '{}'", cfg.dest_table);
            sink.atomic_swap(&cfg.dest_table, &staging, &plan.dest_columns).await?;
            sink.drop_table(&staging).await?;
        }
        let mut rows_deleted = 0u64;
        if cfg.mode == SyncMode::Incremental {
            if used_staging {
                check_fatal(&cfg, sink.as_ref(), &warnings, Before::Merge)?;
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
                // A MERGE run again upserts the same keys: nothing to count.
                if !sink.requires_staging_for_incremental() {
                    landed.fetch_add(counters.rows_written.load(Ordering::Relaxed), Ordering::Relaxed);
                }
            }
            let rows_read = counters.rows_read.load(Ordering::Relaxed);
            if new_watermark.is_none() && rows_read > 0 {
                warn_on_unwatermarked_read(&cfg, rows_read, &warnings);
            }
            if let Some(c) = &cursor_check {
                c.warn_if_not_advanced(&cfg, rows_read, &warnings);
            }
            check_fatal(&cfg, sink.as_ref(), &warnings, Before::Cursor)?;
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
            rows_written_failed_attempts: 0,
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
    archive_info: Option<Arc<ArchiveRunInfo>>,
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

    check_fatal(&cfg, sink.as_ref(), &warnings, Before::Write)?;
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
            archive: archive_info.clone(),
            throttle: None,
            warnings: warnings.clone(),
            watermark_max: None,
            chunk_stager: None,
            landed: None,
        };
        let stage_started = Instant::now();
        let mut batcher = ApiBatcher::new(&plan.dest_columns, &lookups, cfg.batch_rows, cfg.batch_bytes)?;
        let schema = batcher.schema();
        let mut sends: JoinSet<Result<()>> = JoinSet::new();
        let mut insert_buf = InsertBuffer::new(cfg.insert_bytes);
        // No discrete partitions on this path (the fetch is sequential, like
        // the BigQuery-source flow) — "all" is the only file this run will
        // ever archive for this table.
        let mut archive_writer = match &ctx.archive {
            Some(info) => Some(info.writer_for("all", schema.clone())?),
            None => None,
        };

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
                // Every cursor already fetched. The chain is a walk that must
                // never revisit a node: comparing only against the *previous*
                // cursor catches A -> A but not A -> B -> A, and with no page
                // cap a cycle of length two spins forever, re-appending the
                // same records until the process is killed. Cursors run to
                // ~1,900 characters, so the set holds hashes rather than the
                // tokens themselves.
                let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
                let cursor_hash = |c: &str| {
                    use std::hash::{Hash, Hasher};
                    let mut h = std::collections::hash_map::DefaultHasher::new();
                    c.hash(&mut h);
                    h.finish()
                };
                seen.insert(cursor_hash(&cursor));
                // Set when the export ends in a state that means the
                // destination holds fewer records than the source has. The run
                // still succeeds — the rows read are real — but the caller is
                // told, because nothing else can tell this apart from a quiet
                // day.
                let mut incomplete: Option<String> = None;
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
                    // The vendor is not documented to send "partial", and the
                    // module treats "success" as non-terminal precisely because
                    // "partial" never arrives. If it ever does, that is a
                    // contract change, and the value reaching only a debug!
                    // line would let it pass unnoticed — which is the whole
                    // failure mode this warning exists to close.
                    if page.status == crate::source::clevertap::PageStatus::Partial
                        && incomplete.is_none()
                    {
                        incomplete = Some(format!(
                            "CleverTap export for '{}' returned a page with status \"partial\" \
                             on page {pages}, a status this API is not documented to send. Treat \
                             the export as a contract change and verify the record count against \
                             the vendor.",
                            c.event_name,
                        ));
                    }
                    for rec in &page.records {
                        if let Some(b) = batcher.append_record(rec)? {
                            if let Some(w) = archive_writer.as_mut() {
                                w.write(&b).await?;
                            }
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
                    // A cursor that repeats one already fetched cannot
                    // advance the chain, and an export that genuinely ends says
                    // so by omitting the key rather than by repeating it. Any
                    // revisit — immediate or further back — is a vendor-side
                    // anomaly, and stopping is the only way out of the cycle.
                    if !seen.insert(cursor_hash(&next)) {
                        let msg = format!(
                            "CleverTap export for '{}' stopped after {pages} page(s) and \
                             {records_total} record(s): the vendor returned a cursor already \
                             fetched, so the chain was not advancing. The destination holds \
                             fewer records than the source has.",
                            c.event_name,
                        );
                        tracing::warn!("{msg}");
                        incomplete = Some(msg);
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
                // A single page is the signature of every paging defect this
                // module has had — each produced exactly one page and a
                // reported success. It is also what a genuinely quiet day looks
                // like, and the run cannot tell them apart, so it says so
                // rather than silently picking one. Only raised when records
                // came back: a truly empty day is a legitimate zero.
                if pages == 1 && records_total > 0 && incomplete.is_none() {
                    incomplete = Some(format!(
                        "CleverTap export for '{}' ended after a single page ({records_total} \
                         record(s)). For a busy event that is the signature of a paging failure \
                         rather than a quiet day; verify against the vendor's own count before \
                         trusting this run.",
                        c.event_name,
                    ));
                }
                if let Some(message) = incomplete {
                    tracing::warn!("{message}");
                    warnings.push(TransferWarning {
                        kind: WarningKind::IncompleteExport,
                        column: None,
                        count: records_total,
                        sample: None,
                        message,
                    });
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
                        if let Some(w) = archive_writer.as_mut() {
                            w.write(&b).await?;
                        }
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
                        if let Some(w) = archive_writer.as_mut() {
                            w.write(&b).await?;
                        }
                        ctx.push_batch(&mut sends, &mut insert_buf, schema.clone(), b).await;
                        reap(&mut sends, false).await?;
                    }
                }
            }
            _ => unreachable!("run_transfer_api only handles API sources"),
        }
        if let Some(b) = batcher.finish()? {
            if let Some(w) = archive_writer.as_mut() {
                w.write(&b).await?;
            }
            ctx.push_batch(&mut sends, &mut insert_buf, schema.clone(), b).await;
        }
        if let Some(w) = archive_writer.take() {
            w.close().await?;
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
            check_fatal(&cfg, sink.as_ref(), &warnings, Before::Swap)?;
            tracing::info!("swapping staging table into '{}'", cfg.dest_table);
            sink.atomic_swap(&cfg.dest_table, &staging, &plan.dest_columns).await?;
            sink.drop_table(&staging).await?;
        }
        let mut rows_deleted = 0u64;
        if cfg.mode == SyncMode::Incremental {
            if used_staging {
                check_fatal(&cfg, sink.as_ref(), &warnings, Before::Merge)?;
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
            check_fatal(&cfg, sink.as_ref(), &warnings, Before::Cursor)?;
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
            // After the cursor, unlike an incremental run: append keeps no key
            // to converge on, so a run that read these rows again would append
            // them a second time.
            check_fatal(&cfg, sink.as_ref(), &warnings, Before::Done)?;
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
            rows_written_failed_attempts: 0,
        })
    }
    .await;

    if outcome.is_err() && used_staging {
        cleanup_staging(&cleanup_sink, &cleanup_staging_name).await;
    }
    outcome
}

/// Transfer an in-memory Arrow frame (`quickhouse.from_pandas`) into either
/// destination.
///
/// Structurally the API flow with the fetching removed: the schema arrives with
/// the data, so there is no catalog to probe, no window to derive, no cursor to
/// resume and no connection to open. What is left is decode -> push -> promote,
/// sharing `prepare_target`, `SendCtx` and the promotion tail with every other
/// source.
///
/// The frame is decoded with `StreamReader` rather than
/// [`crate::decode_clickhouse::ChArrowDecoder`] on purpose. That decoder is
/// push-based because an HTTP body arrives in arbitrary chunks, and it copies
/// every chunk (`Buffer::from_vec(chunk.to_vec())`) to own it — correct there,
/// and a pointless full copy of a buffer we already hold. `StreamReader` also
/// takes a column projection natively, which is what makes `include=`/`exclude=`
/// free here.
#[allow(clippy::too_many_arguments)]
async fn run_transfer_frame(
    frame: crate::config::ArrowFrameConfig,
    sink: Arc<dyn Sink>,
    mut cfg: TransferConfig,
    progress: Option<ProgressCb>,
    on_staged: Option<StagedValidationCb>,
    started: Instant,
    archive_info: Option<Arc<ArchiveRunInfo>>,
    staging: String,
    warnings: Warnings,
) -> Result<TransferResult> {
    use std::io::Cursor;

    use arrow::ipc::reader::StreamReader;
    use arrow_schema::{Field, Schema};

    // Without a source_table, `effective_state_key()` is empty — give the
    // incremental cursor a stable identity. A user `state_key` still wins, and
    // so does an explicit frame label.
    if cfg.state_key.is_none() {
        cfg.state_key = Some(
            frame
                .label
                .as_ref()
                .map(|l| format!("frame:{l}"))
                .unwrap_or_else(|| format!("frame:{}", cfg.dest_table)),
        );
    }

    // Schema probe: `try_new` reads the stream's leading schema message and
    // stops, so this costs one message, not a decode of the whole frame.
    let incoming = StreamReader::try_new(Cursor::new(&frame.ipc[..]), None)
        .map_err(|e| {
            EtlError::decode(format!(
                "the frame is not a readable Arrow IPC stream ({e}). It is produced by \
                 quickhouse's own Python layer, so this generally means the bytes were \
                 truncated or built by something else."
            ))
        })?
        .schema();
    let source_cols = crate::types::arrow_frame::columns_from_arrow_schema(&incoming)?;

    // `true`, unlike the ClickHouse source. A frame carries no NOT NULL
    // constraint — pandas has no such concept, so a `nullable: false` here is a
    // claim about *this* frame that the next one breaks (run 1 has no nulls in
    // `amount`, run 2 does, and the second fails the Arrow schema-consistency
    // check). `key`/`order_by`/`primary_key`/`not_null` still force NOT NULL
    // where it is actually required.
    let plan: SelectPlan = transform::plan_with(&source_cols, &cfg, sink.dest_kind(), true)?;
    let plan = Arc::new(plan);

    // `include=`/`exclude=` become an Arrow column projection, applied by the
    // reader itself. `transform::plan` has already validated every name.
    let projection: Option<Vec<usize>> = if plan.source_columns.len() == incoming.fields().len() {
        None
    } else {
        Some(
            plan.source_columns
                .iter()
                .map(|n| {
                    incoming.index_of(n).map_err(|_| {
                        EtlError::internal(format!(
                            "column '{n}' survived planning but is not in the frame schema"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        )
    };

    // Identical to the DB and API flows: a gated incremental run into a
    // directly-inserting destination needs a staging table for the gate to
    // validate, and so does a window-scoped delete the upsert cannot express.
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

    check_fatal(&cfg, sink.as_ref(), &warnings, Before::Write)?;
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
            "quickhouse: transferring a {}-byte Arrow frame into {}",
            frame.ipc.len(),
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
            archive: archive_info.clone(),
            throttle: None,
            warnings: warnings.clone(),
            watermark_max: None,
            chunk_stager: None,
            landed: None,
        };
        let stage_started = Instant::now();
        let schema: SchemaRef = Arc::new(Schema::new(
            plan.dest_columns
                .iter()
                .map(|c| Field::new(&c.name, c.arrow.clone(), c.nullable))
                .collect::<Vec<_>>(),
        ));
        let mut sends: JoinSet<Result<()>> = JoinSet::new();
        let mut insert_buf = InsertBuffer::new(cfg.insert_bytes);
        let mut rows_read = 0u64;
        // A frame is one unit with no partitions, so "all" is the only file
        // this run will ever archive for this table.
        let mut archive_writer = match &ctx.archive {
            Some(info) => Some(info.writer_for("all", schema.clone())?),
            None => None,
        };

        let mut reader = StreamReader::try_new(Cursor::new(&frame.ipc[..]), projection)
            .map_err(EtlError::from)?;
        let mut coercions =
            crate::decimal::CoercionTally::new(schema.fields().iter().map(|f| f.name().as_str()));
        for batch in reader.by_ref() {
            let batch = crate::decode_clickhouse::adapt_to_plan(
                batch.map_err(EtlError::from)?,
                &schema,
                "the frame",
                Some(&mut coercions),
            )?;
            rows_read += batch.num_rows() as u64;
            for slice in crate::decode_clickhouse::split_to_bytes(batch, cfg.batch_bytes) {
                if let Some(w) = archive_writer.as_mut() {
                    w.write(&slice).await?;
                }
                ctx.push_batch(&mut sends, &mut insert_buf, schema.clone(), slice)
                    .await;
                reap(&mut sends, false).await?;
            }
        }
        // A stream that stops without its end-of-stream marker is a truncated
        // frame, not a short one. Left unchecked it would swap a partial table
        // into place and report success.
        if !reader.is_finished() {
            return Err(EtlError::decode(
                "the Arrow IPC stream ended without an end-of-stream marker — the frame was \
                 serialized incompletely, so an unknown number of rows are missing",
            ));
        }
        report_coercions("the frame", coercions.entries(), &warnings);
        if let Some(w) = archive_writer.take() {
            w.close().await?;
        }
        ctx.flush(&mut sends, &mut insert_buf, schema.clone()).await;
        reap(&mut sends, true).await?;
        counters.rows_read.fetch_add(rows_read, Ordering::Relaxed);
        emit_progress(&counters, &progress, started);
        let stage_secs = stage_started.elapsed().as_secs_f64();
        let promote_started = Instant::now();
        tracing::info!(
            "frame read complete: {} rows written",
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
            check_fatal(&cfg, sink.as_ref(), &warnings, Before::Swap)?;
            tracing::info!("swapping staging table into '{}'", cfg.dest_table);
            sink.atomic_swap(&cfg.dest_table, &staging, &plan.dest_columns)
                .await?;
            sink.drop_table(&staging).await?;
        }
        let mut rows_deleted = 0u64;
        if cfg.mode == SyncMode::Incremental && used_staging {
            // `cfg.watermark` is the dedup ordering column and is normally
            // `None` here: incremental from a frame upserts on `key`, and both
            // destinations already fall back to ordering by the key list. Pass
            // it through anyway, so a caller who *did* nominate one gets
            // last-wins ordering rather than an arbitrary winner.
            check_fatal(&cfg, sink.as_ref(), &warnings, Before::Merge)?;
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
        // Append inserts straight into the destination: no staging, no merge,
        // no swap. And a frame has no resumable cursor to persist either way —
        // which is why there is no `persist_watermark` call anywhere in this
        // flow, unlike the API one. A run that wrote straight in, or whose
        // MERGE raised something, can still fail on it.
        check_fatal(&cfg, sink.as_ref(), &warnings, Before::Done)?;

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
            new_watermark: None,
            warnings: warnings.drain(),
            rows_written_failed_attempts: 0,
        })
    }
    .await;

    if outcome.is_err() && used_staging {
        cleanup_staging(&cleanup_sink, &cleanup_staging_name).await;
    }
    outcome
}

#[allow(clippy::too_many_arguments)]
async fn setup_postgres(
    s: &PgSource,
    cfg: &TransferConfig,
    base_table: Option<&str>,
    base_query: Option<&str>,
    watermark: Option<&str>,
    // The saved cursor (or the first run's seed), when there is one.
    cursor: Option<&str>,
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
            "both source_table and source_query are set; schema and data come from \
             source_query. source_table is still used for the windowed read's key-bounds \
             probe, and to check a cursor that is ahead of source_query's MAX(watermark). \
             That is why setting both is useful: the bounds and the check come from the \
             table instead of inheriting source_query's own filter"
        );
    }
    let source_cols = s
        .resolve_columns(&control, &schema_probe, not_null_from)
        .await?;
    // `chunk_rows` needs a NOT NULL keyset, and every `source_query` column
    // resolves as nullable, so ask the query itself. A failed check only means
    // "not proven": `build_chunk_plan` then refuses, and says how to assert it.
    let keyset_not_null = match (cfg.chunk_rows, base_query, cfg.keyset_column()) {
        (Some(_), Some(q), Some(k))
            if !cfg.keyset_not_null && source_cols.iter().any(|c| c.name == k && c.nullable) =>
        {
            match s.result_column_not_null(&control, q, &k).await {
                Ok(proven) => {
                    if proven {
                        tracing::info!(
                            "keyset column '{k}' is a NOT NULL table column in source_query, \
                             whose plan has no outer join or grouping sets: accepted for chunk_rows"
                        );
                    }
                    proven
                }
                Err(e) => {
                    tracing::warn!("could not check keyset column '{k}' for NOT NULL: {e}");
                    false
                }
            }
        }
        _ => false,
    };

    // Only needed for incremental mode (the value is discarded otherwise) —
    // skip it in full-refresh so a watermark column left set alongside
    // mode="full" can't add a spurious query or fail on an aggregate edge
    // case (e.g. an empty table's MAX() being NULL) for a result nothing uses.
    let mut stream_max_cursor = false;
    let mut window_activated = false;
    let mut max_from_table = false;
    let mut report = None;
    let snapshot_max = if cfg.mode == SyncMode::Incremental {
        if let Some(w) = watermark {
            ensure_watermark_column(w, &source_cols)?;
            ensure_lookback_compatible(w, cfg.lookback_seconds, &source_cols)?;
            let nullable = watermark_column_nullable(w, &source_cols);
            let expr = cfg.watermark_source_expr.as_deref();
            // Ask the planner what each probe would cost before paying for it.
            // EXPLAIN plans only — nothing is executed — and unlike a catalog
            // lookup it sees straight through a `source_query`'s derived table.
            let count_sql = PgSource::count_null_watermark_sql(base_table, base_query, w, expr);
            let count_cost = if nullable {
                s.explain_cost(&control, &count_sql).await
            } else {
                // Never run for a NOT NULL column, so its cost is moot.
                ProbeCost::Known(0.0)
            };
            let query_cost = {
                let sql = PgSource::max_watermark_sql(base_table, base_query, w, expr);
                s.explain_cost(&control, &sql).await
            };
            // Only when the query's own MAX would be a scan does source_table's
            // stand in for it: see `table_max_eligible`.
            let mut table_max = None;
            if query_cost.should_skip(cfg.probe_max_cost)
                && table_max_eligible(cfg, &source_cols, w, cursor)
            {
                let sql = PgSource::max_watermark_sql(base_table, None, w, expr);
                let cost = s.explain_cost(&control, &sql).await;
                if table_max_affordable(&cost, cfg.probe_max_cost) {
                    match s.max_watermark(&control, base_table, None, w, expr).await {
                        // It has to read as the query's own column: a query
                        // that converts it (`UNIX_TIMESTAMP(updated_at) AS
                        // updated_at`) would get a bound that matches nothing.
                        Ok(max)
                            if !watermark_value_parses(
                                &source_cols,
                                w,
                                max.as_deref(),
                                watermark_pg_is_tz_aware(w, &source_cols),
                            ) =>
                        {
                            tracing::info!(
                                "MAX({w}) on source_table ({max:?}) doesn't read as \
                                 source_query's '{w}'; keeping source_query's plan"
                            )
                        }
                        Ok(max) => table_max = Some((max, cost)),
                        Err(e) => tracing::info!(
                            "MAX({w}) on source_table failed ({e}); keeping source_query's plan"
                        ),
                    }
                }
            }
            max_from_table = table_max.is_some();
            let (table_snapshot, max_cost) = match table_max {
                Some((max, cost)) => (Some(max), cost),
                None => (None, query_cost),
            };
            let probes = plan_watermark_probes(
                count_cost.clone(),
                max_cost.clone(),
                cfg.probe_max_cost,
                cfg.lookback_seconds,
            );
            if nullable && probes.count_nulls {
                let null_count = s
                    .count_null_watermark(&control, base_table, base_query, w, expr)
                    .await?;
                warn_on_null_watermark(w, null_count, warnings);
            }
            stream_max_cursor = probes.stream_max;
            window_activated = probes.max_too_dear;
            report = Some(ProbeReport {
                watermark: w,
                plan: probes,
                count_cost,
                max_cost,
                nullable,
                lookback_seconds: cfg.lookback_seconds,
                swept: false,
                suggest_source_table: base_table.is_none()
                    && table_max_possible(cfg, &source_cols, w, cursor),
                index_ddl: pg_watermark_index_ddl(w),
            });
            match table_snapshot {
                Some(max) => max,
                // No frozen upper bound: the filter is `wm > committed` alone,
                // and the cursor comes from the rows actually read.
                None if probes.stream_max => None,
                None => {
                    s.max_watermark(&control, base_table, base_query, w, expr)
                        .await?
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    // Does the read itself plan as a sequential scan? If so it will not
    // reliably finish inside a standby's conflict window, and must be swept in
    // bounded key ranges rather than attempted in one pass.
    // The read's own filter is the watermark predicate, and the probes above
    // already established whether that column can be served by an index. If it
    // cannot, `WHERE wm > x` is a sequential scan that will not reliably finish
    // inside a standby's conflict window — sweep it in bounded key ranges
    // instead of attempting it in one pass. No extra EXPLAIN is needed: the
    // signal is the one already computed.
    let mut key_bounded = false;
    // A chunked read is bounded per chunk already, and reads through its own
    // keyset loop, which a sweep would bypass: on a staged destination the
    // rows would land in the template table every chunk is cloned from, and
    // be dropped with it.
    let window = if window_activated && cfg.chunk_rows.is_none() {
        match window_key(cfg, &source_cols) {
            Some((col, _)) if watermark_bounds_the_key(watermark, &col, cursor) => {
                key_bounded = true;
                None
            }
            Some((col, nullable_key)) => {
                let first = s
                    .key_bounds(&control, base_table, base_query, &col, None)
                    .await;
                // A retry gets its own connection: the error may have closed
                // this one.
                let bounds = key_bounds_with_retry(&col, first, warnings, || async {
                    let c = s.connect().await?;
                    s.key_bounds(&c, base_table, base_query, &col, None).await
                })
                .await;
                plan_read_window(cfg, bounds, quote_pg(&col), nullable_key)
            }
            None => {
                tracing::info!(
                    "the read plans as a sequential scan, but no usable integer key was found to \
                     window it on; reading in one pass"
                );
                None
            }
        }
    } else {
        None
    };
    if let Some(mut r) = report {
        r.swept = window.is_some();
        r.warn(warnings);
    }

    // A windowed read IS the sequencing: the sweep already walks the whole key
    // space in bounded steps, so fanning out range partitions on top would make
    // every partition sweep the entire space and multiply the work by
    // `parallelism`. Same reasoning as chunked reads, which are likewise always
    // single-stream.
    // So is a read the watermark already bounds by key, which a sweep would
    // have read: its partition probe would go through source_query's filter.
    let partitions = if window.is_some() || key_bounded {
        vec![Partition {
            label: "all".into(),
            predicate: None,
        }]
    } else {
        compute_partitions_pg(s, &control, cfg, &source_cols, cursor).await?
    };
    Ok(SourceSetup {
        source_cols,
        snapshot_max,
        max_from_table,
        partitions,
        stream_max_cursor,
        window,
        watermark_type: None,
        keyset_not_null,
        control: Some(ControlConn::Postgres(control)),
    })
}

#[allow(clippy::too_many_arguments)]
async fn setup_mysql(
    s: &MySqlSource,
    cfg: &TransferConfig,
    base_table: Option<&str>,
    base_query: Option<&str>,
    watermark: Option<&str>,
    // The saved cursor (or the first run's seed), when there is one.
    cursor: Option<&str>,
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
            "both source_table and source_query are set; schema and data come from \
             source_query. source_table is still used for the windowed read's key-bounds \
             probe, and to check a cursor that is ahead of source_query's MAX(watermark). \
             That is why setting both is useful: the bounds and the check come from the \
             table instead of inheriting source_query's own filter"
        );
    }
    let source_cols = s
        .resolve_columns(&mut control, &schema_probe, cfg.tinyint1_as_bool)
        .await?;
    warn_on_shifted_timestamps(s, &mut control, &source_cols, cfg, warnings).await;

    // Only needed for incremental mode (the value is discarded otherwise) —
    // skip it in full-refresh so a watermark column left set alongside
    // mode="full" can't add a spurious query or fail on an aggregate edge
    // case (e.g. an empty table's MAX() being NULL) for a result nothing uses.
    let mut stream_max_cursor = false;
    let mut window_activated = false;
    let mut max_from_table = false;
    let mut report = None;
    let snapshot_max = if cfg.mode == SyncMode::Incremental {
        if let Some(w) = watermark {
            ensure_watermark_column(w, &source_cols)?;
            ensure_lookback_compatible(w, cfg.lookback_seconds, &source_cols)?;
            // See the equivalent block in `setup_postgres`.
            let nullable = watermark_column_nullable(w, &source_cols);
            let expr = cfg.watermark_source_expr.as_deref();
            let count_sql = MySqlSource::count_null_watermark_sql(base_table, base_query, w, expr);
            let count_cost = if nullable {
                s.explain_cost(&mut control, &count_sql).await
            } else {
                ProbeCost::Known(0.0)
            };
            let query_cost = {
                let sql = MySqlSource::max_watermark_sql(base_table, base_query, w, expr);
                s.explain_cost(&mut control, &sql).await
            };
            // Only when the query's own MAX would be a scan does source_table's
            // stand in for it: see `table_max_eligible`.
            let mut table_max = None;
            if query_cost.should_skip(cfg.probe_max_cost)
                && table_max_eligible(cfg, &source_cols, w, cursor)
            {
                let sql = MySqlSource::max_watermark_sql(base_table, None, w, expr);
                let cost = s.explain_cost(&mut control, &sql).await;
                if table_max_affordable(&cost, cfg.probe_max_cost) {
                    match s
                        .max_watermark(&mut control, base_table, None, w, expr)
                        .await
                    {
                        // It has to read as the query's own column: a query
                        // that converts it (`UNIX_TIMESTAMP(updated_at) AS
                        // updated_at`) would get a bound that matches nothing.
                        Ok(max)
                            if !watermark_value_parses(&source_cols, w, max.as_deref(), false) =>
                        {
                            tracing::info!(
                                "MAX({w}) on source_table ({max:?}) doesn't read as \
                                 source_query's '{w}'; keeping source_query's plan"
                            )
                        }
                        Ok(max) => table_max = Some((max, cost)),
                        Err(e) => tracing::info!(
                            "MAX({w}) on source_table failed ({e}); keeping source_query's plan"
                        ),
                    }
                }
            }
            max_from_table = table_max.is_some();
            let (table_snapshot, max_cost) = match table_max {
                Some((max, cost)) => (Some(max), cost),
                None => (None, query_cost),
            };
            let probes = plan_watermark_probes(
                count_cost.clone(),
                max_cost.clone(),
                cfg.probe_max_cost,
                cfg.lookback_seconds,
            );
            if nullable && probes.count_nulls {
                let null_count = s
                    .count_null_watermark(&mut control, base_table, base_query, w, expr)
                    .await?;
                warn_on_null_watermark(w, null_count, warnings);
            }
            stream_max_cursor = probes.stream_max;
            window_activated = probes.max_too_dear;
            report = Some(ProbeReport {
                watermark: w,
                plan: probes,
                count_cost,
                max_cost,
                nullable,
                lookback_seconds: cfg.lookback_seconds,
                swept: false,
                suggest_source_table: base_table.is_none()
                    && table_max_possible(cfg, &source_cols, w, cursor),
                index_ddl: mysql_watermark_index_ddl(w),
            });
            match table_snapshot {
                Some(max) => max,
                None if probes.stream_max => None,
                None => {
                    s.max_watermark(&mut control, base_table, base_query, w, expr)
                        .await?
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    // See the equivalent block in `setup_postgres`.
    // The read's own filter is the watermark predicate, and the probes above
    // already established whether that column can be served by an index. If it
    // cannot, `WHERE wm > x` is a sequential scan that will not reliably finish
    // inside a standby's conflict window — sweep it in bounded key ranges
    // instead of attempting it in one pass. No extra EXPLAIN is needed: the
    // signal is the one already computed.
    let mut key_bounded = false;
    // A chunked read is bounded per chunk already, and reads through its own
    // keyset loop, which a sweep would bypass: on a staged destination the
    // rows would land in the template table every chunk is cloned from, and
    // be dropped with it.
    let window = if window_activated && cfg.chunk_rows.is_none() {
        match window_key(cfg, &source_cols) {
            Some((col, _)) if watermark_bounds_the_key(watermark, &col, cursor) => {
                key_bounded = true;
                None
            }
            Some((col, nullable_key)) => {
                let first = s
                    .key_bounds(&mut control, base_table, base_query, &col, None)
                    .await;
                // See the equivalent call in `setup_postgres`.
                let bounds = key_bounds_with_retry(&col, first, warnings, || async {
                    let mut c = s.connect().await?;
                    s.key_bounds(&mut c, base_table, base_query, &col, None)
                        .await
                })
                .await;
                plan_read_window(cfg, bounds, quote_my(&col), nullable_key)
            }
            None => {
                tracing::info!(
                    "the read plans as a sequential scan, but no usable integer key was found to \
                     window it on; reading in one pass"
                );
                None
            }
        }
    } else {
        None
    };
    if let Some(mut r) = report {
        r.swept = window.is_some();
        r.warn(warnings);
    }

    // A windowed read IS the sequencing: the sweep already walks the whole key
    // space in bounded steps, so fanning out range partitions on top would make
    // every partition sweep the entire space and multiply the work by
    // `parallelism`. Same reasoning as chunked reads, which are likewise always
    // single-stream.
    // So is a read the watermark already bounds by key, which a sweep would
    // have read: its partition probe would go through source_query's filter.
    let partitions = if window.is_some() || key_bounded {
        vec![Partition {
            label: "all".into(),
            predicate: None,
        }]
    } else {
        compute_partitions_mysql(s, &mut control, cfg, &source_cols, cursor).await?
    };
    Ok(SourceSetup {
        source_cols,
        snapshot_max,
        max_from_table,
        partitions,
        stream_max_cursor,
        window,
        watermark_type: None,
        keyset_not_null: false,
        control: Some(ControlConn::MySql(control)),
    })
}

/// Resolved plan for a keyset-chunked resumable read (see
/// `TransferConfig::chunk_rows`). Built once per run in `run_transfer_impl`.
/// Split a chunk-resume marker `(cursor, upper)` into the frozen upper bound
/// and the cursor to resume past. A marker written while the run had no
/// frozen upper bound (the MAX probe was skipped, or MAX was NULL) stores an
/// empty `upper`; that means "no pin", never a literal `<= ''` bound.
fn resume_bounds(resume: Option<&(String, String)>) -> (Option<String>, Option<String>) {
    match resume {
        Some((cursor, upper)) => (
            (!upper.is_empty()).then(|| upper.clone()),
            Some(cursor.clone()),
        ),
        None => (None, None),
    }
}

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
    /// What each chunk's marker records as its upper bound.
    marker: MarkerBound,
}

/// What a chunked read records as `chunk_upper` in each resume marker: the
/// bound a resume reads up to and then saves as the cursor. A resume reads
/// only the chunks after the marker, so it can't take a cursor from what it
/// reads: a row in an earlier chunk that changed after it was read sits below
/// their newest watermark, and would never be read again.
#[derive(Debug, Clone)]
enum MarkerBound {
    /// `effective_upper`, or nothing without one: the snapshot MAX, which
    /// is no later than when the read began, or the bound the marker this run
    /// resumed past froze.
    Frozen,
    /// Nothing: this run resumed past a marker that recorded no bound, so it
    /// can't account for the chunks before that marker, and keeps the
    /// committed cursor whatever else it reads.
    Unbounded,
    /// A cursor taken from the rows read: the largest, over the chunks
    /// committed so far, of the cursor the read would have saved had it
    /// ended with that chunk (moved back by the time it had taken beyond the
    /// lookback, see `stream_cursor_rewind_secs`, and not below the cursor it
    /// started from, `floor`, unless everything read is). Each, less the
    /// lookback, is no later than when the read began, so every row changed
    /// since is read again. Empty until a non-NULL watermark is read.
    Stream {
        started: Instant,
        floor: Option<String>,
        /// The largest so far, in the tracker's unit (`i64::MIN`: none).
        best: Arc<AtomicI64>,
    },
}

impl ChunkPlan {
    /// The `chunk_upper` a chunk's resume marker records: see [`MarkerBound`].
    fn marker_upper(&self, tracker: Option<&WatermarkTracker>, lookback_seconds: u64) -> String {
        match &self.marker {
            MarkerBound::Frozen => self.effective_upper.clone().unwrap_or_default(),
            MarkerBound::Unbounded => String::new(),
            MarkerBound::Stream {
                started,
                floor,
                best,
            } => tracker
                .and_then(|t| {
                    let rewind =
                        t.stream_rewind_secs(started.elapsed().as_secs_f64(), lookback_seconds);
                    let v = t.rewound(rewind, floor.as_deref())?;
                    t.render_value(best.fetch_max(v, Ordering::Relaxed).max(v))
                })
                .unwrap_or_default(),
        }
    }
}

/// Validate and resolve the keyset plan for a chunked read. Enforces the
/// correctness contract: the keyset column must be selected, an integer type,
/// NOT NULL (a NULL key is silently skipped by `> cursor`), and not
/// value-transformed (the cursor is compared against the raw column in SQL, so
/// the decoded value must be the raw column value). `proven_not_null` is the
/// source's own proof for a column that resolved as nullable (see
/// `SourceSetup::keyset_not_null`); `cfg.keyset_not_null` is the caller's.
#[allow(clippy::too_many_arguments)]
fn build_chunk_plan(
    cfg: &TransferConfig,
    plan: &SelectPlan,
    source_cols: &[ColumnType],
    limit: usize,
    committed: Option<String>,
    effective_upper: Option<String>,
    start_cursor: Option<String>,
    proven_not_null: bool,
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
    if col.nullable && !proven_not_null && !cfg.keyset_not_null {
        return Err(EtlError::config(format!(
            "keyset column '{keyset_col}' must be NOT NULL for chunk_rows (a NULL key is silently \
             skipped by the cursor). A source_query's keyset is accepted when it is a plain \
             reference to a NOT NULL table column and the query has no outer join or grouping \
             sets; where it isn't (a view, a join), pass keyset_not_null=True to assert that the \
             column never holds NULL"
        )));
    }
    Ok(ChunkPlan {
        limit,
        keyset_col,
        keyset_idx,
        committed,
        effective_upper,
        start_cursor,
        marker: MarkerBound::Frozen,
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
    let mut archive = ChunkArchive::new(ctx.archive.clone(), schema.clone());
    tracing::info!(
        "keyset chunked read starting on '{}' (chunk_rows={}, resume_cursor={:?})",
        chunk.keyset_col,
        chunk.limit,
        cursor
    );

    loop {
        let chunk_ctx = ctx.begin_chunk().await?;
        let keyset = Keyset {
            col_quoted: col_quoted.clone(),
            cursor: cursor.clone(),
            bound: KeysetBound::OrderedLimit(chunk.limit),
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
        let mut chunks = CopyChunks::new(stream);
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
        let mut pending = await_source(chunks.next_chunk(), &ctx.counters, idle, &scope).await?;
        while let Some(bytes) = pending {
            let bytes = bytes?;
            let decoding = feed_off_reactor(decoder, bytes);
            let (next, joined) = tokio::join!(
                await_source(chunks.next_chunk(), &ctx.counters, idle, &scope),
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
                archive.write(&batch).await?;
                chunk_ctx
                    .push_batch(&mut sends, &mut insert_buf, schema.clone(), batch)
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
            let rows = batch.num_rows() as u64;
            if let Some(k) = last_int_key(&batch, chunk.keyset_idx)? {
                cursor_candidate = Some(cursor_candidate.map_or(k, |c| c.max(k)));
            }
            archive.write(&batch).await?;
            chunk_ctx
                .push_batch(&mut sends, &mut insert_buf, schema.clone(), batch)
                .await;
            if let Some(t) = &ctx.throttle {
                t.acquire(rows).await;
            }
        }
        // Force this chunk's rows durable in the destination BEFORE advancing
        // the cursor — the invariant that makes a crash resumable.
        chunk_ctx
            .flush(&mut sends, &mut insert_buf, schema.clone())
            .await;
        reap(&mut sends, true).await?;

        let rows_this_chunk = decoder.rows_total;
        ctx.counters
            .rows_read
            .fetch_add(rows_this_chunk, Ordering::Relaxed);
        report_coercions("keyset read", decoder.coercions(), &ctx.warnings);

        // Land the chunk in the destination before its cursor is committed
        // below (a no-op where the flush above already did).
        check_fatal(cfg, ctx.sink.as_ref(), &ctx.warnings, Before::Chunk)?;
        ctx.end_chunk(&chunk_ctx, rows_this_chunk).await?;
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
        // The chunk's archive file is durable before its cursor, like its rows.
        archive.finish_chunk().await?;
        let cur = next.to_string();
        ctx.sink
            .persist_chunk_cursor(
                cfg,
                chunk.committed.as_deref(),
                &cur,
                &chunk.marker_upper(ctx.watermark_max.as_deref(), cfg.lookback_seconds),
                ctx.counters.rows_written.load(Ordering::Relaxed),
            )
            .await?;
        // A retry resumes past this chunk, so it won't write its rows again.
        ctx.chunk_committed();
        cursor = Some(cur);
        emit_progress(&ctx.counters, &ctx.progress, ctx.started);

        if (rows_this_chunk as usize) < chunk.limit {
            break; // short chunk — the window is exhausted
        }
    }
    tracing::info!(
        "keyset chunked read complete: {} rows read",
        ctx.counters.rows_read.load(Ordering::Relaxed)
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
/// The column a windowed sweep can bound on, and whether it may hold NULLs.
///
/// The contract is deliberately weaker than the chunked-read path's, because
/// windows need less:
///
/// * **Integer** — required, since a window is range arithmetic over the key.
/// * **Untransformed** — required: a value-transform would mean the predicate
///   bounds something other than the stored column, so no index could serve it.
/// * **Unique** — *not* required. A repeated key value still falls in exactly
///   one `(lo, hi]` window. Uniqueness matters for a resumable cursor, which
///   this is not.
/// * **NOT NULL** — *not* required. A NULL key matches no range predicate, so
///   those rows get their own trailing `IS NULL` window, exactly as
///   `range_partitions` already does for a nullable partition key.
///
/// That last point matters in practice: reading through a `source_query` gives
/// PostgreSQL no base table to read NOT NULL constraints from, so every column
/// resolves as nullable. Rejecting nullable keys would make windowing inert for
/// precisely the transfers that need it — the same blind spot a catalog-based
/// index check had.
///
/// Resolution order matches `keyset_column()`: `partition_column`, else the
/// first `key` column — in practice the primary key, which an index covers.
fn window_key(cfg: &TransferConfig, source_cols: &[ColumnType]) -> Option<(String, bool)> {
    let col = cfg.keyset_column()?;
    if cfg.column_transforms.contains_key(&col) {
        return None;
    }
    let c = source_cols.iter().find(|c| c.name == col)?;
    let integer = matches!(
        c.arrow,
        DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::UInt32
    );
    if !integer {
        return None;
    }
    Some((col, c.nullable))
}

/// Whether `MAX(watermark)` is probed on `source_table` itself rather than
/// through `source_query`, when both are set.
///
/// Probed through the query, `MAX` inherits its filter: `MAX(id)` over
/// `SELECT ... WHERE is_test = 0` can't come from the primary key, and was
/// measured at 1.5 s and a planner cost of 308,743 where the table's own is
/// "Select tables optimized away". The read itself, `WHERE id > x` pushed
/// into the query, is a range scan either way. The table's MAX is a valid
/// upper bound for the filtered read, but not a cursor: a row the filter
/// excludes (a test row dated 2099, say) would carry the cursor past rows not
/// read yet. So the read is bounded by it and the cursor saved is the largest
/// watermark the read returned, folded by a [`WatermarkTracker`], which needs
/// the watermark projected untransformed and of a type it can fold.
/// `watermark_source_expr` is evaluated over the query's columns, not the
/// table's, so it rules this out too. So does a first run seeded with
/// `skip_to_max` (no `cursor` yet): its seed is the MAX itself, and would carry
/// the cursor past every row the filter does return.
///
/// Used only when the query's own MAX would be a scan, so a transfer whose
/// query MAX is affordable reads exactly as before; `probe_max_cost=0` keeps it
/// that way always. A `source_query` that converts the watermark (a time-zone
/// shift, say) is the case to watch: the table's MAX is then a bound in another
/// value domain.
fn table_max_eligible(
    cfg: &TransferConfig,
    source_cols: &[ColumnType],
    watermark: &str,
    cursor: Option<&str>,
) -> bool {
    cfg.source_table.is_some() && table_max_possible(cfg, source_cols, watermark, cursor)
}

/// [`table_max_eligible`] but for the `source_table` being set: whether
/// setting one would have the MAX probed on it.
///
/// Not for a `chunk_rows` read either: its chunk markers keep the upper bound
/// a resume reads to, and a table's MAX kept there would outlive the run that
/// knew it was no cursor. And not for a watermark the transfer leaves out or
/// overrides, which the tracker could not fold.
fn table_max_possible(
    cfg: &TransferConfig,
    source_cols: &[ColumnType],
    watermark: &str,
    cursor: Option<&str>,
) -> bool {
    cfg.source_query.is_some()
        && cfg.chunk_rows.is_none()
        && cfg.watermark_source_expr.is_none()
        && !cfg.column_transforms.contains_key(watermark)
        && !cfg.type_overrides.contains_key(watermark)
        && (cfg.include.is_empty() || cfg.include.iter().any(|c| c == watermark))
        && !cfg.exclude.iter().any(|c| c == watermark)
        && !(cfg.seed_watermark == WatermarkSeed::CurrentMax && cursor.is_none())
        && source_cols
            .iter()
            .find(|c| c.name == watermark)
            .is_some_and(|c| WatermarkUnit::of(&c.arrow, false).is_some())
}

/// Whether `value`, a MAX read off `source_table`, reads as the query's own
/// `watermark` column: same unit, same offset form. An empty table's NULL
/// does.
fn watermark_value_parses(
    source_cols: &[ColumnType],
    watermark: &str,
    value: Option<&str>,
    utc_offset: bool,
) -> bool {
    let Some(value) = value else {
        return true;
    };
    source_cols
        .iter()
        .find(|c| c.name == watermark)
        .and_then(|c| WatermarkUnit::of(&c.arrow, utc_offset))
        .is_some_and(|unit| unit.parse(value).is_some())
}

/// Whether the planner prices `source_table`'s MAX within `threshold`. An
/// unknown cost never qualifies, whatever the threshold: the EXPLAIN may have
/// failed because the table has no column by that name.
fn table_max_affordable(cost: &ProbeCost, threshold: f64) -> bool {
    !matches!(cost, ProbeCost::Unknown) && !cost.should_skip(threshold)
}

/// Whether the read needs no sweep because the watermark is the window key
/// itself and there is a cursor: the read's own filter, `key > cursor`, is
/// then the key range a window would have bounded, and covers only what is
/// new. Through a filtered `source_query` its `MAX` can still price as a scan,
/// which used to sweep the whole key space every run, with a full-scan
/// `MIN`/`MAX` for the bounds on top.
fn watermark_bounds_the_key(watermark: Option<&str>, key: &str, cursor: Option<&str>) -> bool {
    let bounds = watermark == Some(key) && cursor.is_some();
    if bounds {
        tracing::info!(
            "the watermark '{key}' is the window key, so the read's own filter is the key range \
             above the cursor: reading it in one pass"
        );
    }
    bounds
}

/// Decide whether the read must be swept in windows, and size the first one.
///
/// Activation is decided by the caller from the planner-cost probe 0.18.0
/// already runs against the watermark column: if that column cannot be served
/// by an index, the read's `WHERE wm > x` is a sequential scan. This function
/// only sizes the sweep.
///
/// Returns `None` (read in one pass, exactly as before) when the relation is
/// empty or windowing is disabled.
fn plan_read_window(
    cfg: &TransferConfig,
    bounds: Option<(i64, i64)>,
    col_quoted: String,
    nullable_key: bool,
) -> Option<WindowPlan> {
    let (min, max) = bounds?;
    // `read_window_rows` sets the ceiling; the sweep starts below it and grows
    // into it only if measured durations allow.
    let max_step = cfg
        .read_window_rows
        .unwrap_or(crate::source::DEFAULT_READ_WINDOW_ROWS);
    if max_step == 0 {
        return None;
    }
    let step = crate::source::INITIAL_READ_WINDOW_ROWS.min(max_step);
    // The floor can never exceed the ceiling: an explicitly-configured window
    // smaller than MIN_READ_WINDOW_ROWS is a deliberate choice (and the tests
    // use tiny widths on purpose), so clamp rather than panic on an inverted
    // range.
    let floor = (step / 64)
        .max(crate::source::MIN_READ_WINDOW_ROWS)
        .min(step);
    Some(WindowPlan {
        col_quoted,
        min,
        max,
        start: step,
        max_step,
        target_secs: cfg
            .window_target_secs
            .unwrap_or(crate::source::DEFAULT_WINDOW_TARGET_SECS),
        floor,
        nullable_key,
    })
}

/// Read one partition, as one pass or as a sweep of bounded windows.
///
/// The sweep reuses the single-pass statement reader verbatim: a window is
/// just an extra `key > lo AND key <= hi` conjunct on the partition
/// predicate, so every downstream behaviour — decode, backpressure, archival,
/// the watermark tracker that folds through `push_batch` — is unchanged. Its
/// windows share one connection and one insert buffer: see [`ReadOut`].
macro_rules! sweeping_partition {
    ($name:ident, $inner:ident, $stmt:ident, $src:ty) => {
        #[allow(clippy::too_many_arguments)]
        async fn $name(
            source: &$src,
            plan: &SelectPlan,
            cfg: &TransferConfig,
            ctx: &SendCtx,
            base_table: Option<&str>,
            base_query: Option<&str>,
            extra_filter: Option<&str>,
            partition: Partition,
            chunk: Option<&ChunkPlan>,
            window: Option<&WindowPlan>,
        ) -> Result<()> {
            // Never a sweep of a chunked read: see `setup_postgres`.
            let w = match (window, chunk) {
                (Some(w), None) => w,
                _ => {
                    return $inner(
                        source,
                        plan,
                        cfg,
                        ctx,
                        base_table,
                        base_query,
                        extra_filter,
                        partition,
                        chunk,
                    )
                    .await
                }
            };
            let mut out = ReadOut::new(cfg);
            // Sent only between windows, so a window that fails and is read
            // again has sent none of its rows: see `InsertBuffer::deferred`.
            out.insert_buf.deferred = true;
            // Half-open (lo, hi]: start just below `min` so the first window
            // includes it, and every key is covered exactly once.
            let mut lo = w.min.saturating_sub(1);
            let mut step = w.start;
            let mut shrinks = 0u32;
            let mut windows = 0u32;
            tracing::info!(
                "windowed read on {} over [{}, {}], starting width {} (ceiling {}, target {:.1}s)",
                w.col_quoted,
                w.min,
                w.max,
                w.start,
                w.max_step,
                w.target_secs
            );
            while lo < w.max {
                let hi = window_hi(lo, step, w.max);
                let pred = format!(
                    "{c} > {lo} AND {c} <= {hi}",
                    c = w.col_quoted,
                    lo = lo,
                    hi = hi
                );
                let part = Partition {
                    label: format!("{}:({lo},{hi}]", partition.label),
                    predicate: Some(match &partition.predicate {
                        Some(p) => format!("({p}) AND ({pred})"),
                        None => pred,
                    }),
                };
                let started = Instant::now();
                let mark = out.insert_buf.mark();
                let res = $stmt(
                    source,
                    plan,
                    cfg,
                    ctx,
                    base_table,
                    base_query,
                    extra_filter,
                    part,
                    &mut out,
                )
                .await;
                match res {
                    Ok(()) => {
                        windows += 1;
                        out.between_windows(ctx).await?;
                        let secs = started.elapsed().as_secs_f64();
                        let (n_lo, n_step) =
                            next_window(lo, hi, step, w, WindowOutcome::Done(secs));
                        if n_step != step {
                            tracing::debug!(
                                "window took {secs:.1}s (target {:.1}s); width {step} -> {n_step}",
                                w.target_secs
                            );
                        }
                        lo = n_lo;
                        step = n_step;
                    }
                    Err(e) if e.is_transient_source() && step > w.floor => {
                        // The window is read again from scratch: on a new
                        // connection, since a statement that failed can leave
                        // this one mid-result, and without the rows it had
                        // buffered.
                        out.conn = None;
                        out.insert_buf.truncate(mark);
                        shrinks += 1;
                        let (n_lo, n_step) = next_window(lo, hi, step, w, WindowOutcome::Cancelled);
                        lo = n_lo;
                        step = n_step;
                        tracing::warn!(
                            "window ({lo}, {hi}] was cancelled by the source ({e}); \
                             retrying it at width {step}"
                        );
                    }
                    // At the floor, or not a transient cancellation: this is a
                    // real failure, not a sizing problem.
                    Err(e) => return Err(e),
                }
            }
            // A NULL key matches no range predicate, so those rows would be
            // dropped by the sweep. Give them their own window — the same
            // trailing `IS NULL` partition `range_partitions` already emits.
            if w.nullable_key {
                let pred = format!("{} IS NULL", w.col_quoted);
                let part = Partition {
                    label: format!("{}:null-key", partition.label),
                    predicate: Some(match &partition.predicate {
                        Some(p) => format!("({p}) AND ({pred})"),
                        None => pred,
                    }),
                };
                $stmt(
                    source,
                    plan,
                    cfg,
                    ctx,
                    base_table,
                    base_query,
                    extra_filter,
                    part,
                    &mut out,
                )
                .await?;
                windows += 1;
            }
            tracing::info!("windowed read complete: {windows} window(s), {shrinks} shrink(s)");
            out.finish(ctx).await
        }
    };
}

sweeping_partition!(
    transfer_partition_postgres,
    read_one_partition_postgres,
    read_statement_postgres,
    PgSource
);
sweeping_partition!(
    transfer_partition_mysql,
    read_one_partition_mysql,
    read_statement_mysql,
    MySqlSource
);

/// A sweep of bounded key-range windows, used when the read's own filter is a
/// sequential scan.
///
/// **Why this exists.** On a hot standby, a read longer than
/// `max_standby_streaming_delay` is cancelled with `SQLSTATE 40001`. Retrying
/// the whole read cannot converge — every attempt restarts the same scan
/// against the same window — so a 160s scan on a 30s tolerance fails every
/// time. Measured on a real replica: two probes of this shape never completed
/// in three attempts each.
///
/// A window bounds the **key range examined**, not the rows returned. With an
/// index on the key, the work is proportional to the range whatever the rest of
/// the WHERE selects, so each read's duration is predictable. A window that is
/// cancelled anyway is retried at half the size, so the sweep provably reaches
/// a size that fits inside the tolerance instead of re-sampling a doomed scan.
#[derive(Debug, Clone)]
struct WindowPlan {
    /// Already-quoted key expression the windows bound.
    col_quoted: String,
    /// Inclusive key bounds of the relation.
    min: i64,
    max: i64,
    /// Width of the first window, before anything has been measured.
    ///
    /// Deliberately cautious and separate from [`Self::max_step`]: the first
    /// window is the only one sized by guesswork, and guessing high costs a
    /// cancelled read plus a backoff, while guessing low costs one quick read.
    start: u64,
    /// Ceiling on window width. Only a runaway guard — [`Self::target_secs`] is
    /// the real governor, and the sweep converges there on its own. Set it too
    /// close to `start` and a fast table is stuck reading far more windows than
    /// it needs: measured, 0.80s windows against a 5s target were being capped
    /// at 6x more windows than the target implied.
    max_step: u64,
    /// What one window should take. The width is adjusted after every window to
    /// converge on this, which is what keeps a read inside a standby's
    /// conflict window without anyone having to guess a row count.
    target_secs: f64,
    /// Never shrink below this; a window still failing here is a real error,
    /// not a sizing problem, and must surface rather than loop.
    floor: u64,
    /// The key may hold NULLs, which no range predicate matches — sweep a
    /// final `IS NULL` window so those rows are not silently dropped.
    nullable_key: bool,
}

/// Step the sweep: `(lo, hi]` windows over `[min, max]`, sized by how long the
/// last one actually took.
///
/// **Why duration, not a row count.** A fixed window width cannot know how wide
/// the rows are. Measured on a real 41 GB table, a 2,000,000-key window took
/// 37-121 seconds — far outside the 30s `max_standby_streaming_delay` it had to
/// fit inside — while the same width on a narrow table would be trivial. The
/// only portable signal is the clock: aim each window at
/// [`WindowPlan::target_secs`] and let the width find itself.
///
/// Returns the next `(lo, step)`. Kept separate from the I/O so the covering and
/// resizing behaviour is unit-testable without a database.
fn next_window(
    lo: i64,
    hi: i64,
    step: u64,
    plan: &WindowPlan,
    outcome: WindowOutcome,
) -> (i64, u64) {
    let target = plan.target_secs.max(0.1);
    match outcome {
        // Cancelled: halve immediately and retry the SAME window. Nothing was
        // learned about a workable size except that this one is too big.
        WindowOutcome::Cancelled => (lo, (step / 2).max(plan.floor)),
        WindowOutcome::Done(secs) => {
            // Asymmetric on purpose (AIMD): shrink hard, grow gently.
            //
            // Window duration is far more variable than width — measured on a
            // real table, p50 was 3.2s against a p90 of 9.6s and a max of 82s,
            // because how long a key range takes depends on how many rows in it
            // actually match, not on how wide it is. Growing as eagerly as we
            // shrink means one empty range doubles the width straight into a
            // dense one, and that overshoot is what lands past the standby's
            // limit. Backing off fast and creeping back up keeps the tail in
            // check at a small cost in window count.
            let ratio = (target / secs.max(0.01)).clamp(0.25, 1.25);
            let next = ((step as f64) * ratio).round() as u64;
            (hi, next.clamp(plan.floor, plan.max_step))
        }
    }
}

/// What happened to one window, and how long it took.
#[derive(Debug, Clone, Copy)]
enum WindowOutcome {
    Done(f64),
    Cancelled,
}

/// The upper edge of the window starting just above `lo`, clamped to `max`.
fn window_hi(lo: i64, step: u64, max: i64) -> i64 {
    match lo.checked_add(step as i64) {
        Some(h) => h.min(max),
        None => max,
    }
}

#[allow(clippy::too_many_arguments)]
async fn read_one_partition_postgres(
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
    let mut out = ReadOut::new(cfg);
    read_statement_postgres(
        source,
        plan,
        cfg,
        ctx,
        base_table,
        base_query,
        extra_filter,
        partition,
        &mut out,
    )
    .await?;
    out.finish(ctx).await
}

/// Read one statement, a partition or one window of a sweep, into `out`,
/// connecting first if `out` has no connection. Leaves the last of its rows
/// buffered: [`ReadOut::finish`] sends them.
#[allow(clippy::too_many_arguments)]
async fn read_statement_postgres(
    source: &PgSource,
    plan: &SelectPlan,
    cfg: &TransferConfig,
    ctx: &SendCtx,
    base_table: Option<&str>,
    base_query: Option<&str>,
    extra_filter: Option<&str>,
    partition: Partition,
    out: &mut ReadOut<tokio_postgres::Client>,
) -> Result<()> {
    tracing::info!("partition '{}' starting", partition.label);
    let ReadOut {
        conn,
        sends,
        insert_buf,
        schema: out_schema,
    } = out;
    if conn.is_none() {
        *conn = Some(source.connect().await?);
    }
    let client = conn.as_ref().expect("connected above");
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

    let stream = source.copy_stream(client, &copy_sql).await?;
    futures::pin_mut!(stream);
    let mut chunks = CopyChunks::new(stream);

    let mut decoder =
        CopyDecoder::with_batch_bytes(&plan.dest_columns, cfg.batch_rows, cfg.batch_bytes)?;
    let schema = decoder.schema();
    *out_schema = Some(schema.clone());
    let mut archive_writer = match &ctx.archive {
        Some(info) => Some(info.writer_for(&partition.label, schema.clone())?),
        None => None,
    };

    // A failed read ends the run, or (a window of a sweep) is read again
    // narrower; either way its file is aborted here, or a run that goes on
    // to succeed would leave the upload's parts in the bucket.
    let read = async {
        // Read/parse overlap: each chunk's parse runs on the blocking pool while
        // the next chunk is pulled off the socket, so the COPY stream keeps draining
        // instead of idling for the duration of every parse. A chunk is every row
        // that has already arrived, not one `CopyData` message — see `CopyChunks`.
        // The idle timer wraps the source await and nothing else: it is recorded
        // *inside* `await_source`, at the moment the chunk arrives, so the
        // concurrent decode below neither trips it nor inflates read_secs.
        let idle = cfg.read_idle_timeout_secs;
        let scope = format!("partition '{}'", partition.label);
        let mut pending = await_source(chunks.next_chunk(), &ctx.counters, idle, &scope).await?;
        while let Some(chunk) = pending {
            let chunk = chunk?;
            let decoding = feed_off_reactor(decoder, chunk);
            let (next, joined) = tokio::join!(
                await_source(chunks.next_chunk(), &ctx.counters, idle, &scope),
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
                ctx.push_batch(sends, insert_buf, schema.clone(), batch)
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
            reap(sends, false).await?; // surface any upload error promptly
        }
        if !decoder.saw_trailer() {
            return Err(EtlError::decode(format!(
                "COPY stream for partition {} ended without a trailer",
                partition.label
            )));
        }
        if let Some(batch) = decoder.finish()? {
            let rows = batch.num_rows() as u64;
            if let Some(w) = archive_writer.as_mut() {
                w.write(&batch).await?;
            }
            ctx.push_batch(sends, insert_buf, schema.clone(), batch)
                .await;
            // A statement's last batch counts toward the pace too: a window
            // smaller than one batch is all last batch.
            if let Some(t) = &ctx.throttle {
                t.acquire(rows).await;
            }
        }
        if let Some(w) = archive_writer.take() {
            w.close().await?;
        }
        Ok::<CopyDecoder, EtlError>(decoder)
    }
    .await;
    let decoder = match read {
        Ok(decoder) => decoder,
        Err(e) => {
            if let Some(w) = archive_writer.take() {
                w.abort().await;
            }
            return Err(e);
        }
    };

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
    let mut archive = ChunkArchive::new(ctx.archive.clone(), schema.clone());
    tracing::info!(
        "keyset chunked read starting on '{}' (chunk_rows={}, resume_cursor={:?})",
        chunk.keyset_col,
        chunk.limit,
        cursor
    );

    loop {
        let chunk_ctx = ctx.begin_chunk().await?;
        let keyset = Keyset {
            col_quoted: col_quoted.clone(),
            cursor: cursor.clone(),
            bound: KeysetBound::OrderedLimit(chunk.limit),
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
                archive.write(&batch).await?;
                chunk_ctx
                    .push_batch(&mut sends, &mut insert_buf, schema.clone(), batch)
                    .await;
                reap(&mut sends, false).await?;
                if let Some(t) = &ctx.throttle {
                    t.acquire(rows).await;
                }
            }
        }
        if let Some(batch) = batcher.finish()? {
            let rows = batch.num_rows() as u64;
            if let Some(k) = last_int_key(&batch, chunk.keyset_idx)? {
                cursor_candidate = Some(cursor_candidate.map_or(k, |c| c.max(k)));
            }
            archive.write(&batch).await?;
            chunk_ctx
                .push_batch(&mut sends, &mut insert_buf, schema.clone(), batch)
                .await;
            if let Some(t) = &ctx.throttle {
                t.acquire(rows).await;
            }
        }
        chunk_ctx
            .flush(&mut sends, &mut insert_buf, schema.clone())
            .await;
        reap(&mut sends, true).await?;

        let rows_this_chunk = batcher.rows_total;
        ctx.counters
            .rows_read
            .fetch_add(rows_this_chunk, Ordering::Relaxed);
        report_coercions("keyset read", batcher.coercions(), &ctx.warnings);

        // Land the chunk in the destination before its cursor is committed
        // below (a no-op where the flush above already did).
        check_fatal(cfg, ctx.sink.as_ref(), &ctx.warnings, Before::Chunk)?;
        ctx.end_chunk(&chunk_ctx, rows_this_chunk).await?;
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
        // The chunk's archive file is durable before its cursor, like its rows.
        archive.finish_chunk().await?;
        let cur = next.to_string();
        ctx.sink
            .persist_chunk_cursor(
                cfg,
                chunk.committed.as_deref(),
                &cur,
                &chunk.marker_upper(ctx.watermark_max.as_deref(), cfg.lookback_seconds),
                ctx.counters.rows_written.load(Ordering::Relaxed),
            )
            .await?;
        // A retry resumes past this chunk, so it won't write its rows again.
        ctx.chunk_committed();
        cursor = Some(cur);
        emit_progress(&ctx.counters, &ctx.progress, ctx.started);

        if (rows_this_chunk as usize) < chunk.limit {
            break;
        }
    }
    tracing::info!(
        "keyset chunked read complete: {} rows read",
        ctx.counters.rows_read.load(Ordering::Relaxed)
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn read_one_partition_mysql(
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
    let mut out = ReadOut::new(cfg);
    read_statement_mysql(
        source,
        plan,
        cfg,
        ctx,
        base_table,
        base_query,
        extra_filter,
        partition,
        &mut out,
    )
    .await?;
    out.finish(ctx).await
}

/// The MySQL [`read_statement_postgres`].
#[allow(clippy::too_many_arguments)]
async fn read_statement_mysql(
    source: &MySqlSource,
    plan: &SelectPlan,
    cfg: &TransferConfig,
    ctx: &SendCtx,
    base_table: Option<&str>,
    base_query: Option<&str>,
    extra_filter: Option<&str>,
    partition: Partition,
    out: &mut ReadOut<mysql_async::Conn>,
) -> Result<()> {
    tracing::info!("partition '{}' starting", partition.label);
    let ReadOut {
        conn,
        sends,
        insert_buf,
        schema: out_schema,
    } = out;
    if conn.is_none() {
        *conn = Some(source.connect().await?);
    }
    let conn = conn.as_mut().expect("connected above");
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
    *out_schema = Some(schema.clone());
    let mut archive_writer = match &ctx.archive {
        Some(info) => Some(info.writer_for(&partition.label, schema.clone())?),
        None => None,
    };

    // A failed read ends the run, or (a window of a sweep) is read again
    // narrower; either way its file is aborted here, or a run that goes on
    // to succeed would leave the upload's parts in the bucket.
    let read = async {
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
                ctx.push_batch(sends, insert_buf, schema.clone(), batch)
                    .await;
                reap(sends, false).await?; // surface any upload error promptly
                                           // Pace the read: pausing before fetching more rows applies
                                           // backpressure to the streaming result set, slowing the scan.
                if let Some(t) = &ctx.throttle {
                    t.acquire(rows).await;
                }
            }
        }
        if let Some(batch) = batcher.finish()? {
            let rows = batch.num_rows() as u64;
            if let Some(w) = archive_writer.as_mut() {
                w.write(&batch).await?;
            }
            ctx.push_batch(sends, insert_buf, schema.clone(), batch)
                .await;
            // See the equivalent pace in `read_statement_postgres`.
            if let Some(t) = &ctx.throttle {
                t.acquire(rows).await;
            }
        }
        if let Some(w) = archive_writer.take() {
            w.close().await?;
        }
        Ok::<(), EtlError>(())
    }
    .await;
    if let Err(e) = read {
        if let Some(w) = archive_writer.take() {
            w.abort().await;
        }
        return Err(e);
    }

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

async fn setup_clickhouse(
    s: &ClickHouseSource,
    cfg: &TransferConfig,
    base_table: Option<&str>,
    base_query: Option<&str>,
    watermark: Option<&str>,
    warnings: &Warnings,
) -> Result<SourceSetup> {
    tracing::info!("resolving clickhouse source schema...");
    // `source_query` wins, matching `ClickHouseSource::select_sql` and
    // `source_table`'s documented contract — see the equivalent comment in
    // `setup_postgres` for the mis-decode this inversion caused.
    let schema_probe = match (base_table, base_query) {
        (_, Some(q)) => q.to_string(),
        (Some(t), None) => format!("SELECT * FROM {}", quote_ch_table(t)),
        (None, None) => unreachable!("validated above"),
    };
    if base_table.is_some() && base_query.is_some() {
        tracing::warn!(
            "both source_table and source_query are set; schema and data come from \
             source_query. source_table is used only to check a cursor that is ahead of \
             source_query's MAX(watermark), against the unfiltered table"
        );
    }
    let source_cols = s
        .resolve_columns(&schema_probe, &cfg.include, &cfg.exclude)
        .await?;
    tracing::debug!("clickhouse schema resolved");

    // Only needed for incremental mode — see `setup_mysql` for why full
    // refresh deliberately skips the probe entirely.
    let (snapshot_max, watermark_type) = if cfg.mode == SyncMode::Incremental {
        if let Some(w) = watermark {
            ensure_watermark_column(w, &source_cols)?;
            ensure_lookback_compatible(w, cfg.lookback_seconds, &source_cols)?;
            if watermark_column_nullable(w, &source_cols) {
                let null_count = s
                    .count_null_watermark(
                        base_table,
                        base_query,
                        w,
                        cfg.watermark_source_expr.as_deref(),
                    )
                    .await?;
                warn_on_null_watermark(w, null_count, warnings);
            }
            s.max_watermark(
                base_table,
                base_query,
                w,
                cfg.watermark_source_expr.as_deref(),
            )
            .await?
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    let partitions = compute_partitions_clickhouse(s, cfg, &source_cols).await?;
    Ok(SourceSetup {
        source_cols,
        snapshot_max,
        partitions,
        watermark_type,
        window: None,
        // ClickHouse reads are not gated on probe cost: its sparse primary
        // index makes both probes cheap, and there is no seq-scan cliff to
        // detect.
        stream_max_cursor: false,
        keyset_not_null: false,
        max_from_table: false,
        // Every ClickHouse query is its own HTTP request: nothing to keep.
        control: None,
    })
}

async fn compute_partitions_clickhouse(
    source: &ClickHouseSource,
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
        crate::types::clickhouse::is_range_partitionable,
    )? {
        Some(t) => t,
        None => return Ok(single),
    };

    source
        .range_partitions(
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

/// Keyset-chunked resumable read for a ClickHouse source — the ClickHouse
/// analogue of [`transfer_keyset_mysql`]: bounded `SELECT ... WHERE key >
/// cursor ORDER BY key LIMIT N` chunks, each made durable before its cursor is
/// persisted.
#[allow(clippy::too_many_arguments)]
async fn transfer_keyset_clickhouse(
    source: &ClickHouseSource,
    plan: &SelectPlan,
    cfg: &TransferConfig,
    ctx: &SendCtx,
    base_table: Option<&str>,
    base_query: Option<&str>,
    extra_filter: Option<&str>,
    chunk: &ChunkPlan,
) -> Result<()> {
    let col_quoted = crate::ddl::quote_ident(&chunk.keyset_col);
    let mut cursor = chunk.start_cursor.clone();
    let partition = Partition {
        label: "keyset".into(),
        predicate: None,
    };
    let schema = ChArrowDecoder::new(&plan.dest_columns, cfg.batch_bytes).schema();
    let mut archive = ChunkArchive::new(ctx.archive.clone(), schema.clone());
    tracing::info!(
        "keyset chunked read starting on '{}' (chunk_rows={}, resume_cursor={:?})",
        chunk.keyset_col,
        chunk.limit,
        cursor
    );

    loop {
        let chunk_ctx = ctx.begin_chunk().await?;
        let keyset = Keyset {
            col_quoted: col_quoted.clone(),
            cursor: cursor.clone(),
            bound: KeysetBound::OrderedLimit(chunk.limit),
        };
        let select_sql = source.select_sql(
            &plan.source_columns,
            &plan.source_select_exprs,
            &plan.dest_columns,
            base_table,
            base_query,
            &partition,
            extra_filter,
            Some(keyset),
        );
        tracing::debug!("keyset chunk: {select_sql}");
        let mut decoder = ChArrowDecoder::new(&plan.dest_columns, cfg.batch_bytes);
        let mut sends: JoinSet<Result<()>> = JoinSet::new();
        let mut insert_buf = InsertBuffer::new(cfg.insert_bytes);
        let mut cursor_candidate: Option<i128> = None;

        let resp = source.stream_arrow(&select_sql, cfg.batch_rows).await?;
        let body = resp.bytes_stream();
        futures::pin_mut!(body);

        let idle = cfg.read_idle_timeout_secs;
        let scope = format!("partition '{}'", partition.label);
        while let Some(part) = await_source(body.next(), &ctx.counters, idle, &scope).await? {
            let part = part.map_err(EtlError::from)?;
            for batch in decoder.feed(part)? {
                let rows = batch.num_rows() as u64;
                if let Some(k) = last_int_key(&batch, chunk.keyset_idx)? {
                    cursor_candidate = Some(cursor_candidate.map_or(k, |c| c.max(k)));
                }
                archive.write(&batch).await?;
                chunk_ctx
                    .push_batch(&mut sends, &mut insert_buf, schema.clone(), batch)
                    .await;
                reap(&mut sends, false).await?;
                if let Some(t) = &ctx.throttle {
                    t.acquire(rows).await;
                }
            }
        }
        decoder.finish()?;
        chunk_ctx
            .flush(&mut sends, &mut insert_buf, schema.clone())
            .await;
        reap(&mut sends, true).await?;

        let rows_this_chunk = decoder.rows_total;
        ctx.counters
            .rows_read
            .fetch_add(rows_this_chunk, Ordering::Relaxed);

        // Land the chunk in the destination before its cursor is committed
        // below (a no-op where the flush above already did).
        check_fatal(cfg, ctx.sink.as_ref(), &ctx.warnings, Before::Chunk)?;
        ctx.end_chunk(&chunk_ctx, rows_this_chunk).await?;
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
        // The chunk's archive file is durable before its cursor, like its rows.
        archive.finish_chunk().await?;
        let cur = next.to_string();
        ctx.sink
            .persist_chunk_cursor(
                cfg,
                chunk.committed.as_deref(),
                &cur,
                &chunk.marker_upper(ctx.watermark_max.as_deref(), cfg.lookback_seconds),
                ctx.counters.rows_written.load(Ordering::Relaxed),
            )
            .await?;
        // A retry resumes past this chunk, so it won't write its rows again.
        ctx.chunk_committed();
        cursor = Some(cur);
        emit_progress(&ctx.counters, &ctx.progress, ctx.started);

        if (rows_this_chunk as usize) < chunk.limit {
            break;
        }
    }
    tracing::info!(
        "keyset chunked read complete: {} rows read",
        ctx.counters.rows_read.load(Ordering::Relaxed)
    );
    Ok(())
}

/// Read one partition from a ClickHouse source.
///
/// Structurally the same loop as [`transfer_partition_mysql`], with one
/// difference worth naming: there is no per-row decode step. The body of the
/// HTTP response *is* an Arrow IPC stream, so each chunk of bytes goes straight
/// into [`ChArrowDecoder`] and comes back out as finished `RecordBatch`es —
/// which is also why `read_idle_timeout_secs` here measures the gap between
/// response *chunks* rather than between rows. On a ClickHouse server those are
/// the same thing at any meaningful scale (a block is flushed as it is
/// produced), but a read stalled inside a single block is not something this
/// timer can see.
#[allow(clippy::too_many_arguments)]
async fn transfer_partition_clickhouse(
    source: &ClickHouseSource,
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
        return transfer_keyset_clickhouse(
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
    let select_sql = source.select_sql(
        &plan.source_columns,
        &plan.source_select_exprs,
        &plan.dest_columns,
        base_table,
        base_query,
        &partition,
        extra_filter,
        None,
    );
    tracing::debug!("partition {}: {select_sql}", partition.label);

    let mut decoder = ChArrowDecoder::new(&plan.dest_columns, cfg.batch_bytes);
    let schema = decoder.schema();
    let mut sends: JoinSet<Result<()>> = JoinSet::new();
    let mut insert_buf = InsertBuffer::new(cfg.insert_bytes);
    let mut archive_writer = match &ctx.archive {
        Some(info) => Some(info.writer_for(&partition.label, schema.clone())?),
        None => None,
    };

    let resp = source.stream_arrow(&select_sql, cfg.batch_rows).await?;
    let body = resp.bytes_stream();
    futures::pin_mut!(body);

    let idle = cfg.read_idle_timeout_secs;
    let scope = format!("partition '{}'", partition.label);
    while let Some(part) = await_source(body.next(), &ctx.counters, idle, &scope).await? {
        let part = part.map_err(EtlError::from)?;
        for batch in decoder.feed(part)? {
            let rows = batch.num_rows() as u64;
            if let Some(w) = archive_writer.as_mut() {
                w.write(&batch).await?;
            }
            ctx.push_batch(&mut sends, &mut insert_buf, schema.clone(), batch)
                .await;
            reap(&mut sends, false).await?; // surface any upload error promptly
                                            // Pace the read: pausing before pulling more of the
                                            // response body applies backpressure to the HTTP
                                            // stream, slowing the server-side scan.
            if let Some(t) = &ctx.throttle {
                t.acquire(rows).await;
            }
        }
    }
    decoder.finish()?;
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
        | WarningKind::UnclusteredMergeTarget
        | WarningKind::IncompleteExport
        | WarningKind::IgnoredSourceArchive
        | WarningKind::UnindexedWatermark
        | WarningKind::DecimalMappingMixed
        | WarningKind::WatermarkNotAdvanced
        | WarningKind::WatermarkAheadOfSource
        | WarningKind::ShiftedTimestamp
        | WarningKind::WindowBoundsUnavailable
        | WarningKind::NullCheckSkipped
        | WarningKind::RetriedAfterPartialWrite
        | WarningKind::StorageWriteCountMismatch => {
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

/// Which setup-phase watermark probes to run, decided from what the planner
/// says each would cost.
///
/// An incremental run probes the watermark column before any data moves:
/// `MAX(watermark)` for the snapshot bound, and on a nullable column
/// `count(*) WHERE watermark IS NULL` for the completeness check. Served by an
/// index both are trivial (measured planner costs 0.65 and 2.07); unserved both
/// are full sequential scans (3,031,034 and 3,031,091 for the same probes on a
/// 14.9 GB table, 56.65s and 57.19s of wall clock, on every scheduled run).
///
/// Two earlier approaches were tried and rejected, both for measured reasons:
///
/// * **Bounding the `IS NULL` count with a `LIMIT`.** When no row is NULL — the
///   common case — the planner must still scan everything to prove it, for
///   identical cost. Skipping is the only lever.
/// * **Reading `pg_index` to find the leading btree columns.** That cannot see
///   through a `source_query`'s derived table, so the check was inert for
///   exactly the transfers that needed it.
#[derive(Debug, Clone, Copy, PartialEq)]
struct WatermarkProbePlan {
    /// Run `count_null_watermark` (only ever considered for a nullable column).
    count_nulls: bool,
    /// Skip the `MAX(watermark)` bound and take the cursor from the read
    /// stream instead. Requires a lookback window — see [`plan_watermark_probes`].
    stream_max: bool,
    /// `MAX(watermark)` was too costly: no index serves the watermark, so the
    /// read's `WHERE wm > x` scans the table. Emits `unindexed_watermark`, and
    /// is what switches a windowed sweep on.
    max_too_dear: bool,
    /// The NULL count was too costly. Emits `null_check_skipped` and nothing
    /// else: an indexed watermark with many NULLs prices the count high while
    /// its MAX comes straight from the index and its read is a range scan.
    count_too_dear: bool,
}

/// Decide the probe plan from the two planner estimates.
///
/// `first_run` matters for the completeness count: a first run reads the whole
/// table anyway, so one more scan is a marginal cost paid once — and it is the
/// moment the NULL-watermark condition matters most, since a pipeline that
/// starts out excluding rows excludes them forever. Ongoing runs skip it, where
/// the same scan is pure overhead repeated on every schedule tick.
///
/// `lookback_seconds` gates replacing `MAX(watermark)` with the stream-observed
/// maximum. Without the frozen upper bound, rows written mid-read with high
/// watermarks are read too, so the cursor can land above the `MAX` a frozen
/// bound would have used — and anything in that widened band that was *not*
/// read is then skipped. A lookback exceeding the read's own duration re-covers
/// that band on the next run. With no lookback there is nothing to re-cover it,
/// so the `MAX` scan is paid for instead.
fn plan_watermark_probes(
    count_cost: ProbeCost,
    max_cost: ProbeCost,
    threshold: f64,
    lookback_seconds: u64,
) -> WatermarkProbePlan {
    let count_too_dear = count_cost.should_skip(threshold);
    let max_too_dear = max_cost.should_skip(threshold);
    WatermarkProbePlan {
        // Cost gates the NULL check even on a first run.
        //
        // Forcing it when `first_run` made the completeness check
        // unconditional, but on a large table with an unindexed watermark that
        // scan cannot finish inside a hot standby's
        // `max_standby_streaming_delay` — measured, it was cancelled with
        // 40001 on three consecutive attempts and never completed once. A
        // first run on such a table therefore died here, before the read was
        // even attempted, and no first run could ever succeed to make the
        // second one cheaper. Skipping it is reported loudly by
        // `warn_on_costly_watermark`, which is explicit that the check did not
        // run rather than implying it passed.
        count_nulls: !count_too_dear,
        stream_max: max_too_dear && lookback_seconds > 0,
        max_too_dear,
        count_too_dear,
    }
}

/// Attempts at the key-bounds probe before a windowed read falls back to one
/// pass, counting the first.
const KEY_BOUNDS_ATTEMPTS: u32 = 3;

/// Settle the key bounds of a read that has to be windowed, given the first
/// probe's outcome and a way to probe again on a fresh connection.
///
/// A transient failure (a hot standby's recovery conflict, a statement
/// timeout, a dropped connection) is retried with backoff: the probe is cheap
/// to repeat, and the one-pass read that replaces the sweep without bounds
/// is exactly the long scan the standby cancels. Never fails the run: when the
/// bounds stay unavailable the read goes ahead in one pass, and says so with a
/// `window_bounds_unavailable` warning an orchestrator can see. `None` is also
/// an empty relation, which needs no sweep.
async fn key_bounds_with_retry<F, Fut>(
    col: &str,
    first: Result<Option<(i64, i64)>>,
    warnings: &Warnings,
    mut probe: F,
) -> Option<(i64, i64)>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<(i64, i64)>>>,
{
    let mut outcome = first;
    let mut attempt = 1;
    loop {
        let e = match outcome {
            Ok(bounds) => return bounds,
            Err(e) => e,
        };
        if attempt >= KEY_BOUNDS_ATTEMPTS || !e.is_transient_source() {
            warn_window_bounds_unavailable(col, attempt, &e, warnings);
            return None;
        }
        let delay = crate::sink::backoff_delay(attempt);
        tracing::warn!(
            "probing the key bounds of '{col}' failed on attempt {attempt} ({e}); retrying in \
             {delay:?}"
        );
        tokio::time::sleep(delay).await;
        attempt += 1;
        outcome = probe().await;
    }
}

/// `window_bounds_unavailable`: the read needed a sweep, but the key bounds
/// could not be probed, so it runs in one pass.
fn warn_window_bounds_unavailable(col: &str, attempts: u32, e: &EtlError, warnings: &Warnings) {
    let message = format!(
        "the read plans as a sequential scan, but the key bounds of '{col}' could not be \
         probed after {attempts} attempt(s) ({e}), so it cannot be windowed and will be read in \
         one pass. On a hot standby that pass is likely to be cancelled. If source_query embeds \
         its own unindexed filter, the probe inherits it: set source_table as well so the \
         bounds come from the table, project the raw key and set partition_source_expr, or \
         index the filtered column."
    );
    tracing::warn!("{message}");
    warnings.push(TransferWarning {
        kind: WarningKind::WindowBoundsUnavailable,
        column: Some(col.to_string()),
        count: 0,
        sample: None,
        message,
    });
}

/// What the watermark probes skipped, and why, for [`Self::warn`].
struct ProbeReport<'a> {
    watermark: &'a str,
    plan: WatermarkProbePlan,
    count_cost: ProbeCost,
    max_cost: ProbeCost,
    nullable: bool,
    lookback_seconds: u64,
    /// The read is swept in key windows.
    swept: bool,
    /// `MAX` went through a `source_query` with no `source_table`, and setting
    /// one would have it probed on the table instead: a filter in the query
    /// may be what made it costly.
    suggest_source_table: bool,
    /// The statement that adds the index, in the source's own dialect: see
    /// [`pg_watermark_index_ddl`] and [`mysql_watermark_index_ddl`].
    index_ddl: String,
}

impl ProbeReport<'_> {
    /// Report each probe the planner priced out, naming its evidence and what
    /// was skipped because of it: `unindexed_watermark` for the `MAX`, and
    /// `null_check_skipped` for the NULL count. They used to be one warning,
    /// raised by either, so a costly NULL count on an indexed watermark was
    /// reported as a missing index, and it switched on a windowed sweep of a
    /// read that was already a range scan.
    ///
    /// Deliberately explicit when the NULL-watermark completeness check did
    /// not run: reporting a skipped check as a clean one is how the condition
    /// [`warn_on_null_watermark`] exists to catch goes unnoticed.
    fn warn(&self, warnings: &Warnings) {
        let w = self.watermark;
        if self.plan.max_too_dear {
            let mut message = format!(
                "watermark column '{w}' cannot be probed cheaply — {}. The incremental filter \
                 `WHERE {w} > x` therefore scans the whole table on every run",
                self.max_cost.describe()
            );
            if self.plan.stream_max {
                message.push_str(
                    ". quickhouse skipped the MAX(watermark) snapshot scan and took the cursor \
                     from the rows it actually read instead",
                );
            } else if self.lookback_seconds == 0 {
                message.push_str(
                    ". The MAX(watermark) snapshot scan still ran: taking the cursor from the \
                     read stream instead needs lookback_seconds > 0, so that a row written \
                     mid-read is re-covered by the next run",
                );
            }
            if self.suggest_source_table {
                message.push_str(
                    ". The MAX was taken through source_query, whose own filter can make it a \
                     scan even when the column is indexed: set source_table as well, and the \
                     MAX is probed on the table",
                );
            }
            if self.swept {
                message.push_str(&format!(
                    ". The read itself is swept in bounded key windows so it still completes; \
                     the durable fix that makes the whole sweep unnecessary is an index: {}.",
                    self.index_ddl
                ));
            } else {
                message.push_str(&format!(
                    ". The durable fix is an index: {}.",
                    self.index_ddl
                ));
            }
            tracing::warn!("{message}");
            warnings.push(TransferWarning {
                kind: WarningKind::UnindexedWatermark,
                column: Some(w.to_string()),
                count: 0,
                sample: None,
                message,
            });
        }
        if self.nullable && self.plan.count_too_dear {
            let message = format!(
                "the nullable-watermark completeness count on '{w}' cannot be run cheaply — {}, \
                 so quickhouse SKIPPED the check behind the null_watermark warning: if any row \
                 holds a NULL {w}, it is being silently excluded from this and every future \
                 incremental run and this run cannot tell you. (A read that returns rows with \
                 no {w} at all is still reported, except into a destination that can't hold a \
                 NULL {w}, such as a ReplacingMergeTree versioned by it: a first read leaves \
                 those rows out unread.) Set probe_max_cost=0 to pay for the count, or make the \
                 column NOT NULL.",
                self.count_cost.describe()
            );
            tracing::warn!("{message}");
            warnings.push(TransferWarning {
                kind: WarningKind::NullCheckSkipped,
                column: Some(w.to_string()),
                count: 0,
                sample: None,
                message,
            });
        }
    }
}

/// The PostgreSQL statement that indexes the watermark without blocking writes.
fn pg_watermark_index_ddl(watermark: &str) -> String {
    format!(
        "CREATE INDEX CONCURRENTLY ON <table> ({})",
        quote_pg(watermark)
    )
}

/// The MySQL equivalent of [`pg_watermark_index_ddl`]: an online DDL, which
/// MySQL spells as an `ALTER TABLE` (it has no `CREATE INDEX CONCURRENTLY`).
fn mysql_watermark_index_ddl(watermark: &str) -> String {
    format!(
        "ALTER TABLE <table> ADD INDEX ({}), ALGORITHM=INPLACE, LOCK=NONE",
        quote_my(watermark)
    )
}

/// Report each MySQL `TIMESTAMP` column this run lands shifted by the
/// session's time zone — see [`WarningKind::ShiftedTimestamp`]. Advisory: if
/// the server can't say what zone the session is in, the run goes on without
/// the check.
async fn warn_on_shifted_timestamps(
    s: &MySqlSource,
    conn: &mut mysql_async::Conn,
    cols: &[ColumnType],
    cfg: &TransferConfig,
    warnings: &Warnings,
) {
    if s.utc_session() {
        return;
    }
    let columns = utc_landed_timestamps(cols, cfg);
    if columns.is_empty() {
        return;
    }
    let offsets = match s.session_utc_offsets(conn).await {
        Ok(o) => o,
        Err(e) => {
            tracing::debug!("skipping the TIMESTAMP time-zone check: {e}");
            return;
        }
    };
    if offsets == [0, 0] {
        return;
    }
    let offset = describe_utc_offsets(offsets);
    for column in columns {
        let message = format!(
            "column '{column}' is a MySQL TIMESTAMP read in a session at {offset}. MySQL renders \
             it in that zone and quickhouse stores the wall-clock time as UTC, so every value \
             lands shifted by that offset. Pass utc_session=True to MySQL(...) to read it as the \
             instant it stores (rows already landed, and a cursor saved from this column, keep \
             the shift until re-read), or override it to a naive type to keep the wall-clock \
             time on purpose: type_overrides={{'{column}': 'DATETIME'}}."
        );
        tracing::warn!("{message}");
        warnings.push(TransferWarning {
            kind: WarningKind::ShiftedTimestamp,
            column: Some(column.to_string()),
            count: 0,
            sample: Some(offset.clone()),
            message,
        });
    }
}

/// The MySQL `TIMESTAMP` columns this run lands as UTC instants: transferred
/// (`include` / `exclude`), not replaced by a `column_transforms` expression,
/// and not overridden to a naive type, which asks for the wall-clock time as
/// MySQL renders it.
fn utc_landed_timestamps<'a>(cols: &'a [ColumnType], cfg: &TransferConfig) -> Vec<&'a str> {
    cols.iter()
        .filter(|c| crate::source::mysql::is_timestamp(c))
        .filter(|c| cfg.include.is_empty() || cfg.include.contains(&c.name))
        .filter(|c| !cfg.exclude.contains(&c.name))
        .filter(|c| !cfg.column_transforms.contains_key(&c.name))
        .filter(|c| {
            let dest = cfg.rename.get(&c.name).unwrap_or(&c.name);
            let over = cfg
                .type_overrides
                .get(&c.name)
                .or_else(|| cfg.type_overrides.get(dest));
            over.and_then(|t| transform::datetime_override_tz(t)) != Some(None)
        })
        .map(|c| c.name.as_str())
        .collect()
}

/// `UTC+07:00`, or `UTC+00:00 (January) / UTC+01:00 (July)` for a zone with
/// daylight saving time.
fn describe_utc_offsets([jan, jul]: [i64; 2]) -> String {
    let utc = |secs: i64| {
        let sign = if secs < 0 { '-' } else { '+' };
        let abs = secs.unsigned_abs();
        format!("UTC{sign}{:02}:{:02}", abs / 3600, abs % 3600 / 60)
    };
    if jan == jul {
        utc(jan)
    } else {
        format!("{} (January) / {} (July)", utc(jan), utc(jul))
    }
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
    cursor: Option<&str>,
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
            partitions_above(cfg, &part_col, source_expr, cursor),
        )
        .await
}

/// Where range partitions start when the watermark is the partition key: just
/// above the cursor. The read is `key > cursor` then, so splitting the whole
/// table's `[MIN, MAX]` put every new row in the last partition, and with
/// `parallelism=2` the other one opened a connection to read nothing. `None`
/// (split the whole range) otherwise.
fn partitions_above(
    cfg: &TransferConfig,
    part_col: &str,
    source_expr: Option<&str>,
    cursor: Option<&str>,
) -> Option<i64> {
    // With a watermark_source_expr the read compares that expression, not the
    // key, to the cursor.
    if cfg.mode != SyncMode::Incremental
        || source_expr.is_some()
        || cfg.watermark_source_expr.is_some()
        || cfg.watermark.as_deref() != Some(part_col)
    {
        return None;
    }
    cursor?.trim().parse().ok()
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
    cursor: Option<&str>,
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
            partitions_above(cfg, &part_col, source_expr, cursor),
        )
        .await
}

/// The size at which [`CopyChunks`] stops adding messages to a chunk, in bytes
/// of raw `COPY` data.
///
/// Only a ceiling: rows are never held back to reach it, so a slow source still
/// gets small chunks. It bounds the transient buffer; beyond a few dozen rows
/// per chunk its exact value stops mattering (64 KiB and 1 MiB measured the
/// same).
const COPY_CHUNK_BYTES: usize = 256 * 1024;

/// A binary `COPY` stream regrouped into multi-row chunks for the decoder.
///
/// PostgreSQL sends a binary `COPY` as one `CopyData` message **per row**, and
/// tokio-postgres yields one stream item per message. Handed straight to
/// [`feed_off_reactor`], every row paid a blocking-pool round trip — a thread
/// wake-up in each direction, several times the cost of parsing the row itself.
/// Measured on TPC-H `lineitem` (6M rows, PostgreSQL → ClickHouse, 2 vCPUs):
/// 6,001,216 decode tasks and 72–78s, against 23s once chunked.
///
/// [`next_chunk`](Self::next_chunk) waits for one message, then takes every
/// further message that has *already arrived*, without waiting, until the chunk
/// reaches [`COPY_CHUNK_BYTES`]. Rows are never held back to fill a chunk: a
/// trickling source is decoded as it arrives, and `read_idle_timeout_secs`
/// still times the wait for the next row.
///
/// Two details are load-bearing:
/// - The inner stream is fused. A drain can reach the end of the stream in the
///   middle of a chunk, and tokio-postgres's `CopyOutStream` does not report
///   the end again if polled after `CopyDone`: the next protocol message
///   surfaces as an "unexpected message" error instead.
/// - An error met mid-drain is held back until the rows ahead of it have been
///   returned, so the decoder sees every byte the server sent before failing.
struct CopyChunks<S, E> {
    stream: futures::stream::Fuse<S>,
    pending_error: Option<E>,
}

impl<S, E> CopyChunks<S, E>
where
    S: futures::Stream<Item = std::result::Result<Bytes, E>> + Unpin,
{
    fn new(stream: S) -> Self {
        CopyChunks {
            stream: stream.fuse(),
            pending_error: None,
        }
    }

    /// The next chunk: one or more whole `CopyData` messages, concatenated.
    /// `None` once the stream has ended.
    async fn next_chunk(&mut self) -> Option<std::result::Result<Bytes, E>> {
        if let Some(e) = self.pending_error.take() {
            return Some(Err(e));
        }
        let first = match self.stream.next().await? {
            Ok(bytes) => bytes,
            Err(e) => return Some(Err(e)),
        };
        if first.len() >= COPY_CHUNK_BYTES {
            return Some(Ok(first));
        }
        // Sized for the usual case — a few dozen rows ready at once — rather
        // than for the ceiling; the buffer grows if more has arrived.
        let mut chunk = bytes::BytesMut::with_capacity(64 * 1024);
        chunk.extend_from_slice(&first);
        while chunk.len() < COPY_CHUNK_BYTES {
            match futures::FutureExt::now_or_never(self.stream.next()) {
                Some(Some(Ok(bytes))) => chunk.extend_from_slice(&bytes),
                Some(Some(Err(e))) => {
                    self.pending_error = Some(e);
                    break;
                }
                // Nothing more has arrived yet, or the stream has ended.
                Some(None) | None => break,
            }
        }
        Some(Ok(chunk.freeze()))
    }
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
/// A chunk must hold many rows, not one `CopyData` message: the hand-off itself
/// costs more than parsing a row. The call sites get chunks from
/// [`CopyChunks`].
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

/// Raise [`WarningKind::DecimalMappingMixed`]: the destination holds exact
/// decimals beside `Float64` for source columns that declare their precision.
/// A column new to such a table keeps the 0.20.1 `Decimal(P, S)` default,
/// since there's no single convention left to follow.
fn warn_mixed_decimals(table: &str, decimal: &[String], float: &[String], warnings: &Warnings) {
    let message = format!(
        "destination '{table}' mixes exact decimal columns ({}) with non-decimal ones ({}), \
         all fed by source columns that declare a precision. ClickHouse has no arithmetic \
         between Decimal and Float64 and no common type for them in if/coalesce/UNION ALL, so \
         queries treating these columns alike fail. ALTER one side to match the other, or pin \
         the mapping with numeric_as_decimal / type_overrides.",
        decimal.join(", "),
        float.join(", "),
    );
    tracing::warn!("{message}");
    warnings.push(TransferWarning {
        kind: WarningKind::DecimalMappingMixed,
        column: None,
        count: (decimal.len() + float.len()) as u64,
        sample: None,
        message,
    });
}

/// What the destination table already holds, for
/// `transform::plan_for_destination`.
async fn existing_columns(sink: &dyn Sink, table: &str) -> Result<transform::ExistingColumns> {
    if !sink.table_exists(table).await? {
        return Ok(transform::ExistingColumns::NoTable);
    }
    Ok(match sink.column_types(table).await? {
        Some(cols) => transform::ExistingColumns::Known(cols),
        None => transform::ExistingColumns::Unknown,
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

/// The lower bound the incremental filter compares against, exactly as
/// `build_watermark_filter_{pg,mysql,clickhouse}` embed it (they call the same
/// `lookback_lower_bound_*`), for [`ensure_lower_bound_not_null`].
fn lower_bound_sql(
    source: &Source,
    last: &str,
    lookback_seconds: u64,
    watermark: &str,
    source_cols: &[ColumnType],
    watermark_type: Option<&str>,
) -> String {
    match source {
        Source::Postgres(_) => lookback_lower_bound_pg(
            last,
            lookback_seconds,
            watermark_pg_is_tz_aware(watermark, source_cols),
        ),
        Source::MySql(_) => lookback_lower_bound_mysql(last, lookback_seconds),
        Source::ClickHouse(_) => lookback_lower_bound_clickhouse(
            last,
            lookback_seconds,
            watermark_type.filter(|t| is_clickhouse_temporal_type(t)),
        ),
        Source::BigQuery(_) => {
            unreachable!("BigQuery is handled via the early return in run_transfer")
        }
    }
}

/// `MAX(watermark)` through `source_query` (or on `source_table` when there is
/// no query), on the setup connection when there is one.
async fn query_max_watermark(
    source: &Source,
    control: Option<&mut ControlConn>,
    cfg: &TransferConfig,
) -> Result<Option<String>> {
    let w = cfg.watermark.as_deref().unwrap_or_default();
    let (t, q) = (cfg.source_table.as_deref(), cfg.source_query.as_deref());
    let expr = cfg.watermark_source_expr.as_deref();
    match (source, control) {
        (Source::Postgres(s), Some(ControlConn::Postgres(c))) => {
            s.max_watermark(c, t, q, w, expr).await
        }
        (Source::Postgres(s), _) => s.max_watermark(&s.connect().await?, t, q, w, expr).await,
        (Source::MySql(s), Some(ControlConn::MySql(c))) => s.max_watermark(c, t, q, w, expr).await,
        (Source::MySql(s), _) => {
            s.max_watermark(&mut s.connect().await?, t, q, w, expr)
                .await
        }
        (Source::ClickHouse(s), _) => Ok(s.max_watermark(t, q, w, expr).await?.0),
        (Source::BigQuery(_), _) => {
            unreachable!("BigQuery is handled via the early return in run_transfer")
        }
    }
}

/// Refuse to read when the lower bound evaluates to NULL on the source.
///
/// `watermark > NULL` matches no row, so the run would read 0 rows and
/// succeed, the cursor would never move, and every run after it would do the
/// same. That is how a `+00` cursor MySQL couldn't parse (issue #2) froze 19
/// tables for 12 days with every run green. Whatever puts an unparseable
/// cursor in the state table next, this fails the first run it affects. One
/// round trip that touches no table.
async fn ensure_lower_bound_not_null(
    source: &Source,
    control: Option<&mut ControlConn>,
    bound: &str,
    cfg: &TransferConfig,
    cursor: &str,
    from_state: bool,
) -> Result<()> {
    // The setup connection can be gone by now: a key-bounds probe that failed
    // on it may have been a dropped connection. Then a fresh one asks.
    let is_null = match (source, control) {
        (Source::Postgres(s), Some(ControlConn::Postgres(c))) => {
            match PgSource::is_null_on(c, bound).await {
                Err(e) if e.is_transient_source() => s.is_null(bound).await?,
                other => other?,
            }
        }
        (Source::MySql(s), Some(ControlConn::MySql(c))) => {
            match MySqlSource::is_null_on(c, bound).await {
                Err(e) if e.is_transient_source() => s.is_null(bound).await?,
                other => other?,
            }
        }
        (Source::Postgres(s), _) => s.is_null(bound).await?,
        (Source::MySql(s), _) => s.is_null(bound).await?,
        (Source::ClickHouse(s), _) => s.is_null(bound).await?,
        (Source::BigQuery(_), _) => {
            unreachable!("BigQuery is handled via the early return in run_transfer")
        }
    };
    if !is_null {
        return Ok(());
    }
    let origin = if from_state {
        "saved cursor"
    } else {
        "seed_watermark"
    };
    Err(EtlError::other(format!(
        "lower bound for state_key '{key}' evaluates to NULL on the source ({origin} \
         '{cursor}', bound {bound}). `{watermark} > NULL` matches no row, so this run, and \
         every run after it, would read 0 rows and report success. The {origin} is in a form \
         the source can't parse: correct it, or append a state row with a cursor the source \
         can compare.",
        key = cfg.effective_state_key(),
        watermark = cfg.watermark.as_deref().unwrap_or("?"),
    )))
}

/// What the `MAX(watermark)` probe says about the cursor, from values the run
/// already has — no extra query.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CursorCheck {
    watermark: String,
    cursor: String,
    max: String,
    /// The cursor is past everything the source holds.
    cursor_ahead: bool,
    /// The source's MAX is past the lower bound, so the read has to return at
    /// least the row(s) holding it: they satisfy both `> lower` and `<= MAX`.
    max_above_lower_bound: bool,
}

/// Compare the cursor with the source's MAX, as the watermark's type orders
/// them. `None` when either value doesn't parse as that type, or one carries a
/// UTC offset and the other doesn't: ordering those would mean guessing the
/// naive one's zone, and a wrong guess raises a false warning.
fn check_cursor_against_max(
    watermark: &str,
    cursor: &str,
    max: &str,
    lookback_seconds: u64,
    source_cols: &[ColumnType],
) -> Option<CursorCheck> {
    let arrow = &source_cols.iter().find(|c| c.name == watermark)?.arrow;
    let (c, m, lookback) = match arrow {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => (
            cursor.trim().parse::<i64>().ok()?,
            max.trim().parse::<i64>().ok()?,
            0,
        ),
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {
            let (c, c_offset) = parse_temporal_micros(cursor)?;
            let (m, m_offset) = parse_temporal_micros(max)?;
            if c_offset != m_offset {
                return None;
            }
            // BigQuery rounds a DATE lookback up to whole days, so its real
            // bound can only sit lower than this one: never a false warning.
            (
                c,
                m,
                i64::try_from(lookback_seconds)
                    .ok()?
                    .checked_mul(1_000_000)?,
            )
        }
        _ => return None,
    };
    Some(CursorCheck {
        watermark: watermark.to_string(),
        cursor: cursor.to_string(),
        max: max.to_string(),
        cursor_ahead: c > m,
        max_above_lower_bound: m > c.saturating_sub(lookback),
    })
}

/// A temporal watermark literal as microseconds since the epoch, and whether
/// it carried a UTC offset (already applied when it did). Takes what the MAX
/// probes and the stream cursor render: `YYYY-MM-DD`, and
/// `YYYY-MM-DD[ T]HH:MM:SS[.ffffff]` bare or with `+07`, `+05:30` or `Z`.
fn parse_temporal_micros(v: &str) -> Option<(i64, bool)> {
    let v = v.trim();
    let zoned = v.strip_suffix('Z').map(|n| format!("{n}+00"));
    let zoned = zoned.as_deref().unwrap_or(v);
    for f in ["%Y-%m-%d %H:%M:%S%.f%#z", "%Y-%m-%dT%H:%M:%S%.f%#z"] {
        if let Ok(dt) = chrono::DateTime::parse_from_str(zoned, f) {
            return Some((dt.timestamp_micros(), true));
        }
    }
    for f in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(v, f) {
            return Some((dt.and_utc().timestamp_micros(), false));
        }
    }
    let d = chrono::NaiveDate::parse_from_str(v, "%Y-%m-%d").ok()?;
    Some((d.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros(), false))
}

impl CursorCheck {
    /// `watermark_ahead_of_source`: raised before the read. `filtered` is
    /// `Some` when the MAX came through `source_query` and the unfiltered table
    /// couldn't settle whether the filter explains it, with a sentence saying
    /// why: see [`judge_cursor_ahead_of_query`].
    fn warn_if_cursor_ahead(
        &self,
        cfg: &TransferConfig,
        filtered: Option<&str>,
        warnings: &Warnings,
    ) {
        if !self.cursor_ahead {
            return;
        }
        let mut message = format!(
            "the incremental cursor for state_key '{key}' ({cursor}) is ahead of the source's \
             MAX({w}) ({max}). ",
            key = cfg.effective_state_key(),
            cursor = self.cursor,
            w = self.watermark,
            max = self.max,
        );
        if let Some(why) = filtered {
            message.push_str(&format!(
                "That MAX is computed through source_query, and a filter that excludes the \
                 newest rows (a rolling time window on a quiet table, say) puts it below a \
                 correct cursor. {why} This run saves that MAX as the cursor all the same: \
                 moving it back costs a re-read at most, while keeping a cursor that really is \
                 ahead would skip rows. Otherwise the typical causes are a cursor converted into \
                 the wrong time zone, or one seeded from another table."
            ));
        } else {
            message.push_str(
                "Typical causes: a cursor converted into the wrong time zone, one seeded from \
                 another table, or the source's newest rows deleted. This run saves the source's \
                 MAX as the cursor. If the cursor had been shifted, rows between its true \
                 position and that MAX may never have been read; re-sync that range.",
            );
        }
        tracing::warn!("{message}");
        warnings.push(TransferWarning {
            kind: WarningKind::WatermarkAheadOfSource,
            column: Some(self.watermark.clone()),
            count: 0,
            sample: Some(self.cursor.clone()),
            message,
        });
    }

    /// `watermark_not_advanced`: raised after the read, when it returned
    /// nothing although the probe says it had to return something.
    fn warn_if_not_advanced(&self, cfg: &TransferConfig, rows_read: u64, warnings: &Warnings) {
        if !self.max_above_lower_bound || rows_read > 0 {
            return;
        }
        let message = format!(
            "the source's MAX({w}) is {max}, past this run's lower bound (cursor {cursor}, \
             lookback_seconds={lookback}), yet the read returned 0 rows for state_key '{key}'. \
             The row holding that MAX satisfies the filter, so something between the bound and \
             the predicate is wrong, typically a cursor the source compares differently than it \
             was written. (Or that row changed between the MAX probe and the read, in which \
             case the next run reads it.)",
            w = self.watermark,
            max = self.max,
            cursor = self.cursor,
            lookback = cfg.lookback_seconds,
            key = cfg.effective_state_key(),
        );
        tracing::warn!("{message}");
        warnings.push(TransferWarning {
            kind: WarningKind::WatermarkNotAdvanced,
            column: Some(self.watermark.clone()),
            count: 0,
            sample: Some(self.cursor.clone()),
            message,
        });
    }
}

/// What the incremental setup decided about the cursor, for after the read.
#[derive(Debug, Default)]
struct CursorPlan {
    /// The cursor this run started from: the committed one, or the first run's
    /// seed.
    last: Option<String>,
    /// `last` is a seed (or absent): no cursor was saved yet.
    seeded: bool,
    /// The cursor is ahead of the source for real: save the MAX, moving it
    /// back.
    rewind_to_max: bool,
    /// What a cursor moved back is never moved below: see [`cursor_floor`].
    floor: Option<String>,
    /// The run resumes an interrupted chunked read past a marker that froze
    /// an upper bound, which the read is held to and saves as the cursor:
    /// see [`MarkerBound`].
    pinned: bool,
    /// The run resumes past a marker that froze none, so it can't account
    /// for the chunks before it and keeps the committed cursor: see
    /// [`MarkerBound::Unbounded`]. A first run, which has none to keep, takes
    /// one from what it reads, as a fresh read would.
    keep_committed: bool,
}

/// The cursor a cursor moved back is never moved below: the committed one,
/// or on a first run a seed, but only when it parses in the tracker's own
/// unit. PostgreSQL reads an offset-less seed of a `timestamptz` in the
/// session's zone, so taking it as UTC could put the floor hours later than
/// the read's own lower bound, and cancel the rewind it limits.
fn cursor_floor(
    committed: Option<&str>,
    seed: Option<&str>,
    tracker: Option<&WatermarkTracker>,
) -> Option<String> {
    match committed {
        Some(c) => Some(c.to_string()),
        None => seed
            .filter(|s| tracker.is_some_and(|t| t.parse(s).is_some()))
            .map(str::to_string),
    }
}

/// What a cursor ahead of `source_query`'s MAX means. Whatever it is, the run
/// saves that MAX as the cursor, as it always has: moving the cursor back costs
/// a re-read, while keeping one that really is ahead (shifted by a time-zone
/// conversion, say) would skip every row that arrives below it. What changes
/// is only what is reported.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CursorAhead {
    /// Ahead of the source itself: the cursor is wrong. Warn.
    Real,
    /// Ahead of the filtered MAX but within the unfiltered table's: the
    /// filter explains it. Say nothing.
    WithinTable,
    /// Ahead of the filtered MAX, with no unfiltered MAX to tell why. Warn,
    /// with this sentence saying why the table didn't settle it.
    Unexplained(String),
}

/// Judge a cursor that is ahead of the MAX probed through `source_query`.
///
/// A filter that excludes the newest rows puts that MAX below a correct
/// cursor: a rolling `WHERE write_date >= now() - interval '3 days'` on a quiet
/// table, once the rows age out of it. Moving the cursor back to that MAX
/// only makes the next run re-read more, but the warning that came with it
/// sent the operator after a time-zone bug that isn't there. So the cursor is
/// compared against `source_table`'s own MAX when that is set, and the
/// warning names the filter when nothing can tell.
///
/// A quiet table hits this on every run, so the table's MAX is held to
/// `probe_max_cost` like the other probes: a full scan on each run to settle a
/// warning is not worth it.
async fn judge_cursor_ahead_of_query(
    source: &Source,
    control: Option<&mut ControlConn>,
    cfg: &TransferConfig,
    check: &CursorCheck,
    source_cols: &[ColumnType],
) -> CursorAhead {
    let w = check.watermark.as_str();
    let Some(table) = cfg.source_table.as_deref() else {
        return CursorAhead::Unexplained(
            "Set source_table as well to have the cursor checked against the unfiltered table \
             instead."
                .to_string(),
        );
    };
    let expr = cfg.watermark_source_expr.as_deref();
    let too_costly = |cost: ProbeCost| {
        cost.should_skip(cfg.probe_max_cost).then(|| {
            CursorAhead::Unexplained(format!(
                "source_table's own MAX({w}) was not read to check it: {} (probe_max_cost={}).",
                cost.describe(),
                cfg.probe_max_cost
            ))
        })
    };
    let table_max = match (source, control) {
        (Source::Postgres(s), Some(ControlConn::Postgres(c))) => {
            let sql = PgSource::max_watermark_sql(Some(table), None, w, expr);
            if let Some(v) = too_costly(s.explain_cost(c, &sql).await) {
                return v;
            }
            s.max_watermark(c, Some(table), None, w, expr).await
        }
        (Source::MySql(s), Some(ControlConn::MySql(c))) => {
            let sql = MySqlSource::max_watermark_sql(Some(table), None, w, expr);
            if let Some(v) = too_costly(s.explain_cost(c, &sql).await) {
                return v;
            }
            s.max_watermark(c, Some(table), None, w, expr).await
        }
        (Source::Postgres(_) | Source::MySql(_), _) => Err(EtlError::internal(
            "the setup connection is gone before the cursor check",
        )),
        (Source::ClickHouse(s), _) => s
            .max_watermark(Some(table), None, w, expr)
            .await
            .map(|(max, _)| max),
        (Source::BigQuery(_), _) => {
            unreachable!("BigQuery is handled via the early return in run_transfer")
        }
    };
    let unexplained = || {
        CursorAhead::Unexplained(format!(
            "source_table's own MAX({w}) gave nothing to check it against."
        ))
    };
    let table_max = match table_max {
        Ok(Some(m)) => m,
        Ok(None) => return unexplained(),
        // source_query may rename or derive the column, so the table need not
        // have one by that name.
        Err(e) => {
            return CursorAhead::Unexplained(format!(
                "source_table's own MAX({w}) could not be read to check it ({e})."
            ))
        }
    };
    match check_cursor_against_max(
        w,
        &check.cursor,
        &table_max,
        cfg.lookback_seconds,
        source_cols,
    ) {
        Some(c) if c.cursor_ahead => CursorAhead::Real,
        Some(_) => CursorAhead::WithinTable,
        None => unexplained(),
    }
}

/// How far to move a stream-derived cursor back, in seconds, after a read of
/// several statements that took `read_secs`.
///
/// Each window (or partition, or chunk) reads its own snapshot, one after
/// another. A row in a window already read can be updated mid-read while a
/// row in a later window is updated after it, and the cursor lands on the
/// later update. The next run starts at `cursor - lookback`, which is past
/// the first update once the read took longer than the lookback, so that
/// row is never read again. Moving the cursor back by the excess keeps
/// `cursor - lookback` at or before the read's start: the stream's maximum
/// can't be later than the read's end, as long as the watermark follows the
/// source's clock. 0 when the read fit inside the lookback.
fn stream_cursor_rewind_secs(read_secs: f64, lookback_seconds: u64) -> u64 {
    (read_secs.max(0.0).ceil() as u64).saturating_sub(lookback_seconds)
}

/// Report an incremental read that returned rows but no cursor: every row
/// read had a NULL watermark. With no cursor saved, the next run has no lower
/// bound and reads them all again, as does every run after it, while the
/// NULL-count probe that raises `null_watermark` may have been skipped as too
/// costly. Raised from the read itself so that can't hide it, unless that
/// probe already reported the column.
fn warn_on_unwatermarked_read(cfg: &TransferConfig, rows_read: u64, warnings: &Warnings) {
    let w = cfg.watermark.as_deref().unwrap_or("?");
    let message = format!(
        "the incremental read for state_key '{key}' returned {rows_read} row(s), but none had \
         a non-NULL '{w}', so there is no cursor to save (cursor unchanged). The next run has \
         no lower bound either and reads every row again, as will each run after it. Populate \
         '{w}', pick a watermark column that is set on every row, or load the table with \
         mode=\"full\". Once a cursor exists, `WHERE {w} > x` never matches the NULL rows.",
        key = cfg.effective_state_key(),
    );
    tracing::warn!("{message}");
    if warnings.contains(WarningKind::NullWatermark, Some(w)) {
        return;
    }
    warnings.push(TransferWarning {
        kind: WarningKind::NullWatermark,
        column: Some(w.to_string()),
        count: rows_read,
        sample: None,
        message,
    });
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
    tz_aware: bool,
) -> Option<String> {
    let col = source_expr
        .map(str::to_string)
        .unwrap_or_else(|| format!("\"{}\"", watermark.replace('"', "\"\"")));
    let lower = last.map(|l| lookback_lower_bound_pg(l, lookback_seconds, tz_aware));
    let upper = snapshot_max.map(quote_sql_literal);
    build_watermark_filter(&col, lower, upper)
}

/// Whether the watermark column resolved as a tz-aware `timestamptz`
/// (`Timestamp(_, Some(_))`) rather than a naive `timestamp` — see
/// [`lookback_lower_bound_pg`] for why the distinction matters. `false` for
/// any non-temporal or unresolved column, which keeps today's (correct only
/// for a UTC session) `::timestamp` cast as the fallback.
fn watermark_pg_is_tz_aware(watermark: &str, source_cols: &[ColumnType]) -> bool {
    source_cols
        .iter()
        .find(|c| c.name == watermark)
        .is_some_and(|c| matches!(c.arrow, DataType::Timestamp(_, Some(_))))
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

/// ClickHouse uses backtick identifier quoting and MySQL-style backslash
/// escaping in string literals, so the literals are quoted the same way as
/// MySQL's. What differs is how the cursor is read back.
///
/// The cursor is `toString(max(col))`, which ClickHouse renders in the column's
/// own timezone: the declared one (`DateTime('Asia/Jakarta')`), or the
/// server's for a bare `DateTime`. A bare `'...'` literal is not a safe way to
/// read that back, because `select_sql` projects the column as
/// `CAST(col AS DateTime64(6, 'UTC')) AS col` and a WHERE on `col` binds to
/// that alias, which parses the literal as UTC. So a temporal cursor is
/// written as `CAST('...' AS <watermark_type>)`: it parses in the same zone
/// `toString` rendered it in, and the comparison is then between instants,
/// whichever of the column or the alias it binds to. Cursors already persisted
/// keep working, since their format does not change.
///
/// `watermark_type` is the watermark's `toTypeName` from
/// `ClickHouseSource::max_watermark`. Non-temporal watermarks (integers,
/// strings) keep plain literals.
fn build_watermark_filter_clickhouse(
    watermark: &str,
    source_expr: Option<&str>,
    last: Option<&str>,
    snapshot_max: Option<&str>,
    lookback_seconds: u64,
    watermark_type: Option<&str>,
) -> Option<String> {
    let col = source_expr
        .map(str::to_string)
        .unwrap_or_else(|| crate::ddl::quote_ident(watermark));
    let temporal_type = watermark_type.filter(|t| is_clickhouse_temporal_type(t));
    let lower = last.map(|l| lookback_lower_bound_clickhouse(l, lookback_seconds, temporal_type));
    let upper = snapshot_max.map(|u| clickhouse_cursor_literal(u, temporal_type));
    build_watermark_filter(&col, lower, upper)
}

/// ClickHouse treats backslash as an active escape inside `'...'` (and also
/// accepts the doubled-quote convention), so — same failure mode as the MySQL
/// and BigQuery literal quoters — backslash has to be escaped first or a
/// trailing one would escape the literal's own closing quote.
fn quote_clickhouse_literal(m: &str) -> String {
    format!("'{}'", m.replace('\\', "\\\\").replace('\'', "''"))
}

/// A `Date`/`Date32`/`DateTime`/`DateTime64` type, possibly wrapped in
/// `Nullable(...)` or `LowCardinality(...)`. The type text comes from the
/// server and is spliced into SQL unquoted, so anything with characters a
/// type name never needs is refused (and the cursor falls back to a plain
/// literal).
fn is_clickhouse_temporal_type(t: &str) -> bool {
    let safe = !t.contains("--")
        && t.chars()
            .all(|c| c.is_ascii_alphanumeric() || " (),'/_+-.".contains(c));
    let mut inner = t.trim();
    loop {
        let unwrapped = ["Nullable(", "LowCardinality("]
            .iter()
            .find_map(|w| inner.strip_prefix(w).and_then(|r| r.strip_suffix(')')));
        match unwrapped {
            Some(rest) => inner = rest.trim(),
            None => break,
        }
    }
    safe && inner.starts_with("Date")
}

/// The cursor as a ClickHouse literal: `CAST('...' AS <type>)` for a temporal
/// watermark (see `build_watermark_filter_clickhouse`), plain `'...'` otherwise.
fn clickhouse_cursor_literal(value: &str, temporal_type: Option<&str>) -> String {
    let quoted = quote_clickhouse_literal(value);
    match temporal_type {
        Some(t) => format!("CAST({quoted} AS {t})"),
        None => quoted,
    }
}

/// Widen `last`'s lower bound by `lookback_seconds` using ClickHouse's own
/// parse-and-subtract. `lookback_seconds == 0` returns the same literal as the
/// upper bound would use. `ensure_lookback_compatible` has already restricted
/// lookback to a date/timestamp watermark, so `temporal_type` is always known
/// here in practice; the `toDateTime64(..., 'UTC')` branch is only a fallback
/// for a type the server did not report.
fn lookback_lower_bound_clickhouse(
    last: &str,
    lookback_seconds: u64,
    temporal_type: Option<&str>,
) -> String {
    if lookback_seconds == 0 {
        return clickhouse_cursor_literal(last, temporal_type);
    }
    match temporal_type {
        Some(_) => format!(
            "({} - INTERVAL {lookback_seconds} SECOND)",
            clickhouse_cursor_literal(last, temporal_type)
        ),
        None => format!(
            "(toDateTime64({}, 6, 'UTC') - INTERVAL {lookback_seconds} SECOND)",
            quote_clickhouse_literal(last)
        ),
    }
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
fn lookback_lower_bound_pg(last: &str, lookback_seconds: u64, tz_aware: bool) -> String {
    let l = last.replace('\'', "''");
    if lookback_seconds == 0 {
        return format!("'{l}'");
    }
    // The cursor for a tz-aware column is rendered with its UTC offset
    // (`...+00`, see `WatermarkTracker::render`), and a bare `::timestamp`
    // cast silently drops that offset, reinterpreting the wall-clock digits in
    // the session's own TimeZone (which quickhouse never sets, so it is
    // whatever the server defaults to) instead of UTC. `::timestamptz` keeps
    // the offset the string carries, so the round trip is exact whatever the
    // session zone is. A naive `timestamp` column has no offset to lose, so it
    // keeps the cast it always used.
    let cast = if tz_aware { "timestamptz" } else { "timestamp" };
    format!("('{l}'::{cast} - interval '{lookback_seconds} seconds')")
}

fn lookback_lower_bound_mysql(last: &str, lookback_seconds: u64) -> String {
    let l = strip_zero_utc_offset(last)
        .replace('\\', "\\\\")
        .replace('\'', "''");
    if lookback_seconds == 0 {
        return format!("'{l}'");
    }
    format!("(CAST('{l}' AS DATETIME) - INTERVAL {lookback_seconds} SECOND)")
}

/// A MySQL cursor with its zero UTC offset (`+00` or `+00:00`) removed, and
/// nothing converted. 0.18 to 0.20.1 saved a stream-derived cursor on a
/// tz-aware watermark as `...+00`, which MySQL can't compare (see
/// [`WatermarkTracker::new`]); every run after that read 0 rows and succeeded.
/// Stripping it on the way back in is what lets a table frozen that way resume
/// on upgrade with no state edit. Only a datetime is touched, and only a zero
/// offset: any other offset is left for MySQL to convert as it documents.
fn strip_zero_utc_offset(cursor: &str) -> &str {
    for suffix in ["+00:00", "+00"] {
        if let Some(naive) = cursor.strip_suffix(suffix) {
            let is_datetime = ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"]
                .iter()
                .any(|f| chrono::NaiveDateTime::parse_from_str(naive, f).is_ok());
            if is_datetime {
                return naive;
            }
        }
    }
    cursor
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

/// Whether a row whose watermark is NULL can be written: not where the
/// destination column can't be NULL, a ClickHouse ReplacingMergeTree's
/// version column (the watermark) or a key, and the insert would fail on
/// the first one. A watermark the transfer leaves out has no column to fail.
fn null_watermark_lands(plan: &SelectPlan, watermark: &str) -> bool {
    plan.source_columns
        .iter()
        .position(|c| c == watermark)
        .and_then(|i| plan.dest_columns.get(i))
        .map_or(true, |c| c.nullable)
}

/// The watermark as a read's filter names it: `watermark_source_expr` when
/// set, else the column, quoted for the source's dialect.
fn watermark_column_sql(source: &Source, watermark: &str, source_expr: Option<&str>) -> String {
    match (source_expr, source) {
        (Some(e), _) => e.to_string(),
        (None, Source::Postgres(_)) => format!("\"{}\"", watermark.replace('"', "\"\"")),
        (None, Source::ClickHouse(_)) => crate::ddl::quote_ident(watermark),
        (None, Source::MySql(_) | Source::BigQuery(_)) => quote_my(watermark),
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
            declared_decimal: None,
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

    /// One step of a scripted `COPY` byte stream for exercising `CopyChunks`.
    enum Step {
        Item(std::result::Result<Bytes, &'static str>),
        /// The next message has not arrived yet: `Pending`, re-waking itself.
        NotYet,
        End,
    }

    fn row(bytes: &'static [u8]) -> Step {
        Step::Item(Ok(Bytes::from_static(bytes)))
    }

    /// Plays back a script. Polling past the end of the script reports an
    /// error, the way tokio-postgres's `CopyOutStream` does if it is polled
    /// again after `CopyDone`.
    struct Scripted(std::collections::VecDeque<Step>);

    impl futures::Stream for Scripted {
        type Item = std::result::Result<Bytes, &'static str>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            use std::task::Poll;
            match self.0.pop_front() {
                Some(Step::Item(item)) => Poll::Ready(Some(item)),
                Some(Step::NotYet) => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Some(Step::End) => Poll::Ready(None),
                None => Poll::Ready(Some(Err("polled after the end"))),
            }
        }
    }

    fn chunks_of(steps: Vec<Step>) -> CopyChunks<Scripted, &'static str> {
        CopyChunks::new(Scripted(steps.into()))
    }

    #[tokio::test]
    async fn copy_chunks_join_the_rows_that_have_already_arrived() {
        let mut chunks = chunks_of(vec![row(b"row1"), row(b"row2"), row(b"row3"), Step::End]);
        assert_eq!(
            chunks.next_chunk().await,
            Some(Ok(Bytes::from_static(b"row1row2row3")))
        );
        // The drain already saw the end. The fused stream is not polled past
        // it — `Scripted`, like `CopyOutStream`, would answer with an error.
        assert_eq!(chunks.next_chunk().await, None);
        assert_eq!(chunks.next_chunk().await, None);
    }

    #[tokio::test]
    async fn copy_chunks_never_wait_for_rows_that_have_not_arrived() {
        let mut chunks = chunks_of(vec![row(b"row1"), Step::NotYet, row(b"row2"), Step::End]);
        assert_eq!(
            chunks.next_chunk().await,
            Some(Ok(Bytes::from_static(b"row1")))
        );
        assert_eq!(
            chunks.next_chunk().await,
            Some(Ok(Bytes::from_static(b"row2")))
        );
        assert_eq!(chunks.next_chunk().await, None);
    }

    #[tokio::test]
    async fn copy_chunks_return_the_rows_ahead_of_an_error_first() {
        let mut chunks = chunks_of(vec![
            row(b"row1"),
            row(b"row2"),
            Step::Item(Err("connection reset")),
            row(b"row3"),
        ]);
        assert_eq!(
            chunks.next_chunk().await,
            Some(Ok(Bytes::from_static(b"row1row2")))
        );
        assert_eq!(chunks.next_chunk().await, Some(Err("connection reset")));
    }

    #[tokio::test]
    async fn copy_chunks_close_at_the_ceiling_and_pass_a_bigger_row_whole() {
        let big = Bytes::from(vec![9u8; COPY_CHUNK_BYTES + 1024]);
        let small = Bytes::from(vec![7u8; COPY_CHUNK_BYTES / 2 + 1]);
        let mut steps = vec![Step::Item(Ok(big.clone()))];
        steps.extend((0..3).map(|_| Step::Item(Ok(small.clone()))));
        steps.push(Step::End);
        let mut chunks = chunks_of(steps);

        let mut sizes = Vec::new();
        let mut all = Vec::new();
        while let Some(chunk) = chunks.next_chunk().await {
            let chunk = chunk.unwrap();
            sizes.push(chunk.len());
            all.extend_from_slice(&chunk);
        }
        // A row over the ceiling goes through alone and unsplit; the next chunk
        // closes on the row that takes it past the ceiling.
        assert_eq!(sizes, vec![big.len(), 2 * small.len(), small.len()]);
        let mut expected = big.to_vec();
        for _ in 0..3 {
            expected.extend_from_slice(&small);
        }
        assert_eq!(all, expected);
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
    const CHEAP: ProbeCost = ProbeCost::Known(2.07);
    const DEAR: ProbeCost = ProbeCost::Known(3_031_034.0);
    const T: f64 = crate::source::DEFAULT_PROBE_MAX_COST;

    #[test]
    fn cheap_probes_are_all_still_run() {
        // The backward-compatibility guard: where the planner says both probes
        // are index lookups, nothing changes and nothing is reported.
        let p = plan_watermark_probes(CHEAP, CHEAP, T, 86_400);
        assert!(p.count_nulls);
        assert!(!p.stream_max, "a cheap MAX must still be used as the bound");
        assert!(!p.max_too_dear && !p.count_too_dear);
    }

    #[test]
    fn costly_probes_are_skipped_on_an_ongoing_run() {
        let p = plan_watermark_probes(DEAR, DEAR, T, 86_400);
        assert!(
            !p.count_nulls,
            "a full-scan count is pure overhead per tick"
        );
        assert!(
            p.stream_max,
            "a full-scan MAX should give way to the stream"
        );
        assert!(p.max_too_dear && p.count_too_dear);
    }

    #[test]
    fn even_a_first_run_skips_a_completeness_count_it_cannot_afford() {
        // This reverses an earlier decision, deliberately.
        //
        // The reasoning for paying on a first run was sound in isolation: the
        // read touches the whole table anyway, and a pipeline that starts out
        // excluding NULL-watermark rows excludes them forever. But on a large
        // table with an unindexed watermark, read from a hot standby, this
        // scan does not merely cost — it never finishes. Measured, it was
        // cancelled with 40001 on three consecutive attempts, which made the
        // first run fail before reading anything, so no first run could ever
        // complete to make a later one cheaper. A check that cannot run is not
        // a check.
        //
        // What protects the user instead is that the skip is *reported*:
        // `null_check_skipped` states the check did not run rather than
        // implying it passed.
        let p = plan_watermark_probes(DEAR, DEAR, T, 86_400);
        assert!(
            !p.count_nulls,
            "an unaffordable check must not be attempted"
        );
        assert!(p.count_too_dear, "and the user must be told it was skipped");
    }

    #[test]
    fn a_first_run_still_runs_a_completeness_count_it_can_afford() {
        // The check is only dropped when the planner says it is unaffordable.
        // When it is cheap, a first run still verifies completeness.
        let p = plan_watermark_probes(CHEAP, DEAR, T, 86_400);
        assert!(p.count_nulls);
    }

    #[test]
    fn a_costly_null_count_alone_is_not_an_unindexed_watermark() {
        // Issue #16, case 1: an indexed, nullable watermark with 1.9M NULLs.
        // MAX comes straight from the index, while the planner prices the
        // NULL count at 100,312. Only the count is skipped; the read is a
        // range scan, so nothing may switch a sweep on.
        let p = plan_watermark_probes(DEAR, CHEAP, T, 86_400);
        assert!(!p.count_nulls);
        assert!(!p.max_too_dear, "the MAX is what decides the sweep");
        assert!(!p.stream_max);
        assert!(p.count_too_dear);
    }

    #[test]
    fn stream_max_requires_a_lookback_window() {
        // Without a trailing re-scan nothing re-covers a row written mid-read
        // that the scan had already passed, so the MAX scan is paid for.
        let p = plan_watermark_probes(DEAR, DEAR, T, 0);
        assert!(!p.stream_max);
        assert!(p.max_too_dear);
    }

    #[test]
    fn an_unreachable_planner_skips_rather_than_scans() {
        // The inverse of the earlier bug, where a cancelled probe silently
        // upgraded the plan to "run the full scan and say nothing".
        let p = plan_watermark_probes(ProbeCost::Unknown, ProbeCost::Unknown, T, 86_400);
        assert!(!p.count_nulls);
        assert!(p.stream_max);
        assert!(p.max_too_dear && p.count_too_dear);
    }

    #[test]
    fn zero_threshold_restores_unconditional_probing() {
        let p = plan_watermark_probes(DEAR, DEAR, 0.0, 86_400);
        assert!(p.count_nulls);
        assert!(!p.stream_max);
        assert!(!p.max_too_dear && !p.count_too_dear);
    }

    fn report(
        watermark: &'static str,
        count: ProbeCost,
        max: ProbeCost,
        nullable: bool,
        index_ddl: String,
    ) -> Vec<TransferWarning> {
        let warnings = Warnings::default();
        ProbeReport {
            watermark,
            plan: plan_watermark_probes(count.clone(), max.clone(), T, 86_400),
            count_cost: count,
            max_cost: max,
            nullable,
            lookback_seconds: 86_400,
            swept: true,
            suggest_source_table: false,
            index_ddl,
        }
        .warn(&warnings);
        warnings.drain()
    }

    #[test]
    fn the_warnings_name_the_evidence_and_the_skipped_check() {
        let out = report(
            "write_date",
            DEAR,
            DEAR,
            true,
            pg_watermark_index_ddl("write_date"),
        );
        let kinds = out.iter().map(|w| w.kind).collect::<Vec<_>>();
        assert_eq!(
            kinds,
            vec![
                WarningKind::UnindexedWatermark,
                WarningKind::NullCheckSkipped
            ]
        );
        let unindexed = &out[0];
        assert_eq!(unindexed.column.as_deref(), Some("write_date"));
        // The planner estimate must be quoted, so an operator can act on it.
        assert!(
            unindexed.message.contains("3031034"),
            "{}",
            unindexed.message
        );
        assert!(unindexed.message.contains("CREATE INDEX CONCURRENTLY"));
        assert!(
            !unindexed.message.contains("SKIPPED"),
            "{}",
            unindexed.message
        );
        let skipped = &out[1];
        assert_eq!(skipped.column.as_deref(), Some("write_date"));
        assert!(skipped.message.contains("SKIPPED"), "{}", skipped.message);
        assert!(skipped.message.contains("3031034"), "{}", skipped.message);
    }

    #[test]
    fn an_indexed_watermark_with_many_nulls_reports_only_the_skipped_count() {
        let out = report(
            "updated_date",
            DEAR,
            CHEAP,
            true,
            mysql_watermark_index_ddl("x"),
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind, WarningKind::NullCheckSkipped);
    }

    #[test]
    fn the_warning_gives_mysql_its_own_index_syntax() {
        // `CREATE INDEX CONCURRENTLY` is PostgreSQL-only; MySQL rejects it.
        let out = report(
            "updated_date",
            DEAR,
            DEAR,
            true,
            mysql_watermark_index_ddl("updated_date"),
        );
        let msg = &out[0].message;
        assert_eq!(out[0].kind, WarningKind::UnindexedWatermark);
        assert!(
            msg.contains(
                "ALTER TABLE <table> ADD INDEX (`updated_date`), ALGORITHM=INPLACE, LOCK=NONE"
            ),
            "{msg}"
        );
        assert!(!msg.contains("CONCURRENTLY"), "{msg}");
    }

    #[test]
    fn no_warning_when_both_probes_are_cheap() {
        let out = report(
            "write_date",
            CHEAP,
            CHEAP,
            true,
            pg_watermark_index_ddl("x"),
        );
        assert!(out.is_empty());
    }

    #[test]
    fn the_warning_omits_the_skip_note_for_a_not_null_watermark() {
        // Nothing was skipped, because the count never runs on a NOT NULL
        // column — the warning must not claim a lost check.
        let out = report(
            "write_date",
            ProbeCost::Known(0.0),
            DEAR,
            false,
            pg_watermark_index_ddl("write_date"),
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind, WarningKind::UnindexedWatermark);
        assert!(
            !out[0].message.contains("SKIPPED the check"),
            "{}",
            out[0].message
        );
    }

    #[test]
    fn a_table_max_is_probed_only_when_the_tracker_can_take_the_cursor() {
        let mut cfg = crate::config::default_test_config();
        cfg.source_table = Some("t".into());
        cfg.source_query = Some("SELECT * FROM t WHERE is_test = 0".into());
        let cols = [
            col_typed("id", DataType::Int64),
            col_typed(
                "updated_date",
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            col_typed("name", DataType::Utf8),
        ];
        assert!(table_max_eligible(&cfg, &cols, "id", None));
        assert!(table_max_eligible(&cfg, &cols, "updated_date", None));
        assert!(
            !table_max_eligible(&cfg, &cols, "name", None),
            "no tracker for text"
        );

        // The filter is evaluated over the query's columns, the table's MAX
        // over the table's: a source expression can't be trusted on both.
        cfg.watermark_source_expr = Some("updated_raw".into());
        assert!(!table_max_eligible(&cfg, &cols, "updated_date", None));
        cfg.watermark_source_expr = None;
        cfg.column_transforms.insert(
            "updated_date".into(),
            "updated_date + INTERVAL 1 HOUR".into(),
        );
        assert!(!table_max_eligible(&cfg, &cols, "updated_date", None));
        cfg.column_transforms.clear();

        // Not for a chunked read, nor a watermark left out or overridden.
        cfg.chunk_rows = Some(1000);
        assert!(!table_max_eligible(&cfg, &cols, "id", None));
        cfg.chunk_rows = None;
        cfg.exclude = vec!["id".into()];
        assert!(!table_max_eligible(&cfg, &cols, "id", None));
        cfg.exclude.clear();
        cfg.include = vec!["updated_date".into()];
        assert!(!table_max_eligible(&cfg, &cols, "id", None));
        cfg.include.clear();
        cfg.type_overrides.insert("id".into(), "String".into());
        assert!(!table_max_eligible(&cfg, &cols, "id", None));
        cfg.type_overrides.clear();

        // A skip_to_max first run seeds from the MAX: a filtered-out row past
        // every real one would carry the cursor beyond them all.
        cfg.seed_watermark = WatermarkSeed::CurrentMax;
        assert!(!table_max_eligible(&cfg, &cols, "id", None));
        assert!(
            table_max_eligible(&cfg, &cols, "id", Some("41")),
            "a later run has a cursor"
        );
        cfg.seed_watermark = WatermarkSeed::None;

        cfg.source_table = None;
        assert!(
            !table_max_eligible(&cfg, &cols, "id", None),
            "no table to probe"
        );

        // An EXPLAIN that failed (no such column on the table) never qualifies.
        assert!(!table_max_affordable(&ProbeCost::Unknown, 0.0));
        assert!(table_max_affordable(&CHEAP, T));
        assert!(!table_max_affordable(&DEAR, T));
        assert!(table_max_affordable(&DEAR, 0.0));
    }

    #[test]
    fn a_cursor_under_a_table_max_advances_only_to_what_was_read() {
        let t = WatermarkTracker {
            idx: 0,
            unit: WatermarkUnit::Int,
            max: AtomicI64::new(i64::MIN),
        };
        assert!(!t.seen());
        assert_eq!(t.advance_from(Some("100"), 0), None, "nothing read");
        t.max.store(150, Ordering::Relaxed);
        assert!(t.seen());
        assert_eq!(t.advance_from(Some("100"), 0).as_deref(), Some("150"));
        assert_eq!(t.advance_from(None, 0).as_deref(), Some("150"));
        // Only the lookback band was read again: the cursor stays put.
        assert_eq!(t.advance_from(Some("150"), 0), None);
        assert_eq!(t.advance_from(Some("200"), 0), None);
    }

    #[test]
    fn a_table_mode_cursor_takes_the_rewind_as_a_margin() {
        let t = WatermarkTracker {
            idx: 0,
            unit: WatermarkUnit::NaiveMicros,
            max: AtomicI64::new(i64::MIN),
        };
        t.max
            .store(t.parse("2026-01-01 00:00:10").unwrap(), Ordering::Relaxed);
        assert_eq!(
            t.advance_from(Some("2026-01-01 00:00:00"), 4).as_deref(),
            Some("2026-01-01 00:00:06.000000")
        );
        assert_eq!(
            t.advance_from(Some("2026-01-01 00:00:08"), 4),
            None,
            "a rewind that lands at or below the starting cursor keeps it"
        );
    }

    #[test]
    fn a_timestamptz_cursor_keeps_its_offset_whatever_its_destination_type() {
        // A PostgreSQL timestamptz overridden to a naive destination type still
        // compares as an instant: a cursor without `+00` would be read in the
        // session's TimeZone.
        let plan = |arrow: DataType| SelectPlan {
            source_columns: vec!["wm".into()],
            source_select_exprs: vec![None],
            dest_columns: vec![col_typed("wm", arrow)],
        };
        let naive = DataType::Timestamp(TimeUnit::Microsecond, None);
        let zoned = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let unit = |arrow, utc| WatermarkTracker::new("wm", &plan(arrow), utc).unwrap().unit;
        assert_eq!(unit(naive.clone(), true), WatermarkUnit::UtcMicros);
        assert_eq!(unit(zoned.clone(), true), WatermarkUnit::UtcMicros);
        assert_eq!(unit(naive, false), WatermarkUnit::NaiveMicros);
        assert_eq!(unit(zoned, false), WatermarkUnit::NaiveMicros);
    }

    #[test]
    fn integer_watermarks_fold_from_every_integer_width() {
        let plan_cols = |arrow: DataType| SelectPlan {
            source_columns: vec!["id".into()],
            source_select_exprs: vec![None],
            dest_columns: vec![col_typed("id", arrow)],
        };
        for arrow in [
            DataType::Int32,
            DataType::Int64,
            DataType::UInt32,
            DataType::Int16,
        ] {
            let t = WatermarkTracker::new("id", &plan_cols(arrow.clone()), false)
                .unwrap_or_else(|| panic!("{arrow:?} should fold"));
            assert_eq!(t.unit, WatermarkUnit::Int);
        }
        assert!(WatermarkTracker::new("id", &plan_cols(DataType::UInt64), false).is_none());
        let t = WatermarkTracker::new("id", &plan_cols(DataType::Int32), false).unwrap();
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            DataType::Int32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int32Array::from(vec![
                Some(7),
                None,
                Some(42),
                Some(-3),
            ]))],
        )
        .unwrap();
        t.observe(&batch);
        assert_eq!(t.advance_from(None, 0).as_deref(), Some("42"));
    }

    #[test]
    fn partitions_split_above_the_cursor_only_for_a_watermark_key() {
        let mut cfg = crate::config::default_test_config();
        cfg.mode = SyncMode::Incremental;
        cfg.watermark = Some("id".into());
        assert_eq!(partitions_above(&cfg, "id", None, Some("41")), Some(41));
        assert_eq!(
            partitions_above(&cfg, "id", None, None),
            None,
            "a first run"
        );
        assert_eq!(
            partitions_above(&cfg, "id", Some("id_raw"), Some("41")),
            None
        );
        assert_eq!(partitions_above(&cfg, "other", None, Some("41")), None);
        assert_eq!(
            partitions_above(&cfg, "id", None, Some("2026-01-01")),
            None,
            "not an integer cursor"
        );
        // The read compares watermark_source_expr, not the key, to the cursor.
        cfg.watermark_source_expr = Some("COALESCE(id, legacy_id)".into());
        assert_eq!(partitions_above(&cfg, "id", None, Some("41")), None);
        cfg.watermark_source_expr = None;
        cfg.mode = SyncMode::Full;
        assert_eq!(partitions_above(&cfg, "id", None, Some("41")), None);
    }

    #[test]
    fn a_watermark_key_with_a_cursor_needs_no_sweep() {
        assert!(watermark_bounds_the_key(Some("id"), "id", Some("10")));
        assert!(
            !watermark_bounds_the_key(Some("id"), "id", None),
            "a first run sweeps"
        );
        assert!(!watermark_bounds_the_key(
            Some("updated_date"),
            "id",
            Some("x")
        ));
    }

    #[test]
    fn watermark_renders_at_fixed_width_and_orders_chronologically() {
        // PostgreSQL's own timestamp::text drops trailing zeros (.100000 -> .1),
        // so a stored cursor and a freshly rendered one can differ as STRINGS
        // while being equal as timestamps. Rendering at fixed width is what
        // makes the values we generate mutually comparable; the server casts
        // the literal either way, which is what keeps them interchangeable with
        // cursors written by earlier versions.
        let t = WatermarkTracker {
            idx: 0,
            unit: WatermarkUnit::NaiveMicros,
            max: AtomicI64::new(0),
        };
        // 2024-01-01T00:00:00.100000Z
        t.max.store(1_704_067_200_100_000, Ordering::Relaxed);
        assert_eq!(t.render().as_deref(), Some("2024-01-01 00:00:00.100000"));
        t.max.store(1_704_067_200_000_000, Ordering::Relaxed);
        assert_eq!(t.render().as_deref(), Some("2024-01-01 00:00:00.000000"));
        t.max.store(1_704_067_200_123_456, Ordering::Relaxed);
        assert_eq!(t.render().as_deref(), Some("2024-01-01 00:00:00.123456"));
        // Fixed width means lexicographic order matches chronological order.
        assert!("2024-01-01 00:00:00.000000" < "2024-01-01 00:00:00.100000");
    }

    #[test]
    fn nothing_read_means_no_cursor_advance() {
        // The sentinel must not render as a date ~292,000 years before the
        // epoch; it must leave the cursor exactly where it was.
        let t = WatermarkTracker {
            idx: 0,
            unit: WatermarkUnit::NaiveMicros,
            max: AtomicI64::new(i64::MIN),
        };
        assert_eq!(t.render(), None);
    }

    #[test]
    fn a_date_watermark_renders_as_a_bare_date() {
        let t = WatermarkTracker {
            idx: 0,
            unit: WatermarkUnit::Days,
            max: AtomicI64::new(19_723), // 2024-01-01
        };
        assert_eq!(t.render().as_deref(), Some("2024-01-01"));
    }

    /// Issue #2: MySQL can't parse the `+00` a PostgreSQL `timestamptz` cursor
    /// carries, so the same tz-aware column renders bare for MySQL.
    #[test]
    fn a_tz_aware_cursor_carries_an_offset_only_for_postgres() {
        let utc = DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()));
        let plan = SelectPlan {
            source_columns: vec!["wm".to_string()],
            source_select_exprs: vec![None],
            dest_columns: vec![col_typed("wm", utc)],
        };
        let pg = WatermarkTracker::new("wm", &plan, true).unwrap();
        let my = WatermarkTracker::new("wm", &plan, false).unwrap();
        for t in [&pg, &my] {
            t.max.store(1_704_067_200_000_000, Ordering::Relaxed);
        }
        assert_eq!(
            pg.render().as_deref(),
            Some("2024-01-01 00:00:00.000000+00")
        );
        assert_eq!(my.render().as_deref(), Some("2024-01-01 00:00:00.000000"));
    }

    /// A destination that stages incremental loads, recording every call a
    /// chunked run makes of it, in order.
    #[derive(Default)]
    struct RecordingSink(Mutex<Vec<String>>);

    impl RecordingSink {
        fn log(&self, call: String) {
            self.0.lock().unwrap().push(call);
        }
        fn calls(&self) -> Vec<String> {
            self.0.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl Sink for RecordingSink {
        async fn table_exists(&self, _: &str) -> Result<bool> {
            unreachable!()
        }
        async fn create_table(&self, _: &str, _: &[ColumnType], _: &TransferConfig) -> Result<()> {
            unreachable!()
        }
        async fn clone_table_structure(&self, new_table: &str, like: &str) -> Result<()> {
            self.log(format!("clone {new_table} like {like}"));
            Ok(())
        }
        async fn insert_batches(&self, _: &str, _: SchemaRef, _: &[RecordBatch]) -> Result<u64> {
            unreachable!()
        }
        async fn atomic_swap(&self, _: &str, _: &str, _: &[ColumnType]) -> Result<()> {
            unreachable!()
        }
        async fn current_row_count(&self, _: &str) -> Result<Option<u64>> {
            unreachable!()
        }
        async fn drop_table(&self, table: &str) -> Result<()> {
            self.log(format!("drop {table}"));
            Ok(())
        }
        async fn ensure_state_table(&self, _: &str) -> Result<()> {
            unreachable!()
        }
        async fn read_last_watermark(&self, _: &TransferConfig) -> Result<Option<String>> {
            unreachable!()
        }
        async fn persist_watermark(&self, _: &TransferConfig, _: &str, _: u64) -> Result<()> {
            unreachable!()
        }
        async fn add_missing_columns(
            &self,
            _: &str,
            _: &[ColumnType],
            _: &TransferConfig,
        ) -> Result<Vec<String>> {
            unreachable!()
        }
        fn dest_kind(&self) -> crate::config::DestKind {
            crate::config::DestKind::BigQuery
        }
        fn namespace(&self) -> &str {
            "ds"
        }
        fn requires_staging_for_incremental(&self) -> bool {
            true
        }
        async fn merge_into(
            &self,
            dest: &str,
            staging: &str,
            key: &[String],
            _: &[ColumnType],
            _: Option<&str>,
            _: bool,
            _: usize,
            delete_stale: bool,
            dedup_order: Option<&str>,
        ) -> Result<()> {
            self.log(format!(
                "merge {staging} into {dest} on {key:?} (delete_stale={delete_stale}, \
                 newest by {dedup_order:?})"
            ));
            Ok(())
        }
    }

    fn stager(sink: &Arc<RecordingSink>) -> Arc<ChunkStager> {
        Arc::new(ChunkStager {
            sink: sink.clone(),
            dest_table: "orders".into(),
            template: "orders_stg".into(),
            key: vec!["id".into()],
            columns: vec![],
            merge_prune_partition_by: None,
            merge_prune_key_range: true,
            merge_prune_key_list_max: 0,
            dedup_order: Some("write_date".into()),
            warnings: Warnings::default(),
            next: AtomicU64::new(0),
            open: Mutex::new(None),
            landed: None,
        })
    }

    fn send_ctx(sink: Arc<dyn Sink>, chunk_stager: Option<Arc<ChunkStager>>) -> SendCtx {
        SendCtx {
            sink,
            budget: MemoryBudget::new(1 << 20),
            target_table: Arc::new("orders_stg".into()),
            counters: Arc::new(Counters::default()),
            progress: None,
            started: Instant::now(),
            archive: None,
            throttle: None,
            warnings: Warnings::default(),
            watermark_max: None,
            chunk_stager,
            landed: None,
        }
    }

    /// Issue #4: on a destination that stages incremental loads, every chunk
    /// is loaded into a table of its own, merged, and dropped before the next
    /// one opens, so its cursor can be committed.
    #[tokio::test]
    async fn each_chunk_is_merged_through_its_own_staging_table() {
        let sink = Arc::new(RecordingSink::default());
        let ctx = send_ctx(sink.clone(), Some(stager(&sink)));

        let first = ctx.begin_chunk().await.unwrap();
        assert_eq!(first.target_table.as_str(), "orders_stg_c0");
        ctx.end_chunk(&first, 40).await.unwrap();
        // The read that finds nothing left: no merge, which would still scan.
        let last = ctx.begin_chunk().await.unwrap();
        assert_eq!(last.target_table.as_str(), "orders_stg_c1");
        ctx.end_chunk(&last, 0).await.unwrap();

        assert_eq!(
            sink.calls(),
            [
                "clone orders_stg_c0 like orders_stg",
                "merge orders_stg_c0 into orders on [\"id\"] (delete_stale=false, newest by \
                 Some(\"write_date\"))",
                "drop orders_stg_c0",
                "clone orders_stg_c1 like orders_stg",
                "drop orders_stg_c1",
            ]
        );
    }

    #[tokio::test]
    async fn a_failed_chunk_leaves_no_staging_table_behind() {
        let sink = Arc::new(RecordingSink::default());
        let stager = stager(&sink);
        let ctx = send_ctx(sink.clone(), Some(stager.clone()));
        let done = ctx.begin_chunk().await.unwrap();
        ctx.end_chunk(&done, 5).await.unwrap();
        let _failed = ctx.begin_chunk().await.unwrap();
        stager.cleanup().await;
        // Nothing left open, so a second cleanup is a no-op.
        stager.cleanup().await;
        let calls = sink.calls();
        assert_eq!(calls.last().map(String::as_str), Some("drop orders_stg_c1"));
        assert_eq!(calls.iter().filter(|c| c.starts_with("drop ")).count(), 2);
    }

    /// A direct-insert destination: a chunk writes where the run writes, and
    /// the flush alone lands it.
    #[tokio::test]
    async fn a_direct_insert_chunk_needs_no_staging() {
        let sink = Arc::new(RecordingSink::default());
        let ctx = send_ctx(sink.clone(), None);
        let chunk = ctx.begin_chunk().await.unwrap();
        assert_eq!(chunk.target_table.as_str(), "orders_stg");
        ctx.end_chunk(&chunk, 40).await.unwrap();
        assert!(sink.calls().is_empty());
    }

    fn check(cursor: &str, max: &str, lookback: u64, arrow: DataType) -> Option<CursorCheck> {
        check_cursor_against_max("wm", cursor, max, lookback, &[col_typed("wm", arrow)])
    }

    /// Issue #3, checks B and C: the cursor and the probe's MAX are ordered by
    /// the watermark's type, never as strings.
    #[test]
    fn cursor_check_orders_values_by_the_watermarks_type() {
        let naive = DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None);
        let utc = DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()));
        let flags = |c: CursorCheck| (c.cursor_ahead, c.max_above_lower_bound);

        // Numbers, not strings: "99" sorts after "100" lexicographically.
        assert_eq!(
            flags(check("99", "100", 0, DataType::Int64).unwrap()),
            (false, true)
        );
        // One instant in two renderings (a PostgreSQL MAX in a +07 session, a
        // stream cursor in UTC) is a quiet run: nothing to say.
        assert_eq!(
            flags(
                check(
                    "2024-01-01 00:00:00.000000+00",
                    "2024-01-01 07:00:00+07",
                    0,
                    utc.clone()
                )
                .unwrap()
            ),
            (false, false)
        );
        // Stream and probe renderings of the same MySQL DATETIME.
        assert_eq!(
            flags(
                check(
                    "2024-01-01 00:00:00.000000",
                    "2024-01-01 00:00:00",
                    0,
                    utc.clone()
                )
                .unwrap()
            ),
            (false, false)
        );
        // New rows at the source.
        assert_eq!(
            flags(
                check(
                    "2024-01-01 00:00:00",
                    "2024-02-01T00:00:00",
                    0,
                    naive.clone()
                )
                .unwrap()
            ),
            (false, true)
        );
        // A cursor shifted +7h past the source...
        assert_eq!(
            flags(
                check(
                    "2024-01-01 07:00:00",
                    "2024-01-01 00:00:00",
                    0,
                    naive.clone()
                )
                .unwrap()
            ),
            (true, false)
        );
        // ...which a wide enough lookback still reaches back under.
        assert_eq!(
            flags(
                check(
                    "2024-01-01 07:00:00",
                    "2024-01-01 00:00:00",
                    8 * 3600,
                    naive.clone()
                )
                .unwrap()
            ),
            (true, true)
        );
        assert_eq!(
            flags(check("2024-01-02", "2024-01-01", 0, DataType::Date32).unwrap()),
            (true, false)
        );
        assert_eq!(
            flags(
                check(
                    "2024-01-01T00:00:00Z",
                    "2024-01-01 00:00:01+00",
                    0,
                    utc.clone()
                )
                .unwrap()
            ),
            (false, true)
        );

        // Not compared: an offset against a naive value (whose zone is
        // unknown), anything unparseable, and types with no ordering here.
        assert!(check("2024-01-01 00:00:00+00", "2024-01-01 00:00:00", 0, utc).is_none());
        assert!(check("garbage", "2024-01-01", 0, DataType::Date32).is_none());
        assert!(check("a", "b", 0, DataType::Utf8).is_none());
        assert!(check_cursor_against_max(
            "other",
            "1",
            "2",
            0,
            &[col_typed("wm", DataType::Int64)]
        )
        .is_none());
    }

    #[test]
    fn cursor_check_warns_only_on_the_contradiction_it_describes() {
        let mut cfg = crate::config::default_test_config();
        cfg.watermark = Some("wm".into());
        let kinds = |w: &Warnings| w.drain().into_iter().map(|w| w.kind).collect::<Vec<_>>();

        let due = check("1", "5", 0, DataType::Int64).unwrap();
        let w = Warnings::default();
        due.warn_if_cursor_ahead(&cfg, None, &w);
        due.warn_if_not_advanced(&cfg, 4, &w);
        assert!(kinds(&w).is_empty(), "rows were read: nothing to report");
        due.warn_if_not_advanced(&cfg, 0, &w);
        assert_eq!(kinds(&w), vec![WarningKind::WatermarkNotAdvanced]);

        let quiet = check("5", "5", 0, DataType::Int64).unwrap();
        quiet.warn_if_cursor_ahead(&cfg, None, &w);
        quiet.warn_if_not_advanced(&cfg, 0, &w);
        assert!(
            kinds(&w).is_empty(),
            "MAX equals the cursor: an ordinary quiet run"
        );

        let ahead = check("9", "5", 0, DataType::Int64).unwrap();
        ahead.warn_if_cursor_ahead(&cfg, None, &w);
        ahead.warn_if_not_advanced(&cfg, 0, &w);
        let out = w.drain();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind, WarningKind::WatermarkAheadOfSource);
        assert_eq!(out[0].column.as_deref(), Some("wm"));
        assert_eq!(out[0].sample.as_deref(), Some("9"));
        assert!(out[0].message.contains("saves the source's MAX"));

        // Through a filtered source_query the message names the filter as a
        // cause, and still saves the MAX: moving back costs a re-read, while
        // holding a cursor that really is ahead would skip rows.
        ahead.warn_if_cursor_ahead(&cfg, Some("Set source_table as well."), &w);
        let out = w.drain();
        assert_eq!(out[0].kind, WarningKind::WatermarkAheadOfSource);
        assert!(
            out[0].message.contains("filter that excludes"),
            "{}",
            out[0].message
        );
        assert!(out[0].message.contains("Set source_table as well."));
        assert!(
            out[0].message.contains("saves that MAX"),
            "{}",
            out[0].message
        );
    }

    #[tokio::test]
    async fn key_bounds_probe_is_retried_after_a_transient_failure() {
        let w = Warnings::default();
        let calls = std::cell::Cell::new(0);
        let transient = || EtlError::read_idle_timeout("probe", 1);
        let bounds = key_bounds_with_retry("id", Err(transient()), &w, || {
            calls.set(calls.get() + 1);
            async { Ok(Some((1, 9))) }
        })
        .await;
        assert_eq!(bounds, Some((1, 9)), "the retry's bounds window the read");
        assert_eq!(calls.get(), 1);
        assert!(w.drain().is_empty());
    }

    #[tokio::test]
    async fn key_bounds_probe_that_keeps_failing_warns_and_reads_in_one_pass() {
        let w = Warnings::default();
        let calls = std::cell::Cell::new(0);
        let bounds = key_bounds_with_retry(
            "id",
            Err(EtlError::read_idle_timeout("probe", 1)),
            &w,
            || {
                calls.set(calls.get() + 1);
                async { Err(EtlError::read_idle_timeout("probe", 1)) }
            },
        )
        .await;
        assert_eq!(bounds, None);
        assert_eq!(calls.get(), KEY_BOUNDS_ATTEMPTS - 1);
        let out = w.drain();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind, WarningKind::WindowBoundsUnavailable);
        assert_eq!(out[0].column.as_deref(), Some("id"));
        assert!(
            out[0].message.contains("3 attempt(s)"),
            "{}",
            out[0].message
        );

        // A failure a retry can't fix (a syntax error, a missing column) is
        // not retried at all.
        let calls = std::cell::Cell::new(0);
        let bounds =
            key_bounds_with_retry("id", Err(EtlError::other("no such column")), &w, || {
                calls.set(calls.get() + 1);
                async { Ok(Some((1, 9))) }
            })
            .await;
        assert_eq!(bounds, None);
        assert_eq!(calls.get(), 0);
        assert_eq!(w.drain()[0].kind, WarningKind::WindowBoundsUnavailable);
    }

    #[test]
    fn a_sweep_buffer_is_sent_between_windows_only() {
        let budget = MemoryBudget::new(1_000);
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = || {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arrow_array::Int32Array::from(vec![1]))],
            )
            .unwrap()
        };
        let mut buf = InsertBuffer::new(10);
        buf.deferred = true;
        // Past the target, but a window's rows are never sent mid-window.
        assert!(!buf.push(batch(), budget.try_reserve(8).unwrap(), 8));
        assert!(!buf.push(batch(), budget.try_reserve(8).unwrap(), 8));
        assert!(buf.full(), "sent once the window ends");
        buf.deferred = false;
        assert!(buf.push(batch(), budget.try_reserve(8).unwrap(), 8));
    }

    #[test]
    fn a_failed_window_gives_back_only_its_own_buffered_rows() {
        let budget = MemoryBudget::new(100);
        let batch = || {
            let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "id",
                DataType::Int32,
                false,
            )]));
            RecordBatch::try_new(
                schema,
                vec![Arc::new(arrow_array::Int32Array::from(vec![1]))],
            )
            .unwrap()
        };
        let mut buf = InsertBuffer::new(usize::MAX);
        let push = |buf: &mut InsertBuffer, size| {
            buf.push(batch(), budget.try_reserve(size).unwrap(), size);
        };
        // An earlier window's rows, then a failing window's.
        push(&mut buf, 10);
        let mark = buf.mark();
        push(&mut buf, 20);
        push(&mut buf, 30);
        buf.truncate(mark);
        assert_eq!(buf.batches.len(), 1, "the earlier window's rows stay");
        assert_eq!(buf.bytes, 10);
        assert!(
            budget.try_reserve(90).is_some(),
            "and the failed window's memory is released"
        );

        // The buffer was sent mid-window: all it holds now is the failed
        // window's.
        let mark = buf.mark();
        push(&mut buf, 5);
        drop(buf.take());
        push(&mut buf, 7);
        buf.truncate(mark);
        assert!(buf.is_empty());
        assert_eq!(buf.bytes, 0);
    }

    #[test]
    fn a_fatal_warning_stops_the_run_and_says_what_was_left_in_place() {
        let mut cfg = crate::config::default_test_config();
        let w = Warnings::default();
        w.push(TransferWarning {
            kind: WarningKind::CollapsedBool,
            column: Some("x_state".into()),
            count: 3,
            sample: Some("7".into()),
            message: "flattened".into(),
        });
        w.push(TransferWarning {
            kind: WarningKind::CollapsedBool,
            column: Some("x_state".into()),
            count: 4,
            sample: None,
            message: "flattened".into(),
        });
        // Nothing is fatal by default.
        assert!(check_fatal_warnings(&cfg, &w, Before::Cursor).is_ok());
        cfg.fail_on_warnings = vec![WarningKind::NullWatermark];
        assert!(check_fatal_warnings(&cfg, &w, Before::Cursor).is_ok());

        cfg.fail_on_warnings = vec![WarningKind::NullWatermark, WarningKind::CollapsedBool];
        let err = check_fatal_warnings(&cfg, &w, Before::Merge)
            .unwrap_err()
            .to_string();
        assert!(err.contains("fail_on_warnings: collapsed_bool"), "{err}");
        assert!(err.contains("column 'x_state'"), "{err}");
        assert!(err.contains("count 7"), "folded like drain: {err}");
        assert!(err.contains("destination is untouched"), "{err}");
        assert!(err.contains("cursor was not saved"), "{err}");
        // Checking takes nothing: the result still reports it.
        assert_eq!(w.drain().len(), 1);
    }

    #[test]
    fn a_storage_write_mismatch_stops_a_merge_but_not_an_append() {
        struct Mismatched(Mutex<Vec<TransferWarning>>);
        #[async_trait::async_trait]
        impl Sink for Mismatched {
            fn take_write_warnings(&self) -> Vec<TransferWarning> {
                std::mem::take(&mut *self.0.lock().unwrap())
            }
            async fn table_exists(&self, _: &str) -> Result<bool> {
                unimplemented!()
            }
            async fn create_table(
                &self,
                _: &str,
                _: &[ColumnType],
                _: &TransferConfig,
            ) -> Result<()> {
                unimplemented!()
            }
            async fn clone_table_structure(&self, _: &str, _: &str) -> Result<()> {
                unimplemented!()
            }
            async fn insert_batches(
                &self,
                _: &str,
                _: SchemaRef,
                _: &[RecordBatch],
            ) -> Result<u64> {
                unimplemented!()
            }
            async fn atomic_swap(&self, _: &str, _: &str, _: &[ColumnType]) -> Result<()> {
                unimplemented!()
            }
            async fn current_row_count(&self, _: &str) -> Result<Option<u64>> {
                unimplemented!()
            }
            async fn drop_table(&self, _: &str) -> Result<()> {
                unimplemented!()
            }
            async fn ensure_state_table(&self, _: &str) -> Result<()> {
                unimplemented!()
            }
            async fn read_last_watermark(&self, _: &TransferConfig) -> Result<Option<String>> {
                unimplemented!()
            }
            async fn persist_watermark(&self, _: &TransferConfig, _: &str, _: u64) -> Result<()> {
                unimplemented!()
            }
            async fn add_missing_columns(
                &self,
                _: &str,
                _: &[ColumnType],
                _: &TransferConfig,
            ) -> Result<Vec<String>> {
                unimplemented!()
            }
            fn dest_kind(&self) -> crate::config::DestKind {
                crate::config::DestKind::BigQuery
            }
            fn namespace(&self) -> &str {
                "ds"
            }
        }
        let mismatch = || TransferWarning {
            kind: WarningKind::StorageWriteCountMismatch,
            column: None,
            count: 3,
            sample: Some("ds.stg".into()),
            message: "finalized with 103 rows, 100 appended".into(),
        };
        let cfg = crate::config::default_test_config();
        let sink = Mismatched(Mutex::new(vec![mismatch()]));
        let w = Warnings::default();
        let err = check_fatal(&cfg, &sink, &w, Before::Merge)
            .unwrap_err()
            .to_string();
        assert!(err.starts_with("storage_write_count_mismatch"), "{err}");
        assert!(err.contains("destination is untouched"), "{err}");

        // Written straight into the destination, it is reported, not fatal.
        let sink = Mismatched(Mutex::new(vec![mismatch()]));
        let w = Warnings::default();
        assert!(check_fatal(&cfg, &sink, &w, Before::Done).is_ok());
        assert_eq!(w.drain()[0].kind, WarningKind::StorageWriteCountMismatch);
    }

    #[test]
    fn a_table_max_must_read_as_the_query_column() {
        let cols = [
            col_typed("id", DataType::Int64),
            col_typed("updated", DataType::Timestamp(TimeUnit::Microsecond, None)),
            col_typed("since", DataType::Int64),
        ];
        assert!(watermark_value_parses(&cols, "id", Some("42"), false));
        assert!(
            watermark_value_parses(&cols, "id", None, false),
            "an empty table"
        );
        assert!(watermark_value_parses(
            &cols,
            "updated",
            Some("2026-10-06 10:00:00"),
            false
        ));
        // `UNIX_TIMESTAMP(updated_at) AS since` over a DATETIME column: the
        // table's MAX would be a bound in another type.
        assert!(!watermark_value_parses(
            &cols,
            "since",
            Some("2026-10-06 10:00:00"),
            false
        ));
        assert!(!watermark_value_parses(
            &cols,
            "updated",
            Some("1759744800"),
            false
        ));
        // A timestamptz MAX carries its offset; a naive one where one is due doesn't read.
        assert!(watermark_value_parses(
            &cols,
            "updated",
            Some("2026-10-06 10:00:00+00"),
            true
        ));
        assert!(!watermark_value_parses(
            &cols,
            "updated",
            Some("2026-10-06 10:00:00"),
            true
        ));
    }

    #[test]
    fn a_retry_after_a_partial_write_is_a_warning_with_the_rows() {
        let mut cfg = crate::config::default_test_config();
        cfg.dest_table = "events".into();
        let w = partial_write_warning(&cfg, 2, 130_000);
        assert_eq!(w.kind, WarningKind::RetriedAfterPartialWrite);
        assert_eq!(w.count, 130_000);
        assert_eq!(w.column, None);
        assert!(w.message.contains("attempt 2"), "{}", w.message);
        assert!(w.message.contains("'events'"), "{}", w.message);
    }

    #[test]
    fn stream_cursor_rewinds_only_by_what_the_read_took_beyond_the_lookback() {
        assert_eq!(stream_cursor_rewind_secs(0.4, 1), 0);
        assert_eq!(stream_cursor_rewind_secs(60.0, 3_600), 0);
        assert_eq!(stream_cursor_rewind_secs(8.2, 1), 8);
        assert_eq!(stream_cursor_rewind_secs(-1.0, 0), 0);
    }

    #[test]
    fn rewound_stream_cursor_stays_between_the_floor_and_the_max() {
        let t = WatermarkTracker {
            idx: 0,
            unit: WatermarkUnit::NaiveMicros,
            max: AtomicI64::new(i64::MIN),
        };
        assert_eq!(t.render_rewound(10, Some("2026-01-01 00:00:00")), None);
        let at = |s: &str| t.parse(s).unwrap();
        t.max.store(at("2026-01-01 00:00:09"), Ordering::Relaxed);
        assert_eq!(
            t.render_rewound(0, Some("2026-01-01 00:00:05")).as_deref(),
            Some("2026-01-01 00:00:09.000000")
        );
        // The issue's sweep: 8 s read, 1 s lookback, cursor from a late update.
        assert_eq!(
            t.render_rewound(8, None).as_deref(),
            Some("2026-01-01 00:00:01.000000")
        );
        // Never below the cursor the run started from...
        assert_eq!(
            t.render_rewound(8, Some("2026-01-01 00:00:05")).as_deref(),
            Some("2026-01-01 00:00:05.000000")
        );
        // ...and that floor never lifts it past what was read.
        assert_eq!(
            t.render_rewound(8, Some("2026-01-02 00:00:00")).as_deref(),
            Some("2026-01-01 00:00:09.000000")
        );
        // A floor in another unit is ignored rather than guessed at.
        assert_eq!(
            t.render_rewound(8, Some("2026-01-01 00:00:05+00"))
                .as_deref(),
            Some("2026-01-01 00:00:01.000000")
        );
        // A timestamptz cursor saved without its offset (by 0.20.6, for one
        // overridden to a naive type) still floors it, as UTC.
        let z = WatermarkTracker {
            idx: 0,
            unit: WatermarkUnit::UtcMicros,
            max: AtomicI64::new(i64::MIN),
        };
        z.max.store(
            z.parse("2026-01-01 00:00:09+00").unwrap(),
            Ordering::Relaxed,
        );
        assert_eq!(
            z.render_rewound(8, Some("2026-01-01 00:00:05")).as_deref(),
            Some("2026-01-01 00:00:05.000000+00")
        );

        let d = WatermarkTracker {
            idx: 0,
            unit: WatermarkUnit::Days,
            max: AtomicI64::new(i64::MIN),
        };
        d.max
            .store(d.parse("2026-01-10").unwrap(), Ordering::Relaxed);
        // A DATE moves back in whole days, rounded up.
        assert_eq!(d.render_rewound(1, None).as_deref(), Some("2026-01-09"));
        assert_eq!(
            d.render_rewound(86_401, None).as_deref(),
            Some("2026-01-08")
        );
    }

    #[test]
    fn a_read_with_no_watermark_reports_null_watermark_once() {
        let mut cfg = crate::config::default_test_config();
        cfg.watermark = Some("updated_date".into());
        let w = Warnings::default();
        warn_on_unwatermarked_read(&cfg, 1_234, &w);
        let out = w.drain();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind, WarningKind::NullWatermark);
        assert_eq!(out[0].column.as_deref(), Some("updated_date"));
        assert_eq!(out[0].count, 1_234);
        assert!(
            out[0].message.contains("no cursor to save"),
            "{}",
            out[0].message
        );

        // When the NULL-count probe already reported the column, its exact
        // count stands and isn't added to.
        warn_on_null_watermark("updated_date", 7, &w);
        warn_on_unwatermarked_read(&cfg, 1_234, &w);
        let out = w.drain();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].count, 7);
    }

    /// A `+00` cursor saved by 0.18 to 0.20.1 is read back without its zero
    /// offset, in both bound forms, and without converting the time.
    #[test]
    fn a_saved_mysql_cursor_loses_its_zero_offset_and_nothing_else() {
        for saved in [
            "2026-09-16 13:05:03.000000+00",
            "2026-09-16 13:05:03.000000+00:00",
        ] {
            assert_eq!(
                lookback_lower_bound_mysql(saved, 10800),
                "(CAST('2026-09-16 13:05:03.000000' AS DATETIME) - INTERVAL 10800 SECOND)"
            );
            assert_eq!(
                lookback_lower_bound_mysql(saved, 0),
                "'2026-09-16 13:05:03.000000'"
            );
        }
        assert_eq!(
            strip_zero_utc_offset("2026-09-16 13:05:03+00"),
            "2026-09-16 13:05:03"
        );
        assert_eq!(
            strip_zero_utc_offset("2026-09-16T13:05:03+00:00"),
            "2026-09-16T13:05:03"
        );
        // Left alone: a non-zero offset (MySQL converts it, as documented),
        // an already-bare cursor, and anything that isn't a datetime.
        for kept in [
            "2026-09-16 13:05:03+07:00",
            "2026-09-16 13:05:03.000000",
            "2026-09-16",
            "1200",
            "order+00",
        ] {
            assert_eq!(strip_zero_utc_offset(kept), kept);
        }
    }

    fn wplan(min: i64, max: i64, step: u64, floor: u64) -> WindowPlan {
        WindowPlan {
            col_quoted: "\"id\"".into(),
            min,
            max,
            start: step,
            max_step: step,
            target_secs: 5.0,
            floor,
            nullable_key: false,
        }
    }

    /// Walk a sweep the way the dispatcher does, recording every window.
    /// `fail_at` makes those windows report a transient cancellation once, so
    /// the shrink path is exercised without a database.
    fn sweep(plan: &WindowPlan, mut fails: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
        let mut lo = plan.min.saturating_sub(1);
        let mut step = plan.start;
        let mut seen = Vec::new();
        let mut guard = 0;
        while lo < plan.max {
            guard += 1;
            assert!(guard < 10_000, "sweep failed to terminate");
            let hi = window_hi(lo, step, plan.max);
            if let Some(i) = fails.iter().position(|w| *w == (lo, hi)) {
                fails.remove(i);
                let (n_lo, n_step) = next_window(lo, hi, step, plan, WindowOutcome::Cancelled);
                assert_eq!(n_lo, lo, "a failed window must be retried, not skipped");
                assert!(n_step < step || n_step == plan.floor);
                step = n_step;
                continue;
            }
            seen.push((lo, hi));
            let (n_lo, n_step) =
                next_window(lo, hi, step, plan, WindowOutcome::Done(plan.target_secs));
            lo = n_lo;
            step = n_step;
        }
        seen
    }

    #[test]
    fn sweep_covers_every_key_exactly_once() {
        // (lo, hi] windows must tile [min, max] with no gap and no overlap —
        // a gap silently drops rows, an overlap duplicates them.
        let plan = wplan(1, 1000, 250, 10);
        let w = sweep(&plan, vec![]);
        assert_eq!(w.first().unwrap().0, 0, "first window must admit min");
        assert_eq!(w.last().unwrap().1, 1000, "last window must reach max");
        for pair in w.windows(2) {
            assert_eq!(pair[0].1, pair[1].0, "gap or overlap between windows");
        }
    }

    #[test]
    fn sweep_handles_a_span_that_is_not_a_multiple_of_the_step() {
        let plan = wplan(1, 1007, 250, 10);
        let w = sweep(&plan, vec![]);
        assert_eq!(w.last().unwrap().1, 1007);
        for pair in w.windows(2) {
            assert_eq!(pair[0].1, pair[1].0);
        }
    }

    #[test]
    fn sweep_still_covers_everything_when_a_window_shrinks() {
        // The shrink path must not lose the keys the failed window covered.
        let plan = wplan(1, 1000, 500, 10);
        let w = sweep(&plan, vec![(0, 500)]);
        assert_eq!(w.first().unwrap().0, 0);
        assert_eq!(w.last().unwrap().1, 1000);
        for pair in w.windows(2) {
            assert_eq!(pair[0].1, pair[1].0);
        }
    }

    #[test]
    fn a_single_key_relation_is_one_window() {
        let plan = wplan(42, 42, 1000, 10);
        assert_eq!(sweep(&plan, vec![]), vec![(41, 42)]);
    }

    #[test]
    fn a_cancelled_window_halves_and_stops_at_the_floor() {
        let plan = wplan(1, 1_000_000, 1000, 250);
        let (lo1, s1) = next_window(0, 1000, 1000, &plan, WindowOutcome::Cancelled);
        assert_eq!((lo1, s1), (0, 500), "must retry the same window, narrower");
        let (_, s2) = next_window(0, 500, s1, &plan, WindowOutcome::Cancelled);
        assert_eq!(s2, 250);
        // At the floor it stays there; the dispatcher then propagates the error
        // rather than looping, because its guard is `step > floor`.
        let (_, s3) = next_window(0, 250, s2, &plan, WindowOutcome::Cancelled);
        assert_eq!(s3, 250);
    }

    #[test]
    fn width_converges_on_the_target_duration() {
        // The property a fixed row count cannot give: a window that overran
        // shrinks proportionally, one that was quick grows, and neither moves
        // more than 2x at a time.
        let plan = wplan(1, 100_000_000, 100_000, 100);
        // Took 4x the target -> quarter the width (the clamp floor of 0.25).
        let (_, slow) = next_window(0, 100_000, 100_000, &plan, WindowOutcome::Done(20.0));
        assert_eq!(slow, 25_000);
        // Took a tenth of the target -> grows, but gently: 1.25x, not 10x and
        // not even 2x. Overshooting into a dense key range is what lands a
        // window past the standby's limit.
        let (_, fast) = next_window(0, 25_000, 25_000, &plan, WindowOutcome::Done(0.5));
        assert_eq!(fast, 31_250);
        // On target -> unchanged.
        let (_, steady) = next_window(0, 50_000, 50_000, &plan, WindowOutcome::Done(5.0));
        assert_eq!(steady, 50_000);
    }

    #[test]
    fn an_instant_empty_window_cannot_blow_the_next_one_up() {
        // A key range that happened to contain no rows returns in ~0s. Scaling
        // by target/0 would be unbounded; the 2x clamp is what prevents the
        // next window being one that cannot possibly finish.
        let plan = wplan(1, 100_000_000, 1_000_000, 100);
        let (_, next) = next_window(0, 1000, 1000, &plan, WindowOutcome::Done(0.0));
        assert_eq!(next, 1250);
    }

    #[test]
    fn sizing_backs_off_faster_than_it_recovers() {
        // The tail property. One overrun must not be undone by one quick
        // window: measured durations spanned 25x at a fixed width, so
        // symmetric response oscillates straight back over the limit.
        let plan = wplan(1, 100_000_000, 1_000_000, 100);
        let (_, after_slow) = next_window(0, 100_000, 100_000, &plan, WindowOutcome::Done(60.0));
        let (_, recovered) = next_window(
            0,
            after_slow as i64,
            after_slow,
            &plan,
            WindowOutcome::Done(0.1),
        );
        assert!(
            recovered < 100_000,
            "one fast window undid a backoff: {recovered}"
        );
    }

    #[test]
    fn width_never_exceeds_the_configured_ceiling() {
        let plan = wplan(1, 100_000_000, 10_000, 100);
        let (_, capped) = next_window(0, 10_000, 10_000, &plan, WindowOutcome::Done(0.001));
        assert_eq!(capped, 10_000);
    }

    #[test]
    fn window_hi_clamps_to_max_and_cannot_overflow() {
        assert_eq!(window_hi(0, 100, 50), 50);
        assert_eq!(window_hi(0, 100, 500), 100);
        // i64::MAX + step must clamp rather than wrap — the same class of bug
        // the range_partitions i128 regression test guards.
        assert_eq!(window_hi(i64::MAX - 1, 1000, i64::MAX), i64::MAX);
    }

    #[test]
    fn an_explicitly_small_window_does_not_invert_the_floor() {
        // Regression: the floor is derived from the ceiling, so a configured
        // width below MIN_READ_WINDOW_ROWS used to produce floor > step and
        // panic inside `clamp` on the first resize.
        let mut cfg = crate::config::default_test_config();
        cfg.key = vec!["id".to_string()];
        cfg.read_window_rows = Some(10);
        let plan = plan_read_window(&cfg, Some((1, 1000)), "\"id\"".into(), false).unwrap();
        assert!(
            plan.floor <= plan.start,
            "floor must never exceed the ceiling"
        );
        // And resizing from there must stay inside the range.
        let (_, n) = next_window(0, 10, plan.start, &plan, WindowOutcome::Done(0.001));
        assert!(n >= plan.floor && n <= plan.max_step);
    }

    #[test]
    fn the_sweep_starts_below_its_ceiling_and_can_grow_into_it() {
        // Start and ceiling are separate numbers. Tying them together caps a
        // fast table at its starting guess: measured, 0.80s windows against a
        // 5s target sat pinned at the ceiling for a whole sweep, reading ~6x
        // more windows than the target implied.
        let cfg = crate::config::default_test_config();
        let plan = plan_read_window(&cfg, Some((1, 100_000_000)), "\"id\"".into(), false).unwrap();
        assert!(
            plan.start < plan.max_step,
            "start {} should leave room to grow under ceiling {}",
            plan.start,
            plan.max_step
        );
        // A quick window grows past the starting width.
        let (_, grown) = next_window(
            0,
            plan.start as i64,
            plan.start,
            &plan,
            WindowOutcome::Done(0.1),
        );
        assert!(grown > plan.start);
    }

    #[test]
    fn an_explicit_window_size_caps_the_start_too() {
        // `read_window_rows` is the ceiling; the start must never exceed it.
        let mut cfg = crate::config::default_test_config();
        cfg.read_window_rows = Some(1_000);
        let plan = plan_read_window(&cfg, Some((1, 100_000_000)), "\"id\"".into(), false).unwrap();
        assert_eq!(plan.max_step, 1_000);
        assert!(plan.start <= 1_000);
    }

    #[test]
    fn an_empty_relation_produces_no_window_plan() {
        let cfg = crate::config::default_test_config();
        assert!(plan_read_window(&cfg, None, "\"id\"".into(), false).is_none());
    }

    #[test]
    fn window_key_accepts_a_nullable_key_and_reports_it() {
        let mut cfg = crate::config::default_test_config();
        cfg.key = vec!["id".to_string()];
        let int_col = |name: &str, nullable: bool| ColumnType {
            name: name.to_string(),
            type_id: 0,
            nullable,
            arrow: DataType::Int64,
            clickhouse_inner: "Int64".into(),
            arbitrary_precision_decimal: false,
            declared_decimal: None,
        };
        assert_eq!(
            window_key(&cfg, &[int_col("id", false)]),
            Some(("id".to_string(), false))
        );
        // A nullable key is accepted and flagged, so the sweep can add a
        // trailing `IS NULL` window. Rejecting it would make windowing inert
        // for every source_query read, where PostgreSQL reports no NOT NULL
        // constraints at all.
        assert_eq!(
            window_key(&cfg, &[int_col("id", true)]),
            Some(("id".to_string(), true))
        );
        // A transformed key means the predicate bounds something other than
        // the stored column, so no index can serve it.
        cfg.column_transforms.insert("id".into(), "id + 1".into());
        assert!(window_key(&cfg, &[int_col("id", false)]).is_none());
    }

    #[test]
    fn window_predicate_is_half_open_and_unordered() {
        // The SQL a window contributes: bounded range, and NO sort — a sort
        // would reintroduce the unbounded work windows exist to avoid.
        let k = crate::source::Keyset {
            col_quoted: "\"id\"".into(),
            cursor: Some("100".into()),
            bound: crate::source::KeysetBound::UpperBound("200".into()),
        };
        let pred = crate::source::keyset_predicate(&k).unwrap();
        assert_eq!(pred, "\"id\" > 100 AND \"id\" <= 200");
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
            archive: None,
        });
        let cb: StagedValidationCb = Arc::new(|_info: &StagedInfo| Ok(()));
        let err = run_transfer_impl(
            src,
            dst,
            cfg,
            None,
            Some(cb),
            &ArchiveUploads::default(),
            Attempt {
                landed: Arc::new(AtomicU64::new(0)),
                carried: vec![],
                first_started: Instant::now(),
            },
        )
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
            declared_decimal: None,
        }
    }

    #[test]
    fn watermark_column_present_ok() {
        let cols = vec![col("id"), col("write_date")];
        assert!(ensure_watermark_column("write_date", &cols).is_ok());
    }

    /// A MySQL column of wire type `type_id`: 7 is `TIMESTAMP`, 12 `DATETIME`.
    fn my_col(name: &str, type_id: u32) -> ColumnType {
        ColumnType {
            type_id,
            ..col(name)
        }
    }

    #[test]
    fn only_timestamps_landed_as_utc_instants_are_checked() {
        let cols = vec![
            my_col("created_ts", 7),
            my_col("created_dt", 12),
            my_col("naive_ts", 7),
            my_col("renamed_naive_ts", 7),
            my_col("dropped_ts", 7),
            my_col("transformed_ts", 7),
        ];
        let mut cfg = crate::config::default_test_config();
        cfg.exclude = vec!["dropped_ts".into()];
        cfg.rename =
            std::collections::HashMap::from([("renamed_naive_ts".into(), "ts_local".into())]);
        cfg.type_overrides = std::collections::HashMap::from([
            ("naive_ts".into(), "DATETIME".into()),
            // Keyed by the destination name, as `transform::plan` also accepts.
            ("ts_local".into(), "DateTime64(6)".into()),
            // Still a UTC instant, so still shifted.
            ("created_ts".into(), "DateTime64(6, 'UTC')".into()),
        ]);
        cfg.column_transforms = std::collections::HashMap::from([(
            "transformed_ts".into(),
            "UNIX_TIMESTAMP(`transformed_ts`)".into(),
        )]);
        assert_eq!(utc_landed_timestamps(&cols, &cfg), vec!["created_ts"]);
    }

    #[test]
    fn a_session_offset_reads_as_utc_plus_or_minus() {
        assert_eq!(describe_utc_offsets([25_200, 25_200]), "UTC+07:00");
        assert_eq!(describe_utc_offsets([-16_200, -16_200]), "UTC-04:30");
        assert_eq!(
            describe_utc_offsets([0, 3_600]),
            "UTC+00:00 (January) / UTC+01:00 (July)"
        );
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
                0,
                false
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
                0,
                false
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
            false,
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
    fn watermark_filter_pg_casts_a_tz_aware_lookback_bound_to_timestamptz() {
        // A naive `timestamp` column keeps the pre-existing cast.
        let f = build_watermark_filter_pg(
            "write_date",
            None,
            Some("2024-06-10 12:00:00"),
            Some("2024-06-15 00:00:00"),
            3600,
            false,
        )
        .unwrap();
        assert!(
            f.contains("'2024-06-10 12:00:00'::timestamp - interval '3600 seconds'"),
            "got: {f}"
        );

        // A tz-aware `timestamptz` column's cursor carries its UTC offset
        // (`WatermarkTracker::render`); `::timestamp` would silently drop it
        // and reinterpret the wall-clock digits in the session's own
        // TimeZone, shifting the lower bound by that offset.
        let f = build_watermark_filter_pg(
            "write_date",
            None,
            Some("2024-06-10 12:00:00+00"),
            Some("2024-06-15 00:00:00+00"),
            3600,
            true,
        )
        .unwrap();
        assert!(
            f.contains("'2024-06-10 12:00:00+00'::timestamptz - interval '3600 seconds'"),
            "got: {f}"
        );
    }

    #[test]
    fn watermark_pg_is_tz_aware_matches_the_resolved_arrow_type() {
        let cols = vec![
            col_typed("naive_ts", DataType::Timestamp(TimeUnit::Microsecond, None)),
            col_typed(
                "tz_ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            col_typed("id", DataType::Int64),
        ];
        assert!(!watermark_pg_is_tz_aware("naive_ts", &cols));
        assert!(watermark_pg_is_tz_aware("tz_ts", &cols));
        assert!(!watermark_pg_is_tz_aware("id", &cols));
        // Unresolved/unknown column name: false, same as the pre-fix default.
        assert!(!watermark_pg_is_tz_aware("missing", &cols));
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
    fn watermark_filter_clickhouse_types_both_bounds_in_the_columns_zone() {
        // `toString` renders a DateTime('Asia/Jakarta') cursor as Jakarta
        // wall-clock text; both bounds must parse it back in that zone rather
        // than as UTC, or the window shifts by the offset and rows are skipped.
        let t = "DateTime('Asia/Jakarta')";
        let f = build_watermark_filter_clickhouse(
            "updated_at",
            None,
            Some("2024-03-01 19:00:00"),
            Some("2024-03-01 20:00:00"),
            3600,
            Some(t),
        )
        .unwrap();
        assert_eq!(
            f,
            "`updated_at` > (CAST('2024-03-01 19:00:00' AS DateTime('Asia/Jakarta')) - INTERVAL \
             3600 SECOND) AND `updated_at` <= CAST('2024-03-01 20:00:00' AS \
             DateTime('Asia/Jakarta'))"
        );
        assert!(!f.contains("'UTC'"), "got: {f}");

        // No lookback: the same typed literal on both sides. A bare DateTime
        // (server timezone) is typed as bare DateTime, so it parses in the
        // server's zone — the one toString rendered it in.
        let f = build_watermark_filter_clickhouse(
            "ts",
            None,
            Some("2024-01-01 22:00:00"),
            Some("2024-01-02 04:00:00"),
            0,
            Some("Nullable(DateTime64(9))"),
        )
        .unwrap();
        assert_eq!(
            f,
            "`ts` > CAST('2024-01-01 22:00:00' AS Nullable(DateTime64(9))) AND `ts` <= \
             CAST('2024-01-02 04:00:00' AS Nullable(DateTime64(9)))"
        );
    }

    #[test]
    fn watermark_filter_clickhouse_keeps_plain_literals_for_non_temporal_types() {
        let f = build_watermark_filter_clickhouse(
            "id",
            None,
            Some("41"),
            Some("99"),
            0,
            Some("UInt64"),
        )
        .unwrap();
        assert_eq!(f, "`id` > '41' AND `id` <= '99'");
        // A type the server did not report falls back to the old literals.
        let f = build_watermark_filter_clickhouse("ts", None, Some("2024-01-01"), None, 60, None)
            .unwrap();
        assert_eq!(
            f,
            "`ts` > (toDateTime64('2024-01-01', 6, 'UTC') - INTERVAL 60 SECOND)"
        );
    }

    #[test]
    fn clickhouse_temporal_type_detection() {
        for t in [
            "Date",
            "Date32",
            "DateTime",
            "DateTime('Asia/Jakarta')",
            "DateTime64(3, 'America/New_York')",
            "Nullable(DateTime)",
            "LowCardinality(Nullable(Date))",
        ] {
            assert!(is_clickhouse_temporal_type(t), "{t}");
        }
        for t in [
            "UInt64",
            "String",
            "Nullable(String)",
            "DateTime; DROP TABLE x",
            "DateTime('a')--",
        ] {
            assert!(!is_clickhouse_temporal_type(t), "{t}");
        }
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
                declared_decimal: None,
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
    fn a_null_watermark_lands_only_where_the_destination_holds_one() {
        // Issue #27: a ReplacingMergeTree's version column is the watermark.
        let (mut cfg, _, src) = chunk_inputs("id", DataType::Int64, false);
        cfg.chunk_rows = None;
        let mut nullable = src.clone();
        nullable[1].nullable = true;
        let lands = |cfg: &TransferConfig, dest| {
            let plan = crate::transform::plan(&nullable, cfg, dest).unwrap();
            null_watermark_lands(&plan, "wm")
        };
        assert!(!lands(&cfg, crate::config::DestKind::ClickHouse));
        assert!(lands(&cfg, crate::config::DestKind::BigQuery));
        cfg.engine = Some("ReplacingMergeTree()".into());
        assert!(
            lands(&cfg, crate::config::DestKind::ClickHouse),
            "no version column"
        );
        cfg.engine = Some("MergeTree".into());
        assert!(lands(&cfg, crate::config::DestKind::ClickHouse));
        // Left out of the transfer, it has no column to fail.
        cfg.engine = None;
        cfg.exclude = vec!["wm".into()];
        let plan = crate::transform::plan(&nullable, &cfg, crate::config::DestKind::ClickHouse);
        assert!(plan.map_or(true, |p| null_watermark_lands(&p, "wm")));
    }

    #[test]
    fn resume_marker_with_empty_upper_pins_nothing() {
        let with_upper = ("41".to_string(), "2024-06-10 00:00:00".to_string());
        assert_eq!(
            resume_bounds(Some(&with_upper)),
            (
                Some("2024-06-10 00:00:00".to_string()),
                Some("41".to_string())
            )
        );
        let no_upper = ("41".to_string(), String::new());
        assert_eq!(
            resume_bounds(Some(&no_upper)),
            (None, Some("41".to_string()))
        );
        assert_eq!(resume_bounds(None), (None, None));
    }

    #[test]
    fn archive_run_ids_are_unique_across_back_to_back_calls() {
        // Regression guard: whole-second resolution (`now.timestamp()`) let
        // two runs into the same dest_table starting in the same second write
        // the identical archive object key, silently overwriting each other's
        // backup. Calling this in a tight loop is exactly the "quick backfill
        // loop" scenario, and every run_id it produces must be distinct.
        let cfg = ArchiveConfig::Gcs(crate::config::GcsArchiveConfig {
            bucket: "test-bucket".to_string(),
            prefix: "lake".to_string(),
            credentials_file: None,
            credentials_json: None,
            endpoint: None,
            compression: ParquetCompression::Zstd,
        });
        let mut ids = std::collections::HashSet::new();
        for _ in 0..20 {
            let info =
                build_archive_run_info(Some(cfg.clone()), "orders", &ArchiveUploads::default())
                    .unwrap()
                    .unwrap();
            assert!(
                ids.insert(info.run_id.clone()),
                "duplicate run_id: {}",
                info.run_id
            );
        }
    }

    #[test]
    fn build_chunk_plan_accepts_unique_integer_notnull_key() {
        let (cfg, plan, src) = chunk_inputs("id", DataType::Int64, false);
        let cp = build_chunk_plan(
            &cfg,
            &plan,
            &src,
            1000,
            None,
            Some("100".into()),
            None,
            false,
        )
        .unwrap();
        assert_eq!(cp.keyset_col, "id");
        assert_eq!(cp.keyset_idx, 0);
        assert_eq!(cp.limit, 1000);
    }

    #[test]
    fn a_chunk_marker_records_the_cursor_a_resume_may_save() {
        let (cfg, plan, src) = chunk_inputs("id", DataType::Int64, false);
        let t = WatermarkTracker {
            idx: 0,
            unit: WatermarkUnit::NaiveMicros,
            max: AtomicI64::new(i64::MIN),
        };
        let chunk = |upper: Option<&str>, marker: MarkerBound| {
            let mut cp = build_chunk_plan(
                &cfg,
                &plan,
                &src,
                1000,
                None,
                upper.map(String::from),
                None,
                false,
            )
            .unwrap();
            cp.marker = marker;
            cp
        };
        let best = Arc::new(AtomicI64::new(i64::MIN));
        let shared = |ago: u64, floor: Option<&str>, best: &Arc<AtomicI64>| MarkerBound::Stream {
            started: Instant::now() - std::time::Duration::from_secs(ago),
            floor: floor.map(String::from),
            best: best.clone(),
        };
        let stream =
            |ago: u64, floor: Option<&str>| shared(ago, floor, &Arc::new(AtomicI64::new(i64::MIN)));
        // A frozen bound is the bound, whatever was read; without one, none.
        let frozen = chunk(Some("2026-02-01 00:00:00"), MarkerBound::Frozen);
        assert_eq!(frozen.marker_upper(Some(&t), 60), "2026-02-01 00:00:00");
        assert_eq!(chunk(None, MarkerBound::Frozen).marker_upper(None, 60), "");
        // A resume past a marker that recorded no bound records none either,
        // even with a fresh MAX of its own to bound its read.
        let unbounded = chunk(Some("2026-02-01 00:00:00"), MarkerBound::Unbounded);
        assert_eq!(unbounded.marker_upper(Some(&t), 60), "");
        // Taken from the rows read: nothing read yet, no bound.
        let floor = Some("2026-01-01 00:00:00");
        assert_eq!(chunk(None, stream(0, floor)).marker_upper(Some(&t), 60), "");
        t.max
            .store(t.parse("2026-01-01 02:00:00").unwrap(), Ordering::Relaxed);
        // Within the lookback, the newest watermark read.
        assert_eq!(
            chunk(None, stream(0, floor)).marker_upper(Some(&t), 60),
            "2026-01-01 02:00:00.000000"
        );
        // A read that has taken an hour longer than the lookback is moved back
        // by that hour (its seconds rounded up)...
        assert_eq!(
            chunk(None, stream(3_659, floor)).marker_upper(Some(&t), 60),
            "2026-01-01 01:00:00.000000"
        );
        // ...never below the cursor it started from...
        assert_eq!(
            chunk(None, stream(36_000, floor)).marker_upper(Some(&t), 60),
            "2026-01-01 00:00:00.000000"
        );
        // ...unless everything read is, as an uninterrupted read would save:
        // a cursor ahead of the source isn't kept past what was read.
        let ahead = Some("2026-01-01 03:00:00");
        assert_eq!(
            chunk(None, stream(36_000, ahead)).marker_upper(Some(&t), 60),
            "2026-01-01 02:00:00.000000"
        );
        // A later chunk never records less than an earlier one did: each was
        // as safe a bound when recorded, and stays so.
        assert_eq!(
            chunk(None, shared(0, floor, &best)).marker_upper(Some(&t), 60),
            "2026-01-01 02:00:00.000000"
        );
        assert_eq!(
            chunk(None, shared(36_000, floor, &best)).marker_upper(Some(&t), 60),
            "2026-01-01 02:00:00.000000"
        );
        // A first read has no floor.
        assert_eq!(
            chunk(None, stream(35_999, None)).marker_upper(Some(&t), 60),
            "2025-12-31 16:01:00.000000"
        );
    }

    #[test]
    fn a_date_cursor_from_a_read_across_midnight_goes_back_a_day() {
        // Issue #26: a 50-minute chunked read from 23:30 on 01-01 saw a row
        // changed at 00:05, so the newest DATE read is 01-02. A row read at
        // 23:31 and changed at 23:45 is dated 01-01, below the next run's
        // `> '01-02' - 3600s` unless the cursor goes back to 01-01.
        let d = WatermarkTracker {
            idx: 0,
            unit: WatermarkUnit::Days,
            max: AtomicI64::new(i64::MIN),
        };
        d.max
            .store(d.parse("2026-01-02").unwrap(), Ordering::Relaxed);
        let rewound = |lookback| d.render_rewound(d.stream_rewind_secs(3_000.0, lookback), None);
        assert_eq!(rewound(3_600).as_deref(), Some("2026-01-01"));
        assert_eq!(rewound(86_400).as_deref(), Some("2026-01-01"));
        // A day longer than the read, the lookback covers the change's own
        // day already.
        assert_eq!(rewound(129_600).as_deref(), Some("2026-01-02"));
        assert_eq!(rewound(172_800).as_deref(), Some("2026-01-02"));
        // A read longer than what the lookback leaves goes back further.
        assert_eq!(
            d.render_rewound(d.stream_rewind_secs(90_000.0, 3_600), None)
                .as_deref(),
            Some("2025-12-31")
        );
        // Never below the cursor the read started from.
        assert_eq!(
            d.render_rewound(d.stream_rewind_secs(3_000.0, 3_600), Some("2026-01-02"))
                .as_deref(),
            Some("2026-01-02")
        );
        // A timestamp watermark keeps the whole lookback.
        let t = WatermarkTracker {
            idx: 0,
            unit: WatermarkUnit::NaiveMicros,
            max: AtomicI64::new(i64::MIN),
        };
        assert_eq!(t.stream_rewind_secs(3_000.0, 3_600), 0);
        assert_eq!(t.stream_rewind_secs(3_000.0, 600), 2_400);

        // A chunk's resume marker records the same day.
        let (cfg, plan, src) = chunk_inputs("id", DataType::Int64, false);
        let mut cp = build_chunk_plan(&cfg, &plan, &src, 1000, None, None, None, false).unwrap();
        cp.marker = MarkerBound::Stream {
            started: Instant::now() - std::time::Duration::from_secs(3_000),
            floor: None,
            best: Arc::new(AtomicI64::new(i64::MIN)),
        };
        assert_eq!(cp.marker_upper(Some(&d), 3_600), "2026-01-01");
    }

    #[test]
    fn a_seed_floors_a_cursor_only_in_the_trackers_own_unit() {
        let tz = WatermarkTracker {
            idx: 0,
            unit: WatermarkUnit::UtcMicros,
            max: AtomicI64::new(i64::MIN),
        };
        // A committed cursor is quickhouse's own, offset or not.
        assert_eq!(
            cursor_floor(Some("2026-10-06 10:00:00"), None, Some(&tz)).as_deref(),
            Some("2026-10-06 10:00:00")
        );
        // A seed with its offset floors it; one without is read in the
        // session's zone, so it isn't guessed at.
        assert_eq!(
            cursor_floor(None, Some("2026-10-06 09:00:00+00"), Some(&tz)).as_deref(),
            Some("2026-10-06 09:00:00+00")
        );
        assert_eq!(
            cursor_floor(None, Some("2026-10-06 09:00:00"), Some(&tz)),
            None
        );
        assert_eq!(
            cursor_floor(None, Some("2026-10-06 09:00:00+00"), None),
            None
        );
        assert_eq!(cursor_floor(None, None, Some(&tz)), None);
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

    /// Issue #7: a keyset that resolved as nullable (every PostgreSQL
    /// `source_query` column does) is accepted once the source proves it NOT
    /// NULL, or the caller asserts it, and the refusal names the assertion.
    #[test]
    fn a_nullable_keyset_is_accepted_when_proven_or_asserted() {
        let (mut cfg, plan, src) = chunk_inputs("id", DataType::Int64, true);
        let msg = build_chunk_plan(&cfg, &plan, &src, 1000, None, None, None, false)
            .unwrap_err()
            .to_string();
        assert!(msg.contains("keyset_not_null=True"), "{msg}");
        assert!(build_chunk_plan(&cfg, &plan, &src, 1000, None, None, None, true).is_ok());
        cfg.keyset_not_null = true;
        assert!(build_chunk_plan(&cfg, &plan, &src, 1000, None, None, None, false).is_ok());
    }

    #[test]
    fn build_chunk_plan_rejects_nullable_non_integer_and_transformed_keys() {
        // Nullable key (NULLs silently skipped) -> reject.
        let (cfg, plan, src) = chunk_inputs("id", DataType::Int64, true);
        assert!(
            build_chunk_plan(&cfg, &plan, &src, 1000, None, None, None, false)
                .unwrap_err()
                .to_string()
                .contains("NOT NULL")
        );
        // Non-integer key -> reject.
        let (cfg, plan, src) = chunk_inputs("id", DataType::Utf8, false);
        assert!(
            build_chunk_plan(&cfg, &plan, &src, 1000, None, None, None, false)
                .unwrap_err()
                .to_string()
                .contains("integer")
        );
        // Transformed key (decoded value != raw column) -> reject.
        let (mut cfg, plan, src) = chunk_inputs("id", DataType::Int64, false);
        cfg.column_transforms =
            std::collections::HashMap::from([("id".to_string(), "id + 1".to_string())]);
        assert!(
            build_chunk_plan(&cfg, &plan, &src, 1000, None, None, None, false)
                .unwrap_err()
                .to_string()
                .contains("column_transforms")
        );
    }
}
