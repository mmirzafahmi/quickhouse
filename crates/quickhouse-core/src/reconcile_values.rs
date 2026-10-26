//! Value reconciliation: measure how far the *values* a destination holds have
//! drifted from the source they were synced from, column by column.
//!
//! # Why this exists
//!
//! [`reconcile_keys`](crate::reconcile::reconcile_keys) measures key drift:
//! rows the destination holds that the source deleted. It can't see value
//! drift, where the same keys exist on both sides but a column differs, and
//! that class of defect leaves every count-based check green. Issue #28 is the
//! case behind this module: a `source_query` that had dropped columns the
//! destination already held made every incremental run write those columns as
//! NULL, for two months, on 26 tables, while key counts matched throughout. A
//! per-column count of "NULL in the destination, a value in the source" flags
//! exactly that.
//!
//! # What it does
//!
//! 1. Read the key and the compared columns from the destination over a
//!    window, through the ClickHouse source reader, with `FINAL` where the
//!    table's engine merges rows: an unmerged `ReplacingMergeTree` holds more
//!    than one row per key, and only the newest is the destination's value.
//! 2. Read the same from the source, through that engine's own reader. These
//!    are the decoders a sync uses, so a value arrives here as it arrived there.
//! 3. Match rows by key and compare each column under a [`Rule`] picked from
//!    the two sides' types, counting per column the mismatches and the NULLs on
//!    one side only, and keeping sample keys.
//!
//! Both sides' rows are held in memory, so the window is required: this holds
//! rows, where `reconcile_keys` holds only a keyset.
//!
//! Measure only, for now. Comparing per window by hashing rows on both sides,
//! so the full-table form is cheap enough to schedule, and `repair=True`, which
//! re-syncs only the mismatched keys, come next.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use arrow::buffer::NullBuffer;
use arrow::util::display::array_value_to_string;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, TimestampMicrosecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, TimeUnit};
use futures::StreamExt;
use mysql_async::prelude::Queryable;

use crate::config::{ClickHouseSourceConfig, DestinationConfig, SourceConfig};
use crate::ddl::{qualified, quote_ident};
use crate::decimal::decimal128_to_string;
use crate::decode::CopyDecoder;
use crate::decode_clickhouse::ChArrowDecoder;
use crate::decode_mysql::MySqlBatcher;
use crate::error::{EtlError, Result};
use crate::sink::clickhouse::escape_sql_string;
use crate::source::clickhouse::quote_ch_table;
use crate::source::mysql::{quote_my, quote_my_table};
use crate::source::postgres::{quote_pg, quote_pg_table};
use crate::source::{ClickHouseSource, MySqlSource, Partition, PgSource};
use crate::types::clickhouse::map_ch_type;
use crate::types::ColumnType;

/// What to compare.
#[derive(Debug, Clone)]
pub struct ValueReconcileConfig {
    /// Source table to read. Mutually exclusive with [`Self::source_query`];
    /// exactly one is required.
    pub source_table: Option<String>,
    /// Source query to read in place of a table. Must project [`Self::key`]
    /// and the compared columns under their destination names.
    pub source_query: Option<String>,
    /// Destination table to compare against, in the destination's database.
    pub dest_table: String,
    /// The identifying column, present on both sides under the same name.
    /// Matched by typed value, so an `Int32` source key finds its `Int64`
    /// destination twin. A float key is refused: there is no exact match to
    /// make on one.
    pub key: String,
    /// Columns to compare, present on both sides under the same name. Empty
    /// compares every column the two share, matched by exact name, the way the
    /// ClickHouse sink matches an insert's columns to the table's.
    pub columns: Vec<String>,
    /// SQL predicate bounding the source read, in the source's dialect, e.g.
    /// `"id > 1000000"`. Required: both sides' rows are held in memory.
    pub window: String,
    /// The same bound for the destination, in ClickHouse's dialect. Defaults
    /// to [`Self::window`].
    pub dest_window: Option<String>,
    /// Absolute tolerance for every numeric column: values within it of each
    /// other count as equal. `0` compares exactly.
    pub tolerance: f64,
    /// Per-column tolerances, overriding [`Self::tolerance`] for the columns
    /// named. Each must be a compared column that compares as a number.
    pub column_tolerance: HashMap<String, f64>,
    /// How many keys to carry back per column as samples. Diagnostic; the
    /// counts are exact regardless.
    pub sample_limit: usize,
}

impl Default for ValueReconcileConfig {
    fn default() -> Self {
        Self {
            source_table: None,
            source_query: None,
            dest_table: String::new(),
            key: String::new(),
            columns: Vec::new(),
            window: String::new(),
            dest_window: None,
            tolerance: 0.0,
            column_tolerance: HashMap::new(),
            sample_limit: 10,
        }
    }
}

/// What the comparison found, overall and per column.
#[derive(Debug, Clone, Default)]
pub struct ValueReconcileResult {
    /// Rows the source returned in the window.
    pub source_rows: u64,
    /// Rows the destination returned in the window, after `FINAL`.
    pub dest_rows: u64,
    /// Keys present exactly once on each side: the ones whose values were
    /// compared.
    pub compared_keys: u64,
    /// Keys in the source that the destination doesn't have.
    pub missing_keys: u64,
    /// Keys in the destination that the source doesn't have.
    pub orphan_keys: u64,
    /// Keys that appear more than once on either side. Their rows are left out
    /// of the comparison, since there is no telling which one is current.
    pub duplicate_keys: u64,
    /// One entry per compared column, in source column order.
    pub columns: Vec<ColumnDrift>,
    /// `(column, reason)` for each column on both sides that wasn't compared,
    /// because a side's type can't be read or the two types have no common
    /// one. Only when [`ValueReconcileConfig::columns`] is empty; a column
    /// named there that can't be compared is an error instead.
    pub skipped_columns: Vec<(String, String)>,
    pub duration_secs: f64,
}

/// One column's drift.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ColumnDrift {
    pub column: String,
    /// Keys whose value in this column was compared: every compared key.
    pub compared: u64,
    /// Keys whose value differs, including the two NULL counts below.
    pub mismatched: u64,
    /// NULL in the destination where the source holds a value: #28's
    /// signature. In a destination column that can't hold NULL, the value a
    /// NULL is stored as (the column's `DEFAULT`, else its type's default: `0`,
    /// `''`, `1970-01-01`) counts as NULL.
    pub null_in_dest_only: u64,
    /// NULL in the source where the destination holds a value.
    pub null_in_source_only: u64,
    /// Up to `sample_limit` keys whose value differs, rendered as text.
    pub sample: Vec<String>,
}

impl ValueReconcileConfig {
    fn validate(&self) -> Result<()> {
        if self.dest_table.trim().is_empty() {
            return Err(EtlError::config("reconcile_values requires dest_table"));
        }
        if self.key.trim().is_empty() {
            return Err(EtlError::config("reconcile_values requires key"));
        }
        match (&self.source_table, &self.source_query) {
            (Some(_), Some(_)) => Err(EtlError::config(
                "reconcile_values takes source_table or source_query, not both",
            )),
            (None, None) => Err(EtlError::config(
                "reconcile_values requires source_table or source_query",
            )),
            _ => Ok(()),
        }?;
        if self.window.trim().is_empty() {
            return Err(EtlError::config(
                "reconcile_values requires a window: both sides' rows are held in memory, so the \
                 read has to be bounded (e.g. \"id > 1000000\"). Pass \"1 = 1\" to compare a small \
                 table whole",
            ));
        }
        let tolerances = std::iter::once((None, self.tolerance)).chain(
            self.column_tolerance
                .iter()
                .map(|(c, t)| (Some(c.as_str()), *t)),
        );
        for (column, t) in tolerances {
            if !t.is_finite() || t < 0.0 {
                let what = column.map_or("tolerance".to_string(), |c| format!("tolerance['{c}']"));
                return Err(EtlError::config(format!(
                    "reconcile_values {what} must be a finite number >= 0, got {t}"
                )));
            }
        }
        Ok(())
    }

    fn effective_dest_window(&self) -> &str {
        self.dest_window.as_deref().unwrap_or(&self.window)
    }

    /// The key, then the named columns, each once.
    fn selected_columns(&self) -> Vec<&str> {
        let mut out = vec![self.key.as_str()];
        for c in &self.columns {
            if !out.contains(&c.as_str()) {
                out.push(c);
            }
        }
        out
    }
}

/// Compare the values a destination holds with its source's, per column, over
/// a window. See the module docs.
///
/// Sources: PostgreSQL, MySQL and ClickHouse, as for `reconcile_keys`.
/// Destination: ClickHouse; a BigQuery destination is refused for now.
pub async fn reconcile_values(
    source_cfg: SourceConfig,
    dest: DestinationConfig,
    cfg: ValueReconcileConfig,
) -> Result<ValueReconcileResult> {
    // Same one-time rustls provider selection every entry point needs; see
    // `sync::run_transfer_impl` for why it lives at the entry point.
    let _ = rustls::crypto::ring::default_provider().install_default();
    cfg.validate()?;
    let started = Instant::now();

    let dest =
        match dest {
            DestinationConfig::ClickHouse(dest) => dest,
            DestinationConfig::BigQuery(_) => return Err(EtlError::config(
                "reconcile_values does not support a BigQuery destination yet (ClickHouse only); \
                 reconcile_keys measures key drift against either",
            )),
            DestinationConfig::Csv(_) => return Err(EtlError::config(
                "reconcile_values is not supported for a CSV destination (ClickHouse only): it \
                 reads the destination's values with a query, and part files have none to run",
            )),
        };
    let source = SourceReader::open(&source_cfg)?;
    // The destination is read through the ClickHouse *source* reader: the same
    // casts and Arrow decode a ClickHouse -> anything sync relies on. Its
    // settings ride along, as they do on every request the sink makes.
    let reader = ClickHouseSource::new(&ClickHouseSourceConfig {
        url: dest.url.clone(),
        database: dest.database.clone(),
        user: dest.user.clone(),
        password: dest.password.clone(),
        settings: dest.settings.clone(),
        statement_timeout_secs: 0,
        // `dest_base_query` writes the destination's own `FINAL`, by its
        // engine, into the query this reader wraps.
        read_final: Some(false),
    })?;

    // Both schemas first: the plan decides which columns either read carries.
    let dest_table = describe_dest(&reader, &dest.database, &cfg.dest_table).await?;
    let source_cols = resolve_source(&source, &cfg).await?;
    let plan = plan_columns(&cfg, &source_cols, &dest_table.columns)?;

    // Destination first, then source, as `reconcile_keys` does: a row a
    // concurrent sync lands between the two reads is then at worst "missing".
    let dest_batches = read_dest(&reader, &dest_table, &cfg, &plan).await?;
    let source_batches = read_source(&source, &cfg, &plan).await?;

    let mut result = diff(&plan, &source_batches, &dest_batches, cfg.sample_limit)?;
    result.skipped_columns = plan.skipped;
    let drifting: Vec<String> = result
        .columns
        .iter()
        .filter(|c| c.mismatched > 0)
        .map(|c| {
            format!(
                "{} ({} mismatched, {} NULL in the destination only)",
                c.column, c.mismatched, c.null_in_dest_only
            )
        })
        .collect();
    if drifting.is_empty() {
        tracing::info!(
            "reconcile_values: {} key(s) compared across {} column(s) of '{}', no value drift",
            result.compared_keys,
            result.columns.len(),
            cfg.dest_table,
        );
    } else {
        tracing::warn!(
            "reconcile_values: {} of {} compared column(s) of '{}' differ from the source over \
             {} key(s): {}",
            drifting.len(),
            result.columns.len(),
            cfg.dest_table,
            result.compared_keys,
            drifting.join(", "),
        );
    }
    for (column, reason) in &result.skipped_columns {
        tracing::warn!("reconcile_values: column '{column}' not compared: {reason}");
    }
    result.duration_secs = started.elapsed().as_secs_f64();
    Ok(result)
}

/// How many rows each decoded batch holds. Batches only bound the decoders'
/// working set here; every row is kept until the diff anyway.
const READ_BATCH_ROWS: usize = 65_536;

/// The source, opened for the engines `reconcile_values` reads.
enum SourceReader {
    Postgres(PgSource),
    MySql(MySqlSource),
    ClickHouse(ClickHouseSource),
}

impl SourceReader {
    fn open(source_cfg: &SourceConfig) -> Result<Self> {
        Ok(match source_cfg {
            SourceConfig::Postgres(pg) => SourceReader::Postgres(PgSource::new(
                pg.dsn.clone(),
                pg.statement_timeout_secs,
                pg.ca_cert_file.clone(),
                pg.client_cert_file.clone(),
                pg.client_key_file.clone(),
                "quickhouse-reconcile".to_string(),
            )),
            SourceConfig::MySql(my) => SourceReader::MySql(MySqlSource::new(
                my.dsn.clone(),
                my.statement_timeout_secs,
                my.ca_cert_file.clone(),
                my.require_tls,
                my.client_cert_file.clone(),
                my.client_key_file.clone(),
                my.utc_session,
                my.read_only,
            )),
            SourceConfig::ClickHouse(ch) => SourceReader::ClickHouse(ClickHouseSource::new(ch)?),
            other => {
                return Err(EtlError::config(format!(
                    "reconcile_values does not support a {} source (PostgreSQL, MySQL and \
                     ClickHouse only): a BigQuery source is normally itself a mirror rather than \
                     the system of record, and an HTTP API source has no table to compare against",
                    other.kind(),
                )))
            }
        })
    }
}

/// The statement the source's schema is resolved from: the named columns
/// only, so a type the reader can't decode elsewhere in the table doesn't
/// matter, or everything when no columns are named.
fn source_probe(
    cfg: &ValueReconcileConfig,
    quote: fn(&str) -> String,
    quote_table: fn(&str) -> String,
) -> String {
    let from = match (&cfg.source_query, &cfg.source_table) {
        (Some(q), _) => format!("({q}) AS _src"),
        (None, Some(t)) => quote_table(t),
        (None, None) => unreachable!("validated: one of source_table/source_query"),
    };
    if cfg.columns.is_empty() {
        return match &cfg.source_query {
            Some(q) => q.clone(),
            None => format!("SELECT * FROM {from}"),
        };
    }
    let cols = cfg
        .selected_columns()
        .into_iter()
        .map(quote)
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {cols} FROM {from}")
}

/// Resolve the source's columns, as the comparison reads them.
async fn resolve_source(
    source: &SourceReader,
    cfg: &ValueReconcileConfig,
) -> Result<Vec<ColumnType>> {
    let resolved = match source {
        SourceReader::Postgres(s) => {
            let client = s.connect().await?;
            let probe = source_probe(cfg, quote_pg, quote_pg_table);
            s.resolve_columns(&client, &probe, None).await
        }
        SourceReader::MySql(s) => {
            let mut conn = s.connect().await?;
            let probe = source_probe(cfg, quote_my, quote_my_table);
            // `tinyint(1)` as the integer it is, whatever a sync mapped it to:
            // it is matched against a `Bool` destination by truth and against
            // an integer one by value (see `Rule::Truth`).
            s.resolve_columns(&mut conn, &probe, false).await
        }
        SourceReader::ClickHouse(s) => {
            let probe = source_probe(cfg, quote_ident, quote_ch_table);
            s.resolve_columns(&probe, &[], &[]).await
        }
    };
    let cols = resolved.map_err(|e| match e {
        EtlError::UnsupportedType {
            engine,
            column,
            type_name,
        } if cfg.columns.is_empty() => EtlError::config(format!(
            "reconcile_values can't read source column '{column}' ({engine} type `{type_name}`). \
             Name the columns to compare with columns=[...], which leaves it out, or read it \
             converted through a source_query"
        )),
        other => other,
    })?;
    Ok(cols.into_iter().map(read_exactly).collect())
}

/// A resolved source column as the comparison reads it: a decimal that
/// declares its precision decoded exactly, not through `Float64`, and every
/// column nullable, since the decoders coerce some values (a zero date) to
/// NULL.
fn read_exactly(mut c: ColumnType) -> ColumnType {
    if let Some((p, s)) = c.declared_decimal {
        c.arrow = DataType::Decimal128(p, s);
    }
    c.nullable = true;
    c
}

/// One ordinary column of the destination table, from `system.columns`.
#[derive(Debug, Clone)]
struct DestColumn {
    name: String,
    /// As `system.columns` spells it: `LowCardinality(Nullable(String))`.
    declared: String,
    /// The column as the reader decodes it; `None` for a type it can't.
    read: Option<ColumnType>,
    /// Whether the column can hold NULL.
    nullable: bool,
    /// Its `DEFAULT` expression, when it has one.
    default_expression: Option<String>,
}

/// One `system.columns` row as a [`DestColumn`], or `None` for a column the
/// destination computes itself (`MATERIALIZED`, `ALIAS`, `EPHEMERAL`), which a
/// sync never writes.
fn dest_column(
    name: &str,
    declared: &str,
    default_kind: &str,
    default_expression: Option<&str>,
) -> Option<DestColumn> {
    if !matches!(default_kind, "" | "DEFAULT") {
        return None;
    }
    let mapped = map_ch_type(declared);
    Some(DestColumn {
        name: name.to_string(),
        declared: declared.to_string(),
        nullable: mapped.as_ref().is_some_and(|m| m.nullable),
        read: mapped.map(|m| ColumnType {
            name: name.to_string(),
            type_id: m.type_id,
            // Read nullable whatever the column is; `DestColumn::nullable`
            // keeps what it declares.
            nullable: true,
            arrow: m.arrow,
            clickhouse_inner: m.clickhouse_inner,
            arbitrary_precision_decimal: m.arbitrary_precision_decimal,
            declared_decimal: None,
        }),
        default_expression: (default_kind == "DEFAULT")
            .then(|| default_expression.map(str::to_string))
            .flatten()
            .filter(|e| !e.is_empty()),
    })
}

/// The destination table: its ordinary columns, and whether reads need `FINAL`.
#[derive(Debug, Clone)]
struct DestTable {
    /// `` `db`.`table` ``, as the sink names it.
    qualified: String,
    /// The engine merges rows sharing a sorting key, so `FINAL` applies.
    final_: bool,
    columns: Vec<DestColumn>,
}

/// Whether a table engine (as `system.tables` names it) merges rows sharing a
/// sorting key, so a read needs `FINAL` to see one row per key. A plain
/// `MergeTree` keeps every row and rejects `FINAL` outright (`Code: 181
/// ILLEGAL_FINAL` on 24.8); so does anything that isn't a `MergeTree`. The
/// same test a ClickHouse source's default `final=` makes.
fn merges_rows(engine: &str) -> bool {
    crate::source::clickhouse::engine_merges_rows(engine)
}

async fn describe_dest(
    reader: &ClickHouseSource,
    database: &str,
    table: &str,
) -> Result<DestTable> {
    let (db, t) = (escape_sql_string(database), escape_sql_string(table));
    let rows = reader
        .query_rows(&format!(
            "SELECT name, type, default_kind, default_expression FROM system.columns \
             WHERE database = '{db}' AND table = '{t}' ORDER BY position FORMAT TabSeparated"
        ))
        .await
        .map_err(|e| e.context("reading the destination's columns"))?;
    let columns: Vec<DestColumn> = rows
        .iter()
        .filter_map(|r| {
            let cell = |i: usize| r.get(i).cloned().flatten().unwrap_or_default();
            dest_column(
                &cell(0),
                &cell(1),
                &cell(2),
                r.get(3).cloned().flatten().as_deref(),
            )
        })
        .collect();
    if columns.is_empty() {
        return Err(EtlError::config(format!(
            "destination table {database}.{table} does not exist (or has no columns to compare)"
        )));
    }
    Ok(DestTable {
        qualified: qualified(database, table),
        final_: merges_rows(&table_engine(reader, database, table).await?),
        columns,
    })
}

/// A table's engine as `system.tables` names it; empty for one it doesn't list.
async fn table_engine(reader: &ClickHouseSource, database: &str, table: &str) -> Result<String> {
    Ok(reader
        .query_rows(&format!(
            "SELECT engine FROM system.tables WHERE database = '{}' AND name = '{}' \
             FORMAT TabSeparated",
            escape_sql_string(database),
            escape_sql_string(table),
        ))
        .await
        .map_err(|e| e.context("reading a table's engine"))?
        .into_iter()
        .next()
        .and_then(|r| r.into_iter().next().flatten())
        .unwrap_or_default())
}

/// How one column pair is compared, from the two sides' read types. Every rule
/// compares typed values; none compares text rendered on one side with text
/// rendered on the other.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Rule {
    /// Integers and decimals of any width, sign or scale, exactly, at the
    /// larger of the two scales. A source boolean is 1 or 0 here, as it lands
    /// in an integer column.
    Exact,
    /// Anything with a float on either side, as `f64`: equal when both are the
    /// same double, or within the tolerance when there is one.
    Float,
    /// A `Bool` destination: a source integer by its truth, the way an insert
    /// converts one. MySQL's `tinyint(1)` is read as the integer it is, so it
    /// matches a `Bool` this way and a `UInt8` by value.
    Truth,
    /// Text and bytes, byte for byte.
    Bytes,
    /// Calendar days. A source datetime counts as its UTC date.
    Date,
    /// Instants, as UTC microseconds whatever timezone either type names,
    /// truncated toward zero to the destination's precision, as ClickHouse
    /// truncates on insert: `unit` microseconds per tick (`1_000_000` for a
    /// `DateTime`, `1_000` for a `DateTime64(3)`). A source date counts as its
    /// UTC midnight.
    Instant { unit: i64 },
}

/// The kinds of value a read can produce, for picking a [`Rule`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Family {
    Int,
    Decimal,
    Float,
    Bool,
    Bytes,
    Date,
    Timestamp,
}

fn family(dt: &DataType) -> Option<Family> {
    Some(match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Family::Int,
        DataType::Decimal128(_, _) => Family::Decimal,
        DataType::Float32 | DataType::Float64 => Family::Float,
        DataType::Boolean => Family::Bool,
        DataType::Utf8 | DataType::Binary => Family::Bytes,
        DataType::Date32 => Family::Date,
        DataType::Timestamp(TimeUnit::Microsecond, _) => Family::Timestamp,
        _ => return None,
    })
}

fn rule_for(source: Family, dest: Family, dest_unit: i64) -> Option<Rule> {
    use Family::*;
    Some(match (source, dest) {
        (Int | Decimal | Bool, Int | Decimal) => Rule::Exact,
        (Float, Int | Decimal | Float) | (Int | Decimal, Float) => Rule::Float,
        (Int | Bool, Bool) => Rule::Truth,
        (Bytes, Bytes) => Rule::Bytes,
        (Date | Timestamp, Date) => Rule::Date,
        (Date | Timestamp, Timestamp) => Rule::Instant { unit: dest_unit },
        _ => return None,
    })
}

/// Microseconds per tick of a ClickHouse datetime type: `DateTime` holds
/// seconds, `DateTime64(P)` holds `10^-P` seconds (read here to at most
/// microseconds). `1` for anything else.
fn datetime_unit(declared: &str) -> i64 {
    let mut t = declared.trim();
    while let Some(inner) = t
        .strip_prefix("Nullable(")
        .or_else(|| t.strip_prefix("LowCardinality("))
        .and_then(|rest| rest.strip_suffix(')'))
    {
        t = inner.trim();
    }
    let precision = if let Some(args) = t.strip_prefix("DateTime64(") {
        args.split([',', ')'])
            .next()
            .and_then(|p| p.trim().parse::<u32>().ok())
            .unwrap_or(6)
    } else if t == "DateTime" || t.starts_with("DateTime(") {
        0
    } else {
        6
    };
    10i64.pow(6 - precision.min(6))
}

/// The key's kinds of value, which must agree across the two sides.
fn key_family(dt: &DataType) -> Option<&'static str> {
    match family(dt)? {
        Family::Int | Family::Decimal | Family::Bool => Some("a number"),
        Family::Bytes => Some("text"),
        Family::Date => Some("a date"),
        Family::Timestamp => Some("a datetime"),
        Family::Float => None,
    }
}

/// One compared column.
#[derive(Debug, Clone)]
struct ColumnPlan {
    name: String,
    rule: Rule,
    tolerance: f64,
    /// The source column as the read decodes it.
    source: ColumnType,
    /// The destination column as the read decodes it.
    dest: ColumnType,
    /// For a destination column that can't hold NULL: a per-row expression
    /// that is true where it holds what a NULL is stored as. See
    /// [`null_default_flag`].
    null_default: Option<String>,
}

/// What both reads carry and how each column is compared. Both reads return
/// the key first, then [`Self::columns`] in order; the destination's then
/// carries one flag per column with a [`ColumnPlan::null_default`], in order.
#[derive(Debug)]
struct Plan {
    key_source: ColumnType,
    key_dest: ColumnType,
    columns: Vec<ColumnPlan>,
    skipped: Vec<(String, String)>,
}

/// Whether a non-Nullable column holds what a NULL inserted into it is stored
/// as, as a per-row boolean expression: its `DEFAULT` expression, else its
/// type's default value. That is what `input_format_null_as_default` writes,
/// measured on 24.8 through `FORMAT ArrowStream`, the sink's format. The
/// destination evaluates it row by row, so a `DEFAULT` that reads another
/// column is matched exactly (one that isn't deterministic, `now()` say,
/// can't be). `None` for a Nullable column, where a NULL stays NULL.
fn null_default_flag(col: &DestColumn) -> Option<String> {
    if col.nullable {
        return None;
    }
    let c = quote_ident(&col.name);
    Some(match &col.default_expression {
        Some(expr) => format!("{c} = CAST(({expr}) AS {})", col.declared),
        None => format!("{c} = defaultValueOfArgumentType({c})"),
    })
}

fn plan_columns(
    cfg: &ValueReconcileConfig,
    source: &[ColumnType],
    dest: &[DestColumn],
) -> Result<Plan> {
    let find_source = |name: &str| source.iter().find(|c| c.name == name);
    let find_dest = |name: &str| dest.iter().find(|c| c.name == name);

    let key_source = find_source(&cfg.key).ok_or_else(|| {
        EtlError::config(format!("key column '{}' is not in the source", cfg.key))
    })?;
    let key_dest_column = find_dest(&cfg.key).ok_or_else(|| {
        EtlError::config(format!(
            "key column '{}' is not in destination table '{}'",
            cfg.key, cfg.dest_table
        ))
    })?;
    let key_dest = key_dest_column.read.clone().ok_or_else(|| {
        EtlError::config(format!(
            "key column '{}' has a destination type reconcile_values can't read (`{}`)",
            cfg.key, key_dest_column.declared
        ))
    })?;
    match (key_family(&key_source.arrow), key_family(&key_dest.arrow)) {
        (Some(s), Some(d)) if s == d => {}
        (None, _) | (_, None) => {
            return Err(EtlError::config(format!(
                "key column '{}' is a float on one side, which has no exact match to make; key \
                 on an integer or text column",
                cfg.key
            )))
        }
        (Some(s), Some(d)) => {
            return Err(EtlError::config(format!(
                "key column '{}' is {s} in the source and {d} in the destination, so no key \
                 can match",
                cfg.key
            )))
        }
    }

    let explicit = !cfg.columns.is_empty();
    let names: Vec<&str> = if explicit {
        cfg.selected_columns().into_iter().skip(1).collect()
    } else {
        source
            .iter()
            .map(|c| c.name.as_str())
            .filter(|n| *n != cfg.key && find_dest(n).is_some())
            .collect()
    };

    let mut columns = Vec::new();
    let mut skipped = Vec::new();
    for name in names {
        let source_col = find_source(name)
            .ok_or_else(|| EtlError::config(format!("column '{name}' is not in the source")))?;
        let dest_col = find_dest(name).ok_or_else(|| {
            EtlError::config(format!(
                "column '{name}' is not in destination table '{}'",
                cfg.dest_table
            ))
        })?;
        let compared = match &dest_col.read {
            None => Err(format!(
                "its destination type `{}` can't be read",
                dest_col.declared
            )),
            Some(dest_read) => family(&source_col.arrow)
                .zip(family(&dest_read.arrow))
                .and_then(|(s, d)| rule_for(s, d, datetime_unit(&dest_col.declared)))
                .map(|rule| (rule, dest_read.clone()))
                .ok_or_else(|| {
                    format!(
                        "it reads as {:?} in the source and as `{}` in the destination, which \
                         have no common type to compare in",
                        source_col.arrow, dest_col.declared
                    )
                }),
        };
        let (rule, dest_read) = match compared {
            Ok(c) => c,
            Err(reason) if explicit => {
                return Err(EtlError::config(format!(
                    "column '{name}' can't be compared: {reason}"
                )))
            }
            Err(reason) => {
                skipped.push((name.to_string(), reason));
                continue;
            }
        };
        let tolerance = match cfg.column_tolerance.get(name) {
            Some(_) if !matches!(rule, Rule::Exact | Rule::Float) => {
                return Err(EtlError::config(format!(
                    "tolerance names column '{name}', which doesn't compare as a number"
                )))
            }
            Some(t) => *t,
            None => cfg.tolerance,
        };
        columns.push(ColumnPlan {
            name: name.to_string(),
            rule,
            tolerance,
            source: source_col.clone(),
            dest: dest_read,
            null_default: null_default_flag(dest_col),
        });
    }
    for name in cfg.column_tolerance.keys() {
        if !columns.iter().any(|c| &c.name == name) {
            return Err(EtlError::config(format!(
                "tolerance names column '{name}', which isn't compared"
            )));
        }
    }
    Ok(Plan {
        key_source: key_source.clone(),
        key_dest,
        columns,
        skipped,
    })
}

/// The alias the destination read gives column `i`'s NULL-default flag.
fn flag_alias(i: usize) -> String {
    format!("__quickhouse_null_default_{i}")
}

/// The destination read's inner statement: every column plus the NULL-default
/// flags, `FINAL` where the engine merges rows, and the window. The window and
/// the flags sit here, over the table's own columns, not in the projection that
/// wraps this: that projection's casts alias each column under its own name,
/// and ClickHouse resolves an alias before a column.
fn dest_base_query(table: &DestTable, window: &str, flags: &[(String, String)]) -> String {
    let flags: String = flags
        .iter()
        .map(|(alias, expr)| format!(", ({expr}) AS {}", quote_ident(alias)))
        .collect();
    let final_ = if table.final_ { " FINAL" } else { "" };
    format!(
        "SELECT *{flags} FROM {}{final_} WHERE ({window})",
        table.qualified
    )
}

async fn read_dest(
    reader: &ClickHouseSource,
    table: &DestTable,
    cfg: &ValueReconcileConfig,
    plan: &Plan,
) -> Result<Vec<RecordBatch>> {
    let mut cols = vec![plan.key_dest.clone()];
    cols.extend(plan.columns.iter().map(|c| c.dest.clone()));
    let mut flags = Vec::new();
    for (i, c) in plan.columns.iter().enumerate() {
        if let Some(expr) = &c.null_default {
            let alias = flag_alias(i);
            cols.push(ColumnType {
                name: alias.clone(),
                type_id: 0,
                nullable: true,
                arrow: DataType::Boolean,
                clickhouse_inner: "Bool".to_string(),
                arbitrary_precision_decimal: false,
                declared_decimal: None,
            });
            flags.push((alias, expr.clone()));
        }
    }
    let base = dest_base_query(table, cfg.effective_dest_window(), &flags);
    let batches = read_clickhouse(reader, &base, &cols)
        .await
        .map_err(|e| e.context("reading the destination"))?;
    tracing::info!(
        "reconcile_values: destination '{}' returned {} row(s) in the window{}",
        cfg.dest_table,
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        if table.final_ {
            " (read with FINAL)"
        } else {
            ""
        },
    );
    Ok(batches)
}

async fn read_source(
    source: &SourceReader,
    cfg: &ValueReconcileConfig,
    plan: &Plan,
) -> Result<Vec<RecordBatch>> {
    let mut cols = vec![plan.key_source.clone()];
    cols.extend(plan.columns.iter().map(|c| c.source.clone()));
    let names: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();
    let exprs = vec![None; names.len()];
    let partition = Partition {
        label: "reconcile".into(),
        predicate: None,
    };
    let (table, query) = (cfg.source_table.as_deref(), cfg.source_query.as_deref());
    let mut batches = Vec::new();
    match source {
        SourceReader::Postgres(s) => {
            let client = s.connect().await?;
            let sql = s.copy_sql(
                &names,
                &exprs,
                table,
                query,
                &partition,
                Some(&cfg.window),
                None,
            );
            tracing::debug!("reconcile_values source read: {sql}");
            let stream = s.copy_stream(&client, &sql).await?;
            futures::pin_mut!(stream);
            let mut decoder = CopyDecoder::with_batch_bytes(&cols, READ_BATCH_ROWS, 0)?;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| EtlError::from(e).context("reading the source"))?;
                batches.extend(decoder.feed(&chunk)?);
            }
            if !decoder.saw_trailer() {
                return Err(EtlError::decode(
                    "the source's COPY stream ended without a trailer",
                ));
            }
            batches.extend(decoder.finish()?);
        }
        SourceReader::MySql(s) => {
            let mut conn = s.connect().await?;
            let sql = s.select_sql(
                &names,
                &exprs,
                table,
                query,
                &partition,
                Some(&cfg.window),
                None,
            );
            tracing::debug!("reconcile_values source read: {sql}");
            // The binary protocol, as a sync reads: the text protocol returns
            // every value as text, whatever the column's type.
            let stmt = conn
                .prep(sql)
                .await
                .map_err(|e| EtlError::from(e).context("preparing the source read"))?;
            let mut result = conn
                .exec_iter(stmt, ())
                .await
                .map_err(|e| EtlError::from(e).context("reading the source"))?;
            let stream = result
                .stream::<mysql_async::Row>()
                .await
                .map_err(|e| EtlError::from(e).context("reading the source"))?
                .ok_or_else(|| EtlError::other("the source read returned no result set"))?;
            futures::pin_mut!(stream);
            let mut batcher = MySqlBatcher::with_batch_bytes(&cols, READ_BATCH_ROWS, 0)?;
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| EtlError::from(e).context("reading a source row"))?;
                batches.extend(batcher.append_row(row)?);
            }
            batches.extend(batcher.finish()?);
        }
        SourceReader::ClickHouse(s) => {
            let from = match (query, table) {
                (Some(q), _) => format!("({q}) AS _src"),
                (None, Some(t)) => {
                    // `FINAL` by the descriptor's `final=`, by default where
                    // the table's engine merges rows, as a sync reads it.
                    s.resolve_final(Some(t), None).await?;
                    s.table_from(t)
                }
                (None, None) => unreachable!("validated: one of source_table/source_query"),
            };
            let base = format!("SELECT * FROM {from} WHERE ({})", cfg.window);
            batches = read_clickhouse(s, &base, &cols)
                .await
                .map_err(|e| e.context("reading the source"))?;
        }
    }
    tracing::info!(
        "reconcile_values: source '{}' returned {} row(s) in the window",
        table.or(query).unwrap_or("?"),
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
    );
    Ok(batches)
}

/// Read `cols` from `base` (a statement over the table) through the ClickHouse
/// source's casts and Arrow decode.
async fn read_clickhouse(
    reader: &ClickHouseSource,
    base: &str,
    cols: &[ColumnType],
) -> Result<Vec<RecordBatch>> {
    let names: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();
    let exprs = vec![None; names.len()];
    let partition = Partition {
        label: "reconcile".into(),
        predicate: None,
    };
    let sql = reader.select_sql(
        &names,
        &exprs,
        cols,
        None,
        Some(base),
        &partition,
        None,
        None,
    );
    tracing::debug!("reconcile_values clickhouse read: {sql}");
    let resp = reader.stream_arrow(&sql, READ_BATCH_ROWS).await?;
    let body = resp.bytes_stream();
    futures::pin_mut!(body);
    let mut decoder = ChArrowDecoder::new(cols, 0);
    let mut batches = Vec::new();
    while let Some(part) = body.next().await {
        batches.extend(decoder.feed(part.map_err(EtlError::from)?)?);
    }
    decoder.finish()?;
    Ok(batches)
}

/// One value, as a comparison sees it.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Cell<'a> {
    Null,
    Int(i128),
    /// A mantissa and its scale.
    Dec(i128, i8),
    Float(f64),
    Bool(bool),
    Bytes(&'a [u8]),
    /// Days since 1970-01-01.
    Date(i32),
    /// Microseconds since 1970-01-01 UTC.
    Micros(i64),
}

/// One column of one batch, downcast once so each cell read is an index.
struct View<'a> {
    nulls: Option<&'a NullBuffer>,
    values: Values<'a>,
}

enum Values<'a> {
    Bool(&'a BooleanArray),
    I8(&'a Int8Array),
    I16(&'a Int16Array),
    I32(&'a Int32Array),
    I64(&'a Int64Array),
    U8(&'a UInt8Array),
    U16(&'a UInt16Array),
    U32(&'a UInt32Array),
    U64(&'a UInt64Array),
    F32(&'a Float32Array),
    F64(&'a Float64Array),
    Dec(&'a Decimal128Array, i8),
    Text(&'a StringArray),
    Bin(&'a BinaryArray),
    Date(&'a Date32Array),
    Micros(&'a TimestampMicrosecondArray),
}

impl<'a> View<'a> {
    fn new(col: &'a ArrayRef) -> Result<Self> {
        let values = match col.data_type() {
            DataType::Boolean => Values::Bool(col.as_boolean()),
            DataType::Int8 => Values::I8(col.as_primitive::<Int8Type>()),
            DataType::Int16 => Values::I16(col.as_primitive::<Int16Type>()),
            DataType::Int32 => Values::I32(col.as_primitive::<Int32Type>()),
            DataType::Int64 => Values::I64(col.as_primitive::<Int64Type>()),
            DataType::UInt8 => Values::U8(col.as_primitive::<UInt8Type>()),
            DataType::UInt16 => Values::U16(col.as_primitive::<UInt16Type>()),
            DataType::UInt32 => Values::U32(col.as_primitive::<UInt32Type>()),
            DataType::UInt64 => Values::U64(col.as_primitive::<UInt64Type>()),
            DataType::Float32 => Values::F32(col.as_primitive::<Float32Type>()),
            DataType::Float64 => Values::F64(col.as_primitive::<Float64Type>()),
            DataType::Decimal128(_, s) => Values::Dec(col.as_primitive::<Decimal128Type>(), *s),
            DataType::Utf8 => Values::Text(col.as_string::<i32>()),
            DataType::Binary => Values::Bin(col.as_binary::<i32>()),
            DataType::Date32 => Values::Date(col.as_primitive::<Date32Type>()),
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Values::Micros(col.as_primitive::<TimestampMicrosecondType>())
            }
            other => {
                return Err(EtlError::internal(format!(
                    "reconcile_values read a column as {other:?}, which it has no comparison for"
                )))
            }
        };
        Ok(Self {
            nulls: col.nulls(),
            values,
        })
    }

    fn cell(&self, i: usize) -> Cell<'a> {
        if self.nulls.is_some_and(|n| n.is_null(i)) {
            return Cell::Null;
        }
        match self.values {
            Values::Bool(a) => Cell::Bool(a.value(i)),
            Values::I8(a) => Cell::Int(a.value(i).into()),
            Values::I16(a) => Cell::Int(a.value(i).into()),
            Values::I32(a) => Cell::Int(a.value(i).into()),
            Values::I64(a) => Cell::Int(a.value(i).into()),
            Values::U8(a) => Cell::Int(a.value(i).into()),
            Values::U16(a) => Cell::Int(a.value(i).into()),
            Values::U32(a) => Cell::Int(a.value(i).into()),
            Values::U64(a) => Cell::Int(a.value(i).into()),
            Values::F32(a) => Cell::Float(a.value(i).into()),
            Values::F64(a) => Cell::Float(a.value(i)),
            Values::Dec(a, s) => Cell::Dec(a.value(i), s),
            Values::Text(a) => Cell::Bytes(a.value(i).as_bytes()),
            Values::Bin(a) => Cell::Bytes(a.value(i)),
            Values::Date(a) => Cell::Date(a.value(i)),
            Values::Micros(a) => Cell::Micros(a.value(i)),
        }
    }
}

/// Column `idx` of every batch, as views.
fn views(batches: &[RecordBatch], idx: usize) -> Result<Vec<View<'_>>> {
    batches.iter().map(|b| View::new(b.column(idx))).collect()
}

/// A key, normalised so equal values from differently typed columns hash
/// alike: any integer width, and a decimal with its trailing zeros dropped (a
/// whole one as an integer).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum KeyValue<'a> {
    Int(i128),
    Decimal(i128, i8),
    Bytes(&'a [u8]),
    Date(i32),
    Micros(i64),
}

/// `None` for a NULL key, which identifies no row, and for a float, which
/// [`plan_columns`] refuses as a key.
fn key_value(c: Cell<'_>) -> Option<KeyValue<'_>> {
    Some(match c {
        Cell::Int(i) => KeyValue::Int(i),
        Cell::Bool(b) => KeyValue::Int(b.into()),
        Cell::Dec(mut m, mut s) => {
            while s > 0 && m % 10 == 0 {
                m /= 10;
                s -= 1;
            }
            if s == 0 {
                KeyValue::Int(m)
            } else {
                KeyValue::Decimal(m, s)
            }
        }
        Cell::Bytes(b) => KeyValue::Bytes(b),
        Cell::Date(d) => KeyValue::Date(d),
        Cell::Micros(us) => KeyValue::Micros(us),
        Cell::Null | Cell::Float(_) => return None,
    })
}

/// Where a row is: batch, then row within it.
#[derive(Debug, Clone, Copy)]
struct Loc {
    batch: usize,
    row: usize,
}

/// One side's keys: where each first appears, in read order, and which
/// appear more than once.
struct KeyIndex<'a> {
    first: HashMap<KeyValue<'a>, Loc>,
    order: Vec<KeyValue<'a>>,
    dups: HashSet<KeyValue<'a>>,
}

impl<'a> KeyIndex<'a> {
    fn build(batches: &'a [RecordBatch]) -> Result<Self> {
        let mut index = KeyIndex {
            first: HashMap::new(),
            order: Vec::new(),
            dups: HashSet::new(),
        };
        for (b, view) in views(batches, 0)?.into_iter().enumerate() {
            for row in 0..batches[b].num_rows() {
                let Some(k) = key_value(view.cell(row)) else {
                    continue;
                };
                match index.first.entry(k) {
                    Entry::Vacant(e) => {
                        e.insert(Loc { batch: b, row });
                        index.order.push(k);
                    }
                    Entry::Occupied(_) => {
                        index.dups.insert(k);
                    }
                }
            }
        }
        Ok(index)
    }
}

/// How one compared cell came out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Equal,
    Mismatch,
    NullInDestOnly,
    NullInSourceOnly,
}

/// Compare one source cell with its destination cell. `holds_null_default` is
/// whether the destination column can't hold NULL and holds what a NULL is
/// stored as there (see [`null_default_flag`]).
fn classify(
    rule: Rule,
    source: Cell<'_>,
    dest: Cell<'_>,
    holds_null_default: bool,
    tolerance: f64,
) -> Outcome {
    match (source, dest) {
        (Cell::Null, Cell::Null) => Outcome::Equal,
        // What a sync writes for a source NULL into a column that can't hold
        // one: the column's default, `0` or `''` most often.
        (Cell::Null, _) if holds_null_default => Outcome::Equal,
        (Cell::Null, _) => Outcome::NullInSourceOnly,
        (_, Cell::Null) => Outcome::NullInDestOnly,
        _ if equal(rule, source, dest, tolerance) => Outcome::Equal,
        // The same default over a source value is #28's damage, in a column
        // that can't show it as NULL.
        _ if holds_null_default => Outcome::NullInDestOnly,
        _ => Outcome::Mismatch,
    }
}

/// Whether two non-NULL cells are equal under `rule`.
fn equal(rule: Rule, source: Cell<'_>, dest: Cell<'_>, tolerance: f64) -> bool {
    match rule {
        Rule::Exact => match (exact(source), exact(dest)) {
            (Some(s), Some(d)) => decimals_within(s, d, tolerance),
            _ => false,
        },
        Rule::Float => match (as_f64(source), as_f64(dest)) {
            (Some(s), Some(d)) => floats_within(s, d, tolerance),
            _ => false,
        },
        Rule::Truth => truth(source).is_some() && truth(source) == truth(dest),
        Rule::Bytes => matches!((source, dest), (Cell::Bytes(s), Cell::Bytes(d)) if s == d),
        Rule::Date => days(source).is_some() && days(source) == days(dest),
        Rule::Instant { unit } => match (micros(source), micros(dest)) {
            (Some(s), Some(d)) => s / unit == d / unit,
            _ => false,
        },
    }
}

fn exact(c: Cell<'_>) -> Option<(i128, i8)> {
    match c {
        Cell::Int(i) => Some((i, 0)),
        Cell::Dec(m, s) => Some((m, s)),
        Cell::Bool(b) => Some((b.into(), 0)),
        _ => None,
    }
}

fn as_f64(c: Cell<'_>) -> Option<f64> {
    match c {
        Cell::Int(i) => Some(i as f64),
        Cell::Dec(m, s) => Some(decimal_to_f64(m, s)),
        Cell::Float(f) => Some(f),
        _ => None,
    }
}

fn truth(c: Cell<'_>) -> Option<bool> {
    match c {
        Cell::Int(i) => Some(i != 0),
        Cell::Bool(b) => Some(b),
        _ => None,
    }
}

const MICROS_PER_DAY: i64 = 86_400_000_000;

fn days(c: Cell<'_>) -> Option<i64> {
    match c {
        Cell::Date(d) => Some(d.into()),
        Cell::Micros(us) => Some(us.div_euclid(MICROS_PER_DAY)),
        _ => None,
    }
}

fn micros(c: Cell<'_>) -> Option<i64> {
    match c {
        Cell::Micros(us) => Some(us),
        Cell::Date(d) => Some(i64::from(d) * MICROS_PER_DAY),
        _ => None,
    }
}

/// Two decimals, exactly: both rescaled to the larger scale, then compared as
/// integers. Past `i128` at that scale (only near the type's limits), as
/// doubles.
fn decimals_within((s_m, s_scale): (i128, i8), (d_m, d_scale): (i128, i8), tolerance: f64) -> bool {
    let scale = s_scale.max(d_scale);
    let rescale = |m: i128, from: i8| {
        let up = u32::try_from(i32::from(scale) - i32::from(from)).ok()?;
        m.checked_mul(10i128.checked_pow(up)?)
    };
    match rescale(s_m, s_scale)
        .zip(rescale(d_m, d_scale))
        .and_then(|(s, d)| s.checked_sub(d))
    {
        Some(0) => true,
        Some(diff) => tolerance > 0.0 && decimal_to_f64(diff, scale).abs() <= tolerance,
        None => floats_within(
            decimal_to_f64(s_m, s_scale),
            decimal_to_f64(d_m, d_scale),
            tolerance,
        ),
    }
}

/// Two doubles: equal when they are the same double (NaN included), or, with a
/// tolerance, when they are within it.
///
/// The tolerance gets a few units in the last place of slack. A destination
/// that truncated a float into `Decimal(P, 2)` holds a value less than `0.01`
/// below the source's, and `tolerance=0.01` has to pass all of them; but the
/// difference of two doubles carries their rounding, which past a few million
/// can put a truncation that is `0.01 - ε` exactly at `0.01 + 1e-9`.
fn floats_within(source: f64, dest: f64, tolerance: f64) -> bool {
    if source == dest || (source.is_nan() && dest.is_nan()) {
        return true;
    }
    tolerance > 0.0
        && (source - dest).abs() <= tolerance + 4.0 * f64::EPSILON * source.abs().max(dest.abs())
}

/// A decimal as the nearest `f64`, correctly rounded: one division when both
/// operands are exact doubles (Clinger's fast path), else Rust's float parser,
/// which is correctly rounded for any number of digits.
fn decimal_to_f64(mantissa: i128, scale: i8) -> f64 {
    const POW10: [f64; 23] = [
        1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
        1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
    ];
    if mantissa.unsigned_abs() <= 1u128 << 53 && (0..=22).contains(&scale) {
        return mantissa as f64 / POW10[scale as usize];
    }
    decimal128_to_string(mantissa, scale)
        .parse()
        .unwrap_or(f64::NAN)
}

/// Match both sides' rows by key and compare every planned column.
fn diff(
    plan: &Plan,
    source: &[RecordBatch],
    dest: &[RecordBatch],
    sample_limit: usize,
) -> Result<ValueReconcileResult> {
    let source_keys = KeyIndex::build(source)?;
    let dest_keys = KeyIndex::build(dest)?;
    let missing = source_keys
        .first
        .keys()
        .filter(|k| !dest_keys.first.contains_key(*k))
        .count();
    let orphans = dest_keys
        .first
        .keys()
        .filter(|k| !source_keys.first.contains_key(*k))
        .count();

    // Disjoint keysets are not drift: they are the two window predicates
    // selecting different rows. Reported as a result, they would read as
    // "no mismatches" over nothing compared.
    if !source_keys.first.is_empty()
        && !dest_keys.first.is_empty()
        && missing == source_keys.first.len()
    {
        let sample = |batches: &[RecordBatch], index: &KeyIndex| -> Vec<String> {
            index
                .order
                .iter()
                .take(3)
                .map(|k| render_key(batches, index.first[k]))
                .collect()
        };
        return Err(EtlError::config(format!(
            "reconcile_values found NO keys in common between source and destination ({} vs {} \
             distinct '{}' value(s)). That is almost certainly the window predicates selecting \
             different rows on each side, not drift. Source sample: {:?}; destination sample: \
             {:?}",
            source_keys.first.len(),
            dest_keys.first.len(),
            plan.key_source.name,
            sample(source, &source_keys),
            sample(dest, &dest_keys),
        )));
    }

    let duplicates: HashSet<KeyValue> = source_keys.dups.union(&dest_keys.dups).copied().collect();
    let pairs: Vec<(Loc, Loc)> = source_keys
        .order
        .iter()
        .filter(|k| !duplicates.contains(*k))
        .filter_map(|k| Some((source_keys.first[k], *dest_keys.first.get(k)?)))
        .collect();

    let mut flag = 1 + plan.columns.len();
    let mut columns = Vec::with_capacity(plan.columns.len());
    for (i, col) in plan.columns.iter().enumerate() {
        let source_views = views(source, 1 + i)?;
        let dest_views = views(dest, 1 + i)?;
        let flag_views = match col.null_default {
            Some(_) => {
                flag += 1;
                Some(views(dest, flag - 1)?)
            }
            None => None,
        };
        let mut drift = ColumnDrift {
            column: col.name.clone(),
            compared: pairs.len() as u64,
            ..Default::default()
        };
        for &(s, d) in &pairs {
            let holds_null_default = flag_views
                .as_ref()
                .is_some_and(|f| f[d.batch].cell(d.row) == Cell::Bool(true));
            let outcome = classify(
                col.rule,
                source_views[s.batch].cell(s.row),
                dest_views[d.batch].cell(d.row),
                holds_null_default,
                col.tolerance,
            );
            match outcome {
                Outcome::Equal => continue,
                Outcome::Mismatch => {}
                Outcome::NullInDestOnly => drift.null_in_dest_only += 1,
                Outcome::NullInSourceOnly => drift.null_in_source_only += 1,
            }
            drift.mismatched += 1;
            if drift.sample.len() < sample_limit {
                drift.sample.push(render_key(source, s));
            }
        }
        columns.push(drift);
    }

    Ok(ValueReconcileResult {
        source_rows: source.iter().map(|b| b.num_rows() as u64).sum(),
        dest_rows: dest.iter().map(|b| b.num_rows() as u64).sum(),
        compared_keys: pairs.len() as u64,
        missing_keys: missing as u64,
        orphan_keys: orphans as u64,
        duplicate_keys: duplicates.len() as u64,
        columns,
        skipped_columns: Vec::new(),
        duration_secs: 0.0,
    })
}

/// A row's key as text, for a sample a human reads.
fn render_key(batches: &[RecordBatch], at: Loc) -> String {
    array_value_to_string(batches[at.batch].column(0), at.row).unwrap_or_else(|e| format!("<{e}>"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn cfg() -> ValueReconcileConfig {
        ValueReconcileConfig {
            source_table: Some("t".into()),
            dest_table: "t".into(),
            key: "id".into(),
            window: "id > 0".into(),
            ..Default::default()
        }
    }

    fn col(name: &str, arrow: DataType) -> ColumnType {
        ColumnType {
            name: name.into(),
            type_id: 0,
            nullable: true,
            arrow,
            clickhouse_inner: String::new(),
            arbitrary_precision_decimal: false,
            declared_decimal: None,
        }
    }

    fn dest(name: &str, declared: &str) -> DestColumn {
        dest_column(name, declared, "", None).unwrap()
    }

    fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        RecordBatch::try_from_iter(columns).unwrap()
    }

    #[test]
    fn requires_a_destination_a_key_and_a_window() {
        let mut c = cfg();
        c.dest_table = String::new();
        assert!(c.validate().unwrap_err().to_string().contains("dest_table"));
        let mut c = cfg();
        c.key = String::new();
        assert!(c.validate().unwrap_err().to_string().contains("key"));
        // Both sides' rows are held in memory: an unbounded read is refused.
        let mut c = cfg();
        c.window = " ".into();
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains("requires a window"), "{err}");
    }

    #[test]
    fn requires_exactly_one_source_shape() {
        let mut c = cfg();
        c.source_query = Some("SELECT id FROM t".into());
        assert!(c.validate().unwrap_err().to_string().contains("not both"));
        let mut c = cfg();
        c.source_table = None;
        assert!(c
            .validate()
            .unwrap_err()
            .to_string()
            .contains("source_table or source_query"));
    }

    #[test]
    fn a_tolerance_is_a_finite_number_no_less_than_zero() {
        let mut c = cfg();
        c.tolerance = -0.01;
        assert!(c.validate().unwrap_err().to_string().contains("tolerance"));
        let mut c = cfg();
        c.column_tolerance.insert("amount".into(), f64::NAN);
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains("tolerance['amount']"), "{err}");
        let mut c = cfg();
        c.tolerance = 0.01;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn dest_window_falls_back_to_window() {
        let mut c = cfg();
        assert_eq!(c.effective_dest_window(), "id > 0");
        c.dest_window = Some("id > 1".into());
        assert_eq!(c.effective_dest_window(), "id > 1");
    }

    #[test]
    fn the_source_probe_reads_only_the_named_columns() {
        // A type the reader can't decode elsewhere in the table can't fail it.
        let mut c = cfg();
        c.columns = vec!["amount".into(), "id".into(), "amount".into()];
        assert_eq!(
            source_probe(&c, quote_pg, quote_pg_table),
            "SELECT \"id\", \"amount\" FROM \"t\""
        );
        c.columns.clear();
        assert_eq!(
            source_probe(&c, quote_pg, quote_pg_table),
            "SELECT * FROM \"t\""
        );
        c.source_table = None;
        c.source_query = Some("SELECT id, amount FROM t".into());
        assert_eq!(
            source_probe(&c, quote_my, quote_my_table),
            "SELECT id, amount FROM t"
        );
        c.columns = vec!["amount".into()];
        assert_eq!(
            source_probe(&c, quote_my, quote_my_table),
            "SELECT `id`, `amount` FROM (SELECT id, amount FROM t) AS _src"
        );
    }

    #[test]
    fn a_float_truncated_into_a_decimal_is_reported_unless_the_tolerance_covers_it() {
        // ClickHouse casts a Float64 into a Decimal by truncating toward zero,
        // so 0.29 lands in Decimal(15, 2) as 0.28. Measured on 24.8 (research
        // note §8); these are its pairs, the stored side as a mantissa.
        let landed = [
            (0.29, 28),
            (0.57, 56),
            (1.005, 100),
            (2.675, 267),
            (-0.29, -28),
        ];
        for (sent, stored) in landed {
            let (s, d) = (Cell::Float(sent), Cell::Dec(stored, 2));
            assert_eq!(
                classify(Rule::Float, s, d, false, 0.0),
                Outcome::Mismatch,
                "{sent}"
            );
            assert_eq!(
                classify(Rule::Float, s, d, false, 0.01),
                Outcome::Equal,
                "{sent}"
            );
        }
        // The same truncation at every magnitude a double still holds cents
        // for: a tolerance of one step passes all of them, two steps off fails.
        for i in 0..20_000i64 {
            let sent = (i * 7919) as f64 / 100.0 + (i % 13) as f64 * 1_234_567.0;
            let stored = (sent * 100.0).trunc() as i128;
            let ok = classify(
                Rule::Float,
                Cell::Float(sent),
                Cell::Dec(stored, 2),
                false,
                0.01,
            );
            assert_eq!(ok, Outcome::Equal, "{sent} -> {stored}");
            let off = classify(
                Rule::Float,
                Cell::Float(sent),
                Cell::Dec(stored - 2, 2),
                false,
                0.01,
            );
            assert_eq!(off, Outcome::Mismatch, "{sent} -> {}", stored - 2);
        }
        // A value that landed exactly is equal with no tolerance at all.
        let exact = classify(Rule::Float, Cell::Float(0.29), Cell::Dec(29, 2), false, 0.0);
        assert_eq!(exact, Outcome::Equal);
        assert_eq!(
            classify(
                Rule::Float,
                Cell::Float(f64::NAN),
                Cell::Float(f64::NAN),
                false,
                0.0
            ),
            Outcome::Equal
        );
    }

    #[test]
    fn integers_and_decimals_compare_exactly_across_widths_and_scales() {
        let exact = |s, d, tol| classify(Rule::Exact, s, d, false, tol);
        assert_eq!(exact(Cell::Int(5), Cell::Dec(500, 2), 0.0), Outcome::Equal);
        assert_eq!(
            exact(Cell::Dec(12345, 4), Cell::Dec(123, 2), 0.0),
            Outcome::Mismatch
        );
        assert_eq!(
            exact(Cell::Dec(12345, 4), Cell::Dec(123, 2), 0.005),
            Outcome::Equal
        );
        // A tolerance of exactly the difference passes.
        assert_eq!(
            exact(Cell::Dec(29, 2), Cell::Dec(28, 2), 0.01),
            Outcome::Equal
        );
        // u64 past i64, unchanged.
        let big = i128::from(u64::MAX);
        assert_eq!(exact(Cell::Int(big), Cell::Int(big), 0.0), Outcome::Equal);
        // Past i128 at the common scale: compared as doubles rather than lost.
        let wide = Cell::Dec(10i128.pow(37), 0);
        assert_eq!(
            exact(wide, Cell::Dec(10i128.pow(37), 0), 0.0),
            Outcome::Equal
        );
        assert_eq!(
            exact(wide, Cell::Dec(1, 38), 0.0),
            Outcome::Mismatch,
            "a rescale that overflows must not read as equal"
        );
    }

    #[test]
    fn a_null_stored_as_a_non_nullable_columns_default_is_a_null() {
        // A non-Nullable ClickHouse column stores a NULL as its default (`''`,
        // `0`): what a sync writes for a source NULL, so it matches...
        let flagged = |s, d| classify(Rule::Bytes, s, d, true, 0.0);
        assert_eq!(flagged(Cell::Null, Cell::Bytes(b"")), Outcome::Equal);
        assert_eq!(
            classify(Rule::Exact, Cell::Null, Cell::Int(0), true, 0.0),
            Outcome::Equal
        );
        // ...and the same default over a source value is #28's damage.
        assert_eq!(
            flagged(Cell::Bytes(b"out_invoice"), Cell::Bytes(b"")),
            Outcome::NullInDestOnly
        );
        // A source value that is itself the default matches it.
        assert_eq!(flagged(Cell::Bytes(b""), Cell::Bytes(b"")), Outcome::Equal);
        // Anything else against a source NULL is drift the other way.
        let plain = |s, d| classify(Rule::Bytes, s, d, false, 0.0);
        assert_eq!(
            plain(Cell::Null, Cell::Bytes(b"x")),
            Outcome::NullInSourceOnly
        );
        // In a Nullable column a NULL is NULL, and '' is a value.
        assert_eq!(
            plain(Cell::Null, Cell::Bytes(b"")),
            Outcome::NullInSourceOnly
        );
        assert_eq!(
            plain(Cell::Bytes(b"x"), Cell::Null),
            Outcome::NullInDestOnly
        );
        assert_eq!(plain(Cell::Null, Cell::Null), Outcome::Equal);
    }

    #[test]
    fn the_null_default_is_the_columns_default_expression_else_its_types() {
        // Through FORMAT ArrowStream a NULL lands as the DEFAULT expression
        // when the column has one ('n/a'), else as the type's default.
        let plain = dest("s", "LowCardinality(String)");
        assert_eq!(
            null_default_flag(&plain).as_deref(),
            Some("`s` = defaultValueOfArgumentType(`s`)")
        );
        let defaulted = dest_column("s", "String", "DEFAULT", Some("'n/a'")).unwrap();
        assert_eq!(
            null_default_flag(&defaulted).as_deref(),
            Some("`s` = CAST(('n/a') AS String)")
        );
        assert_eq!(null_default_flag(&dest("s", "Nullable(String)")), None);
        // A column the destination computes itself is never compared.
        assert!(dest_column("m", "String", "MATERIALIZED", Some("upper(s)")).is_none());
        assert!(dest_column("a", "String", "ALIAS", Some("s")).is_none());
    }

    #[test]
    fn low_cardinality_and_nullable_wrappers_read_as_their_inner_type() {
        let lc = dest("c", "LowCardinality(String)");
        assert_eq!(lc.read.as_ref().unwrap().arrow, DataType::Utf8);
        assert!(!lc.nullable);
        let lcn = dest("c", "LowCardinality(Nullable(String))");
        assert_eq!(lcn.read.as_ref().unwrap().arrow, DataType::Utf8);
        assert!(lcn.nullable);
        assert_eq!(null_default_flag(&lcn), None);
        let dec = dest("c", "Nullable(Decimal(15, 2))");
        assert_eq!(
            dec.read.as_ref().unwrap().arrow,
            DataType::Decimal128(15, 2)
        );
        assert!(dec.nullable);
        // Wrapped or not, the value compares as what it holds.
        let rule = |d: &DestColumn| {
            rule_for(
                Family::Bytes,
                family(&d.read.as_ref().unwrap().arrow).unwrap(),
                1,
            )
        };
        assert_eq!(rule(&lc), Some(Rule::Bytes));
        assert_eq!(rule(&lcn), Some(Rule::Bytes));
        // An unreadable type stays visible, as unreadable.
        assert!(dest("c", "Array(String)").read.is_none());
    }

    #[test]
    fn datetimes_compare_as_instants_at_the_destination_precision() {
        assert_eq!(datetime_unit("DateTime"), 1_000_000);
        assert_eq!(datetime_unit("DateTime('UTC')"), 1_000_000);
        assert_eq!(datetime_unit("Nullable(DateTime64(3, 'UTC'))"), 1_000);
        assert_eq!(datetime_unit("LowCardinality(Nullable(DateTime64(9)))"), 1);
        assert_eq!(datetime_unit("DateTime64(6, 'Asia/Jakarta')"), 1);

        let t = 1_788_000_000_000_000i64; // 2026-08-29 10:40:00 UTC
        let seconds = Rule::Instant {
            unit: datetime_unit("DateTime"),
        };
        let instant = |rule, s, d| classify(rule, Cell::Micros(s), Cell::Micros(d), false, 0.0);
        // A DateTime holds whole seconds: the source's fraction isn't drift...
        assert_eq!(instant(seconds, t + 123_456, t), Outcome::Equal);
        // ...a different second is.
        assert_eq!(instant(seconds, t + 1_000_000, t), Outcome::Mismatch);
        let millis = Rule::Instant {
            unit: datetime_unit("DateTime64(3)"),
        };
        assert_eq!(instant(millis, t + 123_456, t + 123_000), Outcome::Equal);
        assert_eq!(instant(millis, t + 124_000, t + 123_000), Outcome::Mismatch);
        // Before 1970 ClickHouse truncates toward zero, and so does this:
        // 1960-01-01 00:00:00.999999 is stored as ...00:00:01.000.
        let early = -315_619_199_000_001i64;
        assert_eq!(instant(millis, early, -315_619_199_000_000), Outcome::Equal);
        // A date against a datetime is its UTC midnight.
        let day = 20_694; // 2026-08-29
        let micros = i64::from(day) * MICROS_PER_DAY;
        let exact = Rule::Instant { unit: 1 };
        assert_eq!(
            classify(exact, Cell::Date(day), Cell::Micros(micros), false, 0.0),
            Outcome::Equal
        );
        assert_eq!(
            classify(
                Rule::Date,
                Cell::Micros(micros + 1),
                Cell::Date(day),
                false,
                0.0
            ),
            Outcome::Equal
        );
    }

    #[test]
    fn datetimes_are_instants_whatever_timezone_their_type_names() {
        // A naive PostgreSQL `timestamp` and a `DateTime64(6, 'Asia/Jakarta')`
        // holding the same instant are the same value: Arrow carries epoch
        // microseconds, and the timezone only labels how to render them.
        let t = 1_788_000_000_000_000i64;
        let jakarta = 7 * 3_600_000_000i64;
        let source = batch(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            (
                "at",
                Arc::new(TimestampMicrosecondArray::from(vec![t, t])) as ArrayRef,
            ),
        ]);
        let dest_batch = batch(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            (
                "at",
                Arc::new(
                    // Row 2 is a wall clock stored as if it were UTC.
                    TimestampMicrosecondArray::from(vec![t, t + jakarta])
                        .with_timezone("Asia/Jakarta"),
                ) as ArrayRef,
            ),
        ]);
        let ts = |tz: Option<&str>| DataType::Timestamp(TimeUnit::Microsecond, tz.map(Into::into));
        let plan = Plan {
            key_source: col("id", DataType::Int64),
            key_dest: col("id", DataType::Int64),
            columns: vec![ColumnPlan {
                name: "at".into(),
                rule: Rule::Instant { unit: 1 },
                tolerance: 0.0,
                source: col("at", ts(None)),
                dest: col("at", ts(Some("Asia/Jakarta"))),
                null_default: None,
            }],
            skipped: Vec::new(),
        };
        let r = diff(&plan, &[source], &[dest_batch], 10).unwrap();
        assert_eq!(r.columns[0].mismatched, 1);
        assert_eq!(r.columns[0].sample, vec!["2"]);
    }

    #[test]
    fn a_mysql_tinyint1_matches_a_bool_by_truth_and_a_uint8_by_value() {
        // Read as the integer it is (`tinyint1_as_bool=false` on the read).
        let to_bool = rule_for(Family::Int, Family::Bool, 1).unwrap();
        assert_eq!(to_bool, Rule::Truth);
        let truth = |s, d| classify(to_bool, s, d, false, 0.0);
        // A sync's tinyint1_as_bool lands 2 as true: the same truth.
        assert_eq!(truth(Cell::Int(2), Cell::Bool(true)), Outcome::Equal);
        assert_eq!(truth(Cell::Int(0), Cell::Bool(false)), Outcome::Equal);
        assert_eq!(truth(Cell::Int(0), Cell::Bool(true)), Outcome::Mismatch);
        // Into a UInt8 column, by value: 2 stays 2.
        let to_int = rule_for(Family::Int, Family::Int, 1).unwrap();
        assert_eq!(to_int, Rule::Exact);
        let value = |s, d| classify(to_int, s, d, false, 0.0);
        assert_eq!(value(Cell::Int(2), Cell::Int(2)), Outcome::Equal);
        assert_eq!(value(Cell::Int(2), Cell::Int(1)), Outcome::Mismatch);
        // A boolean into a UInt8 column lands as 1 or 0.
        let from_bool = rule_for(Family::Bool, Family::Int, 1).unwrap();
        let landed = |s, d| classify(from_bool, s, d, false, 0.0);
        assert_eq!(landed(Cell::Bool(true), Cell::Int(1)), Outcome::Equal);
        assert_eq!(landed(Cell::Bool(true), Cell::Int(2)), Outcome::Mismatch);
        // And through the decoders' own arrays.
        let source = batch(vec![
            ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
            ("flag", Arc::new(Int8Array::from(vec![2, 0, 1])) as ArrayRef),
        ]);
        let dest_batch = batch(vec![
            ("id", Arc::new(UInt64Array::from(vec![1, 2, 3])) as ArrayRef),
            (
                "flag",
                Arc::new(BooleanArray::from(vec![true, true, true])) as ArrayRef,
            ),
        ]);
        let plan = Plan {
            key_source: col("id", DataType::Int32),
            key_dest: col("id", DataType::UInt64),
            columns: vec![ColumnPlan {
                name: "flag".into(),
                rule: to_bool,
                tolerance: 0.0,
                source: col("flag", DataType::Int8),
                dest: col("flag", DataType::Boolean),
                null_default: None,
            }],
            skipped: Vec::new(),
        };
        let r = diff(&plan, &[source], &[dest_batch], 10).unwrap();
        assert_eq!(r.compared_keys, 3, "an Int32 key matches its UInt64 twin");
        assert_eq!(r.columns[0].mismatched, 1);
        assert_eq!(r.columns[0].sample, vec!["2"]);
    }

    /// Source and destination batches for `id, amount, state`, with the
    /// destination's NULL-default flag for `state`.
    fn drift_fixture() -> (Plan, RecordBatch, Vec<RecordBatch>) {
        let source = batch(vec![
            (
                "id",
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef,
            ),
            (
                "amount",
                Arc::new(Float64Array::from(vec![
                    Some(0.29),
                    Some(1.5),
                    Some(2.0),
                    None,
                    Some(3.0),
                    Some(9.0),
                ])) as ArrayRef,
            ),
            (
                "state",
                Arc::new(StringArray::from(vec![
                    Some("done"),
                    Some("done"),
                    None,
                    Some("draft"),
                    Some("done"),
                    Some("done"),
                ])) as ArrayRef,
            ),
        ]);
        // Two batches, as the reader returns them. Key 3 twice (a row a
        // ReplacingMergeTree still holds twice after FINAL can't be told
        // apart), key 5 missing, key 7 an orphan.
        let dest_a = batch(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            (
                "amount",
                Arc::new(
                    Decimal128Array::from(vec![Some(28), Some(150), Some(200)])
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ) as ArrayRef,
            ),
            (
                "state",
                Arc::new(StringArray::from(vec!["done", "", "x"])) as ArrayRef,
            ),
            (
                "flag",
                Arc::new(BooleanArray::from(vec![false, true, false])) as ArrayRef,
            ),
        ]);
        let dest_b = batch(vec![
            (
                "id",
                Arc::new(Int64Array::from(vec![3, 4, 6, 7])) as ArrayRef,
            ),
            (
                "amount",
                Arc::new(
                    Decimal128Array::from(vec![Some(200), None, Some(900), Some(1)])
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ) as ArrayRef,
            ),
            (
                "state",
                Arc::new(StringArray::from(vec!["x", "", "done", "done"])) as ArrayRef,
            ),
            (
                "flag",
                Arc::new(BooleanArray::from(vec![false, true, false, false])) as ArrayRef,
            ),
        ]);
        let plan = Plan {
            key_source: col("id", DataType::Int64),
            key_dest: col("id", DataType::Int64),
            columns: vec![
                ColumnPlan {
                    name: "amount".into(),
                    rule: Rule::Float,
                    tolerance: 0.0,
                    source: col("amount", DataType::Float64),
                    dest: col("amount", DataType::Decimal128(15, 2)),
                    null_default: None,
                },
                ColumnPlan {
                    name: "state".into(),
                    rule: Rule::Bytes,
                    tolerance: 0.0,
                    source: col("state", DataType::Utf8),
                    dest: col("state", DataType::Utf8),
                    null_default: Some("`state` = defaultValueOfArgumentType(`state`)".into()),
                },
            ],
            skipped: Vec::new(),
        };
        (plan, source, vec![dest_a, dest_b])
    }

    #[test]
    fn diff_counts_drift_per_column_and_keys_on_one_side_only() {
        let (plan, source, dest_batches) = drift_fixture();
        let r = diff(&plan, &[source], &dest_batches, 10).unwrap();
        assert_eq!(r.source_rows, 6);
        assert_eq!(r.dest_rows, 7);
        assert_eq!(r.missing_keys, 1, "key 5");
        assert_eq!(r.orphan_keys, 1, "key 7");
        assert_eq!(r.duplicate_keys, 1, "key 3");
        // Keys 1, 2, 4 and 6: in both, once.
        assert_eq!(r.compared_keys, 4);

        let amount = &r.columns[0];
        assert_eq!(amount.compared, 4);
        // 0.29 landed as 0.28; key 4 is NULL on both sides.
        assert_eq!(amount.mismatched, 1);
        assert_eq!(amount.sample, vec!["1"]);
        assert_eq!(amount.null_in_dest_only, 0);

        let state = &r.columns[1];
        // Key 2: '' (the default a NULL is stored as) over 'done'. Key 4:
        // 'draft' over the same. Both are #28's signature.
        assert_eq!(state.mismatched, 2);
        assert_eq!(state.null_in_dest_only, 2);
        assert_eq!(state.null_in_source_only, 0);
        assert_eq!(state.sample, vec!["2", "4"]);

        // With a tolerance that covers the truncation, amount is clean.
        let (mut plan, source, dest_batches) = drift_fixture();
        plan.columns[0].tolerance = 0.01;
        let r = diff(&plan, &[source], &dest_batches, 1).unwrap();
        assert_eq!(r.columns[0].mismatched, 0);
        // sample_limit bounds the samples, never the counts.
        assert_eq!(r.columns[1].mismatched, 2);
        assert_eq!(r.columns[1].sample.len(), 1);
    }

    #[test]
    fn several_rows_per_key_are_read_through_final_where_the_engine_merges_them() {
        assert!(merges_rows("ReplacingMergeTree"));
        assert!(merges_rows("ReplicatedReplacingMergeTree"));
        assert!(merges_rows("SharedReplacingMergeTree"));
        assert!(merges_rows("VersionedCollapsingMergeTree"));
        // FINAL is an error on these (Code: 181 ILLEGAL_FINAL on 24.8).
        assert!(!merges_rows("MergeTree"));
        assert!(!merges_rows("Distributed"));
        assert!(!merges_rows(""));

        let table = DestTable {
            qualified: qualified("db", "t"),
            final_: true,
            columns: Vec::new(),
        };
        let flags = vec![(
            flag_alias(0),
            "`s` = defaultValueOfArgumentType(`s`)".into(),
        )];
        assert_eq!(
            dest_base_query(&table, "id > 10", &flags),
            "SELECT *, (`s` = defaultValueOfArgumentType(`s`)) AS \
             `__quickhouse_null_default_0` FROM `db`.`t` FINAL WHERE (id > 10)"
        );
        let plain = DestTable {
            final_: false,
            ..table
        };
        assert_eq!(
            dest_base_query(&plain, "id > 10", &[]),
            "SELECT * FROM `db`.`t` WHERE (id > 10)"
        );
        // A key still doubled after FINAL is counted and left out:
        // `diff_counts_drift_per_column_and_keys_on_one_side_only` key 3.
    }

    #[test]
    fn keys_match_by_value_across_integer_widths_and_decimal_scales() {
        assert_eq!(key_value(Cell::Dec(500, 2)), Some(KeyValue::Int(5)));
        assert_eq!(key_value(Cell::Dec(510, 2)), Some(KeyValue::Decimal(51, 1)));
        assert_eq!(key_value(Cell::Int(5)), Some(KeyValue::Int(5)));
        assert_eq!(key_value(Cell::Null), None);
        assert_eq!(
            key_value(Cell::Bytes(b"XYZ       ")),
            Some(KeyValue::Bytes(b"XYZ       ")),
            "char(n) padding is part of the key on both sides"
        );
    }

    #[test]
    fn a_float_key_or_one_whose_types_disagree_is_refused() {
        let source = [col("id", DataType::Float64), col("a", DataType::Int64)];
        let dest_cols = [dest("id", "Float64"), dest("a", "Int64")];
        let err = plan_columns(&cfg(), &source, &dest_cols)
            .unwrap_err()
            .to_string();
        assert!(err.contains("float"), "{err}");

        let source = [col("id", DataType::Int64)];
        let dest_cols = [dest("id", "String")];
        let err = plan_columns(&cfg(), &source, &dest_cols)
            .unwrap_err()
            .to_string();
        assert!(err.contains("a number in the source and text"), "{err}");

        let dest_cols = [dest("other", "Int64")];
        let err = plan_columns(&cfg(), &source, &dest_cols)
            .unwrap_err()
            .to_string();
        assert!(err.contains("not in destination table"), "{err}");
    }

    #[test]
    fn no_keys_in_common_is_refused_rather_than_reported_as_clean() {
        let (plan, source, _) = drift_fixture();
        let elsewhere = batch(vec![
            ("id", Arc::new(Int64Array::from(vec![100, 101])) as ArrayRef),
            (
                "amount",
                Arc::new(
                    Decimal128Array::from(vec![Some(1), Some(1)])
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ) as ArrayRef,
            ),
            (
                "state",
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            ),
            (
                "flag",
                Arc::new(BooleanArray::from(vec![false, false])) as ArrayRef,
            ),
        ]);
        let err = diff(&plan, std::slice::from_ref(&source), &[elsewhere], 10)
            .unwrap_err()
            .to_string();
        assert!(err.contains("NO keys in common"), "{err}");
        // An empty side is just that: everything missing, nothing compared.
        let r = diff(&plan, &[source], &[], 10).unwrap();
        assert_eq!(r.missing_keys, 6);
        assert_eq!(r.compared_keys, 0);
    }

    #[test]
    fn columns_default_to_those_on_both_sides() {
        let source = [
            col("id", DataType::Int64),
            col("amount", DataType::Float64),
            col("only_in_source", DataType::Utf8),
            col("state", DataType::Utf8),
        ];
        let dest_cols = [
            dest("id", "Int64"),
            dest("state", "LowCardinality(String)"),
            dest("amount", "Nullable(Decimal(15, 2))"),
            dest("only_in_dest", "String"),
        ];
        let plan = plan_columns(&cfg(), &source, &dest_cols).unwrap();
        let names: Vec<&str> = plan.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["amount", "state"], "source order, key left out");
        assert_eq!(plan.columns[0].rule, Rule::Float);
        assert_eq!(plan.columns[0].null_default, None);
        assert!(plan.columns[1].null_default.is_some());
        assert!(plan.skipped.is_empty());
    }

    #[test]
    fn a_column_that_cant_be_compared_is_skipped_by_default_and_refused_by_name() {
        let source = [
            col("id", DataType::Int64),
            col("code", DataType::Int64),
            col("tags", DataType::Utf8),
            col("amount", DataType::Float64),
        ];
        let dest_cols = [
            dest("id", "Int64"),
            dest("code", "String"),
            dest("tags", "Array(String)"),
            dest("amount", "Float64"),
        ];
        let plan = plan_columns(&cfg(), &source, &dest_cols).unwrap();
        assert_eq!(plan.columns.len(), 1);
        let skipped: Vec<&str> = plan.skipped.iter().map(|(c, _)| c.as_str()).collect();
        assert_eq!(skipped, ["code", "tags"]);
        assert!(
            plan.skipped[0].1.contains("no common type"),
            "{:?}",
            plan.skipped
        );
        assert!(
            plan.skipped[1].1.contains("can't be read"),
            "{:?}",
            plan.skipped
        );

        let mut named = cfg();
        named.columns = vec!["code".into()];
        let err = plan_columns(&named, &source, &dest_cols)
            .unwrap_err()
            .to_string();
        assert!(err.contains("column 'code' can't be compared"), "{err}");
        named.columns = vec!["missing".into()];
        let err = plan_columns(&named, &[col("id", DataType::Int64)], &dest_cols)
            .unwrap_err()
            .to_string();
        assert!(err.contains("'missing' is not in the source"), "{err}");
    }

    #[test]
    fn a_tolerance_names_a_compared_numeric_column() {
        let source = [
            col("id", DataType::Int64),
            col("amount", DataType::Float64),
            col("state", DataType::Utf8),
        ];
        let dest_cols = [
            dest("id", "Int64"),
            dest("amount", "Decimal(15, 2)"),
            dest("state", "String"),
        ];
        let mut c = cfg();
        c.tolerance = 0.5;
        c.column_tolerance.insert("amount".into(), 0.01);
        let plan = plan_columns(&c, &source, &dest_cols).unwrap();
        assert_eq!(plan.columns[0].tolerance, 0.01);
        // The blanket tolerance reaches every numeric column, and text
        // compares exactly whatever it says.
        assert_eq!(plan.columns[1].tolerance, 0.5);
        assert_eq!(plan.columns[1].rule, Rule::Bytes);

        let mut c = cfg();
        c.column_tolerance.insert("state".into(), 0.01);
        let err = plan_columns(&c, &source, &dest_cols)
            .unwrap_err()
            .to_string();
        assert!(err.contains("doesn't compare as a number"), "{err}");
        let mut c = cfg();
        c.column_tolerance.insert("nope".into(), 0.01);
        let err = plan_columns(&c, &source, &dest_cols)
            .unwrap_err()
            .to_string();
        assert!(err.contains("isn't compared"), "{err}");
    }

    #[test]
    fn decimal_to_f64_is_correctly_rounded_at_any_width() {
        assert_eq!(decimal_to_f64(29, 2), 0.29);
        assert_eq!(decimal_to_f64(-28, 2), -0.28);
        // Past the fast path: parsed, not divided twice.
        assert_eq!(
            decimal_to_f64(1_234_567_890_123_456_789, 2),
            12_345_678_901_234_567.89
        );
        assert_eq!(decimal_to_f64(5, 30), 5e-30);
    }
}
