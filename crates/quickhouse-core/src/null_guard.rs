//! A NULL in a column the destination can't hold NULL in, whatever made it
//! NULL: a nullable source column, an API record without the field, or a value
//! a decoder couldn't read (a `coerced_*` warning).
//!
//! Two kinds of column, and two outcomes:
//!
//! - **Left out (#49).** A `key` column, on every destination, and the
//!   watermark when the destination's column can't be NULL (a ClickHouse
//!   `ReplacingMergeTree`'s version column). The row is left out and counted,
//!   as `null_key` or `null_watermark`: a key that isn't there can't be
//!   stored meaningfully (rows collapse into one, or a `MERGE` never matches),
//!   nor can a version that would lose to the row already stored.
//! - **Defaulted (#53).** Any other column the plan makes NOT NULL: an
//!   `order_by` or `primary_key` column, one `not_null` names, or one a
//!   ClickHouse engine's parameters name (a collapsing sign, a version that
//!   isn't the watermark). The row is written with the column type's default
//!   there ([`Defaulted`]) and counted as `null_defaulted`.
//!
//! Before, a database source (or a DataFrame) failed the write partway with
//! Arrow's "declared as non-nullable but contains null values", and an API
//! source's NULL landed as the type's default with nothing said (#53), or for
//! a key or version, collapsed or lost (#49).
//!
//! Every source's batches pass through [`NullGuard::filter`] on their way to
//! the destination (`SendCtx::push_batch`) and to the archive, so the rule is
//! the same on every source, and the archive holds what the destination was
//! written. The decoders build batches with the checked columns nullable
//! ([`NullGuard::decode_columns`]), so a NULL reaches the filter instead of
//! failing the batch, and the filter hands on batches whose schema says those
//! columns hold none ([`NullGuard::strict_schema`]), as the destination's
//! does.
//!
//! A database read already leaves out a row whose watermark is NULL (its
//! filter never matches one, and `null_watermark` reports them from a count),
//! so here the watermark only meets a value the decoder made NULL, or one from
//! a source that has no filter (an API, a DataFrame).
//!
//! The default is filled in here, in Arrow, rather than left to ClickHouse's
//! `input_format_null_as_default`: for `FORMAT ArrowStream` that fills the
//! *Arrow* type's default and then converts it, so a NULL text value sent to
//! an `Enum`, `UUID` or IP column fails the insert (`''` reads as none of
//! them; measured on 24.8), and BigQuery fills nothing. Filling here gives
//! both destinations the same values, the archive the values written, and
//! the count exactly the values filled.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::compute::kernels::zip::zip;
use arrow::compute::{filter_record_batch, is_not_null};
use arrow::datatypes::i256;
use arrow_array::{
    make_array, Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array,
    Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, RecordBatch, Scalar,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::config::{DestKind, TransferConfig, WarningKind};
use crate::error::{EtlError, Result};
use crate::transform::SelectPlan;
use crate::types::ColumnType;

/// One column whose NULL rows are left out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Guarded {
    /// Position in the plan, and so in every batch decoded under it.
    pub(crate) idx: usize,
    /// The batch's (destination) name for it, to check the position against.
    dest: String,
    /// The column as the caller named it, which the warning names:
    /// `watermark=` takes the source name, `key=` the destination name.
    pub(crate) column: String,
    /// [`WarningKind::NullWatermark`] or [`WarningKind::NullKey`].
    pub(crate) kind: WarningKind,
}

/// The option that makes a [`Defaulted`] column NOT NULL in the plan (see
/// `transform::plan_for_destination`), which the warning names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NotNullBy {
    OrderBy,
    PrimaryKey,
    NotNull,
    /// A ClickHouse engine parameter: `CollapsingMergeTree(sign)`, or a
    /// `ReplacingMergeTree(ver)` version that isn't the watermark.
    Engine,
}

impl NotNullBy {
    /// Why the destination column can't be NULL, for the warning.
    fn why(self) -> &'static str {
        match self {
            NotNullBy::OrderBy => "order_by names it, and a sorting key column can't be NULL",
            NotNullBy::PrimaryKey => "primary_key names it, and a primary key column can't be NULL",
            NotNullBy::NotNull => "not_null names it",
            NotNullBy::Engine => {
                "the table engine names it, and an engine's sign or version column can't be NULL"
            }
        }
    }
}

/// One column whose NULLs are written as the column type's default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Defaulted {
    /// Position in the plan, and so in every batch decoded under it.
    pub(crate) idx: usize,
    /// The destination column, which the batch and the warning name.
    pub(crate) column: String,
    /// What makes it NOT NULL.
    by: NotNullBy,
    /// What a NULL in a text column becomes when the destination type doesn't
    /// read `''` as its default (an `Enum`, a `UUID`, an IP address, a number
    /// on ClickHouse): see [`clickhouse_text_default`]. `None`: the Arrow
    /// type's zero value (see [`zero_scalar`]).
    text: Option<String>,
    /// How the stored default reads, for the warning: `''`, `0`, `'a'`.
    shown: String,
}

/// The columns a run checks for NULLs, and the filter that leaves out or
/// fills in each.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NullGuard {
    /// Left out, in check order: the watermark first, then the key columns in
    /// `key=` order. A row left out counts once, against the first of them
    /// that is NULL in it, so a row with a NULL watermark counts as
    /// `null_watermark` whatever its key holds, as it does when a database
    /// read leaves it out.
    columns: Vec<Guarded>,
    /// Defaulted, in plan order. Checked on the rows `columns` keeps, so a
    /// row left out isn't counted again here.
    defaulted: Vec<Defaulted>,
}

/// What [`NullGuard::filter`] returns.
#[derive(Debug)]
pub(crate) struct Filtered {
    /// The rows to write, under [`NullGuard::strict_schema`].
    pub(crate) batch: RecordBatch,
    /// Rows left out, per left-out column, in check order.
    pub(crate) left_out: Vec<u64>,
    /// Rows written with a default, per defaulted column, in plan order.
    pub(crate) defaulted: Vec<u64>,
}

impl NullGuard {
    /// The guard for a run writing `plan` under `cfg` into a `dest`
    /// destination, or `None` when there is nothing to check.
    ///
    /// A key column is checked on every destination. The plan makes it NOT
    /// NULL (a ClickHouse sorting key can't be NULL, and BigQuery gets the
    /// same table), and where a BigQuery table already has it NULLABLE, a NULL
    /// key never matches in the `MERGE`, so that row would be inserted again
    /// on every run.
    ///
    /// A column is defaulted when the plan makes it NOT NULL for one of the
    /// [`NotNullBy`] options, and isn't left out already. That is a REQUIRED
    /// column on BigQuery, which quickhouse creates from the same plan. A
    /// column whose source declares it NOT NULL can't receive a NULL (the
    /// plan keeps a type a decoder can make NULL nullable), and isn't checked.
    pub(crate) fn new(plan: &SelectPlan, cfg: &TransferConfig, dest: DestKind) -> Option<Self> {
        // Every CSV field can be empty: a NULL is written as one, wherever it
        // is, so nothing is left out or defaulted (the plan makes no CSV
        // column NOT NULL either).
        if dest == DestKind::Csv {
            return None;
        }
        let mut columns = Vec::new();
        if let Some(w) = cfg.watermark.as_deref() {
            let at = plan.source_columns.iter().position(|c| c == w);
            if let Some((idx, d)) = at.and_then(|i| Some((i, plan.dest_columns.get(i)?))) {
                if !d.nullable {
                    columns.push(Guarded {
                        idx,
                        dest: d.name.clone(),
                        column: w.to_string(),
                        kind: WarningKind::NullWatermark,
                    });
                }
            }
        }
        for k in &cfg.key {
            // `key` names destination columns; BigQuery compares names
            // case-insensitively, and a ClickHouse key that differs only in
            // case is refused by the plan before this.
            let idx = plan
                .dest_columns
                .iter()
                .position(|d| &d.name == k)
                .or_else(|| {
                    plan.dest_columns
                        .iter()
                        .position(|d| d.name.eq_ignore_ascii_case(k))
                });
            let Some(idx) = idx else { continue };
            if columns.iter().any(|g| g.idx == idx) {
                continue;
            }
            columns.push(Guarded {
                idx,
                dest: plan.dest_columns[idx].name.clone(),
                column: k.clone(),
                kind: WarningKind::NullKey,
            });
        }
        // The plan matches these options on the exact destination name.
        let engine = match dest {
            DestKind::ClickHouse => crate::ddl::engine_parameter_columns(&cfg.effective_engine()),
            DestKind::BigQuery | DestKind::Csv => Vec::new(),
        };
        let mut defaulted = Vec::new();
        for (idx, d) in plan.dest_columns.iter().enumerate() {
            if d.nullable || columns.iter().any(|g| g.idx == idx) {
                continue;
            }
            let named = |list: &[String]| list.iter().any(|c| c == &d.name);
            let by = if named(&cfg.order_by) {
                NotNullBy::OrderBy
            } else if named(&cfg.primary_key) {
                NotNullBy::PrimaryKey
            } else if named(&cfg.not_null) {
                NotNullBy::NotNull
            } else if named(&engine) {
                NotNullBy::Engine
            } else {
                continue;
            };
            // The planned type, which is the table's when quickhouse creates
            // it. An existing table that holds another type there gets this
            // type's default, which it may refuse (an `Enum` the plan calls
            // `String` refuses '').
            let text = match (dest, &d.arrow) {
                (DestKind::ClickHouse, DataType::Utf8 | DataType::LargeUtf8) => {
                    clickhouse_text_default(&d.clickhouse_inner)
                }
                _ => None,
            };
            let shown = match &text {
                Some(t) => format!("'{t}'"),
                None => shown_zero(&d.arrow),
            };
            defaulted.push(Defaulted {
                idx,
                column: d.name.clone(),
                by,
                text,
                shown,
            });
        }
        (!columns.is_empty() || !defaulted.is_empty()).then_some(Self { columns, defaulted })
    }

    #[cfg(test)]
    fn columns(&self) -> &[Guarded] {
        &self.columns
    }

    #[cfg(test)]
    fn defaulted(&self) -> &[Defaulted] {
        &self.defaulted
    }

    /// Positions of every checked column, left out or defaulted.
    fn checked(&self) -> impl Iterator<Item = usize> + '_ {
        self.columns
            .iter()
            .map(|g| g.idx)
            .chain(self.defaulted.iter().map(|d| d.idx))
    }

    /// Every non-zero count in `f`, as the warning it adds to: `(kind,
    /// column, rows)`.
    pub(crate) fn tallies<'a>(
        &'a self,
        f: &'a Filtered,
    ) -> impl Iterator<Item = (WarningKind, &'a str, u64)> + 'a {
        let left_out = self
            .columns
            .iter()
            .zip(&f.left_out)
            .map(|(g, n)| (g.kind, g.column.as_str(), *n));
        let defaulted = self
            .defaulted
            .iter()
            .zip(&f.defaulted)
            .map(|(d, n)| (WarningKind::NullDefaulted, d.column.as_str(), *n));
        left_out.chain(defaulted).filter(|(_, _, n)| *n > 0)
    }

    /// `dest_columns` (the plan's) with the checked columns nullable: what a
    /// decoder builds batches with, so a NULL there reaches [`Self::filter`]
    /// rather than failing the batch. The ClickHouse source casts to these
    /// too, as its server refuses to cast a NULL to a non-`Nullable` type.
    pub(crate) fn decode_columns(&self, dest_columns: &[ColumnType]) -> Vec<ColumnType> {
        let mut out = dest_columns.to_vec();
        for idx in self.checked() {
            if let Some(c) = out.get_mut(idx) {
                c.nullable = true;
            }
        }
        out
    }

    /// `schema` with the checked columns non-nullable, as the destination has
    /// them: the schema of what [`Self::filter`] returns.
    pub(crate) fn strict_schema(&self, schema: &SchemaRef) -> SchemaRef {
        if self
            .checked()
            .all(|i| schema.fields().get(i).map_or(true, |f| !f.is_nullable()))
        {
            return schema.clone();
        }
        let fields: Vec<Field> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let checked = self.checked().any(|c| c == i);
                f.as_ref()
                    .clone()
                    .with_nullable(f.is_nullable() && !checked)
            })
            .collect();
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
    }

    /// `batch` without its rows that hold a NULL in a left-out column, and
    /// with the column type's default in place of a NULL in a defaulted one,
    /// under [`Self::strict_schema`]; and how many rows each column left out
    /// or filled in.
    pub(crate) fn filter(&self, batch: RecordBatch) -> Result<Filtered> {
        let mut left_out = vec![0u64; self.columns.len()];
        let mut checked = Vec::with_capacity(self.columns.len());
        for g in &self.columns {
            checked.push(column_at(&batch, g.idx, &g.dest)?.clone());
        }
        let batch = if checked.iter().all(|c| c.null_count() == 0) {
            batch
        } else {
            let keep: BooleanArray = (0..batch.num_rows())
                .map(|row| match checked.iter().position(|c| c.is_null(row)) {
                    Some(j) => {
                        left_out[j] += 1;
                        Some(false)
                    }
                    None => Some(true),
                })
                .collect();
            filter_record_batch(&batch, &keep)?
        };
        let mut defaulted = vec![0u64; self.defaulted.len()];
        let mut arrays: Option<Vec<ArrayRef>> = None;
        for (j, d) in self.defaulted.iter().enumerate() {
            let col = column_at(&batch, d.idx, &d.column)?;
            if col.null_count() == 0 {
                continue;
            }
            defaulted[j] = col.null_count() as u64;
            let filled = fill_nulls(col, d.text.as_deref(), &d.column)?;
            arrays.get_or_insert_with(|| batch.columns().to_vec())[d.idx] = filled;
        }
        let columns = arrays.unwrap_or_else(|| batch.columns().to_vec());
        let schema = self.strict_schema(&batch.schema());
        let batch = RecordBatch::try_new(schema, columns)?;
        Ok(Filtered {
            batch,
            left_out,
            defaulted,
        })
    }

    /// What became of a NULL in each checked column, by destination name,
    /// which is how the decoders name a coerced column.
    pub(crate) fn fates(&self) -> HashMap<String, NullFate> {
        let mut fates = HashMap::new();
        for g in &self.columns {
            fates.insert(g.dest.clone(), NullFate::LeftOut(g.kind));
        }
        for d in &self.defaulted {
            fates.insert(
                d.column.clone(),
                NullFate::Default {
                    by: d.by,
                    shown: d.shown.clone(),
                },
            );
        }
        fates
    }
}

/// `batch`'s column at `idx`, checked to be the one named `name`.
fn column_at<'a>(batch: &'a RecordBatch, idx: usize, name: &str) -> Result<&'a ArrayRef> {
    match batch.schema().fields().get(idx) {
        Some(f) if f.name() == name => Ok(batch.column(idx)),
        _ => Err(EtlError::internal(format!(
            "the batch has no column '{name}' at position {idx} to check for NULLs"
        ))),
    }
}

/// `col` with each NULL replaced by its type's default ([`zero_scalar`], or
/// `text` for a text column), and no validity buffer.
fn fill_nulls(col: &ArrayRef, text: Option<&str>, column: &str) -> Result<ArrayRef> {
    let fill = zero_scalar(col.data_type(), text).ok_or_else(|| {
        EtlError::other(format!(
            "column '{column}' can't be NULL at the destination, and quickhouse has no default \
             to store for a NULL there (Arrow type {:?}). Give those rows a value in the source",
            col.data_type()
        ))
    })?;
    let filled = zip(&is_not_null(col.as_ref())?, col, &Scalar::new(fill))?;
    // `zip` keeps a validity buffer, all valid, when `col` had one.
    let data = filled.into_data();
    if data.nulls().is_none() {
        return Ok(make_array(data));
    }
    Ok(make_array(data.into_builder().nulls(None).build()?))
}

/// A one-row array holding `dt`'s zero value: `''` (or `text`), `0`, `false`,
/// the epoch for a date or timestamp, zero bytes. What ClickHouse stores for a
/// NULL sent to a non-`Nullable` column of the matching type, and a value every
/// BigQuery type reads. `None` for a type no decoder produces.
fn zero_scalar(dt: &DataType, text: Option<&str>) -> Option<ArrayRef> {
    let text = text.unwrap_or("");
    Some(match dt {
        DataType::Utf8 => Arc::new(StringArray::from(vec![text])),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(vec![text])),
        DataType::Binary => Arc::new(BinaryArray::from(vec![&b""[..]])),
        DataType::LargeBinary => Arc::new(LargeBinaryArray::from(vec![&b""[..]])),
        DataType::FixedSizeBinary(n) => Arc::new(
            FixedSizeBinaryArray::try_from_iter(std::iter::once(vec![0u8; *n as usize])).ok()?,
        ),
        DataType::Boolean => Arc::new(BooleanArray::from(vec![false])),
        DataType::Int8 => Arc::new(Int8Array::from(vec![0])),
        DataType::Int16 => Arc::new(Int16Array::from(vec![0])),
        DataType::Int32 => Arc::new(Int32Array::from(vec![0])),
        DataType::Int64 => Arc::new(Int64Array::from(vec![0])),
        DataType::UInt8 => Arc::new(UInt8Array::from(vec![0])),
        DataType::UInt16 => Arc::new(UInt16Array::from(vec![0])),
        DataType::UInt32 => Arc::new(UInt32Array::from(vec![0])),
        DataType::UInt64 => Arc::new(UInt64Array::from(vec![0])),
        DataType::Float32 => Arc::new(Float32Array::from(vec![0.0])),
        DataType::Float64 => Arc::new(Float64Array::from(vec![0.0])),
        DataType::Date32 => Arc::new(Date32Array::from(vec![0])),
        DataType::Date64 => Arc::new(Date64Array::from(vec![0])),
        DataType::Timestamp(unit, tz) => match unit {
            TimeUnit::Second => {
                Arc::new(TimestampSecondArray::from(vec![0]).with_timezone_opt(tz.clone()))
            }
            TimeUnit::Millisecond => {
                Arc::new(TimestampMillisecondArray::from(vec![0]).with_timezone_opt(tz.clone()))
            }
            TimeUnit::Microsecond => {
                Arc::new(TimestampMicrosecondArray::from(vec![0]).with_timezone_opt(tz.clone()))
            }
            TimeUnit::Nanosecond => {
                Arc::new(TimestampNanosecondArray::from(vec![0]).with_timezone_opt(tz.clone()))
            }
        },
        DataType::Decimal128(p, s) => Arc::new(
            Decimal128Array::from(vec![0i128])
                .with_precision_and_scale(*p, *s)
                .ok()?,
        ),
        DataType::Decimal256(p, s) => Arc::new(
            Decimal256Array::from(vec![i256::ZERO])
                .with_precision_and_scale(*p, *s)
                .ok()?,
        ),
        _ => return None,
    })
}

/// How [`zero_scalar`]'s value for `dt` reads, for the warning.
fn shown_zero(dt: &DataType) -> String {
    match dt {
        DataType::Utf8 | DataType::LargeUtf8 => "''",
        DataType::Binary | DataType::LargeBinary => "empty bytes",
        DataType::FixedSizeBinary(_) => "zero bytes",
        DataType::Boolean => "false",
        DataType::Date32 | DataType::Date64 => "1970-01-01",
        DataType::Timestamp(_, Some(_)) => "1970-01-01 00:00:00 UTC",
        DataType::Timestamp(_, None) => "1970-01-01 00:00:00",
        _ => "0",
    }
    .to_string()
}

/// `inner` without a `name(...)` wrapper around all of it, if it has one.
fn strip_wrapper<'a>(inner: &'a str, name: &str) -> Option<&'a str> {
    inner
        .strip_prefix(name)?
        .strip_prefix('(')?
        .strip_suffix(')')
        .map(str::trim)
}

/// The text a ClickHouse column of type `ty` reads as its default
/// (`defaultValueOfTypeName`), for one sent as Arrow text, where it isn't
/// `''`: ClickHouse reads `''` as no `Enum` value, `UUID`, IP address or
/// number. `None` where `''` is the default (`String`, `FixedString`), and for
/// a type this doesn't know (a date sent as text: its default depends on the
/// column's time zone), which keeps `''`.
pub(crate) fn clickhouse_text_default(ty: &str) -> Option<String> {
    let mut ty = ty.trim();
    for wrapper in ["LowCardinality", "Nullable"] {
        ty = strip_wrapper(ty, wrapper).unwrap_or(ty);
    }
    let name = ty.split('(').next().unwrap_or(ty).trim();
    let digits = |rest: &str| !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit());
    match name {
        "UUID" => Some("00000000-0000-0000-0000-000000000000".to_string()),
        "IPv4" => Some("0.0.0.0".to_string()),
        "IPv6" => Some("::".to_string()),
        "Bool" => Some("false".to_string()),
        "Enum" | "Enum8" | "Enum16" => enum_default(ty),
        "Float32" | "Float64" | "BFloat16" => Some("0".to_string()),
        n if n.starts_with("Decimal") => Some("0".to_string()),
        n if n
            .strip_prefix("UInt")
            .or_else(|| n.strip_prefix("Int"))
            .is_some_and(digits) =>
        {
            Some("0".to_string())
        }
        _ => None,
    }
}

/// An `Enum`'s default: its element with the lowest value, as ClickHouse
/// keeps the elements sorted by value. An element without `= n` takes the one
/// after the element before it, starting at 1. `None` if `ty` doesn't parse.
fn enum_default(ty: &str) -> Option<String> {
    let body = ty.split_once('(')?.1.trim_end().strip_suffix(')')?;
    let mut chars = body.chars().peekable();
    let mut lowest: Option<(i64, String)> = None;
    let mut next = 1i64;
    loop {
        while chars.peek().is_some_and(|c| c.is_whitespace() || *c == ',') {
            chars.next();
        }
        if chars.next()? != '\'' {
            return None;
        }
        let mut name = String::new();
        loop {
            match chars.next()? {
                '\\' => name.push(chars.next()?),
                '\'' if chars.peek() == Some(&'\'') => {
                    chars.next();
                    name.push('\'');
                }
                '\'' => break,
                c => name.push(c),
            }
        }
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        let value = if chars.peek() == Some(&'=') {
            chars.next();
            let number: String = std::iter::from_fn(|| {
                chars.next_if(|c| c.is_whitespace() || *c == '-' || c.is_ascii_digit())
            })
            .filter(|c| !c.is_whitespace())
            .collect();
            number.parse().ok()?
        } else {
            next
        };
        next = value + 1;
        if lowest.as_ref().map_or(true, |(v, _)| value < *v) {
            lowest = Some((value, name));
        }
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        match chars.next() {
            Some(',') => continue,
            None => return lowest.map(|(_, name)| name),
            Some(_) => return None,
        }
    }
}

/// What became of a NULL in a column the destination can't hold NULL in,
/// which a `coerced_*` warning's text says for a value the decoder couldn't
/// read there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NullFate {
    /// [`NullGuard`] left the row out, and counted it under this kind.
    LeftOut(WarningKind),
    /// [`NullGuard`] wrote the column type's default, `shown`, in its place,
    /// and counted it as `null_defaulted`.
    Default { by: NotNullBy, shown: String },
}

/// The warning text for `n` rows that [`NullGuard`] left out for a NULL in
/// `column`, counted under `kind`.
pub(crate) fn left_out_message(kind: WarningKind, column: &str, n: u64) -> String {
    let cursor = "An incremental run's cursor still moves past them, so they aren't read again \
                  unless they change.";
    if kind == WarningKind::NullKey {
        format!(
            "key column '{column}' was NULL in {n} row(s) read, which were left out: a key can't \
             be NULL at the destination (ClickHouse sorts and replaces rows by it, and a NULL \
             never matches in a BigQuery MERGE, so the row would be added again on every run). \
             A value quickhouse couldn't read there counts too (see the coerced_* warning on \
             '{column}'). {cursor} Give those rows a key in the source, or name null_key in \
             fail_on_warnings to fail the run on them."
        )
    } else {
        format!(
            "watermark column '{column}' was NULL in {n} row(s) read, which were left out: the \
             destination's '{column}' can't be NULL (a ReplacingMergeTree's version column, or \
             a key), so they are left out as a database read leaves out a row whose watermark \
             is NULL. A value quickhouse couldn't read there counts too (see coerced_date). \
             {cursor} Give those rows a watermark in the source, or name null_watermark in \
             fail_on_warnings to fail the run on them."
        )
    }
}

/// The `null_defaulted` text for `n` rows that [`NullGuard`] wrote with the
/// default in `column`, when `fate` is that column's.
pub(crate) fn defaulted_message(column: &str, n: u64, fate: Option<&NullFate>) -> String {
    let (why, shown) = match fate {
        Some(NullFate::Default { by, shown }) => (by.why(), shown.as_str()),
        _ => ("the plan makes it NOT NULL", "its type's default"),
    };
    format!(
        "column '{column}' was NULL in {n} row(s) read, which were written with {shown}, the \
         column type's default, in its place: the destination's '{column}' can't be NULL, as \
         {why}. A value quickhouse couldn't read there counts too (see the coerced_* warning on \
         '{column}'). Give those rows a value in the source, or name null_defaulted in \
         fail_on_warnings to fail the run on them."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::DataType;

    fn col(name: &str, arrow: DataType, nullable: bool) -> ColumnType {
        typed(name, arrow, nullable, "String")
    }

    fn typed(name: &str, arrow: DataType, nullable: bool, inner: &str) -> ColumnType {
        ColumnType {
            name: name.into(),
            type_id: 0,
            nullable,
            arrow,
            clickhouse_inner: inner.into(),
            arbitrary_precision_decimal: false,
            declared_decimal: None,
        }
    }

    /// `id` and `code` (both NOT NULL at the destination), and a nullable
    /// `note`; `ts` is NOT NULL when `version` says it is the version column.
    fn plan(version: bool) -> SelectPlan {
        let dest = vec![
            col("id", DataType::Int64, false),
            col("code", DataType::Utf8, false),
            col("ts", DataType::Int64, !version),
            col("note", DataType::Utf8, true),
        ];
        SelectPlan {
            source_columns: vec!["id".into(), "src_code".into(), "ts".into(), "note".into()],
            source_select_exprs: vec![None; 4],
            dest_columns: dest,
        }
    }

    fn cfg(key: &[&str], watermark: Option<&str>) -> TransferConfig {
        let mut c = crate::config::default_test_config();
        c.key = key.iter().map(|k| k.to_string()).collect();
        c.watermark = watermark.map(str::to_string);
        c
    }

    fn guard(plan: &SelectPlan, cfg: &TransferConfig) -> Option<NullGuard> {
        NullGuard::new(plan, cfg, DestKind::ClickHouse)
    }

    fn batch(ids: Vec<Option<i64>>, codes: Vec<Option<&str>>, ts: Vec<Option<i64>>) -> RecordBatch {
        let notes: Vec<Option<&str>> = vec![None; ids.len()];
        let fields: Vec<Field> = plan(true)
            .dest_columns
            .iter()
            .map(|c| Field::new(&c.name, c.arrow.clone(), true))
            .collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(codes)),
                Arc::new(Int64Array::from(ts)),
                Arc::new(StringArray::from(notes)),
            ],
        )
        .unwrap()
    }

    fn guarded(g: &NullGuard) -> Vec<(usize, &str, WarningKind)> {
        g.columns()
            .iter()
            .map(|c| (c.idx, c.column.as_str(), c.kind))
            .collect()
    }

    fn defaulted(g: &NullGuard) -> Vec<(usize, &str, NotNullBy)> {
        g.defaulted()
            .iter()
            .map(|d| (d.idx, d.column.as_str(), d.by))
            .collect()
    }

    #[test]
    fn checks_the_key_and_a_watermark_the_destination_cant_hold_null_in() {
        let g = guard(&plan(true), &cfg(&["code", "id"], Some("ts"))).unwrap();
        assert_eq!(
            guarded(&g),
            [
                (2, "ts", WarningKind::NullWatermark),
                (1, "code", WarningKind::NullKey),
                (0, "id", WarningKind::NullKey),
            ]
        );
        // A watermark the destination holds NULL in lands as NULL, as before.
        let g = guard(&plan(false), &cfg(&["id"], Some("ts"))).unwrap();
        assert_eq!(guarded(&g), [(0, "id", WarningKind::NullKey)]);
        // Nothing to check: no key, and the NOT NULL columns are NOT NULL
        // only because their source columns are.
        assert_eq!(guard(&plan(false), &cfg(&[], Some("ts"))), None);
        assert_eq!(guard(&plan(true), &cfg(&[], None)), None);
    }

    #[test]
    fn a_watermark_that_is_also_a_key_counts_as_the_watermark() {
        let g = guard(&plan(true), &cfg(&["ts", "id"], Some("ts"))).unwrap();
        assert_eq!(
            guarded(&g),
            [
                (2, "ts", WarningKind::NullWatermark),
                (0, "id", WarningKind::NullKey),
            ]
        );
    }

    #[test]
    fn a_key_matches_its_destination_column_case_insensitively_as_bigquery_does() {
        let g = guard(&plan(false), &cfg(&["CODE"], None)).unwrap();
        assert_eq!(guarded(&g), [(1, "CODE", WarningKind::NullKey)]);
        let b = batch(
            vec![Some(1), Some(2)],
            vec![None, Some("b")],
            vec![Some(1), Some(2)],
        );
        let f = g.filter(b).unwrap();
        assert_eq!((f.batch.num_rows(), f.left_out), (1, vec![1]));
    }

    #[test]
    fn decode_columns_make_only_the_checked_columns_nullable() {
        let p = plan(true);
        let g = guard(&p, &cfg(&["code"], Some("ts"))).unwrap();
        let nullable: Vec<bool> = g
            .decode_columns(&p.dest_columns)
            .iter()
            .map(|c| c.nullable)
            .collect();
        assert_eq!(nullable, [false, true, true, true]);
        // A defaulted column is relaxed too.
        let mut c = cfg(&["code"], None);
        c.not_null = vec!["id".into()];
        let g = guard(&p, &c).unwrap();
        let nullable: Vec<bool> = g
            .decode_columns(&p.dest_columns)
            .iter()
            .map(|c| c.nullable)
            .collect();
        assert_eq!(nullable, [true, true, false, true]);
    }

    #[test]
    fn filter_leaves_out_null_rows_and_counts_each_against_its_first_null_column() {
        let g = guard(&plan(true), &cfg(&["code", "id"], Some("ts"))).unwrap();
        let b = batch(
            vec![Some(1), None, Some(3), None, Some(5), Some(6)],
            vec![Some("a"), Some("b"), None, None, Some("e"), None],
            vec![Some(10), Some(20), Some(30), Some(40), None, None],
        );
        let f = g.filter(b).unwrap();
        // ts NULL: rows 5 and 6 (6 has a NULL code too); then code: rows 3
        // and 4 (4 has a NULL id too); then id: row 2.
        assert_eq!(f.left_out, [2, 2, 1]);
        let ids = f
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1]);
        // What's left is typed as the destination has it.
        let nullable: Vec<bool> = f
            .batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.is_nullable())
            .collect();
        assert_eq!(nullable, [false, false, false, true]);
    }

    #[test]
    fn a_batch_without_nulls_is_kept_whole_under_the_strict_schema() {
        let g = guard(&plan(true), &cfg(&["code"], None)).unwrap();
        let b = batch(
            vec![Some(1), None],
            vec![Some("a"), Some("b")],
            vec![None, None],
        );
        let f = g.filter(b).unwrap();
        assert_eq!((f.batch.num_rows(), f.left_out), (2, vec![0]));
        assert!(!f.batch.schema().field(1).is_nullable());
        // Only checked columns change: `id` holds a NULL and isn't checked.
        assert!(f.batch.schema().field(0).is_nullable());
    }

    #[test]
    fn a_batch_of_nothing_but_null_keys_leaves_an_empty_batch() {
        let g = guard(&plan(true), &cfg(&["code"], None)).unwrap();
        let b = batch(vec![Some(1), Some(2)], vec![None, None], vec![None, None]);
        let f = g.filter(b).unwrap();
        assert_eq!((f.batch.num_rows(), f.left_out), (0, vec![2]));
    }

    #[test]
    fn a_batch_not_shaped_like_the_plan_is_an_internal_error() {
        let g = guard(&plan(true), &cfg(&["code"], None)).unwrap();
        let other = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        assert!(g.filter(other).is_err());
    }

    /// The plan `transform` makes for `order_by=["grp"]`, `not_null=["amt"]`
    /// and `key=["id"]`: `grp`, `amt` and `id` NOT NULL, `note` nullable.
    fn defaulted_plan() -> (SelectPlan, TransferConfig) {
        let dest = vec![
            col("id", DataType::Int64, false),
            col("grp", DataType::Utf8, false),
            typed("amt", DataType::Decimal128(10, 2), false, "Decimal(10, 2)"),
            col("note", DataType::Utf8, true),
        ];
        let plan = SelectPlan {
            source_columns: dest.iter().map(|c| c.name.clone()).collect(),
            source_select_exprs: vec![None; 4],
            dest_columns: dest,
        };
        let mut c = cfg(&["id"], None);
        c.order_by = vec!["grp".into(), "id".into()];
        c.not_null = vec!["amt".into()];
        (plan, c)
    }

    fn defaulted_batch(
        ids: Vec<Option<i64>>,
        grps: Vec<Option<&str>>,
        amts: Vec<Option<i128>>,
    ) -> RecordBatch {
        let (plan, _) = defaulted_plan();
        let fields: Vec<Field> = plan
            .dest_columns
            .iter()
            .map(|c| Field::new(&c.name, c.arrow.clone(), true))
            .collect();
        let notes: Vec<Option<&str>> = vec![None; ids.len()];
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(grps)),
                Arc::new(
                    Decimal128Array::from(amts)
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
                Arc::new(StringArray::from(notes)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn order_by_primary_key_not_null_and_engine_columns_are_defaulted() {
        let (plan, mut c) = defaulted_plan();
        let g = guard(&plan, &c).unwrap();
        // `id` is the key, so it is left out, not defaulted, though order_by
        // names it too.
        assert_eq!(guarded(&g), [(0, "id", WarningKind::NullKey)]);
        assert_eq!(
            defaulted(&g),
            [
                (1, "grp", NotNullBy::OrderBy),
                (2, "amt", NotNullBy::NotNull)
            ]
        );
        // The first option that names a column is the one the warning gives.
        c.order_by = vec![];
        c.primary_key = vec!["grp".into()];
        c.not_null = vec!["grp".into(), "amt".into()];
        assert_eq!(
            defaulted(&guard(&plan, &c).unwrap()),
            [
                (1, "grp", NotNullBy::PrimaryKey),
                (2, "amt", NotNullBy::NotNull)
            ]
        );
        // An engine parameter, on ClickHouse only: BigQuery has no engine.
        c.primary_key = vec![];
        c.not_null = vec![];
        c.engine = Some("CollapsingMergeTree(amt)".into());
        assert_eq!(
            defaulted(&guard(&plan, &c).unwrap()),
            [(2, "amt", NotNullBy::Engine)]
        );
        assert!(NullGuard::new(&plan, &c, DestKind::BigQuery)
            .unwrap()
            .defaulted()
            .is_empty());
        // A CSV field can be empty: nothing is left out or defaulted.
        c.key = vec!["id".into()];
        assert!(NullGuard::new(&plan, &c, DestKind::Csv).is_none());
    }

    #[test]
    fn a_nullable_column_or_one_no_option_names_is_not_defaulted() {
        let (mut plan, mut c) = defaulted_plan();
        c.key = vec![];
        c.order_by = vec!["note".into()];
        c.not_null = vec![];
        // `note` is nullable here (as a plan never makes an order_by column),
        // and `id` is NOT NULL only because its source column is.
        assert_eq!(guard(&plan, &c), None);
        plan.dest_columns[3].nullable = false;
        assert_eq!(
            defaulted(&guard(&plan, &c).unwrap()),
            [(3, "note", NotNullBy::OrderBy)]
        );
    }

    #[test]
    fn filter_writes_the_default_in_place_of_a_null_and_counts_it() {
        let (plan, c) = defaulted_plan();
        let g = guard(&plan, &c).unwrap();
        let b = defaulted_batch(
            vec![Some(1), Some(2), None, Some(4)],
            vec![Some("a"), None, None, None],
            vec![Some(150), None, Some(300), Some(400)],
        );
        let f = g.filter(b).unwrap();
        // Row 3 has a NULL key: left out, and its NULL grp isn't counted.
        assert_eq!((&f.left_out[..], &f.defaulted[..]), (&[1][..], &[2, 1][..]));
        assert_eq!(f.batch.num_rows(), 3);
        let grp = f
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            grp.iter().collect::<Vec<_>>(),
            [Some("a"), Some(""), Some("")]
        );
        assert!(grp.nulls().is_none());
        let amt = f
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(amt.values(), &[150, 0, 400]);
        assert_eq!(amt.data_type(), &DataType::Decimal128(10, 2));
        let nullable: Vec<bool> = f
            .batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.is_nullable())
            .collect();
        assert_eq!(nullable, [false, false, false, true]);
        let tallies: Vec<_> = g.tallies(&f).collect();
        assert_eq!(
            tallies,
            [
                (WarningKind::NullKey, "id", 1),
                (WarningKind::NullDefaulted, "grp", 2),
                (WarningKind::NullDefaulted, "amt", 1),
            ]
        );
    }

    #[test]
    fn a_text_column_takes_its_clickhouse_types_own_default() {
        let dest = vec![
            col("id", DataType::Int64, false),
            typed("e", DataType::Utf8, false, "Enum8('b' = 2, 'a' = 1)"),
            typed("u", DataType::Utf8, false, "UUID"),
            typed("lc", DataType::Utf8, false, "LowCardinality(String)"),
        ];
        let plan = SelectPlan {
            source_columns: dest.iter().map(|c| c.name.clone()).collect(),
            source_select_exprs: vec![None; 4],
            dest_columns: dest,
        };
        let mut c = cfg(&["id"], None);
        c.order_by = vec!["e".into(), "u".into(), "lc".into(), "id".into()];
        let g = guard(&plan, &c).unwrap();
        let fields: Vec<Field> = plan
            .dest_columns
            .iter()
            .map(|c| Field::new(&c.name, c.arrow.clone(), true))
            .collect();
        let b = RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("b"), None])),
                Arc::new(StringArray::from(vec![None, Some("x")])),
                Arc::new(StringArray::from(vec![None::<&str>, None])),
            ],
        )
        .unwrap();
        let f = g.filter(b).unwrap();
        assert_eq!(f.defaulted, [1, 1, 2]);
        let text = |i: usize| -> Vec<String> {
            let a = f
                .batch
                .column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            a.iter().map(|v| v.unwrap().to_string()).collect()
        };
        assert_eq!(text(1), ["b", "a"]);
        assert_eq!(text(2), ["00000000-0000-0000-0000-000000000000", "x"]);
        assert_eq!(text(3), ["", ""]);
        // BigQuery reads '' as a STRING's default: no ClickHouse type there.
        let g = NullGuard::new(&plan, &c, DestKind::BigQuery).unwrap();
        assert!(g.defaulted().iter().all(|d| d.text.is_none()));
    }

    #[test]
    fn every_type_a_decoder_produces_has_a_zero() {
        let types = [
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::FixedSizeBinary(4),
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Date32,
            DataType::Date64,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Decimal128(38, 9),
            DataType::Decimal256(76, 10),
        ];
        for dt in types {
            let col = arrow_array::new_null_array(&dt, 3);
            let filled = fill_nulls(&col, None, "c").unwrap();
            assert_eq!(filled.data_type(), &dt);
            assert_eq!((filled.len(), filled.null_count()), (3, 0), "{dt:?}");
            assert!(filled.nulls().is_none(), "{dt:?}");
        }
        let list = arrow_array::new_null_array(
            &DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            1,
        );
        let err = fill_nulls(&list, None, "tags").unwrap_err().to_string();
        assert!(
            err.contains("'tags'") && err.contains("no default"),
            "{err}"
        );
    }

    #[test]
    fn clickhouse_text_defaults_are_what_the_server_reads_as_each_types_default() {
        let d = |t: &str| clickhouse_text_default(t);
        assert_eq!(d("String"), None);
        assert_eq!(d("LowCardinality(String)"), None);
        assert_eq!(d("FixedString(4)"), None);
        assert_eq!(d("DateTime64(6, 'UTC')"), None);
        assert_eq!(
            d("UUID").as_deref(),
            Some("00000000-0000-0000-0000-000000000000")
        );
        assert_eq!(d("Nullable(IPv4)").as_deref(), Some("0.0.0.0"));
        assert_eq!(d("IPv6").as_deref(), Some("::"));
        assert_eq!(d("Bool").as_deref(), Some("false"));
        for n in [
            "Int64",
            "UInt8",
            "Int256",
            "Float64",
            "Decimal(18, 4)",
            "Decimal64(2)",
        ] {
            assert_eq!(d(n).as_deref(), Some("0"), "{n}");
        }
        assert_eq!(d("IntervalDay"), None);
        // As SELECT defaultValueOfTypeName(...) gives on 24.8: the element
        // with the lowest value, whatever order they are written in.
        assert_eq!(d("Enum8('b' = 2, 'a' = 1)").as_deref(), Some("a"));
        assert_eq!(
            d("Enum8('x' = 5, 'neg' = -3, 'z' = 0)").as_deref(),
            Some("neg")
        );
        assert_eq!(
            d("LowCardinality(Enum16('it\\'s' = 7, 'q' = 9))").as_deref(),
            Some("it's")
        );
        assert_eq!(d("Enum8('a, b' = 3, 'c''d' = 1)").as_deref(), Some("c'd"));
        assert_eq!(d("Enum('p', 'q')").as_deref(), Some("p"));
        assert_eq!(d("Enum8('a' = 1, oops)"), None);
    }

    #[test]
    fn fates_name_the_left_out_and_the_defaulted_columns() {
        let p = plan(true);
        let mut c = cfg(&["code"], Some("ts"));
        c.order_by = vec!["id".into()];
        let g = guard(&p, &c).unwrap();
        let fates = g.fates();
        assert_eq!(fates.len(), 3);
        assert_eq!(fates["ts"], NullFate::LeftOut(WarningKind::NullWatermark));
        // By destination name, as decoders name a coerced column.
        assert_eq!(fates["code"], NullFate::LeftOut(WarningKind::NullKey));
        assert_eq!(
            fates["id"],
            NullFate::Default {
                by: NotNullBy::OrderBy,
                shown: "0".into()
            }
        );
        assert!(!fates.contains_key("note"));
    }

    #[test]
    fn the_messages_name_the_column_the_count_and_what_happened() {
        let k = left_out_message(WarningKind::NullKey, "code", 3);
        assert!(k.starts_with("key column 'code' was NULL in 3 row(s) read, which were left out"));
        assert!(k.contains("null_key"), "{k}");
        let w = left_out_message(WarningKind::NullWatermark, "ts", 2);
        assert!(w.starts_with("watermark column 'ts' was NULL in 2 row(s) read"));
        assert!(w.contains("null_watermark"), "{w}");
        let fate = NullFate::Default {
            by: NotNullBy::NotNull,
            shown: "''".into(),
        };
        let d = defaulted_message("grp", 4, Some(&fate));
        assert!(
            d.starts_with("column 'grp' was NULL in 4 row(s) read, which were written with ''"),
            "{d}"
        );
        assert!(d.contains("not_null names it"), "{d}");
        assert!(
            d.contains("coerced_*") && d.contains("null_defaulted"),
            "{d}"
        );
    }
}
