//! Rows the destination can't hold because a column that can't be NULL there
//! is NULL (#49): a `key` column, on every destination, and the watermark when
//! the destination's column can't be NULL (a ClickHouse `ReplacingMergeTree`'s
//! version column).
//!
//! Every source's batches pass through [`NullGuard::filter`] on their way to
//! the destination (`SendCtx::push_batch`) and to the archive, so the rule is
//! the same whatever made the value NULL: a nullable source column, an API
//! record without the field, or a value a decoder couldn't read (a
//! `coerced_*` warning). Such a row is left out and counted, as `null_key` or
//! `null_watermark`. Before, a database source failed the write partway with
//! Arrow's "declared as non-nullable but contains null values", and an API
//! source's NULL landed as the type's default (`''`, `1970-01-01`), where keys
//! collapsed into one row and a version lost to the row already stored.
//!
//! A database read already leaves out a row whose watermark is NULL (its
//! filter never matches one, and `null_watermark` reports them from a count),
//! so here the watermark only meets a value the decoder made NULL, or one from
//! a source that has no filter (an API, a DataFrame).
//!
//! The decoders build batches with the checked columns nullable
//! ([`NullGuard::decode_columns`]), so a NULL reaches the filter instead of
//! failing the batch, and the filter hands on batches whose schema says those
//! columns hold none ([`NullGuard::strict_schema`]), as the destination's
//! does.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef};

use crate::config::{TransferConfig, WarningKind};
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

/// The columns whose NULL rows a run leaves out, and the filter that does it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NullGuard {
    /// In check order: the watermark first, then the key columns in `key=`
    /// order. A row left out counts once, against the first of them that is
    /// NULL in it, so a row with a NULL watermark counts as `null_watermark`
    /// whatever its key holds, as it does when a database read leaves it out.
    columns: Vec<Guarded>,
}

impl NullGuard {
    /// The guard for a run writing `plan` under `cfg`, or `None` when there is
    /// nothing to check: no `key`, and a watermark (if any) the destination
    /// can hold NULL in.
    ///
    /// A key column is checked on every destination. The plan makes it NOT
    /// NULL (a ClickHouse sorting key can't be NULL, and BigQuery gets the
    /// same table), and where a BigQuery table already has it NULLABLE, a NULL
    /// key never matches in the `MERGE`, so that row would be inserted again
    /// on every run.
    pub(crate) fn new(plan: &SelectPlan, cfg: &TransferConfig) -> Option<Self> {
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
        (!columns.is_empty()).then_some(Self { columns })
    }

    pub(crate) fn columns(&self) -> &[Guarded] {
        &self.columns
    }

    /// `dest_columns` (the plan's) with the checked columns nullable: what a
    /// decoder builds batches with, so a NULL there reaches [`Self::filter`]
    /// rather than failing the batch. The ClickHouse source casts to these
    /// too, as its server refuses to cast a NULL to a non-`Nullable` type.
    pub(crate) fn decode_columns(&self, dest_columns: &[ColumnType]) -> Vec<ColumnType> {
        let mut out = dest_columns.to_vec();
        for g in &self.columns {
            if let Some(c) = out.get_mut(g.idx) {
                c.nullable = true;
            }
        }
        out
    }

    /// `schema` with the checked columns non-nullable, as the destination has
    /// them: the schema of what [`Self::filter`] returns.
    pub(crate) fn strict_schema(&self, schema: &SchemaRef) -> SchemaRef {
        if self.columns.iter().all(|g| {
            schema
                .fields()
                .get(g.idx)
                .map_or(true, |f| !f.is_nullable())
        }) {
            return schema.clone();
        }
        let fields: Vec<Field> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let checked = self.columns.iter().any(|g| g.idx == i);
                f.as_ref()
                    .clone()
                    .with_nullable(f.is_nullable() && !checked)
            })
            .collect();
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
    }

    /// `batch` without its rows that hold a NULL in a checked column, under
    /// [`Self::strict_schema`], and how many rows each checked column (in
    /// [`Self::columns`] order) left out.
    pub(crate) fn filter(&self, batch: RecordBatch) -> Result<(RecordBatch, Vec<u64>)> {
        let mut left_out = vec![0u64; self.columns.len()];
        let mut checked = Vec::with_capacity(self.columns.len());
        for g in &self.columns {
            let field = batch.schema().fields().get(g.idx).cloned();
            match field {
                Some(f) if f.name() == &g.dest => checked.push(batch.column(g.idx).clone()),
                _ => {
                    return Err(EtlError::internal(format!(
                        "the batch has no column '{}' at position {} to check for NULLs",
                        g.dest, g.idx
                    )))
                }
            }
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
        let schema = self.strict_schema(&batch.schema());
        let batch = RecordBatch::try_new(schema, batch.columns().to_vec())?;
        Ok((batch, left_out))
    }
}

/// What became of a value a decoder couldn't read (a `coerced_*` warning) in
/// a column the destination can't hold NULL in, for that warning's text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NullFate {
    /// [`NullGuard`] left the row out, and counted it under this kind.
    LeftOut(WarningKind),
    /// The row was written, and ClickHouse stored the column's default in
    /// place of the NULL: an API source's decoder hands on NULL in every
    /// column, and ClickHouse fills a non-`Nullable` one with its default.
    /// Only a column the guard doesn't check (`order_by`, `primary_key`,
    /// `not_null`, an engine parameter); elsewhere the NULL fails the batch.
    Default,
}

/// The [`NullFate`] of each destination column that has one, by destination
/// name, which is how the decoders name a coerced column. `defaults_null`
/// says the run writes an API source into ClickHouse (see
/// [`NullFate::Default`]).
pub(crate) fn null_fates(
    plan: &SelectPlan,
    guard: Option<&NullGuard>,
    defaults_null: bool,
) -> HashMap<String, NullFate> {
    let mut fates = HashMap::new();
    for (i, d) in plan.dest_columns.iter().enumerate() {
        let checked = guard.and_then(|g| g.columns.iter().find(|c| c.idx == i));
        if let Some(c) = checked {
            fates.insert(d.name.clone(), NullFate::LeftOut(c.kind));
        } else if defaults_null && !d.nullable {
            fates.insert(d.name.clone(), NullFate::Default);
        }
    }
    fates
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::DataType;

    fn col(name: &str, arrow: DataType, nullable: bool) -> ColumnType {
        ColumnType {
            name: name.into(),
            type_id: 0,
            nullable,
            arrow,
            clickhouse_inner: "String".into(),
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

    #[test]
    fn checks_the_key_and_a_watermark_the_destination_cant_hold_null_in() {
        let g = NullGuard::new(&plan(true), &cfg(&["code", "id"], Some("ts"))).unwrap();
        assert_eq!(
            guarded(&g),
            [
                (2, "ts", WarningKind::NullWatermark),
                (1, "code", WarningKind::NullKey),
                (0, "id", WarningKind::NullKey),
            ]
        );
        // A watermark the destination holds NULL in lands as NULL, as before.
        let g = NullGuard::new(&plan(false), &cfg(&["id"], Some("ts"))).unwrap();
        assert_eq!(guarded(&g), [(0, "id", WarningKind::NullKey)]);
        // Nothing to check.
        assert_eq!(NullGuard::new(&plan(false), &cfg(&[], Some("ts"))), None);
        assert_eq!(NullGuard::new(&plan(true), &cfg(&[], None)), None);
    }

    #[test]
    fn a_watermark_that_is_also_a_key_counts_as_the_watermark() {
        let g = NullGuard::new(&plan(true), &cfg(&["ts", "id"], Some("ts"))).unwrap();
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
        let g = NullGuard::new(&plan(false), &cfg(&["CODE"], None)).unwrap();
        assert_eq!(guarded(&g), [(1, "CODE", WarningKind::NullKey)]);
        let b = batch(
            vec![Some(1), Some(2)],
            vec![None, Some("b")],
            vec![Some(1), Some(2)],
        );
        let (kept, left_out) = g.filter(b).unwrap();
        assert_eq!((kept.num_rows(), left_out), (1, vec![1]));
    }

    #[test]
    fn decode_columns_make_only_the_checked_columns_nullable() {
        let p = plan(true);
        let g = NullGuard::new(&p, &cfg(&["code"], Some("ts"))).unwrap();
        let nullable: Vec<bool> = g
            .decode_columns(&p.dest_columns)
            .iter()
            .map(|c| c.nullable)
            .collect();
        assert_eq!(nullable, [false, true, true, true]);
    }

    #[test]
    fn filter_leaves_out_null_rows_and_counts_each_against_its_first_null_column() {
        let g = NullGuard::new(&plan(true), &cfg(&["code", "id"], Some("ts"))).unwrap();
        let b = batch(
            vec![Some(1), None, Some(3), None, Some(5), Some(6)],
            vec![Some("a"), Some("b"), None, None, Some("e"), None],
            vec![Some(10), Some(20), Some(30), Some(40), None, None],
        );
        let (kept, left_out) = g.filter(b).unwrap();
        // ts NULL: rows 5 and 6 (6 has a NULL code too); then code: rows 3
        // and 4 (4 has a NULL id too); then id: row 2.
        assert_eq!(left_out, [2, 2, 1]);
        let ids = kept
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1]);
        // What's left is typed as the destination has it.
        let nullable: Vec<bool> = kept
            .schema()
            .fields()
            .iter()
            .map(|f| f.is_nullable())
            .collect();
        assert_eq!(nullable, [false, false, false, true]);
    }

    #[test]
    fn a_batch_without_nulls_is_kept_whole_under_the_strict_schema() {
        let g = NullGuard::new(&plan(true), &cfg(&["code"], None)).unwrap();
        let b = batch(
            vec![Some(1), None],
            vec![Some("a"), Some("b")],
            vec![None, None],
        );
        let (kept, left_out) = g.filter(b).unwrap();
        assert_eq!((kept.num_rows(), left_out), (2, vec![0]));
        assert!(!kept.schema().field(1).is_nullable());
        // Only checked columns change: `id` holds a NULL and isn't checked.
        assert!(kept.schema().field(0).is_nullable());
    }

    #[test]
    fn a_batch_of_nothing_but_null_keys_leaves_an_empty_batch() {
        let g = NullGuard::new(&plan(true), &cfg(&["code"], None)).unwrap();
        let b = batch(vec![Some(1), Some(2)], vec![None, None], vec![None, None]);
        let (kept, left_out) = g.filter(b).unwrap();
        assert_eq!((kept.num_rows(), left_out), (0, vec![2]));
    }

    #[test]
    fn a_batch_not_shaped_like_the_plan_is_an_internal_error() {
        let g = NullGuard::new(&plan(true), &cfg(&["code"], None)).unwrap();
        let other = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        assert!(g.filter(other).is_err());
    }

    #[test]
    fn fates_name_the_left_out_columns_and_the_api_defaults() {
        let p = plan(true);
        let g = NullGuard::new(&p, &cfg(&["code"], Some("ts"))).unwrap();
        let fates = null_fates(&p, Some(&g), false);
        assert_eq!(fates.len(), 2);
        assert_eq!(fates["ts"], NullFate::LeftOut(WarningKind::NullWatermark));
        assert_eq!(fates["code"], NullFate::LeftOut(WarningKind::NullKey));
        // An API source into ClickHouse: `id` can't be NULL and isn't checked.
        let fates = null_fates(&p, Some(&g), true);
        assert_eq!(fates["id"], NullFate::Default);
        assert!(!fates.contains_key("note"));
        assert_eq!(null_fates(&p, None, false), HashMap::new());
    }

    #[test]
    fn the_messages_name_the_column_the_count_and_what_happened() {
        let k = left_out_message(WarningKind::NullKey, "code", 3);
        assert!(k.starts_with("key column 'code' was NULL in 3 row(s) read, which were left out"));
        assert!(k.contains("null_key"), "{k}");
        let w = left_out_message(WarningKind::NullWatermark, "ts", 2);
        assert!(w.starts_with("watermark column 'ts' was NULL in 2 row(s) read"));
        assert!(w.contains("null_watermark"), "{w}");
    }
}
