//! Column selection and mapping: turn the resolved *source* schema into a plan
//! describing which source columns to read and how the *destination* columns
//! are named and typed.
//!
//! Renames only relabel Arrow fields (data is identical). Type overrides
//! usually only change the ClickHouse DDL type; ClickHouse converts the
//! incoming Arrow physical type to the destination column type at insert
//! time (e.g. `String` -> `UUID`), which is why the Arrow type is otherwise
//! left untouched. The one exception: an arbitrary-precision NUMERIC/DECIMAL
//! source column overridden to `"Decimal(P, S)"` (`P <= 38`) also gets its
//! Arrow type promoted to `Decimal128(P, S)` here, so the decoder can decode
//! it exactly instead of through a lossy `Float64` round-trip — see
//! `decimal.rs`.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::types::{validate_decimal_precision_and_scale, Decimal128Type};
use arrow_schema::{DataType, TimeUnit};

use crate::config::TransferConfig;
use crate::decimal::parse_decimal_override;
use crate::error::{EtlError, Result};
use crate::types::{may_coerce_to_null, ColumnType};

/// Classify a destination datetime type name into the Arrow timezone it
/// implies: `Some(Some(tz))` = tz-aware (BigQuery `TIMESTAMP` / ClickHouse
/// `DateTime64(P, 'tz')`), `Some(None)` = naive (BigQuery `DATETIME` /
/// ClickHouse `DateTime64(P)`), and `None` = not a datetime type at all (so
/// the source Arrow type is left untouched).
///
/// This is what makes a `type_overrides` entry flip a datetime column's
/// *encoding*, not just its DDL string. MySQL datetimes now default to
/// tz-aware UTC (see `types::map_mysql_type`), so `type_overrides={col:
/// "DATETIME"}` is the per-column opt-out back to naive — and because the Arrow
/// tz flag drives the BigQuery Storage Write proto encoding (Int64 micros vs
/// civil string) and the destination column type, flipping it here fixes the
/// whole chain, which a DDL-only relabel could not (it would leave a
/// TIMESTAMP-encoded payload aimed at a DATETIME column, or vice-versa).
///
/// Recognizes BigQuery names (`TIMESTAMP`/`DATETIME`) and ClickHouse names
/// (`DateTime`/`DateTime64(...)`), case-insensitively.
fn datetime_override_tz(dest_type: &str) -> Option<Option<Arc<str>>> {
    let trimmed = dest_type.trim();
    let upper = trimmed.to_ascii_uppercase();
    // BigQuery scalar names are exact.
    if upper == "TIMESTAMP" {
        return Some(Some(Arc::from("UTC")));
    }
    if upper == "DATETIME" {
        return Some(None);
    }
    // ClickHouse `DateTime` / `DateTime64(P[, 'tz']])`: tz-aware iff a quoted
    // timezone argument is present.
    if upper.starts_with("DATETIME") {
        return Some(extract_quoted_tz(trimmed));
    }
    None
}

/// Extract a single-quoted timezone argument (e.g. `UTC` from
/// `DateTime64(6, 'UTC')`), or `None` when the type carries no timezone.
fn extract_quoted_tz(s: &str) -> Option<Arc<str>> {
    let start = s.find('\'')?;
    let rest = &s[start + 1..];
    let end = rest.find('\'')?;
    Some(Arc::from(&rest[..end]))
}

#[derive(Debug)]
pub struct SelectPlan {
    /// Source column names to read, in order (drives the COPY SELECT list).
    pub source_columns: Vec<String>,
    /// Destination columns: dest name + source OID/Arrow type + (maybe) overridden CH type.
    /// Used both to build the Arrow decode schema and to generate DDL.
    pub dest_columns: Vec<ColumnType>,
}

pub fn plan(source: &[ColumnType], cfg: &TransferConfig) -> Result<SelectPlan> {
    // 1. Apply include (allowlist) then exclude (denylist) on source names.
    let included: Vec<&ColumnType> = source
        .iter()
        .filter(|c| cfg.include.is_empty() || cfg.include.contains(&c.name))
        .filter(|c| !cfg.exclude.contains(&c.name))
        .collect();

    if included.is_empty() {
        return Err(EtlError::config(
            "column selection (include/exclude) left no columns to transfer",
        ));
    }

    // Validate include names actually exist, to fail loudly on typos.
    for want in &cfg.include {
        if !source.iter().any(|c| &c.name == want) {
            return Err(EtlError::config(format!(
                "include column '{want}' not found in source"
            )));
        }
    }

    // ClickHouse rejects a Nullable column in ORDER BY / PRIMARY KEY outright.
    // Nullability is normally resolved from the source's NOT NULL constraints,
    // but that resolution only works for a plain table (see
    // PgSource::resolve_columns's `base_table` param); with `source_query`
    // there's no table to check constraints against, so every column
    // defaults to nullable. A business/dedup key should never be null
    // regardless of how the schema was resolved, so force it here.
    let key_columns: HashSet<&str> = cfg
        .key
        .iter()
        .chain(cfg.order_by.iter())
        .chain(cfg.primary_key.iter())
        .map(|s| s.as_str())
        .collect();

    let mut source_columns = Vec::with_capacity(included.len());
    let mut dest_columns = Vec::with_capacity(included.len());
    for c in included {
        source_columns.push(c.name.clone());
        let dest_name = cfg.rename.get(&c.name).cloned().unwrap_or_else(|| c.name.clone());
        let ch_inner = cfg
            .type_overrides
            .get(&c.name)
            .or_else(|| cfg.type_overrides.get(&dest_name))
            .cloned()
            .unwrap_or_else(|| c.clickhouse_inner.clone());
        let is_key = key_columns.contains(dest_name.as_str());
        // ClickHouse's ReplacingMergeTree also rejects a Nullable version
        // column outright (`Code: 169 BAD_TYPE_OF_FIELD`), same restriction
        // as ORDER BY/PRIMARY KEY above, just for a different column — the
        // watermark, when it's the effective engine's version column.
        // Matched against the *source* name (not `dest_name`): the DDL's
        // `ReplacingMergeTree(<watermark>)` clause already assumes the
        // watermark column's name is unchanged by `rename` (a pre-existing
        // characteristic of `ddl::create_table`, not something this touches).
        let is_version_column =
            cfg.watermark.as_deref() == Some(c.name.as_str()) && cfg.effective_engine() == "ReplacingMergeTree";
        let force_non_nullable = is_key || is_version_column;
        // Date32/Timestamp columns are otherwise forced nullable regardless
        // of the source's own NOT NULL constraint: their decoders coerce
        // unrepresentable values (zero-dates, out-of-ch_range years) to NULL
        // unconditionally, and a NOT NULL Arrow field containing a coerced
        // NULL fails downstream with a schema-consistency error instead of
        // transferring cleanly — see `types::may_coerce_to_null`'s docs.
        // `force_non_nullable` still wins over this: a key or version column
        // that's also a coercible date type and actually hits a zero-date
        // will still fail — an inherent conflict between two hard
        // constraints, not something papered over here (both combinations
        // are expected to be rare: a business/dedup key or watermark column
        // with legacy zero-date values).
        // An arbitrary-precision decimal column overridden to "Decimal(P,
        // S)" gets its Arrow type promoted from Float64 to Decimal128(P,S)
        // so the decoder can decode it exactly (see decimal.rs). Gating on
        // the already-resolved `ch_inner` (rather than re-querying
        // `cfg.type_overrides`) means one code path whether or not this
        // column was overridden: with no override, `ch_inner` is just
        // `c.clickhouse_inner` ("Float64"), which never parses as
        // "Decimal(P,S)", so `arrow` falls through unchanged below.
        //
        // Safe even though `plan()` has no destination-type knowledge (the
        // same `type_overrides` map is reused verbatim for a BigQuery
        // destination's own type-name syntax, e.g. "NUMERIC" — see
        // sink/bigquery.rs::build_table): a BigQuery type name never
        // contains parens or digits, so it can never parse as
        // "Decimal(P,S)" and this branch is simply never taken for that case.
        let arrow = if c.arbitrary_precision_decimal {
            match parse_decimal_override(&ch_inner) {
                Some((p, s)) => {
                    validate_decimal_precision_and_scale::<Decimal128Type>(p, s).map_err(|e| {
                        EtlError::config(format!(
                            "column '{dest_name}': type_overrides '{ch_inner}' is not a valid \
                             Decimal128(P,S) override: {e} (Decimal256/P>38 isn't supported yet — \
                             use P<=38, or drop the override to keep the default lossy Float64 mapping)"
                        ))
                    })?;
                    DataType::Decimal128(p, s)
                }
                None => c.arrow.clone(),
            }
        } else if matches!(c.arrow, DataType::Timestamp(_, _)) {
            // A datetime column's tz-awareness follows its (possibly
            // overridden) destination type — this is the per-column opt-out
            // that lets `type_overrides={col: "DATETIME"}` render a
            // (UTC-by-default) MySQL datetime as a naive BigQuery DATETIME, and
            // the reverse. With no override, `ch_inner` is the source's own
            // default mapping string, so this reproduces the source tz exactly
            // (no behavior change). A non-datetime override (e.g. "String")
            // returns `None` and leaves the Arrow timestamp untouched, matching
            // the pre-existing DDL-only-relabel behavior for such overrides.
            match datetime_override_tz(&ch_inner) {
                Some(tz) => DataType::Timestamp(TimeUnit::Microsecond, tz),
                None => c.arrow.clone(),
            }
        } else {
            c.arrow.clone()
        };
        let nullable = if force_non_nullable {
            false
        } else {
            c.nullable || may_coerce_to_null(&arrow)
        };
        dest_columns.push(ColumnType {
            name: dest_name,
            type_id: c.type_id,
            nullable,
            arrow,
            clickhouse_inner: ch_inner,
            arbitrary_precision_decimal: c.arbitrary_precision_decimal,
        });
    }

    Ok(SelectPlan {
        source_columns,
        dest_columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;
    use std::collections::HashMap;

    fn c(name: &str) -> ColumnType {
        ColumnType {
            name: name.into(),
            type_id: 23,
            nullable: true,
            arrow: DataType::Int32,
            clickhouse_inner: "Int32".into(),
            arbitrary_precision_decimal: false,
        }
    }

    fn typed_col(name: &str, arrow: DataType, nullable: bool) -> ColumnType {
        ColumnType {
            name: name.into(),
            type_id: 0,
            nullable,
            arrow,
            clickhouse_inner: "irrelevant".into(),
            arbitrary_precision_decimal: false,
        }
    }

    /// A source column shaped like a real Postgres/MySQL/BigQuery
    /// arbitrary-precision NUMERIC/DECIMAL column: defaults to Float64 with
    /// `arbitrary_precision_decimal: true`, same as any of the three
    /// sources' own schema resolution would produce.
    fn decimal_col(name: &str) -> ColumnType {
        ColumnType {
            name: name.into(),
            type_id: 0,
            nullable: true,
            arrow: DataType::Float64,
            clickhouse_inner: "Float64".into(),
            arbitrary_precision_decimal: true,
        }
    }

    fn cfg() -> TransferConfig {
        TransferConfig {
            source_table: Some("t".into()),
            source_query: None,
            dest_table: "t".into(),
            mode: crate::config::SyncMode::Full,
            watermark: None,
            lookback_seconds: 0,
            key: vec![],
            create_if_missing: true,
            engine: None,
            order_by: vec![],
            partition_by: None,
            primary_key: vec![],
            parallelism: 1,
            batch_rows: 1,
            batch_bytes: 0,
            max_memory_bytes: 0,
            partition_column: None,
            read_max_rows_per_sec: None,
            type_overrides: HashMap::new(),
            rename: HashMap::new(),
            include: vec![],
            exclude: vec![],
        }
    }

    #[test]
    fn exclude_and_rename_apply() {
        let src = vec![c("id"), c("display_name"), c("amount")];
        let mut cfg = cfg();
        cfg.exclude = vec!["display_name".into()];
        cfg.rename = HashMap::from([("amount".to_string(), "amt".to_string())]);
        cfg.type_overrides = HashMap::from([("amt".to_string(), "Decimal(18, 2)".to_string())]);
        let p = plan(&src, &cfg).unwrap();
        assert_eq!(p.source_columns, vec!["id", "amount"]);
        assert_eq!(p.dest_columns[1].name, "amt");
        assert_eq!(p.dest_columns[1].clickhouse_inner, "Decimal(18, 2)");
    }

    #[test]
    fn unknown_include_errors() {
        let src = vec![c("id")];
        let mut cfg = cfg();
        cfg.include = vec!["nope".into()];
        assert!(plan(&src, &cfg).is_err());
    }

    /// With `source_query` (no base table), every column resolves as
    /// nullable — but a `key` column must never end up `Nullable(...)` in the
    /// generated DDL, since ClickHouse rejects nullable sort keys outright.
    #[test]
    fn key_column_forced_non_nullable() {
        let src = vec![c("id"), c("name")]; // c() always resolves nullable: true
        let mut cfg = cfg();
        cfg.key = vec!["id".into()];
        let p = plan(&src, &cfg).unwrap();
        assert!(!p.dest_columns[0].nullable, "key column must not be nullable");
        assert!(p.dest_columns[1].nullable, "non-key column keeps its resolved nullability");
    }

    /// Regression test for bug report 06: a NOT NULL MySQL DATE/DATETIME
    /// column resolves as `nullable: false` from the source's own
    /// constraint, but decode_mysql.rs coerces legacy zero-dates and
    /// out-of-ch_range years to NULL unconditionally — an Arrow field
    /// declared non-nullable that then receives a coerced NULL fails
    /// downstream with a schema-consistency error. Date32/Timestamp columns
    /// must be forced nullable regardless of the resolved source
    /// nullability, so that coercion (already tested at the decoder level in
    /// decode_mysql.rs/decode.rs/decode_bigquery.rs) can't produce an
    /// inconsistent batch.
    #[test]
    fn not_null_date_and_timestamp_columns_forced_nullable() {
        let src = vec![
            typed_col("id", DataType::Int64, false),
            typed_col("created_date", DataType::Date32, false), // NOT NULL in the source
            typed_col(
                "updated_at",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                false, // NOT NULL in the source
            ),
            typed_col("name", DataType::Utf8, false), // NOT NULL, but not a coercible type
        ];
        let p = plan(&src, &cfg()).unwrap();
        assert!(!p.dest_columns[0].nullable, "plain NOT NULL Int64 column is untouched");
        assert!(
            p.dest_columns[1].nullable,
            "NOT NULL DATE column must be forced nullable (zero-date coercion target)"
        );
        assert!(
            p.dest_columns[2].nullable,
            "NOT NULL DATETIME/TIMESTAMP column must be forced nullable (out-of-range coercion target)"
        );
        assert!(
            !p.dest_columns[3].nullable,
            "NOT NULL text column has no coercion path, so stays non-nullable"
        );
    }

    /// The `key` (or order_by/primary_key) non-nullable rule wins over the
    /// date/timestamp forced-nullable rule — ClickHouse rejects a nullable
    /// sort key outright, a harder constraint than the coercion concern.
    #[test]
    fn key_column_wins_over_date_forced_nullable() {
        let src = vec![typed_col("event_date", DataType::Date32, false)];
        let mut cfg = cfg();
        cfg.key = vec!["event_date".into()];
        let p = plan(&src, &cfg).unwrap();
        assert!(!p.dest_columns[0].nullable, "key column must stay non-nullable even for a date type");
    }

    /// Regression test: forcing the watermark column nullable (as a
    /// Date/Timestamp type normally would be) broke incremental syncs
    /// outright when the effective engine is `ReplacingMergeTree` — its
    /// version column must not be `Nullable(...)` either
    /// (`Code: 169 BAD_TYPE_OF_FIELD`), the same class of constraint as a
    /// sort key, just for a different column. Caught by the existing
    /// integration test suite (`test_incremental_appends_and_is_idempotent`)
    /// failing against a live ClickHouse before this was added.
    #[test]
    fn watermark_column_forced_non_nullable_when_replacing_merge_tree() {
        let src = vec![
            typed_col("id", DataType::Int64, false),
            typed_col(
                "write_date",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                false,
            ),
        ];
        let mut cfg = cfg();
        cfg.mode = crate::config::SyncMode::Incremental;
        cfg.watermark = Some("write_date".into());
        // engine left None -> effective_engine() defaults to ReplacingMergeTree for Incremental.
        let p = plan(&src, &cfg).unwrap();
        assert!(
            !p.dest_columns[1].nullable,
            "watermark column used as the ReplacingMergeTree version column must stay non-nullable"
        );
    }

    /// The same watermark column is NOT forced non-nullable when the
    /// effective engine isn't ReplacingMergeTree (e.g. an explicit
    /// `engine="MergeTree"` override) — nothing structurally requires it
    /// there, so the ordinary coercion-target nullable rule still applies.
    #[test]
    fn watermark_column_not_forced_when_engine_is_not_replacing_merge_tree() {
        let src = vec![typed_col(
            "write_date",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            false,
        )];
        let mut cfg = cfg();
        cfg.mode = crate::config::SyncMode::Incremental;
        cfg.watermark = Some("write_date".into());
        cfg.engine = Some("MergeTree".into());
        let p = plan(&src, &cfg).unwrap();
        assert!(p.dest_columns[0].nullable, "no ReplacingMergeTree version-column constraint applies here");
    }

    /// order_by/primary_key are matched against the *destination* (post-rename)
    /// name, since that's what actually ends up in the DDL's ORDER BY/PRIMARY KEY.
    #[test]
    fn order_by_and_primary_key_matched_after_rename() {
        let src = vec![c("id"), c("amount")];
        let mut cfg = cfg();
        cfg.rename = HashMap::from([("id".to_string(), "pk".to_string())]);
        cfg.order_by = vec!["pk".into()];
        cfg.primary_key = vec!["pk".into()];
        let p = plan(&src, &cfg).unwrap();
        assert_eq!(p.dest_columns[0].name, "pk");
        assert!(!p.dest_columns[0].nullable);
    }

    /// Bug #6 fix: a `type_overrides` "Decimal(P, S)" entry on an
    /// arbitrary-precision decimal column must promote the Arrow type from
    /// Float64 to Decimal128(P, S), so the decoder can decode it exactly
    /// instead of through the previously-lossy Float64 round-trip.
    #[test]
    fn decimal_override_promotes_arrow_type_to_decimal128() {
        let src = vec![decimal_col("amount")];
        let mut cfg = cfg();
        cfg.type_overrides = HashMap::from([("amount".to_string(), "Decimal(30, 10)".to_string())]);
        let p = plan(&src, &cfg).unwrap();
        assert_eq!(p.dest_columns[0].arrow, DataType::Decimal128(30, 10));
        // A Decimal128 column may be coerced to NULL on overflow, so it must
        // be forced nullable regardless of the source's own nullability.
        assert!(p.dest_columns[0].nullable);
    }

    /// Without an override, an arbitrary-precision decimal column keeps its
    /// default Float64 mapping unchanged (today's existing, lossy behavior —
    /// unaffected by this fix, which only activates when the user opts in).
    #[test]
    fn decimal_column_stays_float64_without_override() {
        let src = vec![decimal_col("amount")];
        let p = plan(&src, &cfg()).unwrap();
        assert_eq!(p.dest_columns[0].arrow, DataType::Float64);
    }

    /// Regression guard: a genuine Float64 column (arbitrary_precision_decimal:
    /// false) must NEVER be reinterpreted as Decimal128, even if its
    /// type_overrides string happens to parse as "Decimal(P,S)" syntax.
    #[test]
    fn genuine_float_column_is_never_reinterpreted_as_decimal() {
        let src = vec![typed_col("ratio", DataType::Float64, true)];
        let mut cfg = cfg();
        cfg.type_overrides = HashMap::from([("ratio".to_string(), "Decimal(10, 2)".to_string())]);
        let p = plan(&src, &cfg).unwrap();
        assert_eq!(p.dest_columns[0].arrow, DataType::Float64);
        // The DDL override string itself is still honored (it just doesn't
        // change the Arrow decode path for a non-decimal-sourced column).
        assert_eq!(p.dest_columns[0].clickhouse_inner, "Decimal(10, 2)");
    }

    /// A `Decimal(P, S)` override with `P > 38` is a hard config error at
    /// plan() time — not a silent fallback to the lossy Float64 mapping,
    /// which would defeat the fix for exactly the inputs it exists to
    /// protect while the user believes precision is now handled. Decimal256
    /// (P 39-76) is a documented, not-yet-implemented follow-up.
    #[test]
    fn decimal_override_precision_above_38_is_a_config_error() {
        let src = vec![decimal_col("amount")];
        let mut cfg = cfg();
        cfg.type_overrides = HashMap::from([("amount".to_string(), "Decimal(50, 10)".to_string())]);
        let err = plan(&src, &cfg).unwrap_err().to_string();
        assert!(err.contains("amount"), "must name the column: {err}");
        assert!(err.contains("Decimal256"), "must mention the Decimal256 follow-up: {err}");
    }

    fn ts(tz: Option<&str>) -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, tz.map(Arc::from))
    }

    /// The classifier that drives the per-column datetime opt-out: BigQuery and
    /// ClickHouse type names, tz-aware vs naive, and non-datetime strings.
    #[test]
    fn datetime_override_tz_classifies_type_names() {
        // tz-aware
        assert_eq!(datetime_override_tz("TIMESTAMP"), Some(Some(Arc::from("UTC"))));
        assert_eq!(datetime_override_tz("timestamp"), Some(Some(Arc::from("UTC"))));
        assert_eq!(datetime_override_tz("DateTime64(6, 'UTC')"), Some(Some(Arc::from("UTC"))));
        assert_eq!(datetime_override_tz("DateTime('Asia/Jakarta')"), Some(Some(Arc::from("Asia/Jakarta"))));
        // naive
        assert_eq!(datetime_override_tz("DATETIME"), Some(None));
        assert_eq!(datetime_override_tz("DateTime64(6)"), Some(None));
        assert_eq!(datetime_override_tz("DateTime"), Some(None));
        // not a datetime type at all -> leave the Arrow type alone
        assert_eq!(datetime_override_tz("String"), None);
        assert_eq!(datetime_override_tz("Int64"), None);
        assert_eq!(datetime_override_tz("Decimal(18, 2)"), None);
    }

    /// A `type_overrides={col: "DATETIME"}` on a (UTC-by-default) MySQL-style
    /// timestamp column flips its Arrow tz to naive — the per-column opt-out
    /// that makes it land as a BigQuery DATETIME. This drives the Storage Write
    /// proto encoding, not just the DDL string, so it's the only thing that
    /// actually works (a DDL-only relabel would leave a mismatched payload).
    #[test]
    fn datetime_override_flips_utc_timestamp_to_naive() {
        let src = vec![typed_col("event_at", ts(Some("UTC")), true)];
        let mut cfg = cfg();
        cfg.type_overrides = HashMap::from([("event_at".to_string(), "DATETIME".to_string())]);
        let p = plan(&src, &cfg).unwrap();
        assert_eq!(p.dest_columns[0].arrow, ts(None), "override must flip tz-aware -> naive");
        assert_eq!(p.dest_columns[0].clickhouse_inner, "DATETIME");
    }

    /// The reverse: an override to `TIMESTAMP` promotes a naive timestamp
    /// column to tz-aware UTC (BigQuery TIMESTAMP).
    #[test]
    fn datetime_override_promotes_naive_timestamp_to_utc() {
        let src = vec![typed_col("event_at", ts(None), true)];
        let mut cfg = cfg();
        cfg.type_overrides = HashMap::from([("event_at".to_string(), "TIMESTAMP".to_string())]);
        let p = plan(&src, &cfg).unwrap();
        assert_eq!(p.dest_columns[0].arrow, ts(Some("UTC")), "override must flip naive -> tz-aware UTC");
    }

    /// With no override, a timestamp column keeps its source tz exactly — the
    /// classifier reproduces the default from the source's own `clickhouse_inner`
    /// mapping string, so this is a no-op for existing behavior.
    #[test]
    fn timestamp_without_override_keeps_source_tz() {
        let mut utc_col = typed_col("a", ts(Some("UTC")), true);
        utc_col.clickhouse_inner = "DateTime64(6, 'UTC')".into();
        let mut naive_col = typed_col("b", ts(None), true);
        naive_col.clickhouse_inner = "DateTime64(6)".into();
        let p = plan(&[utc_col, naive_col], &cfg()).unwrap();
        assert_eq!(p.dest_columns[0].arrow, ts(Some("UTC")), "UTC source stays UTC");
        assert_eq!(p.dest_columns[1].arrow, ts(None), "naive source stays naive");
    }

    /// A non-datetime override (e.g. storing a timestamp as text) leaves the
    /// Arrow timestamp untouched — preserving the pre-existing DDL-only-relabel
    /// behavior for such overrides (ClickHouse coerces at insert time).
    #[test]
    fn non_datetime_override_leaves_timestamp_arrow_untouched() {
        let src = vec![typed_col("event_at", ts(Some("UTC")), true)];
        let mut cfg = cfg();
        cfg.type_overrides = HashMap::from([("event_at".to_string(), "String".to_string())]);
        let p = plan(&src, &cfg).unwrap();
        assert_eq!(p.dest_columns[0].arrow, ts(Some("UTC")), "arrow type unchanged by a non-datetime override");
        assert_eq!(p.dest_columns[0].clickhouse_inner, "String", "DDL string still honored");
    }
}
