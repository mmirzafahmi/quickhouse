//! Column selection and mapping: turn the resolved *source* schema into a plan
//! describing which source columns to read and how the *destination* columns
//! are named and typed.
//!
//! Renames only relabel Arrow fields (data is identical). Type overrides change
//! the ClickHouse DDL type; ClickHouse converts the incoming Arrow physical type
//! to the destination column type at insert time (e.g. `String` -> `UUID`,
//! `Float64` -> `Decimal(...)`), which is why the Arrow type is left untouched.

use std::collections::HashSet;

use crate::config::TransferConfig;
use crate::error::{EtlError, Result};
use crate::types::{may_coerce_to_null, ColumnType};

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
        let nullable = if force_non_nullable {
            false
        } else {
            c.nullable || may_coerce_to_null(&c.arrow)
        };
        dest_columns.push(ColumnType {
            name: dest_name,
            type_id: c.type_id,
            nullable,
            arrow: c.arrow.clone(),
            clickhouse_inner: ch_inner,
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
        }
    }

    fn typed_col(name: &str, arrow: DataType, nullable: bool) -> ColumnType {
        ColumnType {
            name: name.into(),
            type_id: 0,
            nullable,
            arrow,
            clickhouse_inner: "irrelevant".into(),
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
}
