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
use crate::types::ColumnType;

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
        let nullable = c.nullable && !key_columns.contains(dest_name.as_str());
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

    fn cfg() -> TransferConfig {
        TransferConfig {
            source_table: Some("t".into()),
            source_query: None,
            dest_table: "t".into(),
            mode: crate::config::SyncMode::Full,
            watermark: None,
            key: vec![],
            create_if_missing: true,
            engine: None,
            order_by: vec![],
            partition_by: None,
            primary_key: vec![],
            parallelism: 1,
            batch_rows: 1,
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
