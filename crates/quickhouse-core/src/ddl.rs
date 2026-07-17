//! ClickHouse DDL generation from a resolved source schema.

use crate::config::{SyncMode, TransferConfig};
use crate::error::{EtlError, Result};
use crate::types::ColumnType;

/// Quote/escape a ClickHouse identifier with backticks.
pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "\\`"))
}

/// Fully-qualified `db`.`table`.
pub fn qualified(db: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(db), quote_ident(table))
}

/// Build a `CREATE TABLE IF NOT EXISTS` statement for the destination.
///
/// `columns` are the *destination* columns (post-rename / post-cast), each with
/// its final ClickHouse type already resolved in `clickhouse_inner`/nullable.
pub fn create_table(
    db: &str,
    table: &str,
    columns: &[ColumnType],
    cfg: &TransferConfig,
) -> Result<String> {
    if columns.is_empty() {
        return Err(EtlError::config("cannot create a table with no columns"));
    }

    let cols_sql = columns
        .iter()
        .map(|c| format!("    {} {}", quote_ident(&c.name), c.clickhouse_type()))
        .collect::<Vec<_>>()
        .join(",\n");

    // ORDER BY: explicit order_by, else key, else all-columns tuple fallback.
    let order_cols: Vec<String> = if !cfg.order_by.is_empty() {
        cfg.order_by.clone()
    } else if !cfg.key.is_empty() {
        cfg.key.clone()
    } else {
        // No key given: ClickHouse allows `ORDER BY tuple()` for an unsorted table.
        vec![]
    };

    let order_by = if order_cols.is_empty() {
        "tuple()".to_string()
    } else {
        order_cols
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ")
    };

    // Engine. ReplacingMergeTree takes an optional version column = watermark.
    let engine = cfg.effective_engine();
    let engine_clause = if engine == "ReplacingMergeTree" {
        match (&cfg.watermark, cfg.mode) {
            (Some(w), SyncMode::Incremental) => {
                format!("ReplacingMergeTree({})", quote_ident(w))
            }
            _ => "ReplacingMergeTree".to_string(),
        }
    } else {
        engine
    };

    let mut stmt = format!(
        "CREATE TABLE IF NOT EXISTS {}\n(\n{}\n)\nENGINE = {}",
        qualified(db, table),
        cols_sql,
        engine_clause
    );

    if !cfg.primary_key.is_empty() {
        let pk = cfg
            .primary_key
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        stmt.push_str(&format!("\nPRIMARY KEY ({pk})"));
    }
    if let Some(pb) = &cfg.partition_by {
        stmt.push_str(&format!("\nPARTITION BY {pb}"));
    }
    stmt.push_str(&format!("\nORDER BY ({order_by})"));

    Ok(stmt)
}

/// DDL for the internal state table that tracks incremental watermarks.
pub fn create_state_table(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {}\n(\n\
         \x20   source_table String,\n\
         \x20   dest_table   String,\n\
         \x20   last_watermark String,\n\
         \x20   rows UInt64,\n\
         \x20   run_ts DateTime64(3) DEFAULT now64(3)\n\
         )\nENGINE = ReplacingMergeTree(run_ts)\nORDER BY (source_table, dest_table)",
        qualified(db, "_quickhouse_state")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SyncMode;
    use arrow_schema::DataType;
    use std::collections::HashMap;

    fn col(name: &str, ch: &str, nullable: bool) -> ColumnType {
        ColumnType {
            name: name.into(),
            type_id: 0,
            nullable,
            arrow: DataType::Int32,
            clickhouse_inner: ch.into(),
        }
    }

    fn base_cfg(mode: SyncMode) -> TransferConfig {
        TransferConfig {
            source_table: Some("t".into()),
            source_query: None,
            dest_table: "t".into(),
            mode,
            watermark: Some("write_date".into()),
            key: vec!["id".into()],
            create_if_missing: true,
            engine: None,
            order_by: vec![],
            partition_by: None,
            primary_key: vec![],
            parallelism: 4,
            batch_rows: 1000,
            batch_bytes: 0,
            partition_column: None,
            type_overrides: HashMap::new(),
            rename: HashMap::new(),
            include: vec![],
            exclude: vec![],
        }
    }

    #[test]
    fn full_refresh_uses_mergetree() {
        let cols = vec![col("id", "Int32", false), col("name", "String", true)];
        let sql = create_table("analytics", "t", &cols, &base_cfg(SyncMode::Full)).unwrap();
        assert!(sql.contains("ENGINE = MergeTree"));
        assert!(sql.contains("Nullable(String)"));
        assert!(sql.contains("ORDER BY (`id`)"));
    }

    #[test]
    fn incremental_uses_replacing_with_watermark() {
        let cols = vec![col("id", "Int32", false)];
        let sql = create_table("analytics", "t", &cols, &base_cfg(SyncMode::Incremental)).unwrap();
        assert!(sql.contains("ReplacingMergeTree(`write_date`)"));
    }
}
