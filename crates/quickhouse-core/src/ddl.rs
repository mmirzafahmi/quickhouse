//! ClickHouse DDL generation from a resolved source schema.

use crate::config::{SyncMode, TransferConfig};
use crate::error::{EtlError, Result};
use crate::types::ColumnType;

/// Quote/escape a ClickHouse identifier with backticks. Backslash is escaped
/// *before* the backtick — a name ending in a backslash (e.g. from
/// `rename={"col": "name\\"}`, unvalidated on the way in) would otherwise
/// escape the closing backtick instead of terminating the identifier
/// (independently verified against a real ClickHouse server: `` CREATE TABLE
/// `bad\` (...) `` fails with "Code: 62. Back quoted string is not closed").
pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}

/// Fully-qualified `db`.`table`.
pub fn qualified(db: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(db), quote_ident(table))
}

/// MergeTree variants that merge/collapse rows comparing equal on the sorting
/// key, and therefore cannot be given an empty one. Matched on the engine
/// *family* — the identifier before any parameter list — so a user-supplied
/// `"ReplacingMergeTree(ver)"` or `"ReplacingMergeTree()"` is caught as well as
/// the bare name this crate generates. `Replicated*` prefixes are handled too,
/// since they share the same merge semantics.
///
/// Deliberately excludes plain `MergeTree`: an unsorted `MergeTree` keeps every
/// row, so `ORDER BY tuple()` there is a legitimate (if unusual) choice.
const ROW_MERGING_ENGINES: [&str; 5] = [
    "ReplacingMergeTree",
    "CollapsingMergeTree",
    "VersionedCollapsingMergeTree",
    "SummingMergeTree",
    "AggregatingMergeTree",
];

/// Whether `engine`'s row-merging semantics are defined by the sorting key.
/// Case-sensitive, as ClickHouse engine names are.
fn is_row_merging_engine(engine: &str) -> bool {
    let family = engine.split('(').next().unwrap_or(engine).trim();
    let family = family.strip_prefix("Replicated").unwrap_or(family);
    ROW_MERGING_ENGINES.contains(&family)
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

    // Engine. ReplacingMergeTree takes an optional version column = watermark.
    let engine = cfg.effective_engine();

    // An empty sorting key is only safe for an engine that doesn't merge rows.
    // `ORDER BY (tuple())` is a genuinely empty sorting key, and every
    // row-merging MergeTree variant defines "which rows are the same row" as
    // "equal on the sorting key" — so with no sorting-key columns *every* row
    // in a part compares equal, and the first background merge collapses the
    // part to a single row. That's silent, permanent, destination-side data
    // loss which no `sync()` would report: the run succeeds, row counts match,
    // and the table empties itself later, asynchronously.
    //
    // This is reachable without any explicit `engine`: incremental mode
    // *defaults* to `ReplacingMergeTree` (see `effective_engine`), and `key`
    // is only mandatory for a destination that stages its incremental writes
    // (BigQuery). Refuse rather than silently picking a sorting key — an
    // invented one would just be a different silent behavior change, and the
    // caller is the only one who knows the table's real identity columns.
    if order_cols.is_empty() && is_row_merging_engine(&engine) {
        return Err(EtlError::config(format!(
            "engine {engine} needs a sorting key, but neither `key` nor `order_by` was set: \
             it deduplicates/merges rows that compare equal on the sorting key, so an empty \
             one (`ORDER BY tuple()`) makes every row equal and background merges would \
             silently collapse {table} to a single row. Set `key` (or `order_by`) to the \
             column(s) identifying a row, or pass an explicit non-merging \
             `engine` (e.g. MergeTree) if an unsorted append-only table is really what you want."
        )));
    }

    let order_by = if order_cols.is_empty() {
        "tuple()".to_string()
    } else {
        order_cols
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ")
    };

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
pub fn create_state_table(db: &str, state_table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {}\n(\n\
         \x20   source_table String,\n\
         \x20   dest_table   String,\n\
         \x20   last_watermark String,\n\
         \x20   rows UInt64,\n\
         \x20   chunk_cursor String DEFAULT '',\n\
         \x20   chunk_upper String DEFAULT '',\n\
         \x20   run_ts DateTime64(3) DEFAULT now64(3)\n\
         )\nENGINE = ReplacingMergeTree(run_ts)\nORDER BY (source_table, dest_table)",
        qualified(db, state_table)
    )
}

/// The chunk-resume columns `migrate_state_table` adds. Kept in sync with the
/// columns `create_state_table` declares — a state table carrying all of these
/// needs no migration. Used by [`state_table_needs_migration`].
pub const CHUNK_RESUME_COLUMNS: [&str; 2] = ["chunk_cursor", "chunk_upper"];

/// Whether a state table whose current columns are `existing` still needs the
/// chunk-resume migration — true iff any [`CHUNK_RESUME_COLUMNS`] is absent
/// (case-sensitive, as ClickHouse column names are).
///
/// This gate is what keeps `ensure_state_table` from re-issuing the migration
/// `ALTER` on every `sync()`. `ADD COLUMN IF NOT EXISTS` is a *schema* no-op on
/// an already-present column, but on a replicated engine it still assigns a
/// fresh metadata version each time; running it unconditionally churns a
/// cluster-wide version counter and races concurrent syncs into `517
/// CANNOT_ASSIGN_ALTER`. Since `create_state_table` already declares both
/// columns, any 0.5+ table returns `false` here and skips the `ALTER` entirely.
pub fn state_table_needs_migration(existing: &[String]) -> bool {
    CHUNK_RESUME_COLUMNS
        .iter()
        .any(|c| !existing.iter().any(|e| e == c))
}

/// Bring a pre-0.5 state table up to date with the chunk-resume columns (keyset
/// resumable reads). Idempotent via `ADD COLUMN IF NOT EXISTS`. Only worth
/// running when [`state_table_needs_migration`] is true — see its docs for why
/// running it unconditionally is harmful on a replicated engine.
pub fn migrate_state_table(db: &str, state_table: &str) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS chunk_cursor String DEFAULT '', \
         ADD COLUMN IF NOT EXISTS chunk_upper String DEFAULT ''",
        qualified(db, state_table)
    )
}

/// `ALTER TABLE ... ADD COLUMN IF NOT EXISTS <col> Nullable(<inner>)` for
/// opt-in schema evolution. Always `Nullable` regardless of the column's
/// resolved nullability: an existing table has rows that predate the column,
/// which must read back as NULL. `IF NOT EXISTS` makes it idempotent.
pub fn add_column(db: &str, table: &str, column: &ColumnType) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} Nullable({})",
        qualified(db, table),
        quote_ident(&column.name),
        column.clickhouse_inner,
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
            arbitrary_precision_decimal: false,
        }
    }

    fn base_cfg(mode: SyncMode) -> TransferConfig {
        TransferConfig {
            source_table: Some("t".into()),
            source_query: None,
            dest_table: "t".into(),
            mode,
            watermark: Some("write_date".into()),
            watermark_source_expr: None,
            lookback_seconds: 0,
            key: vec!["id".into()],
            create_if_missing: true,
            engine: None,
            order_by: vec![],
            partition_by: None,
            primary_key: vec![],
            merge_prune_partition_by: None,
            delete_stale_in_window: false,
            parallelism: 4,
            batch_rows: 1000,
            batch_bytes: 0,
            max_memory_bytes: 0,
            partition_column: None,
            read_max_rows_per_sec: None,
            chunk_rows: None,
            retry_max_attempts: 1,
            column_transforms: HashMap::new(),
            column_transform_types: HashMap::new(),
            evolve_schema: false,
            state_table_name: "_quickhouse_state".into(),
            staging_suffix: "_quickhouse_tmp".into(),
            application_name: "quickhouse".into(),
            state_key: None,
            seed_watermark: crate::config::WatermarkSeed::None,
            advance_watermark: true,
            type_overrides: HashMap::new(),
            rename: HashMap::new(),
            include: vec![],
            exclude: vec![],
            not_null: vec![],
            tinyint1_as_bool: true,
            numeric_as_decimal: None,
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

    #[test]
    fn incremental_without_key_or_order_by_is_rejected() {
        // The landmine this guards: incremental mode defaults to
        // ReplacingMergeTree, and nothing else required `key` for a ClickHouse
        // destination — so this used to emit
        // `ReplacingMergeTree(write_date) ORDER BY (tuple())`, whose first
        // background merge collapses the table to one row. Silent, permanent,
        // and reported as a successful sync.
        let cols = vec![col("id", "Int32", false), col("name", "String", true)];
        let mut cfg = base_cfg(SyncMode::Incremental);
        cfg.key = vec![];
        cfg.order_by = vec![];
        let err = create_table("analytics", "t", &cols, &cfg)
            .unwrap_err()
            .to_string();
        assert!(err.contains("sorting key"), "{err}");
        assert!(
            err.contains("key") && err.contains("order_by"),
            "must point at the two fields that fix it: {err}"
        );
    }

    #[test]
    fn no_sorting_key_is_still_allowed_for_plain_mergetree() {
        // Plain MergeTree keeps every row, so an unsorted table is a valid (if
        // unusual) choice and must not be broken by the guard above.
        let cols = vec![col("id", "Int32", false)];
        let mut cfg = base_cfg(SyncMode::Full);
        cfg.key = vec![];
        cfg.order_by = vec![];
        let sql = create_table("analytics", "t", &cols, &cfg).unwrap();
        assert!(sql.contains("ENGINE = MergeTree"), "{sql}");
        assert!(sql.contains("ORDER BY (tuple())"), "{sql}");
    }

    #[test]
    fn row_merging_engine_family_is_matched_through_params_and_replication() {
        // The guard must catch an explicitly-passed engine string too, not just
        // the bare name this crate generates — including a parameter list and
        // the Replicated* prefix (same merge semantics).
        assert!(is_row_merging_engine("ReplacingMergeTree"));
        assert!(is_row_merging_engine("ReplacingMergeTree()"));
        assert!(is_row_merging_engine("ReplacingMergeTree(ver)"));
        assert!(is_row_merging_engine(
            "ReplicatedReplacingMergeTree('/p', 'r')"
        ));
        assert!(is_row_merging_engine("SummingMergeTree"));
        assert!(is_row_merging_engine("AggregatingMergeTree"));
        assert!(is_row_merging_engine("CollapsingMergeTree(sign)"));
        assert!(is_row_merging_engine(
            "VersionedCollapsingMergeTree(sign, ver)"
        ));
        // Non-merging engines keep every row: not this guard's business.
        assert!(!is_row_merging_engine("MergeTree"));
        assert!(!is_row_merging_engine("MergeTree()"));
        assert!(!is_row_merging_engine("Log"));
    }

    #[test]
    fn explicit_replacing_engine_without_key_is_rejected_even_in_full_mode() {
        // Full-refresh mode defaults to MergeTree, but an explicit
        // ReplacingMergeTree is just as exposed — the engine, not the mode, is
        // what makes an empty sorting key destructive.
        let cols = vec![col("id", "Int32", false)];
        let mut cfg = base_cfg(SyncMode::Full);
        cfg.key = vec![];
        cfg.order_by = vec![];
        cfg.engine = Some("ReplacingMergeTree()".into());
        let err = create_table("analytics", "t", &cols, &cfg)
            .unwrap_err()
            .to_string();
        assert!(err.contains("sorting key"), "{err}");
    }

    #[test]
    fn state_table_migration_skipped_when_chunk_columns_present() {
        // A table created by `create_state_table` (0.5+) already carries both
        // chunk-resume columns, so no migration ALTER should ever fire for it.
        let full = [
            "source_table",
            "dest_table",
            "last_watermark",
            "rows",
            "chunk_cursor",
            "chunk_upper",
            "run_ts",
        ]
        .map(String::from);
        assert!(!state_table_needs_migration(&full));
    }

    #[test]
    fn state_table_migration_needed_when_chunk_columns_absent() {
        // A pre-0.5 table lacks both chunk-resume columns — migrate once.
        let pre_05 = [
            "source_table",
            "dest_table",
            "last_watermark",
            "rows",
            "run_ts",
        ]
        .map(String::from);
        assert!(state_table_needs_migration(&pre_05));

        // Case-sensitive, and a partial migration still counts as needing one.
        let one_missing = [
            "source_table",
            "dest_table",
            "last_watermark",
            "rows",
            "chunk_cursor",
            "run_ts",
        ]
        .map(String::from);
        assert!(state_table_needs_migration(&one_missing));
        assert!(state_table_needs_migration(&[
            "Chunk_Cursor".to_string(),
            "Chunk_Upper".to_string()
        ]));
    }

    #[test]
    fn quote_ident_escapes_backslash_before_backtick() {
        // Regression test: a name ending in a backslash used to escape the
        // closing backtick instead of terminating the identifier (verified
        // against a real ClickHouse server: `` CREATE TABLE `bad\` (...) ``
        // fails with "Code: 62. Back quoted string is not closed").
        assert_eq!(quote_ident(r"bad\"), r"`bad\\`");
        assert_eq!(quote_ident("has`tick"), r"`has\`tick`");
    }
}
