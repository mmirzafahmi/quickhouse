//! MySQL source: schema resolution, parallel range partitioning, and
//! streaming row queries.
//!
//! Unlike PostgreSQL, `mysql_async` already exposes column nullability
//! directly via `ColumnFlags::NOT_NULL_FLAG` on the result-set metadata
//! (available even for arbitrary `source_query` values, not just base
//! tables), so there's no separate catalog lookup needed the way there is
//! for `PgSource::not_null_columns`.
//!
//! TLS uses `mysql_async`'s built-in rustls-backed `SslOpts`, trusting the
//! public CA store by default; pass `ca_cert_file` to additionally trust a
//! private CA (e.g. AWS RDS's regional bundle).

use mysql_async::consts::{ColumnFlags, ColumnType as MyType};
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder, SslOpts, Value};

use crate::error::{EtlError, Result};
use crate::source::Keyset;
use crate::types::{mysql::map_mysql_type, ColumnType};

use super::Partition;

/// Well-known MySQL wire-protocol column type codes (stable, part of the
/// client/server protocol spec) used only to classify a resolved column as
/// an integer type for range partitioning, without round-tripping back
/// through `mysql_async::consts::ColumnType`.
mod type_code {
    pub const TINY: u32 = 1;
    pub const SHORT: u32 = 2;
    pub const LONG: u32 = 3;
    pub const LONGLONG: u32 = 8;
    pub const INT24: u32 = 9;
}

fn build_opts(dsn: &str, ca_cert_file: Option<&str>, require_tls: bool) -> Result<Opts> {
    let base =
        Opts::from_url(dsn).map_err(|e| EtlError::config(format!("invalid MySQL DSN: {e}")))?;
    let mut builder = OptsBuilder::from_opts(base);
    if require_tls || ca_cert_file.is_some() {
        let mut ssl_opts = SslOpts::default();
        if let Some(path) = ca_cert_file {
            ssl_opts = ssl_opts.with_root_certs(vec![std::path::PathBuf::from(path).into()]);
        }
        builder = builder.ssl_opts(Some(ssl_opts));
    }
    Ok(builder.into())
}

pub struct MySqlSource {
    dsn: String,
    statement_timeout_secs: u64,
    ca_cert_file: Option<String>,
    require_tls: bool,
}

impl MySqlSource {
    pub fn new(
        dsn: impl Into<String>,
        statement_timeout_secs: u64,
        ca_cert_file: Option<String>,
        require_tls: bool,
    ) -> Self {
        Self {
            dsn: dsn.into(),
            statement_timeout_secs,
            ca_cert_file,
            require_tls,
        }
    }

    /// Open a fresh connection. Each parallel query stream should use its own.
    pub async fn connect(&self) -> Result<Conn> {
        let opts = build_opts(&self.dsn, self.ca_cert_file.as_deref(), self.require_tls)?;
        let mut conn = Conn::new(opts)
            .await
            .map_err(|e| EtlError::from(e).context("connecting to mysql"))?;
        if self.statement_timeout_secs > 0 {
            conn.query_drop(format!(
                "SET SESSION MAX_EXECUTION_TIME = {}",
                self.statement_timeout_secs * 1000
            ))
            .await
            .map_err(|e| EtlError::from(e).context("setting mysql session timeout"))?;
        }
        Ok(conn)
    }

    /// Resolve all output columns of `select_sql` (name, type, nullability).
    pub async fn resolve_columns(
        &self,
        conn: &mut Conn,
        select_sql: &str,
    ) -> Result<Vec<ColumnType>> {
        let stmt = conn
            .prep(select_sql)
            .await
            .map_err(|e| EtlError::from(e).context("resolving mysql columns"))?;

        let mut cols = Vec::with_capacity(stmt.columns().len());
        for c in stmt.columns() {
            let col_type = c.column_type();
            let is_unsigned = c.flags().contains(ColumnFlags::UNSIGNED_FLAG);
            let is_tinyint1 = col_type == MyType::MYSQL_TYPE_TINY && c.column_length() == 1;
            // Collation id 63 is `binary` — the only way to tell a real BLOB
            // from a TEXT column, which share the same wire type code. Without
            // this, TEXT columns map to BYTES and fail a MERGE into a BigQuery
            // STRING column.
            let is_binary = c.character_set() == 63;
            let (arrow, ch_inner) = map_mysql_type(col_type, is_unsigned, is_tinyint1, is_binary)
                .ok_or_else(|| EtlError::UnsupportedType {
                engine: "MySQL",
                column: c.name_str().to_string(),
                // ColumnType's Debug repr is its wire-protocol constant
                // name (e.g. "MYSQL_TYPE_GEOMETRY") — human-readable,
                // unlike the raw numeric code used previously.
                type_name: format!("{col_type:?}"),
            })?;
            let nullable = !c.flags().contains(ColumnFlags::NOT_NULL_FLAG);
            cols.push(ColumnType {
                name: c.name_str().to_string(),
                type_id: col_type as u8 as u32,
                nullable,
                arrow,
                clickhouse_inner: ch_inner,
                arbitrary_precision_decimal: matches!(
                    col_type,
                    MyType::MYSQL_TYPE_DECIMAL | MyType::MYSQL_TYPE_NEWDECIMAL
                ),
            });
        }
        Ok(cols)
    }

    /// Compute range partitions over `column` for a base table. Falls back to
    /// a single partition when the column isn't an integer type or has no rows.
    pub async fn range_partitions(
        &self,
        conn: &mut Conn,
        table: &str,
        column: &str,
        column_type_id: u32,
        n: usize,
        column_nullable: bool,
    ) -> Result<Vec<Partition>> {
        let single = || {
            vec![Partition {
                label: "all".into(),
                predicate: None,
            }]
        };

        let is_int = matches!(
            column_type_id,
            type_code::TINY
                | type_code::SHORT
                | type_code::INT24
                | type_code::LONG
                | type_code::LONGLONG
        );
        if n <= 1 || !is_int {
            return Ok(single());
        }

        let sql = format!(
            "SELECT MIN({c}), MAX({c}) FROM {t}",
            c = quote_my(column),
            t = quote_my_table(table),
        );
        // Decode as the raw wire `Value` rather than `i64` directly: a
        // BIGINT UNSIGNED column can legitimately hold values above
        // i64::MAX (up to u64::MAX), and MySQL reports those as
        // `Value::UInt`, not `Value::Int` — decoding straight to `i64` would
        // fail the whole probe (and abort the entire transfer) instead of
        // gracefully falling back to a single partition like every other
        // non-partitionable case below.
        let row: Option<(Option<Value>, Option<Value>)> = conn
            .query_first(sql)
            .await
            .map_err(|e| EtlError::from(e).context("computing mysql partition bounds"))?;
        let as_i128 = |v: Value| match v {
            Value::Int(i) => Some(i as i128),
            Value::UInt(u) => Some(u as i128),
            _ => None,
        };
        let (lo, hi) = match row {
            Some((Some(lo), Some(hi))) => match (as_i128(lo), as_i128(hi)) {
                (Some(lo), Some(hi)) if hi >= lo => (lo, hi),
                _ => return Ok(single()),
            },
            _ => return Ok(single()),
        };

        let mut parts = super::range_partitions(lo, hi, n, &quote_my(column));
        if column_nullable {
            parts.push(Partition {
                label: "null-key".into(),
                predicate: Some(format!("{} IS NULL", quote_my(column))),
            });
        }
        Ok(parts)
    }

    /// Build the `SELECT ...` SQL for one partition. See `PgSource::copy_sql`
    /// for the `select_exprs` (per-column transform) and `keyset` (chunked
    /// resumable read) semantics — identical here. Byte-identical to the plain
    /// query when `select_exprs` is all-`None` and `keyset` is `None`.
    #[allow(clippy::too_many_arguments)]
    pub fn select_sql(
        &self,
        columns: &[String],
        select_exprs: &[Option<String>],
        from_table: Option<&str>,
        base_query: Option<&str>,
        partition: &Partition,
        extra_filter: Option<&str>,
        keyset: Option<Keyset>,
    ) -> String {
        let col_list = columns
            .iter()
            .enumerate()
            .map(
                |(i, c)| match select_exprs.get(i).and_then(|e| e.as_ref()) {
                    Some(expr) => format!("{expr} AS {}", quote_my(c)),
                    None => quote_my(c),
                },
            )
            .collect::<Vec<_>>()
            .join(", ");

        let cursor_pred = keyset.as_ref().and_then(|k| {
            k.cursor
                .as_ref()
                .map(|cur| format!("{} > {}", k.col_quoted, cur))
        });
        let extra_owned = extra_filter.map(str::to_string);
        let extra_and_cursor = combine_filters(&extra_owned, cursor_pred.as_deref());
        let filters = combine_filters(&partition.predicate, extra_and_cursor.as_deref());
        let order_limit = keyset
            .as_ref()
            .map(|k| format!(" ORDER BY {} ASC LIMIT {}", k.col_quoted, k.limit))
            .unwrap_or_default();

        let mut sql = if let Some(q) = base_query {
            format!("SELECT {col_list} FROM ({q}) AS _src")
        } else {
            let table = from_table.expect("table or query required");
            format!("SELECT {col_list} FROM {}", quote_my_table(table))
        };
        if let Some(f) = filters {
            sql.push_str(&format!(" WHERE {f}"));
        }
        sql.push_str(&order_limit);
        sql
    }

    /// Read the current max watermark value as text (for incremental sync).
    pub async fn max_watermark(
        &self,
        conn: &mut Conn,
        from_table: Option<&str>,
        base_query: Option<&str>,
        watermark: &str,
    ) -> Result<Option<String>> {
        let sql = if let Some(q) = base_query {
            format!(
                "SELECT CAST(MAX({w}) AS CHAR) FROM ({q}) AS _src",
                w = quote_my(watermark)
            )
        } else {
            format!(
                "SELECT CAST(MAX({w}) AS CHAR) FROM {t}",
                w = quote_my(watermark),
                t = quote_my_table(from_table.expect("table required"))
            )
        };
        // MAX() over an empty table (or an all-NULL column) returns one row
        // whose value is SQL NULL — must be requested as `Option<String>`,
        // not `String`, or mysql_common panics converting NULL to a bare
        // String instead of returning `None` (see FromRow documentation).
        conn.query_first::<Option<String>, _>(sql)
            .await
            .map(|row| row.flatten())
            .map_err(|e| EtlError::from(e).context("reading mysql max watermark"))
    }
}

fn combine_filters(a: &Option<String>, b: Option<&str>) -> Option<String> {
    match (a, b) {
        (Some(a), Some(b)) => Some(format!("({a}) AND ({b})")),
        (Some(a), None) => Some(a.clone()),
        (None, Some(b)) => Some(b.to_string()),
        (None, None) => None,
    }
}

/// Backtick-quote a MySQL identifier.
pub(crate) fn quote_my(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Quote a possibly schema(database)-qualified table name.
pub(crate) fn quote_my_table(table: &str) -> String {
    match table.split_once('.') {
        Some((s, t)) => format!(
            "{}.{}",
            quote_my(s.trim().trim_matches('`')),
            quote_my(t.trim().trim_matches('`'))
        ),
        None => quote_my(table.trim().trim_matches('`')),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_sql_with_table_and_filters() {
        let src = MySqlSource::new("mysql://x", 0, None, false);
        let part = Partition {
            label: "r0".into(),
            predicate: Some("`id` >= 1 AND `id` <= 100".into()),
        };
        let sql = src.select_sql(
            &["id".to_string(), "name".to_string()],
            &[None, None],
            Some("mydb.orders"),
            None,
            &part,
            Some("`updated_at` > '2024-01-01'"),
            None,
        );
        assert!(sql.starts_with("SELECT `id`, `name` FROM `mydb`.`orders`"));
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("AND"));
        assert!(!sql.contains(" AS "));
        assert!(!sql.contains("ORDER BY"));
    }

    #[test]
    fn select_sql_with_query() {
        let src = MySqlSource::new("mysql://x", 0, None, false);
        let part = Partition {
            label: "all".into(),
            predicate: None,
        };
        let sql = src.select_sql(
            &["a".to_string()],
            &[None],
            None,
            Some("SELECT a FROM t"),
            &part,
            None,
            None,
        );
        assert_eq!(sql, "SELECT `a` FROM (SELECT a FROM t) AS _src");
    }

    #[test]
    fn select_sql_transform_and_keyset() {
        let src = MySqlSource::new("mysql://x", 0, None, false);
        let part = Partition {
            label: "all".into(),
            predicate: None,
        };
        let sql = src.select_sql(
            &["id".to_string(), "amt".to_string()],
            &[None, Some("ROUND(`amt`, 9)".to_string())],
            Some("t"),
            None,
            &part,
            None,
            Some(Keyset {
                col_quoted: "`id`".into(),
                cursor: Some("42".into()),
                limit: 500,
            }),
        );
        assert!(sql.contains("ROUND(`amt`, 9) AS `amt`"), "{sql}");
        assert!(sql.contains("`id` > 42"), "{sql}");
        assert!(sql.ends_with("ORDER BY `id` ASC LIMIT 500"), "{sql}");
    }

    #[test]
    fn quote_table_handles_schema_qualification() {
        assert_eq!(quote_my_table("db.t"), "`db`.`t`");
        assert_eq!(quote_my_table("t"), "`t`");
    }
}
