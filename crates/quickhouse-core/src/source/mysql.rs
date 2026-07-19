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
use mysql_async::{Conn, Opts, OptsBuilder, SslOpts};

use crate::error::{EtlError, Result};
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
    let base = Opts::from_url(dsn)
        .map_err(|e| EtlError::config(format!("invalid MySQL DSN: {e}")))?;
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
            .map_err(|e| EtlError::other(format!("mysql connect error: {e}")))?;
        if self.statement_timeout_secs > 0 {
            conn.query_drop(format!(
                "SET SESSION MAX_EXECUTION_TIME = {}",
                self.statement_timeout_secs * 1000
            ))
            .await
            .map_err(|e| EtlError::other(format!("mysql error: {e}")))?;
        }
        Ok(conn)
    }

    /// Resolve all output columns of `select_sql` (name, type, nullability).
    pub async fn resolve_columns(&self, conn: &mut Conn, select_sql: &str) -> Result<Vec<ColumnType>> {
        let stmt = conn
            .prep(select_sql)
            .await
            .map_err(|e| EtlError::other(format!("mysql prepare error: {e}")))?;

        let mut cols = Vec::with_capacity(stmt.columns().len());
        for c in stmt.columns() {
            let col_type = c.column_type();
            let is_unsigned = c.flags().contains(ColumnFlags::UNSIGNED_FLAG);
            let is_tinyint1 = col_type == MyType::MYSQL_TYPE_TINY && c.column_length() == 1;
            let (arrow, ch_inner) = map_mysql_type(col_type, is_unsigned, is_tinyint1)
                .ok_or_else(|| EtlError::UnsupportedType {
                    oid: col_type as u8 as u32,
                    context: c.name_str().to_string(),
                })?;
            let nullable = !c.flags().contains(ColumnFlags::NOT_NULL_FLAG);
            cols.push(ColumnType {
                name: c.name_str().to_string(),
                type_id: col_type as u8 as u32,
                nullable,
                arrow,
                clickhouse_inner: ch_inner,
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
            type_code::TINY | type_code::SHORT | type_code::INT24 | type_code::LONG | type_code::LONGLONG
        );
        if n <= 1 || !is_int {
            return Ok(single());
        }

        let sql = format!(
            "SELECT MIN({c}), MAX({c}) FROM {t}",
            c = quote_my(column),
            t = quote_my_table(table),
        );
        let row: Option<(Option<i64>, Option<i64>)> = conn
            .query_first(sql)
            .await
            .map_err(|e| EtlError::other(format!("mysql error: {e}")))?;
        let (lo, hi) = match row {
            Some((Some(lo), Some(hi))) if hi >= lo => (lo, hi),
            _ => return Ok(single()),
        };

        let span = (hi - lo + 1) as u128;
        let n = n as u128;
        let step = span.div_ceil(n).max(1);

        let mut parts = Vec::new();
        let mut start = lo as i128;
        let mut idx = 0u128;
        while (start as i64) <= hi {
            let end = (start + step as i128 - 1).min(hi as i128);
            let pred = format!("{c} >= {start} AND {c} <= {end}", c = quote_my(column));
            parts.push(Partition {
                label: format!("range-{idx}"),
                predicate: Some(pred),
            });
            start = end + 1;
            idx += 1;
        }
        if column_nullable {
            parts.push(Partition {
                label: "null-key".into(),
                predicate: Some(format!("{} IS NULL", quote_my(column))),
            });
        }
        Ok(parts)
    }

    /// Build the `SELECT ...` SQL for one partition.
    pub fn select_sql(
        &self,
        columns: &[String],
        from_table: Option<&str>,
        base_query: Option<&str>,
        partition: &Partition,
        extra_filter: Option<&str>,
    ) -> String {
        let col_list = columns
            .iter()
            .map(|c| quote_my(c))
            .collect::<Vec<_>>()
            .join(", ");

        if let Some(q) = base_query {
            let mut sql = format!("SELECT {col_list} FROM ({q}) AS _src");
            if let Some(f) = combine_filters(&partition.predicate, extra_filter) {
                sql.push_str(&format!(" WHERE {f}"));
            }
            sql
        } else {
            let table = from_table.expect("table or query required");
            let mut sql = format!("SELECT {col_list} FROM {}", quote_my_table(table));
            if let Some(f) = combine_filters(&partition.predicate, extra_filter) {
                sql.push_str(&format!(" WHERE {f}"));
            }
            sql
        }
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
            .map_err(|e| EtlError::other(format!("mysql error: {e}")))
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
            Some("mydb.orders"),
            None,
            &part,
            Some("`updated_at` > '2024-01-01'"),
        );
        assert!(sql.starts_with("SELECT `id`, `name` FROM `mydb`.`orders`"));
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("AND"));
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
            None,
            Some("SELECT a FROM t"),
            &part,
            None,
        );
        assert_eq!(sql, "SELECT `a` FROM (SELECT a FROM t) AS _src");
    }

    #[test]
    fn quote_table_handles_schema_qualification() {
        assert_eq!(quote_my_table("db.t"), "`db`.`t`");
        assert_eq!(quote_my_table("t"), "`t`");
    }
}
