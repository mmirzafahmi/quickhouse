//! PostgreSQL source: schema resolution, parallel range partitioning, and
//! binary `COPY` streaming.
//!
//! Connections use rustls for TLS (matching the pure-Rust, no-OpenSSL stack
//! used elsewhere in this crate). Whether TLS is actually negotiated is
//! controlled the normal libpq way, via `sslmode` in the connection string
//! (`disable` | `prefer` (default) | `require`); the connector here just
//! makes TLS available when the server offers or requires it. Only
//! publicly-trusted CA certificates are supported for now (via
//! `webpki-roots`) — no custom CA bundles or client-cert (mTLS) auth yet.

use bytes::Bytes;
use futures::Stream;
use rustls::{ClientConfig, RootCertStore};
use tokio_postgres::Client;
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::error::{EtlError, Result};
use crate::types::{map_oid, oid, ColumnType};

fn tls_connector() -> MakeRustlsConnect {
    // Ignore the error: it just means some other crate in the process (e.g.
    // reqwest) already installed a default crypto provider, which is fine.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    MakeRustlsConnect::new(config)
}

/// A unit of parallel work: an optional `WHERE` predicate over the source.
#[derive(Debug, Clone)]
pub struct Partition {
    pub label: String,
    /// Predicate without the `WHERE` keyword; `None` = whole table.
    pub predicate: Option<String>,
}

pub struct PgSource {
    dsn: String,
    statement_timeout_secs: u64,
}

impl PgSource {
    pub fn new(dsn: impl Into<String>, statement_timeout_secs: u64) -> Self {
        Self {
            dsn: dsn.into(),
            statement_timeout_secs,
        }
    }

    /// Open a fresh connection. Each parallel COPY stream should use its own.
    pub async fn connect(&self) -> Result<Client> {
        let (client, connection) = tokio_postgres::connect(&self.dsn, tls_connector()).await?;
        // The connection future must be driven for the client to work.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!("postgres connection error: {e}");
            }
        });
        if self.statement_timeout_secs > 0 {
            client
                .batch_execute(&format!(
                    "SET statement_timeout = {}",
                    self.statement_timeout_secs * 1000
                ))
                .await?;
        }
        Ok(client)
    }

    /// Resolve all output columns of `select_sql` (name, OID, nullability, types).
    ///
    /// `base_table` — when the source is a plain table — is used to look up
    /// NOT NULL constraints; for arbitrary queries columns default to nullable.
    pub async fn resolve_columns(
        &self,
        client: &Client,
        select_sql: &str,
        base_table: Option<&str>,
    ) -> Result<Vec<ColumnType>> {
        let stmt = client.prepare(select_sql).await?;

        let not_null = match base_table {
            Some(t) => self.not_null_columns(client, t).await.unwrap_or_default(),
            None => Default::default(),
        };

        let mut cols = Vec::with_capacity(stmt.columns().len());
        for c in stmt.columns() {
            let pg_oid = c.type_().oid();
            let (arrow, ch_inner) = map_oid(pg_oid).ok_or_else(|| EtlError::UnsupportedType {
                oid: pg_oid,
                context: c.name().to_string(),
            })?;
            let nullable = !not_null.contains(&c.name().to_string());
            cols.push(ColumnType {
                name: c.name().to_string(),
                pg_oid,
                nullable,
                arrow,
                clickhouse_inner: ch_inner,
            });
        }
        Ok(cols)
    }

    /// Names of NOT NULL columns for a (optionally schema-qualified) table.
    async fn not_null_columns(
        &self,
        client: &Client,
        table: &str,
    ) -> Result<std::collections::HashSet<String>> {
        let (schema, name) = split_qualified(table);
        let rows = client
            .query(
                "SELECT a.attname
                   FROM pg_attribute a
                   JOIN pg_class c ON c.oid = a.attrelid
                   JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE a.attnum > 0 AND NOT a.attisdropped
                    AND a.attnotnull
                    AND c.relname = $1
                    AND ($2::text IS NULL OR n.nspname = $2)",
                &[&name, &schema],
            )
            .await?;
        Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
    }

    /// Compute range partitions over `column` for a base table. Falls back to a
    /// single partition when the column is not an integer or has no rows.
    pub async fn range_partitions(
        &self,
        client: &Client,
        table: &str,
        column: &str,
        column_oid: u32,
        n: usize,
        column_nullable: bool,
    ) -> Result<Vec<Partition>> {
        let single = || {
            vec![Partition {
                label: "all".into(),
                predicate: None,
            }]
        };

        let is_int = matches!(column_oid, oid::INT2 | oid::INT4 | oid::INT8);
        if n <= 1 || !is_int {
            return Ok(single());
        }

        let row = client
            .query_one(
                &format!(
                    "SELECT min({c})::bigint, max({c})::bigint FROM {t}",
                    c = quote_pg(column),
                    t = quote_pg_table(table),
                ),
                &[],
            )
            .await?;
        let lo: Option<i64> = row.get(0);
        let hi: Option<i64> = row.get(1);
        let (lo, hi) = match (lo, hi) {
            (Some(lo), Some(hi)) if hi >= lo => (lo, hi),
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
            let pred = format!(
                "{c} >= {start} AND {c} <= {end}",
                c = quote_pg(column),
            );
            parts.push(Partition {
                label: format!("range-{idx}"),
                predicate: Some(pred),
            });
            start = end + 1;
            idx += 1;
        }
        // Rows whose partition key is NULL would be skipped by range predicates.
        if column_nullable {
            parts.push(Partition {
                label: "null-key".into(),
                predicate: Some(format!("{} IS NULL", quote_pg(column))),
            });
        }
        Ok(parts)
    }

    /// Build the `COPY (...) TO STDOUT (FORMAT binary)` SQL for one partition.
    pub fn copy_sql(
        &self,
        columns: &[String],
        from_table: Option<&str>,
        base_query: Option<&str>,
        partition: &Partition,
        extra_filter: Option<&str>,
    ) -> String {
        let col_list = columns
            .iter()
            .map(|c| quote_pg(c))
            .collect::<Vec<_>>()
            .join(", ");

        let inner = if let Some(q) = base_query {
            // Wrap the user query so we can apply partition/incremental filters.
            let mut sql = format!("SELECT {col_list} FROM ({q}) AS _src");
            let filters = combine_filters(&partition.predicate, extra_filter);
            if let Some(f) = filters {
                sql.push_str(&format!(" WHERE {f}"));
            }
            sql
        } else {
            let table = from_table.expect("table or query required");
            let mut sql = format!("SELECT {col_list} FROM {}", quote_pg_table(table));
            let filters = combine_filters(&partition.predicate, extra_filter);
            if let Some(f) = filters {
                sql.push_str(&format!(" WHERE {f}"));
            }
            sql
        };

        format!("COPY ({inner}) TO STDOUT (FORMAT binary)")
    }

    /// Start a binary COPY and return the raw byte stream.
    pub async fn copy_stream(
        &self,
        client: &Client,
        copy_sql: &str,
    ) -> Result<impl Stream<Item = std::result::Result<Bytes, tokio_postgres::Error>>> {
        let stream = client.copy_out(copy_sql).await?;
        Ok(stream)
    }

    /// Read the current max watermark value as text (for incremental sync).
    pub async fn max_watermark(
        &self,
        client: &Client,
        from_table: Option<&str>,
        base_query: Option<&str>,
        watermark: &str,
    ) -> Result<Option<String>> {
        let sql = if let Some(q) = base_query {
            format!(
                "SELECT max({w})::text FROM ({q}) AS _src",
                w = quote_pg(watermark)
            )
        } else {
            format!(
                "SELECT max({w})::text FROM {t}",
                w = quote_pg(watermark),
                t = quote_pg_table(from_table.expect("table required"))
            )
        };
        let row = client.query_one(&sql, &[]).await?;
        Ok(row.get::<_, Option<String>>(0))
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

/// Split `schema.table` into `(Some(schema), table)`, or `(None, table)`.
fn split_qualified(table: &str) -> (Option<String>, String) {
    match table.split_once('.') {
        Some((s, t)) => (Some(unquote(s)), unquote(t)),
        None => (None, unquote(table)),
    }
}

fn unquote(s: &str) -> String {
    s.trim().trim_matches('"').to_string()
}

/// Double-quote a PostgreSQL identifier.
fn quote_pg(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote a possibly schema-qualified table name.
fn quote_pg_table(table: &str) -> String {
    match table.split_once('.') {
        Some((s, t)) => format!("{}.{}", quote_pg(&unquote(s)), quote_pg(&unquote(t))),
        None => quote_pg(&unquote(table)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_sql_with_table_and_filters() {
        let src = PgSource::new("postgresql://x", 0);
        let part = Partition {
            label: "r0".into(),
            predicate: Some("\"id\" >= 1 AND \"id\" <= 100".into()),
        };
        let sql = src.copy_sql(
            &["id".to_string(), "name".to_string()],
            Some("public.account_move_line"),
            None,
            &part,
            Some("\"write_date\" > '2024-01-01'"),
        );
        assert!(sql.starts_with("COPY (SELECT \"id\", \"name\" FROM \"public\".\"account_move_line\""));
        assert!(sql.contains("WHERE"));
        assert!(sql.ends_with("TO STDOUT (FORMAT binary)"));
    }

    #[test]
    fn qualified_split() {
        assert_eq!(split_qualified("public.t"), (Some("public".into()), "t".into()));
        assert_eq!(split_qualified("t"), (None, "t".into()));
    }
}
