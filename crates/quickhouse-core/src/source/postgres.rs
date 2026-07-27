//! PostgreSQL source: schema resolution, parallel range partitioning, and
//! binary `COPY` streaming.
//!
//! Connections use rustls for TLS (matching the pure-Rust, no-OpenSSL stack
//! used elsewhere in this crate). Whether TLS is actually negotiated is
//! controlled the normal libpq way, via `sslmode` in the connection string
//! (`disable` | `prefer` (default) | `require`); the connector here just
//! makes TLS available when the server offers or requires it. The public
//! webpki-roots store is always trusted; pass `ca_cert_file` (a PEM file) to
//! additionally trust a private CA — e.g. AWS RDS's regional bundle, whose
//! certificates don't chain to any public root.

use bytes::Bytes;
use futures::Stream;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore};
use tokio_postgres::Client;
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::error::{EtlError, Result};
use crate::source::Keyset;
use crate::types::{map_oid, oid, ColumnType};

fn load_extra_ca_certs(roots: &mut RootCertStore, path: &str) -> Result<()> {
    let file = std::fs::File::open(path)
        .map_err(|e| EtlError::config(format!("failed to open ca_cert_file '{path}': {e}")))?;
    let mut reader = std::io::BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| EtlError::config(format!("failed to parse ca_cert_file '{path}': {e}")))?;
    if certs.is_empty() {
        return Err(EtlError::config(format!(
            "ca_cert_file '{path}' contained no PEM certificates"
        )));
    }
    for cert in certs {
        roots
            .add(cert)
            .map_err(|e| EtlError::config(format!("invalid CA certificate in '{path}': {e}")))?;
    }
    Ok(())
}

/// Load a client certificate chain + private key (both PEM) for mTLS.
fn load_client_identity(
    cert_path: &str,
    key_path: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let cert_file = std::fs::File::open(cert_path)
        .map_err(|e| EtlError::config(format!("failed to open client_cert_file '{cert_path}': {e}")))?;
    let certs = rustls_pemfile::certs(&mut std::io::BufReader::new(cert_file))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| EtlError::config(format!("failed to parse client_cert_file '{cert_path}': {e}")))?;
    if certs.is_empty() {
        return Err(EtlError::config(format!(
            "client_cert_file '{cert_path}' contained no PEM certificates"
        )));
    }
    let key_file = std::fs::File::open(key_path)
        .map_err(|e| EtlError::config(format!("failed to open client_key_file '{key_path}': {e}")))?;
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(key_file))
        .map_err(|e| EtlError::config(format!("failed to parse client_key_file '{key_path}': {e}")))?
        .ok_or_else(|| EtlError::config(format!("client_key_file '{key_path}' contained no private key")))?;
    Ok((certs, key))
}

fn tls_connector(
    ca_cert_file: Option<&str>,
    client_cert_file: Option<&str>,
    client_key_file: Option<&str>,
) -> Result<MakeRustlsConnect> {
    // Validate the mTLS pair up front (a pure config check, before touching the
    // crypto provider): both files, or neither.
    let client_auth = match (client_cert_file, client_key_file) {
        (Some(cert), Some(key)) => Some((cert, key)),
        (None, None) => None,
        _ => {
            return Err(EtlError::config(
                "client_cert_file and client_key_file must be provided together for mTLS",
            ))
        }
    };
    // The process-wide rustls CryptoProvider is installed once, centrally,
    // in sync::run_transfer() — see its doc comment — so every source
    // (including this one) can assume it's already selected by the time
    // any connection is attempted.
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(path) = ca_cert_file {
        load_extra_ca_certs(&mut roots, path)?;
    }
    let builder = ClientConfig::builder().with_root_certificates(roots);
    let config = match client_auth {
        Some((cert, key)) => {
            let (chain, key_der) = load_client_identity(cert, key)?;
            builder
                .with_client_auth_cert(chain, key_der)
                .map_err(|e| EtlError::config(format!("invalid client certificate/key for mTLS: {e}")))?
        }
        None => builder.with_no_client_auth(),
    };
    Ok(MakeRustlsConnect::new(config))
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
    ca_cert_file: Option<String>,
    client_cert_file: Option<String>,
    client_key_file: Option<String>,
    application_name: String,
}

impl PgSource {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dsn: impl Into<String>,
        statement_timeout_secs: u64,
        ca_cert_file: Option<String>,
        client_cert_file: Option<String>,
        client_key_file: Option<String>,
        application_name: impl Into<String>,
    ) -> Self {
        Self {
            dsn: dsn.into(),
            statement_timeout_secs,
            ca_cert_file,
            client_cert_file,
            client_key_file,
            application_name: application_name.into(),
        }
    }

    /// Open a fresh connection. Each parallel COPY stream should use its own.
    pub async fn connect(&self) -> Result<Client> {
        let tls = tls_connector(
            self.ca_cert_file.as_deref(),
            self.client_cert_file.as_deref(),
            self.client_key_file.as_deref(),
        )?;
        let (client, connection) = tokio_postgres::connect(&self.dsn, tls).await?;
        // The connection future must be driven for the client to work.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!("postgres connection error: {e}");
            }
        });
        // Session setup, run as one round-trip. `application_name` makes this
        // export visible (and killable) in `pg_stat_activity` — exactly what a
        // DBA wants when watching load from a bulk read. When a statement
        // timeout is configured we also cap `idle_in_transaction_session_timeout`
        // at the same value: defense-in-depth so a stalled connection can never
        // sit inside an open transaction holding an MVCC snapshot (which would
        // block vacuum and bloat the source). The bulk COPY itself runs in
        // autocommit, so statement_timeout is the primary guard; this covers the
        // edge/future transactional paths.
        // Single-quote-escape the (usually trivial) application_name for the
        // SQL string literal (standard_conforming_strings is on by default).
        let app = self.application_name.replace('\'', "''");
        let mut setup = format!("SET application_name = '{app}'");
        if self.statement_timeout_secs > 0 {
            let ms = self.statement_timeout_secs * 1000;
            setup.push_str(&format!("; SET statement_timeout = {ms}"));
            setup.push_str(&format!("; SET idle_in_transaction_session_timeout = {ms}"));
        }
        client.batch_execute(&setup).await?;
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
            // `c.type_().name()` is already resolved by tokio_postgres's own
            // pg_type catalog lookup during `prepare()` — a real name like
            // "point"/"interval"/"_int4", not just a numeric oid — even for
            // types this crate doesn't otherwise recognize.
            let (arrow, ch_inner) = map_oid(pg_oid).ok_or_else(|| EtlError::UnsupportedType {
                engine: "PostgreSQL",
                column: c.name().to_string(),
                type_name: c.type_().name().to_string(),
            })?;
            let nullable = !not_null.contains(c.name());
            cols.push(ColumnType {
                name: c.name().to_string(),
                type_id: pg_oid,
                nullable,
                arrow,
                clickhouse_inner: ch_inner,
                arbitrary_precision_decimal: pg_oid == oid::NUMERIC,
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

        let mut parts = super::range_partitions(lo as i128, hi as i128, n, &quote_pg(column));
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
    ///
    /// `select_exprs` is parallel to `columns`: `Some(expr)` emits
    /// `<expr> AS <col>` (a per-column value transform), `None` reads the bare
    /// column. `keyset`, when set, folds a `col > cursor` predicate into the
    /// WHERE and appends `ORDER BY col ASC LIMIT n` for chunked resumable
    /// reads. With `select_exprs` all-`None` and `keyset` `None` the output is
    /// byte-identical to a plain full-partition read.
    #[allow(clippy::too_many_arguments)]
    pub fn copy_sql(
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
                    Some(expr) => format!("{expr} AS {}", quote_pg(c)),
                    None => quote_pg(c),
                },
            )
            .collect::<Vec<_>>()
            .join(", ");

        // Merge the incremental/extra filter with the keyset cursor predicate,
        // then with the partition predicate (all AND-ed). Cursor is a bare
        // validated integer literal — no quoting.
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

        let mut inner = if let Some(q) = base_query {
            // Wrap the user query so we can apply partition/incremental filters.
            format!("SELECT {col_list} FROM ({q}) AS _src")
        } else {
            let table = from_table.expect("table or query required");
            format!("SELECT {col_list} FROM {}", quote_pg_table(table))
        };
        if let Some(f) = filters {
            inner.push_str(&format!(" WHERE {f}"));
        }
        inner.push_str(&order_limit);

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

pub(crate) fn unquote(s: &str) -> String {
    s.trim().trim_matches('"').to_string()
}

/// Double-quote a PostgreSQL identifier.
pub(crate) fn quote_pg(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote a possibly schema-qualified table name.
pub(crate) fn quote_pg_table(table: &str) -> String {
    match table.split_once('.') {
        Some((s, t)) => format!("{}.{}", quote_pg(&unquote(s)), quote_pg(&unquote(t))),
        None => quote_pg(&unquote(table)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Throwaway self-signed cert (openssl req -x509 -newkey rsa:2048 -nodes
    // -days 3650 -subj '/CN=test-ca.example.com'), used only to exercise the
    // PEM-parsing path in load_extra_ca_certs — not a real trust anchor.
    const TEST_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIDHTCCAgWgAwIBAgIUMUDUzLaOof+PJdWnEkzqRiEc4I4wDQYJKoZIhvcNAQEL
BQAwHjEcMBoGA1UEAwwTdGVzdC1jYS5leGFtcGxlLmNvbTAeFw0yNjA3MTYxNDQ0
NTBaFw0zNjA3MTMxNDQ0NTBaMB4xHDAaBgNVBAMME3Rlc3QtY2EuZXhhbXBsZS5j
b20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDHvEJUDZZsBLPfK2dz
u71v/f7jRPERouFTd4U4AChcCE34s3TrMN9sDe00wDByQ6Z7Jkn+kzi65FLgpgEa
n1STX+GHMk3XgrSs6xBtIsLtfeNH4vZakPjJ8bg+IEVkZQXMM6yFjOvQZa0llgOf
mVDHN7IUrDvc2C1Hw1cSN3rtihQ21sq6GaFgu4GlaXXpJTnJrBxKQlOfha6R2rWS
cdAfa8synygQS2lQbPPF/tuaeONN+rd9GvHO8gv3z/rtH3kAUBfTCAHAjHjwTWGu
WOYghU96KJ1crNsUu/XWnH1G9TkvFJYnZevLAsJqFx5Qg/kJirEzoIisqmQ6ex1X
ZvkxAgMBAAGjUzBRMB0GA1UdDgQWBBSzPC1pmZm3vo7hzfrOApcZPjPOEjAfBgNV
HSMEGDAWgBSzPC1pmZm3vo7hzfrOApcZPjPOEjAPBgNVHRMBAf8EBTADAQH/MA0G
CSqGSIb3DQEBCwUAA4IBAQBsq9A4fn9aKXsczE82/BJvqiyzn8yu7f+UGuX/Rr4w
5+N1WjI3HUBiZH/vowvqnmJ4fYEnfsAxH861+UUTTD2Vj0Ig+X/X+k+WnWQFOf6U
23BDGBQHPjRvg7TrnTOj1n/AWmdZceeFlRHVr6hx3/Bmf/Zp1SCnJsIA+DhuDkLi
eIK+iJw8OmcWlO/xI9krijg2p8/EZIkeKe4MlypXcp4wee0hyYZ2/RAzfa7REg45
co6xgNpuI4l7lc+dGW08eWX9SWHmV0zVh4OmXQEnqFo1yOjEZV/xuTdMEkIA57ej
OCm3XK2CW4/x+Z55ntrAffyyonL3V3vHIz7fokiz5H+l
-----END CERTIFICATE-----
";

    fn write_temp_file(contents: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "quickhouse-test-{}-{}.pem",
            std::process::id(),
            contents.len()
        ));
        std::fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn load_extra_ca_certs_accepts_valid_pem() {
        let path = write_temp_file(TEST_CERT_PEM);
        let mut roots = RootCertStore::empty();
        let before = roots.len();
        load_extra_ca_certs(&mut roots, path.to_str().unwrap()).unwrap();
        assert_eq!(roots.len(), before + 1);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn load_extra_ca_certs_rejects_garbage_file() {
        let path = write_temp_file("this is not a PEM certificate\n");
        let mut roots = RootCertStore::empty();
        assert!(load_extra_ca_certs(&mut roots, path.to_str().unwrap()).is_err());
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn load_extra_ca_certs_rejects_missing_file() {
        let mut roots = RootCertStore::empty();
        assert!(load_extra_ca_certs(&mut roots, "/no/such/file.pem").is_err());
    }

    #[test]
    fn tls_connector_requires_client_cert_and_key_together() {
        // The pair check is a pure config validation (before the crypto
        // provider is touched), so it holds even in a bare unit test.
        // `.err().unwrap()` (not `.unwrap_err()`) — the Ok type MakeRustlsConnect
        // isn't Debug, which `.unwrap_err()` would require.
        let err = tls_connector(None, Some("/x/cert.pem"), None).err().unwrap().to_string();
        assert!(err.contains("must be provided together"), "{err}");
        let err = tls_connector(None, None, Some("/x/key.pem")).err().unwrap().to_string();
        assert!(err.contains("must be provided together"), "{err}");
    }

    #[test]
    fn tls_connector_reports_a_missing_client_cert_file() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let err = tls_connector(None, Some("/no/such/cert.pem"), Some("/no/such/key.pem"))
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("client_cert_file"), "{err}");
    }

    #[test]
    fn copy_sql_with_table_and_filters() {
        let src = PgSource::new("postgresql://x", 0, None, None, None, "quickhouse");
        let part = Partition {
            label: "r0".into(),
            predicate: Some("\"id\" >= 1 AND \"id\" <= 100".into()),
        };
        let sql = src.copy_sql(
            &["id".to_string(), "name".to_string()],
            &[None, None],
            Some("public.orders"),
            None,
            &part,
            Some("\"updated_at\" > '2024-01-01'"),
            None,
        );
        assert!(sql.starts_with("COPY (SELECT \"id\", \"name\" FROM \"public\".\"orders\""));
        assert!(sql.contains("WHERE"));
        assert!(sql.ends_with("TO STDOUT (FORMAT binary)"));
        // No transform, no keyset -> no AS alias and no ORDER BY/LIMIT.
        assert!(!sql.contains(" AS "));
        assert!(!sql.contains("ORDER BY"));
    }

    #[test]
    fn copy_sql_applies_column_transform_expr() {
        let src = PgSource::new("postgresql://x", 0, None, None, None, "quickhouse");
        let part = Partition {
            label: "all".into(),
            predicate: None,
        };
        let sql = src.copy_sql(
            &["id".to_string(), "partner_id".to_string()],
            &[None, Some("CAST(\"partner_id\" AS TEXT)".to_string())],
            Some("t"),
            None,
            &part,
            None,
            None,
        );
        // Transformed column is emitted as "<expr> AS <col>"; the other is bare.
        assert!(
            sql.contains("SELECT \"id\", CAST(\"partner_id\" AS TEXT) AS \"partner_id\""),
            "{sql}"
        );
    }

    #[test]
    fn copy_sql_keyset_adds_cursor_and_order_limit() {
        let src = PgSource::new("postgresql://x", 0, None, None, None, "quickhouse");
        let part = Partition {
            label: "all".into(),
            predicate: None,
        };
        let keyset = Keyset {
            col_quoted: "\"id\"".into(),
            cursor: Some("500".into()),
            limit: 1000,
        };
        let sql = src.copy_sql(
            &["id".to_string()],
            &[None],
            Some("t"),
            None,
            &part,
            Some("\"wm\" <= '2024-01-01'"),
            Some(keyset),
        );
        assert!(sql.contains("WHERE"), "{sql}");
        assert!(
            sql.contains("\"id\" > 500"),
            "cursor predicate missing: {sql}"
        );
        assert!(
            sql.contains("\"wm\" <= '2024-01-01'"),
            "extra filter dropped: {sql}"
        );
        assert!(
            sql.contains("ORDER BY \"id\" ASC LIMIT 1000"),
            "order/limit missing: {sql}"
        );
        // First chunk (cursor=None) has ORDER BY/LIMIT but no `> cursor`.
        let first = src.copy_sql(
            &["id".to_string()],
            &[None],
            Some("t"),
            None,
            &part,
            None,
            Some(Keyset {
                col_quoted: "\"id\"".into(),
                cursor: None,
                limit: 1000,
            }),
        );
        assert!(first.contains("ORDER BY \"id\" ASC LIMIT 1000"), "{first}");
        assert!(
            !first.contains(" > "),
            "first chunk must not have a cursor predicate: {first}"
        );
    }

    #[test]
    fn qualified_split() {
        assert_eq!(
            split_qualified("public.t"),
            (Some("public".into()), "t".into())
        );
        assert_eq!(split_qualified("t"), (None, "t".into()));
    }

    #[test]
    fn quote_pg_table_doubles_embedded_double_quotes() {
        // Regression test: sync.rs's own schema-probe query used to quote
        // table names with a simpler, subtly different helper that trimmed
        // but never doubled an embedded `"` — inconsistent with this
        // function (used for the actual bulk COPY read), which correctly
        // doubles it. sync.rs now reuses this function directly instead of
        // maintaining a second, divergent implementation.
        assert_eq!(quote_pg_table("weird\"table"), "\"weird\"\"table\"");
        assert_eq!(
            quote_pg_table("public.weird\"table"),
            "\"public\".\"weird\"\"table\""
        );
    }
}
