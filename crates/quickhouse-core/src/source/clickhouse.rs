//! ClickHouse source: schema resolution via `DESCRIBE`, parallel range
//! partitioning, and streaming reads over the HTTP interface in
//! `FORMAT ArrowStream`.
//!
//! **Why there is no hand-written decoder for this source.** Postgres, MySQL
//! and BigQuery each need one (`decode.rs`, `decode_mysql.rs`,
//! `decode_bigquery.rs`) because their wire formats are their own. ClickHouse's
//! isn't: it will hand back Apache Arrow IPC directly, the exact shape the rest
//! of this crate already moves rows in, so the read path is
//! `StreamDecoder::decode` over the response body (see
//! [`crate::decode_clickhouse`]) and nothing else.
//!
//! What makes that safe is the SELECT this module builds. ClickHouse's Arrow
//! *output* mapping is not the identity on the types you'd expect — `Date`
//! comes out as `UINT16`, `DateTime` as `UINT32`, `Enum8` as its backing
//! `INT8`, `FixedString` as `FIXED_SIZE_BINARY`, and a `DateTime64(P)`'s time
//! unit follows `P` — so every projected column is wrapped in a `CAST` to the
//! ClickHouse type whose Arrow output *is* the destination column's Arrow type
//! (see [`crate::types::clickhouse::arrow_to_ch_cast_type`]). The cast is keyed
//! off the **destination** column, so `type_overrides`, `numeric_as_decimal`
//! and `column_transform_types` are honoured with no extra machinery. The cast
//! is emitted unconditionally rather than only where the types differ: an
//! identity `CAST` is free in ClickHouse's expression analyzer, and making it
//! conditional would mean trusting `clickhouse_inner` to still describe the
//! *source* type after an override has rewritten it — which is exactly when it
//! doesn't.
//!
//! Authentication and settings mirror the ClickHouse *sink* (`X-ClickHouse-User`
//! / `X-ClickHouse-Key` headers, caller settings as query parameters), so one
//! `quickhouse.ClickHouse(...)` descriptor works in either role.

use std::collections::BTreeMap;

use reqwest::{Client, RequestBuilder, Response};

use crate::config::ClickHouseSourceConfig;
use crate::ddl::quote_ident;
use crate::error::{EtlError, Result};
use crate::source::Keyset;
use crate::types::clickhouse::{arrow_to_ch_cast_type, map_ch_type};
use crate::types::ColumnType;

use super::Partition;

/// Settings every read this source issues sends, unless the caller's own
/// `settings` override them by name.
///
/// `output_format_arrow_string_as_string` is the load-bearing one: with it off
/// (the default on older servers) ClickHouse writes `String` columns as Arrow
/// `BINARY`, and every text column would arrive needing a client-side cast.
/// The next two switch off encodings the decoder would otherwise have to
/// understand — a dictionary-encoded `LowCardinality` column and a fixed-width
/// `FixedString` — neither of which survives the `CAST` this module emits
/// anyway, so they are turned off at the source rather than unwound here.
///
/// The compression method is pinned rather than left to the server's default,
/// which has changed across ClickHouse releases: it decides which optional
/// `arrow-ipc` codec the decoder must have compiled in, and a mismatch is a
/// hard failure on the first buffer (`"lz4 IPC decompression requires the lz4
/// feature"`), not a fallback. `lz4_frame` is both the current server default
/// and the cheapest of the three — it is why this source sends no
/// `enable_http_compression`, since compressing the buffers twice would only
/// cost CPU.
fn arrow_output_settings() -> [(&'static str, &'static str); 4] {
    [
        ("output_format_arrow_string_as_string", "1"),
        ("output_format_arrow_low_cardinality_as_dictionary", "0"),
        ("output_format_arrow_fixed_string_as_fixed_byte_array", "0"),
        ("output_format_arrow_compression_method", "lz4_frame"),
    ]
}

pub struct ClickHouseSource {
    client: Client,
    url: String,
    database: String,
    user: String,
    password: String,
    settings: BTreeMap<String, String>,
    statement_timeout_secs: u64,
}

impl ClickHouseSource {
    pub fn new(cfg: &ClickHouseSourceConfig) -> Result<Self> {
        let client = Client::builder().build().map_err(EtlError::from)?;
        Ok(Self {
            client,
            url: cfg.url.clone(),
            database: cfg.database.clone(),
            user: cfg.user.clone(),
            password: cfg.password.clone(),
            settings: cfg.settings.clone(),
            statement_timeout_secs: cfg.statement_timeout_secs,
        })
    }

    /// A POST request carrying auth, the database, and the effective settings.
    /// `extra` holds this call site's own settings; the caller's configured
    /// `settings` are applied *last* so an explicit value always wins over one
    /// quickhouse picked (the sink follows the same rule).
    fn request(&self, extra: &[(&str, String)]) -> RequestBuilder {
        let mut params: BTreeMap<String, String> = BTreeMap::new();
        params.insert("database".to_string(), self.database.clone());
        if self.statement_timeout_secs > 0 {
            params.insert(
                "max_execution_time".to_string(),
                self.statement_timeout_secs.to_string(),
            );
        }
        for (k, v) in extra {
            params.insert((*k).to_string(), v.clone());
        }
        for (k, v) in &self.settings {
            params.insert(k.clone(), v.clone());
        }
        self.client
            .post(&self.url)
            .header("X-ClickHouse-User", &self.user)
            .header("X-ClickHouse-Key", &self.password)
            .query(&params.iter().collect::<Vec<_>>())
    }

    async fn check(resp: Response) -> Result<Response> {
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let text = resp.text().await.unwrap_or_default();
        Err(EtlError::clickhouse(format!("HTTP {status}: {text}")))
    }

    /// Run `sql` and return the raw `TabSeparated` body.
    async fn query_tsv(&self, sql: &str) -> Result<String> {
        let resp = self.request(&[]).body(sql.to_string()).send().await?;
        let resp = Self::check(resp).await?;
        resp.text().await.map_err(EtlError::from)
    }

    /// Run `sql` and return its rows as already-unescaped `TabSeparated` fields.
    async fn query_rows(&self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        let body = self.query_tsv(sql).await?;
        Ok(body
            .lines()
            .filter(|l| !l.is_empty())
            .map(|line| line.split('\t').map(tsv_field).collect())
            .collect())
    }

    /// Resolve every output column of the read (name, ClickHouse type,
    /// nullability) with a single `DESCRIBE`.
    ///
    /// The probe always goes through a subquery — `DESCRIBE (SELECT * FROM t)`
    /// rather than `DESCRIBE TABLE t` — so the resolved column list is exactly
    /// what the read's own `SELECT` will project. `DESCRIBE TABLE` additionally
    /// reports `MATERIALIZED`, `ALIAS` and `EPHEMERAL` columns, which `SELECT *`
    /// does not return; resolving those would leave the destination with
    /// columns no row ever fills.
    ///
    /// `include`/`exclude` are consulted for one purpose only: an unmappable
    /// column the transfer was never going to carry is dropped here instead of
    /// failing the run. `EtlError::UnsupportedType`'s message offers
    /// `exclude=["col"]` as the workaround, and without this that advice is
    /// simply false — column selection happens in `transform::plan`, long after
    /// schema resolution has already errored. ClickHouse makes this matter in a
    /// way the other engines don't: `Array`, `Map`, `Tuple` and `JSON` columns
    /// are ordinary in a real schema, so "read this table except its two array
    /// columns" is a routine ask rather than an exotic one. A *supported*
    /// column is never dropped here even when filtered out, so nothing
    /// downstream that inspects the resolved schema (the watermark and
    /// partition-key lookups) changes behaviour.
    pub async fn resolve_columns(
        &self,
        schema_probe: &str,
        include: &[String],
        exclude: &[String],
    ) -> Result<Vec<ColumnType>> {
        let sql = format!("DESCRIBE ({schema_probe}) FORMAT TabSeparated");
        let rows = self.query_rows(&sql).await?;
        let mut cols = Vec::with_capacity(rows.len());
        for row in rows {
            let name = row
                .first()
                .cloned()
                .flatten()
                .ok_or_else(|| EtlError::clickhouse("DESCRIBE returned a row with no name"))?;
            let declared = row.get(1).cloned().flatten().unwrap_or_default();
            let Some(mapped) = map_ch_type(&declared) else {
                let transferred =
                    (include.is_empty() || include.contains(&name)) && !exclude.contains(&name);
                if !transferred {
                    tracing::debug!(
                        "skipping unsupported column '{name}' ({declared}): not in the \
                         transferred set"
                    );
                    continue;
                }
                return Err(EtlError::UnsupportedType {
                    engine: "ClickHouse",
                    column: name,
                    type_name: declared,
                });
            };
            cols.push(ColumnType {
                name,
                type_id: mapped.type_id,
                nullable: mapped.nullable,
                arrow: mapped.arrow,
                clickhouse_inner: mapped.clickhouse_inner,
                arbitrary_precision_decimal: mapped.arbitrary_precision_decimal,
            });
        }
        if cols.is_empty() {
            return Err(EtlError::clickhouse(
                "DESCRIBE resolved no columns for the source read",
            ));
        }
        Ok(cols)
    }

    /// Compute range partitions over `column`, either for a base table
    /// (`from_table`) or a wrapped `source_query` (`base_query`). Falls back to
    /// a single partition when the column isn't an integer type, or the source
    /// is empty. Mirrors [`super::mysql::MySqlSource::range_partitions`],
    /// including the `source_expr` override — see
    /// `TransferConfig::partition_source_expr`.
    ///
    /// The bounds come back as **text** and are parsed into `i128` here, not
    /// read as a typed integer: a ClickHouse `UInt64` key legitimately holds
    /// values above `i64::MAX`, and the whole point of `range_partitions`
    /// taking `i128` is that such a bound still partitions exactly instead of
    /// wrapping.
    #[allow(clippy::too_many_arguments)]
    pub async fn range_partitions(
        &self,
        from_table: Option<&str>,
        base_query: Option<&str>,
        column: &str,
        source_expr: Option<&str>,
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

        // With an explicit `source_expr` the type gate is the caller's job (the
        // expression need not be a projected column we have a type for), and a
        // non-integer there is a hard config error rather than a quiet fallback.
        if n <= 1
            || (source_expr.is_none()
                && !crate::types::clickhouse::is_range_partitionable(column_type_id))
        {
            return Ok(single());
        }

        let key = source_expr
            .map(str::to_string)
            .unwrap_or_else(|| quote_ident(column));
        // `count()` rides along because ClickHouse's `min`/`max` over zero rows
        // return the column type's *default* (0, or the epoch) rather than
        // NULL, so an empty source would otherwise look like the single-valued
        // range `[0, 0]` instead of "nothing to partition".
        let sql = format!(
            "SELECT toString(min({key})), toString(max({key})), toString(count()) \
             FROM {src} FORMAT TabSeparated",
            src = from_source(from_table, base_query),
        );
        let rows = self.query_rows(&sql).await.map_err(|e| match source_expr {
            // A bad `partition_source_expr` surfaces here as an opaque SQL
            // error; name the knob so the fix is obvious.
            Some(expr) => EtlError::config(format!(
                "partition_source_expr='{expr}' could not be probed for a MIN/MAX range: {e}. \
                 It must be a raw SQL expression over a column source_query projects, and it \
                 must resolve to an integer type."
            )),
            None => e.context("computing clickhouse partition bounds"),
        })?;
        let Some(row) = rows.first() else {
            return Ok(single());
        };
        let parse = |i: usize| -> Option<i128> {
            row.get(i)?.as_deref().and_then(|s| s.trim().parse().ok())
        };
        if parse(2).unwrap_or(0) == 0 {
            return Ok(single());
        }
        let (lo, hi) = match (parse(0), parse(1)) {
            (Some(lo), Some(hi)) if hi >= lo => (lo, hi),
            _ => return Ok(single()),
        };

        let mut parts = super::range_partitions(lo, hi, n, &key);
        if column_nullable {
            parts.push(Partition {
                label: "null-key".into(),
                predicate: Some(format!("{key} IS NULL")),
            });
        }
        Ok(parts)
    }

    /// Build the `SELECT ...` for one partition.
    ///
    /// `dest_columns` is `SelectPlan::dest_columns`, positionally parallel to
    /// `columns`/`select_exprs`; it supplies the `CAST` target that pins each
    /// column's Arrow output type (see the module docs). `select_exprs` carries
    /// `column_transforms`, and the cast wraps the transform rather than
    /// replacing it.
    #[allow(clippy::too_many_arguments)]
    pub fn select_sql(
        &self,
        columns: &[String],
        select_exprs: &[Option<String>],
        dest_columns: &[ColumnType],
        from_table: Option<&str>,
        base_query: Option<&str>,
        partition: &Partition,
        extra_filter: Option<&str>,
        keyset: Option<Keyset>,
    ) -> String {
        let col_list = columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let base = match select_exprs.get(i).and_then(|e| e.as_ref()) {
                    Some(expr) => expr.clone(),
                    None => quote_ident(c),
                };
                let projected = match dest_columns.get(i) {
                    Some(d) => cast_expr(&base, d),
                    // Unreachable in practice (the two lists are built together
                    // in `transform::plan`); reading the column verbatim is the
                    // honest fallback, and `decode_clickhouse` still adapts it.
                    None => base,
                };
                format!("{projected} AS {}", quote_ident(c))
            })
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

        let mut sql = format!(
            "SELECT {col_list} FROM {}",
            from_source(from_table, base_query)
        );
        if let Some(f) = filters {
            sql.push_str(&format!(" WHERE {f}"));
        }
        sql.push_str(&order_limit);
        sql
    }

    /// Stream one `SELECT`'s rows as an Arrow IPC stream. The caller feeds the
    /// response body to [`crate::decode_clickhouse::ChArrowDecoder`].
    ///
    /// `max_block_size` is how the caller's `batch_rows` reaches this source:
    /// ClickHouse decides the Arrow record-batch boundaries server-side, so the
    /// knob has to be pushed down rather than applied on the way out.
    pub async fn stream_arrow(&self, sql: &str, max_block_size: usize) -> Result<Response> {
        let mut extra: Vec<(&str, String)> = arrow_output_settings()
            .iter()
            .map(|(k, v)| (*k, (*v).to_string()))
            .collect();
        if max_block_size > 0 {
            extra.push(("max_block_size", max_block_size.to_string()));
        }
        let body = format!("{sql} FORMAT ArrowStream");
        let resp = self.request(&extra).body(body).send().await?;
        Self::check(resp).await
    }

    /// Read the current max watermark value as text (for incremental sync).
    ///
    /// `source_expr`, when set, replaces the bare `watermark` column — see
    /// `TransferConfig::watermark_source_expr` and
    /// `build_watermark_filter_clickhouse`, which this is kept in lockstep with.
    ///
    /// The `count(...) = 0` guard is not decoration: ClickHouse's `max()` over
    /// zero matching rows returns the column type's default value (the epoch,
    /// or `0`) rather than NULL, so without it an empty source would persist a
    /// 1970 watermark as though it had genuinely read up to there.
    pub async fn max_watermark(
        &self,
        from_table: Option<&str>,
        base_query: Option<&str>,
        watermark: &str,
        source_expr: Option<&str>,
    ) -> Result<Option<String>> {
        let w = source_expr
            .map(str::to_string)
            .unwrap_or_else(|| quote_ident(watermark));
        let sql = format!(
            "SELECT if(count({w}) = 0, NULL, toString(max({w}))) FROM {src} FORMAT TabSeparated",
            src = from_source(from_table, base_query),
        );
        let rows = self
            .query_rows(&sql)
            .await
            .map_err(|e| e.context("reading clickhouse max watermark"))?;
        Ok(rows.first().and_then(|r| r.first().cloned()).flatten())
    }

    /// Count rows whose watermark value is NULL — see
    /// `PgSource::count_null_watermark` for why this matters.
    pub async fn count_null_watermark(
        &self,
        from_table: Option<&str>,
        base_query: Option<&str>,
        watermark: &str,
        source_expr: Option<&str>,
    ) -> Result<i64> {
        let w = source_expr
            .map(str::to_string)
            .unwrap_or_else(|| quote_ident(watermark));
        let sql = format!(
            "SELECT toString(count()) FROM {src} WHERE {w} IS NULL FORMAT TabSeparated",
            src = from_source(from_table, base_query),
        );
        let rows = self
            .query_rows(&sql)
            .await
            .map_err(|e| e.context("reading clickhouse null-watermark count"))?;
        Ok(rows
            .first()
            .and_then(|r| r.first().cloned())
            .flatten()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0))
    }

    /// Every distinct non-NULL value of `key`, as text, over the rows `window`
    /// admits — the ClickHouse half of a `reconcile::reconcile_keys` diff. See
    /// `PgSource::distinct_keys` for why the values come back as text.
    pub async fn distinct_keys(
        &self,
        from_table: Option<&str>,
        base_query: Option<&str>,
        key: &str,
        window: Option<&str>,
    ) -> Result<Vec<String>> {
        let k = quote_ident(key);
        let where_sql = match window {
            Some(w) => format!("WHERE ({w}) AND {k} IS NOT NULL"),
            None => format!("WHERE {k} IS NOT NULL"),
        };
        let sql = format!(
            "SELECT DISTINCT toString({k}) FROM {src} {where_sql} FORMAT TabSeparated",
            src = from_source(from_table, base_query),
        );
        let rows = self
            .query_rows(&sql)
            .await
            .map_err(|e| e.context("reading source keyset"))?;
        Ok(rows
            .into_iter()
            .filter_map(|mut r| if r.is_empty() { None } else { r.swap_remove(0) })
            .collect())
    }
}

/// The `FROM` clause body: a wrapped `source_query` when present, else the
/// quoted table. `source_query` wins, matching every other source.
fn from_source(from_table: Option<&str>, base_query: Option<&str>) -> String {
    match base_query {
        Some(q) => format!("({q}) AS _src"),
        None => quote_ch_table(from_table.expect("table or query required")),
    }
}

/// Wrap `base` in the `CAST` that pins its Arrow output type to `dest`'s — see
/// this module's docs. The cast target is wrapped in `Nullable(...)` exactly
/// when the destination column is nullable, so a NULL survives the cast
/// (ClickHouse refuses to cast NULL to a non-Nullable type) and a column the
/// plan resolved as NOT NULL still fails loudly if it turns out to hold one.
fn cast_expr(base: &str, dest: &ColumnType) -> String {
    match arrow_to_ch_cast_type(&dest.arrow) {
        Some(t) if dest.nullable => format!("CAST({base} AS Nullable({t}))"),
        Some(t) => format!("CAST({base} AS {t})"),
        // An Arrow type with no ClickHouse spelling: read it as-is and let the
        // decoder's own cast deal with whatever arrives.
        None => base.to_string(),
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

/// Quote a possibly database-qualified table name.
pub(crate) fn quote_ch_table(table: &str) -> String {
    match table.split_once('.') {
        Some((d, t)) => format!(
            "{}.{}",
            quote_ident(d.trim().trim_matches('`')),
            quote_ident(t.trim().trim_matches('`'))
        ),
        None => quote_ident(table.trim().trim_matches('`')),
    }
}

/// Decode one `TabSeparated` field. `\N` is SQL NULL (not the two-character
/// string), and the format escapes tab, newline, carriage return and backslash
/// itself — so a `String` column holding a newline arrives as `\n` and has to
/// be put back, or it would silently truncate the value at the escape.
fn tsv_field(raw: &str) -> Option<String> {
    if raw == "\\N" {
        return None;
    }
    if !raw.contains('\\') {
        return Some(raw.to_string());
    }
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('r') => out.push('\r'),
            Some('0') => out.push('\0'),
            Some('\\') => out.push('\\'),
            Some('\'') => out.push('\''),
            // Not an escape ClickHouse emits; keep both characters rather than
            // silently eating the backslash.
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, TimeUnit};

    fn col(name: &str, arrow: DataType, nullable: bool) -> ColumnType {
        ColumnType {
            name: name.to_string(),
            type_id: 0,
            nullable,
            arrow,
            clickhouse_inner: "String".to_string(),
            arbitrary_precision_decimal: false,
        }
    }

    fn source() -> ClickHouseSource {
        ClickHouseSource::new(&ClickHouseSourceConfig {
            url: "http://localhost:8123".into(),
            database: "default".into(),
            user: "default".into(),
            password: String::new(),
            settings: Default::default(),
            statement_timeout_secs: 0,
        })
        .unwrap()
    }

    #[test]
    fn select_sql_casts_every_column_to_its_destination_type() {
        let src = source();
        let part = Partition {
            label: "r0".into(),
            predicate: Some("`id` >= 1 AND `id` <= 100".into()),
        };
        let sql = src.select_sql(
            &["id".to_string(), "seen_at".to_string()],
            &[None, None],
            &[
                col("id", DataType::Int64, false),
                col(
                    "seen_at",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
            ],
            Some("shop.orders"),
            None,
            &part,
            Some("`updated_at` > '2024-01-01'"),
            None,
        );
        assert!(
            sql.starts_with("SELECT CAST(`id` AS Int64) AS `id`, "),
            "{sql}"
        );
        // A nullable destination column casts to Nullable(...), or ClickHouse
        // would refuse the NULL outright.
        assert!(
            sql.contains("CAST(`seen_at` AS Nullable(DateTime64(6, 'UTC'))) AS `seen_at`"),
            "{sql}"
        );
        assert!(sql.contains("FROM `shop`.`orders` WHERE"), "{sql}");
        assert!(!sql.contains("ORDER BY"), "{sql}");
    }

    #[test]
    fn select_sql_wraps_a_column_transform_rather_than_dropping_it() {
        let src = source();
        let part = Partition {
            label: "all".into(),
            predicate: None,
        };
        let sql = src.select_sql(
            &["amt".to_string()],
            &[Some("round(`amt`, 2)".to_string())],
            &[col("amt", DataType::Float64, false)],
            None,
            Some("SELECT amt FROM t"),
            &part,
            None,
            Some(Keyset {
                col_quoted: "`id`".into(),
                cursor: Some("42".into()),
                limit: 500,
            }),
        );
        assert!(
            sql.contains("CAST(round(`amt`, 2) AS Float64) AS `amt`"),
            "{sql}"
        );
        assert!(sql.contains("FROM (SELECT amt FROM t) AS _src"), "{sql}");
        assert!(sql.contains("WHERE `id` > 42"), "{sql}");
        assert!(sql.ends_with("ORDER BY `id` ASC LIMIT 500"), "{sql}");
    }

    #[test]
    fn quote_table_handles_database_qualification() {
        assert_eq!(quote_ch_table("db.t"), "`db`.`t`");
        assert_eq!(quote_ch_table("t"), "`t`");
    }

    #[test]
    fn tsv_field_decodes_nulls_and_escapes() {
        assert_eq!(tsv_field("\\N"), None);
        // The literal two-character string "\N" is not how a NULL is spelled
        // in a value position, but an escaped backslash followed by N is.
        assert_eq!(tsv_field("\\\\N").as_deref(), Some("\\N"));
        assert_eq!(tsv_field("plain").as_deref(), Some("plain"));
        assert_eq!(tsv_field("a\\tb").as_deref(), Some("a\tb"));
        assert_eq!(tsv_field("line\\nbreak").as_deref(), Some("line\nbreak"));
        assert_eq!(tsv_field("").as_deref(), Some(""));
        // An unknown escape keeps both characters rather than eating one.
        assert_eq!(tsv_field("\\q").as_deref(), Some("\\q"));
    }
}
