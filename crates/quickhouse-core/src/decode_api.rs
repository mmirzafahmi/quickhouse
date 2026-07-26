//! Declared-schema decoder for the HTTP API sources (CleverTap, AppsFlyer).
//!
//! Unlike the DB sources there is no catalog to resolve a schema from, so the
//! user *declares* each output column (`ApiColumn`: name + BigQuery type +
//! optional dotted path). This module turns that declaration into a
//! `ColumnType` list (the destination schema) and decodes each incoming record
//! — always a `serde_json::Value` (the AppsFlyer client converts its CSV report
//! into flat JSON objects keyed by header, so there is one decode path) — into
//! Arrow `RecordBatch`es, coercing each declared column's value from JSON/text
//! to its declared type.
//!
//! Coercion policy (matching the crate's "degrade, don't abort" stance): a
//! missing key or JSON null → NULL (not counted); an unparseable scalar → NULL
//! plus the matching counter (`invalid_scalars`/`invalid_dates`/`invalid_decimals`).
//! Every declared column is Nullable, since external data routinely omits keys.
//! The destination is BigQuery-only, whose DATE/TIMESTAMP range spans
//! 0001–9999, so — unlike `decode_bigquery` — there is no `ch_range` clamp.

use std::borrow::Cow;
use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, Int64Builder,
    StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::types::{Decimal128Type, DecimalType};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use base64::Engine;
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use serde_json::Value;

use crate::config::ApiColumn;
use crate::decimal::{parse_decimal_text, rescale_mantissa, DecimalText};
use crate::error::{EtlError, Result};
use crate::types::bigquery::{canonical_bq_type_name, map_type, parse_bq_type_name};
use crate::types::ColumnType;

/// What happened while coercing one declared value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApiCoercion {
    /// Value present and converted, or absent/null (either way, not counted).
    None,
    /// A non-empty scalar (int/float/bool/bytes) failed to parse -> NULL.
    Scalar,
    /// A date/timestamp value failed to parse -> NULL.
    Date,
    /// A NUMERIC value overflowed / failed to parse -> NULL.
    Decimal,
}

/// Resolve the declared `ApiColumn`s into a destination `ColumnType` schema.
/// Every column is Nullable; `NUMERIC` is promoted to exact `Decimal128(38,9)`
/// so declared numerics decode exactly (BIGNUMERIC stays Float64 — needs
/// Decimal256; documented). Rejects an empty list, duplicate names, and
/// unknown/parameterized type names.
pub fn resolve_api_columns(cols: &[ApiColumn]) -> Result<Vec<ColumnType>> {
    use crate::types::bigquery::type_id as id;
    if cols.is_empty() {
        return Err(EtlError::config(
            "an API source needs at least one declared column (name + BigQuery type)",
        ));
    }
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::with_capacity(cols.len());
    for c in cols {
        if !seen.insert(c.name.as_str()) {
            return Err(EtlError::config(format!(
                "duplicate declared column name '{}'",
                c.name
            )));
        }
        let ft = parse_bq_type_name(&c.name, &c.bq_type)?;
        let (type_id, arrow, ch_inner) = map_type(&ft).ok_or_else(|| {
            EtlError::config(format!("column '{}': type '{}' is not supported", c.name, c.bq_type))
        })?;
        // Exact decimal for NUMERIC (parity with the DB sources' NUMERIC path).
        let (arrow, apd) = match type_id {
            id::NUMERIC => (DataType::Decimal128(38, 9), true),
            id::BIGNUMERIC => (arrow, true),
            _ => (arrow, false),
        };
        out.push(ColumnType {
            name: c.name.clone(),
            type_id,
            nullable: true,
            arrow,
            clickhouse_inner: ch_inner,
            arbitrary_precision_decimal: apd,
        });
    }
    Ok(out)
}

/// The canonical BigQuery type name for a declared column — used to seed
/// `type_overrides` so the destination table gets the exact declared type.
pub fn canonical_declared_type(c: &ApiColumn) -> Result<&'static str> {
    Ok(canonical_bq_type_name(&parse_bq_type_name(&c.name, &c.bq_type)?))
}

enum ColBuilder {
    Bool(BooleanBuilder),
    I64(Int64Builder),
    F64(Float64Builder),
    Str(StringBuilder),
    Bin(BinaryBuilder),
    Date(Date32Builder),
    Ts(TimestampMicrosecondBuilder, Option<Arc<str>>),
    Decimal128(Decimal128Builder, u8, i8),
}

impl ColBuilder {
    fn new(dt: &DataType) -> Result<Self> {
        Ok(match dt {
            DataType::Boolean => ColBuilder::Bool(BooleanBuilder::new()),
            DataType::Int64 => ColBuilder::I64(Int64Builder::new()),
            DataType::Float64 => ColBuilder::F64(Float64Builder::new()),
            DataType::Utf8 => ColBuilder::Str(StringBuilder::new()),
            DataType::Binary => ColBuilder::Bin(BinaryBuilder::new()),
            DataType::Date32 => ColBuilder::Date(Date32Builder::new()),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                ColBuilder::Ts(TimestampMicrosecondBuilder::new(), tz.clone())
            }
            DataType::Decimal128(p, s) => {
                ColBuilder::Decimal128(Decimal128Builder::new().with_precision_and_scale(*p, *s)?, *p, *s)
            }
            other => {
                return Err(EtlError::internal(format!(
                    "no API column builder for Arrow type {other:?}"
                )))
            }
        })
    }

    fn append_null(&mut self) {
        match self {
            ColBuilder::Bool(b) => b.append_null(),
            ColBuilder::I64(b) => b.append_null(),
            ColBuilder::F64(b) => b.append_null(),
            ColBuilder::Str(b) => b.append_null(),
            ColBuilder::Bin(b) => b.append_null(),
            ColBuilder::Date(b) => b.append_null(),
            ColBuilder::Ts(b, _) => b.append_null(),
            ColBuilder::Decimal128(b, _, _) => b.append_null(),
        }
    }

    /// Coerce `text` into this column, returning `(approx_bytes, coercion)`.
    /// `None` text (missing/null) -> NULL, not counted.
    fn append_text(&mut self, text: Option<&str>) -> (usize, ApiCoercion) {
        let s = match text {
            None => {
                self.append_null();
                return (0, ApiCoercion::None);
            }
            Some(s) => s,
        };
        let n = s.len();
        match self {
            // STRING / JSON / TIME: append verbatim (empty string is a value).
            ColBuilder::Str(b) => {
                b.append_value(s);
                (n, ApiCoercion::None)
            }
            ColBuilder::Bin(b) => match base64::engine::general_purpose::STANDARD.decode(s) {
                Ok(bytes) => {
                    b.append_value(&bytes);
                    (n, ApiCoercion::None)
                }
                Err(_) => {
                    b.append_null();
                    (n, ApiCoercion::Scalar)
                }
            },
            ColBuilder::I64(b) => {
                if s.trim().is_empty() {
                    b.append_null();
                    return (0, ApiCoercion::None);
                }
                match s.trim().parse::<i64>() {
                    Ok(v) => {
                        b.append_value(v);
                        (8, ApiCoercion::None)
                    }
                    Err(_) => {
                        b.append_null();
                        (n, ApiCoercion::Scalar)
                    }
                }
            }
            ColBuilder::F64(b) => {
                if s.trim().is_empty() {
                    b.append_null();
                    return (0, ApiCoercion::None);
                }
                match s.trim().parse::<f64>() {
                    Ok(v) => {
                        b.append_value(v);
                        (8, ApiCoercion::None)
                    }
                    Err(_) => {
                        b.append_null();
                        (n, ApiCoercion::Scalar)
                    }
                }
            }
            ColBuilder::Bool(b) => {
                let t = s.trim().to_ascii_lowercase();
                if t.is_empty() {
                    b.append_null();
                    return (0, ApiCoercion::None);
                }
                match t.as_str() {
                    "true" | "1" | "yes" | "t" => {
                        b.append_value(true);
                        (1, ApiCoercion::None)
                    }
                    "false" | "0" | "no" | "f" => {
                        b.append_value(false);
                        (1, ApiCoercion::None)
                    }
                    _ => {
                        b.append_null();
                        (n, ApiCoercion::Scalar)
                    }
                }
            }
            ColBuilder::Date(b) => {
                if s.trim().is_empty() {
                    b.append_null();
                    return (0, ApiCoercion::None);
                }
                match parse_date_days(s.trim()) {
                    Some(days) => {
                        b.append_value(days);
                        (4, ApiCoercion::None)
                    }
                    None => {
                        b.append_null();
                        (n, ApiCoercion::Date)
                    }
                }
            }
            ColBuilder::Ts(b, _) => {
                if s.trim().is_empty() {
                    b.append_null();
                    return (0, ApiCoercion::None);
                }
                match parse_ts_micros(s.trim()) {
                    Some(micros) => {
                        b.append_value(micros);
                        (8, ApiCoercion::None)
                    }
                    None => {
                        b.append_null();
                        (n, ApiCoercion::Date)
                    }
                }
            }
            ColBuilder::Decimal128(b, p, s2) => {
                if s.trim().is_empty() {
                    b.append_null();
                    return (0, ApiCoercion::None);
                }
                // Untrusted source: a parse error nulls the cell (unlike
                // decode_bigquery, which trusts BigQuery's own text and errors).
                match parse_decimal_text(s.trim()) {
                    Ok(DecimalText::Ok { negative, magnitude, scale }) => {
                        match rescale_mantissa(magnitude, scale, *s2 as i32) {
                            Some(m) => {
                                let signed = if negative { -m } else { m };
                                if Decimal128Type::is_valid_decimal_precision(signed, *p) {
                                    b.append_value(signed);
                                    (n, ApiCoercion::None)
                                } else {
                                    b.append_null();
                                    (n, ApiCoercion::Decimal)
                                }
                            }
                            None => {
                                b.append_null();
                                (n, ApiCoercion::Decimal)
                            }
                        }
                    }
                    Ok(DecimalText::MagnitudeOverflow) | Err(_) => {
                        b.append_null();
                        (n, ApiCoercion::Decimal)
                    }
                }
            }
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            ColBuilder::Bool(b) => Arc::new(b.finish()),
            ColBuilder::I64(b) => Arc::new(b.finish()),
            ColBuilder::F64(b) => Arc::new(b.finish()),
            ColBuilder::Str(b) => Arc::new(b.finish()),
            ColBuilder::Bin(b) => Arc::new(b.finish()),
            ColBuilder::Date(b) => Arc::new(b.finish()),
            ColBuilder::Ts(b, tz) => {
                let arr = b.finish();
                match tz {
                    Some(tz) => Arc::new(arr.with_timezone(tz.clone())),
                    None => Arc::new(arr),
                }
            }
            ColBuilder::Decimal128(b, _, _) => Arc::new(b.finish()),
        }
    }
}

/// Walk a dotted path (`["profile","identity"]`) through a JSON object; missing
/// key or a non-object mid-walk -> `None`.
fn resolve_path<'a>(v: &'a Value, segs: &[String]) -> Option<&'a Value> {
    let mut cur = v;
    for seg in segs {
        cur = cur.as_object()?.get(seg)?;
    }
    Some(cur)
}

/// Flatten a JSON value to the text a scalar/coercion expects. `Null` -> `None`;
/// a nested Array/Object -> its compact JSON text (so it can land in a STRING or
/// JSON column).
fn json_scalar_text(v: &Value) -> Option<Cow<'_, str>> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(Cow::Borrowed(s)),
        Value::Bool(b) => Some(Cow::Borrowed(if *b { "true" } else { "false" })),
        Value::Number(n) => Some(Cow::Owned(n.to_string())),
        Value::Array(_) | Value::Object(_) => Some(Cow::Owned(v.to_string())),
    }
}

/// Parse a `YYYY-MM-DD` (or integer epoch-seconds) date into Date32 days-from-epoch.
fn parse_date_days(s: &str) -> Option<i32> {
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
        return Some(d.signed_duration_since(epoch).num_days() as i32);
    }
    // Integer epoch seconds -> day.
    if let Ok(secs) = s.parse::<i64>() {
        return Some((secs.div_euclid(86_400)) as i32);
    }
    None
}

/// Parse a timestamp into UTC epoch microseconds. Accepts RFC3339, a
/// `YYYY-MM-DD HH:MM:SS` civil string (interpreted as UTC), and integer epoch
/// **seconds** (CleverTap's `ts` is seconds, not millis).
fn parse_ts_micros(s: &str) -> Option<i64> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_micros());
    }
    if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(ndt.and_utc().timestamp_micros());
    }
    if let Ok(secs) = s.parse::<i64>() {
        return secs.checked_mul(1_000_000);
    }
    None
}

/// Batches declared-schema records (`serde_json::Value`) into `RecordBatch`es.
pub struct ApiBatcher {
    schema: SchemaRef,
    builders: Vec<ColBuilder>,
    /// Pre-split dotted lookup path per column (aligned to `builders`).
    paths: Vec<Vec<String>>,
    batch_rows: usize,
    batch_bytes: usize,
    rows_in_batch: usize,
    bytes_in_batch: usize,
    pub rows_total: u64,
    pub invalid_scalars_total: u64,
    pub invalid_dates_total: u64,
    pub invalid_decimals_total: u64,
}

impl ApiBatcher {
    /// `dest_columns` is the resolved schema; `lookups[i]` is the dotted source
    /// path for column `i` (aligned by index).
    pub fn new(dest_columns: &[ColumnType], lookups: &[String], batch_rows: usize, batch_bytes: usize) -> Result<Self> {
        let fields: Vec<Field> = dest_columns
            .iter()
            .map(|c| Field::new(&c.name, c.arrow.clone(), true))
            .collect();
        let mut builders = Vec::with_capacity(dest_columns.len());
        for c in dest_columns {
            builders.push(ColBuilder::new(&c.arrow)?);
        }
        let paths = lookups
            .iter()
            .map(|l| l.split('.').map(str::to_string).collect())
            .collect();
        Ok(Self {
            schema: Arc::new(Schema::new(fields)),
            builders,
            paths,
            batch_rows,
            batch_bytes,
            rows_in_batch: 0,
            bytes_in_batch: 0,
            rows_total: 0,
            invalid_scalars_total: 0,
            invalid_dates_total: 0,
            invalid_decimals_total: 0,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Append one record; returns a flushed batch if the batch limit was reached.
    pub fn append_record(&mut self, rec: &Value) -> Result<Option<RecordBatch>> {
        let mut row_bytes = 0usize;
        for (i, builder) in self.builders.iter_mut().enumerate() {
            let text = resolve_path(rec, &self.paths[i]).and_then(json_scalar_text);
            let (size, coercion) = builder.append_text(text.as_deref());
            row_bytes += size;
            match coercion {
                ApiCoercion::None => {}
                ApiCoercion::Scalar => self.invalid_scalars_total += 1,
                ApiCoercion::Date => self.invalid_dates_total += 1,
                ApiCoercion::Decimal => self.invalid_decimals_total += 1,
            }
        }
        self.rows_in_batch += 1;
        self.rows_total += 1;
        self.bytes_in_batch += row_bytes;
        if self.rows_in_batch >= self.batch_rows
            || (self.batch_bytes > 0 && self.bytes_in_batch >= self.batch_bytes)
        {
            Ok(Some(self.flush_batch()?))
        } else {
            Ok(None)
        }
    }

    pub fn finish(&mut self) -> Result<Option<RecordBatch>> {
        if self.rows_in_batch > 0 {
            Ok(Some(self.flush_batch()?))
        } else {
            Ok(None)
        }
    }

    fn flush_batch(&mut self) -> Result<RecordBatch> {
        let cols: Vec<ArrayRef> = self.builders.iter_mut().map(|b| b.finish()).collect();
        self.rows_in_batch = 0;
        self.bytes_in_batch = 0;
        RecordBatch::try_new(self.schema.clone(), cols).map_err(EtlError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Decimal128Array, Int64Array, StringArray, TimestampMicrosecondArray};

    fn cols() -> Vec<ApiColumn> {
        vec![
            ApiColumn { name: "id".into(), bq_type: "INTEGER".into(), path: None },
            ApiColumn { name: "email".into(), bq_type: "STRING".into(), path: Some("profile.email".into()) },
            ApiColumn { name: "amount".into(), bq_type: "NUMERIC".into(), path: Some("event_props.amount".into()) },
            ApiColumn { name: "ts".into(), bq_type: "TIMESTAMP".into(), path: None },
        ]
    }

    fn batcher() -> (ApiBatcher, Vec<ColumnType>) {
        let cs = cols();
        let dest = resolve_api_columns(&cs).unwrap();
        let lookups: Vec<String> = cs.iter().map(|c| c.path.clone().unwrap_or_else(|| c.name.clone())).collect();
        (ApiBatcher::new(&dest, &lookups, 10, 0).unwrap(), dest)
    }

    #[test]
    fn resolver_maps_declared_types_and_forces_numeric_decimal() {
        let dest = resolve_api_columns(&cols()).unwrap();
        assert_eq!(dest[0].arrow, DataType::Int64);
        assert_eq!(dest[2].arrow, DataType::Decimal128(38, 9), "NUMERIC -> exact Decimal128(38,9)");
        assert!(dest[2].arbitrary_precision_decimal);
        assert!(dest.iter().all(|c| c.nullable), "every declared column is nullable");
    }

    #[test]
    fn resolver_rejects_empty_dup_and_unknown() {
        assert!(resolve_api_columns(&[]).is_err());
        let dup = vec![
            ApiColumn { name: "a".into(), bq_type: "STRING".into(), path: None },
            ApiColumn { name: "a".into(), bq_type: "STRING".into(), path: None },
        ];
        assert!(resolve_api_columns(&dup).unwrap_err().to_string().contains("duplicate"));
        let bad = vec![ApiColumn { name: "x".into(), bq_type: "RECORD".into(), path: None }];
        assert!(resolve_api_columns(&bad).is_err());
    }

    #[test]
    fn decodes_paths_types_and_coercions() {
        let (mut b, _) = batcher();
        // Well-formed record.
        let r1 = serde_json::json!({
            "id": 5, "ts": 1_700_000_000,
            "profile": {"email": "a@b.co"},
            "event_props": {"amount": "12.50"}
        });
        assert!(b.append_record(&r1).unwrap().is_none());
        // Missing paths + unparseable int -> nulls; bad int counted.
        let r2 = serde_json::json!({ "id": "not-an-int", "ts": null });
        assert!(b.append_record(&r2).unwrap().is_none());
        let batch = b.finish().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.value(0), 5);
        assert!(ids.is_null(1), "unparseable int -> null");
        assert_eq!(b.invalid_scalars_total, 1);
        let email = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(email.value(0), "a@b.co");
        assert!(email.is_null(1), "missing dotted path -> null (not counted)");
        let amt = batch.column(2).as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(amt.value(0), 12_500_000_000, "12.50 @ scale 9 exact");
        let ts = batch.column(3).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        assert_eq!(ts.value(0), 1_700_000_000_000_000, "epoch SECONDS -> micros *1e6");
        assert!(ts.is_null(1));
    }

    #[test]
    fn missing_and_null_do_not_count_as_errors() {
        let (mut b, _) = batcher();
        b.append_record(&serde_json::json!({})).unwrap();
        b.finish().unwrap();
        assert_eq!(b.invalid_scalars_total, 0);
        assert_eq!(b.invalid_dates_total, 0);
        assert_eq!(b.invalid_decimals_total, 0);
    }

    #[test]
    fn numeric_overflow_nulls_and_counts() {
        let cs = vec![ApiColumn { name: "n".into(), bq_type: "NUMERIC".into(), path: None }];
        let dest = resolve_api_columns(&cs).unwrap();
        let mut b = ApiBatcher::new(&dest, &["n".to_string()], 10, 0).unwrap();
        // 30 integer digits > NUMERIC(38,9)'s 29-digit integer capacity.
        b.append_record(&serde_json::json!({"n": "123456789012345678901234567890"})).unwrap();
        b.finish().unwrap();
        assert_eq!(b.invalid_decimals_total, 1);
    }
}
