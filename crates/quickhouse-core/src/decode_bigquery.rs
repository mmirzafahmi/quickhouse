//! Batcher: BigQuery Storage Read API rows (`storage::row::Row`) into Arrow
//! `RecordBatch`es.
//!
//! The wire format is genuinely Arrow under the hood, but the
//! `google-cloud-bigquery` crate's public `Row` type decodes it into
//! individual typed-by-index values (discarding the original columnar
//! batch), so — like `decode_mysql.rs` — we rebuild our own `RecordBatch`
//! from those values rather than getting one directly.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, Int64Builder,
    StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::types::{Decimal128Type, DecimalType};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use google_cloud_bigquery::storage::row::Row;

use crate::decimal::{parse_decimal_text, rescale_mantissa, Coercion, DecimalText};
use crate::error::{EtlError, Result};
use crate::types::bigquery::type_id as id;
use crate::types::{ch_range, ColumnType};

/// A column's value didn't convert to the Rust type its resolved Arrow column
/// expects — only reachable if the schema resolved from BigQuery's table
/// metadata disagrees with what the Storage Read API actually sends for that
/// column, a mapping/decoder mismatch rather than a value a source row itself
/// could hold.
fn conv_err(e: impl std::fmt::Display) -> EtlError {
    EtlError::internal(format!("bigquery row decode error: {e}"))
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
            DataType::Decimal128(p, s) => ColBuilder::Decimal128(
                Decimal128Builder::new().with_precision_and_scale(*p, *s)?,
                *p,
                *s,
            ),
            other => {
                // Reachable only if types.rs maps some BigQuery field type to
                // an Arrow type this decoder doesn't implement a builder for.
                return Err(EtlError::internal(format!(
                    "no column builder for Arrow type {other:?}"
                )));
            }
        })
    }

    /// Appends the value and returns `(approx_size_bytes, coercion)`. The
    /// size is used only to decide when to flush a batch (not exact memory
    /// accounting); [`Coercion::DateRange`] is returned for a valid
    /// date/datetime whose year is outside ClickHouse's representable window
    /// ([`ch_range`]) and was nulled rather than sent on to be rejected at
    /// insert time — BigQuery's DATE range (0001..=9999) is far wider than
    /// ClickHouse's, so this is reachable. [`Coercion::DecimalOverflow`] is
    /// returned for a NUMERIC/BIGNUMERIC value overridden to an exact
    /// `Decimal(P,S)` that doesn't fit `P`.
    fn append_from_row(
        &mut self,
        row: &Row,
        index: usize,
        type_id: u32,
    ) -> Result<(usize, Coercion)> {
        let mut coercion = Coercion::None;
        let size = match (&mut *self, type_id) {
            (ColBuilder::Bool(b), t) if t == id::BOOLEAN => {
                match row.column::<Option<bool>>(index).map_err(conv_err)? {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
                1
            }
            (ColBuilder::I64(b), t) if t == id::INTEGER => {
                match row.column::<Option<i64>>(index).map_err(conv_err)? {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
                8
            }
            (ColBuilder::F64(b), t) if t == id::FLOAT => {
                match row.column::<Option<f64>>(index).map_err(conv_err)? {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
                8
            }
            // NUMERIC/BIGNUMERIC have no direct f64 decode in the crate;
            // they decode to a String (via BigDecimal) which we parse.
            (ColBuilder::F64(b), t) if t == id::NUMERIC || t == id::BIGNUMERIC => {
                match row.column::<Option<String>>(index).map_err(conv_err)? {
                    Some(s) => {
                        let v: f64 = s.parse().map_err(|e| {
                            EtlError::decode(format!("invalid BigQuery numeric '{s}': {e}"))
                        })?;
                        b.append_value(v);
                        s.len()
                    }
                    None => {
                        b.append_null();
                        0
                    }
                }
            }
            // Same NUMERIC/BIGNUMERIC decimal-text source as above, but for a
            // column overridden to an exact `Decimal(P,S)` — parsed into a
            // scaled i128 instead of going through the lossy f64 arm above.
            (ColBuilder::Decimal128(b, p, s), t) if t == id::NUMERIC || t == id::BIGNUMERIC => {
                match row.column::<Option<String>>(index).map_err(conv_err)? {
                    Some(text) => {
                        let n = text.len();
                        match parse_decimal_text(&text)? {
                            DecimalText::MagnitudeOverflow => {
                                b.append_null();
                                coercion = Coercion::DecimalOverflow;
                            }
                            DecimalText::Ok {
                                negative,
                                magnitude,
                                scale,
                            } => match rescale_mantissa(magnitude, scale, *s as i32) {
                                Some(m) => {
                                    let signed = if negative { -m } else { m };
                                    if Decimal128Type::is_valid_decimal_precision(signed, *p) {
                                        b.append_value(signed);
                                    } else {
                                        b.append_null();
                                        coercion = Coercion::DecimalOverflow;
                                    }
                                }
                                None => {
                                    b.append_null();
                                    coercion = Coercion::DecimalOverflow;
                                }
                            },
                        }
                        n
                    }
                    None => {
                        b.append_null();
                        0
                    }
                }
            }
            (ColBuilder::Str(b), t) if t == id::STRING || t == id::JSON => {
                match row.column::<Option<String>>(index).map_err(conv_err)? {
                    Some(v) => {
                        let n = v.len();
                        b.append_value(&v);
                        n
                    }
                    None => {
                        b.append_null();
                        0
                    }
                }
            }
            (ColBuilder::Bin(b), t) if t == id::BYTES => {
                match row.column::<Option<Vec<u8>>>(index).map_err(conv_err)? {
                    Some(v) => {
                        let n = v.len();
                        b.append_value(&v);
                        n
                    }
                    None => {
                        b.append_null();
                        0
                    }
                }
            }
            (ColBuilder::Date(b), t) if t == id::DATE => {
                match row.column::<Option<time::Date>>(index).map_err(conv_err)? {
                    Some(d) if ch_range::year_in_range(d.year()) => {
                        let epoch = time::macros::date!(1970 - 01 - 01);
                        b.append_value((d - epoch).whole_days() as i32);
                    }
                    Some(_) => {
                        b.append_null();
                        coercion = Coercion::DateRange;
                    }
                    None => b.append_null(),
                }
                4
            }
            (ColBuilder::Ts(b, _), t) if t == id::TIMESTAMP || t == id::DATETIME => {
                match row
                    .column::<Option<time::OffsetDateTime>>(index)
                    .map_err(conv_err)?
                {
                    Some(dt) if ch_range::year_in_range(dt.year()) => {
                        b.append_value((dt.unix_timestamp_nanos() / 1000) as i64)
                    }
                    Some(_) => {
                        b.append_null();
                        coercion = Coercion::DateRange;
                    }
                    None => b.append_null(),
                }
                8
            }
            // TIME -> canonical "HH:MM:SS[.ffffff]" text into a String column.
            // BigQuery TIME is a wall clock in [00:00:00, 24:00:00).
            (ColBuilder::Str(b), t) if t == id::TIME => {
                match row.column::<Option<time::Time>>(index).map_err(conv_err)? {
                    Some(v) => {
                        let s = if v.microsecond() > 0 {
                            format!(
                                "{:02}:{:02}:{:02}.{:06}",
                                v.hour(),
                                v.minute(),
                                v.second(),
                                v.microsecond()
                            )
                        } else {
                            format!("{:02}:{:02}:{:02}", v.hour(), v.minute(), v.second())
                        };
                        let n = s.len();
                        b.append_value(&s);
                        n
                    }
                    None => {
                        b.append_null();
                        0
                    }
                }
            }
            (_, t) => {
                // The resolved type_id disagrees with this column's builder —
                // a types.rs mapping/decoder mismatch, not anything a BigQuery
                // row's actual value could cause.
                return Err(EtlError::internal(format!(
                    "unexpected BigQuery type_id {t} for column index {index}"
                )));
            }
        };
        Ok((size, coercion))
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

pub struct BigQueryBatcher {
    schema: SchemaRef,
    builders: Vec<ColBuilder>,
    type_ids: Vec<u32>,
    batch_rows: usize,
    batch_bytes: usize,
    rows_in_batch: usize,
    bytes_in_batch: usize,
    pub rows_total: u64,
    /// Count of valid dates/datetimes whose year fell outside ClickHouse's
    /// representable window and were coerced to NULL (see `append_from_row`).
    pub invalid_dates_total: u64,
    /// Count of NUMERIC/BIGNUMERIC values coerced to NULL because they
    /// overflowed a `Decimal(P,S)` override's precision (see
    /// `append_from_row`'s `Decimal128` arm).
    pub invalid_decimals_total: u64,
}

impl BigQueryBatcher {
    pub fn new(columns: &[ColumnType], batch_rows: usize) -> Result<Self> {
        Self::with_batch_bytes(columns, batch_rows, 0)
    }

    pub fn with_batch_bytes(
        columns: &[ColumnType],
        batch_rows: usize,
        batch_bytes: usize,
    ) -> Result<Self> {
        let fields: Vec<Field> = columns
            .iter()
            .map(|c| Field::new(&c.name, c.arrow.clone(), c.nullable))
            .collect();
        let mut builders = Vec::with_capacity(columns.len());
        for c in columns {
            builders.push(ColBuilder::new(&c.arrow)?);
        }
        Ok(Self {
            schema: Arc::new(Schema::new(fields)),
            builders,
            type_ids: columns.iter().map(|c| c.type_id).collect(),
            batch_rows,
            batch_bytes,
            rows_in_batch: 0,
            bytes_in_batch: 0,
            rows_total: 0,
            invalid_dates_total: 0,
            invalid_decimals_total: 0,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Append one row; returns a flushed batch if `batch_rows`/`batch_bytes` was reached.
    pub fn append_row(&mut self, row: &Row) -> Result<Option<RecordBatch>> {
        let mut row_bytes = 0usize;
        for (i, builder) in self.builders.iter_mut().enumerate() {
            let (size, coercion) = builder
                .append_from_row(row, i, self.type_ids[i])
                .map_err(|e| e.context(format!("column '{}'", self.schema.field(i).name())))?;
            row_bytes += size;
            match coercion {
                Coercion::None => {}
                Coercion::DateRange => self.invalid_dates_total += 1,
                Coercion::DecimalOverflow => self.invalid_decimals_total += 1,
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

    /// Flush any remaining buffered rows. Call once the row stream is exhausted.
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
    use arrow_array::{Array, StringArray};
    use google_cloud_bigquery::storage::value::StructDecodable;

    /// Build a single-column `Row` wrapping a BigQuery NUMERIC/BIGNUMERIC's
    /// decimal-text representation — `Row`'s only public construction path
    /// is via its `StructDecodable` impl (Arrow arrays under the hood, per
    /// this file's own module docs), so a real column array round-trips
    /// through the exact same `row.column::<Option<String>>()` call
    /// `append_from_row` itself uses, with no live BigQuery connection needed.
    fn numeric_row(value: Option<&str>) -> Row {
        let arr: ArrayRef = Arc::new(StringArray::from(vec![value]));
        Row::decode_arrow(&[arr], 0).unwrap()
    }

    /// Golden type matrix for the BigQuery source: for every `TableFieldType`
    /// `map_type` handles, assert the chain from the BQ type through (1) mapping
    /// to (type_id, Arrow, ClickHouse inner), (2) the decoder's output array
    /// type carrying the correct timezone — the TIMESTAMP(→UTC-aware) vs
    /// DATETIME(→naive) split is the exact cell whose MySQL analogue broke in
    /// 0.3.4 and had no BigQuery coverage — (3) the batch-vs-schema check, and
    /// (4) the Arrow→BigQuery bridge (lossy for TIME/NUMERIC, asserted as such).
    #[test]
    fn bigquery_type_golden_matrix() {
        use crate::types::bigquery::arrow_to_bigquery_type as a2b;
        use crate::types::bigquery::{map_type, type_id as id};
        use arrow_schema::{Field, Schema, TimeUnit};
        use google_cloud_bigquery::http::table::TableFieldType as Bq;

        let ts = |tz: Option<&str>| DataType::Timestamp(TimeUnit::Microsecond, tz.map(Arc::from));
        // (input BQ type, expected type_id, expected Arrow, expected CH inner, expected a2b bridge)
        let rows: Vec<(Bq, u32, DataType, &str, Bq)> = vec![
            (Bq::String, id::STRING, DataType::Utf8, "String", Bq::String),
            (Bq::Bytes, id::BYTES, DataType::Binary, "String", Bq::Bytes),
            (
                Bq::Integer,
                id::INTEGER,
                DataType::Int64,
                "Int64",
                Bq::Integer,
            ),
            (
                Bq::Float,
                id::FLOAT,
                DataType::Float64,
                "Float64",
                Bq::Float,
            ),
            (
                Bq::Boolean,
                id::BOOLEAN,
                DataType::Boolean,
                "Bool",
                Bq::Boolean,
            ),
            (
                Bq::Timestamp,
                id::TIMESTAMP,
                ts(Some("UTC")),
                "DateTime64(6, 'UTC')",
                Bq::Timestamp,
            ),
            (Bq::Date, id::DATE, DataType::Date32, "Date32", Bq::Date),
            (Bq::Time, id::TIME, DataType::Utf8, "String", Bq::String), // lossy: Time -> String
            (
                Bq::Datetime,
                id::DATETIME,
                ts(None),
                "DateTime64(6)",
                Bq::Datetime,
            ),
            (
                Bq::Numeric,
                id::NUMERIC,
                DataType::Float64,
                "Float64",
                Bq::Float,
            ), // lossy
            (
                Bq::Bignumeric,
                id::BIGNUMERIC,
                DataType::Float64,
                "Float64",
                Bq::Float,
            ), // lossy
        ];
        for (input, tid, arrow, ch, bridge) in rows {
            let (mapped_id, mapped_arrow, mapped_ch) =
                map_type(&input).unwrap_or_else(|| panic!("{input:?} unmapped"));
            assert_eq!(mapped_id, tid, "{input:?}: type_id");
            assert_eq!(mapped_arrow, arrow, "{input:?}: Arrow type");
            assert_eq!(mapped_ch, ch, "{input:?}: ClickHouse inner");
            let mut b = ColBuilder::new(&arrow).unwrap();
            let out = b.finish();
            assert_eq!(out.data_type(), &arrow, "{input:?}: decoder output type/tz");
            let schema = Arc::new(Schema::new(vec![Field::new("c", arrow.clone(), true)]));
            RecordBatch::try_new(schema, vec![out])
                .unwrap_or_else(|e| panic!("{input:?}: batch: {e}"));
            assert_eq!(
                a2b(&arrow),
                Some(bridge),
                "{input:?}: Arrow->BigQuery bridge"
            );
        }
    }

    #[test]
    fn decimal_decodes_exact_text_value() {
        let mut b = ColBuilder::new(&DataType::Decimal128(10, 4)).unwrap();
        let (_, coercion) = b
            .append_from_row(&numeric_row(Some("123.4500")), 0, id::NUMERIC)
            .unwrap();
        assert_eq!(coercion, Coercion::None);
        let arr = b.finish();
        let arr = arr
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), 1_234_500);
    }

    #[test]
    fn decimal_rounds_half_away_from_zero_when_narrowing() {
        let mut b = ColBuilder::new(&DataType::Decimal128(10, 2)).unwrap();
        let (_, coercion) = b
            .append_from_row(&numeric_row(Some("12.345")), 0, id::BIGNUMERIC)
            .unwrap();
        assert_eq!(coercion, Coercion::None);
        let arr = b.finish();
        let arr = arr
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), 1235); // 12.345 -> 12.35, not truncated to 12.34
    }

    #[test]
    fn decimal_negative_value_round_trips_exactly() {
        let mut b = ColBuilder::new(&DataType::Decimal128(10, 2)).unwrap();
        let (_, coercion) = b
            .append_from_row(&numeric_row(Some("-42.5")), 0, id::NUMERIC)
            .unwrap();
        assert_eq!(coercion, Coercion::None);
        let arr = b.finish();
        let arr = arr
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), -4250);
    }

    #[test]
    fn decimal_coerces_to_null_when_value_overflows_declared_precision() {
        let mut b = ColBuilder::new(&DataType::Decimal128(3, 0)).unwrap();
        let (_, coercion) = b
            .append_from_row(&numeric_row(Some("1234")), 0, id::NUMERIC)
            .unwrap();
        assert_eq!(coercion, Coercion::DecimalOverflow);
        let arr = b.finish();
        let arr = arr
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert!(arr.is_null(0));
    }

    #[test]
    fn decimal_null_value_stays_null_with_no_coercion() {
        let mut b = ColBuilder::new(&DataType::Decimal128(10, 2)).unwrap();
        let (_, coercion) = b
            .append_from_row(&numeric_row(None), 0, id::NUMERIC)
            .unwrap();
        assert_eq!(coercion, Coercion::None);
        let arr = b.finish();
        let arr = arr
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert!(arr.is_null(0));
    }
}
