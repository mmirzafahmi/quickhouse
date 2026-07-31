//! Streaming batcher: MySQL result rows (`mysql_async::Row`) into Arrow
//! `RecordBatch`es.
//!
//! Unlike PostgreSQL's binary `COPY`, `mysql_async` already parses the wire
//! protocol into typed `Value`s per row, so there's no byte-level parsing
//! here — just appending each row's cells into the matching Arrow builder and
//! flushing a batch every `batch_rows` rows, mirroring `decode::CopyDecoder`'s
//! batching behavior for the Postgres source.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder,
    TimestampMicrosecondBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow_array::types::{Decimal128Type, DecimalType};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use mysql_async::{Row, Value};

use crate::decimal::{parse_decimal_text, rescale_mantissa, Coercion, DecimalText};
use crate::error::{EtlError, Result};
use crate::types::{ch_range, ColumnType};

enum ColBuilder {
    Bool(BooleanBuilder),
    I8(Int8Builder),
    U8(UInt8Builder),
    I16(Int16Builder),
    U16(UInt16Builder),
    I32(Int32Builder),
    U32(UInt32Builder),
    I64(Int64Builder),
    U64(UInt64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Str(StringBuilder),
    Bin(BinaryBuilder),
    Date(Date32Builder),
    Ts(TimestampMicrosecondBuilder),
    Decimal128(Decimal128Builder, u8, i8),
}

impl ColBuilder {
    fn new(dt: &DataType) -> Result<Self> {
        Ok(match dt {
            DataType::Boolean => ColBuilder::Bool(BooleanBuilder::new()),
            DataType::Int8 => ColBuilder::I8(Int8Builder::new()),
            DataType::UInt8 => ColBuilder::U8(UInt8Builder::new()),
            DataType::Int16 => ColBuilder::I16(Int16Builder::new()),
            DataType::UInt16 => ColBuilder::U16(UInt16Builder::new()),
            DataType::Int32 => ColBuilder::I32(Int32Builder::new()),
            DataType::UInt32 => ColBuilder::U32(UInt32Builder::new()),
            DataType::Int64 => ColBuilder::I64(Int64Builder::new()),
            DataType::UInt64 => ColBuilder::U64(UInt64Builder::new()),
            DataType::Float32 => ColBuilder::F32(Float32Builder::new()),
            DataType::Float64 => ColBuilder::F64(Float64Builder::new()),
            DataType::Utf8 => ColBuilder::Str(StringBuilder::new()),
            DataType::Binary => ColBuilder::Bin(BinaryBuilder::new()),
            DataType::Date32 => ColBuilder::Date(Date32Builder::new()),
            // Carry the resolved timezone onto the builder so `finish()`
            // produces an array whose type matches the batch schema exactly.
            // MySQL datetimes now resolve to `Timestamp(µs, Some("UTC"))` (see
            // types::map_mysql_type); dropping the tz here (as this used to)
            // built a naive array that failed the batch-vs-schema check with
            // "expected Timestamp(.., Some(\"UTC\")) but found Timestamp(..,
            // None)". The stored micros are unaffected — this only sets the
            // array's tz metadata. Mirrors the Postgres decoder (decode.rs).
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                ColBuilder::Ts(TimestampMicrosecondBuilder::new().with_timezone_opt(tz.clone()))
            }
            DataType::Decimal128(p, s) => ColBuilder::Decimal128(
                Decimal128Builder::new().with_precision_and_scale(*p, *s)?,
                *p,
                *s,
            ),
            other => {
                // Reachable only if types.rs maps some MySQL column type to an
                // Arrow type this decoder doesn't implement a builder for.
                return Err(EtlError::internal(format!(
                    "no column builder for Arrow type {other:?}"
                )));
            }
        })
    }

    fn append_null(&mut self) {
        match self {
            ColBuilder::Bool(b) => b.append_null(),
            ColBuilder::I8(b) => b.append_null(),
            ColBuilder::U8(b) => b.append_null(),
            ColBuilder::I16(b) => b.append_null(),
            ColBuilder::U16(b) => b.append_null(),
            ColBuilder::I32(b) => b.append_null(),
            ColBuilder::U32(b) => b.append_null(),
            ColBuilder::I64(b) => b.append_null(),
            ColBuilder::U64(b) => b.append_null(),
            ColBuilder::F32(b) => b.append_null(),
            ColBuilder::F64(b) => b.append_null(),
            ColBuilder::Str(b) => b.append_null(),
            ColBuilder::Bin(b) => b.append_null(),
            ColBuilder::Date(b) => b.append_null(),
            ColBuilder::Ts(b) => b.append_null(),
            ColBuilder::Decimal128(b, _, _) => b.append_null(),
        }
    }

    /// Appends `value`, returning the [`Coercion`] reason if it was coerced
    /// to NULL instead of erroring or corrupting the whole transfer. Two
    /// date/datetime cases, both common in real legacy tables and both fatal
    /// to a whole multi-million-row transfer if not handled:
    ///   1. The classic `0000-00-00` zero-date (or a partial zero like
    ///      `2024-00-15`) — MySQL allows these by default (no
    ///      `NO_ZERO_DATE`/`NO_ZERO_IN_DATE` sql_mode) and they have no valid
    ///      `chrono::NaiveDate` representation.
    ///   2. A valid calendar date whose year falls outside ClickHouse's
    ///      `Date32`/`DateTime64` window of `[1900-01-01, 2299-12-31]` — e.g.
    ///      the ubiquitous `9999-12-31` "never expires" sentinel or a pre-1900
    ///      date. ClickHouse's ArrowStream reader rejects these outright with
    ///      `VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE`, so they must be filtered here
    ///      rather than at insert time.
    ///
    /// Plus one decimal case: a DECIMAL/NEWDECIMAL value overridden to an
    /// exact `Decimal(P,S)` that overflows `P`, coerced the same way.
    fn append_value(&mut self, value: Value) -> Result<Coercion> {
        if matches!(value, Value::NULL) {
            self.append_null();
            return Ok(Coercion::None);
        }
        let mut coercion = Coercion::None;
        match (self, value) {
            // A Boolean builder here means `types.rs` inferred Bool from
            // MySQL's `tinyint(1)` *display width* — a convention Rails/PHP
            // ORMs follow but plenty of schemas (Odoo, for one) do not, where
            // a `tinyint(1)` holds genuine small integers. Flag any value
            // outside {0, 1} so the caller can report that the distinction
            // between e.g. 2 and 3 was flattened, instead of losing it in
            // silence. `tinyint1_as_bool=False` opts out of the mapping.
            (ColBuilder::Bool(b), Value::Int(i)) => {
                b.append_value(i != 0);
                if i != 0 && i != 1 {
                    coercion = Coercion::BoolCollapse;
                }
            }
            (ColBuilder::Bool(b), Value::UInt(u)) => {
                b.append_value(u != 0);
                if u != 0 && u != 1 {
                    coercion = Coercion::BoolCollapse;
                }
            }
            (ColBuilder::Bool(b), Value::Bytes(v)) => {
                b.append_value(v.first().map(|&x| x != 0).unwrap_or(false));
                if v.first().is_some_and(|&x| x != 0 && x != 1) {
                    coercion = Coercion::BoolCollapse;
                }
            }
            (ColBuilder::I8(b), Value::Int(i)) => b.append_value(i as i8),
            (ColBuilder::I8(b), Value::UInt(u)) => b.append_value(u as i8),
            (ColBuilder::U8(b), Value::Int(i)) => b.append_value(i as u8),
            (ColBuilder::U8(b), Value::UInt(u)) => b.append_value(u as u8),
            (ColBuilder::I16(b), Value::Int(i)) => b.append_value(i as i16),
            (ColBuilder::I16(b), Value::UInt(u)) => b.append_value(u as i16),
            (ColBuilder::U16(b), Value::Int(i)) => b.append_value(i as u16),
            (ColBuilder::U16(b), Value::UInt(u)) => b.append_value(u as u16),
            (ColBuilder::I32(b), Value::Int(i)) => b.append_value(i as i32),
            (ColBuilder::I32(b), Value::UInt(u)) => b.append_value(u as i32),
            (ColBuilder::U32(b), Value::Int(i)) => b.append_value(i as u32),
            (ColBuilder::U32(b), Value::UInt(u)) => b.append_value(u as u32),
            (ColBuilder::I64(b), Value::Int(i)) => b.append_value(i),
            (ColBuilder::I64(b), Value::UInt(u)) => b.append_value(u as i64),
            (ColBuilder::U64(b), Value::Int(i)) => b.append_value(i as u64),
            (ColBuilder::U64(b), Value::UInt(u)) => b.append_value(u),
            (ColBuilder::F32(b), Value::Float(f)) => b.append_value(f),
            (ColBuilder::F32(b), Value::Double(d)) => b.append_value(d as f32),
            (ColBuilder::F64(b), Value::Double(d)) => b.append_value(d),
            (ColBuilder::F64(b), Value::Float(f)) => b.append_value(f as f64),
            // DECIMAL/NEWDECIMAL arrive as Bytes (ASCII text of the number).
            (ColBuilder::F64(b), Value::Bytes(v)) => {
                let s = std::str::from_utf8(&v)
                    .map_err(|e| EtlError::decode(format!("invalid decimal bytes: {e}")))?;
                let f: f64 = s
                    .parse()
                    .map_err(|e| EtlError::decode(format!("invalid decimal '{s}': {e}")))?;
                b.append_value(f);
            }
            (ColBuilder::Str(b), Value::Bytes(v)) => {
                b.append_value(String::from_utf8_lossy(&v).as_ref())
            }
            // MySQL TIME (mapped to a ClickHouse String, not a time-of-day type):
            // render the canonical `[-]HH:MM:SS[.ffffff]` text. Unlike a wall
            // clock, TIME is a signed *duration* that can be negative and exceed
            // 24h (up to +/-838:59:59), so hours accumulate `days*24 + hours`
            // and can be three digits — text preserves all of that losslessly.
            (ColBuilder::Str(b), Value::Time(is_neg, days, hours, minutes, seconds, micros)) => {
                let total_hours = days as u64 * 24 + hours as u64;
                let sign = if is_neg { "-" } else { "" };
                let s = if micros > 0 {
                    format!("{sign}{total_hours:02}:{minutes:02}:{seconds:02}.{micros:06}")
                } else {
                    format!("{sign}{total_hours:02}:{minutes:02}:{seconds:02}")
                };
                b.append_value(&s);
            }
            (ColBuilder::Decimal128(b, p, s), Value::Bytes(v)) => {
                let text = std::str::from_utf8(&v)
                    .map_err(|e| EtlError::decode(format!("invalid decimal bytes: {e}")))?;
                match parse_decimal_text(text)? {
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
            }
            (ColBuilder::Bin(b), Value::Bytes(v)) => b.append_value(&v),
            // Coerce zero-dates (`0000-00-00`, `2024-00-15`) and dates whose
            // year is outside ClickHouse's representable window to NULL rather
            // than aborting the whole transfer — see `append_value`'s docs and
            // `ch_year_in_range`. The `_` arm catches both: an unparseable date
            // (`from_ymd_opt` -> None) and a valid-but-out-of-range one (guard
            // fails).
            (ColBuilder::Date(b), Value::Date(year, month, day, ..)) => {
                match chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32) {
                    Some(date) if ch_range::year_in_range(year as i32) => {
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        b.append_value((date - epoch).num_days() as i32);
                    }
                    _ => {
                        b.append_null();
                        coercion = Coercion::DateRange;
                    }
                }
            }
            (ColBuilder::Ts(b), Value::Date(year, month, day, hour, minute, second, micros)) => {
                let dt = chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)
                    .and_then(|d| {
                        d.and_hms_micro_opt(hour as u32, minute as u32, second as u32, micros)
                    });
                match dt {
                    Some(dt) if ch_range::year_in_range(year as i32) => {
                        b.append_value(dt.and_utc().timestamp_micros())
                    }
                    _ => {
                        b.append_null();
                        coercion = Coercion::DateRange;
                    }
                }
            }
            (_, value) => {
                // The binary protocol (always used for row fetching — see
                // transfer_partition_mysql) hands back a value shape that
                // doesn't match what this column's resolved type expects —
                // a types.rs mapping/decoder mismatch, not bad source data.
                return Err(EtlError::internal(format!(
                    "unexpected MySQL value {value:?} for this column's builder"
                )));
            }
        }
        Ok(coercion)
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            ColBuilder::Bool(b) => Arc::new(b.finish()),
            ColBuilder::I8(b) => Arc::new(b.finish()),
            ColBuilder::U8(b) => Arc::new(b.finish()),
            ColBuilder::I16(b) => Arc::new(b.finish()),
            ColBuilder::U16(b) => Arc::new(b.finish()),
            ColBuilder::I32(b) => Arc::new(b.finish()),
            ColBuilder::U32(b) => Arc::new(b.finish()),
            ColBuilder::I64(b) => Arc::new(b.finish()),
            ColBuilder::U64(b) => Arc::new(b.finish()),
            ColBuilder::F32(b) => Arc::new(b.finish()),
            ColBuilder::F64(b) => Arc::new(b.finish()),
            ColBuilder::Str(b) => Arc::new(b.finish()),
            ColBuilder::Bin(b) => Arc::new(b.finish()),
            ColBuilder::Date(b) => Arc::new(b.finish()),
            ColBuilder::Ts(b) => Arc::new(b.finish()),
            ColBuilder::Decimal128(b, _, _) => Arc::new(b.finish()),
        }
    }
}

/// Rough encoded-size estimate for a MySQL cell, used only to decide when to
/// flush a batch (a proxy for actual bytes, not an exact wire size).
fn value_size(v: &Value) -> usize {
    match v {
        Value::NULL => 0,
        Value::Bytes(b) => b.len(),
        Value::Int(_) | Value::UInt(_) | Value::Double(_) => 8,
        Value::Float(_) => 4,
        Value::Date(..) => 7,
        Value::Time(..) => 8,
    }
}

pub struct MySqlBatcher {
    schema: SchemaRef,
    builders: Vec<ColBuilder>,
    batch_rows: usize,
    batch_bytes: usize,
    rows_in_batch: usize,
    bytes_in_batch: usize,
    pub rows_total: u64,
    /// Count of unrepresentable MySQL dates/datetimes (e.g. `0000-00-00`)
    /// coerced to NULL across the whole stream — see `ColBuilder::append_value`.
    pub invalid_dates_total: u64,
    /// Count of DECIMAL/NEWDECIMAL values coerced to NULL because they
    /// overflowed a `Decimal(P,S)` override's precision (see
    /// `ColBuilder::append_value`'s `Decimal128` arm).
    pub invalid_decimals_total: u64,
    /// Count of `tinyint(1)` values outside `{0, 1}` flattened to a Boolean
    /// (see `ColBuilder::append_value`'s `Bool` arms). Non-zero means the
    /// display-width Boolean inference was wrong for this table and real
    /// integer values were lost — pass `tinyint1_as_bool=False` to keep them.
    pub collapsed_bools_total: u64,
}

impl MySqlBatcher {
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
            batch_rows,
            batch_bytes,
            rows_in_batch: 0,
            bytes_in_batch: 0,
            rows_total: 0,
            invalid_dates_total: 0,
            invalid_decimals_total: 0,
            collapsed_bools_total: 0,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Append one row; returns a flushed batch if `batch_rows`/`batch_bytes` was reached.
    pub fn append_row(&mut self, row: Row) -> Result<Option<RecordBatch>> {
        let n = row.len();
        if n != self.builders.len() {
            // The result set's own shape disagrees with the resolved schema's
            // column count — a decoder/schema mismatch, not bad source data.
            return Err(EtlError::internal(format!(
                "row has {n} column(s) but the resolved schema has {} column(s)",
                self.builders.len()
            )));
        }
        let mut row_bytes = 0usize;
        for (i, builder) in self.builders.iter_mut().enumerate() {
            let value = row.as_ref(i).cloned().unwrap_or(Value::NULL);
            row_bytes += value_size(&value);
            let coercion = builder
                .append_value(value)
                .map_err(|e| e.context(format!("column '{}'", self.schema.field(i).name())))?;
            match coercion {
                Coercion::None => {}
                Coercion::DateRange => self.invalid_dates_total += 1,
                Coercion::DecimalOverflow => self.invalid_decimals_total += 1,
                Coercion::BoolCollapse => self.collapsed_bools_total += 1,
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
    use arrow_array::{Array, Date32Array, StringArray, TimestampMicrosecondArray};

    #[test]
    fn time_formatted_as_signed_text() {
        // TIME maps to a String column: (is_neg, days, hours, minutes, seconds, micros).
        let mut b = ColBuilder::new(&DataType::Utf8).unwrap();
        assert_eq!(
            b.append_value(Value::Time(false, 0, 10, 30, 0, 0)).unwrap(),
            Coercion::None
        ); // 10:30:00
        assert_eq!(
            b.append_value(Value::Time(true, 0, 5, 0, 0, 0)).unwrap(),
            Coercion::None
        ); // -05:00:00
        assert_eq!(
            b.append_value(Value::Time(false, 34, 22, 59, 59, 0))
                .unwrap(),
            Coercion::None
        ); // 838:59:59
        assert_eq!(
            b.append_value(Value::Time(false, 0, 1, 2, 3, 500_000))
                .unwrap(),
            Coercion::None
        ); // sub-second
        assert_eq!(b.append_value(Value::NULL).unwrap(), Coercion::None);

        let arr = b.finish();
        let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "10:30:00");
        assert_eq!(arr.value(1), "-05:00:00");
        assert_eq!(arr.value(2), "838:59:59"); // 34*24 + 22 = 838 hours
        assert_eq!(arr.value(3), "01:02:03.500000");
        assert!(arr.is_null(4));
    }

    #[test]
    fn bool_builder_flags_tinyint1_values_outside_zero_and_one() {
        // Regression test for the production incident: a `tinyint(1)` holding
        // genuine small integers is mapped to Bool from its display width, and
        // every non-zero value collapses to `true`. The collapse itself is
        // preserved (changing it silently would be its own data change) but it
        // must now be *reported*, so a caller can see the distinction was lost.
        let mut b = ColBuilder::new(&DataType::Boolean).unwrap();
        // Genuine booleans: no coercion reported.
        assert_eq!(b.append_value(Value::Int(0)).unwrap(), Coercion::None);
        assert_eq!(b.append_value(Value::Int(1)).unwrap(), Coercion::None);
        assert_eq!(b.append_value(Value::NULL).unwrap(), Coercion::None);
        // Real tier values 2 and 3 both become `true` — flagged.
        assert_eq!(
            b.append_value(Value::Int(2)).unwrap(),
            Coercion::BoolCollapse
        );
        assert_eq!(
            b.append_value(Value::Int(3)).unwrap(),
            Coercion::BoolCollapse
        );
        // Negative and unsigned paths are flagged the same way.
        assert_eq!(
            b.append_value(Value::Int(-5)).unwrap(),
            Coercion::BoolCollapse
        );
        assert_eq!(
            b.append_value(Value::UInt(7)).unwrap(),
            Coercion::BoolCollapse
        );
        assert_eq!(b.append_value(Value::UInt(1)).unwrap(), Coercion::None);

        let arr = b.finish();
        let arr = arr
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap();
        assert!(!arr.value(0));
        assert!(arr.value(1));
        assert!(arr.is_null(2));
        // The lossy part, pinned deliberately: 2, 3 and -5 are indistinguishable.
        assert!(arr.value(3) && arr.value(4) && arr.value(5));
    }

    #[test]
    fn date_builder_coerces_zero_and_out_of_range_to_null() {
        let mut b = ColBuilder::new(&DataType::Date32).unwrap();
        // (year, month, day, hour, min, sec, micros)
        assert_eq!(
            b.append_value(Value::Date(2024, 5, 1, 0, 0, 0, 0)).unwrap(),
            Coercion::None
        ); // in range
        assert_eq!(
            b.append_value(Value::Date(0, 0, 0, 0, 0, 0, 0)).unwrap(),
            Coercion::DateRange
        ); // zero-date
        assert_eq!(
            b.append_value(Value::Date(2024, 0, 15, 0, 0, 0, 0))
                .unwrap(),
            Coercion::DateRange
        ); // partial-zero
        assert_eq!(
            b.append_value(Value::Date(9999, 12, 31, 0, 0, 0, 0))
                .unwrap(),
            Coercion::DateRange
        ); // far future
        assert_eq!(
            b.append_value(Value::Date(1800, 6, 15, 0, 0, 0, 0))
                .unwrap(),
            Coercion::DateRange
        ); // pre-1900
        assert_eq!(b.append_value(Value::NULL).unwrap(), Coercion::None); // NULL is not a coercion

        let arr = b.finish();
        let arr = arr.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(arr.len(), 6);
        assert!(!arr.is_null(0), "valid in-range date must be kept");
        for i in 1..=4 {
            assert!(arr.is_null(i), "row {i} must be coerced to NULL");
        }
        assert!(arr.is_null(5), "explicit NULL stays NULL");
    }

    #[test]
    fn ts_builder_coerces_out_of_range_datetimes_to_null() {
        let mut b = ColBuilder::new(&DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();
        assert_eq!(
            b.append_value(Value::Date(2024, 5, 1, 10, 0, 0, 0))
                .unwrap(),
            Coercion::None
        ); // in range
        assert_eq!(
            b.append_value(Value::Date(9999, 12, 31, 23, 59, 59, 0))
                .unwrap(),
            Coercion::DateRange
        ); // far future
        assert_eq!(
            b.append_value(Value::Date(1899, 12, 31, 0, 0, 0, 0))
                .unwrap(),
            Coercion::DateRange
        ); // pre-1900

        let arr = b.finish();
        let arr = arr
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert!(!arr.is_null(0));
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));
    }

    /// End-to-end reproduction of the reported v0.3.4 failure, hermetic (no
    /// live MySQL needed). The crash was a batch-vs-schema mismatch in
    /// `RecordBatch::try_new` inside `flush_batch`; this rebuilds that exact
    /// step — the schema field from the resolved Arrow type (as
    /// `MySqlBatcher::with_batch_bytes` does via `Field::new(c.arrow, ...)`)
    /// and the column array from `ColBuilder::finish()` — for a UTC-mapped
    /// MySQL timestamp column. Before the decoder fix, `try_new` returned
    /// "column types must match schema types, expected Timestamp(..,
    /// Some(\"UTC\")) but found Timestamp(.., None)"; now the batch builds and
    /// the column keeps its UTC type.
    #[test]
    fn utc_timestamp_column_builds_a_valid_batch() {
        let arrow = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "created_date",
            arrow.clone(),
            true,
        )]));
        let mut b = ColBuilder::new(&arrow).unwrap();
        b.append_value(Value::Date(2024, 5, 1, 10, 0, 0, 0))
            .unwrap();
        let batch = RecordBatch::try_new(schema, vec![b.finish()])
            .expect("v0.3.4 failed here: naive array vs UTC schema");
        assert_eq!(batch.column(0).data_type(), &arrow);
        assert_eq!(batch.num_rows(), 1);
    }

    /// Regression test for the 0.3.4 follow-up bug: the finished array's *type*
    /// must carry the timezone from the resolved schema, not merely the correct
    /// micros. MySQL datetimes resolve to `Timestamp(µs, Some("UTC"))`; a naive
    /// array (tz `None`) built here failed the downstream batch-vs-schema check
    /// with "expected Timestamp(.., Some(\"UTC\")) but found Timestamp(.., None)".
    /// The earlier 0.3.4 tests asserted the mapping and the value, but not this
    /// — the exact gap that let the regression ship.
    #[test]
    fn ts_builder_output_type_carries_timezone() {
        // UTC-aware (the MySQL datetime default): finished array is tz-aware.
        let mut utc = ColBuilder::new(&DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ))
        .unwrap();
        utc.append_value(Value::Date(2024, 5, 1, 10, 0, 0, 0))
            .unwrap();
        assert_eq!(
            utc.finish().data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "MySQL datetime array must carry the UTC timezone to match the resolved schema"
        );
        // Naive (the per-column `type_overrides={col: \"DATETIME\"}` opt-out)
        // must still produce a naive array — the reverse mismatch.
        let mut naive = ColBuilder::new(&DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();
        naive
            .append_value(Value::Date(2024, 5, 1, 10, 0, 0, 0))
            .unwrap();
        assert_eq!(
            naive.finish().data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            "a naive-mapped datetime column must stay naive"
        );
    }

    #[test]
    fn decimal_decodes_exact_text_value() {
        let mut b = ColBuilder::new(&DataType::Decimal128(10, 4)).unwrap();
        let coercion = b.append_value(Value::Bytes(b"123.4500".to_vec())).unwrap();
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
        // 12.345 narrowed to scale 2: 1234 remainder 5, divisor 10 -> rounds
        // up to 1235 (12.35), not truncated to 1234 (12.34).
        let coercion = b.append_value(Value::Bytes(b"12.345".to_vec())).unwrap();
        assert_eq!(coercion, Coercion::None);
        let arr = b.finish();
        let arr = arr
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), 1235);
    }

    #[test]
    fn decimal_negative_value_round_trips_exactly() {
        let mut b = ColBuilder::new(&DataType::Decimal128(10, 2)).unwrap();
        let coercion = b.append_value(Value::Bytes(b"-42.5".to_vec())).unwrap();
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
        let coercion = b.append_value(Value::Bytes(b"1234".to_vec())).unwrap();
        assert_eq!(coercion, Coercion::DecimalOverflow);
        let arr = b.finish();
        let arr = arr
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert!(arr.is_null(0));
    }

    #[test]
    fn decimal_zero_scale_text_has_no_decimal_point() {
        // MySQL's DECIMAL(n, 0) text has no trailing '.' at all.
        let mut b = ColBuilder::new(&DataType::Decimal128(10, 0)).unwrap();
        let coercion = b.append_value(Value::Bytes(b"42".to_vec())).unwrap();
        assert_eq!(coercion, Coercion::None);
        let arr = b.finish();
        let arr = arr
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), 42);
    }
}
