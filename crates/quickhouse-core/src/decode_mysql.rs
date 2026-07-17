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
    BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, Int8Builder, StringBuilder, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use mysql_async::{Row, Value};

use crate::error::{EtlError, Result};
use crate::types::ColumnType;

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
    Time(Time64MicrosecondBuilder),
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
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                ColBuilder::Ts(TimestampMicrosecondBuilder::new())
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                ColBuilder::Time(Time64MicrosecondBuilder::new())
            }
            other => {
                return Err(EtlError::decode(format!(
                    "no column builder for Arrow type {other:?}"
                )))
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
            ColBuilder::Time(b) => b.append_null(),
        }
    }

    fn append_value(&mut self, value: Value) -> Result<()> {
        if matches!(value, Value::NULL) {
            self.append_null();
            return Ok(());
        }
        match (self, value) {
            (ColBuilder::Bool(b), Value::Int(i)) => b.append_value(i != 0),
            (ColBuilder::Bool(b), Value::UInt(u)) => b.append_value(u != 0),
            (ColBuilder::Bool(b), Value::Bytes(v)) => {
                b.append_value(v.first().map(|&x| x != 0).unwrap_or(false))
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
            (ColBuilder::Bin(b), Value::Bytes(v)) => b.append_value(&v),
            (ColBuilder::Date(b), Value::Date(year, month, day, ..)) => {
                let date = chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)
                    .ok_or_else(|| EtlError::decode("invalid MySQL date"))?;
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                b.append_value((date - epoch).num_days() as i32);
            }
            (
                ColBuilder::Ts(b),
                Value::Date(year, month, day, hour, minute, second, micros),
            ) => {
                let dt = chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)
                    .and_then(|d| {
                        d.and_hms_micro_opt(hour as u32, minute as u32, second as u32, micros)
                    })
                    .ok_or_else(|| EtlError::decode("invalid MySQL datetime"))?;
                b.append_value(dt.and_utc().timestamp_micros());
            }
            (ColBuilder::Time(b), Value::Time(is_neg, days, hours, minutes, seconds, micros)) => {
                let total_micros = ((days as i64 * 24 + hours as i64) * 3600
                    + minutes as i64 * 60
                    + seconds as i64)
                    * 1_000_000
                    + micros as i64;
                b.append_value(if is_neg { -total_micros } else { total_micros });
            }
            (_, value) => {
                return Err(EtlError::decode(format!(
                    "unexpected MySQL value {value:?} for this column's builder"
                )))
            }
        }
        Ok(())
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
            ColBuilder::Time(b) => Arc::new(b.finish()),
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
}

impl MySqlBatcher {
    pub fn new(columns: &[ColumnType], batch_rows: usize) -> Result<Self> {
        Self::with_batch_bytes(columns, batch_rows, 0)
    }

    pub fn with_batch_bytes(columns: &[ColumnType], batch_rows: usize, batch_bytes: usize) -> Result<Self> {
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
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Append one row; returns a flushed batch if `batch_rows`/`batch_bytes` was reached.
    pub fn append_row(&mut self, row: Row) -> Result<Option<RecordBatch>> {
        let n = row.len();
        if n != self.builders.len() {
            return Err(EtlError::decode(format!(
                "row has {n} columns, expected {}",
                self.builders.len()
            )));
        }
        let mut row_bytes = 0usize;
        for (i, builder) in self.builders.iter_mut().enumerate() {
            let value = row.as_ref(i).cloned().unwrap_or(Value::NULL);
            row_bytes += value_size(&value);
            builder.append_value(value)?;
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
