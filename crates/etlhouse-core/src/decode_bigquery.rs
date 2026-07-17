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
    BinaryBuilder, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use google_cloud_bigquery::storage::row::Row;

use crate::error::{EtlError, Result};
use crate::types::bigquery::type_id as id;
use crate::types::ColumnType;

fn conv_err(e: impl std::fmt::Display) -> EtlError {
    EtlError::decode(format!("bigquery row decode error: {e}"))
}

enum ColBuilder {
    Bool(BooleanBuilder),
    I64(Int64Builder),
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
            DataType::Int64 => ColBuilder::I64(Int64Builder::new()),
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

    fn append_from_row(&mut self, row: &Row, index: usize, type_id: u32) -> Result<()> {
        match (self, type_id) {
            (ColBuilder::Bool(b), t) if t == id::BOOLEAN => {
                match row.column::<Option<bool>>(index).map_err(conv_err)? {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            (ColBuilder::I64(b), t) if t == id::INTEGER => {
                match row.column::<Option<i64>>(index).map_err(conv_err)? {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            (ColBuilder::F64(b), t) if t == id::FLOAT => {
                match row.column::<Option<f64>>(index).map_err(conv_err)? {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            // NUMERIC/BIGNUMERIC have no direct f64 decode in the crate;
            // they decode to a String (via BigDecimal) which we parse.
            (ColBuilder::F64(b), t) if t == id::NUMERIC || t == id::BIGNUMERIC => {
                match row.column::<Option<String>>(index).map_err(conv_err)? {
                    Some(s) => {
                        let v: f64 = s
                            .parse()
                            .map_err(|e| EtlError::decode(format!("invalid BigQuery numeric '{s}': {e}")))?;
                        b.append_value(v);
                    }
                    None => b.append_null(),
                }
            }
            (ColBuilder::Str(b), t) if t == id::STRING || t == id::JSON => {
                match row.column::<Option<String>>(index).map_err(conv_err)? {
                    Some(v) => b.append_value(&v),
                    None => b.append_null(),
                }
            }
            (ColBuilder::Bin(b), t) if t == id::BYTES => {
                match row.column::<Option<Vec<u8>>>(index).map_err(conv_err)? {
                    Some(v) => b.append_value(&v),
                    None => b.append_null(),
                }
            }
            (ColBuilder::Date(b), t) if t == id::DATE => {
                match row.column::<Option<time::Date>>(index).map_err(conv_err)? {
                    Some(d) => {
                        let epoch = time::macros::date!(1970 - 01 - 01);
                        b.append_value((d - epoch).whole_days() as i32);
                    }
                    None => b.append_null(),
                }
            }
            (ColBuilder::Ts(b), t) if t == id::TIMESTAMP || t == id::DATETIME => {
                match row.column::<Option<time::OffsetDateTime>>(index).map_err(conv_err)? {
                    Some(dt) => b.append_value((dt.unix_timestamp_nanos() / 1000) as i64),
                    None => b.append_null(),
                }
            }
            (ColBuilder::Time(b), t) if t == id::TIME => {
                match row.column::<Option<time::Time>>(index).map_err(conv_err)? {
                    Some(v) => {
                        let micros = v.hour() as i64 * 3_600_000_000
                            + v.minute() as i64 * 60_000_000
                            + v.second() as i64 * 1_000_000
                            + v.microsecond() as i64;
                        b.append_value(micros);
                    }
                    None => b.append_null(),
                }
            }
            (_, t) => {
                return Err(EtlError::decode(format!(
                    "unexpected BigQuery type_id {t} for column index {index}"
                )))
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            ColBuilder::Bool(b) => Arc::new(b.finish()),
            ColBuilder::I64(b) => Arc::new(b.finish()),
            ColBuilder::F64(b) => Arc::new(b.finish()),
            ColBuilder::Str(b) => Arc::new(b.finish()),
            ColBuilder::Bin(b) => Arc::new(b.finish()),
            ColBuilder::Date(b) => Arc::new(b.finish()),
            ColBuilder::Ts(b) => Arc::new(b.finish()),
            ColBuilder::Time(b) => Arc::new(b.finish()),
        }
    }
}

pub struct BigQueryBatcher {
    schema: SchemaRef,
    builders: Vec<ColBuilder>,
    type_ids: Vec<u32>,
    batch_rows: usize,
    rows_in_batch: usize,
    pub rows_total: u64,
}

impl BigQueryBatcher {
    pub fn new(columns: &[ColumnType], batch_rows: usize) -> Result<Self> {
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
            rows_in_batch: 0,
            rows_total: 0,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Append one row; returns a flushed batch if `batch_rows` was reached.
    pub fn append_row(&mut self, row: &Row) -> Result<Option<RecordBatch>> {
        for (i, builder) in self.builders.iter_mut().enumerate() {
            builder.append_from_row(row, i, self.type_ids[i])?;
        }
        self.rows_in_batch += 1;
        self.rows_total += 1;
        if self.rows_in_batch >= self.batch_rows {
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
        RecordBatch::try_new(self.schema.clone(), cols).map_err(EtlError::from)
    }
}
