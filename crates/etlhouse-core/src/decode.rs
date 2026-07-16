//! Streaming decoder: PostgreSQL binary `COPY ... TO STDOUT (FORMAT binary)`
//! into Arrow `RecordBatch`es.
//!
//! `tokio-postgres`'s `copy_out` yields arbitrary `Bytes` chunks, so this is a
//! stateful parser: bytes are accumulated and as many complete tuples as
//! possible are decoded on each `feed`, flushing a `RecordBatch` every
//! `batch_rows` rows to keep memory bounded.
//!
//! Binary COPY layout (all integers big-endian):
//!   - 11-byte signature `PGCOPY\n\xff\r\n\0`, i32 flags, i32 header-ext length + ext bytes
//!   - per tuple: i16 field count, then per field: i32 length (-1 = NULL) + bytes
//!   - trailer: i16 field count = -1

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, StringBuilder, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder, UInt32Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::error::{EtlError, Result};
use crate::types::{oid, ColumnType};

const SIGNATURE: &[u8; 11] = b"PGCOPY\n\xff\r\n\0";
/// Days between 1970-01-01 (Arrow epoch) and 2000-01-01 (PostgreSQL epoch).
const PG_EPOCH_DAYS: i32 = 10_957;
/// Microseconds between the same two epochs.
const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

/// One Arrow column builder. Kept as a hand-rolled enum (rather than
/// `Box<dyn ArrayBuilder>`) so decode is monomorphic and NULL/append logic can
/// live next to the type it applies to.
enum ColBuilder {
    Bool(BooleanBuilder),
    I16(Int16Builder),
    I32(Int32Builder),
    I64(Int64Builder),
    U32(UInt32Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Str(StringBuilder),
    Bin(BinaryBuilder),
    Date(Date32Builder),
    Ts(TimestampMicrosecondBuilder, Option<Arc<str>>),
    Time(Time64MicrosecondBuilder),
}

impl ColBuilder {
    fn new(dt: &DataType) -> Result<Self> {
        Ok(match dt {
            DataType::Boolean => ColBuilder::Bool(BooleanBuilder::new()),
            DataType::Int16 => ColBuilder::I16(Int16Builder::new()),
            DataType::Int32 => ColBuilder::I32(Int32Builder::new()),
            DataType::Int64 => ColBuilder::I64(Int64Builder::new()),
            DataType::UInt32 => ColBuilder::U32(UInt32Builder::new()),
            DataType::Float32 => ColBuilder::F32(Float32Builder::new()),
            DataType::Float64 => ColBuilder::F64(Float64Builder::new()),
            DataType::Utf8 => ColBuilder::Str(StringBuilder::new()),
            DataType::Binary => ColBuilder::Bin(BinaryBuilder::new()),
            DataType::Date32 => ColBuilder::Date(Date32Builder::new()),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                ColBuilder::Ts(TimestampMicrosecondBuilder::new(), tz.clone())
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
            ColBuilder::I16(b) => b.append_null(),
            ColBuilder::I32(b) => b.append_null(),
            ColBuilder::I64(b) => b.append_null(),
            ColBuilder::U32(b) => b.append_null(),
            ColBuilder::F32(b) => b.append_null(),
            ColBuilder::F64(b) => b.append_null(),
            ColBuilder::Str(b) => b.append_null(),
            ColBuilder::Bin(b) => b.append_null(),
            ColBuilder::Date(b) => b.append_null(),
            ColBuilder::Ts(b, _) => b.append_null(),
            ColBuilder::Time(b) => b.append_null(),
        }
    }

    /// Decode a non-NULL field's raw binary bytes for the given PostgreSQL OID.
    fn append_value(&mut self, pg_oid: u32, buf: &[u8]) -> Result<()> {
        match self {
            ColBuilder::Bool(b) => b.append_value(buf.first().map(|&x| x != 0).unwrap_or(false)),
            ColBuilder::I16(b) => b.append_value(read_i16(buf)?),
            ColBuilder::I32(b) => b.append_value(read_i32(buf)?),
            ColBuilder::I64(b) => b.append_value(read_i64(buf)?),
            ColBuilder::U32(b) => b.append_value(read_i32(buf)? as u32),
            ColBuilder::F32(b) => b.append_value(f32::from_bits(read_i32(buf)? as u32)),
            ColBuilder::F64(b) => {
                // FLOAT8 arrives as 8 IEEE bytes; NUMERIC needs its own decode.
                let v = if pg_oid == oid::NUMERIC {
                    decode_numeric(buf)?
                } else {
                    f64::from_bits(read_i64(buf)? as u64)
                };
                b.append_value(v);
            }
            ColBuilder::Str(b) => {
                // jsonb wire format prefixes a 1-byte version header.
                let bytes = if pg_oid == oid::JSONB && !buf.is_empty() {
                    &buf[1..]
                } else {
                    buf
                };
                let s = std::str::from_utf8(bytes)
                    .map_err(|e| EtlError::decode(format!("invalid utf8: {e}")))?;
                b.append_value(s);
            }
            ColBuilder::Bin(b) => b.append_value(buf),
            ColBuilder::Date(b) => b.append_value(read_i32(buf)? + PG_EPOCH_DAYS),
            ColBuilder::Ts(b, _) => b.append_value(read_i64(buf)? + PG_EPOCH_MICROS),
            ColBuilder::Time(b) => b.append_value(read_i64(buf)?),
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            ColBuilder::Bool(b) => Arc::new(b.finish()),
            ColBuilder::I16(b) => Arc::new(b.finish()),
            ColBuilder::I32(b) => Arc::new(b.finish()),
            ColBuilder::I64(b) => Arc::new(b.finish()),
            ColBuilder::U32(b) => Arc::new(b.finish()),
            ColBuilder::F32(b) => Arc::new(b.finish()),
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
            ColBuilder::Time(b) => Arc::new(b.finish()),
        }
    }
}

fn read_i16(buf: &[u8]) -> Result<i16> {
    buf.get(0..2)
        .map(|b| i16::from_be_bytes([b[0], b[1]]))
        .ok_or_else(|| EtlError::decode("short int2"))
}
fn read_i32(buf: &[u8]) -> Result<i32> {
    buf.get(0..4)
        .map(|b| i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
        .ok_or_else(|| EtlError::decode("short int4"))
}
fn read_i64(buf: &[u8]) -> Result<i64> {
    buf.get(0..8)
        .map(|b| i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
        .ok_or_else(|| EtlError::decode("short int8"))
}

/// Decode PostgreSQL's `numeric` binary form to `f64` (approximate; exact
/// arbitrary precision is intentionally out of scope for v1).
fn decode_numeric(buf: &[u8]) -> Result<f64> {
    if buf.len() < 8 {
        return Err(EtlError::decode("short numeric header"));
    }
    let ndigits = i16::from_be_bytes([buf[0], buf[1]]) as usize;
    let weight = i16::from_be_bytes([buf[2], buf[3]]) as i32;
    let sign = u16::from_be_bytes([buf[4], buf[5]]);
    // buf[6..8] = dscale (display scale) — not needed for an f64 value.
    if sign == 0xC000 {
        return Ok(f64::NAN);
    }
    if buf.len() < 8 + ndigits * 2 {
        return Err(EtlError::decode("short numeric digits"));
    }
    let mut value = 0f64;
    for i in 0..ndigits {
        let off = 8 + i * 2;
        let digit = i16::from_be_bytes([buf[off], buf[off + 1]]) as f64;
        let power = weight - i as i32;
        value += digit * 10_000f64.powi(power);
    }
    if sign == 0x4000 {
        value = -value;
    }
    Ok(value)
}

/// Result of trying to parse a single tuple from the front of the buffer.
enum Parsed {
    /// A full tuple was decoded; this many bytes were consumed.
    Tuple(usize),
    /// The end-of-data trailer (`-1` field count) was seen.
    End,
    /// Not enough bytes yet; wait for more.
    NeedMore,
}

pub struct CopyDecoder {
    schema: SchemaRef,
    oids: Vec<u32>,
    builders: Vec<ColBuilder>,
    buf: Vec<u8>,
    header_done: bool,
    finished: bool,
    rows_in_batch: usize,
    batch_rows: usize,
    /// Total rows decoded across the whole stream.
    pub rows_total: u64,
}

impl CopyDecoder {
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
            oids: columns.iter().map(|c| c.pg_oid).collect(),
            builders,
            buf: Vec::with_capacity(1 << 16),
            header_done: false,
            finished: false,
            rows_in_batch: 0,
            batch_rows,
            rows_total: 0,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Feed a chunk of the COPY stream; returns any batches completed by it.
    pub fn feed(&mut self, chunk: &[u8]) -> Result<Vec<RecordBatch>> {
        self.buf.extend_from_slice(chunk);
        let mut out = Vec::new();
        let mut cursor = 0usize;

        if !self.header_done {
            match self.parse_header(&self.buf[cursor..])? {
                Some(n) => {
                    cursor += n;
                    self.header_done = true;
                }
                None => {
                    self.buf.drain(0..cursor);
                    return Ok(out);
                }
            }
        }

        loop {
            match self.parse_tuple(cursor)? {
                Parsed::Tuple(n) => {
                    cursor += n;
                    self.rows_in_batch += 1;
                    self.rows_total += 1;
                    if self.rows_in_batch >= self.batch_rows {
                        out.push(self.flush_batch()?);
                    }
                }
                Parsed::End => {
                    self.finished = true;
                    break;
                }
                Parsed::NeedMore => break,
            }
        }

        self.buf.drain(0..cursor);
        Ok(out)
    }

    /// Flush any remaining rows. Call once the COPY stream is exhausted.
    pub fn finish(&mut self) -> Result<Option<RecordBatch>> {
        if self.rows_in_batch > 0 {
            Ok(Some(self.flush_batch()?))
        } else {
            Ok(None)
        }
    }

    pub fn saw_trailer(&self) -> bool {
        self.finished
    }

    fn flush_batch(&mut self) -> Result<RecordBatch> {
        let cols: Vec<ArrayRef> = self.builders.iter_mut().map(|b| b.finish()).collect();
        self.rows_in_batch = 0;
        RecordBatch::try_new(self.schema.clone(), cols).map_err(EtlError::from)
    }

    fn parse_header(&self, buf: &[u8]) -> Result<Option<usize>> {
        // signature(11) + flags(4) + ext-len(4) + ext(ext-len)
        if buf.len() < 19 {
            return Ok(None);
        }
        if &buf[0..11] != SIGNATURE {
            return Err(EtlError::decode("bad COPY signature"));
        }
        let ext_len = read_i32(&buf[15..19])? as usize;
        let total = 19 + ext_len;
        if buf.len() < total {
            return Ok(None);
        }
        Ok(Some(total))
    }

    /// Try to parse one tuple starting at `start` in `self.buf`.
    fn parse_tuple(&mut self, start: usize) -> Result<Parsed> {
        let buf = &self.buf;
        if buf.len() < start + 2 {
            return Ok(Parsed::NeedMore);
        }
        let field_count = i16::from_be_bytes([buf[start], buf[start + 1]]);
        if field_count == -1 {
            return Ok(Parsed::End);
        }
        if field_count as usize != self.oids.len() {
            return Err(EtlError::decode(format!(
                "field count {field_count} != schema columns {}",
                self.oids.len()
            )));
        }

        // First pass: verify the whole tuple is buffered, collecting field spans.
        let mut pos = start + 2;
        let mut spans: Vec<Option<(usize, usize)>> = Vec::with_capacity(self.oids.len());
        for _ in 0..self.oids.len() {
            if buf.len() < pos + 4 {
                return Ok(Parsed::NeedMore);
            }
            let len = read_i32(&buf[pos..pos + 4])?;
            pos += 4;
            if len == -1 {
                spans.push(None);
            } else {
                let len = len as usize;
                if buf.len() < pos + len {
                    return Ok(Parsed::NeedMore);
                }
                spans.push(Some((pos, pos + len)));
                pos += len;
            }
        }

        // Second pass: append into builders now that the tuple is complete.
        for (i, span) in spans.iter().enumerate() {
            match span {
                None => self.builders[i].append_null(),
                Some((s, e)) => {
                    let oid = self.oids[i];
                    self.builders[i].append_value(oid, &self.buf[*s..*e])?;
                }
            }
        }
        Ok(Parsed::Tuple(pos - start))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Int32Array, StringArray};
    use arrow_schema::DataType;

    fn col(name: &str, oid: u32, dt: DataType, nullable: bool) -> ColumnType {
        ColumnType {
            name: name.into(),
            pg_oid: oid,
            nullable,
            arrow: dt,
            clickhouse_inner: "x".into(),
        }
    }

    /// Build a minimal valid binary COPY stream for two columns (int4, text)
    /// and two rows, one with a NULL text.
    fn sample_stream() -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(SIGNATURE);
        v.extend_from_slice(&0i32.to_be_bytes()); // flags
        v.extend_from_slice(&0i32.to_be_bytes()); // ext len

        // row 1: (42, "hi")
        v.extend_from_slice(&2i16.to_be_bytes());
        v.extend_from_slice(&4i32.to_be_bytes());
        v.extend_from_slice(&42i32.to_be_bytes());
        v.extend_from_slice(&2i32.to_be_bytes());
        v.extend_from_slice(b"hi");

        // row 2: (7, NULL)
        v.extend_from_slice(&2i16.to_be_bytes());
        v.extend_from_slice(&4i32.to_be_bytes());
        v.extend_from_slice(&7i32.to_be_bytes());
        v.extend_from_slice(&(-1i32).to_be_bytes());

        // trailer
        v.extend_from_slice(&(-1i16).to_be_bytes());
        v
    }

    #[test]
    fn decodes_rows_and_nulls() {
        let cols = vec![
            col("id", oid::INT4, DataType::Int32, false),
            col("name", oid::TEXT, DataType::Utf8, true),
        ];
        let mut dec = CopyDecoder::new(&cols, 1024).unwrap();
        let mut batches = dec.feed(&sample_stream()).unwrap();
        if let Some(b) = dec.finish().unwrap() {
            batches.push(b);
        }
        assert!(dec.saw_trailer());
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 42);
        assert_eq!(ids.value(1), 7);
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "hi");
        assert!(names.is_null(1));
    }

    #[test]
    fn handles_split_chunks() {
        // Feed the stream one byte at a time to exercise buffering.
        let cols = vec![
            col("id", oid::INT4, DataType::Int32, false),
            col("name", oid::TEXT, DataType::Utf8, true),
        ];
        let mut dec = CopyDecoder::new(&cols, 1024).unwrap();
        let stream = sample_stream();
        let mut batches = Vec::new();
        for byte in &stream {
            batches.extend(dec.feed(&[*byte]).unwrap());
        }
        if let Some(b) = dec.finish().unwrap() {
            batches.push(b);
        }
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    }
}
