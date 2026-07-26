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
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder, UInt32Builder,
};
use arrow_array::types::{Decimal128Type, DecimalType};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::decimal::{rescale_mantissa, Coercion};
use crate::error::{EtlError, Result};
use crate::types::{ch_range, oid, ColumnType};

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
    Decimal128(Decimal128Builder, u8, i8),
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
            DataType::Decimal128(p, s) => ColBuilder::Decimal128(
                Decimal128Builder::new().with_precision_and_scale(*p, *s)?,
                *p,
                *s,
            ),
            other => {
                // Reachable only if types.rs maps some OID to an Arrow type
                // this decoder doesn't implement a builder for — a mapping/
                // decoder mismatch, not anything a source column can trigger.
                return Err(EtlError::internal(format!(
                    "no column builder for Arrow type {other:?}"
                )));
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
            ColBuilder::Decimal128(b, _, _) => b.append_null(),
        }
    }

    /// Decode a non-NULL field's raw binary bytes for the given PostgreSQL OID.
    ///
    /// Returns [`Coercion::DateRange`] if the value was a valid date/datetime
    /// whose year is outside ClickHouse's representable window ([`ch_range`])
    /// and so was coerced to NULL rather than sent on to be rejected at
    /// insert time (which would abort the whole transfer) — PostgreSQL's
    /// DATE/TIMESTAMP range is far wider than ClickHouse's, so this is
    /// reachable with ordinary data. Returns [`Coercion::DecimalOverflow`]
    /// for a `numeric` value that doesn't fit the declared `Decimal(P,S)`
    /// override's precision, or is NaN/Infinity (`numeric` supports both;
    /// neither has a finite `Decimal128` representation).
    fn append_value(&mut self, pg_oid: u32, buf: &[u8]) -> Result<Coercion> {
        let mut coercion = Coercion::None;
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
                if pg_oid == oid::TIME {
                    // TIME arrives as an i64 of microseconds since midnight and
                    // maps to a ClickHouse String (no time-of-day type); render
                    // it as canonical "HH:MM:SS[.ffffff]" text.
                    b.append_value(format_pg_time(read_i64(buf)?));
                } else {
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
            }
            ColBuilder::Bin(b) => b.append_value(buf),
            ColBuilder::Date(b) => {
                let days = read_i32(buf)?.saturating_add(PG_EPOCH_DAYS);
                if ch_range::days_in_range(days) {
                    b.append_value(days);
                } else {
                    b.append_null();
                    coercion = Coercion::DateRange;
                }
            }
            ColBuilder::Ts(b, _) => {
                let micros = read_i64(buf)?.saturating_add(PG_EPOCH_MICROS);
                if ch_range::micros_in_range(micros) {
                    b.append_value(micros);
                } else {
                    b.append_null();
                    coercion = Coercion::DateRange;
                }
            }
            ColBuilder::Decimal128(b, p, s) => match parse_numeric_wire(buf)? {
                NumericWire::NanOrInf | NumericWire::MagnitudeOverflow => {
                    b.append_null();
                    coercion = Coercion::DecimalOverflow;
                }
                NumericWire::Value {
                    negative,
                    magnitude,
                    native_scale,
                } => match rescale_mantissa(magnitude, native_scale, *s as i32) {
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
            },
        }
        Ok(coercion)
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
            ColBuilder::Decimal128(b, _, _) => Arc::new(b.finish()),
        }
    }
}

/// Format a PostgreSQL `time` value (microseconds since midnight, always in
/// `[0, 24h)`) as canonical `HH:MM:SS[.ffffff]` text for a ClickHouse String
/// column. The fractional part is emitted only when non-zero, matching how
/// PostgreSQL and MySQL render TIME.
fn format_pg_time(micros: i64) -> String {
    let micros = micros.max(0);
    let sub = micros % 1_000_000;
    let total_secs = micros / 1_000_000;
    let (h, m, s) = (total_secs / 3600, (total_secs % 3600) / 60, total_secs % 60);
    if sub > 0 {
        format!("{h:02}:{m:02}:{s:02}.{sub:06}")
    } else {
        format!("{h:02}:{m:02}:{s:02}")
    }
}

// These "short" checks fail only if a field's declared wire length doesn't
// match what its resolved column type requires — i.e. the schema resolved by
// `resolve_columns` disagrees with what COPY is actually streaming (e.g. the
// table's schema changed between the two). Not something malformed source
// *data* can trigger on its own, so these are framed as internal errors.
fn read_i16(buf: &[u8]) -> Result<i16> {
    buf.get(0..2)
        .map(|b| i16::from_be_bytes([b[0], b[1]]))
        .ok_or_else(|| {
            EtlError::internal(format!(
                "expected a 2-byte int2 field, got {} byte(s)",
                buf.len()
            ))
        })
}
fn read_i32(buf: &[u8]) -> Result<i32> {
    buf.get(0..4)
        .map(|b| i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
        .ok_or_else(|| {
            EtlError::internal(format!(
                "expected a 4-byte int4 field, got {} byte(s)",
                buf.len()
            ))
        })
}
fn read_i64(buf: &[u8]) -> Result<i64> {
    buf.get(0..8)
        .map(|b| i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
        .ok_or_else(|| {
            EtlError::internal(format!(
                "expected an 8-byte int8 field, got {} byte(s)",
                buf.len()
            ))
        })
}

/// Decode PostgreSQL's `numeric` binary form to `f64` (approximate; used
/// only when no `Decimal(P,S)` override applies — see `parse_numeric_wire`
/// for the exact-precision path).
fn decode_numeric(buf: &[u8]) -> Result<f64> {
    if buf.len() < 8 {
        return Err(EtlError::internal(format!(
            "truncated numeric header: expected at least 8 bytes, got {}",
            buf.len()
        )));
    }
    let ndigits = i16::from_be_bytes([buf[0], buf[1]]) as usize;
    let weight = i16::from_be_bytes([buf[2], buf[3]]) as i32;
    let sign = u16::from_be_bytes([buf[4], buf[5]]);
    // buf[6..8] = dscale (display scale) — not needed for an f64 value.
    // 0xC000/0xD000/0xF000 are PostgreSQL's NaN/+Infinity/-Infinity
    // sentinels (the latter two added in PG14) — previously only NaN was
    // checked here, so a real `Infinity`/`-Infinity` numeric value silently
    // fell through the digit loop below (which sees no digit bytes, since
    // these sentinels carry `ndigits=0`) and decoded to a plain `0.0`.
    match sign {
        0xC000 => return Ok(f64::NAN),
        0xD000 => return Ok(f64::INFINITY),
        0xF000 => return Ok(f64::NEG_INFINITY),
        _ => {}
    }
    if buf.len() < 8 + ndigits * 2 {
        return Err(EtlError::internal(format!(
            "truncated numeric digits: expected {} more byte(s), got {}",
            8 + ndigits * 2,
            buf.len()
        )));
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

/// A PostgreSQL `numeric` binary payload parsed exactly (no `f64` round
/// trip): either a finite sign/magnitude/scale, or a non-finite sentinel.
/// `MagnitudeOverflow` covers a value with more base-10000 digits than fit
/// in an i128 even before any rescaling is attempted — the same "too large
/// for any Decimal128" category as a post-rescale precision overflow (see
/// `append_value`'s `ColBuilder::Decimal128` arm), not a hard error.
enum NumericWire {
    Value {
        negative: bool,
        magnitude: i128,
        native_scale: i32,
    },
    MagnitudeOverflow,
    NanOrInf,
}

/// Parse PostgreSQL's `numeric` binary wire format — header
/// (`ndigits:i16, weight:i16, sign:u16, dscale:i16`), then `ndigits`
/// big-endian `i16` base-10000 digits — into an exact `(sign, magnitude,
/// native_scale)`. `magnitude` is the digits read as one big base-10000
/// integer (Horner's method: `magnitude = magnitude*10000 + digit`, digits
/// in wire order from most to least significant); at that construction,
/// `magnitude` is the unscaled value at `native_scale = (ndigits - 1 -
/// weight) * 4` (a multiple of 4, and possibly negative for a large whole
/// number) — see `rescale_mantissa` to convert to an arbitrary target scale.
fn parse_numeric_wire(buf: &[u8]) -> Result<NumericWire> {
    if buf.len() < 8 {
        return Err(EtlError::internal(format!(
            "truncated numeric header: expected at least 8 bytes, got {}",
            buf.len()
        )));
    }
    let ndigits = i16::from_be_bytes([buf[0], buf[1]]) as usize;
    let weight = i16::from_be_bytes([buf[2], buf[3]]) as i32;
    let sign = u16::from_be_bytes([buf[4], buf[5]]);
    // buf[6..8] = dscale — irrelevant here; the target scale comes from the
    // Decimal128(P,S) override, not the source's own display scale.
    if matches!(sign, 0xC000 | 0xD000 | 0xF000) {
        return Ok(NumericWire::NanOrInf);
    }
    if buf.len() < 8 + ndigits * 2 {
        return Err(EtlError::internal(format!(
            "truncated numeric digits: expected {} more byte(s), got {}",
            8 + ndigits * 2,
            buf.len()
        )));
    }
    let mut magnitude: i128 = 0;
    for i in 0..ndigits {
        let off = 8 + i * 2;
        let digit = i16::from_be_bytes([buf[off], buf[off + 1]]) as i128;
        magnitude = match magnitude
            .checked_mul(10_000)
            .and_then(|m| m.checked_add(digit))
        {
            Some(m) => m,
            None => return Ok(NumericWire::MagnitudeOverflow),
        };
    }
    Ok(NumericWire::Value {
        negative: sign == 0x4000,
        magnitude,
        native_scale: (ndigits as i32 - 1 - weight) * 4,
    })
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
    bytes_in_batch: usize,
    batch_rows: usize,
    batch_bytes: usize,
    /// Total rows decoded across the whole stream.
    pub rows_total: u64,
    /// Count of valid dates/datetimes whose year fell outside ClickHouse's
    /// representable window and were coerced to NULL (see `ColBuilder::append_value`).
    pub invalid_dates_total: u64,
    /// Count of `numeric` values coerced to NULL because they overflowed a
    /// `Decimal(P,S)` override's precision, or were NaN/Infinity (see
    /// `ColBuilder::append_value`'s `Decimal128` arm).
    pub invalid_decimals_total: u64,
}

impl CopyDecoder {
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
            oids: columns.iter().map(|c| c.type_id).collect(),
            builders,
            buf: Vec::with_capacity(1 << 16),
            header_done: false,
            finished: false,
            rows_in_batch: 0,
            bytes_in_batch: 0,
            batch_rows,
            batch_bytes,
            rows_total: 0,
            invalid_dates_total: 0,
            invalid_decimals_total: 0,
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
                    self.bytes_in_batch += n;
                    if self.rows_in_batch >= self.batch_rows
                        || (self.batch_bytes > 0 && self.bytes_in_batch >= self.batch_bytes)
                    {
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
        self.bytes_in_batch = 0;
        RecordBatch::try_new(self.schema.clone(), cols).map_err(EtlError::from)
    }

    fn parse_header(&self, buf: &[u8]) -> Result<Option<usize>> {
        // signature(11) + flags(4) + ext-len(4) + ext(ext-len)
        if buf.len() < 19 {
            return Ok(None);
        }
        if &buf[0..11] != SIGNATURE {
            // The stream doesn't start with PGCOPY's fixed magic bytes — only
            // possible if something other than `COPY ... (FORMAT binary)` fed
            // this decoder, not from any content a source table could hold.
            return Err(EtlError::internal(
                "COPY stream did not start with the expected PGCOPY binary signature",
            ));
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
            // The row's field count disagrees with the resolved schema's
            // column count — a decoder/schema mismatch, not something bad
            // source data alone can cause.
            return Err(EtlError::internal(format!(
                "row has {field_count} field(s) but the resolved schema has {} column(s)",
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
                    let coercion = self.builders[i]
                        .append_value(oid, &self.buf[*s..*e])
                        .map_err(|err| {
                            err.context(format!("column '{}'", self.schema.field(i).name()))
                        })?;
                    match coercion {
                        Coercion::None => {}
                        Coercion::DateRange => self.invalid_dates_total += 1,
                        Coercion::DecimalOverflow => self.invalid_decimals_total += 1,
                    }
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
            type_id: oid,
            nullable,
            arrow: dt,
            clickhouse_inner: "x".into(),
            arbitrary_precision_decimal: false,
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

    /// Golden type matrix for the PostgreSQL source: for every OID `map_oid`
    /// produces, assert the full type chain END-TO-END from the source type —
    /// (1) mapping to Arrow + ClickHouse inner, (2) the decoder's output array
    /// type carries the correct timezone, (3) the batch-vs-schema check
    /// (`RecordBatch::try_new`, the literal `flush_batch` validation) passes,
    /// and (4) the Arrow→BigQuery destination-type bridge. Starting from the
    /// OID (not a hand-built Arrow type) is what makes this catch a mapping
    /// revert — the exact class of regression that shipped in 0.3.4 for MySQL
    /// (there was no equivalent Postgres tz coverage at all before this).
    #[test]
    fn postgres_type_golden_matrix() {
        use crate::types::bigquery::arrow_to_bigquery_type as a2b;
        use crate::types::{map_oid, oid};
        use arrow_schema::{Field, Schema, TimeUnit};
        use google_cloud_bigquery::http::table::TableFieldType as Bq;
        use std::sync::Arc;

        let ts = |tz: Option<&str>| DataType::Timestamp(TimeUnit::Microsecond, tz.map(Arc::from));
        // (oid, expected Arrow, expected ClickHouse inner, expected BigQuery type)
        let rows: Vec<(u32, DataType, &str, Bq)> = vec![
            (oid::BOOL, DataType::Boolean, "Bool", Bq::Boolean),
            (oid::INT2, DataType::Int16, "Int16", Bq::Integer),
            (oid::INT4, DataType::Int32, "Int32", Bq::Integer),
            (oid::INT8, DataType::Int64, "Int64", Bq::Integer),
            (oid::OID, DataType::UInt32, "UInt32", Bq::Integer),
            (oid::FLOAT4, DataType::Float32, "Float32", Bq::Float),
            (oid::FLOAT8, DataType::Float64, "Float64", Bq::Float),
            (oid::NUMERIC, DataType::Float64, "Float64", Bq::Float),
            (oid::TEXT, DataType::Utf8, "String", Bq::String),
            (oid::UUID, DataType::Utf8, "UUID", Bq::String),
            (oid::BYTEA, DataType::Binary, "String", Bq::Bytes),
            (oid::DATE, DataType::Date32, "Date32", Bq::Date),
            (oid::TIMESTAMP, ts(None), "DateTime64(6)", Bq::Datetime),
            (
                oid::TIMESTAMPTZ,
                ts(Some("UTC")),
                "DateTime64(6, 'UTC')",
                Bq::Timestamp,
            ),
            (oid::TIME, DataType::Utf8, "String", Bq::String),
        ];
        for (o, arrow, ch, bq) in rows {
            let (mapped_arrow, mapped_ch) =
                map_oid(o).unwrap_or_else(|| panic!("oid {o} unmapped"));
            assert_eq!(mapped_arrow, arrow, "oid {o}: Arrow type");
            assert_eq!(mapped_ch, ch, "oid {o}: ClickHouse inner");
            // Decoder output type (incl. tz) must equal the resolved schema type.
            let mut b = ColBuilder::new(&arrow).unwrap();
            let out = b.finish();
            assert_eq!(out.data_type(), &arrow, "oid {o}: decoder output type/tz");
            // The literal flush_batch validation must accept it.
            let schema = Arc::new(Schema::new(vec![Field::new("c", arrow.clone(), true)]));
            RecordBatch::try_new(schema, vec![out])
                .unwrap_or_else(|e| panic!("oid {o}: batch: {e}"));
            // Destination bridge (Arrow -> BigQuery column type).
            assert_eq!(a2b(&arrow), Some(bq), "oid {o}: BigQuery type");
        }
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

    /// A COPY stream of `n` rows, each `(id: int4, text of `payload_len` bytes)`.
    fn wide_rows_stream(n: usize, payload_len: usize) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(SIGNATURE);
        v.extend_from_slice(&0i32.to_be_bytes());
        v.extend_from_slice(&0i32.to_be_bytes());
        let payload = vec![b'x'; payload_len];
        for i in 0..n {
            v.extend_from_slice(&2i16.to_be_bytes());
            v.extend_from_slice(&4i32.to_be_bytes());
            v.extend_from_slice(&(i as i32).to_be_bytes());
            v.extend_from_slice(&(payload_len as i32).to_be_bytes());
            v.extend_from_slice(&payload);
        }
        v.extend_from_slice(&(-1i16).to_be_bytes());
        v
    }

    #[test]
    fn batch_bytes_flushes_before_batch_rows_for_wide_rows() {
        let cols = vec![
            col("id", oid::INT4, DataType::Int32, false),
            col("payload", oid::TEXT, DataType::Utf8, false),
        ];
        // 10 rows of ~100 bytes each; batch_rows is high enough to never
        // trigger on its own, but batch_bytes should force multiple flushes.
        let mut dec = CopyDecoder::with_batch_bytes(&cols, 1_000, 250).unwrap();
        let mut batches = dec.feed(&wide_rows_stream(10, 100)).unwrap();
        if let Some(b) = dec.finish().unwrap() {
            batches.push(b);
        }
        assert!(dec.saw_trailer());
        assert!(
            batches.len() > 1,
            "expected batch_bytes to force multiple flushes, got {}",
            batches.len()
        );
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 10);
        // No single batch should wildly exceed the byte budget.
        for b in &batches {
            assert!(
                b.num_rows() <= 3,
                "batch too large for a 250-byte budget at ~106B/row"
            );
        }
    }

    #[test]
    fn batch_bytes_zero_disables_byte_based_flush() {
        let cols = vec![
            col("id", oid::INT4, DataType::Int32, false),
            col("payload", oid::TEXT, DataType::Utf8, false),
        ];
        let mut dec = CopyDecoder::with_batch_bytes(&cols, 1_000, 0).unwrap();
        let mut batches = dec.feed(&wide_rows_stream(10, 100)).unwrap();
        if let Some(b) = dec.finish().unwrap() {
            batches.push(b);
        }
        assert_eq!(
            batches.len(),
            1,
            "batch_bytes=0 should leave row count as the only trigger"
        );
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

    /// Build a PostgreSQL `numeric` binary payload: header
    /// (ndigits/weight/sign/dscale=0) followed by `digits`, each a
    /// base-10000 group in `[0, 9999]`.
    fn numeric_wire(weight: i16, sign: u16, digits: &[i16]) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&(digits.len() as i16).to_be_bytes());
        v.extend_from_slice(&weight.to_be_bytes());
        v.extend_from_slice(&sign.to_be_bytes());
        v.extend_from_slice(&0i16.to_be_bytes()); // dscale — unused by parse_numeric_wire
        for d in digits {
            v.extend_from_slice(&d.to_be_bytes());
        }
        v
    }

    fn decimal_value(b: &mut ColBuilder, buf: &[u8]) -> (Coercion, ArrayRef) {
        let coercion = b.append_value(oid::NUMERIC, buf).unwrap();
        (coercion, b.finish())
    }

    #[test]
    fn decimal_decodes_exact_value_with_no_rounding_needed() {
        // digits=[12, 3400], weight=0 -> native_scale=4, magnitude=123400,
        // i.e. the value 12.3400. Target scale matches native scale exactly.
        let mut b = ColBuilder::new(&DataType::Decimal128(10, 4)).unwrap();
        let (coercion, arr) = decimal_value(&mut b, &numeric_wire(0, 0x0000, &[12, 3400]));
        assert_eq!(coercion, Coercion::None);
        let arr = arr
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), 123_400);
    }

    #[test]
    fn decimal_rounds_half_away_from_zero_when_narrowing() {
        // Same shape as above but digits=[12, 3450] (12.3450), narrowed to
        // scale 2: 123450 / 100 = 1234 remainder 50 -> rounds up to 1235
        // (12.35), not truncated to 1234 (12.34).
        let mut b = ColBuilder::new(&DataType::Decimal128(10, 2)).unwrap();
        let (coercion, arr) = decimal_value(&mut b, &numeric_wire(0, 0x0000, &[12, 3450]));
        assert_eq!(coercion, Coercion::None);
        let arr = arr
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), 1235);
    }

    #[test]
    fn decimal_negative_value_round_trips_exactly() {
        let mut b = ColBuilder::new(&DataType::Decimal128(10, 4)).unwrap();
        let (coercion, arr) = decimal_value(&mut b, &numeric_wire(0, 0x4000, &[12, 3400]));
        assert_eq!(coercion, Coercion::None);
        let arr = arr
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), -123_400);
    }

    #[test]
    fn decimal_coerces_to_null_when_value_overflows_declared_precision() {
        // 1234 (a 4-digit whole number) doesn't fit Decimal(3, 0)'s max of 999.
        let mut b = ColBuilder::new(&DataType::Decimal128(3, 0)).unwrap();
        let (coercion, arr) = decimal_value(&mut b, &numeric_wire(0, 0x0000, &[1234]));
        assert_eq!(coercion, Coercion::DecimalOverflow);
        let arr = arr
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert!(arr.is_null(0));
    }

    #[test]
    fn decimal_coerces_nan_and_infinity_to_null() {
        for sign in [0xC000u16, 0xD000, 0xF000] {
            let mut b = ColBuilder::new(&DataType::Decimal128(10, 2)).unwrap();
            let (coercion, arr) = decimal_value(&mut b, &numeric_wire(0, sign, &[]));
            assert_eq!(coercion, Coercion::DecimalOverflow, "sign {sign:#06x}");
            let arr = arr
                .as_any()
                .downcast_ref::<arrow_array::Decimal128Array>()
                .unwrap();
            assert!(arr.is_null(0), "sign {sign:#06x}");
        }
    }

    #[test]
    fn decimal_zero_does_not_overflow_at_a_wide_target_scale() {
        // Regression test: PostgreSQL encodes a literal zero as ndigits=0,
        // weight=0, giving native_scale=-4. Rescaling that to a wide target
        // scale (e.g. 30) previously (without rescale_mantissa's zero
        // short-circuit) needed 10^34, overflowing i128 and wrongly nulling
        // out a plain zero.
        let mut b = ColBuilder::new(&DataType::Decimal128(38, 30)).unwrap();
        let (coercion, arr) = decimal_value(&mut b, &numeric_wire(0, 0x0000, &[]));
        assert_eq!(coercion, Coercion::None);
        let arr = arr
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), 0);
    }

    #[test]
    fn decode_numeric_f64_fallback_handles_nan_and_infinity() {
        // The plain (non-overridden) Float64 path: previously only NaN was
        // checked, so a real Infinity/-Infinity value silently decoded to
        // 0.0 instead of representing it (or erroring).
        assert!(decode_numeric(&numeric_wire(0, 0xC000, &[]))
            .unwrap()
            .is_nan());
        assert_eq!(
            decode_numeric(&numeric_wire(0, 0xD000, &[])).unwrap(),
            f64::INFINITY
        );
        assert_eq!(
            decode_numeric(&numeric_wire(0, 0xF000, &[])).unwrap(),
            f64::NEG_INFINITY
        );
    }
}
