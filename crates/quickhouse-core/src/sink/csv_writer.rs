//! The text a [`super::csv::CsvSink`] writes: RFC 4180 fields over Arrow
//! batches, and one part file (new in 0.21.0).
//!
//! `arrow::csv`'s writer was the first candidate and doesn't fit: it writes a
//! NULL and an empty string the same way (an empty field, or one `with_null`
//! string for both), with no way to quote one field and not another, and it
//! writes a UTC timestamp with a `+00:00` offset and binary as hex. So the
//! fields are framed here, and Arrow's own display formatting
//! ([`ArrayFormatter`]) still renders the values it renders exactly: integers,
//! floats (shortest round-trip text, `NaN`, `inf`), decimals at their scale.
//!
//! The rules, which `docs/guide/destinations/csv.md` documents:
//!
//! - A NULL is an empty field. A non-NULL value that is empty (an empty
//!   string, empty bytes) is written quoted, `""`, so the two read apart.
//! - A field is quoted when it holds the delimiter, a double quote, CR or LF,
//!   with each double quote doubled.
//! - Rows end with `\n`.
//! - A date is `YYYY-MM-DD`. A timestamp is ISO 8601 with a `T`, with as many
//!   fractional digits as its unit holds (6 for microseconds, the unit every
//!   source decodes to). A timezone-aware one is written in UTC with a `Z`; a
//!   naive one has no offset.
//! - A boolean is `true` / `false`. Binary is base64 (standard alphabet,
//!   padded), as BigQuery exports `BYTES`.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use arrow::util::display::{ArrayFormatter, FormatOptions};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use base64::Engine as _;
use serde::{Deserialize, Serialize};

use crate::config::CsvCompression;
use crate::error::{EtlError, Result};

/// Formatted rows are handed to the file in pieces about this big, so a large
/// insert never holds its whole CSV text in memory at once.
const FLUSH_BYTES: usize = 1 << 20;

/// Days from 0001-01-01 (chrono's day 1 of the common era) to 1970-01-01.
const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719_163;

/// How a part's text is laid out: what the manifest records and an append
/// must match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PartFormat {
    pub delimiter: u8,
    pub header: bool,
    pub compression: CsvCompression,
}

impl PartFormat {
    /// The part file name for run `run_id`'s `n`th part.
    pub(crate) fn file_name(&self, run_id: &str, n: u64) -> String {
        let ext = match self.compression {
            CsvCompression::None => "csv",
            CsvCompression::Gzip => "csv.gz",
        };
        format!("part-{run_id}-{n:05}.{ext}")
    }
}

/// One finished part, as the manifest lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PartInfo {
    /// The file name, inside the table's folder.
    pub file: String,
    /// Data rows, not counting the header.
    pub rows: u64,
    /// The file's size on disk (compressed, for gzip).
    pub bytes: u64,
}

/// Append `value` as one field: quoted when it is empty or holds the
/// delimiter, a double quote, CR or LF, each double quote doubled. Empty is
/// quoted because a NULL is the empty field.
pub(crate) fn push_field(out: &mut Vec<u8>, value: &[u8], delimiter: u8) {
    if value.is_empty() {
        out.extend_from_slice(b"\"\"");
        return;
    }
    let plain = !value
        .iter()
        .any(|&b| b == delimiter || b == b'"' || b == b'\n' || b == b'\r');
    if plain {
        out.extend_from_slice(value);
        return;
    }
    out.push(b'"');
    for &b in value {
        if b == b'"' {
            out.push(b'"');
        }
        out.push(b);
    }
    out.push(b'"');
}

/// Append the header row: the column names, as fields.
pub(crate) fn push_header(out: &mut Vec<u8>, names: &[String], delimiter: u8) {
    for (i, name) in names.iter().enumerate() {
        if i > 0 {
            out.push(delimiter);
        }
        push_field(out, name.as_bytes(), delimiter);
    }
    out.push(b'\n');
}

/// How one column's non-NULL values become text.
enum Encoder<'a> {
    Utf8(&'a arrow_array::StringArray),
    LargeUtf8(&'a arrow_array::LargeStringArray),
    Binary(&'a arrow_array::BinaryArray),
    LargeBinary(&'a arrow_array::LargeBinaryArray),
    FixedBinary(&'a arrow_array::FixedSizeBinaryArray),
    Boolean(&'a arrow_array::BooleanArray),
    /// Days since 1970-01-01.
    Date32(&'a [i32]),
    /// Milliseconds since 1970-01-01, a whole number of days.
    Date64(&'a [i64]),
    Timestamp {
        values: &'a [i64],
        unit: TimeUnit,
        utc: bool,
    },
    /// A column whose every value is NULL.
    Null,
    /// Everything Arrow's display already writes exactly: integers, floats,
    /// decimals, times, durations.
    Other(ArrayFormatter<'a>),
}

/// One column of a batch being written.
struct Column<'a> {
    array: &'a dyn Array,
    encoder: Encoder<'a>,
}

impl<'a> Column<'a> {
    fn new(array: &'a dyn Array, name: &str) -> Result<Self> {
        let encoder = match array.data_type() {
            DataType::Utf8 => Encoder::Utf8(array.as_string::<i32>()),
            DataType::LargeUtf8 => Encoder::LargeUtf8(array.as_string::<i64>()),
            DataType::Binary => Encoder::Binary(array.as_binary::<i32>()),
            DataType::LargeBinary => Encoder::LargeBinary(array.as_binary::<i64>()),
            DataType::FixedSizeBinary(_) => Encoder::FixedBinary(array.as_fixed_size_binary()),
            DataType::Boolean => Encoder::Boolean(array.as_boolean()),
            DataType::Date32 => Encoder::Date32(array.as_primitive::<Date32Type>().values()),
            DataType::Date64 => Encoder::Date64(array.as_primitive::<Date64Type>().values()),
            DataType::Timestamp(unit, tz) => {
                let values: &[i64] = match unit {
                    TimeUnit::Second => array.as_primitive::<TimestampSecondType>().values(),
                    TimeUnit::Millisecond => {
                        array.as_primitive::<TimestampMillisecondType>().values()
                    }
                    TimeUnit::Microsecond => {
                        array.as_primitive::<TimestampMicrosecondType>().values()
                    }
                    TimeUnit::Nanosecond => {
                        array.as_primitive::<TimestampNanosecondType>().values()
                    }
                };
                Encoder::Timestamp {
                    values,
                    unit: *unit,
                    utc: tz.is_some(),
                }
            }
            DataType::Null => Encoder::Null,
            other if other.is_nested() => {
                return Err(EtlError::config(format!(
                    "column '{name}' is a nested type ({other}), which has no CSV text; \
                     serialize it to a string (JSON, say) in the source"
                )))
            }
            _ => Encoder::Other(
                ArrayFormatter::try_new(array, &FormatOptions::default())
                    .map_err(EtlError::from)?,
            ),
        };
        Ok(Column { array, encoder })
    }

    /// Append row `i`'s field: nothing for a NULL.
    fn push(&self, out: &mut Vec<u8>, i: usize, delimiter: u8, scratch: &mut String) -> Result<()> {
        if self.array.is_null(i) {
            return Ok(());
        }
        match &self.encoder {
            Encoder::Utf8(a) => push_field(out, a.value(i).as_bytes(), delimiter),
            Encoder::LargeUtf8(a) => push_field(out, a.value(i).as_bytes(), delimiter),
            Encoder::Binary(a) => push_base64(out, a.value(i), delimiter, scratch),
            Encoder::LargeBinary(a) => push_base64(out, a.value(i), delimiter, scratch),
            Encoder::FixedBinary(a) => push_base64(out, a.value(i), delimiter, scratch),
            Encoder::Boolean(a) => {
                out.extend_from_slice(if a.value(i) { b"true" } else { b"false" })
            }
            Encoder::Date32(v) => {
                scratch.clear();
                write_date(scratch, v[i])?;
                push_field(out, scratch.as_bytes(), delimiter);
            }
            Encoder::Date64(v) => {
                scratch.clear();
                let days = v[i].div_euclid(86_400_000);
                let days = i32::try_from(days)
                    .map_err(|_| EtlError::decode(format!("date {} ms is out of range", v[i])))?;
                write_date(scratch, days)?;
                push_field(out, scratch.as_bytes(), delimiter);
            }
            Encoder::Timestamp { values, unit, utc } => {
                scratch.clear();
                write_timestamp(scratch, values[i], *unit, *utc)?;
                push_field(out, scratch.as_bytes(), delimiter);
            }
            Encoder::Null => {}
            Encoder::Other(f) => {
                scratch.clear();
                f.value(i)
                    .write(scratch)
                    .map_err(|e| EtlError::decode(format!("could not format a value: {e}")))?;
                push_field(out, scratch.as_bytes(), delimiter);
            }
        }
        Ok(())
    }
}

fn push_base64(out: &mut Vec<u8>, bytes: &[u8], delimiter: u8, scratch: &mut String) {
    scratch.clear();
    base64::engine::general_purpose::STANDARD.encode_string(bytes, scratch);
    push_field(out, scratch.as_bytes(), delimiter);
}

/// A year as ISO 8601 writes it: four digits, signed outside 0000..=9999.
fn write_year(out: &mut String, year: i32) -> std::fmt::Result {
    use std::fmt::Write as _;
    if (0..=9999).contains(&year) {
        write!(out, "{year:04}")
    } else {
        write!(out, "{year:+05}")
    }
}

/// `YYYY-MM-DD` for `days` since 1970-01-01.
fn write_date(out: &mut String, days: i32) -> Result<()> {
    use chrono::Datelike;
    use std::fmt::Write as _;
    let date = days
        .checked_add(UNIX_EPOCH_DAYS_FROM_CE)
        .and_then(chrono::NaiveDate::from_num_days_from_ce_opt)
        .ok_or_else(|| EtlError::decode(format!("date {days} days from 1970 is out of range")))?;
    let fmt = |e: std::fmt::Error| EtlError::decode(format!("could not format a date: {e}"));
    write_year(out, date.year()).map_err(fmt)?;
    write!(out, "-{:02}-{:02}", date.month(), date.day()).map_err(fmt)
}

/// ISO 8601 for `value` in `unit` since 1970-01-01 00:00:00 UTC: with a `Z`
/// when the column is timezone-aware (Arrow stores those as UTC instants),
/// and as the wall-clock reading it is, with no offset, when it is naive.
fn write_timestamp(out: &mut String, value: i64, unit: TimeUnit, utc: bool) -> Result<()> {
    use chrono::{Datelike, Timelike};
    use std::fmt::Write as _;
    let (per_sec, digits): (i64, usize) = match unit {
        TimeUnit::Second => (1, 0),
        TimeUnit::Millisecond => (1_000, 3),
        TimeUnit::Microsecond => (1_000_000, 6),
        TimeUnit::Nanosecond => (1_000_000_000, 9),
    };
    let secs = value.div_euclid(per_sec);
    let frac = value.rem_euclid(per_sec);
    let nanos = (frac * (1_000_000_000 / per_sec)) as u32;
    let t = chrono::DateTime::from_timestamp(secs, nanos)
        .ok_or_else(|| EtlError::decode(format!("timestamp {value} ({unit:?}) is out of range")))?
        .naive_utc();
    let fmt = |e: std::fmt::Error| EtlError::decode(format!("could not format a timestamp: {e}"));
    write_year(out, t.year()).map_err(fmt)?;
    write!(
        out,
        "-{:02}-{:02}T{:02}:{:02}:{:02}",
        t.month(),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    )
    .map_err(fmt)?;
    if digits > 0 {
        write!(out, ".{frac:0digits$}").map_err(fmt)?;
    }
    if utc {
        out.push('Z');
    }
    Ok(())
}

/// The bytes a part file goes through: plain, or a gzip stream.
enum Out {
    Plain(BufWriter<File>),
    Gzip(flate2::write::GzEncoder<BufWriter<File>>),
}

impl Out {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        match self {
            Out::Plain(w) => w.write_all(bytes),
            Out::Gzip(w) => w.write_all(bytes),
        }
    }

    /// Flush everything (the gzip trailer too) and hand back the file.
    fn finish(self) -> std::io::Result<File> {
        let buffered = match self {
            Out::Plain(w) => w,
            Out::Gzip(w) => w.finish()?,
        };
        buffered.into_inner().map_err(|e| e.into_error())
    }
}

/// One part file being written. Rows go in through [`Self::write`];
/// [`Self::finish`] flushes, syncs and closes it.
pub(crate) struct PartWriter {
    path: PathBuf,
    file: String,
    out: Out,
    delimiter: u8,
    rows: u64,
    buf: Vec<u8>,
}

impl PartWriter {
    /// Create `dir/file` (which must not exist yet), with the header row when
    /// `format` asks for one.
    pub(crate) fn create(
        dir: &Path,
        file: String,
        format: &PartFormat,
        names: &[String],
    ) -> Result<Self> {
        let path = dir.join(&file);
        let f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| io_error("create", &path, e))?;
        let buffered = BufWriter::with_capacity(256 * 1024, f);
        let out = match format.compression {
            CsvCompression::None => Out::Plain(buffered),
            CsvCompression::Gzip => Out::Gzip(flate2::write::GzEncoder::new(
                buffered,
                flate2::Compression::default(),
            )),
        };
        let mut writer = PartWriter {
            path,
            file,
            out,
            delimiter: format.delimiter,
            rows: 0,
            buf: Vec::with_capacity(FLUSH_BYTES + 64 * 1024),
        };
        if format.header {
            push_header(&mut writer.buf, names, format.delimiter);
            writer.flush_buf()?;
        }
        Ok(writer)
    }

    /// Data rows written so far.
    pub(crate) fn rows(&self) -> u64 {
        self.rows
    }

    /// Append `batch`'s rows. Returns the bytes of CSV text they came to
    /// (before compression).
    pub(crate) fn write(&mut self, batch: &RecordBatch) -> Result<u64> {
        let schema = batch.schema();
        let columns = batch
            .columns()
            .iter()
            .zip(schema.fields())
            .map(|(a, f)| Column::new(a.as_ref(), f.name()))
            .collect::<Result<Vec<_>>>()?;
        let mut scratch = String::new();
        let mut written = 0u64;
        for row in 0..batch.num_rows() {
            for (i, column) in columns.iter().enumerate() {
                if i > 0 {
                    self.buf.push(self.delimiter);
                }
                column.push(&mut self.buf, row, self.delimiter, &mut scratch)?;
            }
            self.buf.push(b'\n');
            if self.buf.len() >= FLUSH_BYTES {
                written += self.buf.len() as u64;
                self.flush_buf()?;
            }
        }
        written += self.buf.len() as u64;
        self.flush_buf()?;
        self.rows += batch.num_rows() as u64;
        Ok(written)
    }

    fn flush_buf(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.out
            .write_all(&self.buf)
            .map_err(|e| io_error("write", &self.path, e))?;
        self.buf.clear();
        Ok(())
    }

    /// Flush, sync to disk and close the file.
    pub(crate) fn finish(mut self) -> Result<PartInfo> {
        self.flush_buf()?;
        let path = self.path;
        let file = self.out.finish().map_err(|e| io_error("write", &path, e))?;
        file.sync_all().map_err(|e| io_error("sync", &path, e))?;
        let bytes = file
            .metadata()
            .map_err(|e| io_error("read the size of", &path, e))?
            .len();
        Ok(PartInfo {
            file: self.file,
            rows: self.rows,
            bytes,
        })
    }
}

/// An I/O failure on `path`, naming what was being done.
pub(crate) fn io_error(action: &str, path: &Path, e: std::io::Error) -> EtlError {
    EtlError::other(format!(
        "csv destination: could not {action} '{}': {e}",
        path.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::builder::{Decimal128Builder, StringBuilder};
    use arrow_array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Float64Array, Int64Array,
        TimestampMicrosecondArray,
    };
    use arrow_schema::{Field, Schema};

    fn text(batch: &RecordBatch, delimiter: u8) -> String {
        let schema = batch.schema();
        let columns = batch
            .columns()
            .iter()
            .zip(schema.fields())
            .map(|(a, f)| Column::new(a.as_ref(), f.name()).unwrap())
            .collect::<Vec<_>>();
        let mut out = Vec::new();
        let mut scratch = String::new();
        for row in 0..batch.num_rows() {
            for (i, c) in columns.iter().enumerate() {
                if i > 0 {
                    out.push(delimiter);
                }
                c.push(&mut out, row, delimiter, &mut scratch).unwrap();
            }
            out.push(b'\n');
        }
        String::from_utf8(out).unwrap()
    }

    fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let fields = columns
            .iter()
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns.into_iter().map(|(_, a)| a).collect(),
        )
        .unwrap()
    }

    #[test]
    fn a_null_is_empty_and_an_empty_string_is_quoted() {
        let mut s = StringBuilder::new();
        s.append_value("plain");
        s.append_null();
        s.append_value("");
        s.append_value("a,b");
        s.append_value("say \"hi\"");
        s.append_value("two\nlines");
        s.append_value("cr\rhere");
        let b = batch(vec![("s", Arc::new(s.finish()) as ArrayRef)]);
        assert_eq!(
            text(&b, b','),
            "plain\n\n\"\"\n\"a,b\"\n\"say \"\"hi\"\"\"\n\"two\nlines\"\n\"cr\rhere\"\n"
        );
        // Only the delimiter in use is quoted for.
        let b = batch(vec![(
            "s",
            Arc::new(arrow_array::StringArray::from(vec!["a,b", "a;b"])) as ArrayRef,
        )]);
        assert_eq!(text(&b, b';'), "a,b\n\"a;b\"\n");
    }

    #[test]
    fn dates_timestamps_decimals_booleans_and_binary() {
        let mut dec = Decimal128Builder::new()
            .with_precision_and_scale(12, 2)
            .unwrap();
        dec.append_value(1250);
        dec.append_value(-5);
        dec.append_null();
        let b = batch(vec![
            (
                "d",
                Arc::new(Date32Array::from(vec![Some(19_723), Some(-1), None])) as ArrayRef,
            ),
            (
                "naive",
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1_704_164_645_123_456),
                    Some(-1),
                    None,
                ])),
            ),
            (
                "utc",
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(1_704_164_645_000_000), None, None])
                        .with_timezone("UTC"),
                ),
            ),
            ("amount", Arc::new(dec.finish())),
            (
                "ok",
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
            ),
            (
                "raw",
                Arc::new(BinaryArray::from(vec![
                    Some(&b"hi"[..]),
                    Some(&b""[..]),
                    None,
                ])),
            ),
        ]);
        assert_eq!(
            text(&b, b','),
            "2024-01-01,2024-01-02T03:04:05.123456,2024-01-02T03:04:05.000000Z,12.50,true,aGk=\n\
             1969-12-31,1969-12-31T23:59:59.999999,,-0.05,false,\"\"\n\
             ,,,,,\n"
        );
    }

    #[test]
    fn a_timezone_other_than_utc_is_written_as_the_utc_instant() {
        let a = TimestampMicrosecondArray::from(vec![Some(0)]).with_timezone("Asia/Jakarta");
        let b = batch(vec![("t", Arc::new(a) as ArrayRef)]);
        assert_eq!(text(&b, b','), "1970-01-01T00:00:00.000000Z\n");
    }

    #[test]
    fn numbers_render_as_arrow_displays_them() {
        let b = batch(vec![
            (
                "i",
                Arc::new(Int64Array::from(vec![Some(-42), Some(i64::MAX), None])) as ArrayRef,
            ),
            (
                "f",
                Arc::new(Float64Array::from(vec![
                    Some(0.1),
                    Some(1e20),
                    Some(f64::NAN),
                ])),
            ),
        ]);
        assert_eq!(text(&b, b','), "-42,0.1\n9223372036854775807,1e20\n,NaN\n");
        // A delimiter that a number can hold gets the number quoted.
        let b = batch(vec![(
            "f",
            Arc::new(Float64Array::from(vec![1.5])) as ArrayRef,
        )]);
        assert_eq!(text(&b, b'.'), "\"1.5\"\n");
    }

    #[test]
    fn the_header_is_quoted_like_any_field() {
        let mut out = Vec::new();
        push_header(&mut out, &["id".into(), "a,b".into(), "".into()], b',');
        assert_eq!(String::from_utf8(out).unwrap(), "id,\"a,b\",\"\"\n");
    }

    #[test]
    fn a_nested_column_is_refused_by_name() {
        let list = arrow_array::ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>(
            vec![Some(vec![Some(1)])],
        );
        let err = Column::new(&list, "tags").err().unwrap().to_string();
        assert!(err.contains("'tags'") && err.contains("nested"), "{err}");
    }

    #[test]
    fn a_part_file_has_its_header_rows_and_size() {
        let dir = std::env::temp_dir().join(format!(
            "qh-csv-writer-{}-{}",
            std::process::id(),
            time::OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        for compression in [CsvCompression::None, CsvCompression::Gzip] {
            let format = PartFormat {
                delimiter: b',',
                header: true,
                compression,
            };
            let name = format.file_name("123", 7);
            let mut w = PartWriter::create(&dir, name.clone(), &format, &["id".into(), "s".into()])
                .unwrap();
            let b = batch(vec![
                ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
                (
                    "s",
                    Arc::new(arrow_array::StringArray::from(vec![Some("x"), None])),
                ),
            ]);
            assert_eq!(w.write(&b).unwrap(), "1,x\n2,\n".len() as u64);
            assert_eq!(w.rows(), 2);
            let info = w.finish().unwrap();
            assert_eq!(info.rows, 2);
            assert_eq!(info.file, name);
            let raw = std::fs::read(dir.join(&name)).unwrap();
            assert_eq!(info.bytes, raw.len() as u64);
            let body = match compression {
                CsvCompression::None => {
                    assert_eq!(name, "part-123-00007.csv");
                    String::from_utf8(raw).unwrap()
                }
                CsvCompression::Gzip => {
                    use std::io::Read;
                    assert_eq!(name, "part-123-00007.csv.gz");
                    let mut s = String::new();
                    flate2::read::GzDecoder::new(&raw[..])
                        .read_to_string(&mut s)
                        .unwrap();
                    s
                }
            };
            assert_eq!(body, "id,s\n1,x\n2,\n");
            // A part name is never reused.
            assert!(PartWriter::create(&dir, name, &format, &[]).is_err());
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
