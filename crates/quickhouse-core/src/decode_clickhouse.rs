//! ClickHouse source decoder: Arrow IPC in, Arrow `RecordBatch`es out.
//!
//! This is the thinnest decoder in the crate by a wide margin, and deliberately
//! so — ClickHouse serves `FORMAT ArrowStream`, so there is no wire format to
//! hand-decode the way `decode.rs` / `decode_mysql.rs` / `decode_bigquery.rs`
//! must. `source/clickhouse.rs` casts every projected column server-side to the
//! ClickHouse type whose Arrow output type *is* the destination column's, so in
//! the normal case the batches arriving here already match the planned schema
//! and this module only re-labels them.
//!
//! Three things still have to happen on this side:
//!
//! 1. **Re-label to the planned schema.** The incoming fields carry the
//!    *source* column names and ClickHouse's own nullability; the rest of the
//!    pipeline (DDL, the sink, the Parquet archive) works off
//!    `SelectPlan::dest_columns`, which applies `rename` and may widen a column
//!    to nullable (`types::may_coerce_to_null`). Arrays are reused as-is —
//!    this is a schema swap, not a copy.
//! 2. **Cast anything that still doesn't match**, which the server-side cast
//!    leaves for exactly one case today: a destination `Binary` column, since
//!    ClickHouse has no `String`-vs-`Binary` distinction to cast to and the
//!    bytes arrive as `Utf8`.
//! 3. **Bound the batch size in bytes.** `batch_rows` is pushed down to the
//!    server as `max_block_size`, but `batch_bytes` cannot be — ClickHouse
//!    counts rows, not bytes — so a wide table's blocks are sliced here to keep
//!    the memory budget meaningful.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::buffer::Buffer;
use arrow::compute::cast;
use arrow::ipc::reader::StreamDecoder;
use arrow_array::builder::Decimal128Builder;
use arrow_array::types::{Decimal128Type, DecimalType};
use arrow_array::{Array, ArrayRef, Decimal128Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;

use crate::config::WarningKind;
use crate::decimal::{rescale_mantissa, CoercionTally};
use crate::error::{EtlError, Result};
use crate::types::ColumnType;

/// Push-based Arrow IPC stream decoder for one ClickHouse read.
pub struct ChArrowDecoder {
    decoder: StreamDecoder,
    schema: SchemaRef,
    /// Slice batches to at most this many bytes (0 = don't slice).
    batch_bytes: usize,
    ready: VecDeque<RecordBatch>,
    pub rows_total: u64,
}

impl ChArrowDecoder {
    /// `dest_columns` is `SelectPlan::dest_columns`; the decoded batches are
    /// re-labelled to it positionally, in the same order `select_sql` projects.
    pub fn new(dest_columns: &[ColumnType], batch_bytes: usize) -> Self {
        let fields: Vec<Field> = dest_columns
            .iter()
            .map(|c| Field::new(&c.name, c.arrow.clone(), c.nullable))
            .collect();
        Self {
            decoder: StreamDecoder::new(),
            schema: Arc::new(Schema::new(fields)),
            batch_bytes,
            ready: VecDeque::new(),
            rows_total: 0,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Feed one chunk of the HTTP response body. Returns every batch that
    /// completed within it (often none — a chunk is an arbitrary byte range,
    /// and `StreamDecoder` retains any partial message itself).
    pub fn feed(&mut self, chunk: Bytes) -> Result<Vec<RecordBatch>> {
        let mut buffer = Buffer::from_vec(chunk.to_vec());
        while !buffer.is_empty() {
            match self.decoder.decode(&mut buffer)? {
                Some(batch) => {
                    let batch = self.adapt(batch)?;
                    self.rows_total += batch.num_rows() as u64;
                    for slice in self.split(batch) {
                        self.ready.push_back(slice);
                    }
                }
                None => break,
            }
        }
        Ok(self.ready.drain(..).collect())
    }

    /// Assert the stream ended on a message boundary. A truncated body — the
    /// server aborting mid-result, which over HTTP arrives as a clean EOF with
    /// a `200` already sent — would otherwise look like a short but successful
    /// read, and quickhouse would swap a partial table into place.
    pub fn finish(&mut self) -> Result<()> {
        self.decoder.finish().map_err(|e| {
            EtlError::decode(format!(
                "clickhouse Arrow stream ended mid-message ({e}) — the server most likely aborted \
                 the query after the response had already started (check the server log, and \
                 max_execution_time / memory limits)"
            ))
        })
    }

    fn adapt(&self, batch: RecordBatch) -> Result<RecordBatch> {
        adapt_to_plan(batch, &self.schema, "clickhouse", None)
    }

    fn split(&self, batch: RecordBatch) -> Vec<RecordBatch> {
        split_to_bytes(batch, self.batch_bytes)
    }
}

/// Re-label one decoded batch onto the planned schema, casting any column whose
/// type the producer could not be made to pin down.
///
/// Shared by the ClickHouse source (where the producer is the server's
/// `FORMAT ArrowStream`, and `source` reads "clickhouse") and the DataFrame
/// source (where it is the caller's own frame, and `source` reads "the frame").
/// `source` only shapes the error text.
///
/// A decimal-to-decimal conversion does not go through arrow's `cast`: in
/// arrow 53 a same-scale narrowing (e.g. `Decimal128(38, 2)` to
/// `Decimal128(18, 2)`) adds one unit to every value, and a value that no
/// longer fits turns into NULL silently. [`rescale_decimal128`] rounds half
/// away from zero like every other decoder here, and counts each value that
/// overflows the target precision in `tally` (when given) so it surfaces as a
/// `CoercedDecimal` warning.
pub(crate) fn adapt_to_plan(
    batch: RecordBatch,
    schema: &SchemaRef,
    source: &str,
    mut tally: Option<&mut CoercionTally>,
) -> Result<RecordBatch> {
    if batch.num_columns() != schema.fields().len() {
        return Err(EtlError::decode(format!(
            "{source} returned {} column(s) for a read planned with {}",
            batch.num_columns(),
            schema.fields().len()
        )));
    }
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        if col.data_type() == field.data_type() {
            columns.push(col.clone());
        } else if let (DataType::Decimal128(_, from_s), DataType::Decimal128(to_p, to_s)) =
            (col.data_type(), field.data_type())
        {
            columns.push(rescale_decimal128(
                col,
                *from_s,
                *to_p,
                *to_s,
                i,
                tally.as_deref_mut(),
            )?);
        } else {
            columns.push(cast(col, field.data_type()).map_err(|e| {
                EtlError::decode(format!(
                    "column '{}': {source} returned {:?} where {:?} was planned, and the \
                     two don't convert ({e})",
                    field.name(),
                    col.data_type(),
                    field.data_type()
                ))
            })?);
        }
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(EtlError::from)
}

/// Convert a `Decimal128` column to another precision/scale, rounding half
/// away from zero. A value that does not fit the target precision becomes
/// NULL and is recorded in `tally` as column `col_idx`.
fn rescale_decimal128(
    col: &ArrayRef,
    from_s: i8,
    to_p: u8,
    to_s: i8,
    col_idx: usize,
    mut tally: Option<&mut CoercionTally>,
) -> Result<ArrayRef> {
    let arr = col
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| EtlError::internal("Decimal128 column is not a Decimal128Array"))?;
    let mut b = Decimal128Builder::with_capacity(arr.len())
        .with_precision_and_scale(to_p, to_s)
        .map_err(EtlError::from)?;
    for v in arr.iter() {
        let Some(v) = v else {
            b.append_null();
            continue;
        };
        let rescaled = v
            .checked_abs()
            .and_then(|m| rescale_mantissa(m, from_s as i32, to_s as i32))
            .map(|m| if v < 0 { -m } else { m })
            .filter(|m| Decimal128Type::is_valid_decimal_precision(*m, to_p));
        match rescaled {
            Some(m) => b.append_value(m),
            None => {
                b.append_null();
                if let Some(t) = tally.as_deref_mut() {
                    t.record(col_idx, WarningKind::CoercedDecimal);
                }
            }
        }
    }
    Ok(std::sync::Arc::new(b.finish()))
}

/// Split a batch that exceeds `batch_bytes` into equal row slices. Slicing is
/// zero-copy (each slice shares the parent's buffers), so this bounds what the
/// *insert path* holds and hands back to the memory budget, not the peak of the
/// decode that produced it. `batch_bytes == 0` disables splitting.
pub(crate) fn split_to_bytes(batch: RecordBatch, batch_bytes: usize) -> Vec<RecordBatch> {
    let rows = batch.num_rows();
    if batch_bytes == 0 || rows <= 1 {
        return vec![batch];
    }
    let bytes = batch.get_array_memory_size();
    if bytes <= batch_bytes {
        return vec![batch];
    }
    let per_row = (bytes / rows).max(1);
    let chunk = (batch_bytes / per_row).max(1);
    let mut out = Vec::with_capacity(rows.div_ceil(chunk));
    let mut offset = 0;
    while offset < rows {
        let len = chunk.min(rows - offset);
        out.push(batch.slice(offset, len));
        offset += len;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::ipc::writer::StreamWriter;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::DataType;

    fn dest(name: &str, arrow: DataType, nullable: bool) -> ColumnType {
        ColumnType {
            name: name.to_string(),
            type_id: 0,
            nullable,
            arrow,
            clickhouse_inner: "String".to_string(),
            arbitrary_precision_decimal: false,
        }
    }

    /// Serialize `batch` the way ClickHouse's `FORMAT ArrowStream` would.
    fn ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
            w.write(batch).unwrap();
            w.finish().unwrap();
        }
        buf
    }

    fn source_batch() -> RecordBatch {
        // Source-side names differ from the destination names below: the
        // decoder is positional, matching how `select_sql` projects.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn decodes_and_relabels_to_the_planned_schema() {
        let cols = vec![
            dest("order_id", DataType::Int64, false),
            // Widened to nullable by the plan even though the source column
            // isn't — the decoder must follow the plan, not the wire.
            dest("label", DataType::Utf8, true),
        ];
        let mut d = ChArrowDecoder::new(&cols, 0);
        let batches = d.feed(Bytes::from(ipc_bytes(&source_batch()))).unwrap();
        d.finish().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(d.rows_total, 3);
        let b = &batches[0];
        assert_eq!(b.schema().field(0).name(), "order_id");
        assert_eq!(b.schema().field(1).name(), "label");
        assert!(b.schema().field(1).is_nullable());
        assert_eq!(b.num_rows(), 3);
    }

    #[test]
    fn reassembles_batches_split_across_chunk_boundaries() {
        let cols = vec![
            dest("order_id", DataType::Int64, false),
            dest("label", DataType::Utf8, false),
        ];
        let bytes = ipc_bytes(&source_batch());
        let mut d = ChArrowDecoder::new(&cols, 0);
        let mut rows = 0;
        // One byte at a time: the worst case a chunked HTTP body can produce.
        for b in &bytes {
            for batch in d.feed(Bytes::from(vec![*b])).unwrap() {
                rows += batch.num_rows();
            }
        }
        d.finish().unwrap();
        assert_eq!(rows, 3);
    }

    #[test]
    fn decimal_narrowing_keeps_values_exact_and_counts_overflow() {
        // arrow 53's cast added one unit to every value on a same-scale
        // narrowing (12.34 -> 12.35) and NULLed overflows silently.
        let src: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(0), Some(1234), Some(-550), Some(9999), None])
                .with_precision_and_scale(38, 2)
                .unwrap(),
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                src.data_type().clone(),
                true,
            )])),
            vec![src],
        )
        .unwrap();
        let plan = |p: u8, s: i8| {
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(p, s),
                true,
            )]))
        };
        let values = |b: &RecordBatch| {
            b.column(0)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        };

        let out = adapt_to_plan(batch.clone(), &plan(18, 2), "the frame", None).unwrap();
        assert_eq!(
            values(&out),
            vec![Some(0), Some(1234), Some(-550), Some(9999), None]
        );

        // Scale down by one: half away from zero, as the other decoders round.
        let out = adapt_to_plan(batch.clone(), &plan(18, 1), "the frame", None).unwrap();
        assert_eq!(
            values(&out),
            vec![Some(0), Some(123), Some(-55), Some(1000), None]
        );

        // 99.99 does not fit Decimal(3, 2): NULL, and counted.
        let mut tally = CoercionTally::new(["amount"]);
        let out = adapt_to_plan(batch, &plan(3, 2), "the frame", Some(&mut tally)).unwrap();
        assert_eq!(values(&out), vec![Some(0), None, Some(-550), None, None]);
        assert_eq!(tally.total(WarningKind::CoercedDecimal), 2);
    }

    #[test]
    fn casts_a_column_the_server_side_cast_could_not_pin() {
        // ClickHouse has no Binary type to cast to, so a destination Binary
        // column arrives as Utf8 and is converted here.
        let cols = vec![
            dest("order_id", DataType::Int64, false),
            dest("label", DataType::Binary, false),
        ];
        let mut d = ChArrowDecoder::new(&cols, 0);
        let batches = d.feed(Bytes::from(ipc_bytes(&source_batch()))).unwrap();
        assert_eq!(batches[0].column(1).data_type(), &DataType::Binary);
    }

    #[test]
    fn splits_a_batch_that_exceeds_the_byte_budget() {
        let cols = vec![
            dest("order_id", DataType::Int64, false),
            dest("label", DataType::Utf8, false),
        ];
        // A budget below the batch's footprint forces slicing; every row must
        // still come through exactly once.
        let mut d = ChArrowDecoder::new(&cols, 1);
        let batches = d.feed(Bytes::from(ipc_bytes(&source_batch()))).unwrap();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        assert_eq!(d.rows_total, 3);
    }

    #[test]
    fn a_truncated_stream_is_an_error_not_a_short_read() {
        let cols = vec![
            dest("order_id", DataType::Int64, false),
            dest("label", DataType::Utf8, false),
        ];
        let bytes = ipc_bytes(&source_batch());
        let mut d = ChArrowDecoder::new(&cols, 0);
        d.feed(Bytes::from(bytes[..bytes.len() / 2].to_vec()))
            .unwrap();
        let err = d.finish().unwrap_err().to_string();
        assert!(err.contains("ended mid-message"), "{err}");
    }
}
