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
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
use bytes::Bytes;

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

    /// Re-label one decoded batch onto the planned schema, casting any column
    /// whose type the server-side cast could not pin down.
    fn adapt(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if batch.num_columns() != self.schema.fields().len() {
            return Err(EtlError::decode(format!(
                "clickhouse returned {} column(s) for a read planned with {}",
                batch.num_columns(),
                self.schema.fields().len()
            )));
        }
        let mut columns = Vec::with_capacity(batch.num_columns());
        for (i, field) in self.schema.fields().iter().enumerate() {
            let col = batch.column(i);
            if col.data_type() == field.data_type() {
                columns.push(col.clone());
            } else {
                columns.push(cast(col, field.data_type()).map_err(|e| {
                    EtlError::decode(format!(
                        "column '{}': clickhouse returned {:?} where {:?} was planned, and the \
                         two don't convert ({e})",
                        field.name(),
                        col.data_type(),
                        field.data_type()
                    ))
                })?);
            }
        }
        RecordBatch::try_new(self.schema.clone(), columns).map_err(EtlError::from)
    }

    /// Split a batch that exceeds `batch_bytes` into equal row slices. Slicing
    /// is zero-copy (each slice shares the parent's buffers), so this bounds
    /// what the *insert path* holds and hands back to the memory budget, not
    /// the peak of this decode.
    fn split(&self, batch: RecordBatch) -> Vec<RecordBatch> {
        let rows = batch.num_rows();
        if self.batch_bytes == 0 || rows <= 1 {
            return vec![batch];
        }
        let bytes = batch.get_array_memory_size();
        if bytes <= self.batch_bytes {
            return vec![batch];
        }
        let per_row = (bytes / rows).max(1);
        let chunk = (self.batch_bytes / per_row).max(1);
        let mut out = Vec::with_capacity(rows.div_ceil(chunk));
        let mut offset = 0;
        while offset < rows {
            let len = chunk.min(rows - offset);
            out.push(batch.slice(offset, len));
            offset += len;
        }
        out
    }
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
