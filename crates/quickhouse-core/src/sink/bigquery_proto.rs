//! Dynamic protobuf encoding for the BigQuery Storage Write API — the opt-in
//! destination write path selected by
//! [`crate::config::BigQueryWriteMethod::StorageWrite`].
//!
//! The Storage Write API appends rows as *serialized protobuf messages*,
//! accompanied by a `DescriptorProto` that describes the message shape.
//! quickhouse has no compile-time protobuf message type — the schema is only
//! known at runtime — so this module builds that descriptor from the resolved
//! Arrow schema and hand-encodes each Arrow row to protobuf wire bytes.
//!
//! BigQuery matches proto fields to table columns **by name** (not position),
//! so each descriptor field's `name` is the column name; `number` is its
//! 1-based position, reused as the protobuf field tag during encoding.
//!
//! proto3 / BigQuery NULL semantics: a null cell **omits its field entirely**
//! (no tag written); every non-null value — including zero/empty — is written,
//! so BigQuery stores a present zero rather than reading the field as absent.
//!
//! The type coverage matches the insertAll JSON encoder (`array_value_to_json`
//! in [`super::bigquery`]); the one representational difference is `Binary`,
//! which goes as raw protobuf `bytes` here rather than base64 text.

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, Schema, TimeUnit};
use prost_types::field_descriptor_proto::{Label, Type as ProtoType};
use prost_types::{DescriptorProto, FieldDescriptorProto};

use crate::error::{EtlError, Result};
use crate::sink::bigquery::timestamp_micros_to_iso;

// protobuf wire types (see the protobuf encoding spec).
const WIRE_VARINT: u32 = 0;
const WIRE_I64: u32 = 1;
const WIRE_LEN: u32 = 2;

/// One column's encoding plan: its 1-based protobuf field number and the Arrow
/// type that dictates how each cell is written.
#[derive(Debug)]
pub(crate) struct FieldEnc {
    name: String,
    number: i32,
    data_type: DataType,
}

/// Validate every column has a supported Arrow→proto mapping and assign field
/// numbers. Fails fast (before any network call) on an unsupported type, the
/// same set the insertAll path rejects.
pub(crate) fn resolve_fields(schema: &Schema) -> Result<Vec<FieldEnc>> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let data_type = f.data_type().clone();
            // Validate up front; the returned type is recomputed in the
            // descriptor builder so the two can't drift.
            proto_type_for(&data_type)?;
            Ok(FieldEnc {
                name: f.name().clone(),
                number: (i + 1) as i32,
                data_type,
            })
        })
        .collect()
}

/// The protobuf scalar type each Arrow type is written as. This is the single
/// source of truth shared by the descriptor and the row encoder.
fn proto_type_for(dt: &DataType) -> Result<ProtoType> {
    Ok(match dt {
        DataType::Boolean => ProtoType::Bool,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => ProtoType::Int64,
        DataType::Float32 | DataType::Float64 => ProtoType::Double,
        DataType::Utf8 => ProtoType::String,
        DataType::Binary => ProtoType::Bytes,
        // BigQuery DATE takes int32 days-from-epoch on the Storage Write API.
        DataType::Date32 => ProtoType::Int32,
        // BigQuery TIMESTAMP takes int64 microseconds-from-epoch.
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => ProtoType::Int64,
        // BigQuery DATETIME: written as a canonical civil string — the
        // unambiguously accepted representation (see the plan's DATETIME note).
        DataType::Timestamp(TimeUnit::Microsecond, None) => ProtoType::String,
        other => {
            return Err(EtlError::internal(format!(
                "no BigQuery Storage Write proto mapping for Arrow type {other:?}"
            )))
        }
    })
}

/// Build the `DescriptorProto` (protobuf message shape) for the write stream's
/// `writer_schema`. Field names are the column names (BigQuery matches by
/// name); all fields are `optional` so nullability is expressed by presence.
pub(crate) fn build_proto_descriptor(fields: &[FieldEnc]) -> Result<DescriptorProto> {
    let field = fields
        .iter()
        .map(|fe| {
            Ok(FieldDescriptorProto {
                name: Some(fe.name.clone()),
                number: Some(fe.number),
                label: Some(Label::Optional as i32),
                r#type: Some(proto_type_for(&fe.data_type)? as i32),
                ..Default::default()
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(DescriptorProto {
        name: Some("QuickhouseRow".to_string()),
        field,
        ..Default::default()
    })
}

/// Encode one Arrow row into protobuf wire bytes, appended to `buf`. Null cells
/// are skipped (proto3 absence = NULL).
pub(crate) fn encode_row(batch: &RecordBatch, row: usize, fields: &[FieldEnc], buf: &mut Vec<u8>) -> Result<()> {
    for fe in fields {
        let col = batch.column((fe.number - 1) as usize);
        if col.is_null(row) {
            continue;
        }
        match &fe.data_type {
            DataType::Boolean => {
                write_varint(buf, fe.number, downcast::<BooleanArray>(col)?.value(row) as u64)
            }
            DataType::Int8 => write_int(buf, fe.number, downcast::<Int8Array>(col)?.value(row) as i64),
            DataType::Int16 => write_int(buf, fe.number, downcast::<Int16Array>(col)?.value(row) as i64),
            DataType::Int32 => write_int(buf, fe.number, downcast::<Int32Array>(col)?.value(row) as i64),
            DataType::Int64 => write_int(buf, fe.number, downcast::<Int64Array>(col)?.value(row)),
            DataType::UInt8 => write_int(buf, fe.number, downcast::<UInt8Array>(col)?.value(row) as i64),
            DataType::UInt16 => write_int(buf, fe.number, downcast::<UInt16Array>(col)?.value(row) as i64),
            DataType::UInt32 => write_int(buf, fe.number, downcast::<UInt32Array>(col)?.value(row) as i64),
            // BigQuery INTEGER is signed 64-bit; a UInt64 above i64::MAX wraps
            // to negative here (documented overflow caveat, same as the DDL /
            // JSON paths).
            DataType::UInt64 => write_int(buf, fe.number, downcast::<UInt64Array>(col)?.value(row) as i64),
            DataType::Float32 => {
                write_double(buf, fe.number, downcast::<Float32Array>(col)?.value(row) as f64)
            }
            DataType::Float64 => write_double(buf, fe.number, downcast::<Float64Array>(col)?.value(row)),
            DataType::Utf8 => write_len(buf, fe.number, downcast::<StringArray>(col)?.value(row).as_bytes()),
            DataType::Binary => write_len(buf, fe.number, downcast::<BinaryArray>(col)?.value(row)),
            // int32 days-from-epoch; negatives (pre-1970) sign-extend to a
            // 10-byte varint, same as any negative proto int.
            DataType::Date32 => write_int(buf, fe.number, downcast::<Date32Array>(col)?.value(row) as i64),
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
                write_int(buf, fe.number, downcast::<TimestampMicrosecondArray>(col)?.value(row))
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let micros = downcast::<TimestampMicrosecondArray>(col)?.value(row);
                let s = timestamp_micros_to_iso(micros, false)?;
                write_len(buf, fe.number, s.as_bytes());
            }
            other => {
                return Err(EtlError::internal(format!(
                    "no BigQuery Storage Write proto encoding for Arrow type {other:?}"
                )))
            }
        }
    }
    Ok(())
}

fn downcast<T: 'static>(col: &dyn Array) -> Result<&T> {
    col.as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| EtlError::internal("Arrow array downcast failed (schema/builder type mismatch)"))
}

// ---- protobuf wire-format primitives ----

/// Append a base-128 varint.
fn push_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            buf.push(byte | 0x80);
        } else {
            buf.push(byte);
            break;
        }
    }
}

/// Append a field tag: `(field_number << 3) | wire_type`, varint-encoded.
fn push_tag(buf: &mut Vec<u8>, field_number: i32, wire_type: u32) {
    push_varint(buf, ((field_number as u64) << 3) | wire_type as u64);
}

/// A raw varint field (bool as 0/1).
fn write_varint(buf: &mut Vec<u8>, field_number: i32, v: u64) {
    push_tag(buf, field_number, WIRE_VARINT);
    push_varint(buf, v);
}

/// A proto `int32`/`int64` field. Negative values are cast to their
/// two's-complement `u64` bit pattern, which varint-encodes to 10 bytes —
/// exactly how protobuf sign-extends negative `int32`/`int64`.
fn write_int(buf: &mut Vec<u8>, field_number: i32, v: i64) {
    push_tag(buf, field_number, WIRE_VARINT);
    push_varint(buf, v as u64);
}

/// A proto `double` field: 8 little-endian bytes (fixed 64-bit).
fn write_double(buf: &mut Vec<u8>, field_number: i32, v: f64) {
    push_tag(buf, field_number, WIRE_I64);
    buf.extend_from_slice(&v.to_le_bytes());
}

/// A length-delimited field (`string`/`bytes`): varint length prefix + bytes.
fn write_len(buf: &mut Vec<u8>, field_number: i32, bytes: &[u8]) {
    push_tag(buf, field_number, WIRE_LEN);
    push_varint(buf, bytes.len() as u64);
    buf.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        BinaryArray, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow_schema::{Field, Schema};
    use prost::Message;
    use std::sync::Arc;

    // A protobuf message mirroring the descriptor field numbers below, decoded
    // with prost to validate our hand-rolled wire format against a real decoder.
    // `optional` scalars use explicit presence, so absent (NULL) decodes to
    // `None` and a present zero decodes to `Some(0)`.
    #[derive(Clone, PartialEq, prost::Message)]
    struct DecodedRow {
        #[prost(bool, optional, tag = "1")]
        b: Option<bool>,
        #[prost(int64, optional, tag = "2")]
        i: Option<i64>,
        #[prost(double, optional, tag = "3")]
        f: Option<f64>,
        #[prost(string, optional, tag = "4")]
        s: Option<String>,
        #[prost(bytes = "vec", optional, tag = "5")]
        by: Option<Vec<u8>>,
        #[prost(int32, optional, tag = "6")]
        d: Option<i32>,
        #[prost(int64, optional, tag = "7")]
        ts: Option<i64>,
        #[prost(string, optional, tag = "8")]
        dt: Option<String>,
    }

    fn mixed_schema() -> Schema {
        Schema::new(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("i", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("by", DataType::Binary, true),
            Field::new("d", DataType::Date32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
            Field::new("dt", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ])
    }

    #[allow(clippy::too_many_arguments)]
    fn batch_with(
        b: Option<bool>,
        i: Option<i64>,
        f: Option<f64>,
        s: Option<&str>,
        by: Option<&[u8]>,
        d: Option<i32>,
        ts: Option<i64>,
        dt: Option<i64>,
    ) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(mixed_schema()),
            vec![
                Arc::new(BooleanArray::from(vec![b])),
                Arc::new(Int64Array::from(vec![i])),
                Arc::new(Float64Array::from(vec![f])),
                Arc::new(StringArray::from(vec![s])),
                Arc::new(BinaryArray::from_opt_vec(vec![by])),
                Arc::new(Date32Array::from(vec![d])),
                Arc::new(TimestampMicrosecondArray::from(vec![ts]).with_timezone("UTC")),
                Arc::new(TimestampMicrosecondArray::from(vec![dt])),
            ],
        )
        .unwrap()
    }

    fn encode_first_row(batch: &RecordBatch) -> DecodedRow {
        let fields = resolve_fields(&batch.schema()).unwrap();
        let mut buf = Vec::new();
        encode_row(batch, 0, &fields, &mut buf).unwrap();
        DecodedRow::decode(&buf[..]).unwrap()
    }

    #[test]
    fn descriptor_maps_names_numbers_and_types() {
        let fields = resolve_fields(&mixed_schema()).unwrap();
        let d = build_proto_descriptor(&fields).unwrap();
        assert_eq!(d.field.len(), 8);
        assert_eq!(d.field[0].name(), "b");
        assert_eq!(d.field[0].number(), 1);
        assert_eq!(d.field[0].r#type(), ProtoType::Bool);
        assert_eq!(d.field[1].r#type(), ProtoType::Int64);
        assert_eq!(d.field[2].r#type(), ProtoType::Double);
        assert_eq!(d.field[3].r#type(), ProtoType::String);
        assert_eq!(d.field[4].r#type(), ProtoType::Bytes);
        assert_eq!(d.field[5].r#type(), ProtoType::Int32); // Date32
        assert_eq!(d.field[6].r#type(), ProtoType::Int64); // TIMESTAMP
        assert_eq!(d.field[7].r#type(), ProtoType::String); // DATETIME
        assert!(d.field.iter().all(|f| f.label() == Label::Optional));
    }

    #[test]
    fn encodes_all_types_round_trip() {
        let batch = batch_with(
            Some(true),
            Some(-42),
            Some(3.5),
            Some("héllo"),
            Some(&[0xDE, 0xAD, 0xBE, 0xEF]),
            Some(19723),                 // 2024-01-01
            Some(1_704_067_200_000_000), // 2024-01-01T00:00:00Z in µs
            Some(1_704_067_200_000_000),
        );
        let got = encode_first_row(&batch);
        assert_eq!(got.b, Some(true));
        assert_eq!(got.i, Some(-42));
        assert_eq!(got.f, Some(3.5));
        assert_eq!(got.s.as_deref(), Some("héllo"));
        assert_eq!(got.by.as_deref(), Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]));
        assert_eq!(got.d, Some(19723));
        assert_eq!(got.ts, Some(1_704_067_200_000_000));
        assert_eq!(got.dt.as_deref(), Some("2024-01-01T00:00:00.000000"));
    }

    #[test]
    fn null_cells_are_omitted() {
        // Every column null → an empty message → all fields decode to None.
        let batch = batch_with(None, None, None, None, None, None, None, None);
        let got = encode_first_row(&batch);
        assert_eq!(got, DecodedRow::default());
    }

    #[test]
    fn present_zero_is_distinct_from_null() {
        // A present zero/empty must be written (Some), not omitted (None).
        let batch = batch_with(Some(false), Some(0), Some(0.0), Some(""), Some(&[]), Some(0), Some(0), Some(0));
        let got = encode_first_row(&batch);
        assert_eq!(got.b, Some(false));
        assert_eq!(got.i, Some(0));
        assert_eq!(got.f, Some(0.0));
        assert_eq!(got.s.as_deref(), Some(""));
        assert_eq!(got.by.as_deref(), Some(&[][..]));
        assert_eq!(got.d, Some(0));
        assert_eq!(got.ts, Some(0));
    }

    #[test]
    fn negative_date_and_int_sign_extend() {
        let batch = batch_with(None, Some(-1), None, None, None, Some(-1), None, None);
        let got = encode_first_row(&batch);
        assert_eq!(got.i, Some(-1));
        assert_eq!(got.d, Some(-1)); // 1969-12-31
    }

    #[test]
    fn resolve_rejects_unsupported_type() {
        let schema = Schema::new(vec![Field::new("x", DataType::Duration(TimeUnit::Second), true)]);
        let err = resolve_fields(&schema).unwrap_err().to_string();
        assert!(err.contains("no BigQuery Storage Write proto mapping"), "got: {err}");
    }
}
