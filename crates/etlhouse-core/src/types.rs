//! PostgreSQL type OID <-> Arrow <-> ClickHouse type mapping.
//!
//! The engine reads the binary COPY stream (which carries no type info), so we
//! resolve every column's PostgreSQL type OID up-front (via a catalog query) and
//! derive both the Arrow `DataType` used to build batches and the ClickHouse
//! column type used for DDL generation.

use arrow_schema::DataType;

/// Well-known PostgreSQL `pg_type.oid` values we decode natively.
pub mod oid {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    pub const NAME: u32 = 19;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const OID: u32 = 26;
    pub const JSON: u32 = 114;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const BPCHAR: u32 = 1042;
    pub const VARCHAR: u32 = 1043;
    pub const DATE: u32 = 1082;
    pub const TIME: u32 = 1083;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const NUMERIC: u32 = 1700;
    pub const UUID: u32 = 2950;
    pub const JSONB: u32 = 3802;
}

/// A source column with its resolved types across the three type systems.
#[derive(Debug, Clone)]
pub struct ColumnType {
    pub name: String,
    pub pg_oid: u32,
    pub nullable: bool,
    pub arrow: DataType,
    /// ClickHouse type *without* the `Nullable(...)` wrapper.
    pub clickhouse_inner: String,
}

impl ColumnType {
    /// The ClickHouse type as it should appear in DDL, applying nullability.
    pub fn clickhouse_type(&self) -> String {
        if self.nullable {
            format!("Nullable({})", self.clickhouse_inner)
        } else {
            self.clickhouse_inner.clone()
        }
    }
}

/// Resolve a PostgreSQL OID to (Arrow type, ClickHouse inner type).
///
/// Timestamps use microsecond precision to match PostgreSQL's native binary
/// representation. `numeric` is mapped to `Float64` by default because arbitrary
/// precision/scale is unknown from the OID alone; callers can override to a
/// `Decimal(P, S)` via `type_overrides`.
pub fn map_oid(oid: u32) -> Option<(DataType, String)> {
    use self::oid as o;
    let mapped = match oid {
        o::BOOL => (DataType::Boolean, "Bool".to_string()),
        o::INT2 => (DataType::Int16, "Int16".to_string()),
        o::INT4 => (DataType::Int32, "Int32".to_string()),
        o::OID => (DataType::UInt32, "UInt32".to_string()),
        o::INT8 => (DataType::Int64, "Int64".to_string()),
        o::FLOAT4 => (DataType::Float32, "Float32".to_string()),
        o::FLOAT8 | o::NUMERIC => (DataType::Float64, "Float64".to_string()),
        o::TEXT | o::VARCHAR | o::BPCHAR | o::NAME | o::JSON | o::JSONB => {
            (DataType::Utf8, "String".to_string())
        }
        o::UUID => (DataType::Utf8, "UUID".to_string()),
        o::BYTEA => (DataType::Binary, "String".to_string()),
        o::DATE => (DataType::Date32, "Date32".to_string()),
        o::TIMESTAMP => (
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            "DateTime64(6)".to_string(),
        ),
        o::TIMESTAMPTZ => (
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
            "DateTime64(6, 'UTC')".to_string(),
        ),
        o::TIME => (DataType::Time64(arrow_schema::TimeUnit::Microsecond), "String".to_string()),
        _ => return None,
    };
    Some(mapped)
}

/// Whether we can decode this OID from the binary COPY stream.
pub fn is_supported(oid: u32) -> bool {
    map_oid(oid).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_common_scalars() {
        assert_eq!(map_oid(oid::INT4).unwrap().1, "Int32");
        assert_eq!(map_oid(oid::INT8).unwrap().1, "Int64");
        assert_eq!(map_oid(oid::TEXT).unwrap().1, "String");
        assert_eq!(map_oid(oid::BOOL).unwrap().1, "Bool");
        assert_eq!(map_oid(oid::TIMESTAMP).unwrap().1, "DateTime64(6)");
        assert!(map_oid(oid::TIMESTAMPTZ).unwrap().1.contains("UTC"));
    }

    #[test]
    fn nullable_wrapping() {
        let c = ColumnType {
            name: "x".into(),
            pg_oid: oid::INT4,
            nullable: true,
            arrow: DataType::Int32,
            clickhouse_inner: "Int32".into(),
        };
        assert_eq!(c.clickhouse_type(), "Nullable(Int32)");
    }

    #[test]
    fn unsupported_returns_none() {
        assert!(map_oid(999_999).is_none());
    }
}
