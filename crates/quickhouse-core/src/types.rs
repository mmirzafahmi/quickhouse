//! PostgreSQL type OID <-> Arrow <-> ClickHouse type mapping.
//!
//! The engine reads the binary COPY stream (which carries no type info), so we
//! resolve every column's PostgreSQL type OID up-front (via a catalog query) and
//! derive both the Arrow `DataType` used to build batches and the ClickHouse
//! column type used for DDL generation.

use arrow_schema::DataType;

/// ClickHouse's `Date32`/`DateTime64` representable window, `[1900-01-01,
/// 2299-12-31]`. Any date/datetime outside this is rejected by ClickHouse's
/// ArrowStream reader with `Code: 321 VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE`,
/// which aborts the whole transfer. Source engines allow far wider ranges
/// (MySQL DATE `1000..=9999`, Postgres/BigQuery wider still), so every source's
/// decoder coerces out-of-range values to NULL before they reach the insert.
///
/// The three helpers cover the two shapes decoders actually hold a value in: a
/// calendar year (MySQL/BigQuery, which decode to date components) or a raw
/// day/microsecond offset from the Unix epoch (Postgres binary COPY).
pub mod ch_range {
    pub const MIN_YEAR: i32 = 1900;
    pub const MAX_YEAR: i32 = 2299;
    /// Days from the Unix epoch to 1900-01-01 / 2299-12-31 (the Date32 bounds).
    /// 2299-12-31 is epoch day 120_529, not 120_530 (that's 2300-01-01) — a
    /// one-line fencepost error here previously let a Postgres date/timestamp
    /// of exactly 2300-01-01 slip through the NULL-coercion guard unmodified.
    pub const MIN_DAYS: i32 = -25_567;
    pub const MAX_DAYS: i32 = 120_529;
    /// Microseconds from the Unix epoch to the first instant of 1900-01-01 and
    /// the last microsecond of 2299-12-31 (the DateTime64 bounds).
    pub const MIN_MICROS: i64 = MIN_DAYS as i64 * 86_400 * 1_000_000;
    pub const MAX_MICROS: i64 = (MAX_DAYS as i64 + 1) * 86_400 * 1_000_000 - 1;

    pub fn year_in_range(year: i32) -> bool {
        (MIN_YEAR..=MAX_YEAR).contains(&year)
    }
    pub fn days_in_range(days: i32) -> bool {
        (MIN_DAYS..=MAX_DAYS).contains(&days)
    }
    pub fn micros_in_range(micros: i64) -> bool {
        (MIN_MICROS..=MAX_MICROS).contains(&micros)
    }
}

/// Whether `arrow` is a type whose decoders may coerce an otherwise-valid
/// value to NULL — `Date32`/`Timestamp` (zero-dates and out-of-`ch_range`
/// years; see each decoder's `ColBuilder::append_value`), and `Decimal128`
/// (a value that overflows the declared `Decimal(P,S)` precision, or is
/// NaN/Infinity on the Postgres path; see `decimal.rs` and each decoder's
/// decimal handling).
///
/// A destination column of one of these types must be resolved as nullable
/// regardless of the source's own `NOT NULL` constraint. Without this, a
/// `NOT NULL` MySQL `DATE`/`DATETIME` column containing a legacy zero-date
/// (a common pattern — MySQL allows `0000-00-00` in `NOT NULL` columns by
/// default) decodes fine, coerces to NULL, and then fails downstream with an
/// Arrow schema-consistency error ("column is declared as non-nullable but
/// contains null values") instead of transferring cleanly — trading the
/// original hard decode error for an equally-fatal one, just later and more
/// confusing. Forcing nullability here closes that gap at its single source
/// of truth: `transform::plan` feeds both the Arrow schema construction (via
/// each decoder's `Field::new(.., nullable)`) and the destination DDL.
pub fn may_coerce_to_null(arrow: &DataType) -> bool {
    matches!(arrow, DataType::Date32 | DataType::Timestamp(_, _) | DataType::Decimal128(_, _))
}

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
    /// Source-engine type identifier: PostgreSQL OID, or a MySQL
    /// `ColumnType` cast to `u32`. Only meaningful to that source's own
    /// decoder; other sources ignore it.
    pub type_id: u32,
    pub nullable: bool,
    pub arrow: DataType,
    /// ClickHouse type *without* the `Nullable(...)` wrapper.
    pub clickhouse_inner: String,
    /// True only for a source column whose declared type is
    /// arbitrary-precision (Postgres `numeric`, MySQL
    /// DECIMAL/NEWDECIMAL, BigQuery NUMERIC/BIGNUMERIC) — never for a
    /// genuine FLOAT/DOUBLE column, which must not be reinterpreted as
    /// `Decimal128` by a `type_overrides` entry even though both currently
    /// default to Arrow `Float64`. Set once by each source's own
    /// `resolve_columns`/`columns_from_schema`; consumed only by
    /// `transform::plan` (decoders don't need it directly — see
    /// `decimal.rs`'s module docs).
    pub arbitrary_precision_decimal: bool,
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
        // TIME is transferred as text ("HH:MM:SS[.ffffff]") into a ClickHouse
        // String column: ClickHouse has no time-of-day type, and an Arrow
        // Time64 physical column does not round-trip into a String column
        // (it lands as a bogus epoch-relative datetime). See the decoders.
        o::TIME => (DataType::Utf8, "String".to_string()),
        _ => return None,
    };
    Some(mapped)
}

/// Whether we can decode this OID from the binary COPY stream.
pub fn is_supported(oid: u32) -> bool {
    map_oid(oid).is_some()
}

/// BigQuery `TableFieldType` <-> Arrow <-> ClickHouse type mapping.
///
/// Unlike Postgres/MySQL, BigQuery's own type system has no small integer
/// code we can reuse for `ColumnType::type_id`, so we assign our own stable
/// constants here (arbitrary, just needs to round-trip through `map_type`).
pub mod bigquery {
    use arrow_schema::{DataType, TimeUnit};
    use google_cloud_bigquery::http::table::TableFieldType as BqType;

    pub mod type_id {
        pub const STRING: u32 = 1;
        pub const BYTES: u32 = 2;
        pub const INTEGER: u32 = 3;
        pub const FLOAT: u32 = 4;
        pub const BOOLEAN: u32 = 5;
        pub const TIMESTAMP: u32 = 6;
        pub const DATE: u32 = 7;
        pub const TIME: u32 = 8;
        pub const DATETIME: u32 = 9;
        pub const NUMERIC: u32 = 10;
        pub const BIGNUMERIC: u32 = 11;
        pub const JSON: u32 = 12;
    }

    /// Map a BigQuery field type to (`type_id`, Arrow type, ClickHouse inner type).
    ///
    /// `NUMERIC`/`BIGNUMERIC`/`DECIMAL`/`BIGDECIMAL` map to `Float64` by
    /// default (same override-via-`type_overrides` policy as the other
    /// sources' arbitrary-precision numeric types). `RECORD`/`STRUCT` and
    /// repeated (ARRAY) fields aren't supported in v1 — same scalar-only
    /// scope as the Postgres/MySQL sources.
    pub fn map_type(field_type: &BqType) -> Option<(u32, DataType, String)> {
        use type_id as id;
        let mapped = match field_type {
            BqType::String | BqType::Json => (id::STRING, DataType::Utf8, "String".to_string()),
            BqType::Bytes => (id::BYTES, DataType::Binary, "String".to_string()),
            BqType::Integer | BqType::Int64 => (id::INTEGER, DataType::Int64, "Int64".to_string()),
            BqType::Float | BqType::Float64 => (id::FLOAT, DataType::Float64, "Float64".to_string()),
            BqType::Boolean | BqType::Bool => (id::BOOLEAN, DataType::Boolean, "Bool".to_string()),
            BqType::Timestamp => (
                id::TIMESTAMP,
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                "DateTime64(6, 'UTC')".to_string(),
            ),
            BqType::Date => (id::DATE, DataType::Date32, "Date32".to_string()),
            // TIME -> text into a ClickHouse String column (see the Postgres
            // TIME note above; ClickHouse has no time-of-day type).
            BqType::Time => (id::TIME, DataType::Utf8, "String".to_string()),
            BqType::Datetime => (
                id::DATETIME,
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                "DateTime64(6)".to_string(),
            ),
            BqType::Numeric => (id::NUMERIC, DataType::Float64, "Float64".to_string()),
            BqType::Bignumeric | BqType::Decimal | BqType::Bigdecimal => {
                (id::BIGNUMERIC, DataType::Float64, "Float64".to_string())
            }
            BqType::Record | BqType::Struct | BqType::Interval => return None,
        };
        Some(mapped)
    }

    /// Map an Arrow type (as produced by any source's decoder) to a BigQuery
    /// column type, for generating a destination `TableSchema`. The inverse
    /// of [`map_type`], but must cover every Arrow type any source ever
    /// produces — not just the subset BigQuery-as-source uses — since any
    /// source can now feed a BigQuery destination.
    ///
    /// Two known limitations, deliberately left as documented caveats rather
    /// than solved (matching this crate's existing policy for arbitrary-
    /// precision numerics, e.g. `numeric` -> `Float64`): BigQuery's
    /// `INTEGER` is signed 64-bit, so a source `UInt64` column with values
    /// above `i64::MAX` would overflow on insert. BigQuery's
    /// DATE/DATETIME/TIMESTAMP range (0001-01-01..=9999-12-31) is far wider
    /// than Arrow's, so — unlike the ClickHouse destination's `ch_range` —
    /// no range coercion is needed here.
    pub fn arrow_to_bigquery_type(arrow: &DataType) -> Option<BqType> {
        match arrow {
            DataType::Boolean => Some(BqType::Boolean),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Some(BqType::Integer),
            DataType::Float32 | DataType::Float64 => Some(BqType::Float),
            DataType::Utf8 => Some(BqType::String),
            DataType::Binary => Some(BqType::Bytes),
            DataType::Date32 => Some(BqType::Date),
            DataType::Timestamp(TimeUnit::Microsecond, None) => Some(BqType::Datetime),
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Some(BqType::Timestamp),
            _ => None,
        }
    }
}

/// MySQL `Column::column_type()` (`mysql_async::consts::ColumnType`) <-> Arrow
/// <-> ClickHouse type mapping. Unlike PostgreSQL's binary COPY protocol,
/// MySQL's wire protocol exposes nullability directly in column metadata
/// (`ColumnFlags::NOT_NULL_FLAG`), so there's no separate catalog lookup
/// needed the way there is for `PgSource::not_null_columns`.
pub mod mysql {
    use arrow_schema::DataType;
    use mysql_async::consts::ColumnType as MyType;

    /// Resolve a MySQL column type to (Arrow type, ClickHouse inner type).
    ///
    /// `is_unsigned` distinguishes e.g. `INT UNSIGNED` (fits `UInt32`) from
    /// signed `INT`. `TINYINT(1)` is treated as MySQL's de facto boolean
    /// convention (matching most MySQL client libraries); other TINYINT
    /// widths map to `Int8`. `numeric`/`DECIMAL` maps to `Float64` by default,
    /// same policy as the PostgreSQL source — override via `type_overrides`
    /// for exact `Decimal(P, S)` semantics.
    pub fn map_mysql_type(
        col_type: MyType,
        is_unsigned: bool,
        is_tinyint1: bool,
    ) -> Option<(DataType, String)> {
        let mapped = match col_type {
            MyType::MYSQL_TYPE_TINY if is_tinyint1 => (DataType::Boolean, "Bool".to_string()),
            MyType::MYSQL_TYPE_TINY => {
                if is_unsigned {
                    (DataType::UInt8, "UInt8".to_string())
                } else {
                    (DataType::Int8, "Int8".to_string())
                }
            }
            MyType::MYSQL_TYPE_SHORT | MyType::MYSQL_TYPE_YEAR => {
                if is_unsigned {
                    (DataType::UInt16, "UInt16".to_string())
                } else {
                    (DataType::Int16, "Int16".to_string())
                }
            }
            MyType::MYSQL_TYPE_INT24 | MyType::MYSQL_TYPE_LONG => {
                if is_unsigned {
                    (DataType::UInt32, "UInt32".to_string())
                } else {
                    (DataType::Int32, "Int32".to_string())
                }
            }
            MyType::MYSQL_TYPE_LONGLONG => {
                if is_unsigned {
                    (DataType::UInt64, "UInt64".to_string())
                } else {
                    (DataType::Int64, "Int64".to_string())
                }
            }
            MyType::MYSQL_TYPE_FLOAT => (DataType::Float32, "Float32".to_string()),
            MyType::MYSQL_TYPE_DOUBLE
            | MyType::MYSQL_TYPE_DECIMAL
            | MyType::MYSQL_TYPE_NEWDECIMAL => (DataType::Float64, "Float64".to_string()),
            MyType::MYSQL_TYPE_VARCHAR
            | MyType::MYSQL_TYPE_VAR_STRING
            | MyType::MYSQL_TYPE_STRING
            | MyType::MYSQL_TYPE_ENUM
            | MyType::MYSQL_TYPE_SET
            | MyType::MYSQL_TYPE_JSON => (DataType::Utf8, "String".to_string()),
            MyType::MYSQL_TYPE_TINY_BLOB
            | MyType::MYSQL_TYPE_MEDIUM_BLOB
            | MyType::MYSQL_TYPE_LONG_BLOB
            | MyType::MYSQL_TYPE_BLOB => (DataType::Binary, "String".to_string()),
            MyType::MYSQL_TYPE_DATE | MyType::MYSQL_TYPE_NEWDATE => {
                (DataType::Date32, "Date32".to_string())
            }
            // MySQL DATETIME/TIMESTAMP default to a tz-aware UTC mapping
            // (Arrow `Some("UTC")` -> BigQuery `TIMESTAMP` / ClickHouse
            // `DateTime64(6, 'UTC')`), NOT the tz-naive arm. Rationale: the
            // decoder already interprets the wall-clock value as UTC
            // (`dt.and_utc().timestamp_micros()`, decode_mysql.rs) and MySQL
            // has no wire type that would ever reach a tz-aware arm otherwise,
            // so a MySQL datetime could previously only ever land as BigQuery
            // DATETIME — making it impossible to sync into an existing BigQuery
            // `TIMESTAMP` column (fails the staging->dest MERGE with "Value of
            // type DATETIME cannot be assigned to <col>, which has type
            // TIMESTAMP"). This matches the legacy pandas->to_gbq semantics
            // (naive wall-clock stored into BQ TIMESTAMP as UTC — same instant)
            // and the Postgres `TIMESTAMPTZ` mapping. A column that genuinely
            // wants naive (BigQuery DATETIME / `DateTime64(6)`) opts out
            // per-column via `type_overrides={col: "DATETIME"}` — see
            // `transform::datetime_override_tz`, which flips the Arrow tz flag
            // (and thus the Storage Write proto encoding), not just the DDL.
            MyType::MYSQL_TYPE_DATETIME
            | MyType::MYSQL_TYPE_DATETIME2
            | MyType::MYSQL_TYPE_TIMESTAMP
            | MyType::MYSQL_TYPE_TIMESTAMP2 => (
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                "DateTime64(6, 'UTC')".to_string(),
            ),
            // TIME -> text into a ClickHouse String column. MySQL TIME can be
            // negative and exceed 24h (range +/-838:59:59), which no time-of-day
            // type can hold anyway; text preserves it losslessly. See decode_mysql.
            MyType::MYSQL_TYPE_TIME | MyType::MYSQL_TYPE_TIME2 => {
                (DataType::Utf8, "String".to_string())
            }
            MyType::MYSQL_TYPE_BIT => (DataType::Binary, "String".to_string()),
            _ => return None,
        };
        Some(mapped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn may_coerce_to_null_covers_date_timestamp_and_decimal_only() {
        assert!(may_coerce_to_null(&DataType::Date32));
        assert!(may_coerce_to_null(&DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)));
        assert!(may_coerce_to_null(&DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond,
            Some("UTC".into())
        )));
        // Regression test: a NOT NULL Decimal128 column whose declared
        // precision a source row later overflows must be forced nullable —
        // same hazard class as the date/timestamp coercions above.
        assert!(may_coerce_to_null(&DataType::Decimal128(30, 10)));
        // TIME is text (Utf8) in this project — never coerced, so not covered.
        assert!(!may_coerce_to_null(&DataType::Utf8));
        assert!(!may_coerce_to_null(&DataType::Int64));
        assert!(!may_coerce_to_null(&DataType::Boolean));
        // A genuine Float64 column (no decimal override) is never coerced.
        assert!(!may_coerce_to_null(&DataType::Float64));
    }

    #[test]
    fn maps_common_scalars() {
        assert_eq!(map_oid(oid::INT4).unwrap().1, "Int32");
        assert_eq!(map_oid(oid::INT8).unwrap().1, "Int64");
        assert_eq!(map_oid(oid::TEXT).unwrap().1, "String");
        assert_eq!(map_oid(oid::BOOL).unwrap().1, "Bool");
        assert_eq!(map_oid(oid::TIMESTAMP).unwrap().1, "DateTime64(6)");
        assert!(map_oid(oid::TIMESTAMPTZ).unwrap().1.contains("UTC"));
    }

    /// MySQL DATETIME/TIMESTAMP default to a tz-aware UTC mapping so they can
    /// land in a BigQuery `TIMESTAMP` column (see the map arm's comment). The
    /// decoder computes the same UTC epoch micros regardless of this flag, so
    /// this only changes the destination type, not the stored instant.
    #[test]
    fn mysql_datetime_defaults_to_utc_aware() {
        use super::mysql::map_mysql_type;
        use mysql_async::consts::ColumnType as MyType;
        for ty in [
            MyType::MYSQL_TYPE_DATETIME,
            MyType::MYSQL_TYPE_DATETIME2,
            MyType::MYSQL_TYPE_TIMESTAMP,
            MyType::MYSQL_TYPE_TIMESTAMP2,
        ] {
            let (arrow, ch) = map_mysql_type(ty, false, false).unwrap();
            assert_eq!(
                arrow,
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                "{ty:?} must map to a tz-aware (UTC) Arrow timestamp"
            );
            assert_eq!(ch, "DateTime64(6, 'UTC')", "{ty:?} ClickHouse type");
        }
    }

    #[test]
    fn nullable_wrapping() {
        let c = ColumnType {
            name: "x".into(),
            type_id: oid::INT4,
            nullable: true,
            arrow: DataType::Int32,
            clickhouse_inner: "Int32".into(),
            arbitrary_precision_decimal: false,
        };
        assert_eq!(c.clickhouse_type(), "Nullable(Int32)");
    }

    #[test]
    fn unsupported_returns_none() {
        assert!(map_oid(999_999).is_none());
    }

    #[test]
    fn ch_range_bounds_match_clickhouse_window() {
        use super::ch_range;
        // Year form (MySQL/BigQuery decoders).
        assert!(!ch_range::year_in_range(1899));
        assert!(ch_range::year_in_range(1900));
        assert!(ch_range::year_in_range(2299));
        assert!(!ch_range::year_in_range(2300));
        assert!(!ch_range::year_in_range(1000)); // legacy MySQL min
        assert!(!ch_range::year_in_range(9999)); // "never expires" sentinel
        // Day form (Postgres Date32). Endpoints are epoch-day offsets for
        // 1900-01-01 / 2299-12-31 (independently confirmed: 2299-12-31 is day
        // 120_529, 2300-01-01 is day 120_530 — regression guard for the
        // fencepost bug where MAX_DAYS was off by one).
        assert!(ch_range::days_in_range(-25_567)); // 1900-01-01
        assert!(ch_range::days_in_range(120_529)); // 2299-12-31
        assert!(!ch_range::days_in_range(-25_568));
        assert!(!ch_range::days_in_range(120_530)); // 2300-01-01 — must be rejected
        // Micro form (Postgres DateTime64): first/last representable instants.
        assert!(ch_range::micros_in_range(ch_range::MIN_MICROS));
        assert!(ch_range::micros_in_range(ch_range::MAX_MICROS));
        assert!(!ch_range::micros_in_range(ch_range::MIN_MICROS - 1));
        assert!(!ch_range::micros_in_range(ch_range::MAX_MICROS + 1));
    }

    #[test]
    fn arrow_to_bigquery_type_covers_every_arrow_type_a_source_produces() {
        use super::bigquery::arrow_to_bigquery_type as a2b;
        use arrow_schema::TimeUnit;
        use google_cloud_bigquery::http::table::TableFieldType as BqType;

        assert_eq!(a2b(&DataType::Boolean), Some(BqType::Boolean));
        for int in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ] {
            assert_eq!(a2b(&int), Some(BqType::Integer), "{int:?}");
        }
        assert_eq!(a2b(&DataType::Float32), Some(BqType::Float));
        assert_eq!(a2b(&DataType::Float64), Some(BqType::Float));
        assert_eq!(a2b(&DataType::Utf8), Some(BqType::String));
        assert_eq!(a2b(&DataType::Binary), Some(BqType::Bytes));
        assert_eq!(a2b(&DataType::Date32), Some(BqType::Date));
        assert_eq!(
            a2b(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            Some(BqType::Datetime)
        );
        assert_eq!(
            a2b(&DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))),
            Some(BqType::Timestamp)
        );
        // Not produced by any current decoder (all three sources map TIME to
        // Utf8 text, not Arrow Time64) — confirms there's no silent gap.
        assert_eq!(a2b(&DataType::Time64(TimeUnit::Microsecond)), None);
    }
}
