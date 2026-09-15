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
    matches!(
        arrow,
        DataType::Date32 | DataType::Timestamp(_, _) | DataType::Decimal128(_, _)
    )
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
    ///
    /// `LowCardinality` is the one wrapper that has to go on the *outside*:
    /// ClickHouse spells a nullable dictionary column
    /// `LowCardinality(Nullable(T))` and rejects `Nullable(LowCardinality(T))`
    /// outright (`Code: 43 ILLEGAL_TYPE_OF_ARGUMENT`, at `CREATE TABLE` time).
    /// Reached by a nullable `LowCardinality` column read from a ClickHouse
    /// source, and by a `type_overrides={col: "LowCardinality(String)"}` on any
    /// nullable column.
    pub fn clickhouse_type(&self) -> String {
        if !self.nullable {
            return self.clickhouse_inner.clone();
        }
        if let Some(inner) = strip_low_cardinality(&self.clickhouse_inner) {
            // Already spelled with the Nullable inside — leave it alone rather
            // than double-wrapping.
            if inner.starts_with("Nullable(") {
                return self.clickhouse_inner.clone();
            }
            return format!("LowCardinality(Nullable({inner}))");
        }
        format!("Nullable({})", self.clickhouse_inner)
    }
}

/// `LowCardinality(T)` -> `Some("T")`, anything else -> `None`. Matches only
/// when the closing paren ends the string, so a nested occurrence inside a
/// larger type can't be mistaken for a wrapper around the whole thing.
fn strip_low_cardinality(t: &str) -> Option<&str> {
    t.strip_prefix("LowCardinality(")?.strip_suffix(')')
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

    /// Parse a user-declared BigQuery type-name string (for an API source's
    /// declared schema) into a `TableFieldType`. Case-insensitive, closed
    /// allow-list; `RECORD`/`STRUCT`/`INTERVAL`/`RANGE` and anything unknown are
    /// rejected up front with a clear error naming the column.
    pub fn parse_bq_type_name(col: &str, decl: &str) -> crate::error::Result<BqType> {
        use BqType as T;
        Ok(match decl.trim().to_ascii_uppercase().as_str() {
            "STRING" => T::String,
            "BYTES" => T::Bytes,
            "INTEGER" | "INT64" => T::Integer,
            "FLOAT" | "FLOAT64" => T::Float,
            "BOOLEAN" | "BOOL" => T::Boolean,
            "TIMESTAMP" => T::Timestamp,
            "DATE" => T::Date,
            "TIME" => T::Time,
            "DATETIME" => T::Datetime,
            "NUMERIC" | "DECIMAL" => T::Numeric,
            "BIGNUMERIC" | "BIGDECIMAL" => T::Bignumeric,
            "JSON" => T::Json,
            other => {
                return Err(crate::error::EtlError::config(format!(
                    "column '{col}': unsupported declared BigQuery type '{other}'; expected one of \
                     STRING, BYTES, INTEGER, FLOAT, BOOLEAN, TIMESTAMP, DATE, TIME, DATETIME, \
                     NUMERIC, BIGNUMERIC, JSON"
                )))
            }
        })
    }

    /// The exact SCREAMING BigQuery type name that round-trips through
    /// `bq_field_data_type`'s `serde_json::from_value(Value::String(..))` in the
    /// sink — i.e. the canonical serde name, avoiding the `INT64`/`FLOAT64`/
    /// `BOOL` aliases. Used to seed `type_overrides` for a declared API column
    /// so the destination table gets the exact declared type.
    pub fn canonical_bq_type_name(t: &BqType) -> &'static str {
        use BqType as T;
        match t {
            T::String => "STRING",
            T::Bytes => "BYTES",
            T::Integer | T::Int64 => "INTEGER",
            T::Float | T::Float64 => "FLOAT",
            T::Boolean | T::Bool => "BOOLEAN",
            T::Timestamp => "TIMESTAMP",
            T::Date => "DATE",
            T::Time => "TIME",
            T::Datetime => "DATETIME",
            T::Numeric => "NUMERIC",
            T::Bignumeric => "BIGNUMERIC",
            T::Json => "JSON",
            _ => "STRING",
        }
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
            BqType::Float | BqType::Float64 => {
                (id::FLOAT, DataType::Float64, "Float64".to_string())
            }
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
            // An exact-decimal column (from a promoted NUMERIC override) maps
            // to BigQuery NUMERIC — for the DDL round-trip when the override is
            // keyed by source name + a rename, and so partition_by validation
            // correctly rejects a NUMERIC column as non-temporal.
            DataType::Decimal128(_, _) => Some(BqType::Numeric),
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
    /// widths map to `Int8`. That convention is a *display-width* guess, not a
    /// real type, and it's wrong for schemas that store genuine small integers
    /// in a `tinyint(1)` — the caller passes `is_tinyint1: false` (from
    /// `tinyint1_as_bool=False`) to fall through to the plain `Int8`/`UInt8`
    /// arm below. `numeric`/`DECIMAL` maps to `Float64` by default,
    /// same policy as the PostgreSQL source — override via `type_overrides`
    /// for exact `Decimal(P, S)` semantics.
    pub fn map_mysql_type(
        col_type: MyType,
        is_unsigned: bool,
        is_tinyint1: bool,
        is_binary: bool,
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
            // The BLOB-family wire codes are shared by real binary BLOB and the
            // TEXT/MEDIUMTEXT/LONGTEXT types — the ONLY distinguisher is the
            // charset (binary collation 63 => BLOB, anything else => TEXT),
            // which the caller passes as `is_binary`. A TEXT column MUST map to
            // Utf8 (BigQuery STRING); mapping it to Binary/BYTES fails a MERGE
            // into a STRING column ("Value of type BYTES cannot be assigned").
            MyType::MYSQL_TYPE_TINY_BLOB
            | MyType::MYSQL_TYPE_MEDIUM_BLOB
            | MyType::MYSQL_TYPE_LONG_BLOB
            | MyType::MYSQL_TYPE_BLOB => {
                if is_binary {
                    (DataType::Binary, "String".to_string())
                } else {
                    (DataType::Utf8, "String".to_string())
                }
            }
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

/// ClickHouse type-name <-> Arrow <-> ClickHouse type mapping, for reading
/// *from* ClickHouse.
///
/// Unlike every other source, the schema arrives as a **type name string**
/// (`DESCRIBE` output), not a numeric wire code — so there is no small integer
/// to reuse for `ColumnType::type_id` and we assign our own stable constants,
/// the same way [`super::bigquery`] does.
///
/// The read path is ClickHouse's own `FORMAT ArrowStream`, so the decoder is
/// Arrow IPC rather than a hand-written wire decoder (see
/// `crate::decode_clickhouse`). What makes that safe is that the SELECT casts
/// every column to a ClickHouse type whose Arrow output type is fixed and
/// documented — see [`arrow_to_ch_cast_type`], which is the other half of this
/// mapping and must stay in lockstep with it.
pub mod clickhouse {
    use arrow_schema::{DataType, TimeUnit};
    use std::sync::Arc;

    /// Our own stable identifiers for the ClickHouse type families we read.
    /// Only meaningful to this source (`ColumnType::type_id`'s contract).
    pub mod type_id {
        pub const BOOL: u32 = 1;
        pub const INT8: u32 = 2;
        pub const INT16: u32 = 3;
        pub const INT32: u32 = 4;
        pub const INT64: u32 = 5;
        pub const UINT8: u32 = 6;
        pub const UINT16: u32 = 7;
        pub const UINT32: u32 = 8;
        pub const UINT64: u32 = 9;
        pub const FLOAT32: u32 = 10;
        pub const FLOAT64: u32 = 11;
        pub const DECIMAL: u32 = 12;
        pub const STRING: u32 = 13;
        pub const UUID: u32 = 14;
        pub const DATE: u32 = 15;
        pub const DATETIME: u32 = 16;
        pub const ENUM: u32 = 17;
        pub const IP: u32 = 18;
    }

    /// One resolved ClickHouse source column.
    #[derive(Debug, Clone, PartialEq)]
    pub struct ChColumn {
        pub type_id: u32,
        pub arrow: DataType,
        /// The ClickHouse type to declare at the destination — the source's own
        /// declared type with any `Nullable(...)` wrapper stripped, so a
        /// ClickHouse -> ClickHouse copy reproduces `UUID`, `IPv4`,
        /// `LowCardinality(String)`, `Enum8(...)` and friends verbatim rather
        /// than flattening every one of them to `String`.
        pub clickhouse_inner: String,
        pub nullable: bool,
        /// True for `Decimal*` — the `arbitrary_precision_decimal` flag, which
        /// gates `numeric_as_decimal` / the exact-`Decimal(P,S)` promotion.
        pub arbitrary_precision_decimal: bool,
    }

    /// Whether a resolved column can be split into numeric ranges for parallel
    /// partitioning (the ClickHouse analogue of
    /// `crate::source::mysql::is_range_partitionable`). `UInt64` is included
    /// even though its upper half overflows `i64`: the MIN/MAX probe reads the
    /// bounds as text and parses them into `i128`, so the emitted range
    /// predicates stay exact.
    pub fn is_range_partitionable(id: u32) -> bool {
        use type_id as t;
        matches!(
            id,
            t::INT8 | t::INT16 | t::INT32 | t::INT64 | t::UINT8 | t::UINT16 | t::UINT32 | t::UINT64
        )
    }

    /// Split `Nullable(T)` into `(T, true)`, anything else into `(t, false)`.
    /// `LowCardinality(Nullable(T))` is normalised to `LowCardinality(T)` +
    /// nullable, so the destination keeps the dictionary encoding.
    fn split_nullable(t: &str) -> (String, bool) {
        let t = t.trim();
        if let Some(inner) = strip_wrapper(t, "Nullable") {
            return (inner.trim().to_string(), true);
        }
        if let Some(inner) = strip_wrapper(t, "LowCardinality") {
            let (inner, nullable) = split_nullable(inner);
            return (format!("LowCardinality({inner})"), nullable);
        }
        (t.to_string(), false)
    }

    /// `strip_wrapper("Nullable(Int32)", "Nullable") == Some("Int32")`. Matches
    /// only when the closing paren is the *last* character, so a nested
    /// occurrence (`Map(String, Nullable(Int8))`) can't be mistaken for a
    /// wrapper around the whole type.
    fn strip_wrapper<'a>(t: &'a str, name: &str) -> Option<&'a str> {
        let rest = t.strip_prefix(name)?;
        let rest = rest.strip_prefix('(')?;
        rest.strip_suffix(')')
    }

    /// The type name with any parameter list removed: `Decimal(18, 4)` ->
    /// `Decimal`, `DateTime64(3, 'UTC')` -> `DateTime64`, `String` -> `String`.
    fn base_name(t: &str) -> &str {
        match t.find('(') {
            Some(i) => t[..i].trim_end(),
            None => t,
        }
    }

    /// The comma-separated arguments inside a parameterised type name, trimmed.
    /// Only ever called on types whose arguments cannot themselves be a
    /// comma-bearing nested type (`Decimal`, `DateTime`, `DateTime64`).
    fn args(t: &str) -> Vec<&str> {
        let Some(open) = t.find('(') else {
            return Vec::new();
        };
        let Some(close) = t.rfind(')') else {
            return Vec::new();
        };
        if close <= open + 1 {
            return Vec::new();
        }
        t[open + 1..close].split(',').map(str::trim).collect()
    }

    /// Strip the single quotes ClickHouse renders a timezone argument with.
    fn unquote(s: &str) -> &str {
        s.trim().trim_matches('\'')
    }

    /// The implied precision of the fixed-width `Decimal32/64/128(S)` spellings.
    fn sized_decimal_precision(base: &str) -> Option<u8> {
        match base {
            "Decimal32" => Some(9),
            "Decimal64" => Some(18),
            "Decimal128" => Some(38),
            _ => None,
        }
    }

    /// Resolve one `DESCRIBE`-reported ClickHouse type name into the Arrow type
    /// we decode it as, the ClickHouse type to declare at the destination, and
    /// its nullability. `None` for a type this source can't read yet —
    /// `Array`/`Map`/`Tuple`/`Nested`/`JSON`/`Variant`/`Dynamic`, the 256-bit
    /// integers and `Decimal256` (all still reachable through a `source_query`
    /// that casts them to `String`).
    ///
    /// Datetimes always resolve tz-aware: a ClickHouse `DateTime`/`DateTime64`
    /// is an absolute instant on the wire (epoch seconds/ticks) whatever the
    /// timezone in its type name, which only decides how it is *rendered*. That
    /// matches the MySQL source's default and lands in a BigQuery `TIMESTAMP`
    /// rather than a `DATETIME`; `type_overrides={col: "DATETIME"}` is the
    /// per-column opt-out, exactly as it is there.
    pub fn map_ch_type(declared: &str) -> Option<ChColumn> {
        use type_id as id;
        let (inner, nullable) = split_nullable(declared);
        // A LowCardinality column decodes as its own underlying type; the
        // wrapper only survives into `clickhouse_inner`, for the DDL.
        let payload = strip_wrapper(&inner, "LowCardinality").unwrap_or(&inner);
        let base = base_name(payload);

        let (type_id, arrow) = match base {
            "Bool" | "Boolean" => (id::BOOL, DataType::Boolean),
            "Int8" => (id::INT8, DataType::Int8),
            "Int16" => (id::INT16, DataType::Int16),
            "Int32" => (id::INT32, DataType::Int32),
            "Int64" => (id::INT64, DataType::Int64),
            "UInt8" => (id::UINT8, DataType::UInt8),
            "UInt16" => (id::UINT16, DataType::UInt16),
            "UInt32" => (id::UINT32, DataType::UInt32),
            "UInt64" => (id::UINT64, DataType::UInt64),
            "Float32" => (id::FLOAT32, DataType::Float32),
            "Float64" => (id::FLOAT64, DataType::Float64),
            "String" | "FixedString" => (id::STRING, DataType::Utf8),
            "UUID" => (id::UUID, DataType::Utf8),
            "IPv4" | "IPv6" => (id::IP, DataType::Utf8),
            "Enum" | "Enum8" | "Enum16" => (id::ENUM, DataType::Utf8),
            "Date" | "Date32" => (id::DATE, DataType::Date32),
            "DateTime" | "DateTime64" => {
                // The timezone argument is the last one (`DateTime('UTC')`,
                // `DateTime64(3, 'UTC')`); absent means the server timezone,
                // normalised to UTC here since the stored value is an absolute
                // instant either way. The `contains('/')` test is what keeps
                // `DateTime64(3)`'s precision digit from being read as a
                // timezone name.
                let tz = args(payload)
                    .last()
                    .map(|a| unquote(a))
                    .filter(|a| a.contains('/') || a.eq_ignore_ascii_case("UTC"))
                    .map(Arc::<str>::from)
                    .unwrap_or_else(|| Arc::from("UTC"));
                (
                    id::DATETIME,
                    DataType::Timestamp(TimeUnit::Microsecond, Some(tz)),
                )
            }
            "Decimal" | "Decimal32" | "Decimal64" | "Decimal128" => {
                let a = args(payload);
                let (p, s) = match (sized_decimal_precision(base), a.as_slice()) {
                    // Decimal32(S) / Decimal64(S) / Decimal128(S)
                    (Some(p), [s]) => (p, s.parse::<i8>().ok()?),
                    // Decimal(P, S)
                    (None, [p, s]) => (p.parse::<u8>().ok()?, s.parse::<i8>().ok()?),
                    _ => return None,
                };
                // Decimal128 is the widest exact decimal the rest of the crate
                // handles (see decimal.rs); anything wider has no lossless
                // Arrow home here.
                if p == 0 || p > 38 || s < 0 || (s as u8) > p {
                    return None;
                }
                (id::DECIMAL, DataType::Decimal128(p, s))
            }
            _ => return None,
        };

        Some(ChColumn {
            type_id,
            arrow,
            clickhouse_inner: inner,
            nullable,
            arbitrary_precision_decimal: type_id == id::DECIMAL,
        })
    }

    /// The ClickHouse type to `CAST` a column to so its `FORMAT ArrowStream`
    /// output lands as exactly `arrow` — the other half of [`map_ch_type`], and
    /// the reason the read path needs no hand-written wire decoder.
    ///
    /// Several ClickHouse types do *not* round-trip through Arrow as their
    /// obvious counterpart, which is what this exists to paper over: `Date` is
    /// written as `UINT16` and `DateTime` as `UINT32` (only `Date32` and
    /// `DateTime64` produce Arrow `DATE32`/`TIMESTAMP`), `FixedString` becomes
    /// `FIXED_SIZE_BINARY`, `Enum8`/`Enum16` become their backing
    /// `INT8`/`INT16`, and a `DateTime64(P)`'s Arrow time unit follows `P`
    /// rather than being microseconds. Casting first makes the output type a
    /// function of the *destination* column type alone, so `type_overrides` and
    /// `column_transform_types` are honoured for free.
    ///
    /// `None` for an Arrow type with no ClickHouse spelling — the caller leaves
    /// such a column uncast and lets `decode_clickhouse` adapt whatever arrives.
    pub fn arrow_to_ch_cast_type(arrow: &DataType) -> Option<String> {
        Some(match arrow {
            DataType::Boolean => "Bool".to_string(),
            DataType::Int8 => "Int8".to_string(),
            DataType::Int16 => "Int16".to_string(),
            DataType::Int32 => "Int32".to_string(),
            DataType::Int64 => "Int64".to_string(),
            DataType::UInt8 => "UInt8".to_string(),
            DataType::UInt16 => "UInt16".to_string(),
            DataType::UInt32 => "UInt32".to_string(),
            DataType::UInt64 => "UInt64".to_string(),
            DataType::Float32 => "Float32".to_string(),
            DataType::Float64 => "Float64".to_string(),
            // `Binary` has no separate ClickHouse spelling — `String` is both.
            // It arrives as Arrow `Utf8` (the SELECT sets
            // `output_format_arrow_string_as_string=1`) and `decode_clickhouse`
            // casts it the rest of the way.
            DataType::Utf8 | DataType::Binary => "String".to_string(),
            DataType::Date32 => "Date32".to_string(),
            DataType::Timestamp(_, tz) => {
                let tz = tz.as_deref().unwrap_or("UTC");
                format!("DateTime64(6, '{}')", tz.replace('\'', "\\'"))
            }
            DataType::Decimal128(p, s) => format!("Decimal({p}, {s})"),
            _ => return None,
        })
    }
}

/// Schema resolution for an in-memory Arrow frame (`quickhouse.from_pandas`).
///
/// Every other source resolves its schema by asking an engine — a catalog
/// query, `DESCRIBE`, prepared-statement metadata, `tables.get`. A frame
/// carries its schema *with* the data, so this module's whole job is to decide
/// which Arrow types this crate is willing to move, and to say so by name when
/// the answer is no.
pub mod arrow_frame {
    use arrow_schema::{DataType, Schema};

    use crate::error::{EtlError, Result};
    use crate::types::ColumnType;

    /// Resolve a frame's Arrow schema into the destination column list.
    ///
    /// The frame *is* the catalog: each field's declared type and nullability
    /// are taken at face value. Two deliberate choices:
    ///
    /// * `clickhouse_inner` comes from
    ///   [`super::clickhouse::arrow_to_ch_cast_type`], reused rather than
    ///   rewritten. Its doc frames it as a `CAST` target, but every string it
    ///   can return is also a legal DDL type, and it returns `None` for exactly
    ///   the types this crate has no home for — which is the rejection we want.
    ///   **That reuse is only sound because the Python layer normalises every
    ///   timestamp to microseconds first**: the function pins any
    ///   `Timestamp(_, tz)` to `DateTime64(6, tz)` regardless of unit, so a
    ///   nanosecond column would be handed a microsecond DDL type and land
    ///   1000x off. The round-trip test in this crate's test module pins the
    ///   two mappings together.
    /// * `type_id` is `0`. It is documented on [`ColumnType`] as meaningful
    ///   only to its own source's decoder, and a frame has no decoder — the
    ///   bytes are already Arrow.
    pub fn columns_from_arrow_schema(schema: &Schema) -> Result<Vec<ColumnType>> {
        let mut seen = std::collections::HashSet::new();
        let mut cols = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let name = field.name();
            if !seen.insert(name.as_str()) {
                return Err(EtlError::config(format!(
                    "duplicate column '{name}' in the frame: a destination table cannot hold two \
                     columns of the same name, and matching them up positionally would silently \
                     misalign the data. Rename one before syncing."
                )));
            }
            let arrow = field.data_type();
            let clickhouse_inner = ch_type_for(arrow).ok_or_else(|| unsupported(name, arrow))?;
            cols.push(ColumnType {
                name: name.clone(),
                type_id: 0,
                nullable: field.is_nullable(),
                arrow: arrow.clone(),
                clickhouse_inner,
                arbitrary_precision_decimal: matches!(arrow, DataType::Decimal128(_, _)),
            });
        }
        if cols.is_empty() {
            return Err(EtlError::config(
                "the frame has no columns; there is nothing to transfer",
            ));
        }
        Ok(cols)
    }

    /// The destination type for one frame column.
    ///
    /// Delegates to [`super::clickhouse::arrow_to_ch_cast_type`] for everything
    /// except a **timezone-naive** timestamp, where the two callers genuinely
    /// want different things. That function defaults a missing timezone to
    /// `'UTC'`, which is right for a ClickHouse *source* — a ClickHouse datetime
    /// is an absolute instant whatever its type says. A frame is the opposite
    /// case: pandas' `datetime64[ns]` with no tz is a wall-clock reading that
    /// nobody has placed on the globe, so it maps to a naive `DateTime64(6)`,
    /// the same way the Postgres source maps `timestamp` (vs. `timestamptz`).
    ///
    /// Getting this wrong is not cosmetic. `transform::datetime_override_tz`
    /// reads the tz back *out* of this string to decide the destination Arrow
    /// type, so claiming UTC here would plan a tz-aware column, leave the frame
    /// supplying a naive one, and fail the batch with an unconvertible-types
    /// decode error.
    fn ch_type_for(arrow: &DataType) -> Option<String> {
        match arrow {
            // Rejected before anything else, and before delegating:
            // `arrow_to_ch_cast_type` maps a timestamp of ANY unit to
            // `DateTime64(6, ..)`, which is correct for a ClickHouse source
            // (the SELECT casts the column server-side to match) but a silent
            // 1000x error here, where the frame's own bytes are what arrive.
            // The Python layer normalises every timestamp to microseconds, so
            // this fires only for a caller who bypassed it.
            DataType::Timestamp(unit, _) if *unit != arrow_schema::TimeUnit::Microsecond => None,
            DataType::Timestamp(_, None) => Some("DateTime64(6)".to_string()),
            other => super::clickhouse::arrow_to_ch_cast_type(other),
        }
    }

    /// The rejection, with the fix. The Python layer normalises away everything
    /// it can (nanosecond timestamps, dictionaries, large/view string types,
    /// `float16`, `date64`, times), so reaching this generally means a genuinely
    /// unmappable type — or a caller who bypassed that layer.
    fn unsupported(column: &str, arrow: &DataType) -> EtlError {
        let hint = match arrow {
            DataType::Timestamp(unit, _) if *unit != arrow_schema::TimeUnit::Microsecond => {
                " — quickhouse standardises on microsecond timestamps; cast the column first \
                 (in pandas: `df[col] = df[col].dt.floor('us')`)"
            }
            DataType::Dictionary(_, _) => {
                " — decode the dictionary first (in pandas, a categorical: `df[col].astype(str)`)"
            }
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Map(_, _) => {
                " — nested types have no destination column type; serialize it, e.g. \
                 `df[col] = df[col].map(json.dumps)`"
            }
            DataType::Decimal256(_, _) => {
                " — quickhouse has no Decimal256; use a Decimal128 (precision <= 38), a float, \
                 or a string column"
            }
            DataType::Null => {
                " — the column holds only nulls, so there is no type to infer; give it one, \
                 e.g. `df[col] = df[col].astype('string')`"
            }
            _ => "",
        };
        EtlError::UnsupportedType {
            engine: "DataFrame",
            column: column.to_string(),
            type_name: format!("{arrow}{hint}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn may_coerce_to_null_covers_date_timestamp_and_decimal_only() {
        assert!(may_coerce_to_null(&DataType::Date32));
        assert!(may_coerce_to_null(&DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond,
            None
        )));
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
            let (arrow, ch) = map_mysql_type(ty, false, false, false).unwrap();
            assert_eq!(
                arrow,
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                "{ty:?} must map to a tz-aware (UTC) Arrow timestamp"
            );
            assert_eq!(ch, "DateTime64(6, 'UTC')", "{ty:?} ClickHouse type");
        }
    }

    /// `tinyint(1)` -> Bool is a display-width guess that some schemas violate
    /// by storing genuine small integers there (a real production incident:
    /// 9 columns across 8 tables silently collapsed to 0/1). `is_tinyint1:
    /// false` — what `tinyint1_as_bool=False` resolves to — must fall through
    /// to the plain integer arm, honouring UNSIGNED.
    #[test]
    fn mysql_tinyint1_bool_mapping_is_opt_outable() {
        use super::mysql::map_mysql_type;
        use mysql_async::consts::ColumnType as MyType;
        let t = MyType::MYSQL_TYPE_TINY;
        // Default (the convention): Bool.
        assert_eq!(
            map_mysql_type(t, false, true, false).unwrap(),
            (DataType::Boolean, "Bool".to_string())
        );
        // Opted out: the integer it really is, signedness preserved.
        assert_eq!(
            map_mysql_type(t, false, false, false).unwrap(),
            (DataType::Int8, "Int8".to_string())
        );
        assert_eq!(
            map_mysql_type(t, true, false, false).unwrap(),
            (DataType::UInt8, "UInt8".to_string())
        );
    }

    /// BLOB-family wire codes are shared by real BLOB and TEXT; only the charset
    /// (`is_binary`, collation 63) distinguishes them. TEXT must be Utf8 (BQ
    /// STRING); real BLOB stays Binary (BQ BYTES).
    #[test]
    fn mysql_blob_codes_split_on_binary_charset() {
        use super::mysql::map_mysql_type;
        use mysql_async::consts::ColumnType as MyType;
        for ty in [
            MyType::MYSQL_TYPE_TINY_BLOB,
            MyType::MYSQL_TYPE_MEDIUM_BLOB,
            MyType::MYSQL_TYPE_LONG_BLOB,
            MyType::MYSQL_TYPE_BLOB,
        ] {
            // Non-binary charset => TEXT => Utf8/STRING.
            assert_eq!(
                map_mysql_type(ty, false, false, false).unwrap().0,
                DataType::Utf8,
                "{ty:?} TEXT"
            );
            // Binary charset => real BLOB => Binary/BYTES.
            assert_eq!(
                map_mysql_type(ty, false, false, true).unwrap().0,
                DataType::Binary,
                "{ty:?} BLOB"
            );
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
            a2b(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            )),
            Some(BqType::Timestamp)
        );
        // Not produced by any current decoder (all three sources map TIME to
        // Utf8 text, not Arrow Time64) — confirms there's no silent gap.
        assert_eq!(a2b(&DataType::Time64(TimeUnit::Microsecond)), None);
    }

    /// Build a one-column frame schema.
    fn frame_field(arrow: DataType, nullable: bool) -> arrow_schema::Schema {
        arrow_schema::Schema::new(vec![arrow_schema::Field::new("c", arrow, nullable)])
    }

    fn resolve_one(arrow: DataType) -> ColumnType {
        crate::types::arrow_frame::columns_from_arrow_schema(&frame_field(arrow, true))
            .unwrap()
            .remove(0)
    }

    #[test]
    fn frame_resolves_every_type_the_python_layer_can_produce() {
        use arrow_schema::TimeUnit;
        for (arrow, ch) in [
            (DataType::Boolean, "Bool"),
            (DataType::Int8, "Int8"),
            (DataType::Int16, "Int16"),
            (DataType::Int32, "Int32"),
            (DataType::Int64, "Int64"),
            (DataType::UInt8, "UInt8"),
            (DataType::UInt16, "UInt16"),
            (DataType::UInt32, "UInt32"),
            (DataType::UInt64, "UInt64"),
            (DataType::Float32, "Float32"),
            (DataType::Float64, "Float64"),
            (DataType::Utf8, "String"),
            (DataType::Binary, "String"),
            (DataType::Date32, "Date32"),
            (DataType::Decimal128(18, 4), "Decimal(18, 4)"),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("Asia/Jakarta".into())),
                "DateTime64(6, 'Asia/Jakarta')",
            ),
        ] {
            let c = resolve_one(arrow.clone());
            assert_eq!(c.clickhouse_inner, ch, "{arrow}");
            assert_eq!(c.arrow, arrow);
            // Only a decimal is arbitrary-precision, which is what gates
            // `numeric_as_decimal` and the exact-Decimal promotion.
            assert_eq!(
                c.arbitrary_precision_decimal,
                matches!(arrow, DataType::Decimal128(_, _)),
                "{arrow}"
            );
        }
    }

    #[test]
    fn frame_keeps_a_naive_timestamp_naive() {
        use arrow_schema::TimeUnit;
        // Regression test, and the reason `arrow_frame` does not delegate this
        // case: `arrow_to_ch_cast_type` defaults a missing timezone to 'UTC',
        // which is right for a ClickHouse source (its datetimes are absolute
        // instants) and wrong for a frame (pandas' datetime64[ns] with no tz is
        // an unplaced wall clock). Claiming UTC here made
        // `transform::datetime_override_tz` plan a tz-AWARE Arrow column while
        // the frame supplied a naive one, and every batch failed to convert.
        let c = resolve_one(DataType::Timestamp(TimeUnit::Microsecond, None));
        assert_eq!(c.clickhouse_inner, "DateTime64(6)");
        assert_eq!(c.arrow, DataType::Timestamp(TimeUnit::Microsecond, None));
        // The tz-aware case must still carry its zone, or the same mismatch
        // happens in the other direction.
        let c = resolve_one(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ));
        assert_eq!(c.clickhouse_inner, "DateTime64(6, 'UTC')");
    }

    #[test]
    fn frame_nullability_is_taken_from_the_frame() {
        let cols = crate::types::arrow_frame::columns_from_arrow_schema(&frame_field(
            DataType::Int64,
            false,
        ))
        .unwrap();
        assert!(!cols[0].nullable);
        let cols = crate::types::arrow_frame::columns_from_arrow_schema(&frame_field(
            DataType::Int64,
            true,
        ))
        .unwrap();
        assert!(cols[0].nullable);
    }

    #[test]
    fn frame_rejects_unmappable_types_by_name_with_a_fix() {
        use arrow_schema::{Field, TimeUnit};
        use std::sync::Arc;
        let cases: Vec<(DataType, &str)> = vec![
            // The Python layer normalises these away; reaching Rust with one
            // means a caller bypassed it, so the hint still has to be useful.
            (
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                "floor('us')",
            ),
            (
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                "astype(str)",
            ),
            (
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                "json.dumps",
            ),
            (DataType::Decimal256(40, 2), "Decimal256"),
            (DataType::Null, "astype('string')"),
        ];
        for (arrow, hint) in cases {
            let err = crate::types::arrow_frame::columns_from_arrow_schema(&frame_field(
                arrow.clone(),
                true,
            ))
            .unwrap_err()
            .to_string();
            assert!(err.contains('c'), "{arrow}: {err}");
            assert!(
                err.contains(hint),
                "{arrow}: expected hint {hint:?}, got {err}"
            );
        }
    }

    #[test]
    fn frame_rejects_duplicate_column_names() {
        let schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", DataType::Int64, false),
            arrow_schema::Field::new("id", DataType::Utf8, true),
        ]);
        let err = crate::types::arrow_frame::columns_from_arrow_schema(&schema)
            .unwrap_err()
            .to_string();
        assert!(err.contains("duplicate column 'id'"), "{err}");
    }

    #[test]
    fn frame_rejects_a_schema_with_no_columns() {
        let err =
            crate::types::arrow_frame::columns_from_arrow_schema(&arrow_schema::Schema::empty())
                .unwrap_err()
                .to_string();
        assert!(err.contains("no columns"), "{err}");
    }

    #[test]
    fn nullable_low_cardinality_wraps_the_other_way_round() {
        // ClickHouse rejects `Nullable(LowCardinality(String))` outright
        // (Code: 43) — the only legal spelling is with Nullable on the inside.
        let c = ColumnType {
            name: "tag".into(),
            type_id: 0,
            nullable: true,
            arrow: DataType::Utf8,
            clickhouse_inner: "LowCardinality(String)".into(),
            arbitrary_precision_decimal: false,
        };
        assert_eq!(c.clickhouse_type(), "LowCardinality(Nullable(String))");
        // Not nullable: untouched.
        let c = ColumnType {
            nullable: false,
            ..c
        };
        assert_eq!(c.clickhouse_type(), "LowCardinality(String)");
        // Already spelled with Nullable inside: not double-wrapped.
        let c = ColumnType {
            nullable: true,
            clickhouse_inner: "LowCardinality(Nullable(String))".into(),
            ..c
        };
        assert_eq!(c.clickhouse_type(), "LowCardinality(Nullable(String))");
        // A type that merely mentions LowCardinality inside is not a wrapper
        // around the whole thing, and keeps the ordinary outer Nullable.
        let c = ColumnType {
            nullable: true,
            clickhouse_inner: "Map(LowCardinality(String), UInt8)".into(),
            ..c
        };
        assert_eq!(
            c.clickhouse_type(),
            "Nullable(Map(LowCardinality(String), UInt8))"
        );
    }

    #[test]
    fn ch_maps_the_scalar_types_and_unwraps_nullable() {
        use crate::types::clickhouse::map_ch_type;
        let c = map_ch_type("Int32").unwrap();
        assert_eq!(c.arrow, DataType::Int32);
        assert_eq!(c.clickhouse_inner, "Int32");
        assert!(!c.nullable);

        let c = map_ch_type("Nullable(String)").unwrap();
        assert_eq!(c.arrow, DataType::Utf8);
        // The Nullable wrapper is stripped from the DDL type and moved onto
        // the flag — `ColumnType::clickhouse_type()` re-applies it.
        assert_eq!(c.clickhouse_inner, "String");
        assert!(c.nullable);

        // Types with no Arrow counterpart of their own still round-trip into
        // the destination DDL rather than flattening to String.
        for (declared, inner) in [
            ("UUID", "UUID"),
            ("IPv4", "IPv4"),
            ("FixedString(16)", "FixedString(16)"),
            ("Enum8('a' = 1, 'b' = 2)", "Enum8('a' = 1, 'b' = 2)"),
        ] {
            let c = map_ch_type(declared).unwrap();
            assert_eq!(c.arrow, DataType::Utf8, "{declared}");
            assert_eq!(c.clickhouse_inner, inner, "{declared}");
        }
    }

    #[test]
    fn ch_low_cardinality_keeps_the_wrapper_but_not_the_nullable() {
        use crate::types::clickhouse::map_ch_type;
        let c = map_ch_type("LowCardinality(Nullable(String))").unwrap();
        assert_eq!(c.arrow, DataType::Utf8);
        assert!(c.nullable);
        // Nullable moves out of the middle of the type, so the destination
        // column stays dictionary-encoded instead of becoming a plain String.
        assert_eq!(c.clickhouse_inner, "LowCardinality(String)");
    }

    #[test]
    fn ch_datetimes_are_always_tz_aware() {
        use crate::types::clickhouse::map_ch_type;
        use arrow_schema::TimeUnit;
        // A bare DateTime carries no timezone in its name, but the stored
        // value is an absolute instant, so it resolves as UTC rather than naive.
        for declared in ["DateTime", "DateTime64(3)", "DateTime64(9)"] {
            let c = map_ch_type(declared).unwrap();
            assert_eq!(
                c.arrow,
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                "{declared}"
            );
        }
        // A named timezone is carried through to the Arrow type.
        let c = map_ch_type("DateTime64(3, 'Asia/Jakarta')").unwrap();
        assert_eq!(
            c.arrow,
            DataType::Timestamp(TimeUnit::Microsecond, Some("Asia/Jakarta".into()))
        );
        // Regression guard: the precision digit of `DateTime64(3)` is the last
        // argument too, and must not be read as a timezone name.
        assert_eq!(
            map_ch_type("DateTime64(3)").unwrap().arrow,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn ch_decimals_resolve_exact_precision_and_scale() {
        use crate::types::clickhouse::map_ch_type;
        let c = map_ch_type("Decimal(18, 4)").unwrap();
        assert_eq!(c.arrow, DataType::Decimal128(18, 4));
        assert!(c.arbitrary_precision_decimal);
        // The fixed-width spellings carry their precision implicitly.
        assert_eq!(
            map_ch_type("Decimal64(6)").unwrap().arrow,
            DataType::Decimal128(18, 6)
        );
        assert_eq!(
            map_ch_type("Decimal32(2)").unwrap().arrow,
            DataType::Decimal128(9, 2)
        );
        // Wider than Decimal128 has no lossless Arrow home here.
        assert!(map_ch_type("Decimal256(10)").is_none());
        assert!(map_ch_type("Decimal(40, 2)").is_none());
    }

    #[test]
    fn ch_rejects_the_types_this_source_cannot_read() {
        use crate::types::clickhouse::map_ch_type;
        for declared in [
            "Array(String)",
            "Map(String, UInt64)",
            "Tuple(UInt8, String)",
            "JSON",
            "Int256",
            "Nothing",
        ] {
            assert!(
                map_ch_type(declared).is_none(),
                "{declared} should be rejected"
            );
        }
    }

    #[test]
    fn ch_cast_targets_pin_the_arrow_output_type() {
        use crate::types::clickhouse::arrow_to_ch_cast_type as cast;
        use arrow_schema::TimeUnit;
        // The four that exist because ClickHouse's Arrow writer does NOT use
        // the obvious counterpart: Date -> UINT16 and DateTime -> UINT32, and a
        // DateTime64's unit follows its own precision.
        assert_eq!(cast(&DataType::Date32).unwrap(), "Date32");
        assert_eq!(
            cast(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            ))
            .unwrap(),
            "DateTime64(6, 'UTC')"
        );
        // A naive destination timestamp still has to name a timezone for
        // ClickHouse; UTC keeps the instant unchanged.
        assert_eq!(
            cast(&DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap(),
            "DateTime64(6, 'UTC')"
        );
        assert_eq!(
            cast(&DataType::Decimal128(18, 4)).unwrap(),
            "Decimal(18, 4)"
        );
        // Binary has no ClickHouse spelling of its own — it rides in as String
        // and `decode_clickhouse` finishes the conversion.
        assert_eq!(cast(&DataType::Binary).unwrap(), "String");
        assert_eq!(cast(&DataType::Utf8).unwrap(), "String");
    }

    #[test]
    fn ch_round_trips_every_mapped_type_through_its_cast_target() {
        use crate::types::clickhouse::{arrow_to_ch_cast_type, map_ch_type};
        // Every type `map_ch_type` accepts must have a cast target, or
        // `select_sql` would silently fall back to reading it uncast.
        for declared in [
            "Bool",
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "Float32",
            "Float64",
            "String",
            "FixedString(4)",
            "UUID",
            "IPv6",
            "Enum16('x' = 1)",
            "Date",
            "Date32",
            "DateTime",
            "DateTime64(6, 'UTC')",
            "Decimal(10, 2)",
        ] {
            let c = map_ch_type(declared).unwrap_or_else(|| panic!("{declared} should map"));
            assert!(
                arrow_to_ch_cast_type(&c.arrow).is_some(),
                "{declared} -> {:?} has no cast target",
                c.arrow
            );
        }
    }
}
