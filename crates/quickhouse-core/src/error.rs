//! Error type shared across the engine.

use std::error::Error as StdError;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, EtlError>;

#[derive(Debug, Error)]
pub enum EtlError {
    #[error("postgres error: {}", fmt_pg_error(.0))]
    Postgres(#[from] tokio_postgres::Error),

    #[error("clickhouse error: {0}")]
    ClickHouse(String),

    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    // Not Postgres-specific despite the historical name — MySQL and BigQuery's
    // decoders (decode_mysql.rs, decode_bigquery.rs) raise this too, so the
    // message stays engine-agnostic. Genuinely Postgres-specific detail (e.g.
    // "bad COPY signature") is spelled out in the message text at the call site.
    #[error("decode error: {0}")]
    Decode(String),

    /// A source column's type has no mapping to Arrow/ClickHouse yet (e.g. a
    /// Postgres array, PostGIS geometry, or a BigQuery `RECORD`). Distinct from
    /// [`Decode`] — this fails during schema resolution, before any row is
    /// read, and is something the caller can work around (see the message).
    #[error(
        "unsupported {engine} type `{type_name}` for column `{column}` — quickhouse doesn't know \
         how to map it to ClickHouse yet. Work around it with `exclude=[\"{column}\"]`, or read a \
         cast/converted value instead via a custom `source_query` (e.g. `CAST({column} AS ...)`)."
    )]
    UnsupportedType {
        /// Human-facing source name, e.g. `"PostgreSQL"`, `"MySQL"`, `"BigQuery"`.
        engine: &'static str,
        column: String,
        type_name: String,
    },

    #[error("configuration error: {0}")]
    Config(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("transfer was cancelled")]
    Cancelled,

    /// A failure that should be structurally impossible from valid source data
    /// or config alone — e.g. a type resolved during schema mapping that the
    /// matching decoder doesn't implement a builder for. Framed as "report
    /// this" rather than "fix your data/config" so it isn't mistaken for one.
    #[error(
        "internal error (this indicates a quickhouse bug, not a problem with your data or \
         config — please file an issue at https://github.com/mmirzafahmi/quickhouse/issues \
         including this message): {0}"
    )]
    Internal(String),

    /// Attaches a short "where this happened" prefix (a table, column, or
    /// partition name) to an existing error without discarding it — the
    /// original error and its full source chain are still reachable via
    /// `source()`. Wrapping more than once composes left-to-right, e.g.
    /// `"orders -> analytics.orders: column 'amount': decode error: ..."`.
    #[error("{context}: {source}")]
    Context {
        context: String,
        #[source]
        source: Box<EtlError>,
    },

    #[error("{0}")]
    Other(String),
}

/// Format a `tokio_postgres::Error` with the detail its own `Display` hides.
///
/// `tokio_postgres::Error`'s `Display` collapses to a generic tag — "db error"
/// for anything the server rejected, "error communicating with the server" for
/// transport failures — throwing away the actual cause. For server-side errors
/// the real message, SQLSTATE code, and detail/hint live in the attached
/// `DbError`; for transport errors they're in the `source()` chain. Surface
/// both so failures like a cancelled statement (`57014`) or a hot-standby
/// recovery conflict (`40001`) are diagnosable instead of an opaque "db error".
fn fmt_pg_error(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        let mut s = format!("{} [{}] {}", db.severity(), db.code().code(), db.message());
        if let Some(detail) = db.detail() {
            s.push_str(" | detail: ");
            s.push_str(detail);
        }
        if let Some(hint) = db.hint() {
            s.push_str(" | hint: ");
            s.push_str(hint);
        }
        s
    } else {
        // Transport/protocol error (no server DbError) — walk the source chain
        // so it's more than just the generic top-level tag.
        let mut s = e.to_string();
        let mut src = StdError::source(e);
        while let Some(inner) = src {
            s.push_str(": ");
            s.push_str(&inner.to_string());
            src = inner.source();
        }
        s
    }
}

impl EtlError {
    pub fn decode(msg: impl Into<String>) -> Self {
        EtlError::Decode(msg.into())
    }
    pub fn config(msg: impl Into<String>) -> Self {
        EtlError::Config(msg.into())
    }
    pub fn clickhouse(msg: impl Into<String>) -> Self {
        EtlError::ClickHouse(msg.into())
    }
    pub fn other(msg: impl Into<String>) -> Self {
        EtlError::Other(msg.into())
    }
    pub fn internal(msg: impl Into<String>) -> Self {
        EtlError::Internal(msg.into())
    }

    /// Prefix this error with "where it happened" (a table, column, or
    /// partition name) without losing the original error. See the `Context`
    /// variant's docs.
    pub fn context(self, ctx: impl Into<String>) -> Self {
        EtlError::Context {
            context: ctx.into(),
            source: Box::new(self),
        }
    }
}

/// Adds `.context(...)` to any `Result<T, EtlError>`, so a fallible call can be
/// annotated inline — `conn.query(..).await.context("resolving columns")?` —
/// instead of a `.map_err(|e| e.context(...))` closure at every call site.
pub trait ResultExt<T> {
    fn context(self, ctx: impl Into<String>) -> Result<T>;
}

impl<T> ResultExt<T> for Result<T> {
    fn context(self, ctx: impl Into<String>) -> Result<T> {
        self.map_err(|e| e.context(ctx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_prefixes_without_losing_the_original_message() {
        let e = EtlError::decode("short int4").context("column 'id'");
        assert_eq!(e.to_string(), "column 'id': decode error: short int4");
    }

    #[test]
    fn context_composes_left_to_right_when_wrapped_twice() {
        let e = EtlError::decode("short int4")
            .context("column 'id'")
            .context("orders -> analytics.orders");
        assert_eq!(
            e.to_string(),
            "orders -> analytics.orders: column 'id': decode error: short int4"
        );
    }

    #[test]
    fn result_ext_context_only_touches_the_err_path() {
        let ok: Result<i32> = Ok(5);
        assert_eq!(ok.context("irrelevant").unwrap(), 5);

        let err: Result<i32> = Err(EtlError::other("boom"));
        assert_eq!(err.context("while doing x").unwrap_err().to_string(), "while doing x: boom");
    }

    #[test]
    fn unsupported_type_names_the_real_engine_and_type() {
        let e = EtlError::UnsupportedType {
            engine: "MySQL",
            column: "location".into(),
            type_name: "MYSQL_TYPE_GEOMETRY".into(),
        };
        let msg = e.to_string();
        assert!(msg.contains("MySQL"), "must name the real engine, not PostgreSQL: {msg}");
        assert!(msg.contains("MYSQL_TYPE_GEOMETRY"), "must name the real type: {msg}");
        assert!(msg.contains("location"), "must name the column: {msg}");
        assert!(msg.contains("exclude"), "must suggest a workaround: {msg}");
    }

    #[test]
    fn internal_error_frames_it_as_a_bug_not_a_data_problem() {
        let msg = EtlError::internal("no column builder for Arrow type Utf8").to_string();
        assert!(msg.contains("quickhouse bug"), "must say it's not the user's fault: {msg}");
        assert!(msg.contains("no column builder"), "must keep the original detail: {msg}");
    }
}
