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

    #[error("binary COPY decode error: {0}")]
    Decode(String),

    #[error("unsupported PostgreSQL type: oid={oid} ({context})")]
    UnsupportedType { oid: u32, context: String },

    #[error("configuration error: {0}")]
    Config(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("transfer was cancelled")]
    Cancelled,

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
}
