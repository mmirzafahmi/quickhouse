//! Error type shared across the engine.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, EtlError>;

#[derive(Debug, Error)]
pub enum EtlError {
    #[error("postgres error: {0}")]
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
