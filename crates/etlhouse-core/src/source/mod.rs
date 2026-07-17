pub mod bigquery;
pub mod mysql;
pub mod postgres;

pub use bigquery::BigQuerySource;
pub use mysql::MySqlSource;
pub use postgres::{PgSource, Partition};

/// Which database engine a transfer reads from. `sync.rs` matches on this to
/// dispatch to each engine's own connection/decode logic.
pub enum Source {
    Postgres(PgSource),
    MySql(MySqlSource),
    BigQuery(BigQuerySource),
}
