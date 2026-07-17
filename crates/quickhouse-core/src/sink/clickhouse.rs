//! ClickHouse sink over the HTTP interface.
//!
//! Inserts stream Arrow batches serialized as an Arrow IPC stream, ingested by
//! ClickHouse's native `FORMAT ArrowStream`. DDL, reads, and table swaps go
//! through the same HTTP endpoint as plain SQL.

use std::io::Write;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use flate2::write::GzEncoder;
use flate2::Compression as GzLevel;
use reqwest::Client;

use crate::config::{ClickHouseConfig, Compression};
use crate::error::{EtlError, Result};

#[derive(Clone)]
pub struct ClickHouseSink {
    client: Client,
    cfg: Arc<ClickHouseConfig>,
}

impl ClickHouseSink {
    pub fn new(cfg: ClickHouseConfig) -> Result<Self> {
        let client = Client::builder()
            .build()
            .map_err(EtlError::from)?;
        Ok(Self {
            client,
            cfg: Arc::new(cfg),
        })
    }

    pub fn database(&self) -> &str {
        &self.cfg.database
    }

    fn base_request(&self) -> reqwest::RequestBuilder {
        self.client
            .post(&self.cfg.url)
            .header("X-ClickHouse-User", &self.cfg.user)
            .header("X-ClickHouse-Key", &self.cfg.password)
            .query(&[("database", &self.cfg.database)])
    }

    /// Execute a statement that returns no rows (DDL, TRUNCATE, EXCHANGE, ...).
    pub async fn execute(&self, sql: &str) -> Result<()> {
        let resp = self.base_request().body(sql.to_string()).send().await?;
        Self::check(resp).await.map(|_| ())
    }

    /// Run a query expected to return a single scalar; `None` if it returns no rows.
    pub async fn query_scalar(&self, sql: &str) -> Result<Option<String>> {
        let resp = self.base_request().body(sql.to_string()).send().await?;
        let body = Self::check(resp).await?;
        let trimmed = body.trim();
        if trimmed.is_empty() {
            Ok(None)
        } else {
            Ok(Some(trimmed.to_string()))
        }
    }

    pub async fn table_exists(&self, table: &str) -> Result<bool> {
        let sql = format!(
            "EXISTS TABLE {}.{}",
            ident(&self.cfg.database),
            ident(table)
        );
        Ok(self.query_scalar(&sql).await?.as_deref() == Some("1"))
    }

    /// Insert a group of Arrow batches into `table` via `FORMAT ArrowStream`.
    /// Returns the number of compressed/plain bytes sent.
    pub async fn insert_batches(
        &self,
        table: &str,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<u64> {
        if batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(0);
        }
        let payload = serialize_ipc(schema, batches)?;

        let query = format!(
            "INSERT INTO {}.{} FORMAT ArrowStream",
            ident(&self.cfg.database),
            ident(table)
        );

        let mut req = self
            .base_request()
            .query(&[("query", query.as_str())]);

        let body = match self.cfg.compression {
            Compression::Gzip => {
                let gz = gzip(&payload)?;
                req = req.header("Content-Encoding", "gzip");
                gz
            }
            Compression::None => payload,
        };
        let sent = body.len() as u64;

        let resp = req.body(body).send().await?;
        Self::check(resp).await?;
        Ok(sent)
    }

    /// Atomically replace `dest` with `staging` (both must exist).
    pub async fn exchange_tables(&self, dest: &str, staging: &str) -> Result<()> {
        let sql = format!(
            "EXCHANGE TABLES {}.{} AND {}.{}",
            ident(&self.cfg.database),
            ident(dest),
            ident(&self.cfg.database),
            ident(staging)
        );
        self.execute(&sql).await
    }

    pub async fn drop_table(&self, table: &str) -> Result<()> {
        let sql = format!(
            "DROP TABLE IF EXISTS {}.{}",
            ident(&self.cfg.database),
            ident(table)
        );
        self.execute(&sql).await
    }

    async fn check(resp: reqwest::Response) -> Result<String> {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if status.is_success() {
            Ok(text)
        } else {
            Err(EtlError::clickhouse(format!("HTTP {status}: {text}")))
        }
    }
}

fn ident(name: &str) -> String {
    crate::ddl::quote_ident(name)
}

/// Serialize batches to an in-memory Arrow IPC stream.
fn serialize_ipc(schema: SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema)?;
        for b in batches {
            writer.write(b)?;
        }
        writer.finish()?;
    }
    Ok(buf)
}

fn gzip(data: &[u8]) -> Result<Vec<u8>> {
    let mut enc = GzEncoder::new(Vec::new(), GzLevel::fast());
    enc.write_all(data).map_err(EtlError::from)?;
    enc.finish().map_err(EtlError::from)
}
