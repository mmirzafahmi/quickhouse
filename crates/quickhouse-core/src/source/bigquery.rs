//! BigQuery source: auth, schema resolution, and streaming reads via the
//! BigQuery Storage Read API.
//!
//! Unlike Postgres/MySQL, BigQuery has no TCP wire protocol to hand-decode —
//! it's accessed over gRPC, and the Storage Read API's wire format is already
//! Apache Arrow. The `google-cloud-bigquery` crate's own `storage::Iterator`
//! decodes that Arrow data into individual `Row` objects for a friendly
//! typed-by-index API, and drains BigQuery's parallel read streams
//! **sequentially within one task** rather than fanning them out
//! concurrently — `max_stream_count` only tells BigQuery how many streams to
//! prepare server-side; consumption here is single-threaded. True
//! client-side parallel consumption would need the crate's lower-level gRPC
//! client directly; deferred as a possible future enhancement.
//!
//! `source_table` reads a plain table's schema via the REST `tables.get` API.
//! `source_query` runs the query as a job (whose response includes the
//! result schema directly) then resolves its destination table for the
//! actual bulk Storage API read.

use google_cloud_bigquery::client::google_cloud_auth::credentials::CredentialsFile;
use google_cloud_bigquery::client::{Client, ClientConfig, ReadTableOption};
use google_cloud_bigquery::http::job::get::GetJobRequest;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::http::job::JobType;
use google_cloud_bigquery::http::table::{TableFieldMode, TableReference, TableSchema};
use google_cloud_googleapis::cloud::bigquery::storage::v1::read_session::TableReadOptions;

use crate::error::{EtlError, Result};
use crate::types::bigquery::map_type;
use crate::types::ColumnType;

pub struct BigQuerySource {
    project_id: Option<String>,
    credentials_file: Option<String>,
}

impl BigQuerySource {
    pub fn new(project_id: Option<String>, credentials_file: Option<String>) -> Self {
        Self {
            project_id,
            credentials_file,
        }
    }

    /// Authenticate and return a connected client plus the resolved project ID.
    pub async fn connect(&self) -> Result<(Client, String)> {
        let (config, resolved_project) = match &self.credentials_file {
            Some(path) => {
                let cred = CredentialsFile::new_from_file(path.clone())
                    .await
                    .map_err(|e| EtlError::other(format!("bigquery credentials error: {e}")))?;
                ClientConfig::new_with_credentials(cred)
                    .await
                    .map_err(|e| EtlError::other(format!("bigquery auth error: {e}")))?
            }
            None => ClientConfig::new_with_auth()
                .await
                .map_err(|e| EtlError::other(format!("bigquery auth error: {e}")))?,
        };
        let project_id = self
            .project_id
            .clone()
            .or(resolved_project)
            .ok_or_else(|| EtlError::config("bigquery project_id could not be resolved from credentials; pass project_id explicitly"))?;
        let client = Client::new(config)
            .await
            .map_err(|e| EtlError::other(format!("bigquery client error: {e}")))?;
        Ok((client, project_id))
    }

    /// Parse `"dataset.table"` or `"project.dataset.table"` into a [`TableReference`].
    pub fn parse_table_ref(&self, spec: &str, default_project: &str) -> Result<TableReference> {
        let parts: Vec<&str> = spec.split('.').collect();
        match parts.as_slice() {
            [dataset, table] => Ok(TableReference {
                project_id: default_project.to_string(),
                dataset_id: dataset.to_string(),
                table_id: table.to_string(),
            }),
            [project, dataset, table] => Ok(TableReference {
                project_id: project.to_string(),
                dataset_id: dataset.to_string(),
                table_id: table.to_string(),
            }),
            _ => Err(EtlError::config(format!(
                "invalid BigQuery table reference '{spec}'; expected 'dataset.table' or 'project.dataset.table'"
            ))),
        }
    }

    /// Resolve a base table's columns via the REST `tables.get` API.
    pub async fn resolve_table_columns(&self, client: &Client, table: &TableReference) -> Result<Vec<ColumnType>> {
        let t = client
            .table()
            .get(&table.project_id, &table.dataset_id, &table.table_id)
            .await
            .map_err(|e| EtlError::other(format!("bigquery tables.get error: {e}")))?;
        let schema = t
            .schema
            .ok_or_else(|| EtlError::other(format!("BigQuery table '{}' has no schema", table.table_id)))?;
        columns_from_schema(&schema)
    }

    /// Run `query` as a job and return (its result columns, the destination
    /// table to bulk-read via the Storage Read API). Polls until the job
    /// completes; does not support multi-statement script jobs.
    pub async fn run_query(
        &self,
        client: &Client,
        project_id: &str,
        query: &str,
    ) -> Result<(Vec<ColumnType>, TableReference)> {
        let request = QueryRequest {
            query: query.to_string(),
            ..Default::default()
        };
        let mut result = client
            .job()
            .query(project_id, &request)
            .await
            .map_err(|e| EtlError::other(format!("bigquery query error: {e}")))?;

        let job_ref = result.job_reference.clone();
        while !result.job_complete {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            result = client
                .job()
                .query(
                    project_id,
                    &QueryRequest {
                        query: query.to_string(),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| EtlError::other(format!("bigquery query error: {e}")))?;
        }

        let schema = result
            .schema
            .ok_or_else(|| EtlError::other("BigQuery query returned no schema"))?;
        let columns = columns_from_schema(&schema)?;

        let job = client
            .job()
            .get(
                &job_ref.project_id,
                &job_ref.job_id,
                &GetJobRequest {
                    location: job_ref.location.clone(),
                },
            )
            .await
            .map_err(|e| EtlError::other(format!("bigquery jobs.get error: {e}")))?;
        let query_config = match &job.configuration.job {
            JobType::Query(config) => config,
            _ => return Err(EtlError::other("BigQuery job is not a query job")),
        };
        let destination = query_config
            .destination_table
            .clone()
            .ok_or_else(|| EtlError::other("BigQuery query job has no destination table (script jobs aren't supported)"))?;

        Ok((columns, destination))
    }

    /// Read a table's rows via the Storage Read API. `row_restriction` is a
    /// BigQuery SQL-like filter expression (used for the incremental
    /// watermark filter); `columns` are the source column names to project.
    /// `max_stream_count` maps from our `parallelism` config — see the
    /// module docs for why consumption is still single-threaded.
    pub async fn read_table<T>(
        &self,
        client: &Client,
        table: &TableReference,
        columns: &[String],
        row_restriction: Option<&str>,
        max_stream_count: i32,
    ) -> Result<google_cloud_bigquery::storage::Iterator<T>>
    where
        T: google_cloud_bigquery::storage::value::StructDecodable,
    {
        let option = ReadTableOption::default()
            .with_max_stream_count(max_stream_count)
            .with_session_read_options(TableReadOptions {
                selected_fields: columns.to_vec(),
                row_restriction: row_restriction.unwrap_or_default().to_string(),
                ..Default::default()
            });
        client
            .read_table::<T>(table, Some(option))
            .await
            .map_err(|e| EtlError::other(format!("bigquery read_table error: {e}")))
    }

    /// Read the current max watermark value as text (for incremental sync).
    pub async fn max_watermark(
        &self,
        client: &Client,
        project_id: &str,
        table_sql: &str,
        watermark: &str,
    ) -> Result<Option<String>> {
        let query = format!("SELECT CAST(MAX(`{watermark}`) AS STRING) AS mx FROM {table_sql}");
        let request = QueryRequest {
            query,
            ..Default::default()
        };
        let mut iter = client
            .query::<google_cloud_bigquery::query::row::Row>(project_id, request)
            .await
            .map_err(|e| EtlError::other(format!("bigquery query error: {e}")))?;
        if let Some(row) = iter
            .next()
            .await
            .map_err(|e| EtlError::other(format!("bigquery row error: {e}")))?
        {
            let v: Option<String> = row
                .column(0)
                .map_err(|e| EtlError::other(format!("bigquery column error: {e}")))?;
            Ok(v)
        } else {
            Ok(None)
        }
    }
}

/// The BigQuery-SQL table expression for a resolved [`TableReference`], for
/// interpolating into ad hoc queries like [`BigQuerySource::max_watermark`].
pub fn table_sql(table: &TableReference) -> String {
    format!("`{}`.`{}`.`{}`", table.project_id, table.dataset_id, table.table_id)
}

fn columns_from_schema(schema: &TableSchema) -> Result<Vec<ColumnType>> {
    let mut cols = Vec::with_capacity(schema.fields.len());
    for f in &schema.fields {
        let (type_id, arrow, ch_inner) = map_type(&f.data_type).ok_or_else(|| EtlError::UnsupportedType {
            oid: 0,
            context: f.name.clone(),
        })?;
        let nullable = !matches!(f.mode, Some(TableFieldMode::Required));
        cols.push(ColumnType {
            name: f.name.clone(),
            type_id,
            nullable,
            arrow,
            clickhouse_inner: ch_inner,
        });
    }
    Ok(cols)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_table_ref_two_parts() {
        let src = BigQuerySource::new(None, None);
        let t = src.parse_table_ref("dataset.table", "default-proj").unwrap();
        assert_eq!(t.project_id, "default-proj");
        assert_eq!(t.dataset_id, "dataset");
        assert_eq!(t.table_id, "table");
    }

    #[test]
    fn parse_table_ref_three_parts() {
        let src = BigQuerySource::new(None, None);
        let t = src.parse_table_ref("proj.dataset.table", "default-proj").unwrap();
        assert_eq!(t.project_id, "proj");
        assert_eq!(t.dataset_id, "dataset");
        assert_eq!(t.table_id, "table");
    }

    #[test]
    fn parse_table_ref_invalid() {
        let src = BigQuerySource::new(None, None);
        assert!(src.parse_table_ref("just_a_table", "default-proj").is_err());
    }

    #[test]
    fn table_sql_quotes_all_three_parts() {
        let t = TableReference {
            project_id: "p".into(),
            dataset_id: "d".into(),
            table_id: "t".into(),
        };
        assert_eq!(table_sql(&t), "`p`.`d`.`t`");
    }
}
