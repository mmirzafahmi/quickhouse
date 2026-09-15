//! PyO3 bindings for quickhouse-core.
//!
//! Exposes `Postgres`, `MySQL`, `BigQuery`, `ClickHouse`, `sync(...)`, and the
//! result/progress types. `BigQuery` and `ClickHouse` each double as either a
//! source or a destination for `sync()` (see their doc comments). The transfer
//! runs on a Tokio runtime inside
//! `Python::allow_threads`, so the GIL is released for the duration and only
//! re-acquired to fire `on_progress`.
//!
//! `#![allow(clippy::useless_conversion)]`: the `#[pyfunction]` macro's own
//! generated wrapper around `sync()`'s `PyResult` return type trips this lint
//! once a second fallible `?` conversion (`target.into_config()?`, alongside
//! the pre-existing `parse_mode(&mode)?`) is present in the function body —
//! the warning's span lands in macro-generated code no local `#[allow]` on
//! the visible function/statement can reach; confirmed not a real redundant
//! conversion in this file's own code.
#![allow(clippy::useless_conversion)]

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Once};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use quickhouse_core as core;

fn map_err(e: core::EtlError) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

static INIT_LOGGING: Once = Once::new();

/// Print `quickhouse_core`'s step-by-step `tracing` logs to stderr the first
/// time `sync()` runs. Defaults to INFO level for our own crate (connect,
/// schema resolution, DDL, per-partition progress, watermark handling, swap)
/// while staying quiet about noisy dependency internals (tokio/hyper/tonic/
/// etc.); override with the `RUST_LOG` env var, e.g. `RUST_LOG=debug` for
/// everything or `RUST_LOG=quickhouse_core=debug` for just this crate's SQL/DDL
/// text. This is separate from `on_progress`/`progress_bar()`, which only
/// fires during the actual row-ingestion loop.
fn init_logging() {
    INIT_LOGGING.call_once(|| {
        use tracing_subscriber::EnvFilter;
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("quickhouse_core=info"));
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .with_writer(std::io::stderr)
            .try_init();
    });
}

/// Assemble a `scheme://user:pass@host:port/db` DSN from discrete fields,
/// percent-encoding the userinfo/database so special characters (`@ : / ...`)
/// survive. Over-encoding unreserved chars is harmless — the driver's URL parser
/// decodes them back. `host`/`port` are used verbatim.
fn build_dsn(
    scheme: &str,
    host: &str,
    port: Option<u16>,
    user: Option<&str>,
    password: Option<&str>,
    database: Option<&str>,
) -> String {
    use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
    let enc = |s: &str| utf8_percent_encode(s, NON_ALPHANUMERIC).to_string();
    let mut url = format!("{scheme}://");
    if let Some(u) = user {
        url.push_str(&enc(u));
        if let Some(p) = password {
            url.push(':');
            url.push_str(&enc(p));
        }
        url.push('@');
    }
    url.push_str(host);
    if let Some(port) = port {
        url.push_str(&format!(":{port}"));
    }
    url.push('/');
    if let Some(db) = database {
        url.push_str(&enc(db));
    }
    url
}

/// Resolve a connection to a single DSN string from either an explicit `dsn` or
/// discrete `host`/`port`/... fields (exactly one path must be given).
#[allow(clippy::too_many_arguments)]
fn resolve_dsn(
    scheme: &str,
    role: &str,
    dsn: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
) -> PyResult<String> {
    match (dsn, host) {
        (Some(d), None) => Ok(d),
        (None, Some(h)) => Ok(build_dsn(
            scheme,
            &h,
            port,
            user.as_deref(),
            password.as_deref(),
            database.as_deref(),
        )),
        (Some(_), Some(_)) => Err(PyRuntimeError::new_err(format!(
            "{role}: pass either `dsn` or discrete host/port/user/... fields, not both"
        ))),
        (None, None) => Err(PyRuntimeError::new_err(format!(
            "{role}: pass a `dsn`, or discrete `host` (with optional port/user/password/database)"
        ))),
    }
}

/// PostgreSQL source connection descriptor.
#[pyclass]
#[derive(Clone)]
struct Postgres {
    dsn: String,
    statement_timeout_secs: u64,
    ca_cert_file: Option<String>,
    client_cert_file: Option<String>,
    client_key_file: Option<String>,
}

#[pymethods]
impl Postgres {
    #[new]
    #[pyo3(signature = (dsn=None, *, host=None, port=None, user=None, password=None, database=None, statement_timeout_secs=0, ca_cert_file=None, client_cert_file=None, client_key_file=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        dsn: Option<String>,
        host: Option<String>,
        port: Option<u16>,
        user: Option<String>,
        password: Option<String>,
        database: Option<String>,
        statement_timeout_secs: u64,
        ca_cert_file: Option<String>,
        client_cert_file: Option<String>,
        client_key_file: Option<String>,
    ) -> PyResult<Self> {
        let dsn = resolve_dsn(
            "postgresql",
            "Postgres",
            dsn,
            host,
            port,
            user,
            password,
            database,
        )?;
        Ok(Postgres {
            dsn,
            statement_timeout_secs,
            ca_cert_file,
            client_cert_file,
            client_key_file,
        })
    }

    fn __repr__(&self) -> String {
        "Postgres(dsn=***)".to_string()
    }
}

/// MySQL source connection descriptor (e.g. AWS RDS for MySQL).
#[pyclass]
#[derive(Clone)]
struct MySQL {
    dsn: String,
    statement_timeout_secs: u64,
    ca_cert_file: Option<String>,
    require_tls: bool,
    client_cert_file: Option<String>,
    client_key_file: Option<String>,
}

#[pymethods]
impl MySQL {
    #[new]
    #[pyo3(signature = (dsn=None, *, host=None, port=None, user=None, password=None, database=None, statement_timeout_secs=0, ca_cert_file=None, require_tls=false, client_cert_file=None, client_key_file=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        dsn: Option<String>,
        host: Option<String>,
        port: Option<u16>,
        user: Option<String>,
        password: Option<String>,
        database: Option<String>,
        statement_timeout_secs: u64,
        ca_cert_file: Option<String>,
        require_tls: bool,
        client_cert_file: Option<String>,
        client_key_file: Option<String>,
    ) -> PyResult<Self> {
        let dsn = resolve_dsn("mysql", "MySQL", dsn, host, port, user, password, database)?;
        Ok(MySQL {
            dsn,
            statement_timeout_secs,
            ca_cert_file,
            require_tls,
            client_cert_file,
            client_key_file,
        })
    }

    fn __repr__(&self) -> String {
        "MySQL(dsn=***)".to_string()
    }
}

/// Google BigQuery connection descriptor — usable as either a `source` or a
/// `target` for `sync()`.
///
/// Authenticates via a service-account JSON key file (`credentials_file`) if
/// given, otherwise falls back to Application Default Credentials (ADC) —
/// `GOOGLE_APPLICATION_CREDENTIALS`, `GOOGLE_APPLICATION_CREDENTIALS_JSON`,
/// the GCE/GKE metadata server, or the `gcloud` CLI's well-known ADC file.
///
/// `dataset_id` is only required when this is plugged in as `target=`
/// (BigQuery's equivalent of ClickHouse's `database`) — as a `source=` it's
/// unused, since `source_table`/`source_query` already carry the dataset.
///
/// `write_method` (`"insert_all"` (default) or `"storage_write"`) selects how
/// rows are written when this is a `target=`; ignored as a `source=`.
#[pyclass]
#[derive(Clone)]
struct BigQuery {
    project_id: Option<String>,
    credentials_file: Option<String>,
    credentials_json: Option<String>,
    dataset_id: Option<String>,
    write_method: String,
}

#[pymethods]
impl BigQuery {
    #[new]
    #[pyo3(signature = (project_id=None, *, credentials_file=None, credentials_json=None, dataset_id=None, write_method="storage_write".to_string()))]
    fn new(
        project_id: Option<String>,
        credentials_file: Option<String>,
        credentials_json: Option<String>,
        dataset_id: Option<String>,
        write_method: String,
    ) -> Self {
        BigQuery {
            project_id,
            credentials_file,
            credentials_json,
            dataset_id,
            write_method,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "BigQuery(project_id={:?}, dataset_id={:?}, write_method={:?})",
            self.project_id, self.dataset_id, self.write_method
        )
    }
}

/// Parse a declared-column spec into `ApiColumn`s. Accepts either a list of
/// `(name, type)` / `(name, type, path)` tuples, or a `{name: type}` dict
/// (with optional `paths={name: dotted_path}`).
fn parse_columns(
    columns: &Bound<'_, PyAny>,
    paths: Option<HashMap<String, String>>,
) -> PyResult<Vec<core::ApiColumn>> {
    let paths = paths.unwrap_or_default();
    let mut out = Vec::new();
    if let Ok(dict) = columns.downcast::<pyo3::types::PyDict>() {
        for (k, v) in dict.iter() {
            let name: String = k.extract()?;
            let bq_type: String = v.extract()?;
            let path = paths.get(&name).cloned();
            out.push(core::ApiColumn {
                name,
                bq_type,
                path,
            });
        }
        if out.is_empty() {
            return Err(PyRuntimeError::new_err(
                "columns is empty; declare at least one column",
            ));
        }
        return Ok(out);
    }
    for item in columns.iter()? {
        let item = item?;
        if let Ok((name, bq_type, path)) = item.extract::<(String, String, String)>() {
            out.push(core::ApiColumn {
                name,
                bq_type,
                path: Some(path),
            });
        } else if let Ok((name, bq_type)) = item.extract::<(String, String)>() {
            let path = paths.get(&name).cloned();
            out.push(core::ApiColumn {
                name,
                bq_type,
                path,
            });
        } else {
            return Err(PyRuntimeError::new_err(
                "each column must be (name, type) or (name, type, path), or use a {name: type} dict",
            ));
        }
    }
    if out.is_empty() {
        return Err(PyRuntimeError::new_err(
            "columns is empty; declare at least one column",
        ));
    }
    Ok(out)
}

/// CleverTap Data Export API source (events). Writes to BigQuery or ClickHouse.
///
/// `columns` declares the output schema (list of `(name, bq_type)` /
/// `(name, bq_type, dotted_path)` tuples, or a `{name: bq_type}` dict); `path`
/// (or `paths={name: "a.b"}`) extracts a value from the nested event JSON.
/// Auth is `account_id` + `passcode`; `region` picks the API host (default
/// `sg1` -> `https://sg1.api.clevertap.com`). The `[from_date, to_date]` window
/// (`"YYYY-MM-DD"`) is the full-refresh window / incremental first-run floor.
#[pyclass]
#[derive(Clone)]
struct CleverTap {
    base_url: String,
    account_id: String,
    passcode: String,
    event_name: String,
    batch_size: u32,
    columns: Vec<core::ApiColumn>,
    from_date: Option<String>,
    to_date: Option<String>,
    lookback_days: u32,
}

#[pymethods]
impl CleverTap {
    #[new]
    #[pyo3(signature = (account_id, passcode, event_name, columns, *, region="sg1".to_string(), batch_size=5000, from_date=None, to_date=None, lookback_days=0, paths=None, base_url=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        account_id: String,
        passcode: String,
        event_name: String,
        columns: &Bound<'_, PyAny>,
        region: String,
        batch_size: u32,
        from_date: Option<String>,
        to_date: Option<String>,
        lookback_days: u32,
        paths: Option<HashMap<String, String>>,
        base_url: Option<String>,
    ) -> PyResult<Self> {
        let columns = parse_columns(columns, paths)?;
        let base_url = base_url.unwrap_or_else(|| format!("https://{region}.api.clevertap.com"));
        Ok(CleverTap {
            base_url,
            account_id,
            passcode,
            event_name,
            batch_size,
            columns,
            from_date,
            to_date,
            lookback_days,
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "CleverTap(event_name={:?}, columns={}, account_id=***, passcode=***)",
            self.event_name,
            self.columns.len()
        )
    }
}

/// AppsFlyer raw-data Pull API source (CSV report). Writes to BigQuery or ClickHouse.
///
/// `columns` declares the output schema; each column reads the CSV header equal
/// to its `path` (or its `name`). Auth is the V2.0 `api_token`. The Pull API has
/// hard daily-call/row caps — for high volume use AppsFlyer Data Locker.
#[pyclass]
#[derive(Clone)]
struct AppsFlyer {
    base_url: String,
    api_token: String,
    app_id: String,
    report_type: String,
    extra_params: HashMap<String, String>,
    columns: Vec<core::ApiColumn>,
    from_date: Option<String>,
    to_date: Option<String>,
    lookback_days: u32,
}

#[pymethods]
impl AppsFlyer {
    #[new]
    #[pyo3(signature = (api_token, app_id, report_type, columns, *, from_date=None, to_date=None, lookback_days=0, paths=None, extra_params=None, base_url="https://hq1.appsflyer.com".to_string()))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        api_token: String,
        app_id: String,
        report_type: String,
        columns: &Bound<'_, PyAny>,
        from_date: Option<String>,
        to_date: Option<String>,
        lookback_days: u32,
        paths: Option<HashMap<String, String>>,
        extra_params: Option<HashMap<String, String>>,
        base_url: String,
    ) -> PyResult<Self> {
        let columns = parse_columns(columns, paths)?;
        Ok(AppsFlyer {
            base_url,
            api_token,
            app_id,
            report_type,
            extra_params: extra_params.unwrap_or_default(),
            columns,
            from_date,
            to_date,
            lookback_days,
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "AppsFlyer(app_id={:?}, report_type={:?}, columns={}, api_token=***)",
            self.app_id,
            self.report_type,
            self.columns.len()
        )
    }
}

/// Generic HTTP/REST or CSV API source (BigQuery or ClickHouse destination).
///
/// Issues a `GET`/`POST` to `url` with the given `headers` (put auth here), then
/// parses the response as JSON (`format="json"`, with `records_path` locating
/// the records array) or CSV (`format="csv"`). `{from}`/`{to}` in `url`/`body`
/// are replaced with the window dates. For cursor pagination, set
/// `next_cursor_path` (where the next cursor is in the JSON response) and
/// `cursor_param` (the query param to send it back as). Declare the output
/// schema via `columns` (same forms as `CleverTap`).
#[pyclass]
#[derive(Clone)]
struct HttpApi {
    url: String,
    method: String,
    headers: HashMap<String, String>,
    body: Option<String>,
    format: core::HttpFormat,
    next_cursor_path: Option<String>,
    cursor_param: Option<String>,
    state_id: Option<String>,
    columns: Vec<core::ApiColumn>,
    from_date: Option<String>,
    to_date: Option<String>,
    lookback_days: u32,
}

#[pymethods]
impl HttpApi {
    #[new]
    #[pyo3(signature = (url, columns, *, method="GET".to_string(), headers=None, body=None, format="json".to_string(), records_path=None, next_cursor_path=None, cursor_param=None, state_id=None, from_date=None, to_date=None, lookback_days=0, paths=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        url: String,
        columns: &Bound<'_, PyAny>,
        method: String,
        headers: Option<HashMap<String, String>>,
        body: Option<String>,
        format: String,
        records_path: Option<String>,
        next_cursor_path: Option<String>,
        cursor_param: Option<String>,
        state_id: Option<String>,
        from_date: Option<String>,
        to_date: Option<String>,
        lookback_days: u32,
        paths: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let columns = parse_columns(columns, paths)?;
        let format = match format.to_ascii_lowercase().as_str() {
            "json" => core::HttpFormat::Json { records_path },
            "csv" => core::HttpFormat::Csv,
            other => {
                return Err(PyRuntimeError::new_err(format!(
                    "invalid format {other:?}; expected 'json' or 'csv'"
                )))
            }
        };
        match (&next_cursor_path, &cursor_param) {
            (Some(_), Some(_)) | (None, None) => {}
            _ => {
                return Err(PyRuntimeError::new_err(
                    "next_cursor_path and cursor_param must be set together for cursor pagination",
                ))
            }
        }
        Ok(HttpApi {
            url,
            method,
            headers: headers.unwrap_or_default(),
            body,
            format,
            next_cursor_path,
            cursor_param,
            state_id,
            columns,
            from_date,
            to_date,
            lookback_days,
        })
    }

    fn __repr__(&self) -> String {
        // headers may carry auth — never echo them.
        format!(
            "HttpApi(url={:?}, method={:?}, columns={}, headers=***)",
            self.url,
            self.method,
            self.columns.len()
        )
    }
}

/// The serialized Arrow IPC stream `quickhouse.from_pandas` produces.
///
/// A hand-written extractor rather than `Vec<u8>`: the derived one would accept
/// any sequence of small ints, so a caller passing something odd would be
/// matched here instead of failing against the real descriptors.
///
/// The bytes are copied out while the GIL is still held. pyo3 0.22 offers
/// `PyBackedBytes`, which would avoid the copy, but it keeps a `Py<PyBytes>`
/// alive — and this module's whole boundary discipline (see the module docs) is
/// that no live Python object crosses `allow_threads`. The copy is the price of
/// that, and it is paid once.
struct FrameBytes(Vec<u8>);

impl<'py> FromPyObject<'py> for FrameBytes {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(FrameBytes(
            ob.downcast::<pyo3::types::PyBytes>()
                .map_err(|_| {
                    PyRuntimeError::new_err(
                        "expected a connection descriptor (Postgres, MySQL, BigQuery, ClickHouse, \
                         CleverTap, AppsFlyer, HttpApi) as the sync() source",
                    )
                })?
                .as_bytes()
                .to_vec(),
        ))
    }
}

/// Accepts `Postgres`, `MySQL`, `BigQuery`, `ClickHouse`, `CleverTap`,
/// `AppsFlyer`, or `HttpApi` as `sync()`'s `source` argument.
///
/// `Frame` is private and undocumented: it is how `quickhouse.from_pandas`
/// hands an already-serialized DataFrame to the engine, and it is last so every
/// real descriptor is tried first.
#[derive(FromPyObject)]
enum AnySource {
    Postgres(Postgres),
    MySQL(MySQL),
    BigQuery(BigQuery),
    ClickHouse(ClickHouse),
    CleverTap(CleverTap),
    AppsFlyer(AppsFlyer),
    HttpApi(HttpApi),
    Frame(FrameBytes),
}

impl From<AnySource> for core::SourceConfig {
    fn from(source: AnySource) -> Self {
        match source {
            AnySource::Postgres(p) => core::SourceConfig::Postgres(core::PostgresConfig {
                dsn: p.dsn,
                statement_timeout_secs: p.statement_timeout_secs,
                ca_cert_file: p.ca_cert_file,
                client_cert_file: p.client_cert_file,
                client_key_file: p.client_key_file,
            }),
            AnySource::MySQL(m) => core::SourceConfig::MySql(core::MySqlConfig {
                dsn: m.dsn,
                statement_timeout_secs: m.statement_timeout_secs,
                ca_cert_file: m.ca_cert_file,
                require_tls: m.require_tls,
                client_cert_file: m.client_cert_file,
                client_key_file: m.client_key_file,
            }),
            AnySource::BigQuery(b) => core::SourceConfig::BigQuery(core::BigQueryConfig {
                project_id: b.project_id,
                credentials_file: b.credentials_file,
                credentials_json: b.credentials_json,
                // dataset_id is a target-only field (see BigQuery's doc comment) — ignored here.
            }),
            AnySource::ClickHouse(c) => {
                core::SourceConfig::ClickHouse(core::ClickHouseSourceConfig {
                    url: c.url,
                    database: c.database,
                    user: c.user,
                    password: c.password,
                    statement_timeout_secs: c.statement_timeout_secs,
                    settings: c.settings,
                    // compression/archive/insert_dedup_token are write-path
                    // fields (see ClickHouse's doc comment) — ignored here.
                })
            }
            AnySource::CleverTap(c) => core::SourceConfig::CleverTap(core::CleverTapConfig {
                base_url: c.base_url,
                account_id: c.account_id,
                passcode: c.passcode,
                event_name: c.event_name,
                batch_size: c.batch_size,
                columns: c.columns,
                from_date: c.from_date,
                to_date: c.to_date,
                lookback_days: c.lookback_days,
            }),
            AnySource::AppsFlyer(a) => core::SourceConfig::AppsFlyer(core::AppsFlyerConfig {
                base_url: a.base_url,
                api_token: a.api_token,
                app_id: a.app_id,
                report_type: a.report_type,
                extra_params: a.extra_params,
                columns: a.columns,
                from_date: a.from_date,
                to_date: a.to_date,
                lookback_days: a.lookback_days,
            }),
            AnySource::HttpApi(h) => core::SourceConfig::HttpApi(core::HttpApiConfig {
                url: h.url,
                method: h.method,
                headers: h.headers,
                body: h.body,
                format: h.format,
                next_cursor_path: h.next_cursor_path,
                cursor_param: h.cursor_param,
                state_id: h.state_id,
                columns: h.columns,
                from_date: h.from_date,
                to_date: h.to_date,
                lookback_days: h.lookback_days,
            }),
            AnySource::Frame(b) => core::SourceConfig::Arrow(core::ArrowFrameConfig {
                ipc: b.0.into(),
                label: None,
            }),
        }
    }
}

/// Optional S3 (or S3-compatible, e.g. MinIO) data-lake archive attached to a
/// `ClickHouse` destination via its `archive=` parameter — every batch synced
/// into ClickHouse is also written as Parquet, one file per parallel
/// partition, to `s3://{bucket}/{prefix}/{dest_table}/dt=<date>/run=<id>/
/// part-<partition>.parquet`. A secondary, best-effort-free backup/historical
/// side channel; omitting `archive` entirely disables it and has zero effect
/// on the ClickHouse write path.
///
/// `region`/`access_key_id`/`secret_access_key` default to the standard AWS
/// credential chain (env vars, IAM role) when omitted — set them explicitly
/// to override, or to point at an S3-compatible service via `endpoint`
/// (plain HTTP is allowed automatically whenever `endpoint` is set; real AWS
/// S3 always uses HTTPS).
#[pyclass]
#[derive(Clone)]
struct S3Archive {
    bucket: String,
    prefix: String,
    region: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    endpoint: Option<String>,
    compression: String,
}

#[pymethods]
impl S3Archive {
    #[new]
    #[pyo3(signature = (bucket, *, prefix="".to_string(), region=None, access_key_id=None, secret_access_key=None, endpoint=None, compression="zstd".to_string()))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        bucket: String,
        prefix: String,
        region: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        endpoint: Option<String>,
        compression: String,
    ) -> Self {
        S3Archive {
            bucket,
            prefix,
            region,
            access_key_id,
            secret_access_key,
            endpoint,
            compression,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "S3Archive(bucket={:?}, prefix={:?}, endpoint={:?})",
            self.bucket, self.prefix, self.endpoint
        )
    }
}

/// ClickHouse connection descriptor — usable as either a `source` or a
/// `target` for `sync()`, like `BigQuery`.
///
/// `url`, `database`, `user`, `password` and `settings` apply in both roles.
/// `compression`, `archive` and `insert_dedup_token` are write-path only and
/// are ignored when this is plugged in as a `source=`; `statement_timeout_secs`
/// is read-path only and is ignored as a `target=`.
///
/// As a source, reads go over the same HTTP interface in `FORMAT ArrowStream`,
/// which makes ClickHouse -> ClickHouse (a cross-cluster or cross-database
/// copy) and ClickHouse -> BigQuery (publishing a mart into a warehouse) both
/// ordinary transfers with the full partition / incremental / chunk-resume
/// machinery behind them.
#[pyclass]
#[derive(Clone)]
struct ClickHouse {
    url: String,
    database: String,
    user: String,
    password: String,
    compression: String,
    archive: Option<S3Archive>,
    settings: BTreeMap<String, String>,
    insert_dedup_token: bool,
    statement_timeout_secs: u64,
}

#[pymethods]
impl ClickHouse {
    #[new]
    #[pyo3(signature = (url, *, database="default".to_string(), user="default".to_string(), password="".to_string(), compression="zstd".to_string(), archive=None, settings=None, insert_dedup_token=false, statement_timeout_secs=0))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        url: String,
        database: String,
        user: String,
        password: String,
        compression: String,
        archive: Option<S3Archive>,
        settings: Option<BTreeMap<String, String>>,
        insert_dedup_token: bool,
        statement_timeout_secs: u64,
    ) -> Self {
        ClickHouse {
            url,
            database,
            user,
            password,
            compression,
            archive,
            settings: settings.unwrap_or_default(),
            insert_dedup_token,
            statement_timeout_secs,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "ClickHouse(url={:?}, database={:?})",
            self.url, self.database
        )
    }
}

/// Accepts `ClickHouse` or `BigQuery` as `sync()`'s `target` argument.
///
/// `#[allow(clippy::large_enum_variant)]`: the `ClickHouse` descriptor is a few
/// hundred bytes wider than the `BigQuery` one (mostly its optional
/// `S3Archive`), and the obvious fix — boxing the variant — doesn't apply here,
/// since `FromPyObject` is derived and pyo3 has no `Box<T>` extraction to
/// derive through. One short-lived value per `sync()` call; not worth a
/// hand-written extractor.
#[allow(clippy::large_enum_variant)]
#[derive(FromPyObject)]
enum AnyDestination {
    ClickHouse(ClickHouse),
    BigQuery(BigQuery),
}

impl AnyDestination {
    /// Fallible unlike `AnySource`'s plain `From` — a BigQuery target without
    /// `dataset_id` is a config error we can catch here, before ever
    /// touching the network.
    fn into_config(self) -> PyResult<core::DestinationConfig> {
        match self {
            AnyDestination::ClickHouse(c) => {
                let s3_archive = match c.archive {
                    Some(a) => {
                        if a.bucket.is_empty() {
                            return Err(PyRuntimeError::new_err(
                                "S3Archive(...) requires a non-empty bucket",
                            ));
                        }
                        Some(core::S3ArchiveConfig {
                            bucket: a.bucket,
                            prefix: a.prefix,
                            region: a.region,
                            access_key_id: a.access_key_id,
                            secret_access_key: a.secret_access_key,
                            endpoint: a.endpoint,
                            compression: parse_parquet_compression(&a.compression)?,
                        })
                    }
                    None => None,
                };
                Ok(core::DestinationConfig::ClickHouse(
                    core::ClickHouseConfig {
                        url: c.url,
                        database: c.database,
                        user: c.user,
                        password: c.password,
                        compression: parse_compression(&c.compression)?,
                        insert_dedup_token: c.insert_dedup_token,
                        settings: c.settings,
                        s3_archive,
                    },
                ))
            }
            AnyDestination::BigQuery(b) => {
                let dataset_id = b.dataset_id.ok_or_else(|| {
                    PyRuntimeError::new_err(
                        "BigQuery(...) used as a sync() target requires dataset_id, \
                         e.g. quickhouse.BigQuery(\"my-project\", dataset_id=\"analytics\")",
                    )
                })?;
                Ok(core::DestinationConfig::BigQuery(
                    core::BigQueryDestConfig {
                        project_id: b.project_id,
                        credentials_file: b.credentials_file,
                        credentials_json: b.credentials_json,
                        dataset_id,
                        write_method: parse_bq_write_method(&b.write_method)?,
                    },
                ))
            }
        }
    }
}

/// Live progress passed to an `on_progress` callback.
#[pyclass]
#[derive(Clone)]
struct Progress {
    #[pyo3(get)]
    rows_read: u64,
    #[pyo3(get)]
    rows_written: u64,
    #[pyo3(get)]
    bytes_written: u64,
    #[pyo3(get)]
    elapsed_secs: f64,
    #[pyo3(get)]
    rows_per_sec: f64,
}

#[pymethods]
impl Progress {
    fn __repr__(&self) -> String {
        format!(
            "Progress(rows_written={}, rows_per_sec={:.0}, elapsed_secs={:.1})",
            self.rows_written, self.rows_per_sec, self.elapsed_secs
        )
    }
}

impl From<core::Progress> for Progress {
    fn from(p: core::Progress) -> Self {
        Progress {
            rows_read: p.rows_read,
            rows_written: p.rows_written,
            bytes_written: p.bytes_written,
            elapsed_secs: p.elapsed_secs,
            rows_per_sec: p.rows_per_sec,
        }
    }
}

/// Context passed to a `validate=` staged-validation callback: the per-run
/// staging table (fully loaded, not yet promoted) and where it lives.
#[pyclass]
#[derive(Clone)]
struct StagedInfo {
    #[pyo3(get)]
    staging_table: String,
    #[pyo3(get)]
    database: String,
    #[pyo3(get)]
    dest_kind: String,
    #[pyo3(get)]
    rows_written: u64,
}

#[pymethods]
impl StagedInfo {
    fn __repr__(&self) -> String {
        format!(
            "StagedInfo(staging_table={:?}, database={:?}, dest_kind={:?}, rows_written={})",
            self.staging_table, self.database, self.dest_kind, self.rows_written
        )
    }
}

impl StagedInfo {
    fn from_core(info: &core::StagedInfo) -> Self {
        StagedInfo {
            staging_table: info.staging_table.clone(),
            database: info.database.clone(),
            dest_kind: match info.dest_kind {
                core::config::DestKind::ClickHouse => "clickhouse".to_string(),
                core::config::DestKind::BigQuery => "bigquery".to_string(),
            },
            rows_written: info.rows_written,
        }
    }
}

/// Summary returned by `sync`.
/// One structured warning from a transfer — a condition quickhouse detected,
/// recovered from, and kept going past, but which changed what the destination
/// now holds.
///
/// Named `TransferWarning`, not `Warning`, because `Warning` is a Python
/// builtin exception class and shadowing it in a caller's namespace would be a
/// trap. These are values, not exceptions: nothing is raised, and it is the
/// caller who decides whether any of them should fail their run.
#[pyclass]
struct TransferWarning {
    /// Stable machine-readable kind, e.g. `"collapsed_bool"`. Match on this;
    /// `message` is human-facing and its wording is not stable.
    #[pyo3(get)]
    kind: String,
    /// Source column responsible, or `None` for a table-level condition.
    #[pyo3(get)]
    column: Option<String>,
    /// How many values/rows tripped the condition.
    #[pyo3(get)]
    count: u64,
    /// A representative offending value where one was free to capture.
    #[pyo3(get)]
    sample: Option<String>,
    #[pyo3(get)]
    message: String,
}

#[pymethods]
impl TransferWarning {
    fn __repr__(&self) -> String {
        format!(
            "TransferWarning(kind={:?}, column={:?}, count={}, sample={:?})",
            self.kind, self.column, self.count, self.sample
        )
    }

    fn __str__(&self) -> String {
        self.message.clone()
    }
}

impl TransferWarning {
    fn from_core(w: &core::TransferWarning) -> Self {
        Self {
            kind: w.kind.as_str().to_string(),
            column: w.column.clone(),
            count: w.count,
            sample: w.sample.clone(),
            message: w.message.clone(),
        }
    }
}

#[pyclass]
struct TransferResult {
    #[pyo3(get)]
    rows_read: u64,
    #[pyo3(get)]
    rows_written: u64,
    #[pyo3(get)]
    bytes_written: u64,
    #[pyo3(get)]
    rows_deleted: u64,
    #[pyo3(get)]
    duration_secs: f64,
    #[pyo3(get)]
    read_secs: f64,
    #[pyo3(get)]
    stage_secs: f64,
    #[pyo3(get)]
    promote_secs: f64,
    #[pyo3(get)]
    new_watermark: Option<String>,
    #[pyo3(get)]
    warnings: Vec<Py<TransferWarning>>,
}

#[pymethods]
impl TransferResult {
    fn __repr__(&self) -> String {
        format!(
            "TransferResult(rows_read={}, rows_written={}, bytes_written={}, rows_deleted={}, \
             duration_secs={:.3}, read_secs={:.3}, stage_secs={:.3}, promote_secs={:.3}, \
             new_watermark={:?}, warnings={})",
            self.rows_read,
            self.rows_written,
            self.bytes_written,
            self.rows_deleted,
            self.duration_secs,
            self.read_secs,
            self.stage_secs,
            self.promote_secs,
            self.new_watermark,
            self.warnings.len(),
        )
    }
}

/// What a `reconcile_keys()` diff found, and what it did about it.
#[pyclass]
struct ReconcileResult {
    #[pyo3(get)]
    source_keys: u64,
    #[pyo3(get)]
    dest_keys: u64,
    #[pyo3(get)]
    orphan_keys: u64,
    #[pyo3(get)]
    missing_keys: u64,
    #[pyo3(get)]
    rows_deleted: u64,
    #[pyo3(get)]
    orphan_sample: Vec<String>,
    #[pyo3(get)]
    missing_sample: Vec<String>,
    #[pyo3(get)]
    duration_secs: f64,
}

#[pymethods]
impl ReconcileResult {
    fn __repr__(&self) -> String {
        format!(
            "ReconcileResult(source_keys={}, dest_keys={}, orphan_keys={}, missing_keys={}, \
             rows_deleted={}, duration_secs={:.3})",
            self.source_keys,
            self.dest_keys,
            self.orphan_keys,
            self.missing_keys,
            self.rows_deleted,
            self.duration_secs,
        )
    }
}

/// `mode` has no safe default, so there isn't one.
///
/// It used to default to `"full"`, which means "replace the destination
/// wholesale" — the single most destructive of the three modes, handed to
/// anyone who did not think about the argument. Against a partitioned table a
/// full refresh scoped to one partition destroys the rest. Requiring the caller
/// to name the mode costs one keyword and removes a footgun that fires silently.
fn require_mode(mode: Option<String>) -> PyResult<String> {
    mode.ok_or_else(|| {
        PyRuntimeError::new_err(
            "sync() requires mode=: \"full\" REPLACES the destination table (and is not              partition-aware, so a partial run destroys the rest), \"incremental\" upserts on key=              or watermark=, \"append\" inserts without deduplicating. This used to default to              \"full\"; it no longer does, because that default silently replaced tables nobody              meant to replace.",
        )
    })
}

fn parse_mode(mode: &str) -> PyResult<core::SyncMode> {
    match mode.to_ascii_lowercase().as_str() {
        "full" => Ok(core::SyncMode::Full),
        "incremental" | "inc" => Ok(core::SyncMode::Incremental),
        "append" => Ok(core::SyncMode::Append),
        other => Err(PyRuntimeError::new_err(format!(
            "invalid mode {other:?}; expected 'full', 'incremental', or 'append'"
        ))),
    }
}

fn parse_compression(c: &str) -> PyResult<core::Compression> {
    match c.to_ascii_lowercase().as_str() {
        "none" | "off" | "" => Ok(core::Compression::None),
        "gzip" | "gz" => Ok(core::Compression::Gzip),
        "zstd" | "zst" => Ok(core::Compression::Zstd),
        other => Err(PyRuntimeError::new_err(format!(
            "invalid compression {other:?}; expected 'none', 'gzip', or 'zstd'"
        ))),
    }
}

fn parse_bq_write_method(m: &str) -> PyResult<core::BigQueryWriteMethod> {
    match m.to_ascii_lowercase().as_str() {
        "insert_all" | "insertall" | "insert" => Ok(core::BigQueryWriteMethod::InsertAll),
        "storage_write" | "storagewrite" | "storage" => Ok(core::BigQueryWriteMethod::StorageWrite),
        other => Err(PyRuntimeError::new_err(format!(
            "invalid write_method {other:?}; expected 'insert_all' or 'storage_write'"
        ))),
    }
}

fn parse_parquet_compression(c: &str) -> PyResult<core::ParquetCompression> {
    match c.to_ascii_lowercase().as_str() {
        "zstd" | "zst" => Ok(core::ParquetCompression::Zstd),
        "snappy" | "snap" => Ok(core::ParquetCompression::Snappy),
        "none" | "uncompressed" | "off" => Ok(core::ParquetCompression::Uncompressed),
        other => Err(PyRuntimeError::new_err(format!(
            "invalid archive compression {other:?}; expected 'zstd', 'snappy', or 'uncompressed'"
        ))),
    }
}

/// Transfer one table from PostgreSQL, MySQL, BigQuery or ClickHouse into
/// ClickHouse or BigQuery.
#[pyfunction]
#[pyo3(signature = (
    source,
    target,
    dest_table,
    *,
    source_table=None,
    source_query=None,
    state_key=None,
    mode=None,
    watermark=None,
    watermark_source_expr=None,
    lookback_seconds=0,
    seed_watermark=None,
    skip_to_max=false,
    advance_watermark=true,
    key=None,
    create_if_missing=true,
    engine=None,
    order_by=None,
    partition_by=None,
    primary_key=None,
    merge_prune_partition_by=None,
    merge_prune_key_range=true,
    merge_prune_key_list_max=0,
    delete_stale_in_window=false,
    allow_full_refresh_shrink=false,
    parallelism=0,
    batch_rows=100_000,
    batch_bytes=4_194_304,
    insert_bytes=33_554_432,
    max_memory_bytes=536_870_912,
    max_memory_fraction=0.0,
    partition_column=None,
    partition_source_expr=None,
    read_max_rows_per_sec=None,
    read_idle_timeout_secs=0,
    chunk_rows=None,
    retry_max_attempts=1,
    column_transforms=None,
    column_transform_types=None,
    evolve_schema=false,
    state_table_name="_quickhouse_state".to_string(),
    staging_suffix="_quickhouse_tmp".to_string(),
    application_name="quickhouse".to_string(),
    type_overrides=None,
    rename=None,
    include=None,
    exclude=None,
    not_null=None,
    tinyint1_as_bool=true,
    numeric_as_decimal=None,
    on_progress=None,
    validate=None,
))]
#[allow(clippy::too_many_arguments)]
fn sync(
    py: Python<'_>,
    source: AnySource,
    target: AnyDestination,
    dest_table: String,
    source_table: Option<String>,
    source_query: Option<String>,
    state_key: Option<String>,
    mode: Option<String>,
    watermark: Option<String>,
    watermark_source_expr: Option<String>,
    lookback_seconds: u64,
    seed_watermark: Option<String>,
    skip_to_max: bool,
    advance_watermark: bool,
    key: Option<Vec<String>>,
    create_if_missing: bool,
    engine: Option<String>,
    order_by: Option<Vec<String>>,
    partition_by: Option<String>,
    primary_key: Option<Vec<String>>,
    merge_prune_partition_by: Option<String>,
    merge_prune_key_range: bool,
    merge_prune_key_list_max: usize,
    delete_stale_in_window: bool,
    allow_full_refresh_shrink: bool,
    parallelism: usize,
    batch_rows: usize,
    batch_bytes: usize,
    insert_bytes: usize,
    max_memory_bytes: usize,
    max_memory_fraction: f64,
    partition_column: Option<String>,
    partition_source_expr: Option<String>,
    read_max_rows_per_sec: Option<u64>,
    read_idle_timeout_secs: u64,
    chunk_rows: Option<usize>,
    retry_max_attempts: u32,
    column_transforms: Option<HashMap<String, String>>,
    column_transform_types: Option<HashMap<String, String>>,
    evolve_schema: bool,
    state_table_name: String,
    staging_suffix: String,
    application_name: String,
    type_overrides: Option<HashMap<String, String>>,
    rename: Option<HashMap<String, String>>,
    include: Option<Vec<String>>,
    exclude: Option<Vec<String>>,
    not_null: Option<Vec<String>>,
    tinyint1_as_bool: bool,
    numeric_as_decimal: Option<String>,
    on_progress: Option<PyObject>,
    validate: Option<PyObject>,
) -> PyResult<TransferResult> {
    init_logging();
    let source_cfg: core::SourceConfig = source.into();
    let dest_cfg = target.into_config()?;
    // `seed_watermark` (an explicit floor) and `skip_to_max` (seed to the
    // source's current MAX) are mutually exclusive ways to seed the first run.
    let seed_watermark = match (seed_watermark, skip_to_max) {
        (Some(_), true) => {
            return Err(PyRuntimeError::new_err(
                "seed_watermark and skip_to_max are mutually exclusive",
            ))
        }
        (Some(v), false) => core::WatermarkSeed::Value(v),
        (None, true) => core::WatermarkSeed::CurrentMax,
        (None, false) => core::WatermarkSeed::None,
    };
    let cfg = core::TransferConfig {
        source_table,
        source_query,
        dest_table,
        state_key,
        mode: parse_mode(&require_mode(mode)?)?,
        watermark,
        watermark_source_expr,
        lookback_seconds,
        seed_watermark,
        advance_watermark,
        key: key.unwrap_or_default(),
        create_if_missing,
        engine,
        order_by: order_by.unwrap_or_default(),
        partition_by,
        primary_key: primary_key.unwrap_or_default(),
        merge_prune_partition_by,
        merge_prune_key_range,
        merge_prune_key_list_max,
        delete_stale_in_window,
        allow_full_refresh_shrink,
        parallelism,
        batch_rows,
        batch_bytes,
        insert_bytes,
        max_memory_bytes,
        max_memory_fraction,
        partition_column,
        partition_source_expr,
        read_max_rows_per_sec,
        read_idle_timeout_secs,
        chunk_rows,
        retry_max_attempts,
        column_transforms: column_transforms.unwrap_or_default(),
        column_transform_types: column_transform_types.unwrap_or_default(),
        evolve_schema,
        state_table_name,
        staging_suffix,
        application_name,
        type_overrides: type_overrides.unwrap_or_default(),
        rename: rename.unwrap_or_default(),
        include: include.unwrap_or_default(),
        exclude: exclude.unwrap_or_default(),
        not_null: not_null.unwrap_or_default(),
        tinyint1_as_bool,
        numeric_as_decimal,
    };

    // Build the progress callback (fires from Tokio worker threads).
    let progress: Option<core::ProgressCb> = on_progress.map(|cb| {
        let cb = Arc::new(cb);
        Arc::new(move |p: core::Progress| {
            Python::with_gil(|py| {
                let arg = Progress::from(p);
                // A raising callback must not abort or corrupt the transfer:
                // print and clear it rather than leaving the error indicator set.
                if let Err(e) = cb.call1(py, (arg,)) {
                    e.print_and_set_sys_last_vars(py);
                }
            });
        }) as core::ProgressCb
    });

    // Build the staged-validation gate (fires once, before the swap/merge).
    // Unlike `on_progress`, a raising callback here is NOT swallowed: it is
    // turned into a transfer error so the promotion is aborted and staging is
    // dropped. The Python exception is flattened to its string at the core
    // boundary (core knows only `EtlError`).
    let on_staged: Option<core::StagedValidationCb> = validate.map(|cb| {
        let cb = Arc::new(cb);
        Arc::new(move |info: &core::StagedInfo| -> core::Result<()> {
            Python::with_gil(|py| {
                let arg = StagedInfo::from_core(info);
                match cb.call1(py, (arg,)) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(core::EtlError::other(format!(
                        "data-quality validation rejected the staged data: {e}"
                    ))),
                }
            })
        }) as core::StagedValidationCb
    });

    // Run with the GIL released so Python threads keep moving and the callbacks
    // can re-acquire it without deadlocking.
    let result = py
        .allow_threads(|| {
            core::run_transfer_blocking(source_cfg, dest_cfg, cfg, progress, on_staged)
        })
        .map_err(map_err)?;

    // Warnings are re-boxed as Python objects here rather than lazily, so a
    // caller can hold onto `result.warnings` after the GIL is handed back.
    let warnings = result
        .warnings
        .iter()
        .map(|w| Py::new(py, TransferWarning::from_core(w)))
        .collect::<PyResult<Vec<_>>>()?;
    Ok(TransferResult {
        rows_read: result.rows_read,
        rows_written: result.rows_written,
        bytes_written: result.bytes_written,
        rows_deleted: result.rows_deleted,
        duration_secs: result.duration_secs,
        read_secs: result.read_secs,
        stage_secs: result.stage_secs,
        promote_secs: result.promote_secs,
        new_watermark: result.new_watermark,
        warnings,
    })
}

/// Diff a source table's keyset against a destination's, and optionally delete
/// the rows the source no longer has.
///
/// An incremental sync never removes anything: a row hard-deleted at the source
/// has no watermark to move, so the destination keeps it forever. This measures
/// that drift for a bounded window, and repairs it only when `delete=True`.
#[pyfunction]
#[pyo3(signature = (
    source,
    target,
    dest_table,
    *,
    key,
    source_table=None,
    source_query=None,
    window=None,
    dest_window=None,
    delete=false,
    max_delete_keys=0,
    sample_limit=10,
))]
#[allow(clippy::too_many_arguments)]
fn reconcile_keys(
    py: Python<'_>,
    source: AnySource,
    target: AnyDestination,
    dest_table: String,
    key: String,
    source_table: Option<String>,
    source_query: Option<String>,
    window: Option<String>,
    dest_window: Option<String>,
    delete: bool,
    max_delete_keys: u64,
    sample_limit: usize,
) -> PyResult<ReconcileResult> {
    init_logging();
    let source_cfg: core::SourceConfig = source.into();
    let dest_cfg = target.into_config()?;
    let cfg = core::ReconcileConfig {
        source_table,
        source_query,
        dest_table,
        key,
        window,
        dest_window,
        delete,
        max_delete_keys,
        sample_limit,
    };
    let result = py
        .allow_threads(|| core::reconcile_keys_blocking(source_cfg, dest_cfg, cfg))
        .map_err(map_err)?;
    Ok(ReconcileResult {
        source_keys: result.source_keys,
        dest_keys: result.dest_keys,
        orphan_keys: result.orphan_keys,
        missing_keys: result.missing_keys,
        rows_deleted: result.rows_deleted,
        orphan_sample: result.orphan_sample,
        missing_sample: result.missing_sample,
        duration_secs: result.duration_secs,
    })
}

/// Return the package version compiled into the extension.
#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pymodule]
fn _quickhouse(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Postgres>()?;
    m.add_class::<MySQL>()?;
    m.add_class::<BigQuery>()?;
    m.add_class::<CleverTap>()?;
    m.add_class::<AppsFlyer>()?;
    m.add_class::<HttpApi>()?;
    m.add_class::<ClickHouse>()?;
    m.add_class::<S3Archive>()?;
    m.add_class::<Progress>()?;
    m.add_class::<StagedInfo>()?;
    m.add_class::<TransferResult>()?;
    m.add_class::<TransferWarning>()?;
    m.add_class::<ReconcileResult>()?;
    m.add_function(wrap_pyfunction!(sync, m)?)?;
    m.add_function(wrap_pyfunction!(reconcile_keys, m)?)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
