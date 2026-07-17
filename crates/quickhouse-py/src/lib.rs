//! PyO3 bindings for quickhouse-core.
//!
//! Exposes `Postgres`, `ClickHouse`, `sync(...)`, and the result/progress types.
//! The transfer runs on a Tokio runtime inside `Python::allow_threads`, so the
//! GIL is released for the duration and only re-acquired to fire `on_progress`.

use std::collections::HashMap;
use std::sync::{Arc, Once};

use quickhouse_core as core;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

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
        let filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("quickhouse_core=info"));
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .with_writer(std::io::stderr)
            .try_init();
    });
}

/// PostgreSQL source connection descriptor.
#[pyclass]
#[derive(Clone)]
struct Postgres {
    dsn: String,
    statement_timeout_secs: u64,
    ca_cert_file: Option<String>,
}

#[pymethods]
impl Postgres {
    #[new]
    #[pyo3(signature = (dsn, *, statement_timeout_secs=0, ca_cert_file=None))]
    fn new(dsn: String, statement_timeout_secs: u64, ca_cert_file: Option<String>) -> Self {
        Postgres {
            dsn,
            statement_timeout_secs,
            ca_cert_file,
        }
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
}

#[pymethods]
impl MySQL {
    #[new]
    #[pyo3(signature = (dsn, *, statement_timeout_secs=0, ca_cert_file=None, require_tls=false))]
    fn new(
        dsn: String,
        statement_timeout_secs: u64,
        ca_cert_file: Option<String>,
        require_tls: bool,
    ) -> Self {
        MySQL {
            dsn,
            statement_timeout_secs,
            ca_cert_file,
            require_tls,
        }
    }

    fn __repr__(&self) -> String {
        "MySQL(dsn=***)".to_string()
    }
}

/// Google BigQuery source connection descriptor.
///
/// Authenticates via a service-account JSON key file (`credentials_file`) if
/// given, otherwise falls back to Application Default Credentials (ADC) —
/// `GOOGLE_APPLICATION_CREDENTIALS`, `GOOGLE_APPLICATION_CREDENTIALS_JSON`,
/// the GCE/GKE metadata server, or the `gcloud` CLI's well-known ADC file.
#[pyclass]
#[derive(Clone)]
struct BigQuery {
    project_id: Option<String>,
    credentials_file: Option<String>,
}

#[pymethods]
impl BigQuery {
    #[new]
    #[pyo3(signature = (project_id=None, *, credentials_file=None))]
    fn new(project_id: Option<String>, credentials_file: Option<String>) -> Self {
        BigQuery {
            project_id,
            credentials_file,
        }
    }

    fn __repr__(&self) -> String {
        format!("BigQuery(project_id={:?})", self.project_id)
    }
}

/// Accepts `Postgres`, `MySQL`, or `BigQuery` as `sync()`'s `source` argument.
#[derive(FromPyObject)]
enum AnySource {
    Postgres(Postgres),
    MySQL(MySQL),
    BigQuery(BigQuery),
}

impl From<AnySource> for core::SourceConfig {
    fn from(source: AnySource) -> Self {
        match source {
            AnySource::Postgres(p) => core::SourceConfig::Postgres(core::PostgresConfig {
                dsn: p.dsn,
                statement_timeout_secs: p.statement_timeout_secs,
                ca_cert_file: p.ca_cert_file,
            }),
            AnySource::MySQL(m) => core::SourceConfig::MySql(core::MySqlConfig {
                dsn: m.dsn,
                statement_timeout_secs: m.statement_timeout_secs,
                ca_cert_file: m.ca_cert_file,
                require_tls: m.require_tls,
            }),
            AnySource::BigQuery(b) => core::SourceConfig::BigQuery(core::BigQueryConfig {
                project_id: b.project_id,
                credentials_file: b.credentials_file,
            }),
        }
    }
}

/// ClickHouse destination connection descriptor.
#[pyclass]
#[derive(Clone)]
struct ClickHouse {
    url: String,
    database: String,
    user: String,
    password: String,
    compression: String,
}

#[pymethods]
impl ClickHouse {
    #[new]
    #[pyo3(signature = (url, *, database="default".to_string(), user="default".to_string(), password="".to_string(), compression="gzip".to_string()))]
    fn new(
        url: String,
        database: String,
        user: String,
        password: String,
        compression: String,
    ) -> Self {
        ClickHouse {
            url,
            database,
            user,
            password,
            compression,
        }
    }

    fn __repr__(&self) -> String {
        format!("ClickHouse(url={:?}, database={:?})", self.url, self.database)
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

/// Summary returned by `sync`.
#[pyclass]
struct TransferResult {
    #[pyo3(get)]
    rows_read: u64,
    #[pyo3(get)]
    rows_written: u64,
    #[pyo3(get)]
    bytes_written: u64,
    #[pyo3(get)]
    duration_secs: f64,
    #[pyo3(get)]
    new_watermark: Option<String>,
}

#[pymethods]
impl TransferResult {
    fn __repr__(&self) -> String {
        format!(
            "TransferResult(rows_read={}, rows_written={}, bytes_written={}, duration_secs={:.3}, new_watermark={:?})",
            self.rows_read,
            self.rows_written,
            self.bytes_written,
            self.duration_secs,
            self.new_watermark
        )
    }
}

fn parse_mode(mode: &str) -> PyResult<core::SyncMode> {
    match mode.to_ascii_lowercase().as_str() {
        "full" => Ok(core::SyncMode::Full),
        "incremental" | "inc" => Ok(core::SyncMode::Incremental),
        other => Err(PyRuntimeError::new_err(format!(
            "invalid mode {other:?}; expected 'full' or 'incremental'"
        ))),
    }
}

fn parse_compression(c: &str) -> PyResult<core::Compression> {
    match c.to_ascii_lowercase().as_str() {
        "none" | "off" | "" => Ok(core::Compression::None),
        "gzip" | "gz" => Ok(core::Compression::Gzip),
        other => Err(PyRuntimeError::new_err(format!(
            "invalid compression {other:?}; expected 'none' or 'gzip'"
        ))),
    }
}

/// Transfer one table from PostgreSQL to ClickHouse.
#[pyfunction]
#[pyo3(signature = (
    source,
    target,
    dest_table,
    *,
    source_table=None,
    source_query=None,
    mode="full".to_string(),
    watermark=None,
    key=None,
    create_if_missing=true,
    engine=None,
    order_by=None,
    partition_by=None,
    primary_key=None,
    parallelism=4,
    batch_rows=100_000,
    batch_bytes=4_194_304,
    partition_column=None,
    type_overrides=None,
    rename=None,
    include=None,
    exclude=None,
    on_progress=None,
))]
#[allow(clippy::too_many_arguments)]
fn sync(
    py: Python<'_>,
    source: AnySource,
    target: ClickHouse,
    dest_table: String,
    source_table: Option<String>,
    source_query: Option<String>,
    mode: String,
    watermark: Option<String>,
    key: Option<Vec<String>>,
    create_if_missing: bool,
    engine: Option<String>,
    order_by: Option<Vec<String>>,
    partition_by: Option<String>,
    primary_key: Option<Vec<String>>,
    parallelism: usize,
    batch_rows: usize,
    batch_bytes: usize,
    partition_column: Option<String>,
    type_overrides: Option<HashMap<String, String>>,
    rename: Option<HashMap<String, String>>,
    include: Option<Vec<String>>,
    exclude: Option<Vec<String>>,
    on_progress: Option<PyObject>,
) -> PyResult<TransferResult> {
    init_logging();
    let source_cfg: core::SourceConfig = source.into();
    let ch = core::ClickHouseConfig {
        url: target.url,
        database: target.database,
        user: target.user,
        password: target.password,
        compression: parse_compression(&target.compression)?,
    };
    let cfg = core::TransferConfig {
        source_table,
        source_query,
        dest_table,
        mode: parse_mode(&mode)?,
        watermark,
        key: key.unwrap_or_default(),
        create_if_missing,
        engine,
        order_by: order_by.unwrap_or_default(),
        partition_by,
        primary_key: primary_key.unwrap_or_default(),
        parallelism,
        batch_rows,
        batch_bytes,
        partition_column,
        type_overrides: type_overrides.unwrap_or_default(),
        rename: rename.unwrap_or_default(),
        include: include.unwrap_or_default(),
        exclude: exclude.unwrap_or_default(),
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

    // Run with the GIL released so Python threads keep moving and the callback
    // can re-acquire it without deadlocking.
    let result = py
        .allow_threads(|| core::run_transfer_blocking(source_cfg, ch, cfg, progress))
        .map_err(map_err)?;

    Ok(TransferResult {
        rows_read: result.rows_read,
        rows_written: result.rows_written,
        bytes_written: result.bytes_written,
        duration_secs: result.duration_secs,
        new_watermark: result.new_watermark,
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
    m.add_class::<ClickHouse>()?;
    m.add_class::<Progress>()?;
    m.add_class::<TransferResult>()?;
    m.add_function(wrap_pyfunction!(sync, m)?)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
