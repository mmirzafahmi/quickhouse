//! The CSV destination (new in 0.21.0): each table a folder of CSV part files
//! on a local path.
//!
//! # Layout
//!
//! ```text
//! <path>/
//!   _quickhouse_state.json          append cursors (`state_table_name` + .json)
//!   <dest_table>/
//!     _quickhouse_manifest.json     columns, format, rows per part
//!     part-<run_id>-00000.csv[.gz]
//!     part-<run_id>-00001.csv[.gz]
//! ```
//!
//! `run_id` is the run's start in nanoseconds since 1970, so the parts sort in
//! the order the runs wrote them. Each part has its own header row (unless
//! `header=False`), so each reads alone. The text format is
//! [`super::csv_writer`]'s.
//!
//! # Writes
//!
//! Every write goes into a staging folder beside the table's,
//! `<path>/<dest_table><staging_suffix>_<run_id>/`, and only a run that read
//! every row moves it in. A reader therefore never sees a part being written,
//! and a run that fails leaves nothing behind: its staging folder is removed
//! (the transfer's cleanup drops it as a staging table).
//!
//! - **Full refresh:** the staging folder, with its own manifest, is swapped
//!   into place. On Linux that is one `renameat2(RENAME_EXCHANGE)`, so the
//!   table's path never stops existing; elsewhere (or on a filesystem without
//!   it) two renames, where the path is briefly missing but never holds a mix.
//!   The old folder ends up at the staging path, which is then removed.
//! - **Append:** the staged parts are renamed into the table's folder, and its
//!   manifest is rewritten with them added (written to a temporary file and
//!   renamed over the old one). Their columns and format must match the
//!   folder's: [`CsvSink::check_write`] refuses a mismatch before anything is
//!   read.
//!
//! Each stream of the read (a parallel partition, or the one stream of an
//! unpartitioned read) writes parts of its own, so a parallel read writes at
//! least one part per partition that returns rows; a stream with two inserts
//! in flight at once opens a second. A part closes and the next opens every
//! `max_rows_per_file` rows.
//!
//! # State
//!
//! An append with a `watermark` keeps its cursor in `<path>/<state_table>.json`,
//! one entry per `(state_key, dest_table)`, written to a temporary file and
//! renamed into place. Writes to it, and to a manifest, are serialized per
//! `<path>`: within a process by a mutex, and across processes by an exclusive
//! `flock` on the `<path>` folder (Unix only).
//!
//! # Safety
//!
//! quickhouse replaces or adds to a folder only when it holds a manifest (or
//! nothing at all), so pointing `path` at a folder that holds other files
//! under a table's name is refused rather than deleted.

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::csv_writer::{io_error, PartFormat, PartInfo, PartWriter};
use super::{Sink, StateKey};
use crate::config::{CsvDestConfig, DestKind, SyncMode, TransferConfig};
use crate::error::{EtlError, Result};
use crate::types::ColumnType;

/// The file in each table folder that describes it.
pub const MANIFEST_FILE: &str = "_quickhouse_manifest.json";
/// What the manifest's `format` field says.
const MANIFEST_FORMAT: &str = "quickhouse-csv";
const MANIFEST_VERSION: u32 = 1;
const STATE_VERSION: u32 = 1;

/// A table folder's manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Manifest {
    pub format: String,
    pub version: u32,
    /// The columns of every part, in order.
    pub columns: Vec<ManifestColumn>,
    pub delimiter: String,
    pub header: bool,
    /// `null` or `"gzip"`.
    pub compression: Option<String>,
    /// Data rows across every part.
    pub rows: u64,
    pub parts: Vec<PartInfo>,
}

/// One column, with the Arrow type its text was written from (informational:
/// nothing reads it back).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ManifestColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub arrow_type: String,
}

impl Manifest {
    fn new(cfg: &CsvDestConfig, columns: Vec<ManifestColumn>, parts: Vec<PartInfo>) -> Self {
        Manifest {
            format: MANIFEST_FORMAT.to_string(),
            version: MANIFEST_VERSION,
            columns,
            delimiter: (cfg.delimiter as char).to_string(),
            header: cfg.header,
            compression: cfg.compression.label().map(str::to_string),
            rows: parts.iter().map(|p| p.rows).sum(),
            parts,
        }
    }

    fn names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }
}

/// What is at a table's path.
#[derive(Debug)]
enum Folder {
    Missing,
    /// A folder with nothing in it: an empty table, which quickhouse may
    /// write into.
    Empty,
    Table(Manifest),
}

/// A table this run created (staging, or a destination created empty), which
/// inserts write parts into.
struct Staged {
    dir: PathBuf,
    columns: Vec<ManifestColumn>,
    next_part: AtomicU64,
    state: Mutex<StagedState>,
}

#[derive(Default)]
struct StagedState {
    /// Open parts no insert is writing to right now, by the stream of the
    /// read that writes them (see [`Sink::insert_stream_batches`]).
    idle: HashMap<String, Vec<PartWriter>>,
    /// Closed parts.
    parts: Vec<PartInfo>,
    /// Set once the table is finalized: no more inserts.
    finished: Option<Manifest>,
}

impl Staged {
    fn names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }
}

/// Writes CSV part files under [`CsvDestConfig::path`]. See the module docs.
pub struct CsvSink {
    cfg: Arc<CsvDestConfig>,
    /// [`CsvDestConfig::path`] as text, for [`Sink::namespace`].
    namespace: String,
    /// This run's id: the `<run_id>` of every part it writes.
    run_id: String,
    tables: Mutex<HashMap<String, Arc<Staged>>>,
}

impl CsvSink {
    pub fn new(cfg: CsvDestConfig) -> Result<Self> {
        if cfg.path.as_os_str().is_empty() {
            return Err(EtlError::config("Csv(path) needs a non-empty path"));
        }
        if cfg.path.exists() && !cfg.path.is_dir() {
            return Err(EtlError::config(format!(
                "Csv(path='{}'): that path is a file, and a CSV destination writes a folder per \
                 table under it",
                cfg.path.display()
            )));
        }
        check_delimiter(cfg.delimiter)?;
        if cfg.max_rows_per_file == 0 {
            return Err(EtlError::config(
                "Csv(max_rows_per_file=0): it must be at least 1",
            ));
        }
        let namespace = cfg.path.display().to_string();
        Ok(CsvSink {
            cfg: Arc::new(cfg),
            namespace,
            run_id: time::OffsetDateTime::now_utc()
                .unix_timestamp_nanos()
                .to_string(),
            tables: Mutex::new(HashMap::new()),
        })
    }

    fn format(&self) -> PartFormat {
        part_format(&self.cfg)
    }

    /// The folder of `table`, a single, safe path component under the root.
    fn table_dir(&self, table: &str) -> Result<PathBuf> {
        check_name("table name", table)?;
        Ok(self.cfg.path.join(table))
    }

    fn state_file(&self, state_table: &str) -> Result<PathBuf> {
        check_name("state_table_name", state_table)?;
        Ok(self.cfg.path.join(format!("{state_table}.json")))
    }

    fn staged(&self, table: &str) -> Option<Arc<Staged>> {
        self.tables
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(table)
            .cloned()
    }

    /// Close every part of `staged` and write its manifest. With
    /// `empty_part`, a table no row went into still gets one part holding the
    /// header (when there is one), so the folder a full refresh swaps in
    /// says what its columns are. Idempotent.
    async fn finalize(&self, staged: Arc<Staged>, empty_part: bool) -> Result<Manifest> {
        let cfg = self.cfg.clone();
        let run_id = self.run_id.clone();
        blocking(move || {
            let (writers, mut parts) = {
                let mut st = staged.state.lock().unwrap_or_else(PoisonError::into_inner);
                if let Some(m) = &st.finished {
                    return Ok(m.clone());
                }
                (std::mem::take(&mut st.idle), std::mem::take(&mut st.parts))
            };
            for w in writers.into_values().flatten() {
                parts.push(w.finish()?);
            }
            let format = part_format(&cfg);
            if parts.is_empty() && empty_part && format.header {
                let n = staged.next_part.fetch_add(1, Ordering::Relaxed);
                let w = PartWriter::create(
                    &staged.dir,
                    format.file_name(&run_id, n),
                    &format,
                    &staged.names(),
                )?;
                parts.push(w.finish()?);
            }
            parts.sort_by(|a, b| a.file.cmp(&b.file));
            let manifest = Manifest::new(&cfg, staged.columns.clone(), parts);
            write_manifest(&staged.dir, &manifest)?;
            sync_dir(&staged.dir);
            staged
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .finished = Some(manifest.clone());
            Ok(manifest)
        })
        .await
    }
}

fn part_format(cfg: &CsvDestConfig) -> PartFormat {
    PartFormat {
        delimiter: cfg.delimiter,
        header: cfg.header,
        compression: cfg.compression,
    }
}

/// Run blocking file work off the async reactor.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| EtlError::internal(format!("csv destination: a file task failed: {e}")))?
}

/// Refuse a delimiter a field can't be framed with.
pub fn check_delimiter(delimiter: u8) -> Result<()> {
    if !delimiter.is_ascii() || matches!(delimiter, b'"' | b'\r' | b'\n') {
        return Err(EtlError::config(format!(
            "Csv(delimiter={:?}): the delimiter must be one ASCII character other than a double \
             quote, CR or LF",
            delimiter as char
        )));
    }
    Ok(())
}

/// Refuse a name that isn't one portable path component: a table name (and
/// so a staging folder's) or the state file's.
fn check_name(what: &str, name: &str) -> Result<()> {
    const BAD: &[char] = &['/', '\\', ':', '*', '?', '"', '<', '>', '|'];
    if name.is_empty()
        || name.starts_with('.')
        || name.chars().any(|c| BAD.contains(&c) || c.is_control())
    {
        return Err(EtlError::config(format!(
            "{what} {name:?} can't name a folder or file under a CSV destination's path: it must \
             be non-empty, must not start with '.', and must not hold a path separator or any of \
             : * ? \" < > |"
        )));
    }
    Ok(())
}

/// Refuse, before anything runs, what a CSV destination can't do (0.21.0):
/// `mode="incremental"`, the options that only shape a database table or its
/// `MERGE`, `type_overrides` and `evolve_schema`, and a `validate=` gate.
/// `validate` says the transfer has one. Everything is named at once.
pub fn refuse_unsupported_options(cfg: &TransferConfig, validate: bool) -> Result<()> {
    if cfg.mode == SyncMode::Incremental {
        return Err(EtlError::config(format!(
            "mode=\"incremental\" is not supported for a CSV destination: a CSV file can't update \
             or delete rows, so there is nothing to upsert into. Use mode=\"append\" with \
             watermark= to add only the rows past a saved cursor on each run (the cursor is kept \
             in <path>/{}.json, keyed by state_key), or mode=\"full\" to rewrite the table's \
             folder.",
            cfg.state_table_name
        )));
    }
    let mut refused: Vec<String> = Vec::new();
    let mut refuse = |set: bool, option: &str, why: &str| {
        if set {
            refused.push(format!("{option} ({why})"));
        }
    };
    refuse(
        cfg.engine.is_some(),
        "engine=",
        "a ClickHouse table engine; a CSV folder has none",
    );
    refuse(
        !cfg.order_by.is_empty(),
        "order_by=",
        "a ClickHouse sorting key; part files keep rows in the order they were read",
    );
    refuse(
        cfg.partition_by.is_some(),
        "partition_by=",
        "a database table's partitioning; a CSV table is one folder",
    );
    refuse(
        !cfg.primary_key.is_empty(),
        "primary_key=",
        "a ClickHouse primary key",
    );
    refuse(
        !cfg.not_null.is_empty(),
        "not_null=",
        "makes a column NOT NULL, and every CSV field can be empty",
    );
    let merge_tuned = cfg.merge_prune_partition_by.is_some()
        || !cfg.merge_prune_key_range
        || cfg.merge_prune_key_list_max > 0;
    refuse(
        merge_tuned,
        "merge_prune_partition_by=/merge_prune_key_range=/merge_prune_key_list_max=",
        "tune the MERGE of an incremental run, and a CSV destination never merges",
    );
    refuse(
        cfg.delete_stale_in_window,
        "delete_stale_in_window=",
        "deletes destination rows, and CSV files can't delete rows",
    );
    refuse(
        !cfg.type_overrides.is_empty(),
        "type_overrides=",
        "names destination column types, and a CSV file has none; to write an unconstrained \
         numeric exactly use numeric_as_decimal=, and to change a column's text transform it in \
         source_query or column_transforms",
    );
    refuse(
        cfg.evolve_schema,
        "evolve_schema=",
        "adds columns to an existing table, and every part of a CSV folder shares one header: an \
         append must match the folder's columns, and a full refresh writes this run's columns \
         anyway",
    );
    refuse(
        validate,
        "validate=",
        "runs a Great Expectations suite against a staging table in the destination database, \
         and a CSV destination has none",
    );
    if !refused.is_empty() {
        return Err(EtlError::config(format!(
            "not supported for a CSV destination, so refused rather than ignored: {}",
            refused.join("; ")
        )));
    }
    check_name("dest_table", &cfg.dest_table)?;
    // The staging folder is `<dest_table><staging_suffix>_<run_id>`.
    check_name(
        "staging_suffix",
        &format!("{}{}_0", cfg.dest_table, cfg.staging_suffix),
    )?;
    check_name("state_table_name", &cfg.state_table_name)?;
    Ok(())
}

/// What is at `dir`: nothing, an empty folder, or a table with its manifest.
/// A folder that holds files but no manifest is refused: quickhouse didn't
/// write it, and replacing or adding to it would lose or mix someone's files.
fn read_folder(dir: &Path) -> Result<Folder> {
    match std::fs::metadata(dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Folder::Missing),
        Err(e) => return Err(io_error("read", dir, e)),
        Ok(m) if !m.is_dir() => {
            return Err(EtlError::config(format!(
                "'{}' is a file, where the CSV destination keeps a table's folder",
                dir.display()
            )))
        }
        Ok(_) => {}
    }
    let path = dir.join(MANIFEST_FILE);
    match std::fs::read(&path) {
        Ok(bytes) => {
            let manifest: Manifest = serde_json::from_slice(&bytes).map_err(|e| {
                EtlError::other(format!(
                    "csv destination: '{}' is not a manifest quickhouse can read ({e}); restore \
                     it, or remove the folder to start the table over",
                    path.display()
                ))
            })?;
            if manifest.format != MANIFEST_FORMAT || manifest.version > MANIFEST_VERSION {
                return Err(EtlError::other(format!(
                    "csv destination: '{}' is a {:?} manifest, version {}, which this quickhouse \
                     can't write to",
                    path.display(),
                    manifest.format,
                    manifest.version
                )));
            }
            Ok(Folder::Table(manifest))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let mut entries = std::fs::read_dir(dir).map_err(|e| io_error("list", dir, e))?;
            if entries.next().is_none() {
                Ok(Folder::Empty)
            } else {
                Err(EtlError::config(format!(
                    "'{}' holds files but no {MANIFEST_FILE}, so quickhouse didn't write it and \
                     won't replace or add to it. Move it away, or pick another dest_table",
                    dir.display()
                )))
            }
        }
        Err(e) => Err(io_error("read", &path, e)),
    }
}

/// Write `bytes` to `path` through a temporary file in the same folder,
/// synced and renamed over it: a reader sees the old file or the new one,
/// never part of one.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let tmp = dir.join(format!(
        ".{name}.tmp-{}-{}",
        std::process::id(),
        time::OffsetDateTime::now_utc().unix_timestamp_nanos()
    ));
    let result = (|| {
        let mut f = File::create(&tmp).map_err(|e| io_error("create", &tmp, e))?;
        f.write_all(bytes).map_err(|e| io_error("write", &tmp, e))?;
        f.sync_all().map_err(|e| io_error("sync", &tmp, e))?;
        drop(f);
        std::fs::rename(&tmp, path).map_err(|e| io_error("replace", path, e))
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

fn write_manifest(dir: &Path, manifest: &Manifest) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|e| EtlError::internal(format!("csv destination: manifest: {e}")))?;
    write_atomic(&dir.join(MANIFEST_FILE), &bytes)
}

/// Sync a folder's entries to disk after a rename into or out of it. Best
/// effort, and Unix only: Windows can't open a folder as a file.
fn sync_dir(dir: &Path) {
    #[cfg(unix)]
    if let Ok(f) = File::open(dir) {
        let _ = f.sync_all();
    }
    #[cfg(not(unix))]
    let _ = dir;
}

/// Run `f` holding the lock on `root`'s state and manifests: a mutex for this
/// process, and on Unix an exclusive `flock` on the folder for every process.
/// A filesystem that can't `flock` is logged and passed: the mutex still holds.
fn with_root_lock<T>(root: &Path, f: impl FnOnce() -> Result<T>) -> Result<T> {
    type Locks = Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>;
    static LOCKS: OnceLock<Locks> = OnceLock::new();
    std::fs::create_dir_all(root).map_err(|e| io_error("create", root, e))?;
    let key = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    let lock = LOCKS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(key)
        .or_default()
        .clone();
    let _held = lock.lock().unwrap_or_else(PoisonError::into_inner);
    let _flock = DirLock::acquire(root);
    f()
}

/// An exclusive `flock` on a folder, released when dropped (the file closes).
#[cfg(unix)]
struct DirLock(#[allow(dead_code)] Option<File>);

#[cfg(unix)]
impl DirLock {
    fn acquire(dir: &Path) -> Self {
        use std::os::unix::io::AsRawFd;
        let f = match File::open(dir) {
            Ok(f) => f,
            Err(e) => {
                tracing::debug!("csv destination: no flock on '{}': {e}", dir.display());
                return DirLock(None);
            }
        };
        // SAFETY: `flock` on a descriptor this function owns, kept open (and
        // so locked) as long as the returned guard lives.
        let rc = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX) };
        if rc != 0 {
            tracing::debug!(
                "csv destination: no flock on '{}': {}",
                dir.display(),
                std::io::Error::last_os_error()
            );
            return DirLock(None);
        }
        DirLock(Some(f))
    }
}

#[cfg(not(unix))]
struct DirLock;

#[cfg(not(unix))]
impl DirLock {
    fn acquire(_dir: &Path) -> Self {
        DirLock
    }
}

/// Swap two folders in one step, where the platform can.
#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
fn exchange(a: &Path, b: &Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let cpath = |p: &Path| {
        CString::new(p.as_os_str().as_bytes())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
    };
    let (a, b) = (cpath(a)?, cpath(b)?);
    // SAFETY: two NUL-terminated paths that outlive the call. The raw
    // syscall, not glibc's `renameat2` wrapper, which the manylinux2014
    // glibc (2.17) predates.
    let rc = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            a.as_ptr(),
            libc::AT_FDCWD,
            b.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
)))]
fn exchange(_a: &Path, _b: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "no atomic folder exchange on this platform",
    ))
}

/// Put `staging` at `dest`'s path, and what was at `dest` at `staging`'s, for
/// the caller to remove. Atomic where [`exchange`] is; otherwise two renames,
/// undone when the second fails.
fn swap_dirs(dest: &Path, staging: &Path) -> Result<()> {
    let root = dest.parent().unwrap_or_else(|| Path::new("."));
    if !dest.exists() {
        std::fs::rename(staging, dest).map_err(|e| io_error("move into place", staging, e))?;
        sync_dir(root);
        return Ok(());
    }
    match exchange(dest, staging) {
        Ok(()) => {
            sync_dir(root);
            return Ok(());
        }
        Err(e) => tracing::debug!(
            "csv destination: no atomic exchange of '{}' and '{}' ({e}); renaming twice",
            dest.display(),
            staging.display()
        ),
    }
    swap_dirs_by_renames(dest, staging)?;
    sync_dir(root);
    Ok(())
}

/// [`swap_dirs`] without [`exchange`]: `dest` aside, `staging` into its
/// place, and the old folder to `staging`'s path.
fn swap_dirs_by_renames(dest: &Path, staging: &Path) -> Result<()> {
    let aside = staging.with_file_name(format!(
        "{}_old",
        staging
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default()
    ));
    std::fs::rename(dest, &aside).map_err(|e| io_error("move aside", dest, e))?;
    if let Err(e) = std::fs::rename(staging, dest) {
        let undo = std::fs::rename(&aside, dest);
        let undone = match undo {
            Ok(()) => "the old folder is back in place".to_string(),
            Err(u) => format!(
                "and moving the old folder back failed too ({u}): it is at '{}'",
                aside.display()
            ),
        };
        return Err(EtlError::other(format!(
            "csv destination: could not move '{}' into place ({e}); {undone}",
            staging.display()
        )));
    }
    if let Err(e) = std::fs::rename(&aside, staging) {
        tracing::warn!(
            "csv destination: the old folder at '{}' could not be moved for removal ({e}); \
             removing it where it is",
            aside.display()
        );
        let _ = std::fs::remove_dir_all(&aside);
    }
    Ok(())
}

/// How `manifest`'s folder differs from what this run writes: its columns
/// (names, then order) and its format. Empty when an append can add parts.
fn layout_differences(manifest: &Manifest, names: &[String], cfg: &CsvDestConfig) -> Vec<String> {
    let mut diffs = Vec::new();
    let have = manifest.names();
    if have != names {
        let missing: Vec<&str> = have
            .iter()
            .filter(|c| !names.contains(c))
            .map(String::as_str)
            .collect();
        let added: Vec<&str> = names
            .iter()
            .filter(|c| !have.contains(c))
            .map(String::as_str)
            .collect();
        if !missing.is_empty() {
            diffs.push(format!(
                "this run doesn't write column(s) {} that the folder's parts have",
                missing.join(", ")
            ));
        }
        if !added.is_empty() {
            diffs.push(format!(
                "this run writes column(s) {} that the folder's parts don't have",
                added.join(", ")
            ));
        }
        if missing.is_empty() && added.is_empty() {
            diffs.push(format!(
                "the columns are in another order: the folder's parts have [{}], this run writes \
                 [{}]",
                have.join(", "),
                names.join(", ")
            ));
        }
    }
    let delimiter = (cfg.delimiter as char).to_string();
    if manifest.delimiter != delimiter {
        diffs.push(format!(
            "the folder's parts use delimiter {:?} and this run {delimiter:?}",
            manifest.delimiter
        ));
    }
    if manifest.header != cfg.header {
        diffs.push(format!(
            "the folder's parts were written with header={} and this run has header={}",
            py_bool(manifest.header),
            py_bool(cfg.header)
        ));
    }
    let compression = cfg.compression.label().map(str::to_string);
    if manifest.compression != compression {
        diffs.push(format!(
            "the folder's parts have compression={} and this run compression={}",
            py_opt(manifest.compression.as_deref()),
            py_opt(compression.as_deref())
        ));
    }
    diffs
}

fn py_bool(b: bool) -> &'static str {
    if b {
        "True"
    } else {
        "False"
    }
}

fn py_opt(s: Option<&str>) -> String {
    s.map_or_else(|| "None".to_string(), |s| format!("{s:?}"))
}

/// The config error for an append into `dir` whose layout differs.
fn layout_error(dir: &Path, diffs: &[String]) -> EtlError {
    EtlError::config(format!(
        "can't append to '{}': {}. Every part in a CSV table's folder shares one column layout \
         and format, so readers can read the folder as one table, and evolve_schema isn't \
         supported for a CSV destination. Run mode=\"full\" once to rewrite the folder with this \
         run's columns, or append to a new dest_table",
        dir.display(),
        diffs.join("; ")
    ))
}

// ---- the state file ----

#[derive(Debug, Default, Serialize, Deserialize)]
struct StateFile {
    version: u32,
    keys: Vec<StateEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateEntry {
    state_key: String,
    dest_table: String,
    /// Empty once cleared (a recreated destination's cursor).
    last_watermark: String,
    /// Rows the run that saved it wrote.
    rows: u64,
    /// When it was saved, UTC, `YYYY-MM-DD HH:MM:SS.ffffff`.
    last_run: String,
    /// The same, in microseconds since 1970, for sorting and `idle_days`.
    last_run_micros: i64,
}

fn read_state(path: &Path) -> Result<StateFile> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
            EtlError::other(format!(
                "csv destination: the state file '{}' is not readable JSON ({e}); restore it, or \
                 remove it to start every cursor over",
                path.display()
            ))
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(StateFile {
            version: STATE_VERSION,
            keys: Vec::new(),
        }),
        Err(e) => Err(io_error("read", path, e)),
    }
}

fn now_micros() -> i64 {
    (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000) as i64
}

/// `micros` since 1970 as `YYYY-MM-DD HH:MM:SS.ffffff` (UTC), as BigQuery's
/// state table reports `last_run`.
fn render_micros(micros: i64) -> String {
    chrono::DateTime::from_timestamp_micros(micros)
        .map(|t| t.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string())
        .unwrap_or_default()
}

#[async_trait]
impl Sink for CsvSink {
    async fn table_exists(&self, table: &str) -> Result<bool> {
        let dir = self.table_dir(table)?;
        match std::fs::metadata(&dir) {
            Ok(m) if m.is_dir() => Ok(true),
            Ok(_) => Err(EtlError::config(format!(
                "'{}' is a file, where the CSV destination keeps a table's folder",
                dir.display()
            ))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(io_error("read", &dir, e)),
        }
    }

    /// Create `table`'s folder with a manifest of `columns` and no parts, and
    /// take inserts into it. The folder must not exist yet: a destination is
    /// only created when [`Self::table_exists`] says it's missing, and a
    /// staging folder has a name no run used before.
    async fn create_table(
        &self,
        table: &str,
        columns: &[ColumnType],
        _cfg: &TransferConfig,
    ) -> Result<()> {
        let dir = self.table_dir(table)?;
        let columns: Vec<ManifestColumn> = columns
            .iter()
            .map(|c| ManifestColumn {
                name: c.name.clone(),
                arrow_type: c.arrow.to_string(),
            })
            .collect();
        let cfg = self.cfg.clone();
        let manifest = Manifest::new(&cfg, columns.clone(), Vec::new());
        let created = dir.clone();
        blocking(move || {
            std::fs::create_dir_all(&cfg.path).map_err(|e| io_error("create", &cfg.path, e))?;
            std::fs::create_dir(&created).map_err(|e| io_error("create", &created, e))?;
            write_manifest(&created, &manifest)?;
            sync_dir(&cfg.path);
            Ok(())
        })
        .await?;
        self.tables
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                table.to_string(),
                Arc::new(Staged {
                    dir,
                    columns,
                    next_part: AtomicU64::new(0),
                    state: Mutex::new(StagedState::default()),
                }),
            );
        Ok(())
    }

    /// A new, empty `new_table` with `like_table`'s columns. Not used by a
    /// transfer, which creates CSV staging from its own columns (see
    /// [`Sink::stages_from_destination`]).
    async fn clone_table_structure(&self, new_table: &str, like_table: &str) -> Result<()> {
        let like = self.table_dir(like_table)?;
        let manifest = blocking(move || match read_folder(&like)? {
            Folder::Table(m) => Ok(m),
            _ => Err(EtlError::config(format!(
                "csv destination: '{}' has no manifest to copy columns from",
                like.display()
            ))),
        })
        .await?;
        let dir = self.table_dir(new_table)?;
        let columns = manifest.columns.clone();
        let fresh = Manifest::new(&self.cfg, columns.clone(), Vec::new());
        let created = dir.clone();
        blocking(move || {
            std::fs::create_dir(&created).map_err(|e| io_error("create", &created, e))?;
            write_manifest(&created, &fresh)
        })
        .await?;
        self.tables
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                new_table.to_string(),
                Arc::new(Staged {
                    dir,
                    columns,
                    next_part: AtomicU64::new(0),
                    state: Mutex::new(StagedState::default()),
                }),
            );
        Ok(())
    }

    /// [`Self::insert_stream_batches`] from a read with one stream.
    async fn insert_batches(
        &self,
        table: &str,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<u64> {
        self.insert_stream_batches(table, "all", schema, batches)
            .await
    }

    /// Write `batches` into one of `stream`'s open parts of `table` (a new one
    /// when the stream has none free: its first insert, or one while another
    /// of its inserts is still writing), opening the next part every
    /// `max_rows_per_file` rows. So each parallel partition writes parts of
    /// its own. Returns the bytes of CSV text written, before compression.
    async fn insert_stream_batches(
        &self,
        table: &str,
        stream: &str,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<u64> {
        let staged = self.staged(table).ok_or_else(|| {
            EtlError::internal(format!(
                "csv destination: an insert into '{table}', which this run didn't create"
            ))
        })?;
        let written: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        if written != staged.names() {
            return Err(EtlError::internal(format!(
                "csv destination: an insert into '{table}' writes columns [{}], and the table has \
                 [{}]",
                written.join(", "),
                staged.names().join(", ")
            )));
        }
        let writer = {
            let mut st = staged.state.lock().unwrap_or_else(PoisonError::into_inner);
            if st.finished.is_some() {
                return Err(EtlError::internal(format!(
                    "csv destination: an insert into '{table}' after its parts were closed"
                )));
            }
            st.idle.get_mut(stream).and_then(Vec::pop)
        };
        let batches = batches.to_vec();
        let format = self.format();
        let max_rows = self.cfg.max_rows_per_file;
        let run_id = self.run_id.clone();
        let work = staged.clone();
        let (writer, closed, bytes) = blocking(move || {
            let names = work.names();
            let mut writer = writer;
            let mut closed = Vec::new();
            let mut bytes = 0u64;
            for batch in &batches {
                let rows = batch.num_rows();
                let mut offset = 0;
                while offset < rows {
                    let w = match writer.take() {
                        Some(w) if w.rows() < max_rows => w,
                        Some(full) => {
                            closed.push(full.finish()?);
                            new_part(&work, &format, &run_id, &names)?
                        }
                        None => new_part(&work, &format, &run_id, &names)?,
                    };
                    let mut w = w;
                    let room = (max_rows - w.rows()).min((rows - offset) as u64) as usize;
                    bytes += w.write(&batch.slice(offset, room))?;
                    offset += room;
                    writer = Some(w);
                }
            }
            Ok((writer, closed, bytes))
        })
        .await?;
        let mut st = staged.state.lock().unwrap_or_else(PoisonError::into_inner);
        st.parts.extend(closed);
        if let Some(w) = writer {
            st.idle.entry(stream.to_string()).or_default().push(w);
        }
        Ok(bytes)
    }

    /// Close `staging`'s parts, write its manifest, and swap its folder into
    /// `dest`'s place (see [`swap_dirs`]). The old folder is left at
    /// `staging`'s path, which the transfer then drops.
    async fn atomic_swap(&self, dest: &str, staging: &str, _columns: &[ColumnType]) -> Result<()> {
        let staged = self.staged(staging).ok_or_else(|| {
            EtlError::internal(format!(
                "csv destination: a swap from '{staging}', which this run didn't create"
            ))
        })?;
        let manifest = self.finalize(staged.clone(), true).await?;
        let dest_dir = self.table_dir(dest)?;
        let root = self.cfg.path.clone();
        let staging_dir = staged.dir.clone();
        blocking(move || {
            with_root_lock(&root, || {
                // Refused here too, not only up front: the folder could have
                // changed while the run read.
                read_folder(&dest_dir)?;
                swap_dirs(&dest_dir, &staging_dir)
            })
        })
        .await?;
        tracing::info!(
            "csv destination: swapped {} part(s), {} row(s), into '{}'",
            manifest.parts.len(),
            manifest.rows,
            self.cfg.path.join(dest).display()
        );
        Ok(())
    }

    /// Rows `table` holds, from its manifest, without reading a part: what
    /// the full-refresh shrink guard compares. `None` when it doesn't exist.
    /// A staging folder counts only once finalized; nothing asks before.
    async fn current_row_count(&self, table: &str) -> Result<Option<u64>> {
        let dir = self.table_dir(table)?;
        blocking(move || {
            Ok(match read_folder(&dir)? {
                Folder::Missing => None,
                Folder::Empty => Some(0),
                Folder::Table(m) => Some(m.rows),
            })
        })
        .await
    }

    /// Remove `table`'s folder: one this run created, or one holding a
    /// manifest (never a folder of someone else's files). Missing is fine.
    async fn drop_table(&self, table: &str) -> Result<()> {
        let staged = self
            .tables
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(table);
        let dir = self.table_dir(table)?;
        blocking(move || {
            // Close (and so release) whatever parts it still has open.
            if let Some(staged) = &staged {
                let mut st = staged.state.lock().unwrap_or_else(PoisonError::into_inner);
                st.idle.clear();
            }
            if staged.is_none() {
                if let Folder::Missing = read_folder(&dir)? {
                    return Ok(());
                }
            }
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(io_error("remove", &dir, e)),
            }
        })
        .await
    }

    async fn ensure_state_table(&self, state_table: &str) -> Result<()> {
        self.state_file(state_table)?;
        let root = self.cfg.path.clone();
        blocking(move || std::fs::create_dir_all(&root).map_err(|e| io_error("create", &root, e)))
            .await
    }

    async fn read_last_watermark(&self, cfg: &TransferConfig) -> Result<Option<String>> {
        let path = self.state_file(&cfg.state_table_name)?;
        let key = cfg.effective_state_key();
        let dest = cfg.dest_table.clone();
        blocking(move || {
            Ok(read_state(&path)?
                .keys
                .into_iter()
                .find(|e| e.state_key == key && e.dest_table == dest)
                .map(|e| e.last_watermark)
                .filter(|w| !w.is_empty()))
        })
        .await
    }

    async fn persist_watermark(
        &self,
        cfg: &TransferConfig,
        watermark: &str,
        rows: u64,
    ) -> Result<()> {
        let path = self.state_file(&cfg.state_table_name)?;
        let root = self.cfg.path.clone();
        let micros = now_micros();
        let entry = StateEntry {
            state_key: cfg.effective_state_key(),
            dest_table: cfg.dest_table.clone(),
            last_watermark: watermark.to_string(),
            rows,
            last_run: render_micros(micros),
            last_run_micros: micros,
        };
        blocking(move || {
            with_root_lock(&root, || {
                let mut state = read_state(&path)?;
                state.version = STATE_VERSION;
                match state
                    .keys
                    .iter_mut()
                    .find(|e| e.state_key == entry.state_key && e.dest_table == entry.dest_table)
                {
                    Some(e) => *e = entry,
                    None => state.keys.push(entry),
                }
                let bytes = serde_json::to_vec_pretty(&state)
                    .map_err(|e| EtlError::internal(format!("csv destination: state: {e}")))?;
                write_atomic(&path, &bytes)?;
                sync_dir(&root);
                Ok(())
            })
        })
        .await
    }

    async fn add_missing_columns(
        &self,
        _table: &str,
        _columns: &[ColumnType],
        _cfg: &TransferConfig,
    ) -> Result<Vec<String>> {
        Err(EtlError::config(
            "evolve_schema is not supported for a CSV destination",
        ))
    }

    fn dest_kind(&self) -> DestKind {
        DestKind::Csv
    }

    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn stages_direct_writes(&self) -> bool {
        true
    }

    fn stages_from_destination(&self) -> bool {
        false
    }

    /// Refuse a write into a folder quickhouse didn't write, and an append
    /// whose columns (names and order) or format differ from the folder's.
    async fn check_write(
        &self,
        table: &str,
        columns: &[ColumnType],
        cfg: &TransferConfig,
    ) -> Result<()> {
        let dir = self.table_dir(table)?;
        let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        let append = cfg.mode != SyncMode::Full;
        let csv = self.cfg.clone();
        blocking(move || {
            let folder = read_folder(&dir)?;
            if let (true, Folder::Table(m)) = (append, &folder) {
                if !m.columns.is_empty() {
                    let diffs = layout_differences(m, &names, &csv);
                    if !diffs.is_empty() {
                        return Err(layout_error(&dir, &diffs));
                    }
                }
            }
            Ok(())
        })
        .await
    }

    /// Move `staging`'s closed parts into `dest`'s folder and add them to its
    /// manifest. A part that can't be moved puts the ones moved before it
    /// back, so the folder never holds parts its manifest doesn't list.
    async fn insert_select(
        &self,
        dest: &str,
        staging: &str,
        _columns: &[ColumnType],
    ) -> Result<()> {
        let staged = self.staged(staging).ok_or_else(|| {
            EtlError::internal(format!(
                "csv destination: a move from '{staging}', which this run didn't create"
            ))
        })?;
        let staged_manifest = self.finalize(staged.clone(), false).await?;
        let dest_dir = self.table_dir(dest)?;
        let root = self.cfg.path.clone();
        let csv = self.cfg.clone();
        let from_dir = staged.dir.clone();
        let parts = staged_manifest.parts.clone();
        let moved_rows = staged_manifest.rows;
        let n = parts.len();
        blocking(move || {
            with_root_lock(&root, || {
                let mut target = match read_folder(&dest_dir)? {
                    Folder::Table(m) => m,
                    Folder::Empty => Manifest::new(&csv, staged_manifest.columns.clone(), vec![]),
                    Folder::Missing => {
                        return Err(EtlError::other(format!(
                            "csv destination: '{}' disappeared while the run read",
                            dest_dir.display()
                        )))
                    }
                };
                if target.columns.is_empty() {
                    target.columns = staged_manifest.columns.clone();
                } else {
                    let diffs = layout_differences(&target, &staged_manifest.names(), &csv);
                    if !diffs.is_empty() {
                        return Err(layout_error(&dest_dir, &diffs));
                    }
                }
                let mut moved: Vec<&str> = Vec::new();
                let undo = |moved: &[&str]| {
                    for f in moved.iter().rev() {
                        let _ = std::fs::rename(dest_dir.join(f), from_dir.join(f));
                    }
                };
                for p in &parts {
                    let to = dest_dir.join(&p.file);
                    let result = if to.exists() {
                        Err(EtlError::other(format!(
                            "csv destination: '{}' exists already",
                            to.display()
                        )))
                    } else {
                        std::fs::rename(from_dir.join(&p.file), &to)
                            .map_err(|e| io_error("move into place", &to, e))
                    };
                    if let Err(e) = result {
                        undo(&moved);
                        return Err(e);
                    }
                    moved.push(&p.file);
                }
                target.parts.extend(parts.iter().cloned());
                target.rows = target.parts.iter().map(|p| p.rows).sum();
                if let Err(e) = write_manifest(&dest_dir, &target) {
                    undo(&moved);
                    return Err(e);
                }
                sync_dir(&dest_dir);
                sync_dir(&from_dir);
                Ok(())
            })
        })
        .await?;
        tracing::info!(
            "csv destination: appended {n} part(s), {moved_rows} row(s), to '{}'",
            self.cfg.path.join(dest).display()
        );
        Ok(())
    }

    /// Nothing to compact: the state file holds one entry per key.
    async fn compact_state(&self, state_table: &str) -> Result<u64> {
        self.state_file(state_table)?;
        Ok(0)
    }

    async fn state_keys(&self, state_table: &str, idle_days: Option<u32>) -> Result<Vec<StateKey>> {
        let path = self.state_file(state_table)?;
        let cutoff = idle_days.map(|d| now_micros() - i64::from(d) * 86_400 * 1_000_000);
        blocking(move || {
            let mut keys = read_state(&path)?.keys;
            if let Some(cutoff) = cutoff {
                keys.retain(|e| e.last_run_micros < cutoff);
            }
            keys.sort_by(|a, b| {
                (a.last_run_micros, &a.state_key, &a.dest_table).cmp(&(
                    b.last_run_micros,
                    &b.state_key,
                    &b.dest_table,
                ))
            });
            Ok(keys
                .into_iter()
                .map(|e| StateKey {
                    state_key: e.state_key,
                    dest_table: e.dest_table,
                    last_watermark: e.last_watermark,
                    last_run: e.last_run,
                    state_rows: 1,
                })
                .collect())
        })
        .await
    }
}

/// Open `staged`'s next part.
fn new_part(
    staged: &Staged,
    format: &PartFormat,
    run_id: &str,
    names: &[String],
) -> Result<PartWriter> {
    let n = staged.next_part.fetch_add(1, Ordering::Relaxed);
    PartWriter::create(&staged.dir, format.file_name(run_id, n), format, names)
}

impl std::fmt::Debug for CsvSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvSink")
            .field("path", &self.cfg.path)
            .field("run_id", &self.run_id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CsvCompression;
    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    /// A fresh folder under the system temp dir, removed when dropped.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let p = std::env::temp_dir().join(format!(
                "qh-csv-{tag}-{}-{}",
                std::process::id(),
                time::OffsetDateTime::now_utc().unix_timestamp_nanos()
            ));
            std::fs::create_dir_all(&p).unwrap();
            TempDir(p)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn csv_cfg(path: &Path) -> CsvDestConfig {
        CsvDestConfig {
            path: path.to_path_buf(),
            delimiter: b',',
            header: true,
            compression: CsvCompression::None,
            max_rows_per_file: 1_000,
        }
    }

    fn cols() -> Vec<ColumnType> {
        ["id", "name"]
            .iter()
            .zip([DataType::Int64, DataType::Utf8])
            .map(|(n, t)| ColumnType {
                name: n.to_string(),
                type_id: 0,
                nullable: true,
                clickhouse_inner: String::new(),
                arrow: t,
                arbitrary_precision_decimal: false,
                declared_decimal: None,
            })
            .collect()
    }

    fn rows(ids: std::ops::Range<i64>) -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let names: Vec<String> = ids.clone().map(|i| format!("n{i}")).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids.collect::<Vec<_>>())) as ArrayRef,
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    fn tcfg(dest: &str, mode: SyncMode) -> TransferConfig {
        let mut c = crate::config::default_test_config();
        c.dest_table = dest.into();
        c.mode = mode;
        c
    }

    fn manifest(dir: &Path) -> Manifest {
        match read_folder(dir).unwrap() {
            Folder::Table(m) => m,
            other => panic!("no manifest in {}: {other:?}", dir.display()),
        }
    }

    fn part_files(dir: &Path) -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("part-"))
            .collect();
        v.sort();
        v
    }

    /// One full refresh through the sink, as `sync.rs` drives it.
    async fn full_refresh(sink: &CsvSink, dest: &str, staging: &str, ids: std::ops::Range<i64>) {
        let cfg = tcfg(dest, SyncMode::Full);
        sink.check_write(dest, &cols(), &cfg).await.unwrap();
        if !sink.table_exists(dest).await.unwrap() {
            sink.create_table(dest, &cols(), &cfg).await.unwrap();
        }
        sink.create_table(staging, &cols(), &cfg).await.unwrap();
        let (schema, batch) = rows(ids);
        if batch.num_rows() > 0 {
            sink.insert_batches(staging, schema, &[batch])
                .await
                .unwrap();
        }
        sink.atomic_swap(dest, staging, &cols()).await.unwrap();
        sink.drop_table(staging).await.unwrap();
    }

    #[tokio::test]
    async fn a_full_refresh_swaps_in_new_parts_and_removes_the_old() {
        let tmp = TempDir::new("swap");
        let mut c = csv_cfg(&tmp.0);
        c.max_rows_per_file = 4;
        let sink = CsvSink::new(c.clone()).unwrap();
        full_refresh(&sink, "orders", "orders_tmp_1", 0..10).await;
        let dir = tmp.0.join("orders");
        let first = part_files(&dir);
        // 10 rows at 4 per file: three parts, each with its own header.
        assert_eq!(first.len(), 3);
        let m = manifest(&dir);
        assert_eq!(m.rows, 10);
        assert_eq!(
            m.parts.iter().map(|p| p.rows).collect::<Vec<_>>(),
            vec![4, 4, 2]
        );
        assert_eq!(m.names(), vec!["id", "name"]);
        let text = std::fs::read_to_string(dir.join(&first[2])).unwrap();
        assert_eq!(text, "id,name\n8,n8\n9,n9\n");
        assert!(!tmp.0.join("orders_tmp_1").exists());

        // A second refresh (another run) replaces every part.
        let sink = CsvSink::new(c).unwrap();
        full_refresh(&sink, "orders", "orders_tmp_2", 100..102).await;
        let second = part_files(&dir);
        assert_eq!(second.len(), 1);
        assert!(first.iter().all(|f| !second.contains(f)));
        assert_eq!(manifest(&dir).rows, 2);
        assert!(!tmp.0.join("orders_tmp_2").exists());
        assert!(!tmp.0.join("orders_tmp_2_old").exists());
    }

    #[tokio::test]
    async fn an_empty_full_refresh_leaves_one_header_only_part() {
        let tmp = TempDir::new("empty");
        let sink = CsvSink::new(csv_cfg(&tmp.0)).unwrap();
        full_refresh(&sink, "t", "t_tmp", 0..0).await;
        let dir = tmp.0.join("t");
        let parts = part_files(&dir);
        assert_eq!(parts.len(), 1);
        assert_eq!(
            std::fs::read_to_string(dir.join(&parts[0])).unwrap(),
            "id,name\n"
        );
        assert_eq!(manifest(&dir).rows, 0);
    }

    #[test]
    fn the_rename_fallback_swaps_and_leaves_the_old_folder_at_staging() {
        let tmp = TempDir::new("fallback");
        let dest = tmp.0.join("t");
        let staging = tmp.0.join("t_tmp");
        std::fs::create_dir(&dest).unwrap();
        std::fs::write(dest.join("old.csv"), "old").unwrap();
        std::fs::create_dir(&staging).unwrap();
        std::fs::write(staging.join("new.csv"), "new").unwrap();
        swap_dirs_by_renames(&dest, &staging).unwrap();
        assert!(dest.join("new.csv").exists() && !dest.join("old.csv").exists());
        assert!(staging.join("old.csv").exists());
        assert!(!tmp.0.join("t_tmp_old").exists());
        // And the exchange where the platform has one: the same outcome.
        if exchange(&dest, &staging).is_ok() {
            assert!(dest.join("old.csv").exists() && staging.join("new.csv").exists());
        }
    }

    #[tokio::test]
    async fn an_aborted_run_leaves_the_table_as_it_was() {
        let tmp = TempDir::new("abort");
        let c = csv_cfg(&tmp.0);
        full_refresh(&CsvSink::new(c.clone()).unwrap(), "t", "t_tmp_1", 0..3).await;
        let dir = tmp.0.join("t");
        let before = std::fs::read_dir(&dir).unwrap().count();
        let before_parts = part_files(&dir);

        // A run that wrote into staging and failed: the transfer drops staging.
        let sink = CsvSink::new(c).unwrap();
        let cfg = tcfg("t", SyncMode::Full);
        sink.create_table("t_tmp_2", &cols(), &cfg).await.unwrap();
        let (schema, batch) = rows(10..20);
        sink.insert_batches("t_tmp_2", schema, &[batch])
            .await
            .unwrap();
        assert!(tmp.0.join("t_tmp_2").exists());
        sink.drop_table("t_tmp_2").await.unwrap();
        assert!(!tmp.0.join("t_tmp_2").exists());
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), before);
        assert_eq!(part_files(&dir), before_parts);
        assert_eq!(manifest(&dir).rows, 3);
    }

    #[tokio::test]
    async fn an_append_moves_its_parts_in_and_extends_the_manifest() {
        let tmp = TempDir::new("append");
        let c = csv_cfg(&tmp.0);
        let cfg = tcfg("events", SyncMode::Append);
        for (run, ids) in [(1, 0..2), (2, 2..5)] {
            let sink = CsvSink::new(c.clone()).unwrap();
            sink.check_write("events", &cols(), &cfg).await.unwrap();
            if !sink.table_exists("events").await.unwrap() {
                sink.create_table("events", &cols(), &cfg).await.unwrap();
            }
            let staging = format!("events_tmp_{run}");
            sink.create_table(&staging, &cols(), &cfg).await.unwrap();
            let (schema, batch) = rows(ids);
            sink.insert_batches(&staging, schema, &[batch])
                .await
                .unwrap();
            sink.insert_select("events", &staging, &cols())
                .await
                .unwrap();
            sink.drop_table(&staging).await.unwrap();
            // Distinct run ids: a nanosecond clock, read at construction.
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let dir = tmp.0.join("events");
        let m = manifest(&dir);
        assert_eq!(m.rows, 5);
        assert_eq!(m.parts.len(), 2);
        assert_eq!(part_files(&dir).len(), 2);
        assert_eq!(
            m.parts.iter().map(|p| p.file.clone()).collect::<Vec<_>>(),
            part_files(&dir)
        );
        assert!(!tmp.0.join("events_tmp_1").exists());
        // An empty append adds no part.
        let sink = CsvSink::new(c).unwrap();
        assert_eq!(sink.current_row_count("events").await.unwrap(), Some(5));
        sink.create_table("events_tmp_3", &cols(), &cfg)
            .await
            .unwrap();
        sink.insert_select("events", "events_tmp_3", &cols())
            .await
            .unwrap();
        assert_eq!(manifest(&dir).parts.len(), 2);
        assert_eq!(sink.current_row_count("events").await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn an_append_with_other_columns_or_format_is_refused() {
        let tmp = TempDir::new("mismatch");
        let c = csv_cfg(&tmp.0);
        full_refresh(&CsvSink::new(c.clone()).unwrap(), "t", "t_tmp", 0..1).await;
        let cfg = tcfg("t", SyncMode::Append);
        let sink = CsvSink::new(c.clone()).unwrap();
        let mut other = cols();
        other[1].name = "label".into();
        let err = sink
            .check_write("t", &other, &cfg)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("configuration error"), "{err}");
        assert!(err.contains("doesn't write column(s) name"), "{err}");
        assert!(err.contains("writes column(s) label"), "{err}");
        assert!(err.contains("evolve_schema isn't supported"), "{err}");
        let mut reversed = cols();
        reversed.reverse();
        let err = sink
            .check_write("t", &reversed, &cfg)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("another order"), "{err}");
        // The same columns pass; a full refresh may change them.
        sink.check_write("t", &cols(), &cfg).await.unwrap();
        sink.check_write("t", &other, &tcfg("t", SyncMode::Full))
            .await
            .unwrap();
        // Another delimiter is another format.
        let mut semi = c;
        semi.delimiter = b';';
        let err = CsvSink::new(semi)
            .unwrap()
            .check_write("t", &cols(), &cfg)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("delimiter \",\" and this run \";\""), "{err}");
    }

    #[tokio::test]
    async fn a_folder_quickhouse_did_not_write_is_never_replaced() {
        let tmp = TempDir::new("foreign");
        let dir = tmp.0.join("t");
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("notes.txt"), "mine").unwrap();
        let sink = CsvSink::new(csv_cfg(&tmp.0)).unwrap();
        let err = sink
            .check_write("t", &cols(), &tcfg("t", SyncMode::Full))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("no _quickhouse_manifest.json"), "{err}");
        assert!(sink.drop_table("t").await.is_err());
        assert!(dir.join("notes.txt").exists());
        // An empty folder is an empty table.
        std::fs::remove_file(dir.join("notes.txt")).unwrap();
        assert_eq!(sink.current_row_count("t").await.unwrap(), Some(0));
    }

    #[tokio::test]
    async fn the_state_file_keeps_one_cursor_per_key() {
        let tmp = TempDir::new("state");
        let sink = CsvSink::new(csv_cfg(&tmp.0)).unwrap();
        let mut a = tcfg("orders", SyncMode::Append);
        a.state_key = Some("pg.orders".into());
        let mut b = a.clone();
        b.dest_table = "orders_copy".into();
        assert_eq!(sink.read_last_watermark(&a).await.unwrap(), None);
        sink.ensure_state_table(&a.state_table_name).await.unwrap();
        sink.persist_watermark(&a, "2024-01-01", 3).await.unwrap();
        sink.persist_watermark(&b, "7", 1).await.unwrap();
        sink.persist_watermark(&a, "2024-01-02", 2).await.unwrap();
        assert_eq!(
            sink.read_last_watermark(&a).await.unwrap().as_deref(),
            Some("2024-01-02")
        );
        assert_eq!(
            sink.read_last_watermark(&b).await.unwrap().as_deref(),
            Some("7")
        );
        let keys = sink.state_keys(&a.state_table_name, None).await.unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].dest_table, "orders_copy");
        assert_eq!(keys[1].last_watermark, "2024-01-02");
        assert!(keys.iter().all(|k| k.state_rows == 1));
        assert!(sink
            .state_keys(&a.state_table_name, Some(1))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(sink.compact_state(&a.state_table_name).await.unwrap(), 0);
        // Cleared (a recreated destination's cursor) reads as none.
        sink.persist_watermark(&a, "", 0).await.unwrap();
        assert_eq!(sink.read_last_watermark(&a).await.unwrap(), None);
        // Written whole, through a temporary file that doesn't stay.
        let names: Vec<String> = std::fs::read_dir(&tmp.0)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["_quickhouse_state.json"]);
        let raw: serde_json::Value =
            serde_json::from_slice(&std::fs::read(tmp.0.join("_quickhouse_state.json")).unwrap())
                .unwrap();
        assert_eq!(raw["keys"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn options_a_csv_destination_cannot_apply_are_refused_together() {
        let mut c = tcfg("t", SyncMode::Incremental);
        let err = refuse_unsupported_options(&c, false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("can't update"), "{err}");
        assert!(err.contains("mode=\"append\" with watermark="), "{err}");

        c.mode = SyncMode::Append;
        refuse_unsupported_options(&c, false).unwrap();
        // key is allowed: it only picks the column a parallel read splits on.
        c.key = vec!["id".into()];
        refuse_unsupported_options(&c, false).unwrap();
        c.engine = Some("MergeTree".into());
        c.order_by = vec!["id".into()];
        c.type_overrides.insert("id".into(), "String".into());
        c.evolve_schema = true;
        let err = refuse_unsupported_options(&c, true)
            .unwrap_err()
            .to_string();
        for option in [
            "engine=",
            "order_by=",
            "type_overrides=",
            "evolve_schema=",
            "validate=",
        ] {
            assert!(err.contains(option), "{option} missing from: {err}");
        }
        assert!(!err.contains("primary_key="), "{err}");

        let mut bad = tcfg("../escape", SyncMode::Full);
        assert!(refuse_unsupported_options(&bad, false).is_err());
        bad.dest_table = ".hidden".into();
        assert!(refuse_unsupported_options(&bad, false).is_err());
        bad.dest_table = "ok".into();
        bad.staging_suffix = "/x".into();
        assert!(refuse_unsupported_options(&bad, false).is_err());
    }

    #[test]
    fn a_bad_delimiter_or_path_is_refused() {
        assert!(check_delimiter(b'"').is_err());
        assert!(check_delimiter(b'\n').is_err());
        assert!(check_delimiter(b'\t').is_ok());
        let tmp = TempDir::new("file");
        let file = tmp.0.join("f");
        std::fs::write(&file, "x").unwrap();
        assert!(CsvSink::new(csv_cfg(&file)).is_err());
        let mut zero = csv_cfg(&tmp.0);
        zero.max_rows_per_file = 0;
        assert!(CsvSink::new(zero).is_err());
    }

    #[tokio::test]
    async fn each_stream_writes_parts_of_its_own() {
        let tmp = TempDir::new("streams");
        let sink = CsvSink::new(csv_cfg(&tmp.0)).unwrap();
        let cfg = tcfg("t", SyncMode::Full);
        sink.create_table("t_tmp", &cols(), &cfg).await.unwrap();
        // One after another, so a shared pool would reuse one part.
        for (stream, ids) in [("range-0", 0..3), ("range-1", 3..6), ("range-0", 6..9)] {
            let (schema, batch) = rows(ids);
            sink.insert_stream_batches("t_tmp", stream, schema, &[batch])
                .await
                .unwrap();
        }
        sink.atomic_swap("t", "t_tmp", &cols()).await.unwrap();
        let m = manifest(&tmp.0.join("t"));
        assert_eq!(m.parts.len(), 2);
        let mut per_part: Vec<u64> = m.parts.iter().map(|p| p.rows).collect();
        per_part.sort();
        assert_eq!(per_part, vec![3, 6]);
    }

    #[tokio::test]
    async fn parallel_inserts_write_parts_of_their_own() {
        let tmp = TempDir::new("parallel");
        let sink = Arc::new(CsvSink::new(csv_cfg(&tmp.0)).unwrap());
        let cfg = tcfg("t", SyncMode::Full);
        sink.create_table("t", &cols(), &cfg).await.unwrap();
        sink.create_table("t_tmp", &cols(), &cfg).await.unwrap();
        let mut tasks = Vec::new();
        for k in 0..4i64 {
            let sink = sink.clone();
            tasks.push(tokio::spawn(async move {
                for j in 0..5i64 {
                    let start = k * 1000 + j * 10;
                    let (schema, batch) = rows(start..start + 10);
                    sink.insert_batches("t_tmp", schema, &[batch])
                        .await
                        .unwrap();
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        sink.atomic_swap("t", "t_tmp", &cols()).await.unwrap();
        sink.drop_table("t_tmp").await.unwrap();
        let dir = tmp.0.join("t");
        let m = manifest(&dir);
        assert_eq!(m.rows, 200);
        assert!(!m.parts.is_empty());
        // Every row lands exactly once, whichever part took it.
        let mut ids: Vec<i64> = Vec::new();
        for p in part_files(&dir) {
            let text = std::fs::read_to_string(dir.join(p)).unwrap();
            let mut lines = text.lines();
            assert_eq!(lines.next(), Some("id,name"));
            ids.extend(lines.map(|l| l.split(',').next().unwrap().parse::<i64>().unwrap()));
        }
        ids.sort();
        let mut want: Vec<i64> = (0..4)
            .flat_map(|k| (0..50).map(move |i| k * 1000 + i))
            .collect();
        want.sort();
        assert_eq!(ids, want);
    }
}
