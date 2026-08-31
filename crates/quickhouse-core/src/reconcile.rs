//! Keyset reconciliation: measure — and optionally repair — the drift between a
//! source table and a destination that has been synced from it.
//!
//! # Why this exists
//!
//! An incremental sync is insert-and-update only. It finds rows whose watermark
//! moved and upserts them; a row the source *deleted* has no watermark to move,
//! so nothing about it ever reaches the destination again. The destination keeps
//! it forever.
//!
//! That is not a corner case for any source that hard-deletes as a matter of
//! routine — an ERP cancelling a reservation, a queue draining, a soft-delete
//! that is really a `DELETE`. The drift is strictly one-directional (the
//! destination can only ever hold *more* distinct keys than the live source) and
//! it grows without bound. It is also lopsided in a way that hides it: a
//! measured production table here carried +1.30% phantom rows against +0.0168%
//! on a quantity sum, so every COUNT-based model over-reported materially while
//! every SUM-based one looked fine.
//!
//! [`TransferConfig::delete_stale_in_window`](crate::config::TransferConfig::delete_stale_in_window)
//! prevents the drift going forward, inside a sync, for the window that sync
//! touched. This module is the other half: it answers "how far apart are these
//! two right now?" for an arbitrary window, on its own schedule, and repairs the
//! difference only when asked. Measuring is the part worth running continuously;
//! deleting is the part worth approving.
//!
//! # What it does
//!
//! 1. Read every distinct key the source holds in the window, as text.
//! 2. Read every distinct key the destination holds in the same window.
//! 3. Diff the two sets, reporting **orphans** (in the destination, gone from
//!    the source — the drift) and **missing** keys (in the source, absent from
//!    the destination — ordinary sync lag, or a genuinely incomplete load).
//! 4. If [`ReconcileConfig::delete`] is set, delete the orphans and report how
//!    many rows went.
//!
//! Both keysets are held in memory, so bound the window to something sane — this
//! is a keyset, not the rows, but a hundred million keys is still a hundred
//! million `String`s.

use std::collections::HashSet;
use std::time::Instant;

use crate::config::{DestinationConfig, SourceConfig};
use crate::error::{EtlError, Result};
use crate::sink::build_sink;
use crate::source::{MySqlSource, PgSource};

/// What to reconcile, and whether to repair it.
#[derive(Debug, Clone)]
pub struct ReconcileConfig {
    /// Source table to read keys from. Mutually exclusive with
    /// [`Self::source_query`]; exactly one is required.
    pub source_table: Option<String>,
    /// Source query to read keys from, in place of a table. Must project a
    /// column named [`Self::key`].
    pub source_query: Option<String>,
    /// Destination table to diff against.
    pub dest_table: String,
    /// The identifying column, present on both sides under the same name.
    ///
    /// **Single column, and realistically an integer or string.** Both sides
    /// render their keys as text so the diff can be a set comparison, and the
    /// two renderings only agree for types with one obvious text form. An
    /// integer id (the normal case) round-trips exactly; a timestamp does not
    /// (PostgreSQL writes `2026-08-30 12:00:00+00`, ClickHouse writes
    /// `2026-08-30 12:00:00`), and every key would read as drift in both
    /// directions. That total-mismatch shape is detected and refused rather
    /// than acted on — see [`reconcile_keys`] — but pick a sane key and it
    /// never comes up.
    pub key: String,
    /// SQL predicate bounding the source read, in the source's dialect —
    /// e.g. `"create_date >= '2026-07-01' AND create_date < '2026-08-01'"`.
    /// `None` reads the whole table.
    pub window: Option<String>,
    /// The same bound for the destination, in the *destination's* dialect.
    /// Defaults to [`Self::window`], which is right whenever the destination
    /// mirrors the source's column names and the predicate is portable.
    pub dest_window: Option<String>,
    /// Delete the orphans. `false` (the default worth having) measures only.
    pub delete: bool,
    /// Refuse to delete when the orphan count exceeds this. `0` = no ceiling.
    ///
    /// A reconcile is only as good as its window: a predicate that means
    /// something different on each side, or a key that renders differently on
    /// each side, both look like "almost everything is an orphan". This is the
    /// blunt guard against acting on that — set it to a few times the drift you
    /// actually expect.
    pub max_delete_keys: u64,
    /// How many orphan/missing keys to carry back as samples. Diagnostic; the
    /// counts are always exact regardless of this.
    pub sample_limit: usize,
}

impl Default for ReconcileConfig {
    fn default() -> Self {
        Self {
            source_table: None,
            source_query: None,
            dest_table: String::new(),
            key: String::new(),
            window: None,
            dest_window: None,
            delete: false,
            max_delete_keys: 0,
            sample_limit: 10,
        }
    }
}

/// What the diff found, and what was done about it.
#[derive(Debug, Clone, Default)]
pub struct ReconcileResult {
    /// Distinct keys the source holds in the window.
    pub source_keys: u64,
    /// Distinct keys the destination holds in the window.
    pub dest_keys: u64,
    /// Keys in the destination that the source no longer has — the drift.
    pub orphan_keys: u64,
    /// Keys in the source that the destination does not have. Usually ordinary
    /// sync lag; a large number here means the load itself is incomplete, which
    /// is a different problem from the one this module repairs.
    pub missing_keys: u64,
    /// Destination rows actually deleted. `0` unless [`ReconcileConfig::delete`]
    /// was set. Can exceed `orphan_keys` where the destination holds more than
    /// one row per key (an un-merged `ReplacingMergeTree`, for one).
    pub rows_deleted: u64,
    /// Up to `sample_limit` orphan keys, for a message a human can act on.
    pub orphan_sample: Vec<String>,
    /// Up to `sample_limit` missing keys.
    pub missing_sample: Vec<String>,
    pub duration_secs: f64,
}

impl ReconcileConfig {
    fn validate(&self) -> Result<()> {
        if self.dest_table.trim().is_empty() {
            return Err(EtlError::config("reconcile_keys requires dest_table"));
        }
        if self.key.trim().is_empty() {
            return Err(EtlError::config("reconcile_keys requires key"));
        }
        match (&self.source_table, &self.source_query) {
            (Some(_), Some(_)) => Err(EtlError::config(
                "reconcile_keys takes source_table or source_query, not both",
            )),
            (None, None) => Err(EtlError::config(
                "reconcile_keys requires source_table or source_query",
            )),
            _ => Ok(()),
        }?;
        // A delete with no window is "delete everything the source does not
        // currently have", evaluated against a keyset read at a slightly
        // different instant. That is not a reconcile, it is a full refresh with
        // a race in it — refuse rather than let a missing argument mean it.
        if self.delete && self.effective_dest_window().is_none() {
            return Err(EtlError::config(
                "reconcile_keys with delete=True requires a window (an unbounded delete would \
                 remove every destination key the source does not hold at this instant, which \
                 is a full refresh with a race in it, not a reconcile)",
            ));
        }
        Ok(())
    }

    fn effective_dest_window(&self) -> Option<&str> {
        self.dest_window.as_deref().or(self.window.as_deref())
    }
}

/// Diff a source table's keyset against a destination's, and optionally delete
/// the destination rows the source no longer has. See the module docs.
///
/// Sources: PostgreSQL and MySQL. A BigQuery or HTTP-API source is rejected —
/// the first is normally itself a mirror rather than the system of record, and
/// the second has no keyset query to speak of. Destinations: both ClickHouse
/// and BigQuery.
pub async fn reconcile_keys(
    source_cfg: SourceConfig,
    dest: DestinationConfig,
    cfg: ReconcileConfig,
) -> Result<ReconcileResult> {
    // Same one-time rustls provider selection every entry point needs; see
    // `sync::run_transfer_impl` for why it lives at the entry point.
    let _ = rustls::crypto::ring::default_provider().install_default();
    cfg.validate()?;
    let started = Instant::now();

    let sink = build_sink(dest).await?;
    if cfg.delete && !sink.supports_row_delete() {
        return Err(EtlError::config(
            "this destination cannot delete individual rows, so reconcile_keys can only \
             measure against it (delete=False)",
        ));
    }

    let source_keys = read_source_keys(&source_cfg, &cfg).await?;
    tracing::info!(
        "reconcile: source '{}' holds {} distinct '{}' value(s) in the window",
        cfg.source_table
            .as_deref()
            .or(cfg.source_query.as_deref())
            .unwrap_or("?"),
        source_keys.len(),
        cfg.key,
    );
    let dest_keys = sink
        .distinct_keys(&cfg.dest_table, &cfg.key, cfg.effective_dest_window())
        .await?;
    tracing::info!(
        "reconcile: destination '{}' holds {} distinct '{}' value(s) in the window",
        cfg.dest_table,
        dest_keys.len(),
        cfg.key,
    );

    let source_set: HashSet<&str> = source_keys.iter().map(String::as_str).collect();
    let dest_set: HashSet<&str> = dest_keys.iter().map(String::as_str).collect();
    let orphans: Vec<String> = dest_keys
        .iter()
        .filter(|k| !source_set.contains(k.as_str()))
        .cloned()
        .collect();
    let missing: Vec<String> = source_keys
        .iter()
        .filter(|k| !dest_set.contains(k.as_str()))
        .cloned()
        .collect();

    let mut result = ReconcileResult {
        source_keys: source_set.len() as u64,
        dest_keys: dest_set.len() as u64,
        orphan_keys: orphans.len() as u64,
        missing_keys: missing.len() as u64,
        rows_deleted: 0,
        orphan_sample: orphans.iter().take(cfg.sample_limit).cloned().collect(),
        missing_sample: missing.iter().take(cfg.sample_limit).cloned().collect(),
        duration_secs: 0.0,
    };

    // Total disagreement in both directions is not drift — drift is
    // one-directional by construction. It is the signature of two keysets that
    // were never comparable: a key column whose text rendering differs between
    // the engines, or a window predicate that selects different rows on each
    // side. Refuse rather than report 100% drift, and certainly rather than
    // delete on it.
    if !source_set.is_empty()
        && !dest_set.is_empty()
        && orphans.len() == dest_set.len()
        && missing.len() == source_set.len()
    {
        return Err(EtlError::config(format!(
            "reconcile found NO keys in common between source and destination ({} vs {} \
             distinct '{}' value(s), zero overlap). Genuine drift is one-directional, so this \
             is almost certainly the two sides rendering the same key differently, or the \
             window predicate selecting different rows on each side — not {} deletable \
             orphans. Source sample: {:?}; destination sample: {:?}",
            source_set.len(),
            dest_set.len(),
            cfg.key,
            dest_set.len(),
            source_keys.iter().take(3).collect::<Vec<_>>(),
            dest_keys.iter().take(3).collect::<Vec<_>>(),
        )));
    }

    if cfg.delete && !orphans.is_empty() {
        if cfg.max_delete_keys > 0 && result.orphan_keys > cfg.max_delete_keys {
            return Err(EtlError::config(format!(
                "reconcile found {} orphan key(s) in '{}', above the max_delete_keys ceiling of \
                 {}; refusing to delete. Widen the ceiling deliberately, or narrow the window.",
                result.orphan_keys, cfg.dest_table, cfg.max_delete_keys,
            )));
        }
        result.rows_deleted = sink
            .delete_keys(
                &cfg.dest_table,
                &cfg.key,
                &orphans,
                cfg.effective_dest_window(),
            )
            .await?;
        tracing::info!(
            "reconcile: deleted {} row(s) from '{}' for {} orphan key(s)",
            result.rows_deleted,
            cfg.dest_table,
            result.orphan_keys,
        );
    } else if !orphans.is_empty() {
        tracing::warn!(
            "reconcile: '{}' holds {} key(s) the source no longer has (delete=False, so \
             nothing was removed). Sample: {:?}",
            cfg.dest_table,
            result.orphan_keys,
            result.orphan_sample,
        );
    }
    if !missing.is_empty() {
        tracing::warn!(
            "reconcile: the source holds {} key(s) '{}' does not — ordinary sync lag if small, \
             an incomplete load if not. Sample: {:?}",
            result.missing_keys,
            cfg.dest_table,
            result.missing_sample,
        );
    }

    result.duration_secs = started.elapsed().as_secs_f64();
    Ok(result)
}

/// Read the source's keyset over the window, per source engine.
async fn read_source_keys(source_cfg: &SourceConfig, cfg: &ReconcileConfig) -> Result<Vec<String>> {
    match source_cfg {
        SourceConfig::Postgres(pg) => {
            let source = PgSource::new(
                pg.dsn.clone(),
                pg.statement_timeout_secs,
                pg.ca_cert_file.clone(),
                pg.client_cert_file.clone(),
                pg.client_key_file.clone(),
                "quickhouse-reconcile".to_string(),
            );
            let client = source.connect().await?;
            source
                .distinct_keys(
                    &client,
                    cfg.source_table.as_deref(),
                    cfg.source_query.as_deref(),
                    &cfg.key,
                    cfg.window.as_deref(),
                )
                .await
        }
        SourceConfig::MySql(my) => {
            let source = MySqlSource::new(
                my.dsn.clone(),
                my.statement_timeout_secs,
                my.ca_cert_file.clone(),
                my.require_tls,
                my.client_cert_file.clone(),
                my.client_key_file.clone(),
            );
            let mut conn = source.connect().await?;
            source
                .distinct_keys(
                    &mut conn,
                    cfg.source_table.as_deref(),
                    cfg.source_query.as_deref(),
                    &cfg.key,
                    cfg.window.as_deref(),
                )
                .await
        }
        other => Err(EtlError::config(format!(
            "reconcile_keys does not support a {} source (PostgreSQL and MySQL only): a \
             BigQuery source is normally itself a mirror rather than the system of record, \
             and an HTTP API source has no keyset query to diff against",
            other.kind(),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> ReconcileConfig {
        ReconcileConfig {
            source_table: Some("t".into()),
            dest_table: "t".into(),
            key: "id".into(),
            ..Default::default()
        }
    }

    #[test]
    fn requires_a_destination_and_a_key() {
        let mut c = cfg();
        c.dest_table = String::new();
        assert!(c.validate().unwrap_err().to_string().contains("dest_table"));
        let mut c = cfg();
        c.key = String::new();
        assert!(c.validate().unwrap_err().to_string().contains("key"));
    }

    #[test]
    fn requires_exactly_one_source_shape() {
        let mut c = cfg();
        c.source_query = Some("SELECT id FROM t".into());
        assert!(c.validate().unwrap_err().to_string().contains("not both"));
        let mut c = cfg();
        c.source_table = None;
        assert!(c
            .validate()
            .unwrap_err()
            .to_string()
            .contains("source_table or source_query"));
    }

    #[test]
    fn a_delete_without_a_window_is_refused() {
        // The whole safety property of the delete is that it is scoped; an
        // unbounded one is a full refresh with a race in it.
        let mut c = cfg();
        c.delete = true;
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains("requires a window"), "{err}");
        // Either window field satisfies it, since dest_window defaults to window.
        c.window = Some("created_at >= '2026-07-01'".into());
        assert!(c.validate().is_ok());
        c.window = None;
        c.dest_window = Some("created_at >= '2026-07-01'".into());
        assert!(c.validate().is_ok());
        // ...and measuring without one stays allowed.
        c.dest_window = None;
        c.delete = false;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn dest_window_falls_back_to_window() {
        let mut c = cfg();
        c.window = Some("a = 1".into());
        assert_eq!(c.effective_dest_window(), Some("a = 1"));
        c.dest_window = Some("b = 2".into());
        assert_eq!(c.effective_dest_window(), Some("b = 2"));
    }
}
