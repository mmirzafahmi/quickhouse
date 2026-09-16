pub mod appsflyer;
pub mod bigquery;
pub mod clevertap;
pub mod clickhouse;
pub mod explain;
pub mod http_api;
pub mod mysql;
pub mod postgres;

pub use bigquery::BigQuerySource;
pub use clickhouse::ClickHouseSource;
pub use explain::{
    ProbeCost, DEFAULT_PROBE_MAX_COST, DEFAULT_READ_WINDOW_ROWS, DEFAULT_WINDOW_TARGET_SECS,
    INITIAL_READ_WINDOW_ROWS, MIN_READ_WINDOW_ROWS,
};
pub use mysql::MySqlSource;
pub use postgres::{Partition, PgSource};

/// How a keyset read is bounded above.
///
/// The two forms exist because they bound *different things*, and confusing
/// them is what makes an unindexed read fail:
///
/// * [`Self::OrderedLimit`] bounds the number of **matching rows returned**.
///   The server must keep scanning until it has found that many, so when the
///   other predicates are selective and unindexed the work is unbounded — a
///   `LIMIT 1000` over a 130M-row table can walk the whole table to find its
///   thousand rows.
/// * [`Self::UpperBound`] bounds the **key range examined**. With an index on
///   the key the work is proportional to the range, whatever the rest of the
///   WHERE clause selects, which makes each read's duration predictable — the
///   property that lets a read finish inside a hot standby's
///   `max_standby_streaming_delay` instead of being cancelled.
#[derive(Debug, Clone)]
pub enum KeysetBound {
    /// `ORDER BY {col} ASC LIMIT {n}` — chunked resumable reads
    /// (`TransferConfig::chunk_rows`).
    OrderedLimit(usize),
    /// `{col} <= {hi}` — one window of a bounded sweep. No sort is emitted:
    /// windows are disjoint and stepped in order by the driver, so the server
    /// has nothing to order.
    UpperBound(String),
}

/// One keyset-bounded read. Shared by both SQL sources: the reader appends
/// `{col_quoted} > {cursor}` (when resuming or mid-sweep) plus whatever
/// [`KeysetBound`] specifies. `cursor` and any bound are bare integer literals
/// (the keyset column is gated to an integer type), re-validated as
/// `^-?[0-9]+$` before injection.
#[derive(Debug, Clone)]
pub struct Keyset {
    pub col_quoted: String,
    pub cursor: Option<String>,
    pub bound: KeysetBound,
}

/// The WHERE fragment a [`Keyset`] contributes: the exclusive lower bound from
/// `cursor` (when set) AND, for a window, the inclusive upper bound. Returns
/// `None` when the keyset constrains nothing — the first chunk of an
/// ordered-limit read, whose bound lives in the `LIMIT` instead.
///
/// Shared by both SQL builders so the two engines cannot drift on the
/// half-open `(lo, hi]` convention, which is what guarantees a sweep covers
/// every key exactly once with no gap and no overlap.
pub(crate) fn keyset_predicate(k: &Keyset) -> Option<String> {
    let lower = k
        .cursor
        .as_ref()
        .map(|cur| format!("{} > {}", k.col_quoted, cur));
    let upper = match &k.bound {
        KeysetBound::UpperBound(hi) => Some(format!("{} <= {}", k.col_quoted, hi)),
        KeysetBound::OrderedLimit(_) => None,
    };
    match (lower, upper) {
        (Some(l), Some(u)) => Some(format!("{l} AND {u}")),
        (Some(l), None) => Some(l),
        (None, Some(u)) => Some(u),
        (None, None) => None,
    }
}

/// Which database engine a transfer reads from. `sync.rs` matches on this to
/// dispatch to each engine's own connection/decode logic.
pub enum Source {
    Postgres(PgSource),
    MySql(MySqlSource),
    BigQuery(BigQuerySource),
    ClickHouse(ClickHouseSource),
}

/// The first `max` **bytes** of a response body, rendered for an error message.
///
/// Slicing a `String`/`&str` at a fixed byte offset panics when that offset
/// lands inside a multi-byte UTF-8 sequence, so `&body[..200]` on a vendor
/// error message containing any non-ASCII character crashes the process
/// instead of producing the error it was building. Truncating the *bytes* and
/// then lossily decoding cannot panic and cannot split a codepoint.
pub(crate) fn body_head(bytes: &[u8], max: usize) -> String {
    let end = bytes.len().min(max);
    let mut s = String::from_utf8_lossy(&bytes[..end]).into_owned();
    if bytes.len() > end {
        s.push('\u{2026}');
    }
    s
}

/// Split the inclusive range `[lo, hi]` into up to `n` roughly-equal
/// partitions, each rendered as a `"{quoted_col} >= start AND {quoted_col} <=
/// end"` predicate over the already-quoted column expression. Shared by
/// `mysql::MySqlSource::range_partitions` and
/// `postgres::PgSource::range_partitions` — the two engines only differ in
/// how they resolve `lo`/`hi` and quote the column.
///
/// `lo`/`hi` are `i128` specifically so the loop's own termination check
/// never narrows back to `i64`: when the true column max is exactly
/// `i64::MAX`, the next candidate `start` (`i64::MAX + 1`, fine in `i128`)
/// used to get cast back down to `i64` for the loop guard, wrapping to
/// `i64::MIN` and making the guard `i64::MIN <= i64::MAX` — always true, an
/// infinite loop with unboundedly growing output (see the regression test).
pub(crate) fn range_partitions(lo: i128, hi: i128, n: usize, quoted_col: &str) -> Vec<Partition> {
    let span = (hi - lo + 1) as u128;
    let n = n as u128;
    let step = span.div_ceil(n).max(1);

    let mut parts = Vec::new();
    let mut start = lo;
    let mut idx = 0u128;
    while start <= hi {
        let end = (start + step as i128 - 1).min(hi);
        parts.push(Partition {
            label: format!("range-{idx}"),
            predicate: Some(format!("{quoted_col} >= {start} AND {quoted_col} <= {end}")),
        });
        start = end + 1;
        idx += 1;
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_head_never_panics_on_multibyte_boundaries() {
        // The bug this guards: `&body[..body.len().min(200)]` on a String
        // panics when byte 200 falls inside a UTF-8 sequence. A vendor error
        // message in any non-Latin script did exactly that.
        let body = "\u{00d7}".repeat(300); // 2 bytes/char -> byte 201 splits one
        let _ = body_head(body.as_bytes(), 201);
        // A cut landing mid-sequence yields the replacement char, not a panic.
        let cut = body_head("h\u{00e9}llo".as_bytes(), 2);
        assert!(cut.starts_with('h'), "{cut:?}");
        // Short bodies pass through untouched, with no ellipsis.
        assert_eq!(body_head(b"short", 200), "short");
        assert_eq!(body_head(b"", 200), "");
        // Truncation is marked.
        assert!(body_head(b"abcdef", 3).starts_with("abc"));
        assert!(body_head(b"abcdef", 3).ends_with('\u{2026}'));
        // Invalid UTF-8 degrades rather than panicking.
        assert!(!body_head(&[0xff, 0xfe, 0x00], 200).is_empty());
    }

    #[test]
    fn range_partitions_terminates_when_hi_is_i64_max() {
        // Regression test (bug report, HIGH severity): with the loop guard
        // narrowed back to i64, this used to hang forever and grow `parts`
        // without bound instead of returning. If this regresses, this test
        // hangs rather than fails cleanly — that itself is the proof the bug
        // is back.
        let parts = range_partitions(0, i64::MAX as i128, 4, "`id`");
        assert_eq!(parts.len(), 4);
        assert_eq!(
            parts[0].predicate.as_deref(),
            Some("`id` >= 0 AND `id` <= 2305843009213693951")
        );
        assert_eq!(
            parts[3].predicate.as_deref(),
            Some("`id` >= 6917529027641081856 AND `id` <= 9223372036854775807")
        );
    }

    #[test]
    fn range_partitions_covers_full_span_with_no_gaps_or_overlaps() {
        let parts = range_partitions(10, 25, 3, "col");
        assert_eq!(parts.len(), 3);
        assert_eq!(
            parts[0].predicate.as_deref(),
            Some("col >= 10 AND col <= 15")
        );
        assert_eq!(
            parts[1].predicate.as_deref(),
            Some("col >= 16 AND col <= 21")
        );
        assert_eq!(
            parts[2].predicate.as_deref(),
            Some("col >= 22 AND col <= 25")
        );
    }
}
