pub mod bigquery;
pub mod mysql;
pub mod postgres;

pub use bigquery::BigQuerySource;
pub use mysql::MySqlSource;
pub use postgres::{PgSource, Partition};

/// One chunk's keyset bound for resumable reads (see `TransferConfig::chunk_rows`).
/// Shared by both SQL sources: the reader appends `{col_quoted} > {cursor}`
/// (when resuming) plus `ORDER BY {col_quoted} ASC LIMIT {limit}` to the
/// SELECT, so each chunk reads the next `limit` rows past the cursor in key
/// order. `cursor` is a bare integer literal (the keyset column is gated to an
/// integer type), re-validated as `^-?[0-9]+$` before injection.
#[derive(Debug, Clone)]
pub struct Keyset {
    pub col_quoted: String,
    pub cursor: Option<String>,
    pub limit: usize,
}

/// Which database engine a transfer reads from. `sync.rs` matches on this to
/// dispatch to each engine's own connection/decode logic.
pub enum Source {
    Postgres(PgSource),
    MySql(MySqlSource),
    BigQuery(BigQuerySource),
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
    fn range_partitions_terminates_when_hi_is_i64_max() {
        // Regression test (bug report, HIGH severity): with the loop guard
        // narrowed back to i64, this used to hang forever and grow `parts`
        // without bound instead of returning. If this regresses, this test
        // hangs rather than fails cleanly — that itself is the proof the bug
        // is back.
        let parts = range_partitions(0, i64::MAX as i128, 4, "`id`");
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0].predicate.as_deref(), Some("`id` >= 0 AND `id` <= 2305843009213693951"));
        assert_eq!(
            parts[3].predicate.as_deref(),
            Some("`id` >= 6917529027641081856 AND `id` <= 9223372036854775807")
        );
    }

    #[test]
    fn range_partitions_covers_full_span_with_no_gaps_or_overlaps() {
        let parts = range_partitions(10, 25, 3, "col");
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].predicate.as_deref(), Some("col >= 10 AND col <= 15"));
        assert_eq!(parts[1].predicate.as_deref(), Some("col >= 16 AND col <= 21"));
        assert_eq!(parts[2].predicate.as_deref(), Some("col >= 22 AND col <= 25"));
    }
}
