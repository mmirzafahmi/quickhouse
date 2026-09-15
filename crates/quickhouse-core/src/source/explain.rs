//! Ask the query planner what a probe will cost, before running it.
//!
//! quickhouse issues two setup-phase probes against the watermark column on
//! every incremental run — a `MAX(watermark)` snapshot bound and, on a nullable
//! column, a `count(*) WHERE watermark IS NULL` completeness check. Both are
//! cheap when an index serves them and catastrophic when one does not, and
//! nothing used to check which case applied.
//!
//! Measured against a PostgreSQL 16 hot standby:
//!
//! | probe | indexed | unindexed |
//! |---|---|---|
//! | `count(*) WHERE wm IS NULL` | cost 2.07 | cost 3,031,034 (56.65s) |
//! | `MAX(wm)` | cost 0.65 | cost 3,031,091 (57.19s) |
//!
//! Three of six such probes were *cancelled outright* by the standby's
//! `max_standby_streaming_delay` (`SQLSTATE 40001`), which ends the transfer.
//!
//! **Why the planner rather than the catalog.** An earlier attempt read
//! `pg_index` to find which columns lead a btree. That cannot see through a
//! `source_query` — there is no base table to look up — and a `source_query` is
//! how most non-trivial transfers are written, so the check was inert exactly
//! where it was needed. `EXPLAIN` has no such blind spot: it plans the real
//! query, derived tables and expressions and partial indexes included, and
//! reports what will actually happen. Without `ANALYZE` it plans only — no rows
//! are read, so asking is free and carries no cancellation risk of its own.
//!
//! **Why cost rather than plan shape.** A MySQL probe on `user_order_payment`
//! reports `access_type: ref` — nominally an indexed access — at a cost of
//! 3,951,736. "Is it a full scan?" waves that through; "what does it cost?"
//! does not.

/// What a planner said about one probe query.
#[derive(Debug, Clone, PartialEq)]
pub enum ProbeCost {
    /// The planner returned an estimate.
    Known(f64),
    /// The plan carried no cost but did report how it reaches rows. MySQL omits
    /// `cost_info` for shapes its optimizer resolves away (`MAX()` over an
    /// indexed column) and for some derived-table queries. Absent cost is not
    /// evidence of cheapness, so the access path decides instead.
    AccessPath { full_scan: bool },
    /// The planner could not be asked — EXPLAIN failed, was itself cancelled,
    /// or returned something unparseable.
    Unknown,
}

impl ProbeCost {
    /// Should the probe be skipped?
    ///
    /// `max_cost` of `0` disables the gate entirely: every probe runs, which is
    /// the behaviour from before this module existed.
    ///
    /// [`ProbeCost::Unknown`] skips. That is deliberate and is the inverse of
    /// an earlier bug: when the planner cannot be reached — often because the
    /// standby just cancelled the EXPLAIN under recovery conflict — the
    /// expensive full-scan query is the one thing that must not then be
    /// attempted. Treating "don't know" as "cheap" maximises load at exactly
    /// the moment the server is least able to absorb it.
    pub fn should_skip(&self, max_cost: f64) -> bool {
        if max_cost <= 0.0 {
            return false;
        }
        match self {
            ProbeCost::Known(c) => *c > max_cost,
            ProbeCost::AccessPath { full_scan } => *full_scan,
            ProbeCost::Unknown => true,
        }
    }

    /// Short description of the evidence, for the warning message — an operator
    /// reading "estimated cost 3031034" can act on it; "skipped" alone cannot
    /// be distinguished from a guess.
    pub fn describe(&self) -> String {
        match self {
            ProbeCost::Known(c) => format!("planner estimated cost {c:.0}"),
            ProbeCost::AccessPath { full_scan: true } => {
                "planner reported a full table scan (no cost estimate available)".to_string()
            }
            ProbeCost::AccessPath { full_scan: false } => {
                "planner reported an indexed access path (no cost estimate available)".to_string()
            }
            ProbeCost::Unknown => {
                "the planner could not be asked (EXPLAIN failed or was cancelled)".to_string()
            }
        }
    }
}

/// Total cost from `EXPLAIN (FORMAT JSON)` output.
///
/// The document is an array with a single element holding `"Plan"`, whose
/// `"Total Cost"` is the estimate for the whole statement.
pub(crate) fn parse_pg_cost(json: &str) -> ProbeCost {
    let v: serde_json::Value = match serde_json::from_str(json) {
        Ok(v) => v,
        Err(_) => return ProbeCost::Unknown,
    };
    let plan = v
        .get(0)
        .and_then(|e| e.get("Plan"))
        .or_else(|| v.get("Plan"));
    match plan
        .and_then(|p| p.get("Total Cost"))
        .and_then(|c| c.as_f64())
    {
        Some(c) => ProbeCost::Known(c),
        None => ProbeCost::Unknown,
    }
}

/// Query cost from `EXPLAIN FORMAT=JSON` output.
///
/// `query_block.cost_info.query_cost` when present. When it is not — the
/// optimizer resolved the query away, or it is a derived-table shape MySQL
/// does not cost — fall back to whether any `access_type` in the tree is
/// `ALL`, which is MySQL's marker for a full table scan.
pub(crate) fn parse_mysql_cost(json: &str) -> ProbeCost {
    let v: serde_json::Value = match serde_json::from_str(json) {
        Ok(v) => v,
        Err(_) => return ProbeCost::Unknown,
    };
    let qb = match v.get("query_block") {
        Some(q) => q,
        None => return ProbeCost::Unknown,
    };
    let cost = qb
        .get("cost_info")
        .and_then(|c| c.get("query_cost"))
        // MySQL renders query_cost as a JSON *string* ("1624649.10"), not a
        // number — reading it as f64 alone silently yields None.
        .and_then(|c| {
            c.as_f64()
                .or_else(|| c.as_str().and_then(|s| s.parse::<f64>().ok()))
        });
    if let Some(c) = cost {
        return ProbeCost::Known(c);
    }
    let mut saw_access = false;
    let mut full_scan = false;
    walk_access_types(qb, &mut |t| {
        saw_access = true;
        if t == "ALL" {
            full_scan = true;
        }
    });
    if saw_access {
        ProbeCost::AccessPath { full_scan }
    } else {
        ProbeCost::Unknown
    }
}

/// Visit every `access_type` string anywhere in a MySQL plan tree. The shape
/// nests differently per query form (`table`, `nested_loop`, `ordering_operation`,
/// `materialized_from_subquery`, …), so walk generically rather than encode
/// one layout.
fn walk_access_types(v: &serde_json::Value, f: &mut impl FnMut(&str)) {
    match v {
        serde_json::Value::Object(map) => {
            for (k, val) in map {
                if k == "access_type" {
                    if let Some(s) = val.as_str() {
                        f(s);
                    }
                }
                walk_access_types(val, f);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                walk_access_types(item, f);
            }
        }
        _ => {}
    }
}

/// `EXPLAIN (FORMAT JSON) <sql>` — never `ANALYZE`, so nothing is executed.
pub(crate) fn pg_explain_sql(sql: &str) -> String {
    format!("EXPLAIN (FORMAT JSON) {sql}")
}

/// `EXPLAIN FORMAT=JSON <sql>` — likewise plan-only.
pub(crate) fn mysql_explain_sql(sql: &str) -> String {
    format!("EXPLAIN FORMAT=JSON {sql}")
}

/// Cost above which a setup-phase probe is skipped rather than run.
///
/// Sits between the two regimes measured on real hardware: every indexed probe
/// observed cost between 0.65 and 8.49, every unindexed one between 1.6M and
/// 11.2M. That is four orders of magnitude of headroom below and one and a half
/// above, so the exact value is not delicate.
pub const DEFAULT_PROBE_MAX_COST: f64 = 50_000.0;

/// Neither engine's cost units are wall-clock seconds, and they are not
/// comparable across engines — PostgreSQL counts notional page fetches, MySQL
/// its own composite. The default is chosen to separate the two *measured*
/// regimes per engine, not to mean anything absolute.
#[cfg(test)]
mod tests {
    use super::*;

    // Verbatim shape of `EXPLAIN (FORMAT JSON)` for the unindexed
    // count(*) probe on a 56.7 GB Odoo mail_message.
    const PG_SEQ_SCAN: &str = r#"[
      {
        "Plan": {
          "Node Type": "Aggregate",
          "Total Cost": 11046344.72,
          "Plans": [
            {"Node Type": "Gather", "Total Cost": 11046344.00,
             "Plans": [{"Node Type": "Seq Scan", "Total Cost": 11044953.94}]}
          ]
        }
      }
    ]"#;

    const PG_INDEX_ONLY: &str = r#"[
      {"Plan": {"Node Type": "Aggregate", "Total Cost": 2.06,
                "Plans": [{"Node Type": "Index Only Scan", "Total Cost": 2.05}]}}
    ]"#;

    #[test]
    fn pg_cost_is_the_root_total() {
        assert_eq!(parse_pg_cost(PG_SEQ_SCAN), ProbeCost::Known(11046344.72));
        assert_eq!(parse_pg_cost(PG_INDEX_ONLY), ProbeCost::Known(2.06));
    }

    #[test]
    fn pg_garbage_is_unknown_not_zero() {
        // Unknown must never read as "cheap" — see ProbeCost::should_skip.
        assert_eq!(parse_pg_cost("not json"), ProbeCost::Unknown);
        assert_eq!(parse_pg_cost("[]"), ProbeCost::Unknown);
        assert_eq!(parse_pg_cost(r#"[{"Plan":{}}]"#), ProbeCost::Unknown);
    }

    #[test]
    fn mysql_cost_is_read_from_a_json_string() {
        // MySQL renders query_cost as a string, not a number. Reading it as f64
        // alone silently yields None and would mis-report every plan as costless.
        let j = r#"{"query_block":{"cost_info":{"query_cost":"1624649.10"},
                    "table":{"access_type":"ALL"}}}"#;
        assert_eq!(parse_mysql_cost(j), ProbeCost::Known(1624649.10));
    }

    #[test]
    fn mysql_numeric_cost_also_parses() {
        let j = r#"{"query_block":{"cost_info":{"query_cost":8.49}}}"#;
        assert_eq!(parse_mysql_cost(j), ProbeCost::Known(8.49));
    }

    #[test]
    fn mysql_without_cost_falls_back_to_access_type() {
        // Measured: MySQL omits cost_info for MAX() over an indexed column
        // (resolved away) and for some derived-table shapes.
        let full = r#"{"query_block":{"table":{"access_type":"ALL"}}}"#;
        assert_eq!(
            parse_mysql_cost(full),
            ProbeCost::AccessPath { full_scan: true }
        );
        let indexed = r#"{"query_block":{"table":{"access_type":"ref"}}}"#;
        assert_eq!(
            parse_mysql_cost(indexed),
            ProbeCost::AccessPath { full_scan: false }
        );
    }

    #[test]
    fn mysql_finds_access_type_nested_anywhere() {
        let nested = r#"{"query_block":{"ordering_operation":{"nested_loop":[
            {"table":{"access_type":"ref"}},
            {"table":{"access_type":"ALL"}}]}}}"#;
        assert_eq!(
            parse_mysql_cost(nested),
            ProbeCost::AccessPath { full_scan: true }
        );
    }

    #[test]
    fn mysql_empty_plan_is_unknown() {
        assert_eq!(
            parse_mysql_cost(r#"{"query_block":{}}"#),
            ProbeCost::Unknown
        );
        assert_eq!(parse_mysql_cost("{}"), ProbeCost::Unknown);
        assert_eq!(parse_mysql_cost("nonsense"), ProbeCost::Unknown);
    }

    #[test]
    fn the_gate_separates_the_two_measured_regimes() {
        let d = DEFAULT_PROBE_MAX_COST;
        // Every indexed probe measured on real hardware.
        for cheap in [0.64, 0.65, 2.06, 2.07, 8.49] {
            assert!(
                !ProbeCost::Known(cheap).should_skip(d),
                "indexed probe at cost {cheap} must still run"
            );
        }
        // Every unindexed one.
        for dear in [1_624_649.10, 3_031_034.0, 3_951_736.65, 11_046_344.72] {
            assert!(
                ProbeCost::Known(dear).should_skip(d),
                "unindexed probe at cost {dear} must be skipped"
            );
        }
    }

    #[test]
    fn unknown_skips_rather_than_runs() {
        // The inverse of the earlier bug: a cancelled EXPLAIN meant "run the
        // full scan and say nothing". A planner we cannot reach is the worst
        // moment to attempt the expensive query.
        assert!(ProbeCost::Unknown.should_skip(DEFAULT_PROBE_MAX_COST));
        assert!(ProbeCost::AccessPath { full_scan: true }.should_skip(DEFAULT_PROBE_MAX_COST));
        assert!(!ProbeCost::AccessPath { full_scan: false }.should_skip(DEFAULT_PROBE_MAX_COST));
    }

    #[test]
    fn zero_threshold_disables_the_gate_entirely() {
        // The documented escape hatch: probe unconditionally, as before.
        assert!(!ProbeCost::Known(11_046_344.72).should_skip(0.0));
        assert!(!ProbeCost::Unknown.should_skip(0.0));
        assert!(!ProbeCost::AccessPath { full_scan: true }.should_skip(0.0));
    }

    #[test]
    fn explain_is_never_analyze() {
        // ANALYZE would execute the very query we are trying to avoid.
        let pg = pg_explain_sql("SELECT count(*) FROM t");
        assert_eq!(pg, "EXPLAIN (FORMAT JSON) SELECT count(*) FROM t");
        assert!(!pg.to_uppercase().contains("ANALYZE"));
        let my = mysql_explain_sql("SELECT count(*) FROM t");
        assert_eq!(my, "EXPLAIN FORMAT=JSON SELECT count(*) FROM t");
        assert!(!my.to_uppercase().contains("ANALYZE"));
    }

    #[test]
    fn describe_names_the_evidence() {
        assert!(ProbeCost::Known(3_031_034.0).describe().contains("3031034"));
        assert!(ProbeCost::Unknown.describe().contains("could not be asked"));
        assert!(ProbeCost::AccessPath { full_scan: true }
            .describe()
            .contains("full table scan"));
    }
}
