//! CleverTap Data Export API source (events).
//!
//! Auth via `X-CleverTap-Account-Id` + `X-CleverTap-Passcode`; a region host
//! like `https://sg1.api.clevertap.com`. Flow: `POST /1/events.json?batch_size=N`
//! with `{"event_name","from":YYYYMMDD,"to":YYYYMMDD}` -> `{"status":"success",
//! "cursor":"…"}`; then `GET /1/events.json?cursor=…` for each page.
//!
//! # The paging contract, settled 2026-08-31
//!
//! This module previously implemented a contract that a production audit
//! disputed, and the disagreement was frozen for want of evidence. It is now
//! resolved against the live sg1 API, and the fixtures that settle it are
//! committed in `tests/fixtures/clevertap/` (captured 2026-08-31, event
//! "App Launched", 2026-08-30). The audit was right on both disputed points,
//! and there was a third defect neither account had noticed.
//!
//! **1. The cursor is already percent-encoded; send it verbatim.** A cursor
//! observed on that account is ~1,900 characters of alphanumerics plus `%2B`,
//! `%2F` and `%3D` escapes, and nothing else — it is URL-safe as it stands.
//! Encoding it again turns `%2B` into `%252B`, and the vendor answers every
//! such request with HTTP **200** carrying
//! `{"status":"fail","error":"Incorrect Usage","code":3}`. This module used to
//! encode it (`query_pairs_mut`), reasoning that an opaque token "routinely
//! contains `+`, `/` and `=`" — true of the *decoded* token, but the wire form
//! never carries those raw. The effect was total: **no page ever loaded**.
//!
//! **2. The next cursor is keyed `next_cursor` on a data page.** The *create*
//! response keys it `cursor`; data pages key it `next_cursor` and carry no
//! `cursor` key at all. Reading `cursor` on a data page always yielded `None`.
//!
//! **3. `status` is `"success"` on every page, final or not.** `"partial"` is
//! never sent. Treating `"success"` as terminal stops the chain after one page.
//!
//! **The chain ends when `next_cursor` is absent**, and only then. The terminal
//! page still says `"success"`.
//!
//! Defects 2 and 3 each independently produce the same symptom — one page, then
//! a reported success — which is why one short table could never say which was
//! at fault, and why both are fixed together on the strength of a captured
//! chain rather than one page. Measured on that account, the old rule would
//! have read **4,991 of 146,852 records (3.40%)** for a single event-day and
//! reported it clean.
//!
//! Two vendor states arrive as HTTP **200** with a `fail` body and are retried
//! rather than treated as fatal: `code: 2` ("export still materialising") and
//! an `error` of "Too many requests". Both are asserted by the audit; neither
//! was reproduced during capture, so they are handled defensively — bounded
//! retries against the same cursor, which costs nothing if they never occur.

use std::time::Duration;

use serde_json::Value;

use crate::config::CleverTapConfig;
use crate::error::{EtlError, Result};

/// Build a region host, rejecting anything not `^[a-z0-9]+$` (URL-injection /
/// SSRF guard). Reserved for a region-based constructor; the Python layer
/// derives the base URL directly, so this is currently exercised by tests.
#[allow(dead_code)]
pub(crate) fn clevertap_host(region: &str) -> Result<String> {
    if region.is_empty()
        || !region
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    {
        return Err(EtlError::config(format!(
            "invalid CleverTap region '{region}' (expected lowercase alphanumeric, e.g. sg1/us1/eu1)"
        )));
    }
    Ok(format!("https://{region}.api.clevertap.com"))
}

/// `"YYYY-MM-DD"` -> the `YYYYMMDD` integer CleverTap's from/to expects.
pub(crate) fn iso_to_yyyymmdd(d: &str) -> Result<u32> {
    let digits: String = d.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() != 8 {
        return Err(EtlError::config(format!(
            "CleverTap date '{d}' must be YYYY-MM-DD"
        )));
    }
    digits
        .parse::<u32>()
        .map_err(|e| EtlError::config(format!("bad date '{d}': {e}")))
}

pub(crate) fn create_export_body(event_name: &str, from: u32, to: u32) -> Value {
    serde_json::json!({ "event_name": event_name, "from": from, "to": to })
}

/// The data-page URL for `cursor`, with the cursor appended **verbatim**.
///
/// The cursor is an opaque vendor token that arrives *already* percent-encoded:
/// observed sg1 values are ~1,900 characters of alphanumerics plus `%2B`, `%2F`
/// and `%3D` escapes and nothing else, so the wire form is URL-safe as it
/// stands. Re-encoding it — which this function used to do via
/// `query_pairs_mut`, on the reasonable-sounding theory that a base64-ish token
/// "routinely contains `+`, `/` and `=`" — turns `%2B` into `%252B`, and the
/// vendor rejects every such request with HTTP 200 and
/// `{"status":"fail","error":"Incorrect Usage","code":3}`. That is not a
/// degraded read; it is no read at all.
///
/// `Url::parse` on the assembled string still validates the result, so a cursor
/// that ever *did* arrive with a raw `&` or space would fail loudly here rather
/// than silently truncating.
pub(crate) fn events_page_url(base_url: &str, cursor: &str) -> Result<reqwest::Url> {
    reqwest::Url::parse(&format!("{base_url}/1/events.json?cursor={cursor}")).map_err(|e| {
        EtlError::config(format!(
            "invalid CleverTap page URL for base_url '{base_url}': {e}"
        ))
    })
}

/// Parse the create-export response, returning the cursor or a clear error.
///
/// Note the asymmetry with a data page: the *create* response keys the cursor
/// `cursor`, while data pages key the next one `next_cursor`. Accept either
/// here, since only the create shape is in question.
pub(crate) fn parse_create_response(bytes: &[u8]) -> Result<String> {
    let v: Value = serde_json::from_slice(bytes)
        .map_err(|e| EtlError::other(format!("CleverTap create-export: bad JSON: {e}")))?;
    let status = v.get("status").and_then(|s| s.as_str()).unwrap_or("");
    if status != "success" {
        let err = v
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("unknown error");
        return Err(EtlError::other(format!(
            "CleverTap create-export failed: {err} (status={status})"
        )));
    }
    v.get("cursor")
        .or_else(|| v.get("next_cursor"))
        .and_then(|c| c.as_str())
        .map(str::to_string)
        .ok_or_else(|| {
            EtlError::other("CleverTap create-export: no cursor in success response".to_string())
        })
}

/// The `status` a page carried. **Not** a termination signal: the live API
/// sends `"success"` on every page, final or not, so the chain's end is decided
/// solely by the absence of a next cursor. Retained because it is worth logging
/// and because a future `"partial"` would be a contract change worth seeing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PageStatus {
    Partial,
    Success,
}

#[derive(Debug)]
pub(crate) struct EventsPage {
    pub status: PageStatus,
    /// The cursor for the *next* page, read from `next_cursor` (a data page's
    /// key) and falling back to `cursor`. `None` means this page is the last —
    /// the one and only termination signal.
    pub next_cursor: Option<String>,
    pub records: Vec<Value>,
}

/// What one page response turned out to be.
#[derive(Debug)]
pub(crate) enum PageOutcome {
    Page(EventsPage),
    /// The vendor answered HTTP 200 with a `fail` body that means "ask again",
    /// not "give up": the export is still materialising (`code: 2`), or we are
    /// being throttled ("Too many requests"). The caller retries the *same*
    /// cursor after a delay. Both states are asserted by the production audit
    /// and neither was reproduced during fixture capture, so treating them as
    /// retryable is the defensive reading — it costs a bounded wait if they
    /// never occur, and prevents a spurious hard failure if they do.
    Retry(String),
}

pub(crate) fn parse_events_page(bytes: &[u8]) -> Result<PageOutcome> {
    let v: Value = serde_json::from_slice(bytes)
        .map_err(|e| EtlError::other(format!("CleverTap events page: bad JSON: {e}")))?;
    let status = match v.get("status").and_then(|s| s.as_str()) {
        Some("partial") => PageStatus::Partial,
        Some("success") => PageStatus::Success,
        Some("fail") => {
            let code = v.get("code").and_then(|c| c.as_u64());
            let err = v
                .get("error")
                .map(|e| e.to_string())
                .unwrap_or_else(|| "no error field".to_string());
            // "still materialising" and "slow down" are both retryable.
            if code == Some(2) || err.to_ascii_lowercase().contains("too many requests") {
                return Ok(PageOutcome::Retry(format!(
                    "{err}{}",
                    code.map(|c| format!(" (code={c})")).unwrap_or_default()
                )));
            }
            return Err(EtlError::other(format!(
                "CleverTap events page failed: {err}{} — body: {}",
                code.map(|c| format!(" (code={c})")).unwrap_or_default(),
                crate::source::body_head(bytes, 200),
            )));
        }
        // Anything else is fatal here. Report every field the vendor gave us:
        // `code` in particular distinguishes the retryable states from the
        // permanent ones, and the previous message dropped it along with a
        // non-string `error`, rendering as `unexpected status 'fail' ` — an
        // empty trailing field and nothing to act on.
        Some(other) => {
            return Err(EtlError::other(format!(
                "CleverTap events page: unexpected status '{other}'{}{} — body: {}",
                v.get("code")
                    .map(|c| format!(" (code={c})"))
                    .unwrap_or_default(),
                v.get("error")
                    .map(|e| format!(" error={e}"))
                    .unwrap_or_default(),
                crate::source::body_head(bytes, 200),
            )));
        }
        None => {
            return Err(EtlError::other(format!(
                "CleverTap events page: missing status — body: {}",
                crate::source::body_head(bytes, 200)
            )))
        }
    };
    // `next_cursor` is what a data page uses; `cursor` is accepted as a
    // fallback so a create-shaped response (or a future contract change back)
    // still pages instead of silently ending the chain after one read.
    let next_cursor = v
        .get("next_cursor")
        .or_else(|| v.get("cursor"))
        .and_then(|c| c.as_str())
        .map(str::to_string);
    // `records` absent or null is a legitimately empty page; `records` present
    // as any *other* type is a protocol change, and coercing it to an empty Vec
    // (the previous `.and_then(as_array).unwrap_or_default()`) turned that into
    // an invisible zero-row page that the caller reports as a clean success.
    let records = match v.get("records") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(a)) => a.clone(),
        Some(other) => {
            return Err(EtlError::other(format!(
                "CleverTap events page: `records` is {}, expected an array — body: {}",
                json_type_name(other),
                crate::source::body_head(bytes, 200)
            )))
        }
    };
    Ok(PageOutcome::Page(EventsPage {
        status,
        next_cursor,
        records,
    }))
}
fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

enum HttpClass {
    Transient,
    Permanent,
}

fn classify_http_status(code: reqwest::StatusCode) -> HttpClass {
    if code.as_u16() == 429 || code.is_server_error() {
        HttpClass::Transient
    } else {
        HttpClass::Permanent
    }
}

/// How many times one page is re-requested while the vendor answers "ask
/// again" (export materialising, or throttled). Generous, because both states
/// resolve on their own and failing the transfer instead just means re-reading
/// the whole export later.
const MAX_PAGE_RETRY_ATTEMPTS: u32 = 8;

pub(crate) struct CleverTapSource {
    client: reqwest::Client,
    base_url: String,
    batch_size: u32,
}

impl CleverTapSource {
    pub(crate) fn new(cfg: &CleverTapConfig) -> Result<Self> {
        use reqwest::header::{HeaderMap, HeaderValue};
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-CleverTap-Account-Id",
            HeaderValue::from_str(&cfg.account_id).map_err(|_| {
                EtlError::config("invalid CleverTap account_id (non-header characters)")
            })?,
        );
        let mut pass = HeaderValue::from_str(&cfg.passcode)
            .map_err(|_| EtlError::config("invalid CleverTap passcode (non-header characters)"))?;
        pass.set_sensitive(true); // keep the passcode out of any header logging
        headers.insert("X-CleverTap-Passcode", pass);
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .gzip(true)
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(|e| EtlError::other(format!("building CleverTap client: {e}")))?;
        Ok(Self {
            client,
            base_url: cfg.base_url.clone(),
            batch_size: cfg.batch_size,
        })
    }

    /// Send a request with transient-retry/backoff, returning the response body.
    async fn send_bytes(&self, build: impl Fn() -> reqwest::RequestBuilder) -> Result<Vec<u8>> {
        let max = crate::sink::MAX_INSERT_ATTEMPTS;
        let mut attempt = 1u32;
        loop {
            let resp = build().send().await;
            match resp {
                Ok(r) => {
                    let status = r.status();
                    if status.is_success() {
                        return r
                            .bytes()
                            .await
                            .map(|b| b.to_vec())
                            .map_err(|e| EtlError::other(format!("CleverTap body read: {e}")));
                    }
                    let retry_after = r
                        .headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok());
                    let body = r.bytes().await.unwrap_or_default();
                    let head = crate::source::body_head(&body, 200);
                    match classify_http_status(status) {
                        HttpClass::Transient if attempt < max => {
                            let delay = retry_after
                                .map(Duration::from_secs)
                                .unwrap_or_else(|| crate::sink::backoff_delay(attempt));
                            tracing::warn!("CleverTap HTTP {status} (attempt {attempt}/{max}); retrying in {delay:?}");
                            tokio::time::sleep(delay).await;
                            attempt += 1;
                        }
                        _ => {
                            return Err(EtlError::other(format!("CleverTap HTTP {status}: {head}")))
                        }
                    }
                }
                Err(e) if attempt < max && (e.is_timeout() || e.is_connect() || e.is_request()) => {
                    let delay = crate::sink::backoff_delay(attempt);
                    tracing::warn!("CleverTap request error (attempt {attempt}/{max}): {e}; retrying in {delay:?}");
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                Err(e) => return Err(EtlError::other(format!("CleverTap request failed: {e}"))),
            }
        }
    }

    /// Create the export job for `[from, to]` (YYYYMMDD) and return the cursor.
    pub(crate) async fn create_export(
        &self,
        event_name: &str,
        from: u32,
        to: u32,
    ) -> Result<String> {
        let bs = if self.batch_size == 0 {
            5000
        } else {
            self.batch_size
        };
        let url = format!("{}/1/events.json?batch_size={bs}", self.base_url);
        let body = serde_json::to_vec(&create_export_body(event_name, from, to))
            .map_err(|e| EtlError::internal(format!("serialize CleverTap body: {e}")))?;
        let bytes = self
            .send_bytes(|| {
                self.client
                    .post(&url)
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(body.clone())
            })
            .await?;
        parse_create_response(&bytes)
    }

    /// Fetch the next page for `cursor`.
    ///
    /// Absorbs the two "ask again" states the vendor delivers as an HTTP 200
    /// with a `fail` body — the export still materialising, and the throttle —
    /// by re-requesting the *same* cursor after a backoff. `send_bytes` already
    /// handles the transport- and HTTP-status-level retries beneath this; these
    /// are invisible to it because the response is a 200.
    pub(crate) async fn next_page(&self, cursor: &str) -> Result<EventsPage> {
        let url = events_page_url(&self.base_url, cursor)?;
        let max = MAX_PAGE_RETRY_ATTEMPTS;
        let mut attempt = 1u32;
        loop {
            let bytes = self.send_bytes(|| self.client.get(url.clone())).await?;
            match parse_events_page(&bytes)? {
                PageOutcome::Page(p) => return Ok(p),
                PageOutcome::Retry(why) if attempt < max => {
                    let delay = crate::sink::backoff_delay(attempt);
                    tracing::warn!(
                        "CleverTap page not ready ({why}) (attempt {attempt}/{max}); \
                         retrying the same cursor in {delay:?}"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                PageOutcome::Retry(why) => {
                    return Err(EtlError::other(format!(
                        "CleverTap page still not ready after {max} attempts: {why}"
                    )))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The walk the driver performs, reduced to its cursor logic so the cycle
    /// rule can be tested without a vendor. Returns the cursors fetched.
    fn walk(chain: &[(&str, Option<&str>)], start: &str) -> Vec<String> {
        use std::collections::HashSet;
        use std::hash::{Hash, Hasher};
        let hash = |c: &str| {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            c.hash(&mut h);
            h.finish()
        };
        let mut seen: HashSet<u64> = HashSet::new();
        let mut cursor = start.to_string();
        seen.insert(hash(&cursor));
        let mut fetched = vec![cursor.clone()];
        let mut guard = 0;
        loop {
            guard += 1;
            assert!(
                guard < 100,
                "walk did not terminate — the cycle rule failed"
            );
            let next = chain
                .iter()
                .find(|(c, _)| *c == cursor)
                .and_then(|(_, n)| *n);
            let Some(next) = next else { break };
            if !seen.insert(hash(next)) {
                break;
            }
            cursor = next.to_string();
            fetched.push(cursor.clone());
        }
        fetched
    }

    #[test]
    fn a_chain_that_ends_by_omitting_the_cursor_reads_every_page() {
        let chain = [
            ("a", Some("b")),
            ("b", Some("c")),
            ("c", None), // the terminal page still says "success"
        ];
        assert_eq!(walk(&chain, "a"), ["a", "b", "c"]);
    }

    #[test]
    fn an_immediately_repeating_cursor_stops() {
        let chain = [("a", Some("a"))];
        assert_eq!(walk(&chain, "a"), ["a"]);
    }

    #[test]
    fn a_longer_cycle_also_stops() {
        // The case the previous `next == cursor` rule could not see: it compares
        // only consecutive pages, so a -> b -> a never tripped it and the loop
        // ran until the process was killed.
        let chain = [("a", Some("b")), ("b", Some("a"))];
        assert_eq!(walk(&chain, "a"), ["a", "b"]);
    }

    #[test]
    fn a_cycle_that_rejoins_further_back_also_stops() {
        let chain = [
            ("a", Some("b")),
            ("b", Some("c")),
            ("c", Some("d")),
            ("d", Some("b")), // rejoins three pages back
        ];
        assert_eq!(walk(&chain, "a"), ["a", "b", "c", "d"]);
    }

    #[test]
    fn host_validates_region() {
        assert_eq!(
            clevertap_host("sg1").unwrap(),
            "https://sg1.api.clevertap.com"
        );
        assert!(clevertap_host("../evil").is_err());
        assert!(clevertap_host("sg1.api.clevertap.com/x").is_err());
        assert!(clevertap_host("").is_err());
    }

    #[test]
    fn iso_to_yyyymmdd_parses() {
        assert_eq!(iso_to_yyyymmdd("2026-07-24").unwrap(), 20260724);
        assert!(iso_to_yyyymmdd("2026/07").is_err());
    }

    #[test]
    fn create_body_shape() {
        let b = create_export_body("App Launched", 20260724, 20260724);
        assert_eq!(b["event_name"], "App Launched");
        assert_eq!(b["from"], 20260724);
        assert_eq!(b["to"], 20260724);
    }

    #[test]
    fn parse_create_ok_and_failures() {
        assert_eq!(
            parse_create_response(br#"{"status":"success","cursor":"abc"}"#).unwrap(),
            "abc"
        );
        assert!(
            parse_create_response(br#"{"status":"fail","error":"Invalid Credentials"}"#)
                .unwrap_err()
                .to_string()
                .contains("Invalid Credentials")
        );
        assert!(
            parse_create_response(br#"{"status":"success"}"#).is_err(),
            "missing cursor"
        );
    }

    /// The captured pages that settled the paging contract. Unlike the
    /// hand-written JSON these tests used to feed the parser — which encoded
    /// exactly the same assumption as the code under test, so a green run was
    /// worth nothing — these are the real vendor responses, scrubbed by
    /// `tests/fixtures/clevertap/capture.py` (sg1, event "App Launched",
    /// 2026-08-30, captured 2026-08-31). Records are truncated to two and the
    /// original page size is preserved as `_records_total`; the envelope is
    /// verbatim.
    const CREATE: &str = include_str!("../../tests/fixtures/clevertap/create_success.json");
    const PAGE_1: &str = include_str!("../../tests/fixtures/clevertap/page_1.json");
    const PAGE_MID: &str = include_str!("../../tests/fixtures/clevertap/page_mid.json");
    const PAGE_TERMINAL: &str = include_str!("../../tests/fixtures/clevertap/page_terminal.json");

    fn page(s: &str) -> EventsPage {
        match parse_events_page(s.as_bytes()).expect("fixture parses") {
            PageOutcome::Page(p) => p,
            PageOutcome::Retry(why) => panic!("fixture unexpectedly classified retryable: {why}"),
        }
    }

    #[test]
    fn real_pages_key_the_next_cursor_as_next_cursor() {
        // Defect 1 of the shipped contract: the parser read `cursor` on a data
        // page. A real data page has no `cursor` key at all, so that always
        // yielded None and the chain ended after one page.
        for (name, raw) in [("page_1", PAGE_1), ("page_mid", PAGE_MID)] {
            let v: Value = serde_json::from_str(raw).unwrap();
            assert!(
                v.get("cursor").is_none(),
                "{name}: a real data page carries no `cursor` key"
            );
            assert!(
                v.get("next_cursor").is_some(),
                "{name}: expected next_cursor"
            );
            assert!(
                page(raw).next_cursor.is_some(),
                "{name}: the parser must follow next_cursor"
            );
        }
        // The *create* response is the asymmetry: it keys the cursor `cursor`.
        let v: Value = serde_json::from_str(CREATE).unwrap();
        assert!(
            v.get("cursor").is_some(),
            "create response keys it `cursor`"
        );
        assert_eq!(parse_create_response(CREATE.as_bytes()).unwrap(), "c1");
    }

    #[test]
    fn real_pages_say_success_even_when_they_are_not_the_last() {
        // Defect 2: `status == "success"` was treated as terminal. Every real
        // page says "success", including ones that carry a next cursor, so
        // that rule stopped every export after its first page.
        for (name, raw) in [
            ("page_1", PAGE_1),
            ("page_mid", PAGE_MID),
            ("page_terminal", PAGE_TERMINAL),
        ] {
            assert_eq!(
                page(raw).status,
                PageStatus::Success,
                "{name}: every real page reports success"
            );
        }
        assert!(
            page(PAGE_1).next_cursor.is_some(),
            "a `success` page that is NOT the last still carries a next cursor — \
             which is exactly why status cannot be the termination signal"
        );
    }

    #[test]
    fn the_terminal_page_is_the_one_with_no_next_cursor() {
        // The whole safety property, from the real end of a real chain: the
        // last page of a 32-page export is `{"status":"success"}` — no
        // next_cursor, and no `records` key at all.
        let p = page(PAGE_TERMINAL);
        assert_eq!(p.status, PageStatus::Success);
        assert!(
            p.next_cursor.is_none(),
            "terminal page must have no next cursor"
        );
        assert!(
            p.records.is_empty(),
            "the terminal page omits `records` entirely; that is an empty page, not an error"
        );
    }

    #[test]
    fn a_real_page_decodes_records_and_the_packed_timestamp() {
        let p = page(PAGE_1);
        assert!(!p.records.is_empty());
        let ts = p.records[0].get("ts").and_then(|t| t.as_u64()).unwrap();
        // Packed yyyyMMddHHmmSS, not epoch seconds — the mapping 0.6.1 had to
        // correct after it produced a 100%-NULL timestamp column.
        assert!(
            (20_260_830_000_000..20_260_831_000_000).contains(&ts),
            "expected a packed yyyyMMddHHmmSS for the captured day, got {ts}"
        );
        // The paths the example/docs advertise resolve against a real record.
        assert!(p.records[0].get("profile").is_some());
        assert!(p.records[0].get("event_props").is_some());
    }

    #[test]
    fn the_fixture_records_a_page_far_larger_than_it_stores() {
        // Guards the truncation: if a future capture forgets `_records_total`,
        // these fixtures silently stop describing a real page's size.
        let v: Value = serde_json::from_str(PAGE_1).unwrap();
        assert!(
            v.get("_records_total").and_then(|n| n.as_u64()).unwrap() > 1000,
            "page_1 should record the real page size it was truncated from"
        );
    }

    #[test]
    fn cursor_goes_into_the_page_url_verbatim() {
        // Defect 3, and the one that made this a total outage rather than a
        // truncation: CleverTap's cursor arrives ALREADY percent-encoded, so
        // encoding it again yields `%252B` and the vendor answers every such
        // request with HTTP 200 `{"status":"fail","error":"Incorrect Usage",
        // "code":3}`. Observed wire form is alphanumerics plus %2B/%2F/%3D.
        let cursor = "AbC123%2Bx%2Fy%3D";
        let u = events_page_url("https://sg1.api.clevertap.com", cursor).unwrap();
        assert_eq!(u.path(), "/1/events.json");
        assert_eq!(
            u.query().unwrap(),
            format!("cursor={cursor}"),
            "the cursor must reach the wire byte-for-byte as the vendor sent it"
        );
        // Decoding the query gives back the *decoded* token, which is what
        // proves we did not double-encode: %2B round-trips to '+', not to '%2B'.
        let pairs: Vec<_> = u.query_pairs().collect();
        assert_eq!(&*pairs[0].1, "AbC123+x/y=");
        // A malformed base URL is still a config error, not a panic.
        assert!(events_page_url("not a url", "c").is_err());
    }

    #[test]
    fn records_of_the_wrong_type_is_an_error_not_a_silent_empty_page() {
        // Coercing a non-array `records` to an empty Vec turns a vendor
        // protocol change into an invisible zero-row page, which the caller
        // then reports as a clean success. Fail loudly instead.
        for body in [
            &br#"{"status":"success","records":{}}"#[..],
            &br#"{"status":"success","records":"nope"}"#[..],
            &br#"{"status":"success","records":5}"#[..],
        ] {
            let e = parse_events_page(body).unwrap_err().to_string();
            assert!(e.contains("records"), "must name the offending key: {e}");
            assert!(e.contains("expected an array"), "{e}");
        }
        // Absent or explicitly null remains a legitimately empty page — which
        // is exactly what the real terminal page sends.
        for body in [
            &br#"{"status":"success"}"#[..],
            &br#"{"status":"success","records":null}"#[..],
        ] {
            match parse_events_page(body).unwrap() {
                PageOutcome::Page(p) => assert!(p.records.is_empty()),
                PageOutcome::Retry(w) => panic!("unexpected retry: {w}"),
            }
        }
    }

    #[test]
    fn the_vendors_ask_again_states_are_retried_not_fatal() {
        // Both arrive as HTTP 200 with a `fail` body. Treating them as fatal
        // turns a transient vendor state into a failed transfer.
        for body in [
            &br#"{"status":"fail","code":2}"#[..],
            &br#"{"status":"fail","error":"Too many requests"}"#[..],
            &br#"{"status":"fail","error":"TOO MANY REQUESTS"}"#[..],
        ] {
            match parse_events_page(body).unwrap() {
                PageOutcome::Retry(_) => {}
                PageOutcome::Page(_) => panic!("expected a retry for {body:?}"),
            }
        }
        // A `fail` that is neither remains a hard error, and names both fields.
        let e = parse_events_page(br#"{"status":"fail","error":"Incorrect Usage","code":3}"#)
            .unwrap_err()
            .to_string();
        assert!(e.contains("code=3"), "{e}");
        assert!(e.contains("Incorrect Usage"), "{e}");
    }

    #[test]
    fn page_errors_carry_code_and_error_for_diagnosis() {
        // A non-string `error` used to vanish entirely (`as_str()` -> None).
        let e = parse_events_page(br#"{"status":"fail","error":{"msg":"nested"}}"#)
            .unwrap_err()
            .to_string();
        assert!(e.contains("nested"), "{e}");
        // A missing status names the body too, so the log is actionable.
        let e = parse_events_page(br#"{"records":[]}"#)
            .unwrap_err()
            .to_string();
        assert!(e.contains("missing status"), "{e}");
        assert!(e.contains("records"), "body head should be echoed: {e}");
        // An unknown status is fatal, never an infinite loop.
        assert!(parse_events_page(br#"{"status":"weird"}"#).is_err());
    }
    #[test]
    fn malformed_bodies_error_and_never_panic() {
        for body in [
            &b""[..],
            &b"   "[..],
            &b"not json at all"[..],
            &br#"{"status":"success","records":[{"a":1}"#[..], // truncated
            &br#"[]"#[..],                                     // array, not object
            &br#"null"#[..],
            &br#"{"status":42}"#[..],    // non-string status must not panic
            &b"\xff\xfe\x00garbage"[..], // invalid UTF-8
        ] {
            assert!(
                parse_events_page(body).is_err(),
                "expected Err for {body:?}"
            );
        }
    }

    #[test]
    fn page_error_survives_a_multibyte_body() {
        // Regression: the error path used to slice the body at byte 200 with
        // `&body[..200]`, panicking whenever that offset landed inside a UTF-8
        // sequence — i.e. on any sufficiently long non-ASCII vendor message.
        //
        // The padding sweep is the point. With no padding the 200-byte cut
        // happens to land exactly on a codepoint boundary (26-byte prefix +
        // 174, and 174 is divisible by 3), so a single fixed body passes even
        // against the buggy slice. Shifting the prefix by 0..3 bytes forces
        // every possible alignment, including the two that split a codepoint.
        for pad in 0..3 {
            let body = format!(
                r#"{{"status":"fail","p":"{}","error":"{}"}}"#,
                "x".repeat(pad),
                "\u{7121}".repeat(300)
            );
            let e = parse_events_page(body.as_bytes()).unwrap_err().to_string();
            // Not panicking is the whole assertion; the message wording is
            // incidental (a `fail` body now routes through the dedicated
            // vendor-error arm rather than the unknown-status one).
            assert!(e.contains("CleverTap events page failed"), "pad={pad}: {e}");
            assert!(
                e.contains("body:"),
                "pad={pad}: the body head is the point: {e}"
            );
        }
    }
}
