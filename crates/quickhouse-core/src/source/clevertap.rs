//! CleverTap Data Export API source (events).
//!
//! Contract (verified live against the sg1 region): auth via
//! `X-CleverTap-Account-Id` + `X-CleverTap-Passcode`; a region host like
//! `https://sg1.api.clevertap.com`. Flow: `POST /1/events.json?batch_size=N`
//! with `{"event_name","from":YYYYMMDD,"to":YYYYMMDD}` -> `{"status":"success",
//! "cursor":"…"}`; then `GET /1/events.json?cursor=…` -> `{"status":"partial"|
//! "success","cursor":"…","records":[…]}`; keep paging while `status=="partial"`
//! and records keep coming, processing the terminal `success` page too.

use std::time::Duration;

use serde_json::Value;

use crate::config::CleverTapConfig;
use crate::error::{EtlError, Result};

/// Build a region host, rejecting anything not `^[a-z0-9]+$` (URL-injection /
/// SSRF guard). Reserved for a region-based constructor; the Python layer
/// derives the base URL directly, so this is currently exercised by tests.
#[allow(dead_code)]
pub(crate) fn clevertap_host(region: &str) -> Result<String> {
    if region.is_empty() || !region.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()) {
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
    digits.parse::<u32>().map_err(|e| EtlError::config(format!("bad date '{d}': {e}")))
}

pub(crate) fn create_export_body(event_name: &str, from: u32, to: u32) -> Value {
    serde_json::json!({ "event_name": event_name, "from": from, "to": to })
}

/// Parse the create-export response, returning the cursor or a clear error.
pub(crate) fn parse_create_response(bytes: &[u8]) -> Result<String> {
    let v: Value = serde_json::from_slice(bytes)
        .map_err(|e| EtlError::other(format!("CleverTap create-export: bad JSON: {e}")))?;
    let status = v.get("status").and_then(|s| s.as_str()).unwrap_or("");
    if status != "success" {
        let err = v.get("error").and_then(|e| e.as_str()).unwrap_or("unknown error");
        return Err(EtlError::other(format!("CleverTap create-export failed: {err} (status={status})")));
    }
    v.get("cursor")
        .and_then(|c| c.as_str())
        .map(str::to_string)
        .ok_or_else(|| EtlError::other("CleverTap create-export: no cursor in success response".to_string()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PageStatus {
    Partial,
    Success,
}

pub(crate) struct EventsPage {
    pub status: PageStatus,
    pub cursor: Option<String>,
    pub records: Vec<Value>,
}

pub(crate) fn parse_events_page(bytes: &[u8]) -> Result<EventsPage> {
    let v: Value = serde_json::from_slice(bytes)
        .map_err(|e| EtlError::other(format!("CleverTap events page: bad JSON: {e}")))?;
    let status = match v.get("status").and_then(|s| s.as_str()) {
        Some("partial") => PageStatus::Partial,
        Some("success") => PageStatus::Success,
        Some(other) => {
            let err = v.get("error").and_then(|e| e.as_str()).unwrap_or("");
            return Err(EtlError::other(format!(
                "CleverTap events page: unexpected status '{other}' {err}"
            )));
        }
        None => return Err(EtlError::other("CleverTap events page: missing status".to_string())),
    };
    let cursor = v.get("cursor").and_then(|c| c.as_str()).map(str::to_string);
    let records = v
        .get("records")
        .and_then(|r| r.as_array())
        .cloned()
        .unwrap_or_default();
    Ok(EventsPage { status, cursor, records })
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
            HeaderValue::from_str(&cfg.account_id)
                .map_err(|_| EtlError::config("invalid CleverTap account_id (non-header characters)"))?,
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
        Ok(Self { client, base_url: cfg.base_url.clone(), batch_size: cfg.batch_size })
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
                    let body = r.text().await.unwrap_or_default();
                    let head = &body[..body.len().min(200)];
                    match classify_http_status(status) {
                        HttpClass::Transient if attempt < max => {
                            let delay = retry_after
                                .map(Duration::from_secs)
                                .unwrap_or_else(|| crate::sink::backoff_delay(attempt));
                            tracing::warn!("CleverTap HTTP {status} (attempt {attempt}/{max}); retrying in {delay:?}");
                            tokio::time::sleep(delay).await;
                            attempt += 1;
                        }
                        _ => return Err(EtlError::other(format!("CleverTap HTTP {status}: {head}"))),
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
    pub(crate) async fn create_export(&self, event_name: &str, from: u32, to: u32) -> Result<String> {
        let bs = if self.batch_size == 0 { 5000 } else { self.batch_size };
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
    pub(crate) async fn next_page(&self, cursor: &str) -> Result<EventsPage> {
        let url = format!("{}/1/events.json?cursor={cursor}", self.base_url);
        let bytes = self.send_bytes(|| self.client.get(&url)).await?;
        parse_events_page(&bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_validates_region() {
        assert_eq!(clevertap_host("sg1").unwrap(), "https://sg1.api.clevertap.com");
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
        assert_eq!(parse_create_response(br#"{"status":"success","cursor":"abc"}"#).unwrap(), "abc");
        assert!(parse_create_response(br#"{"status":"fail","error":"Invalid Credentials"}"#)
            .unwrap_err()
            .to_string()
            .contains("Invalid Credentials"));
        assert!(parse_create_response(br#"{"status":"success"}"#).is_err(), "missing cursor");
    }

    #[test]
    fn parse_page_retains_terminal_success_records() {
        let p = parse_events_page(br#"{"status":"partial","cursor":"n","records":[{"a":1}]}"#).unwrap();
        assert_eq!(p.status, PageStatus::Partial);
        assert_eq!(p.cursor.as_deref(), Some("n"));
        assert_eq!(p.records.len(), 1);
        // The terminal success page STILL carries records — must be processed.
        let s = parse_events_page(br#"{"status":"success","cursor":"z","records":[{"a":2},{"a":3}]}"#).unwrap();
        assert_eq!(s.status, PageStatus::Success);
        assert_eq!(s.records.len(), 2);
        // Unknown status -> error (never an infinite loop).
        assert!(parse_events_page(br#"{"status":"weird"}"#).is_err());
    }
}
