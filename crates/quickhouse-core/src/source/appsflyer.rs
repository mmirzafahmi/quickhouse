//! AppsFlyer raw-data Pull API source.
//!
//! GET `{base}/api/raw-data/export/app/{app_id}/{report_type}/v5?from=&to=`
//! with `Authorization: Bearer <token>`; the response is a CSV report (header
//! row + data rows). The Pull API has hard daily-call and row caps — for high
//! volume, Data Locker (files in a bucket) is the vendor-recommended path.
//!
//! CRITICAL: AppsFlyer returns **HTTP 200 with a plain-text limit message**
//! (not CSV) when a cap is hit. Undetected, that becomes a 1-row garbage table
//! and a full refresh then TRUNCATES the real destination. `classify_appsflyer`
//! guards against this.

use std::collections::HashMap;
use std::time::Duration;

use serde_json::{Map, Value};

use crate::config::AppsFlyerConfig;
use crate::error::{EtlError, Result};

/// Build the v5 raw-data report URL with URL-encoded query params.
pub(crate) fn report_url(
    base_url: &str,
    app_id: &str,
    report_type: &str,
    from: &str,
    to: &str,
    extra: &HashMap<String, String>,
) -> Result<String> {
    let mut u = reqwest::Url::parse(&format!(
        "{base_url}/api/raw-data/export/app/{app_id}/{report_type}/v5"
    ))
    .map_err(|e| EtlError::config(format!("invalid AppsFlyer URL: {e}")))?;
    {
        let mut q = u.query_pairs_mut();
        q.append_pair("from", from);
        q.append_pair("to", to);
        // Deterministic order so the URL is stable/testable.
        let mut keys: Vec<&String> = extra.keys().collect();
        keys.sort();
        for k in keys {
            q.append_pair(k, &extra[k]);
        }
    }
    Ok(u.to_string())
}

pub(crate) enum ResponseClass {
    Ok,
    Transient(String),
    Permanent(String),
}

/// Matches AppsFlyer's cap/limit responses (case-insensitive) in the body head.
pub(crate) fn looks_like_limit_message(head: &str) -> bool {
    let h = head.to_ascii_lowercase();
    ["limit", "reached", "exceeded", "maximum number", "your api calls", "daily"]
        .iter()
        .any(|p| h.contains(p))
}

/// Classify a Pull API response. `head` is the first ~200 bytes of the body.
pub(crate) fn classify_appsflyer(status: u16, head: &str) -> ResponseClass {
    match status {
        200 => {
            if looks_like_limit_message(head) {
                // HTTP 200 + plain-text cap message — NOT data. Fail permanently
                // so a full refresh never truncates the dest with garbage.
                ResponseClass::Permanent(format!("AppsFlyer returned a limit/cap message (not CSV): {head}"))
            } else {
                ResponseClass::Ok
            }
        }
        401 | 403 => ResponseClass::Permanent(format!("AppsFlyer auth error {status}: {head}")),
        404 => ResponseClass::Permanent(format!("AppsFlyer 404 (check app_id/report_type): {head}")),
        429 => {
            if looks_like_limit_message(head) {
                ResponseClass::Permanent(format!("AppsFlyer daily/row cap reached (429): {head}"))
            } else {
                ResponseClass::Transient(format!("AppsFlyer rate-limited (429): {head}"))
            }
        }
        s if (500..=599).contains(&s) => ResponseClass::Transient(format!("AppsFlyer HTTP {s}: {head}")),
        s => ResponseClass::Permanent(format!("AppsFlyer HTTP {s}: {head}")),
    }
}

/// Parse a CSV report into flat JSON objects keyed by header (column-order
/// independent). Strips a UTF-8 BOM off the first header; validates every
/// `expected_headers` entry exists; ragged short rows null-fill.
pub(crate) fn csv_bytes_to_records(bytes: &[u8], expected_headers: &[String]) -> Result<Vec<Value>> {
    // Strip a leading UTF-8 BOM so the first header matches by name.
    let bytes = bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes);
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(bytes);
    let headers: Vec<String> = rdr
        .headers()
        .map_err(|e| EtlError::other(format!("AppsFlyer CSV header parse: {e}")))?
        .iter()
        .map(str::to_string)
        .collect();
    if headers.is_empty() {
        return Err(EtlError::other("AppsFlyer CSV had no header row".to_string()));
    }
    for want in expected_headers {
        if !headers.iter().any(|h| h == want) {
            return Err(EtlError::config(format!(
                "declared column maps to CSV header '{want}', which the AppsFlyer report doesn't \
                 have. Available headers: {}",
                headers.join(", ")
            )));
        }
    }
    let mut out = Vec::new();
    for rec in rdr.records() {
        let rec = rec.map_err(|e| EtlError::other(format!("AppsFlyer CSV row parse: {e}")))?;
        let mut obj = Map::with_capacity(headers.len());
        for (i, h) in headers.iter().enumerate() {
            let cell = rec.get(i).unwrap_or(""); // ragged short row -> ""
            obj.insert(h.clone(), Value::String(cell.to_string()));
        }
        out.push(Value::Object(obj));
    }
    Ok(out)
}

pub(crate) struct AppsFlyerSource {
    client: reqwest::Client,
    base_url: String,
    app_id: String,
    report_type: String,
    extra_params: HashMap<String, String>,
}

impl AppsFlyerSource {
    pub(crate) fn new(cfg: &AppsFlyerConfig) -> Result<Self> {
        use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
        let mut headers = HeaderMap::new();
        let mut auth = HeaderValue::from_str(&format!("Bearer {}", cfg.api_token))
            .map_err(|_| EtlError::config("invalid AppsFlyer api_token (non-header characters)"))?;
        auth.set_sensitive(true);
        headers.insert(AUTHORIZATION, auth);
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .gzip(true)
            .timeout(Duration::from_secs(300))
            .build()
            .map_err(|e| EtlError::other(format!("building AppsFlyer client: {e}")))?;
        Ok(Self {
            client,
            base_url: cfg.base_url.clone(),
            app_id: cfg.app_id.clone(),
            report_type: cfg.report_type.clone(),
            extra_params: cfg.extra_params.clone(),
        })
    }

    /// Fetch and parse the report for `[from, to]` (YYYY-MM-DD) into flat JSON
    /// records. Retries transient failures; surfaces caps as a clear error.
    pub(crate) async fn fetch_records(&self, from: &str, to: &str, expected_headers: &[String]) -> Result<Vec<Value>> {
        let url = report_url(&self.base_url, &self.app_id, &self.report_type, from, to, &self.extra_params)?;
        let max = crate::sink::MAX_INSERT_ATTEMPTS;
        let mut attempt = 1u32;
        loop {
            let resp = self.client.get(&url).send().await;
            match resp {
                Ok(r) => {
                    let status = r.status().as_u16();
                    let retry_after = r
                        .headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok());
                    let bytes = r.bytes().await.map_err(|e| EtlError::other(format!("AppsFlyer body read: {e}")))?;
                    let head = String::from_utf8_lossy(&bytes[..bytes.len().min(200)]).into_owned();
                    match classify_appsflyer(status, &head) {
                        ResponseClass::Ok => return csv_bytes_to_records(&bytes, expected_headers),
                        ResponseClass::Transient(msg) if attempt < max => {
                            let delay = retry_after
                                .map(Duration::from_secs)
                                .unwrap_or_else(|| crate::sink::backoff_delay(attempt));
                            tracing::warn!("{msg} (attempt {attempt}/{max}); retrying in {delay:?}");
                            tokio::time::sleep(delay).await;
                            attempt += 1;
                        }
                        ResponseClass::Transient(msg) | ResponseClass::Permanent(msg) => {
                            return Err(EtlError::other(msg))
                        }
                    }
                }
                Err(e) if attempt < max && (e.is_timeout() || e.is_connect() || e.is_request()) => {
                    let delay = crate::sink::backoff_delay(attempt);
                    tracing::warn!("AppsFlyer request error (attempt {attempt}/{max}): {e}; retrying in {delay:?}");
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                Err(e) => return Err(EtlError::other(format!("AppsFlyer request failed: {e}"))),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_url_builds_v5_path_with_sorted_params() {
        let mut extra = HashMap::new();
        extra.insert("timezone".to_string(), "UTC".to_string());
        extra.insert("maximum_rows".to_string(), "1000".to_string());
        let u = report_url("https://hq1.appsflyer.com", "id123", "installs_report", "2026-07-01", "2026-07-02", &extra).unwrap();
        assert!(u.starts_with("https://hq1.appsflyer.com/api/raw-data/export/app/id123/installs_report/v5?"), "{u}");
        assert!(u.contains("from=2026-07-01") && u.contains("to=2026-07-02"), "{u}");
        assert!(u.contains("maximum_rows=1000") && u.contains("timezone=UTC"), "{u}");
    }

    #[test]
    fn classify_guards_the_200_limit_body() {
        assert!(matches!(classify_appsflyer(200, "install_time,event_name\n"), ResponseClass::Ok));
        assert!(matches!(
            classify_appsflyer(200, "Your daily limit of API calls has been reached"),
            ResponseClass::Permanent(_)
        ));
        assert!(matches!(classify_appsflyer(429, "slow down"), ResponseClass::Transient(_)));
        assert!(matches!(classify_appsflyer(429, "maximum number of rows exceeded"), ResponseClass::Permanent(_)));
        assert!(matches!(classify_appsflyer(401, "bad token"), ResponseClass::Permanent(_)));
        assert!(matches!(classify_appsflyer(503, ""), ResponseClass::Transient(_)));
    }

    #[test]
    fn csv_parses_quotes_bom_ragged_and_validates_headers() {
        // BOM on first header + a quoted field with an embedded comma.
        let csv = "\u{feff}id,name\n1,\"Doe, John\"\n2,\n";
        let recs = csv_bytes_to_records(csv.as_bytes(), &["id".to_string(), "name".to_string()]).unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0]["id"], "1");
        assert_eq!(recs[0]["name"], "Doe, John", "quoted embedded comma preserved");
        assert_eq!(recs[1]["name"], "", "empty cell kept as empty string");
        // Missing declared header -> clear error listing available headers.
        let err = csv_bytes_to_records(csv.as_bytes(), &["nope".to_string()]).unwrap_err().to_string();
        assert!(err.contains("nope") && err.contains("id"), "{err}");
        // Header-only CSV -> 0 rows.
        assert_eq!(csv_bytes_to_records(b"a,b\n", &["a".to_string()]).unwrap().len(), 0);
    }
}
