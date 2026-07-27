//! Generic HTTP/REST + CSV API source.
//!
//! A config-driven source for arbitrary endpoints (the CleverTap/AppsFlyer
//! sources are purpose-built; this one is the escape hatch). It issues a
//! GET/POST with caller-supplied headers, substitutes `{from}`/`{to}` date
//! tokens into the URL/body, parses the response as JSON (a records array at a
//! dotted path) or CSV (reusing the AppsFlyer parser), and optionally follows a
//! response cursor for pagination. Records flow into the same [`ApiBatcher`]
//! (`decode_api`) as the other API sources.
//!
//! [`ApiBatcher`]: crate::decode_api::ApiBatcher

use std::time::Duration;

use serde_json::Value;

use crate::config::{HttpApiConfig, HttpFormat};
use crate::error::{EtlError, Result};

/// Replace `{from}` / `{to}` tokens in a URL or body template.
pub(crate) fn substitute_dates(template: &str, from: &str, to: &str) -> String {
    template.replace("{from}", from).replace("{to}", to)
}

/// Walk a dotted path (`"data.rows"`) through a JSON value; a missing key or a
/// non-object mid-walk yields `None`. An empty path returns the value itself.
fn resolve_path<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    if path.is_empty() {
        return Some(v);
    }
    let mut cur = v;
    for seg in path.split('.') {
        cur = cur.as_object()?.get(seg)?;
    }
    Some(cur)
}

/// Extract the record objects from a parsed JSON body. `records_path` (dotted)
/// locates the array; `None`/empty means the body itself is the array, or — if
/// it's a single object — that one object is the sole record.
pub(crate) fn extract_records(body: &Value, records_path: Option<&str>) -> Vec<Value> {
    let target = match resolve_path(body, records_path.unwrap_or("")) {
        Some(t) => t,
        None => return Vec::new(),
    };
    match target {
        Value::Array(items) => items.clone(),
        Value::Null => Vec::new(),
        other => vec![other.clone()], // a lone object -> one record
    }
}

/// Read a cursor value at a dotted path; `None` if absent/null/empty. Accepts a
/// string or a number (stringified), so `{"next":123}` paginates too.
pub(crate) fn extract_cursor(body: &Value, path: &str) -> Option<String> {
    match resolve_path(body, path)? {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
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

pub(crate) struct HttpApiSource {
    client: reqwest::Client,
    url: String,
    method: reqwest::Method,
    body: Option<String>,
    format: HttpFormat,
    next_cursor_path: Option<String>,
    cursor_param: Option<String>,
}

impl HttpApiSource {
    pub(crate) fn new(cfg: &HttpApiConfig) -> Result<Self> {
        use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
        let mut headers = HeaderMap::new();
        for (k, v) in &cfg.headers {
            let name = HeaderName::from_bytes(k.as_bytes())
                .map_err(|_| EtlError::config(format!("invalid HTTP header name '{k}'")))?;
            let mut val = HeaderValue::from_str(v)
                .map_err(|_| EtlError::config(format!("invalid value for HTTP header '{k}'")))?;
            val.set_sensitive(true); // headers often carry auth — keep them out of logs
            headers.insert(name, val);
        }
        let method = match cfg.method.to_ascii_uppercase().as_str() {
            "GET" => reqwest::Method::GET,
            "POST" => reqwest::Method::POST,
            other => {
                return Err(EtlError::config(format!(
                    "unsupported HTTP method '{other}' (expected GET or POST)"
                )))
            }
        };
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .gzip(true)
            .timeout(Duration::from_secs(300))
            .build()
            .map_err(|e| EtlError::other(format!("building HTTP client: {e}")))?;
        Ok(Self {
            client,
            url: cfg.url.clone(),
            method,
            body: cfg.body.clone(),
            format: cfg.format.clone(),
            next_cursor_path: cfg.next_cursor_path.clone(),
            cursor_param: cfg.cursor_param.clone(),
        })
    }

    /// One request (URL + optional body + optional cursor query param), with
    /// transient-retry/backoff; returns the raw response body.
    async fn send_bytes(
        &self,
        url: &str,
        body: Option<&str>,
        cursor: Option<&str>,
    ) -> Result<Vec<u8>> {
        let max = crate::sink::MAX_INSERT_ATTEMPTS;
        let mut attempt = 1u32;
        loop {
            let mut req = self.client.request(self.method.clone(), url);
            if let (Some(param), Some(c)) = (&self.cursor_param, cursor) {
                req = req.query(&[(param.as_str(), c)]);
            }
            if let Some(b) = body {
                req = req.body(b.to_string());
            }
            match req.send().await {
                Ok(r) => {
                    let status = r.status();
                    if status.is_success() {
                        return r
                            .bytes()
                            .await
                            .map(|b| b.to_vec())
                            .map_err(|e| EtlError::other(format!("HTTP body read: {e}")));
                    }
                    let retry_after = r
                        .headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok());
                    let text = r.text().await.unwrap_or_default();
                    let head = text[..text.len().min(200)].to_string();
                    match classify_http_status(status) {
                        HttpClass::Transient if attempt < max => {
                            let delay = retry_after
                                .map(Duration::from_secs)
                                .unwrap_or_else(|| crate::sink::backoff_delay(attempt));
                            tracing::warn!(
                                "HTTP {status} (attempt {attempt}/{max}); retrying in {delay:?}"
                            );
                            tokio::time::sleep(delay).await;
                            attempt += 1;
                        }
                        _ => return Err(EtlError::other(format!("HTTP {status}: {head}"))),
                    }
                }
                Err(e) if attempt < max && (e.is_timeout() || e.is_connect() || e.is_request()) => {
                    let delay = crate::sink::backoff_delay(attempt);
                    tracing::warn!(
                        "HTTP request error (attempt {attempt}/{max}): {e}; retrying in {delay:?}"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                Err(e) => return Err(EtlError::other(format!("HTTP request failed: {e}"))),
            }
        }
    }

    /// Fetch every record for `[from, to]`, following the response cursor if one
    /// is configured (JSON only; CSV is a single request).
    pub(crate) async fn fetch_records(
        &self,
        from: &str,
        to: &str,
        expected_headers: &[String],
    ) -> Result<Vec<Value>> {
        let url = substitute_dates(&self.url, from, to);
        let body = self.body.as_ref().map(|b| substitute_dates(b, from, to));
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let bytes = self
                .send_bytes(&url, body.as_deref(), cursor.as_deref())
                .await?;
            match &self.format {
                HttpFormat::Csv => {
                    out.extend(crate::source::appsflyer::csv_bytes_to_records(
                        &bytes,
                        expected_headers,
                    )?);
                    break; // CSV: no cursor pagination
                }
                HttpFormat::Json { records_path } => {
                    let v: Value = serde_json::from_slice(&bytes)
                        .map_err(|e| EtlError::other(format!("HTTP JSON parse: {e}")))?;
                    out.extend(extract_records(&v, records_path.as_deref()));
                    // Follow the cursor if configured; stop when it's absent or
                    // stops changing (a defensive no-progress guard).
                    match (&self.next_cursor_path, &self.cursor_param) {
                        (Some(path), Some(_)) => match extract_cursor(&v, path) {
                            Some(next) if Some(&next) != cursor.as_ref() => cursor = Some(next),
                            _ => break,
                        },
                        _ => break,
                    }
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitute_dates_replaces_both_tokens() {
        let u = substitute_dates(
            "https://x/api?from={from}&to={to}",
            "2026-07-01",
            "2026-07-02",
        );
        assert_eq!(u, "https://x/api?from=2026-07-01&to=2026-07-02");
        // Repeated tokens all replaced.
        assert_eq!(substitute_dates("{from}/{from}", "a", "b"), "a/a");
    }

    #[test]
    fn extract_records_handles_path_array_and_lone_object() {
        // Array at a dotted path.
        let body = serde_json::json!({"data": {"rows": [{"id": 1}, {"id": 2}]}});
        assert_eq!(extract_records(&body, Some("data.rows")).len(), 2);
        // Top-level array (no path).
        let arr = serde_json::json!([{"id": 1}]);
        assert_eq!(extract_records(&arr, None).len(), 1);
        // A lone object with no path -> one record.
        let obj = serde_json::json!({"id": 9});
        let recs = extract_records(&obj, None);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0]["id"], 9);
        // Missing path -> empty (not an error).
        assert!(extract_records(&body, Some("nope.missing")).is_empty());
    }

    #[test]
    fn extract_cursor_reads_string_or_number_and_stops_on_empty() {
        assert_eq!(
            extract_cursor(
                &serde_json::json!({"paging": {"next": "abc"}}),
                "paging.next"
            )
            .as_deref(),
            Some("abc")
        );
        assert_eq!(
            extract_cursor(&serde_json::json!({"next": 123}), "next").as_deref(),
            Some("123")
        );
        assert!(extract_cursor(&serde_json::json!({"next": ""}), "next").is_none());
        assert!(extract_cursor(&serde_json::json!({"next": null}), "next").is_none());
        assert!(extract_cursor(&serde_json::json!({}), "next").is_none());
    }
}
