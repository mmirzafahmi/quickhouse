# CleverTap fixtures

These are real vendor responses, captured from the live sg1 API and scrubbed by
`capture.py`. They settled a paging contract that was disputed for weeks, and
they exist so it can never quietly drift back.

## What was in dispute, and how it resolved

`crates/quickhouse-core/src/source/clevertap.rs` implemented one contract; a
production forensics audit (2026-08-28, `bi_bronze_api.clevertap_user_events`)
asserted a materially different one **for the same region**. Both had been
recorded as verified against the live API.

| | incumbent (commit `5ebd187`) | challenger (the audit) | **observed 2026-08-31** |
|---|---|---|---|
| next cursor on a **data** page | `cursor` | `next_cursor` | **`next_cursor`** (no `cursor` key at all) |
| `status` on a **non-final** page | `"partial"` | `"success"` | **`"success"`** (`"partial"` never seen) |
| how the chain ends | first `"success"` page, or first empty `records` | `next_cursor` absent | **`next_cursor` absent** |
| `{"status":"fail","code":2}` | fatal error | poll again | not reproduced — treated as retryable |
| rate limit | HTTP 429 | HTTP 200 + `fail` body | not reproduced — treated as retryable |

**The challenger was right on both disputed points.** There was also a third
defect neither account had noticed, and it was the worst of the three: the
source percent-encoded a cursor that arrives **already** percent-encoded, so
every page request came back `{"status":"fail","error":"Incorrect Usage",
"code":3}` and no page ever loaded at all.

Measured on the captured account, for one event-day: the incumbent rule would
have read **4,991 of 146,852 records — 3.40%** — and reported success. After
the fix, the same window reads **146,864 records across 32 pages, 100.00%
coverage** against an independent raw walk.

## Why one page was never enough

B1 (wrong cursor key) and B2 (`success` treated as terminal) **each
independently produce the identical symptom**: one page per event, then a
reported success. One observation cannot confirm two hypotheses, which is why
the termination rule was deliberately frozen rather than swapped for an equally
unverified one. What settled it was a **whole chain** — a non-final page *and*
the genuine terminal page.

The terminal page is worth seeing, because it is the entire safety property:

```json
{"status": "success"}
```

No `next_cursor`. No `records` key at all. A rule keyed on `status`, or on an
empty `records` array, cannot distinguish that from page 1.

## The files

| file | what it pins |
|---|---|
| `create_success.json` | the create response — keys the cursor `cursor`, the one place that spelling is real |
| `page_1.json` | a non-final data page: `next_cursor` present, `status` `"success"`, `records` an array |
| `page_mid.json` | the same, from the middle of the chain |
| `page_terminal.json` | the real end: `status` `"success"`, no `next_cursor`, no `records` |
| `capture.json` | the per-page facts for the whole 32-page walk |
| `*.headers` | status line + content type — the HTTP **200** is load-bearing for the throttle claim, so JSON alone would lose it |

`records` is truncated to two rows per page, with the true page size preserved
as `_records_total`. The paging contract lives in the envelope; committing 3 MB
per page to prove a cursor key would make the fixtures unreadable and their
diffs useless. One real record is kept so the decoder's dotted paths and the
packed `yyyyMMddHHmmSS` `ts` stay exercised against something the vendor
actually sent.

`crates/quickhouse-core/src/source/clevertap.rs`'s tests load these directly.
That is the point: the tests they replaced fed the parser hand-written JSON
encoding the same assumption as the code under test, so a green suite was worth
zero evidence about what the vendor sends. That is how this shipped.

## Re-capturing

```bash
export CLEVERTAP_ACCOUNT_ID=...
export CLEVERTAP_PASSCODE=...
python capture.py --event "App Launched" --date 2026-08-30 --region sg1 --pages 40
```

Pick a **busy** event and a **busy** day, and set `--pages` high enough to reach
the end of the chain — a middle page alone proves nothing about the exit
condition. `capture.py` scrubs before writing and refuses to write a file that
still trips its own leak checks:

- auth headers are never written (an allowlist of response headers, plus the status line)
- cursor values become `c1`/`c2`/`c3` — they encode account state
- `identity`, `email`, `phone`, `objectId`, device ids, push tokens and similar
  keys become `REDACTED`; email-shaped strings, long digit runs and opaque
  hex/base64 tokens are replaced **wherever they appear**, whatever the key is
  called (a real capture surfaced a 64-character `push_token` that every
  key-name rule waved through)
- long identifiers arriving as JSON *numbers* are zeroed, and floats rounded —
  both used to slip past a string-only scrubber and then trip the leak check
- a 14-digit run is left alone: that's the packed `ts`, and it's real signal

## The probe: score every rule in one run

`examples/clevertap_contract_probe.py` walks the **entire** chain tolerantly —
following whichever cursor key is present, ignoring `status` — then replays each
candidate termination rule over what it saw:

```
    rule                                    records   pages   % of chain
    incumbent (in clevertap.rs today)         4,991       1        3.40%
    challenger (the audit)                  146,852      32      100.00%
    tolerant (both keys, no status)         146,852      32      100.00%
```

That settles B1 and B2 *separately*, which a row count alone cannot.

`benchmarks/bench_clevertap.py` does the same walk as a ground-truth lane and
compares `sync()` against it, so a throughput number can never be reported for a
truncated read.

> A note on a shortcut that does **not** work: `HttpApi(next_cursor_path=...,
> cursor_param=...)` looks like it could express the challenger contract with no
> bespoke code, and its cursor handling is indeed exactly the challenger's. But
> `HttpApiSource` reuses one fixed method, url and body for every request in the
> chain (`source/http_api.rs`), whereas CleverTap needs a POST to create the
> export followed by GETs to page it. It cannot express the flow.

## Captured

| date captured | region | event | notes |
|---|---|---|---|
| 2026-08-31 | sg1 | `App Launched` (day 2026-08-30) | 32 pages, 146,852 records, `batch_size=5000`. Settled B1 + B2 and surfaced the double-encoding defect. No `code:2` or throttle response was seen during the walk. |
