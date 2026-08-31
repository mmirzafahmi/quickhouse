#!/usr/bin/env python3
"""Capture and scrub real CleverTap Data Export pages into test fixtures.

WHY THIS EXISTS
---------------
`source/clevertap.rs` implements one paging contract; a 2026-08-28 production
audit asserts a materially different one for the same region. Both are recorded
as "verified live". Neither is reproducible from this repository, because there
is no captured page — every unit test feeds the parser hand-written JSON that
encodes the same assumption as the code it tests, so a green suite proves
nothing about the vendor.

The two claims differ on things a single response settles outright:

  * which key carries the next cursor on a DATA page (`cursor` vs `next_cursor`)
  * whether `status` reads "partial" or "success" on a NON-FINAL page
  * how the final page signals that it is final
  * whether `{"status":"fail","code":2}` is fatal or means "poll again"
  * whether a rate limit arrives as HTTP 429 or as HTTP 200 with a fail body

This script captures those responses, scrubs them, and prints the verdict.

USAGE
-----
    export CLEVERTAP_ACCOUNT_ID=...
    export CLEVERTAP_PASSCODE=...
    python capture.py --event "App Launched" --date 2026-08-27 --region sg1

Add `--pages N` to walk further down the chain (default 3, which is the minimum
that distinguishes "non-final" from "final"). Nothing is written until the
scrubber has run and the leak assertions have passed.

WHAT IT WRITES
--------------
    create_success.json   the create-export response
    page_1.json           first data page
    page_mid.json         a middle page, if the chain is long enough
    page_terminal.json    the last page reached
    <name>.headers        status line + response headers for each of the above
    capture.json          machine-readable summary of the verdict

SAFETY
------
Auth headers are never written. Cursors are replaced with c1/c2/c3 because they
encode account state. Anything resembling an email, phone number, or long digit
run inside a record is replaced. The script refuses to write a file that still
trips its own leak checks.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

HERE = pathlib.Path(__file__).resolve().parent

# Response headers worth keeping. Everything else is dropped rather than
# reviewed, because the interesting fact is the status line and the throttle
# hints, and an allowlist cannot leak a header nobody thought about.
KEEP_HEADERS = {
    "content-type",
    "retry-after",
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
    "x-ratelimit-reset",
}

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
LONG_DIGITS_RE = re.compile(r"\d{10,}")
# A long hex/base64-ish run is a token or a device id even when its key is not
# one this list anticipated — a real capture surfaced a 64-character
# `push_token` that every key- and digit-based rule waved through, because it
# is neither an email nor a digit run. Redact on the shape as well as the name.
OPAQUE_TOKEN_RE = re.compile(r"\b[A-Fa-f0-9]{32,}\b|\b[A-Za-z0-9_-]{40,}\b")
PII_KEYS = {
    "identity",
    "email",
    "phone",
    "objectid",
    "name",
    "fbid",
    "gpid",
    "advertisingid",
    "deviceid",
    "idfa",
    "idfv",
    "push_token",
    "token",
    "all_identities",
}


def request(url: str, account: str, passcode: str, body: bytes | None):
    """One HTTP call, returning (status, headers, raw body) without raising on 4xx/5xx."""
    req = urllib.request.Request(url, data=body, method="POST" if body else "GET")
    req.add_header("X-CleverTap-Account-Id", account)
    req.add_header("X-CleverTap-Passcode", passcode)
    if body:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        # The whole point: a non-2xx body is as interesting as a 2xx one.
        return e.code, dict(e.headers), e.read()


def scrub_value(v):
    """Recursively redact anything that could identify a person."""
    if isinstance(v, dict):
        return {
            k: ("REDACTED" if k.lower() in PII_KEYS else scrub_value(val))
            for k, val in v.items()
        }
    if isinstance(v, list):
        return [scrub_value(x) for x in v]
    if isinstance(v, str):
        # The placeholders must not themselves match the detectors in
        # `leak_check`, or scrubbing defeats its own gate: substituting
        # "redacted@example.invalid" leaves an email-shaped string, and
        # "0000000000" leaves a 10-digit run, so every capture was refused for
        # PII that the scrubber had just introduced. A bare word matches
        # neither, and these fixtures pin the paging *envelope* — status, the
        # cursor key, records-as-an-array — where the leaf values carry no
        # signal at all.
        v = EMAIL_RE.sub("REDACTED", v)
        v = OPAQUE_TOKEN_RE.sub("REDACTED", v)
        return LONG_DIGITS_RE.sub("REDACTED", v)
    # A long identifier that arrives as a JSON *number* is just as identifying
    # as one that arrives as a string, and it used to sail straight through
    # here — only for `leak_check` to catch it in the rendered text and refuse
    # the whole capture. Zero it, except the 14-digit packed `ts`, which is the
    # one long number in this payload that is real signal (and whose format
    # this crate got wrong once already; see the 0.6.1 correction).
    # `bool` first: it is an `int` subclass and `True` must stay `true`.
    if isinstance(v, bool):
        return v
    # A float is never an identifier, but its shortest round-trip rendering
    # routinely carries a long mantissa (`0.7777777777777778`) that the
    # `\d{10,}` leak detector reads as a surviving digit run and refuses the
    # capture over. Rounding keeps the type — which is the part a contract
    # fixture is pinning — and removes the false positive.
    if isinstance(v, float):
        return round(v, 6)
    if isinstance(v, int):
        digits = len(str(abs(v)))
        return v if digits < 10 or digits == 14 else 0
    return v


def scrub_page(doc: dict, cursor_alias: dict[str, str]) -> dict:
    """Redact records and replace every cursor value with a stable alias."""
    out = {}
    for k, v in doc.items():
        if k in ("cursor", "next_cursor") and isinstance(v, str):
            out[k] = cursor_alias.setdefault(v, f"c{len(cursor_alias) + 1}")
        elif k == "records":
            out[k] = scrub_value(v)
        else:
            out[k] = scrub_value(v)
    return out


def leak_check(text: str) -> list[str]:
    """Refuse to write anything that still looks like it carries real data."""
    problems = []
    if EMAIL_RE.search(text):
        problems.append("an email-shaped string survived scrubbing")
    if OPAQUE_TOKEN_RE.search(text):
        problems.append("an opaque token/device-id-shaped string survived scrubbing")
    for m in LONG_DIGITS_RE.finditer(text):
        # A packed yyyyMMddHHmmSS ts is 14 digits and is legitimate signal.
        if len(m.group()) != 14:
            problems.append(f"a {len(m.group())}-digit run survived scrubbing")
            break
    return problems


def trim_records(doc: dict, keep: int) -> dict:
    """Keep only the first `keep` records, recording the true count.

    A real page carries thousands of records and megabytes of JSON. None of
    that is evidence about *paging* — the envelope is (`status`, which cursor
    key is present, that `records` is an array). Committing 3 MB per page to
    prove a cursor key would make the fixtures unreadable and the diff useless,
    so the array is truncated and the original length preserved alongside it.
    """
    if keep < 0 or not isinstance(doc.get("records"), list):
        return doc
    out = dict(doc)
    out["_records_total"] = len(doc["records"])
    out["records"] = doc["records"][:keep]
    return out


def write(name: str, doc: dict, status: int, headers: dict) -> None:
    text = json.dumps(doc, indent=2, ensure_ascii=False) + "\n"
    problems = leak_check(text)
    if problems:
        sys.exit(f"REFUSING to write {name}: {'; '.join(problems)}")
    (HERE / f"{name}.json").write_text(text, encoding="utf-8")
    kept = {k: v for k, v in headers.items() if k.lower() in KEEP_HEADERS}
    meta = f"HTTP {status}\n" + "".join(f"{k}: {v}\n" for k, v in sorted(kept.items()))
    (HERE / f"{name}.headers").write_text(meta, encoding="utf-8")
    print(f"  wrote {name}.json + {name}.headers (HTTP {status})")


def describe(doc: dict) -> dict:
    """The three facts that settle the dispute, extracted from one page."""
    return {
        "status": doc.get("status"),
        "code": doc.get("code"),
        "has_cursor": "cursor" in doc,
        "has_next_cursor": "next_cursor" in doc,
        "record_count": len(doc["records"]) if isinstance(doc.get("records"), list) else None,
        "records_type": type(doc.get("records")).__name__,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--event", required=True, help='e.g. "App Launched"')
    ap.add_argument("--date", required=True, help="YYYY-MM-DD; used for both from and to")
    ap.add_argument("--region", default="sg1")
    ap.add_argument("--pages", type=int, default=3, help="how far down the chain to walk")
    ap.add_argument("--batch-size", type=int, default=5000)
    ap.add_argument("--keep-records", type=int, default=2,
                    help="records to keep per written fixture (the true count is preserved "
                         "as _records_total). The paging contract lives in the envelope, not "
                         "in thousands of rows.")
    args = ap.parse_args()

    account = os.environ.get("CLEVERTAP_ACCOUNT_ID")
    passcode = os.environ.get("CLEVERTAP_PASSCODE")
    if not account or not passcode:
        return int(bool(sys.stderr.write(
            "set CLEVERTAP_ACCOUNT_ID and CLEVERTAP_PASSCODE\n"
        ))) or 2

    if not re.fullmatch(r"[a-z0-9]+", args.region):
        sys.exit(f"invalid region {args.region!r}")
    base = f"https://{args.region}.api.clevertap.com"
    day = int(args.date.replace("-", ""))

    cursor_alias: dict[str, str] = {}
    summary: dict[str, object] = {"region": args.region, "date": args.date, "pages": []}

    # --- create export -----------------------------------------------------
    url = f"{base}/1/events.json?batch_size={args.batch_size}"
    body = json.dumps({"event_name": args.event, "from": day, "to": day}).encode()
    status, headers, raw = request(url, account, passcode, body)
    try:
        doc = json.loads(raw)
    except json.JSONDecodeError:
        sys.exit(f"create-export returned non-JSON (HTTP {status}): {raw[:200]!r}")
    summary["create"] = {"http": status, **describe(doc)}
    write("create_success", scrub_page(doc, cursor_alias), status, headers)

    cursor = doc.get("cursor") or doc.get("next_cursor")
    if not cursor:
        print("\nNo cursor on the create response — cannot page. Captured anyway.")
        (HERE / "capture.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        return 1

    # --- walk the chain ----------------------------------------------------
    pages = []
    for i in range(args.pages):
        # The cursor is appended VERBATIM. CleverTap hands it back already
        # percent-encoded (observed sg1 2026-08-31: 1,928 chars of alnum plus
        # %2B/%2F/%3D and nothing else), so encoding it again turns %2B into
        # %252B and every page fetch returns HTTP 200 with
        # {"status":"fail","error":"Incorrect Usage","code":3}.
        page_url = f"{base}/1/events.json?cursor={cursor}"
        status, headers, raw = request(page_url, account, passcode, None)
        try:
            doc = json.loads(raw)
        except json.JSONDecodeError:
            print(f"  page {i + 1}: non-JSON (HTTP {status}): {raw[:200]!r}")
            break
        facts = {"http": status, **describe(doc)}
        pages.append((doc, status, headers, facts))
        summary["pages"].append(facts)
        print(f"  page {i + 1}: HTTP {status} status={facts['status']!r} "
              f"cursor={facts['has_cursor']} next_cursor={facts['has_next_cursor']} "
              f"records={facts['record_count']}")

        nxt = doc.get("next_cursor") or doc.get("cursor")
        if not nxt or nxt == cursor:
            break
        cursor = nxt

    if not pages:
        sys.exit("no data pages captured")

    write("page_1", trim_records(scrub_page(pages[0][0], cursor_alias), args.keep_records), pages[0][1], pages[0][2])
    if len(pages) > 2:
        mid = pages[len(pages) // 2]
        write("page_mid", trim_records(scrub_page(mid[0], cursor_alias), args.keep_records), mid[1], mid[2])
    last = pages[-1]
    write("page_terminal", trim_records(scrub_page(last[0], cursor_alias), args.keep_records), last[1], last[2])

    # --- the verdict -------------------------------------------------------
    print("\n" + "=" * 68)
    print("VERDICT")
    print("=" * 68)
    non_final = [f for (_, _, _, f) in pages[:-1]]
    if non_final:
        keys = {("next_cursor" if f["has_next_cursor"] else "cursor" if f["has_cursor"] else "NONE")
                for f in non_final}
        statuses = {f["status"] for f in non_final}
        print(f"  non-final pages carry the next cursor as : {', '.join(sorted(keys))}")
        print(f"  non-final page status                    : {', '.join(map(repr, sorted(map(str, statuses))))}")
    else:
        print("  only ONE page in the chain — cannot classify non-final behaviour.")
        print("  Re-run against a busier event or a larger date range.")
    tf = last[3]
    print(f"  terminal page status                     : {tf['status']!r}")
    print(f"  terminal page has cursor / next_cursor   : {tf['has_cursor']} / {tf['has_next_cursor']}")
    print()
    print("  Read that against the two competing claims in source/clevertap.rs.")
    print("  Whichever it contradicts is the one to stop believing.")

    (HERE / "capture.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(f"\n  summary -> {HERE / 'capture.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
