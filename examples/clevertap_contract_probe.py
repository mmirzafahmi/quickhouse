#!/usr/bin/env python3
"""Walk a full CleverTap export chain and score the competing termination rules.

WHY
---
`quickhouse`'s CleverTap source implements one paging contract; a production
audit asserts a different one for the same region. Both were recorded as
"verified live". The two disagree on facts that decide whether the source reads
a whole day or 0.36% of one.

The trap is that the two headline defects produce the *same* symptom:

  B1  the parser reads `cursor` on data pages, but if the vendor sends
      `next_cursor` the parsed cursor is always None -> stop after page 1
  B2  the loop treats `status == "success"` as terminal, but if the vendor
      sends "success" on every page -> stop after page 1

One observation (a short table) cannot tell you which is true, or whether both
are. This script walks the chain *tolerantly* — following whichever cursor key
is actually present, ignoring `status` entirely — and then replays each
candidate termination rule over what it saw. That turns one run into a verdict
on every rule at once.

It reads only. It writes nothing to any destination.

USAGE
-----
    export CLEVERTAP_ACCOUNT_ID=...
    export CLEVERTAP_PASSCODE=...
    python clevertap_contract_probe.py --event "Load Product Show" --date 2026-08-27

Pick a BUSY event and a BUSY day. An event that genuinely fits in one page
cannot distinguish any of these rules, and the script will tell you so.

NOTE ON `quickhouse.HttpApi`
----------------------------
It might look like `HttpApi(next_cursor_path=..., cursor_param=...)` could test
this without any bespoke code. It cannot: `HttpApiSource` reuses one fixed
method, url and body for every request in the chain, whereas CleverTap needs a
POST to create the export followed by GETs to page it. Hence this script.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

TIMEOUT = 120


def request(url: str, account: str, passcode: str, body: bytes | None):
    req = urllib.request.Request(url, data=body, method="POST" if body else "GET")
    req.add_header("X-CleverTap-Account-Id", account)
    req.add_header("X-CleverTap-Passcode", passcode)
    if body:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--event", required=True)
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--region", default="sg1")
    ap.add_argument("--batch-size", type=int, default=5000)
    ap.add_argument("--max-pages", type=int, default=100_000,
                    help="hard bound so a cursor loop cannot run forever")
    ap.add_argument("--poll-seconds", type=int, default=5,
                    help="wait between retries when the export is still materialising")
    ap.add_argument("--max-wait", type=int, default=900,
                    help="give up after this long waiting for the export")
    args = ap.parse_args()

    account = os.environ.get("CLEVERTAP_ACCOUNT_ID")
    passcode = os.environ.get("CLEVERTAP_PASSCODE")
    if not account or not passcode:
        print("set CLEVERTAP_ACCOUNT_ID and CLEVERTAP_PASSCODE", file=sys.stderr)
        return 2
    if not re.fullmatch(r"[a-z0-9]+", args.region):
        print(f"invalid region {args.region!r}", file=sys.stderr)
        return 2

    base = f"https://{args.region}.api.clevertap.com"
    day = int(args.date.replace("-", ""))

    print(f"creating export: event={args.event!r} day={day} region={args.region}")
    status, raw = request(
        f"{base}/1/events.json?batch_size={args.batch_size}",
        account,
        passcode,
        json.dumps({"event_name": args.event, "from": day, "to": day}).encode(),
    )
    try:
        doc = json.loads(raw)
    except json.JSONDecodeError:
        print(f"create-export returned non-JSON (HTTP {status}): {raw[:200]!r}")
        return 1
    cursor = doc.get("cursor") or doc.get("next_cursor")
    if not cursor:
        print(f"no cursor on create response (HTTP {status}): {json.dumps(doc)[:300]}")
        return 1
    print(f"  ok (HTTP {status}), cursor key = "
          f"{'cursor' if 'cursor' in doc else 'next_cursor'}\n")

    # --- walk tolerantly: follow whatever cursor exists, ignore `status` -----
    pages: list[dict] = []
    seen: set[str] = set()
    started = time.monotonic()
    waited = 0.0

    while len(pages) < args.max_pages:
        if cursor in seen:
            print(f"  !! cursor repeated at page {len(pages) + 1} — stopping (no progress)")
            break
        seen.add(cursor)

        # Verbatim: CleverTap's cursor is already percent-encoded, and
        # re-encoding it yields "Incorrect Usage" (code 3) on every page.
        page_url = f"{base}/1/events.json?cursor={cursor}"
        status, raw = request(page_url, account, passcode, None)
        try:
            doc = json.loads(raw)
        except json.JSONDecodeError:
            print(f"  page {len(pages) + 1}: non-JSON (HTTP {status}): {raw[:200]!r}")
            break

        # "export still materialising" — poll the SAME cursor rather than dying.
        if doc.get("status") == "fail" and doc.get("code") == 2:
            if waited >= args.max_wait:
                print(f"  gave up waiting {waited:.0f}s for the export to materialise")
                break
            time.sleep(args.poll_seconds)
            waited += args.poll_seconds
            print(f"  export not ready (code=2), waited {waited:.0f}s ...")
            continue

        # The throttle that arrives as an HTTP 200.
        err = str(doc.get("error", ""))
        if doc.get("status") == "fail" and "too many requests" in err.lower():
            time.sleep(args.poll_seconds)
            waited += args.poll_seconds
            print(f"  throttled (HTTP {status}, body says fail), waited {waited:.0f}s ...")
            continue

        if doc.get("status") == "fail":
            print(f"  page {len(pages) + 1}: vendor error (HTTP {status}): {json.dumps(doc)[:300]}")
            break

        recs = doc.get("records")
        page = {
            "http": status,
            "status": doc.get("status"),
            "n": len(recs) if isinstance(recs, list) else 0,
            "records_is_list": isinstance(recs, list),
            "cursor": doc.get("cursor"),
            "next_cursor": doc.get("next_cursor"),
        }
        pages.append(page)
        if len(pages) <= 5 or len(pages) % 25 == 0:
            print(f"  page {len(pages)}: HTTP {status} status={page['status']!r} "
                  f"records={page['n']} "
                  f"cursor={'y' if page['cursor'] else 'n'} "
                  f"next_cursor={'y' if page['next_cursor'] else 'n'}")

        nxt = doc.get("next_cursor") or doc.get("cursor")
        if not nxt:
            break
        cursor = nxt

    if not pages:
        print("\nno data pages read — nothing to compare.")
        return 1

    total = sum(p["n"] for p in pages)
    elapsed = time.monotonic() - started

    # --- replay each candidate termination rule over the observed chain -----
    def rule_incumbent(ps):
        """Stop at the first `success` page, or the first empty `records`."""
        got = 0
        for i, p in enumerate(ps, 1):
            got += p["n"]
            if p["status"] == "success" or p["n"] == 0:
                return got, i
        return got, len(ps)

    def rule_challenger(ps):
        """Stop when `next_cursor` is absent; `status` is ignored."""
        got = 0
        for i, p in enumerate(ps, 1):
            got += p["n"]
            if not p["next_cursor"]:
                return got, i
        return got, len(ps)

    def rule_tolerant(ps):
        """Stop when neither cursor key is present. Empty pages never stop it."""
        got = 0
        for i, p in enumerate(ps, 1):
            got += p["n"]
            if not (p["next_cursor"] or p["cursor"]):
                return got, i
        return got, len(ps)

    print("\n" + "=" * 72)
    print(f"OBSERVED: {total:,} records across {len(pages)} page(s) in {elapsed:.0f}s")
    print("=" * 72)
    if len(pages) == 1:
        print("  Only one page. This event/day cannot distinguish the rules —")
        print("  re-run against a busier event or a wider window.")
    keys = {("next_cursor" if p["next_cursor"] else "cursor" if p["cursor"] else "none")
            for p in pages[:-1]}
    statuses = {p["status"] for p in pages[:-1]}
    if keys:
        print(f"  non-final pages carry: {', '.join(sorted(keys))}")
        print(f"  non-final page status: {', '.join(map(repr, sorted(map(str, statuses))))}")
    print(f"  terminal page: status={pages[-1]['status']!r} "
          f"cursor={'y' if pages[-1]['cursor'] else 'n'} "
          f"next_cursor={'y' if pages[-1]['next_cursor'] else 'n'}")
    if not all(p["records_is_list"] for p in pages):
        print("  !! at least one page had a non-list `records` field")

    print("\n  what each termination rule would have read:")
    print(f"    {'rule':<34} {'records':>12}  {'pages':>6}   {'% of chain':>10}")
    for name, fn in (
        ("incumbent (in clevertap.rs today)", rule_incumbent),
        ("challenger (the audit)", rule_challenger),
        ("tolerant (both keys, no status)", rule_tolerant),
    ):
        got, upto = fn(pages)
        pct = (got / total * 100) if total else 0.0
        print(f"    {name:<34} {got:>12,}  {upto:>6}   {pct:>9.2f}%")

    print("\n  Any rule below 100% would silently truncate this event and report")
    print("  success. That is the number the audit put at 0.36%.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
