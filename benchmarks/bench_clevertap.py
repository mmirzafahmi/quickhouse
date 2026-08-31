#!/usr/bin/env python3
"""Benchmark the CleverTap source against a real account — and prove the number.

WHY THIS IS NOT JUST A STOPWATCH
--------------------------------
quickhouse's CleverTap paging contract is disputed and unverified (see the
module docs in ``crates/quickhouse-core/src/source/clevertap.rs``). Two accounts
of the same region's behaviour disagree about which key carries the next cursor
and what terminates the chain, and if the audit is right the source reads
**exactly one page** and reports success.

That makes a naive throughput benchmark actively dangerous: "42,000 rows in 3.1s
= 13.5k rows/s" is a great-looking number that could be 0.36% of the day, read
in one page, at a rate that says nothing about a real pull. A benchmark you
cannot trust is worse than no benchmark.

So this runs **two lanes over the same window** and compares them:

  Lane A — ground truth. A tolerant raw walk of the export chain: follows
           whichever cursor key is actually present, ignores ``status``
           entirely, tolerates the ``code=2`` "still materialising" poll and the
           HTTP-200-with-``status:fail`` throttle. This is the same walk
           ``examples/clevertap_contract_probe.py`` does, and it establishes how
           many records and pages the window really holds.

  Lane B — quickhouse. ``qh.sync()`` over the identical window, timed, reporting
           the 0.15 phase breakdown (read / stage / promote).

The headline output is **coverage**: Lane B's ``rows_read`` as a percentage of
Lane A's total. At 100% the throughput number means something. Below 100% the
throughput number is meaningless and the run has instead found the paging bug —
which is the more valuable outcome, and the thing that unblocks the frozen
termination rule.

Lane A is read-only. Lane B writes to whatever destination you point it at;
default is the local docker-compose ClickHouse, so a benchmark never touches
production by accident.

CREDENTIALS
-----------
Either export them::

    export CLEVERTAP_ACCOUNT_ID=... CLEVERTAP_PASSCODE=...

or let the script read a Secret Manager secret (never printed)::

    python benchmarks/bench_clevertap.py \
        --secret projects/188583357235/secrets/clevertap_secret

The secret may be a bare passcode (pair it with ``--account-id`` or
``CLEVERTAP_ACCOUNT_ID``) or a JSON object; common key spellings for the account
id and passcode are recognised, and the script reports which shape it found
without echoing any value.

USAGE
-----
    docker compose up -d clickhouse
    python benchmarks/bench_clevertap.py \
        --secret projects/188583357235/secrets/clevertap_secret \
        --event user_events --date 2026-08-30

Pick a BUSY day. An event that genuinely fits in one page cannot distinguish a
correct pull from a truncated one, and the script says so rather than reporting
a coverage figure that proves nothing.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

TIMEOUT = 120

# Key spellings seen for the two credentials across secret payloads. CleverTap's
# own UI calls the account id a "Project ID" and the passcode a "Passcode", so a
# secret written from what the console shows uses those words rather than the
# ones the API headers use — hence `project_id` / `user_passcode` first.
ACCOUNT_KEYS = (
    "project_id",
    "account_id",
    "accountId",
    "CLEVERTAP_ACCOUNT_ID",
    "account",
    "id",
)
PASSCODE_KEYS = (
    "user_passcode",
    "passcode",
    "passCode",
    "CLEVERTAP_PASSCODE",
    "password",
    "secret",
    "token",
)


def _request(url: str, account: str, passcode: str, body: bytes | None):
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


def load_credentials(args) -> tuple[str, str]:
    """Resolve (account_id, passcode) without ever printing either."""
    account = args.account_id or os.environ.get("CLEVERTAP_ACCOUNT_ID")
    passcode = os.environ.get("CLEVERTAP_PASSCODE")

    if args.secret:
        cmd = ["gcloud", "secrets", "versions", "access", "latest", f"--secret={args.secret}"]
        if args.secret_project:
            cmd.append(f"--project={args.secret_project}")
        try:
            raw = subprocess.run(
                cmd, check=True, capture_output=True, text=True
            ).stdout.strip()
        except FileNotFoundError:
            sys.exit("gcloud not on PATH — export CLEVERTAP_* instead of using --secret")
        except subprocess.CalledProcessError as e:
            sys.exit(f"could not read {args.secret}:\n{e.stderr.strip()[:400]}")

        try:
            doc = json.loads(raw)
        except json.JSONDecodeError:
            doc = None

        if isinstance(doc, dict):
            found_a = next((k for k in ACCOUNT_KEYS if doc.get(k)), None)
            found_p = next((k for k in PASSCODE_KEYS if doc.get(k)), None)
            print(
                f"secret: JSON object, keys={sorted(doc)} "
                f"-> account from {found_a!r}, passcode from {found_p!r}"
            )
            if found_a:
                account = str(doc[found_a])
            if found_p:
                passcode = str(doc[found_p])
        else:
            print(f"secret: opaque string ({len(raw)} chars) — treating it as the passcode")
            passcode = raw

    if not account or not passcode:
        sys.exit(
            "missing credentials. Need an account id AND a passcode:\n"
            "  --account-id / CLEVERTAP_ACCOUNT_ID, and CLEVERTAP_PASSCODE or --secret"
        )
    return account, passcode


def create_export(base, account, passcode, event, day, batch_size):
    status, raw = _request(
        f"{base}/1/events.json?batch_size={batch_size}",
        account,
        passcode,
        json.dumps({"event_name": event, "from": day, "to": day}).encode(),
    )
    try:
        doc = json.loads(raw)
    except json.JSONDecodeError:
        sys.exit(f"create-export returned non-JSON (HTTP {status}): {raw[:200]!r}")
    cursor = doc.get("cursor") or doc.get("next_cursor")
    if not cursor:
        sys.exit(f"no cursor on create response (HTTP {status}): {json.dumps(doc)[:300]}")
    key = "cursor" if doc.get("cursor") else "next_cursor"
    return cursor, key, status


def walk_ground_truth(base, account, passcode, cursor, args):
    """Follow whatever cursor exists; ignore `status`. Returns observed pages."""
    pages: list[dict] = []
    seen: set[str] = set()
    waited = 0.0
    started = time.monotonic()

    while len(pages) < args.max_pages:
        if cursor in seen:
            print(f"  !! cursor repeated at page {len(pages) + 1} — stopping (no progress)")
            break
        seen.add(cursor)

        # Verbatim: CleverTap's cursor is already percent-encoded, and
        # re-encoding it yields "Incorrect Usage" (code 3) on every page.
        url = f"{base}/1/events.json?cursor={cursor}"
        status, raw = _request(url, account, passcode, None)
        try:
            doc = json.loads(raw)
        except json.JSONDecodeError:
            print(f"  page {len(pages) + 1}: non-JSON (HTTP {status}): {raw[:200]!r}")
            break

        if doc.get("status") == "fail" and doc.get("code") == 2:
            if waited >= args.max_wait:
                print(f"  gave up waiting {waited:.0f}s for the export to materialise")
                break
            time.sleep(args.poll_seconds)
            waited += args.poll_seconds
            print(f"  export not ready (code=2), waited {waited:.0f}s ...")
            continue

        err = str(doc.get("error", ""))
        if doc.get("status") == "fail" and "too many requests" in err.lower():
            time.sleep(args.poll_seconds)
            waited += args.poll_seconds
            print(f"  throttled (HTTP 200 with status=fail), waited {waited:.0f}s ...")
            continue

        if doc.get("status") == "fail":
            print(f"  page {len(pages) + 1}: vendor error: {json.dumps(doc)[:300]}")
            break

        recs = doc.get("records")
        pages.append(
            {
                "http": status,
                "status": doc.get("status"),
                "n": len(recs) if isinstance(recs, list) else 0,
                "cursor": bool(doc.get("cursor")),
                "next_cursor": bool(doc.get("next_cursor")),
            }
        )
        if len(pages) <= 3 or len(pages) % 25 == 0:
            p = pages[-1]
            print(
                f"  page {len(pages)}: status={p['status']!r} records={p['n']} "
                f"cursor={'y' if p['cursor'] else 'n'} "
                f"next_cursor={'y' if p['next_cursor'] else 'n'}"
            )

        nxt = doc.get("next_cursor") or doc.get("cursor")
        if not nxt:
            break
        cursor = nxt

    return pages, time.monotonic() - started - waited, waited


def run_quickhouse(args, account, passcode):
    import quickhouse as qh

    src = qh.CleverTap(
        account_id=account,
        passcode=passcode,
        event_name=args.event,
        region=args.region,
        batch_size=args.batch_size,
        columns=[
            ("ts", "TIMESTAMP"),
            ("event", "STRING", "evtName"),
            ("object_id", "STRING", "profile.objectId"),
            ("identity", "STRING", "profile.identity"),
            ("event_props", "JSON", "event_props"),
        ],
        from_date=args.date,
        to_date=args.date,
        lookback_days=0,
    )
    if args.bq_project:
        dst = qh.BigQuery(args.bq_project, dataset_id=args.bq_dataset)
    else:
        dst = qh.ClickHouse(
            args.ch_url,
            database=args.ch_db,
            user=args.ch_user,
            password=args.ch_password,
        )

    started = time.monotonic()
    result = qh.sync(
        src,
        dst,
        dest_table=args.dest_table,
        mode="append",
        watermark="ts",
        create_if_missing=True,
        # A fresh cursor identity per run, so a benchmark never silently
        # resumes from a previous one and reads nothing.
        state_key=f"bench:{args.event}:{args.date}:{int(started)}",
    )
    return result, time.monotonic() - started


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--event", default="user_events")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD — pick a BUSY day")
    ap.add_argument("--region", default="sg1", help="MUST match the account's region")
    ap.add_argument("--batch-size", type=int, default=5000)
    ap.add_argument("--account-id", default=None)
    ap.add_argument("--secret", default=None, help="Secret Manager secret (name or full path)")
    ap.add_argument("--secret-project", default=None)
    ap.add_argument("--dest-table", default="bench_clevertap")
    ap.add_argument("--ch-url", default=os.environ.get("QUICKHOUSE_CH_URL", "http://localhost:8123"))
    ap.add_argument("--ch-db", default=os.environ.get("QUICKHOUSE_CH_DB", "default"))
    ap.add_argument("--ch-user", default=os.environ.get("QUICKHOUSE_CH_USER", "default"))
    ap.add_argument("--ch-password", default=os.environ.get("QUICKHOUSE_CH_PASSWORD", ""))
    ap.add_argument("--bq-project", default=None, help="write to BigQuery instead of ClickHouse")
    ap.add_argument("--bq-dataset", default=None)
    ap.add_argument("--max-pages", type=int, default=100_000)
    ap.add_argument("--poll-seconds", type=int, default=5)
    ap.add_argument("--max-wait", type=int, default=900)
    ap.add_argument(
        "--skip-ground-truth",
        action="store_true",
        help="run only the quickhouse lane (throughput will be UNVERIFIED)",
    )
    args = ap.parse_args()

    if not re.fullmatch(r"[a-z0-9]+", args.region):
        sys.exit(f"invalid region {args.region!r}")
    if args.bq_project and not args.bq_dataset:
        sys.exit("--bq-project needs --bq-dataset")

    account, passcode = load_credentials(args)
    base = f"https://{args.region}.api.clevertap.com"
    day = int(args.date.replace("-", ""))

    truth_total = truth_pages = None
    truth_secs = 0.0
    if not args.skip_ground_truth:
        print("\n=== Lane A: ground truth (raw, tolerant walk) ===")
        print(f"creating export: event={args.event!r} day={day} region={args.region}")
        cursor, key, http = create_export(
            base, account, passcode, args.event, day, args.batch_size
        )
        print(f"  ok (HTTP {http}), cursor key on the create response = {key!r}")
        pages, truth_secs, waited = walk_ground_truth(base, account, passcode, cursor, args)
        if not pages:
            print("\nno data pages read — cannot establish ground truth.")
            return 1
        truth_pages = len(pages)
        truth_total = sum(p["n"] for p in pages)
        statuses = sorted({repr(p["status"]) for p in pages})
        cursor_keys = sorted(
            {("next_cursor" if p["next_cursor"] else "cursor" if p["cursor"] else "none")
             for p in pages}
        )
        print(f"\n  pages          {truth_pages}")
        print(f"  records        {truth_total:,}")
        print(f"  walk time      {truth_secs:.1f}s (+{waited:.0f}s waiting/throttled)")
        print(f"  status values  {', '.join(statuses)}")
        print(f"  cursor keys    {', '.join(cursor_keys)}")
        if truth_pages == 1:
            print(
                "\n  !! ONE page. This window cannot distinguish a correct pull from a\n"
                "     truncated one — pick a busier event or day before trusting any\n"
                "     coverage figure below."
            )

    print("\n=== Lane B: quickhouse.sync() ===")
    dest = (
        f"bigquery {args.bq_project}.{args.bq_dataset}.{args.dest_table}"
        if args.bq_project
        else f"clickhouse {args.ch_db}.{args.dest_table}"
    )
    print(f"destination: {dest}")
    result, wall = run_quickhouse(args, account, passcode)

    print(f"\n  rows_read      {result.rows_read:,}")
    print(f"  rows_written   {result.rows_written:,}")
    print(f"  bytes_written  {result.bytes_written:,}")
    print(f"  wall           {wall:.1f}s")
    print(f"  read_secs      {result.read_secs:.1f}s")
    print(f"  stage_secs     {result.stage_secs:.1f}s")
    print(f"  promote_secs   {result.promote_secs:.1f}s")
    if wall > 0:
        print(f"  throughput     {result.rows_read / wall:,.0f} rows/s")
    if truth_pages:
        # The number that actually explains the throughput. A CleverTap export
        # is one serial round trip per page, so wall time is pages x vendor
        # latency almost exactly, and rows/s is really a statement about
        # `batch_size` rather than about anything quickhouse does.
        print(f"  per page       {wall / truth_pages:.1f}s across {truth_pages} page(s)")
    if result.read_secs == 0.0:
        print(
            "  (read_secs is 0 for an API source: the idle-read timer instruments the\n"
            "   DB source streams, and HTTP paging is bounded by per-request timeouts\n"
            "   instead — so stage_secs here is the whole fetch-decode-insert loop.)"
        )
    for w in result.warnings:
        print(f"  warning        {w.kind} {w.column or ''}: {w.count}")

    print("\n=== Verdict ===")
    if truth_total is None:
        print("ground truth skipped — the throughput above is UNVERIFIED and may")
        print("describe a truncated read. Re-run without --skip-ground-truth.")
        return 0
    coverage = result.rows_read / truth_total * 100 if truth_total else 0.0
    print(f"ground truth    {truth_total:,} records across {truth_pages} page(s)")
    print(f"quickhouse read {result.rows_read:,}  ({coverage:.2f}% coverage)")
    if coverage >= 99.5:
        print(
            "\nFull coverage: the throughput figure above is real. Speed is the\n"
            "story, and Lane A doubles as the evidence the paging contract works."
        )
        return 0
    print(
        f"\nTRUNCATED READ. quickhouse saw {coverage:.2f}% of this window. The\n"
        f"throughput figure above is meaningless — it measures a partial pull.\n"
        f"\nThis is the disputed paging contract failing in production (see the\n"
        f"module docs in crates/quickhouse-core/src/source/clevertap.rs). Capture\n"
        f"the raw pages that prove it and commit them as fixtures:\n"
        f"\n    python crates/quickhouse-core/tests/fixtures/clevertap/capture.py \\\n"
        f"        --event {args.event!r} --date {args.date} --region {args.region}\n"
        f"\nthen run examples/clevertap_contract_probe.py to score each candidate\n"
        f"termination rule against the real chain."
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
