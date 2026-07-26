"""Land CleverTap events into a BigQuery "bronze" table with append mode.

HTTP API sources have no catalog, so you *declare* the output schema as
`(name, bq_type[, path])` columns; `path` pulls a value out of the nested event
JSON by dotted path. `mode="append"` inserts each window's rows straight into the
destination with no staging/merge/swap and no dedup — a fast bronze-landing write
you consolidate downstream (e.g. a scheduled MERGE with
`ROW_NUMBER() OVER (PARTITION BY ...)`). Re-running with `lookback_days` re-pulls
a rolling window to catch late/restated events.

Running one append per event name into the *same* bronze table is safe: each
event gets its own resume cursor automatically.

Prerequisites
-------------
- `pip install quickhouse`
- CleverTap Account ID + Passcode, and the destination is real BigQuery
  (API sources are BigQuery-only today). ADC or a key file for auth.

Environment variables:
    CT_ACCOUNT_ID, CT_PASSCODE          (required)
    CT_REGION      MUST match your CleverTap account region: sg1/us1/eu1/in1/aps3/mec1
    CT_EVENT       event name to pull (default: App Launched)
    BQ_PROJECT, BQ_DATASET              (required)
    DEST_TABLE     bronze table (default: clevertap_events_bronze)
    FROM_DATE      first-run window start "YYYY-MM-DD" (required on first run)
"""

import os
import sys

import quickhouse as qh


def main() -> None:
    account_id = os.getenv("CT_ACCOUNT_ID")
    passcode = os.getenv("CT_PASSCODE")
    project = os.getenv("BQ_PROJECT")
    dataset = os.getenv("BQ_DATASET")
    if not all([account_id, passcode, project, dataset]):
        sys.exit("Set CT_ACCOUNT_ID, CT_PASSCODE, BQ_PROJECT, BQ_DATASET.")

    src = qh.CleverTap(
        account_id=account_id,
        passcode=passcode,
        event_name=os.getenv("CT_EVENT", "App Launched"),
        region=os.getenv("CT_REGION", "sg1"),   # set to YOUR account's region
        columns=[
            # ts is a packed yyyyMMddHHmmSS integer; declare TIMESTAMP and it's
            # parsed as UTC. It's also the resume watermark below.
            ("ts", "TIMESTAMP"),
            ("event", "STRING", "evtName"),
            ("object_id", "STRING", "profile.objectId"),
            ("identity", "STRING", "profile.identity"),
            # A nested object/array lands as compact JSON text in a JSON column;
            # reconstruct STRUCTs downstream if you need them.
            ("event_props", "JSON", "event_props"),
        ],
        from_date=os.getenv("FROM_DATE"),   # first-run floor
        lookback_days=int(os.getenv("LOOKBACK_DAYS", "2")),
    )
    dst = qh.BigQuery(project, dataset_id=dataset, credentials_file=os.getenv("BQ_CREDENTIALS_FILE"))

    result = qh.sync(
        src,
        dst,
        dest_table=os.getenv("DEST_TABLE", "clevertap_events_bronze"),
        mode="append",          # bronze landing: no merge/dedup; consolidate downstream
        watermark="ts",         # drives the resumable date window
    )
    print(result)


if __name__ == "__main__":
    main()
