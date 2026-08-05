# HTTP APIs

**CleverTap** and **AppsFlyer**. These pull directly from the vendor APIs into
either destination. API data has
no catalog, so you **declare the schema** with `columns`: each column's name,
its BigQuery-style type, and — for nested event JSON — a dotted `path` into the
record. The declared type drives the destination table.

`columns` accepts a list of `(name, bq_type)` / `(name, bq_type, path)` tuples,
or a `{name: bq_type}` dict (with an optional `paths={name: "a.b"}` mapping).

## CleverTap

```python
# CleverTap Data Export API (events) -> BigQuery
qh.sync(
    qh.CleverTap(
        account_id="...", passcode="...",
        event_name="App Launched", region="sg1",
        columns=[
            ("identity", "STRING", "profile.identity"),   # dotted path into the record
            ("email",    "STRING", "profile.email"),
            ("ts",       "TIMESTAMP"),                     # packed yyyyMMddHHmmSS int, parsed as UTC
            ("app_ver",  "STRING", "profile.app_version"),
        ],
        from_date="2026-07-01",             # window start (first-run floor for incremental)
    ),
    qh.BigQuery("my-gcp-project", dataset_id="analytics"),
    dest_table="clevertap_app_launched",
    mode="incremental", watermark="ts", key=["identity"],
)
```

```{admonition} region must match your account
:class: important
`region` selects the API host (`sg1` / `us1` / `eu1` / `in1` / `aps3` / `mec1`).
The **wrong region silently returns no data** — set it to match your CleverTap
account.
```

The top-level `ts` is a packed `yyyyMMddHHmmSS` integer in several regions (e.g.
`sg1`), **not** epoch seconds — declare it `TIMESTAMP`/`DATETIME`/`DATE` and it
is parsed as UTC civil time (10-digit epoch seconds are also accepted).

## AppsFlyer

```python
# AppsFlyer raw-data Pull API (CSV report) -> BigQuery. columns map to CSV headers.
qh.sync(
    qh.AppsFlyer(
        api_token="...", app_id="id123456789", report_type="installs_report",
        columns={"install_time": "TIMESTAMP", "media_source": "STRING", "campaign": "STRING"},
        from_date="2026-07-01",
    ),
    qh.BigQuery("my-gcp-project", dataset_id="analytics"),
    dest_table="af_installs", mode="full",
)
```

The Pull API has **hard daily-call and row caps** — for high volume use
AppsFlyer's Data Locker (files in a bucket) instead. Times are in the account's
timezone unless you pass `extra_params={"timezone": "UTC"}`.

## Notes for both API sources

- **Types.** `NUMERIC` is delivered exactly (declare it only for values sent as
  JSON strings/integers); `BIGNUMERIC` is lossy. Nested `RECORD`/`STRUCT` types
  can't be declared — point a `JSON` (or `STRING`) column at a nested
  object/array via its `path` and it lands as compact JSON text.
- **Incremental re-pulls the boundary day** each run, so `key` is required
  (BigQuery MERGE dedups the overlap).
- **`lookback_days=N`** re-pulls a rolling `N`-day window before the cursor on
  each resume, so late-arriving or restated events past the boundary day aren't
  missed (both APIs restate history; AppsFlyer attribution updates for days).
- **`mode="append"`** is a bronze-landing write: insert each window's rows
  straight into the destination with **no** staging/merge/swap and no dedup,
  running your own consolidation MERGE downstream. `watermark` drives the resume
  window; `key` isn't required. See [Sync modes](../sync-modes.md#append-bronze-landing).
- A full refresh (`mode="full"`, the default) **replaces** the destination;
  since these sources are day/event-scoped, prefer `mode="incremental"` for an
  existing table.
