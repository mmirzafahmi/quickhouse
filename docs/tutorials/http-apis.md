# Pulling from HTTP APIs

**What you'll learn**

- Why an API source needs you to list its columns, and how to do it.
- How to load CleverTap events and AppsFlyer reports, and keep them up to date.
- How to land raw data quickly with append mode, and clean it up afterwards.
- How to read any other JSON or CSV API with `HttpApi`.

**Before you start:** read [Keeping tables up to date](incremental.md). To run
the CleverTap and AppsFlyer examples you need credentials for those services.
The `HttpApi` examples work with any API you can reach.

## The task

Daily Grind's loyalty app sends its events to CleverTap, and AppsFlyer tells
the growth team which ad campaign brought each new app user. Neither service is
a database you can connect to. Both offer an HTTP API that returns records for
a date range. A coffee-bean supplier also has a small API listing its
deliveries to each store.

quickhouse can pull from all three and write into ClickHouse or BigQuery.

## Listing the columns

A database can tell quickhouse what columns a table has. An API can't, so you
list them yourself with `columns`. Each column has a name, a type, and
optionally a **path**: where to find the value inside each record.

Here is one record from the supplier's API:

```text
{"delivery_id": 1,
 "store": {"id": 3, "name": "Airport T3"},
 "delivered_at": "2026-10-23T07:15:00Z",
 "items": [{"sku": "MILK-1L", "qty": 24}],
 "total": "123.45"}
```

And the columns you might declare for it:

```python
columns = [
    ("delivery_id",  "INTEGER"),
    ("store_id",     "INTEGER", "store.id"),   # path: "id" inside "store"
    ("delivered_at", "TIMESTAMP"),
    ("items",        "JSON"),                  # a nested list, kept as JSON text
    ("total",        "NUMERIC"),
]
```

- Without a path, quickhouse looks for a field with the column's name at the
  top of the record.
- A **dotted path** such as `"store.id"` reaches inside nested objects.
- A nested object or list can't become a column of its own. Declare it `JSON`
  (or `STRING`) and it lands as compact JSON text, like
  `[{"qty":24,"sku":"MILK-1L"}]`.

You can also write `columns` as a dictionary, with the paths in a separate
`paths` argument:

```python
columns = {"delivery_id": "INTEGER", "store_id": "INTEGER", "total": "NUMERIC"}
paths = {"store_id": "store.id"}
```

The types are BigQuery's names, even when you write to ClickHouse: `STRING`,
`INTEGER`, `FLOAT`, `BOOLEAN`, `TIMESTAMP`, `DATETIME`, `DATE`, `TIME`,
`NUMERIC`, `BIGNUMERIC`, `BYTES` and `JSON`.

A few rules worth knowing:

- **Values that don't fit become `NULL`.** If a field declared `INTEGER` holds
  `"Ana"`, that cell is left empty and the run reports a `coerced_scalar`
  warning (`coerced_decimal` for `NUMERIC`). The run itself succeeds.
- **Timestamps** can arrive as `2026-10-23T07:15:00Z`, as
  `2026-10-23 07:15:00` (read as UTC), as a packed number like
  `20261023071500`, as seconds since 1970 (`1729750000`), or as milliseconds
  since 1970 (`1729750000000`). quickhouse tells seconds and milliseconds apart
  by their length: 13 digits are milliseconds, so both of those examples land
  as 2024-10-24 06:06:40 UTC. A value that ends up outside the years 1900 to
  2299 becomes `NULL` with a `coerced_date` warning. Milliseconds from before
  September 2001 have only 12 digits, so they are read as seconds and come out
  `NULL` that way.
- **Money** should be `NUMERIC`, for values the API sends as text (like
  `"123.45"`) or whole numbers. It lands exact in both destinations: as
  `NUMERIC` in BigQuery, and as `Decimal(38, 9)` in ClickHouse. For a tighter
  ClickHouse type, also pass `type_overrides={"total": "Decimal(12, 2)"}`.
  quickhouse never changes an existing column, so a ClickHouse table created
  before version 0.20.10, when `NUMERIC` became `Float64`, keeps its float
  column until you drop the table.

## CleverTap: loyalty-app events

The marketing team wants every "App Launched" event in ClickHouse, updated
daily:

```python
import os
import quickhouse as qh

src = qh.CleverTap(
    account_id=os.environ["CLEVERTAP_ACCOUNT_ID"],
    passcode=os.environ["CLEVERTAP_PASSCODE"],
    event_name="App Launched",
    region="sg1",                      # must match your CleverTap account
    columns=[
        ("ts",          "TIMESTAMP"),
        ("object_id",   "STRING", "profile.objectId"),
        ("identity",    "STRING", "profile.identity"),
        ("platform",    "STRING", "profile.platform"),
        ("app_version", "STRING", "profile.app_version"),
        ("event_props", "JSON",   "event_props"),
    ],
    from_date="2026-10-01",            # where the very first run starts
    lookback_days=2,
)
dst = qh.ClickHouse("http://localhost:8123", database="analytics")

result = qh.sync(src, dst, dest_table="app_launched",
                 mode="incremental", watermark="ts", key=["object_id", "ts"])
```

What each part does:

- **`region`** picks CleverTap's server: `sg1`, `us1`, `eu1`, `in1`, `aps3` or
  `mec1`. It must be the region your account lives in.
- **`ts`** is CleverTap's event time. In several regions it arrives as a packed
  number like `20261023083000`, not as seconds. Declare it `TIMESTAMP` and
  quickhouse reads it as 2026-10-23 08:30:00 UTC.
- **`from_date`** is where the first run starts. APIs work in whole days, and
  each run reads up to today (in UTC), or up to `to_date` if you set one.
- **`key`** says what makes one event unique: the same person
  (`object_id`, CleverTap's own ID for each profile) at the same moment. With
  `key=["identity"]` alone you would keep only each member's *latest* launch,
  because every new event would replace the previous one. Pick key fields that
  are never missing: a missing key value is stored as an empty value, so all
  such events would count as one.

```{important}
A wrong `region` doesn't fail. It quietly returns no data, so the run succeeds
with nothing copied. If a new CleverTap job copies nothing, check the region
first.
```

### How an API source keeps its place

A database source remembers the highest watermark value it read. An API source
remembers a **date** instead: the last day it asked for. `result.new_watermark`
shows it, for example `2026-10-24`.

The next run starts from that same day again, because the day wasn't over when
it was last read. That's why `mode="incremental"` needs a `key` here: an event
read a second time replaces its first copy instead of being added again.

`lookback_days=2` widens that overlap. Each run starts two days before the
saved date, which catches events that CleverTap receives late, for example from
phones that were offline. It never reaches back before `from_date`.

### Three warnings to watch

- **`incomplete_export`**: the export ended in a way that suggests some records
  were missing, while the run still succeeded. Compare the row count with
  CleverTap's own dashboard for that day.
- **`coerced_scalar`**: a value didn't match its declared type and became
  `NULL`. Usually a sign that a column's type or path is wrong.
- **`null_key`**: records with no value in a `key` column were left out, since
  a key can't be empty. Check the column's path, or pick a key every record
  has.

Your credentials stay hidden: `print(src)` shows the account ID and passcode
as `***`.

## Landing raw data fast with append

Some teams like to land API data exactly as it arrives, in a **raw** (or
"bronze") table, and clean it up in a separate step. `mode="append"` is made
for that:

```python
raw = qh.ClickHouse("http://localhost:8123", database="raw")

qh.sync(src, raw, dest_table="clevertap_app_launched",
        mode="append", watermark="ts")
```

Append mode writes each run's rows straight into the table: no staging table,
no merge, and no check for duplicates. It still needs `watermark`, because the
saved date decides where the next run starts. It doesn't need a `key`.

Because nothing is merged, the overlap described above shows up as repeated
rows: each run adds the last day's events again. Remove the repeats when you
read the raw table. In ClickHouse, `LIMIT 1 BY` keeps one row per event:

```sql
SELECT *
FROM raw.clevertap_app_launched
ORDER BY ts
LIMIT 1 BY object_id, ts
```

Use append when you load a lot of events and run your own cleanup anyway. In
BigQuery it also avoids creating and swapping a staging table on every run,
which keeps you clear of BigQuery's limit on how often a table's metadata can
change. Append works for API sources and DataFrames, not for database sources.

## AppsFlyer: install reports

AppsFlyer's raw-data Pull API returns a CSV report. Here `columns` map to the
CSV's header names, so the path is the exact header text:

```python
src = qh.AppsFlyer(
    api_token=os.environ["APPSFLYER_API_TOKEN"],
    app_id="id123456789",
    report_type="installs_report",
    columns=[
        ("install_time", "TIMESTAMP", "Install Time"),
        ("appsflyer_id", "STRING",    "AppsFlyer ID"),
        ("media_source", "STRING",    "Media Source"),
        ("campaign",     "STRING",    "Campaign"),
    ],
    from_date="2026-10-01",
    lookback_days=7,
    extra_params={"timezone": "UTC"},
)
dst = qh.BigQuery("daily-grind-data", dataset_id="analytics")

qh.sync(src, dst, dest_table="af_installs",
        mode="incremental", watermark="install_time",
        key=["appsflyer_id", "install_time"])
```

- **`report_type`** chooses the report, for example `installs_report`,
  `in_app_events_report` or `organic_installs_report`.
- **Header names must match exactly.** If one doesn't, the run fails and lists
  the headers the report really has. Download one report by hand first and copy
  the names from it.
- **Time zones.** AppsFlyer reports times in your account's time zone unless
  you pass `extra_params={"timezone": "UTC"}`. With UTC, declare the time column
  `TIMESTAMP`. To keep local wall-clock times instead, leave the parameter out
  and declare `DATETIME`.
- **Late changes.** AppsFlyer keeps updating attribution for several days, so
  a `lookback_days` of a week is a reasonable start.

```{warning}
The Pull API has hard daily limits on calls and rows. When you hit one,
AppsFlyer sends a short message instead of the report. quickhouse recognizes it
and fails the run, so a limit can never replace your table with junk. For large
volumes, use AppsFlyer's Data Locker (files delivered to a bucket) instead.
```

## Any other API: HttpApi

The supplier's delivery API isn't CleverTap or AppsFlyer, so you describe it
yourself with `HttpApi`:

```python
src = qh.HttpApi(
    "https://api.supplier.example/v1/deliveries?from={from}&to={to}",
    headers={"Authorization": f"Bearer {os.environ['SUPPLIER_TOKEN']}"},
    records_path="data.rows",          # where the list of records is
    next_cursor_path="meta.next",      # where the next page's cursor is
    cursor_param="cursor",             # how to send it back
    columns=[
        ("delivery_id",  "INTEGER"),
        ("store_id",     "INTEGER", "store.id"),
        ("delivered_at", "TIMESTAMP"),
        ("total",        "NUMERIC"),
    ],
    from_date="2026-10-01",
    state_id="supplier-deliveries",
)
dst = qh.ClickHouse("http://localhost:8123", database="analytics")

qh.sync(src, dst, dest_table="supplier_deliveries",
        mode="incremental", watermark="delivered_at", key=["delivery_id"])
```

What quickhouse does with this:

1. It replaces `{from}` and `{to}` in the URL with the run's dates.
2. It sends the request with your headers. Put credentials there: quickhouse
   never writes headers to its logs, and `print(src)` shows `headers=***`.
3. It finds the records at `records_path` in the JSON response. Leave
   `records_path` out if the response *is* the list.
4. If `meta.next` holds a value, it asks for the next page with
   `?cursor=<value>`, and repeats until the value is missing.

So the requests for one run look like this:

```text
GET /v1/deliveries?from=2026-10-01&to=2026-10-24
GET /v1/deliveries?from=2026-10-01&to=2026-10-24&cursor=p2
```

The cursor can be text or a number, so an API that returns the next page
number works too (`next_cursor_path="meta.next_page"`,
`cursor_param="page"`). Leave both settings out for an API that returns
everything in one response.

Other shapes of API:

- **POST requests**: pass `method="POST"` and a `body`. `{from}` and `{to}` are
  replaced in the body too, for example
  `body='{"delivered_between": ["{from}", "{to}"]}'`.
- **CSV responses**: pass `format="csv"` and use the CSV headers as paths, as
  for AppsFlyer. CSV responses aren't paged.

`state_id` names the job's saved position. Without it, the URL is the name,
so changing the URL (a new API version, say) would start again from
`from_date`. Set `state_id` once and you can change the URL safely.

Requests that fail with status 429 ("too many requests"), a 5xx server error or
a network error are retried with a growing pause. Other errors, such as 401 or
404, fail the run straight away.

```{note}
`HttpApi` is available from Python only. The `quickhouse run` job file
supports CleverTap and AppsFlyer, but not `HttpApi`.
```

## What's different about API sources

- A full refresh (`mode="full"`) needs `from_date` and replaces the whole
  destination table. For a table you keep adding to, use `incremental` or
  `append`.
- Use `lookback_days`, not `lookback_seconds`, to re-read recent days.
- `result.read_secs` is always `0.0` for an API source. The time spent fetching
  is part of `stage_secs`.

## Recap

- Declare each column as `(name, type)` or `(name, type, path)`. Dotted paths
  reach into nested JSON, and `JSON` columns keep nested data as text.
- `region` must match your CleverTap account, or you silently get no data.
- API sources save the last date they read and re-read that day next time, so
  incremental mode needs a `key` that identifies one record.
- `mode="append"` lands raw rows fast, repeats included. Remove repeats when
  you read the raw table.
- AppsFlyer columns use the CSV header names. Watch its daily limits.
- `HttpApi` handles other JSON or CSV APIs: `{from}`/`{to}` in the URL,
  credentials in `headers`, paging with `next_cursor_path` and `cursor_param`.

## Next

[Loading DataFrames](dataframes.md): write pandas, polars, pyarrow and DuckDB
data straight into ClickHouse or BigQuery.
