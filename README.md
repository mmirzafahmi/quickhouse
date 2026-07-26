# quickhouse

**Move tables from PostgreSQL, MySQL, or BigQuery into ClickHouse or BigQuery — fast, in one function call.**

quickhouse is a small, typed Python API on top of a native Rust engine. You
hand it a source, a destination, and a table name; it figures out the schema,
creates the destination table, streams the rows across in parallel, and keeps
memory flat the whole way. The heavy lifting never touches Python objects —
each database's native wire protocol flows straight into Apache Arrow and out
the other side.

```python
import quickhouse

src = quickhouse.Postgres("postgresql://user:pw@localhost:5432/shop")
dst = quickhouse.ClickHouse("http://localhost:8123", database="analytics")

result = quickhouse.sync(src, dst, dest_table="orders",
                         source_table="orders", key=["id"])
print(result)   # rows_read, rows_written, bytes_written, duration_secs, new_watermark
```

> **Status: pre-1.0.** quickhouse is used against real production data and is
> covered by an integration test suite, but the Python API may still change
> between minor versions before 1.0. Pin a compatible range (e.g.
> `quickhouse~=0.7`) and watch the [CHANGELOG](CHANGELOG.md). A few knobs are
> marked *experimental* in the API docs — those may change without a major bump.

### When to use quickhouse

Reach for it when you want to move whole tables — full refresh or incremental —
from PostgreSQL/MySQL/BigQuery into ClickHouse or BigQuery, fast, from your own
Python jobs with almost no setup. It fits cron/Airflow/Dagster tasks and one-off
backfills well.

Look elsewhere when you need in-warehouse SQL transformations (use **dbt**),
change-data-capture or streaming (use **Debezium/Kafka**), a large catalog of
SaaS connectors (use **Airbyte / Fivetran / dlt**), or arbitrary
source↔destination pairs — quickhouse deliberately supports a focused, fast set:

### Supported sources and destinations

| Source | → ClickHouse | → BigQuery |
|---|:--:|:--:|
| PostgreSQL | ✅ | ✅ |
| MySQL | ✅ | ✅ |
| BigQuery | ✅ | ✅ |
| CleverTap (HTTP API) | — | ✅ |
| AppsFlyer (HTTP API) | — | ✅ |

ClickHouse is a destination only; BigQuery is both a source and a destination.
The HTTP API sources currently write to BigQuery only. Some knobs are
engine-specific: `column_transforms` / `read_max_rows_per_sec` apply to the
PostgreSQL and MySQL sources; `merge_prune_partition_by` / `delete_stale_in_window`
apply to BigQuery-destination incremental syncs; S3 archival applies to ClickHouse
destinations.

## Why quickhouse

- **It's fast.** Rows are decoded straight off the wire into Arrow, in Rust —
  no per-row Python, no intermediate DataFrame. Tables are split into ranges
  and read in parallel, and decoding overlaps uploading. On a laptop-class box
  a 1M-row, 20-column full refresh runs at **hundreds of thousands of rows per
  second** while peak memory stays flat (under ~180 MB) no matter how much you
  parallelize. Reproduce it with `python benchmarks/bench_transfer.py`.

- **It's one function call.** `sync()` replaces the cursor loop, manual
  batching, retry logic, and `CREATE TABLE` you'd otherwise write by hand.
  Defaults handle table creation, type mapping, parallelism, and batching, and
  a typed stub gives you autocomplete on every argument.

- **It's safe with real, messy data.** Full refreshes swap in atomically, so a
  crash never leaves a half-written table. Incremental syncs are idempotent —
  safe to re-run or retry. Transient network blips retry automatically. And
  legacy quirks like MySQL zero-dates or out-of-range timestamps are coerced to
  `NULL` with a warning instead of aborting the run.

- **It's gentle on a small production database.** Set `read_max_rows_per_sec`
  and quickhouse paces the read to that aggregate rate across all partitions;
  because `COPY`/streaming results only produce as fast as the client consumes,
  the source scan itself backs off — you're throttling the database's work, not
  just your own. Combine it with `parallelism=1` (one connection), incremental
  mode (only new rows), and a `statement_timeout_secs`, point it at a read
  replica, and a bulk export stops competing with your app. The Postgres
  connection also shows up as `application_name = 'quickhouse'` in
  `pg_stat_activity`, so a DBA can see and kill it.

- **There's nothing to stand up.** `pip install quickhouse` and you're done —
  no JVM, no Spark cluster, no separate service. It's an ordinary Python
  dependency that runs wherever your jobs already run: cron, Airflow, Dagster,
  a Lambda, or a plain script.

## Install

```bash
pip install quickhouse
pip install "quickhouse[progress]"   # adds a ready-made tqdm progress bar
```

Prebuilt wheels ship for Python 3.9+ on **Linux (x86_64)**, **macOS (Apple
Silicon)**, and **Windows (x64)** — no Rust toolchain needed. On other platforms
(Intel macOS, Linux aarch64) `pip` builds from the source distribution, which
needs a Rust toolchain (see [CONTRIBUTING.md](CONTRIBUTING.md)).

## Using it

A fuller call, with the options you'll reach for most:

```python
import quickhouse as qh

src = qh.Postgres("postgresql://user:pw@localhost:5432/shop")
dst = qh.ClickHouse("http://localhost:8123", database="analytics")

qh.sync(
    src, dst,
    dest_table="orders",
    source_table="orders",        # or source_query="SELECT ..."
    mode="incremental",           # or "full"
    watermark="updated_at",       # required for incremental
    key=["id"],                   # dedup key / ORDER BY
    parallelism=8,
    exclude=["internal_notes"],
    rename={"amount": "amt"},
    on_progress=lambda p: print(f"{p.rows_written:,} rows @ {p.rows_per_sec:,.0f}/s"),
)
```

### Sources and destinations

Pick a source and a destination by constructing the matching object —
everything else about `sync()` stays the same:

```python
# sources
qh.Postgres("postgresql://user:pw@host:5432/db")
qh.MySQL("mysql://user:pw@host:3306/db", require_tls=True)
qh.BigQuery("my-gcp-project")                       # source_table="dataset.table"

# destinations
qh.ClickHouse("http://host:8123", database="analytics")
qh.BigQuery("my-gcp-project", dataset_id="analytics")
```

BigQuery authenticates with a service-account key (`credentials_file=...`) or
Application Default Credentials. As a **destination** it also takes
`write_method`: the default `"insert_all"` (simple, proven) or the opt-in
`"storage_write"` (the gRPC Storage Write API — free and higher-throughput).

### HTTP API sources — CleverTap & AppsFlyer (BigQuery destination only)

These pull directly from the vendor APIs. API data has no catalog, so you
**declare the schema**: each column's name, its BigQuery type, and — for
CleverTap's nested event JSON — a dotted path into the record. The declared
type drives the destination table; the watermark's `from`/`to` date window
drives incremental pulls.

```python
# CleverTap Data Export API (events) -> BigQuery.
# region MUST match your CleverTap account's region (sg1 / us1 / eu1 / in1 / aps3 / mec1);
# it selects the API host, and the wrong region silently returns no data.
qh.sync(
    qh.CleverTap(
        account_id="...", passcode="...",   # or load from a secret manager
        event_name="App Launched", region="sg1",
        columns=[
            ("identity", "STRING", "profile.identity"),   # dotted path into the record
            ("email",    "STRING", "profile.email"),
            ("ts",       "TIMESTAMP"),                     # packed yyyyMMddHHmmSS int (parsed as UTC)
            ("app_ver",  "STRING", "profile.app_version"),
        ],
        from_date="2026-07-01",             # window start (first-run floor for incremental)
    ),
    qh.BigQuery("my-gcp-project", dataset_id="analytics"),
    dest_table="clevertap_app_launched",
    mode="incremental", watermark="ts", key=["identity"],
)

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

Notes: incremental re-pulls the boundary day each run, so `key` is required
(BigQuery MERGE dedups it). `NUMERIC` is delivered exactly (declare it only for
values sent as JSON strings/integers); `BIGNUMERIC` is lossy. CleverTap's
top-level `ts` is a packed `yyyyMMddHHmmSS` integer (not epoch seconds) — declare
it `TIMESTAMP`/`DATETIME`/`DATE` and it's parsed as UTC civil time. Nested
`RECORD`/`STRUCT` types can't be declared in `bq_type`; point a `JSON` (or
`STRING`) column at a nested object/array via its `path` and it lands as compact
JSON text. AppsFlyer's Pull API has hard daily-call/row caps — for high volume
use its Data Locker (files in a bucket) instead. A full-refresh (`mode="full"`,
the default) REPLACES the destination; since these sources are day/event-scoped,
prefer `mode="incremental"` for an existing table (a shrinking full swap is
warned about but still executes).

Three knobs tune the incremental behavior for these sources:

- **`lookback_days=N`** — on each resume, re-pull a rolling `N`-day window before
  the cursor so late-arriving/restated events past the boundary day aren't missed
  (both APIs restate history; AppsFlyer attribution updates for days). MERGE-on-key
  dedups the overlap in incremental mode.
- **`mode="append"`** — a bronze-landing write: insert each window's rows straight
  into the destination with **no** staging/merge/swap and no dedup, running your
  own consolidation MERGE downstream. `watermark` drives the resume window; `key`
  isn't required. Avoids per-run table-metadata churn (BigQuery's
  ~5-ops/10s/table limit) since there's no staging create/swap. API sources only.
- **`delete_stale_in_window=True`** (BigQuery incremental) — also `DELETE`
  destination rows inside the merged window that are absent from the source pull
  ("replace this window"; a NULL merge key nets to a replace instead of
  duplicating). **Requires `merge_prune_partition_by`** — the DELETE is scoped to
  that immutable column's staging range, so it never touches history outside the
  batch (a hard error otherwise).

A ClickHouse destination can also archive every synced batch to S3 as a data
lake — a secondary, best-effort-free backup independent of ClickHouse's own
retention:

```python
qh.ClickHouse(
    "http://host:8123", database="analytics",
    archive=qh.S3Archive(bucket="my-data-lake", prefix="quickhouse"),
)
```

This streams Parquet — one file per parallel partition, never fully buffered
in memory — to `s3://{bucket}/{prefix}/{dest_table}/dt=<date>/run=<id>/
part-<partition>.parquet`, a Hive-style layout directly queryable by Athena,
Spark, or DuckDB. Credentials fall back to the standard AWS chain (env vars,
IAM role) unless overridden; pass `endpoint=` for an S3-compatible service
like MinIO. A persistent upload failure fails the whole `sync()` call, same as
a ClickHouse insert failure. Storage/request costs are billed by AWS as usual
(free on a self-hosted MinIO).

The DDL knobs (`engine`, `partition_by`, `order_by`, `primary_key`, `key`) are
interpreted per destination — for ClickHouse they shape the `MergeTree`
DDL; for BigQuery they map to partitioning and clustering. quickhouse creates
the table for you (`create_if_missing=True` by default) with a sensible schema
derived from the source.

### Full vs. incremental

**Full** reloads the whole table into a staging table, then swaps it into place
atomically — a crash mid-run never leaves the destination partial. For a
BigQuery destination that swap runs as a query (a billed scan of the staged
data), not a free copy job — BigQuery's copy jobs can silently skip rows still
sitting in a table's streaming buffer, so a real query is what keeps this
correct rather than just fast. One accepted tradeoff on the ClickHouse path:
an insert retried after a lost acknowledgment (not after a crash — the
transfer is still running) can duplicate one batch's rows in the staging
table, since `mode="full"` has no engine-level dedup like `ReplacingMergeTree`
— rare, and harmless for `key`-based incremental syncs, but worth knowing if
you see an unexpected small over-count on a full-refresh right after a
transient network blip.

**Incremental** tracks a high-water mark (the `watermark` column) in a small
state table in the destination and copies only newer rows. Updated rows are
deduplicated on `key` — via ClickHouse's `ReplacingMergeTree`, or a `MERGE`
upsert on BigQuery (where `key` is therefore required). Re-running with no new
data does nothing.

Both modes stage through a per-run-unique table (`{dest}_quickhouse_tmp_<id>`)
that's dropped when the run finishes, including on failure. The unique name is
what makes rapid re-runs and whole-call retries safe on BigQuery, whose
streaming ingestion rejects writes into a table recently recreated under the
same name.

For daily syncs that need to catch late-arriving or edited rows, set
`lookback_seconds` to re-scan a trailing window (e.g. `3 * 86400` for the last
three days) — the dedup above keeps that overlap from creating duplicates.

**Cursor control.** A few knobs make the incremental cursor robust in the real
world:

- `state_key` pins the cursor's identity. By default it's keyed by the source
  table (or query text) + destination — so editing a `source_query`'s `WHERE`
  would silently start a fresh full pull, and two syncs into one destination
  tracking different `watermark` columns would clobber each other's cursor. Set
  `state_key="orders:updated_at"` to give each a stable, distinct identity.
- `skip_to_max=True` (or `seed_watermark="<value>"`) seeds the cursor on the
  **first** run only, then self-retires. Use it when the destination already
  holds complete data from a previous pipeline and a full first pull would be a
  waste — it starts the cursor at the source's current max instead of scanning
  everything.
- `advance_watermark=False` reads and merges a window *without* moving the
  cursor — for loading a historical backfill without rewinding your regular
  schedule.

**MERGE cost on large BigQuery tables.** By default an incremental `MERGE`
scans the whole destination table (it joins on `key` only), so upserting a few
delta rows into a huge partitioned table bills the whole table each run. Set
`merge_prune_partition_by="created_at"` to bound the scan to the staging
batch's range and let BigQuery prune partitions. **Only do this for an
*immutable* column** — one whose value never changes for a given `key`, i.e. a
`created_at`/inserted-at column that is also the partition column. Do **not**
point it at a `updated_at`/updated-at column: an updated row's new value lands
in a different partition than the existing row, so pruning would miss it and
insert a duplicate key instead of updating. quickhouse can't detect mutability,
so this is a deliberate opt-in; the default full scan is always correct.

### Watching progress and diagnosing failures

`on_progress` is a plain callback you can point at anything; `qh.progress_bar()`
wraps [tqdm](https://github.com/tqdm/tqdm) for a ready-made bar. Every `sync()`
also logs each step to stderr (`RUST_LOG=quickhouse_core=debug` for the actual
SQL).

When something goes wrong, `sync()` raises a `RuntimeError` written to be
actionable on its own: it names the table involved, and for a bad config or an
unmappable column it says exactly what's wrong and how to fix it (e.g.
`exclude=` the column or cast it in a `source_query`). Underlying database
errors are surfaced verbatim rather than wrapped in something generic.

### Full parameter list

| Parameter | Meaning |
| --- | --- |
| `source_table` / `source_query` | Read a whole table, or a custom `SELECT` (one required) |
| `dest_table` | Destination table name |
| `mode` | `"full"` or `"incremental"` |
| `watermark` | Monotonic column for incremental (e.g. `updated_at`); ignored in full mode |
| `lookback_seconds` | Re-scan a trailing window of the watermark to catch late/edited rows; `0` disables (default) |
| `state_key` | Pin the incremental cursor's identity (stable across `source_query` edits; distinct per watermark column). Default derives it from source+dest |
| `seed_watermark` / `skip_to_max` | Seed the cursor on the first run only (explicit floor, or the source's current max) — skips a doomed first full pull; mutually exclusive |
| `advance_watermark` | `False` reads+merges a window without advancing the cursor (backfill without rewinding the schedule); default `True` |
| `merge_prune_partition_by` | BigQuery incremental: prune the MERGE's destination scan to the staging range on this column. Only safe for an *immutable* partition column (e.g. `created_at`) — never a mutable `updated_at` (would insert dup keys) |
| `chunk_rows` | Read in keyset-ordered chunks of N rows, committing the cursor per chunk so a mid-read failure resumes. Incremental + ClickHouse dest only; keyset column must be a unique NOT-NULL integer. `None` = one read (default) |
| `retry_max_attempts` | Re-run the whole transfer on a transient *source* error (PG recovery-conflict/cancel; MySQL gone-away/lock-wait/deadlock). `1` = no retry (default) |
| `column_transforms` | Per-column SQL value transforms over `source_table=` (e.g. `{"ts":"ts AT TIME ZONE 'UTC'"}`), preserving range partitioning. Postgres/MySQL only |
| `evolve_schema` | Auto-`ADD COLUMN` (Nullable) when the source has a column the destination lacks, instead of erroring. ADD-only. Default `False` |
| `key` | Dedup key (required for BigQuery incremental) |
| `create_if_missing` | Auto-create the destination table (default `True`) |
| `engine`, `order_by`, `partition_by`, `primary_key` | DDL knobs, interpreted per destination |
| `parallelism` | Concurrent read streams |
| `batch_rows` / `batch_bytes` | Per-batch size knobs (rows, or estimated bytes) |
| `max_memory_bytes` | Hard ceiling on total in-flight memory; decoding blocks when hit (default 512 MiB, `0` = unbounded) |
| `read_max_rows_per_sec` | Cap the aggregate source read rate to be gentle on a small DB; `None` = unlimited (default). Postgres/MySQL only |
| `type_overrides` | Force a destination column type, e.g. `{"qty": "Decimal(18, 3)"}` |
| `rename`, `include`, `exclude` | Column renames and allow/deny lists |
| `on_progress` | Progress callback |

## How types are mapped

quickhouse maps each source type to a sensible destination type automatically:
integers to integers, floats to floats, text/JSON/UUID to strings, dates and
timestamps across as-is, and booleans preserved. A few deliberate choices worth
knowing:

- **Arbitrary-precision decimals** (`numeric`/`DECIMAL`/`NUMERIC`) default to
  `Float64`, since precision can't be recovered from the type alone — pin an
  exact type with `type_overrides` (e.g. `"Decimal(18, 2)"`, `P <= 38`) and the
  value is decoded exactly (no `Float64` round-trip), not just declared with
  the right destination type. A value that doesn't fit the declared precision,
  or is NaN/Infinity (PostgreSQL `numeric` only), coerces to `NULL` with a
  warning, same as the out-of-range-date handling below. `P > 38`
  (`Decimal256`) isn't supported yet and is rejected as a config error up
  front, rather than silently falling back to `Float64`.
- **`TIME`** columns transfer as canonical text into a `String` column
  (ClickHouse has no time-of-day type).
- **MySQL `DATETIME`/`TIMESTAMP`** map to a UTC-aware timestamp (BigQuery
  `TIMESTAMP`, ClickHouse `DateTime64(6, 'UTC')`) — the wall-clock value is read
  as UTC, matching how a `TIMESTAMP` column expects it. To land a column as a
  naive BigQuery `DATETIME` instead, opt out per-column with
  `type_overrides={"col": "DATETIME"}` — that flips the actual encoding, not
  just the declared type. (PostgreSQL keeps the distinction natively:
  `timestamptz` → UTC-aware, `timestamp` → naive.)
- **Out-of-range dates** (and MySQL zero-dates like `0000-00-00`) coerce to
  `NULL` with a warning rather than failing the transfer.
- **Nullable** source columns stay nullable in the destination.

Arrays and composite (`RECORD`/`STRUCT`) types aren't supported yet.

## Limitations

- **mTLS** (client-certificate auth) isn't supported yet; server TLS is,
  including an extra CA file via `ca_cert_file=...` for providers like AWS RDS.
  (Connect with either a DSN string or discrete `host`/`port`/`user`/`password`/
  `database` fields; BigQuery accepts `credentials_file`, inline
  `credentials_json`, or ADC.)
- **Array / composite types** aren't mapped yet.
- **BigQuery as a source** reads through a single connection — `parallelism`
  becomes a server-side hint rather than true client-side fan-out (a limitation
  of the underlying crate's read API).
- **CDC / streaming** and arbitrary in-source transforms are out of scope.
- **Single table per call** — `sync()` moves one table; loop over a list for many.

## Command-line interface

For cron/CI jobs you can drive a transfer from a TOML file instead of writing
Python:

```bash
pip install "quickhouse[cli]"   # the extra is only needed on Python < 3.11
quickhouse run job.toml
quickhouse --version
```

The job file has three tables — `[source]`, `[target]`, `[sync]`. `type` picks
the engine; every other key maps to that descriptor's constructor / to `sync()`'s
arguments, and `${ENV_VAR}` is expanded so credentials stay in the environment:

```toml
[source]
type = "postgres"
dsn = "${PG_DSN}"

[target]
type = "clickhouse"
url = "http://localhost:8123"
database = "analytics"

[sync]
dest_table = "orders"
source_table = "orders"
mode = "incremental"
watermark = "updated_at"
key = ["id"]
```

See [examples/job.toml](examples/job.toml).

## Stability and versioning

quickhouse follows [Semantic Versioning](https://semver.org). It is **pre-1.0**:
while on `0.x`, minor versions (`0.7 → 0.8`) may contain breaking API changes,
and patch versions (`0.7.0 → 0.7.1`) are backwards-compatible fixes. Every change
is recorded in the [CHANGELOG](CHANGELOG.md); pin a compatible range (e.g.
`quickhouse~=0.7`).

A few surfaces are **experimental** and may change even in a patch release —
they're flagged in the API docstrings: `chunk_rows`, `merge_prune_partition_by`,
`delete_stale_in_window`, `BigQuery(write_method="storage_write")`, and
`column_transforms`. The core `sync()` call and the connection descriptor classes
are considered stable within a minor version.

## Contributing

Bug reports, new source/type mappings, and PRs are welcome — see
[CONTRIBUTING.md](CONTRIBUTING.md) for build steps, tests, and layout.

## License

MIT
