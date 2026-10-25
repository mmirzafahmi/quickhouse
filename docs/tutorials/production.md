# Running in production

**What you'll learn**

- How to run a sync from a job file with the `quickhouse` command.
- How to run syncs on a schedule with cron, Airflow or Dagster.
- How to keep passwords out of your code.
- What happens when a run fails, and how retries and timeouts help.
- How to read the logs and look after the state table.

**Before you start:** read [Keeping tables up to date](incremental.md). Most
production jobs are incremental syncs.

## The task

Daily Grind's syncs work on your laptop. Now they have to run every 15 minutes
on a server, with nobody watching. If something breaks at 3 a.m., the next run
should recover on its own when it can, and someone should hear about it when it
can't.

## Describe a sync in a job file

A scheduled job doesn't need a Python script. You can write the settings in a
[TOML](https://toml.io) file instead. Save this as `jobs/orders.toml`:

```toml
[source]
type = "postgres"
dsn = "${ORDERS_DB_DSN}"

[target]
type = "clickhouse"
url = "http://localhost:8123"
database = "analytics"

[sync]
source_table = "orders"
dest_table = "orders_live"
mode = "incremental"
watermark = "updated_at"
key = ["id"]
```

Run it, and it prints the `TransferResult` when it finishes:

```bash
export ORDERS_DB_DSN="postgresql://etl:etl@localhost:5432/etl"
quickhouse run jobs/orders.toml
```

The file has three parts:

- `[source]` says where to read. `type` is `postgres`, `mysql`, `bigquery`,
  `clickhouse`, `clevertap` or `appsflyer`. Every other line is an argument to
  that class, named as in Python: this one is `qh.Postgres(dsn=...)`.
- `[target]` says where to write. `type` is `clickhouse` or `bigquery`.
- `[sync]` holds the arguments of `qh.sync()`.

`${ORDERS_DB_DSN}` is replaced with the value of that environment variable when
the job runs, so the password never appears in the file. On Python 3.9 and
3.10, install the TOML reader first with `pip install "quickhouse[cli]"`.

In TOML, `True` and `False` are written `true` and `false`, lists look the same
as in Python, and a dictionary is written `{ name = "value" }`:

```toml
[sync]
exclude = ["email", "phone"]
rename = { full_name = "name" }
tinyint1_as_bool = false
retry_max_attempts = 3
fail_on_warnings = ["null_watermark", "coerced_decimal"]
```

A wrong `type`, or a misspelled setting in `[source]` or `[target]`, gets an
error that names the part of the file, such as
`[source] unknown type 'postgresql'`. A misspelled setting in `[sync]` shows up
as a Python `TypeError` that names the setting.

```{warning}
If an environment variable isn't set, the text `${ORDERS_DB_DSN}` is passed on
unchanged, and the run fails with a confusing error such as
`invalid connection string: unexpected EOF`. Check the variable first.
```

A job file only holds plain values, so options that need a Python object only
work from a script: `archive=` (see [Backing up to cloud storage](backups.md)),
`validate=` (see [Catching bad data](data-quality.md)) and `on_progress=`. The
generic `HttpApi` source isn't available as a `type` either. Each file runs one
sync, so ten tables need ten files.

## Run it on a schedule

### cron

On a plain Linux server, this crontab line runs the job every 15 minutes:

```text
*/15 * * * * cd /opt/daily-grind && . ./secrets.env && NO_COLOR=1 flock -n /tmp/orders.lock .venv/bin/quickhouse run jobs/orders.toml >> logs/orders.log 2>&1
```

- `. ./secrets.env` loads a file of `export ORDERS_DB_DSN=...` lines that only
  the job's user can read.
- `NO_COLOR=1` keeps colour codes out of the log file.
- `flock -n` skips a run while the previous one is still going, so two copies
  of the job never overlap.

A failed run exits with a non-zero status, so a cron monitor can alert on it.

### Airflow

Call `qh.sync()` from a task:

```python
import os

import pendulum
import quickhouse as qh
from airflow.decorators import dag, task

@dag(schedule="*/15 * * * *", start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
     catchup=False, max_active_runs=1)
def orders_to_clickhouse():
    @task(retries=2)
    def sync_orders():
        result = qh.sync(
            qh.Postgres(os.environ["ORDERS_DB_DSN"]),
            qh.ClickHouse(os.environ["CLICKHOUSE_URL"], database="analytics"),
            source_table="orders", dest_table="orders_live",
            mode="incremental", watermark="updated_at", key=["id"],
        )
        return {"rows_written": result.rows_written, "watermark": result.new_watermark}

    sync_orders()

orders_to_clickhouse()
```

`max_active_runs=1` stops runs from overlapping, and `retries=2` runs a failed
task again, which is safe, as explained below.

### Dagster

Make the sync an asset and put it on a schedule:

```python
import os

import dagster as dg
import quickhouse as qh

@dg.asset
def orders_live() -> dg.MaterializeResult:
    result = qh.sync(
        qh.Postgres(os.environ["ORDERS_DB_DSN"]),
        qh.ClickHouse(os.environ["CLICKHOUSE_URL"], database="analytics"),
        source_table="orders", dest_table="orders_live",
        mode="incremental", watermark="updated_at", key=["id"],
    )
    return dg.MaterializeResult(metadata={"rows_written": result.rows_written})

orders_job = dg.define_asset_job("orders_job", selection=[orders_live])
schedule = dg.ScheduleDefinition(job=orders_job, cron_schedule="*/15 * * * *")
defs = dg.Definitions(assets=[orders_live], jobs=[orders_job], schedules=[schedule])
```

## Keep secrets out of your code

The examples above read connection details from environment variables, which
your scheduler or secret manager sets. If a password contains characters such
as `@`, `/` or `:`, pass the connection parts separately and quickhouse encodes
them for you:

```python
src = qh.Postgres(host="orders-replica.internal", port=5432, user="etl",
                  password=os.environ["ORDERS_DB_PASSWORD"], database="etl")
```

For BigQuery, `credentials_json=` takes the contents of a service-account key,
for example a value your secret manager puts in an environment variable, so no
key file sits on disk. On a Google Cloud VM or in GKE you can skip keys
altogether: attach a service account to the machine, pass no credentials, and
quickhouse uses Application Default Credentials.

## When a run fails

### Running it again is safe

You never need to clean up after a failed run, and scheduler retries are safe
to turn on:

- **A full refresh** swaps in its new table only at the very end. A failed run
  leaves the old table as it was.
- **An incremental sync** saves its watermark only when the run succeeds, so
  the next run reads from the same place. Rows the failed run had already
  written are written again, and in the tables quickhouse creates for
  incremental syncs, the key makes them replace the earlier copies.

### Errors say what went wrong

A failed sync raises a `RuntimeError` whose message names the tables and says
what is wrong. Errors from the database itself, such as a wrong password, are
passed on in the database's own words.

```text
RuntimeError: orders -> orders_live: configuration error: watermark column
'modified_at' not found in source; available columns: id, store_id, ...
```

There is one exception to the `RuntimeError` rule. When a `validate=` gate
rejects a load (see [Catching bad data](data-quality.md)), `sync()` raises the
gate's own exception: `qh.ValidationFailed`, which is a `RuntimeError`, for a
Great Expectations suite, but whatever your own gate function raised, such as
a `ValueError`, otherwise.

In a Python job, catch the error to send an alert, then raise it again so the
scheduler still marks the run as failed. Catch `Exception` rather than
`RuntimeError`, so a gate's own error raises the alert too:

```python
try:
    qh.sync(src, dst, source_table="orders", dest_table="orders_live",
            mode="incremental", watermark="updated_at", key=["id"])
except Exception as e:
    send_alert(f"orders sync failed: {e}")   # your own alerting function
    raise
```

### Retrying short outages automatically

Some failures fix themselves a moment later. **Writes to the destination** are
retried automatically, with short pauses. **Reads from the source** are retried
when you set `retry_max_attempts`, which re-runs the whole transfer from the
start after a temporary error:

```python
qh.sync(src, dst, source_table="orders", dest_table="orders_live",
        mode="incremental", watermark="updated_at", key=["id"],
        retry_max_attempts=3)   # the first try plus up to two retries
```

"Temporary" means one of these:

- **PostgreSQL:** a lost connection, a query cancelled on a replica because of
  a conflict with recovery (SQLSTATE `40001`), or any other cancelled query
  (`57014`), such as one stopped by a statement timeout.
- **MySQL:** a lost connection, a lock wait timeout, a deadlock, a killed
  query, or `MAX_EXECUTION_TIME`.
- **Any database source:** a read that went quiet for longer than
  `read_idle_timeout_secs` (see below).

Errors that a retry can't fix, such as a wrong password, fail at once. The
default, `retry_max_attempts=1`, means no retries. Each retry is logged as a
warning.

If the failed attempt had already written rows, the retry writes them again.
The result counts them in `rows_written_failed_attempts` and carries a
`retried_after_partial_write` warning. In a `ReplacingMergeTree` table the
extra copies disappear when ClickHouse merges, and `FINAL` never shows them.

### Two timeouts, and which one you want

`statement_timeout_secs`, set on the `Postgres`, `MySQL` or `ClickHouse`
source, makes the database cancel the query after that many seconds. The catch
is that quickhouse streams rows from that open query straight into the
destination, so the clock covers the **whole transfer**, including time spent
waiting for a slow destination. On a day when ClickHouse is busy, a query that
normally takes a second can be cancelled, and the error makes it look like a
source problem. A retry doesn't help, because the next attempt hits the same
limit.

`read_idle_timeout_secs`, set on `sync()`, fails the run if the source sends no
rows for that many seconds: either partway through a read, or before the first
row, for example while the query waits on a lock. Time spent on a slow
destination doesn't count, so you can set it tightly, and `retry_max_attempts`
retries it:

```python
src = qh.Postgres(os.environ["ORDERS_DB_DSN"], statement_timeout_secs=1800)

qh.sync(src, dst, source_table="orders", dest_table="orders_live",
        mode="incremental", watermark="updated_at", key=["id"],
        read_idle_timeout_secs=120, retry_max_attempts=3)
```

Use a generous `statement_timeout_secs` to guard the source database, and a
tight `read_idle_timeout_secs` to catch a source that hangs. See the
[Performance & safety guide](../guide/performance.md) for more.

## Read the logs

quickhouse writes each step to stderr: connecting, planning the read, saving
the watermark, and a summary such as
`transfer complete: 50000 rows in 0.15s (336474 rows/s)`. Three environment
variables change what you see. Set them before the job starts:

- `RUST_LOG=quickhouse_core=debug` adds the SQL that quickhouse runs, which
  helps when a run reads more or fewer rows than you expected.
- `RUST_LOG=quickhouse_core=warn` keeps only warnings and errors.
- `NO_COLOR=1` turns off colour codes for log files.

Warnings appear in the log but don't fail the run. To make a scheduled job fail
on the ones that matter to you, list them in `fail_on_warnings`.
[Catching bad data](data-quality.md) explains each one.

## Look after the state table

Every incremental run adds a row to `_quickhouse_state`. ClickHouse merges old
rows away in the background, but in BigQuery they stay, so the table grows and
each run reads a little more of it. `state_keys()` lists every saved position,
oldest first, and `compact_state()` deletes all but the newest row of each:

```python
for k in qh.state_keys(dst):
    print(f"{k.state_key:<10} {k.dest_table:<12} {k.last_watermark}  rows={k.state_rows}")

print("deleted", qh.compact_state(dst), "old state rows")
```

```text
customers  customers    2025-10-10 04:26:30.824869+00  rows=2
orders     orders_live  2026-10-24 08:26:46.475482+00  rows=3
deleted 3 old state rows
```

Each sync reads only the newest row, so compacting is safe to run between
syncs, for example once a week.

Renaming a job's `state_key`, or retiring a job, leaves its rows behind for
good. `qh.state_keys(dst, idle_days=30)` lists only the positions that no sync
has updated for 30 days. Make sure a job is really gone before you delete its
rows, because a job whose rows are gone starts again from the beginning. In
ClickHouse:

```sql
-- source_table holds the state_key (or the source table when none was set)
DELETE FROM analytics._quickhouse_state
WHERE source_table = 'orders_old' AND dest_table = 'orders_live'
```

If your team has naming rules, `state_table_name="etl_state"` renames the
state table and `staging_suffix="_loading"` renames the staging tables (to
`orders_live_loading_<id>`, for example). Pass the same `state_table_name` to
`state_keys()` and `compact_state()`, and choose it before the first run: if
you change it later, quickhouse won't find the old positions.

## Recap

- A TOML job file and `quickhouse run` run a sync without Python, with
  `${NAME}` pulling secrets from environment variables.
- Schedule with cron, Airflow or Dagster, and don't let two runs of the same
  job overlap. A failed run is safe to run again.
- `retry_max_attempts` retries temporary source errors. Catch hangs with
  `read_idle_timeout_secs`, and size `statement_timeout_secs` generously.
- `RUST_LOG` and `NO_COLOR` control the logs. `state_keys()` and
  `compact_state()` keep the state table tidy.

## Next

[Big tables and busy databases](performance.md): make big transfers fast, keep
memory under control, and read without hurting the source.
