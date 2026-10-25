# Reading from databases

**What you'll learn**

- How to connect to PostgreSQL and MySQL, including cloud databases that need
  TLS, such as AWS RDS.
- Three MySQL habits that quietly change your data, and how to stop each one.
- How to read from BigQuery and from another ClickHouse server.

**Before you start:** read [Your first sync](first-sync.md) and
[Keeping tables up to date](incremental.md). The examples use the practice
environment from [Set up a practice environment](setup.md).

## The task

Daily Grind's data lives in four kinds of database:

- The ordering app runs on **PostgreSQL**. In production it is an AWS RDS
  instance that only accepts encrypted connections.
- The inventory system runs on **MySQL**. It is an older app with a few
  surprises in its schema.
- The marketing team keeps campaign data in **BigQuery**.
- The loyalty app writes raw events into a **ClickHouse** database called `raw`.

The `sync()` call is the same for all of them, so this page is about
connecting, and about each database's quirks.

## PostgreSQL

You have already seen the short form, a connection string (also called a
DSN):

```python
import quickhouse as qh

src = qh.Postgres("postgresql://etl:etl@localhost:5432/etl")
```

To read a table in another schema, name it the usual way, for example
`source_table="sales.orders"`.

### Passwords with special characters

Characters like `@`, `/` or `#` have a meaning inside a connection string, so
a password such as `p@ss/w:rd#1` breaks it. The run fails with a confusing
error about the host:

```text
postgres error: error connecting to server: failed to lookup address information
```

The easy fix is to pass each part separately. quickhouse then encodes the
password for you:

```python
import os

src = qh.Postgres(
    host="orders-db.internal",
    port=5432,
    user="reporting",
    password=os.environ["ORDERS_DB_PASSWORD"],
    database="shop",
)
```

Pass either a connection string or the separate fields, not both. Mixing them
is an error.

### Encrypted connections (AWS RDS)

Whether PostgreSQL uses TLS is controlled by `sslmode` in the connection
string: `disable`, `prefer` (the default: use TLS when the server offers it) or
`require`. Use `require` for anything that crosses a network you don't
control.

AWS RDS signs its certificates with Amazon's own certificate authority. Download
AWS's bundle (`https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem`)
and pass it as `ca_cert_file`; it is trusted on top of the usual public ones.
Since `sslmode` only exists in the connection string, build one, and encode the
password with Python's `quote`:

```python
import os
from urllib.parse import quote

password = quote(os.environ["ORDERS_DB_PASSWORD"], safe="")

src = qh.Postgres(
    f"postgresql://reporting:{password}@orders-db.abc123.ap-southeast-1.rds.amazonaws.com"
    "/shop?sslmode=require",
    ca_cert_file="global-bundle.pem",
)
```

With `require`, quickhouse checks both that the certificate is signed by an
authority it trusts and that it was issued for the host you connect to.
`ca_cert_file` doesn't turn TLS on by itself, so keep `sslmode=require` in the
string.

If your database team uses client certificates (mutual TLS), add
`client_cert_file` and `client_key_file`, both PEM files. Always set the two
together. Setting only one is a configuration error.

### Being a good guest

The ordering app's database serves customers first. Two habits keep your sync
out of its way:

- **Name your connection.** quickhouse shows up in `pg_stat_activity` as
  `quickhouse`. Give each job its own name, such as
  `sync(..., application_name="daily-grind-orders")`, so a database
  administrator can tell your jobs apart.
- **Set a time limit.** `statement_timeout_secs` on `Postgres(...)` asks the
  server to cancel a query that runs too long. It counts the *whole* transfer,
  including time spent writing to the destination, so size it generously and
  pair it with `read_idle_timeout_secs` on `sync()`, which fails only when the
  source sends no rows for that long, including before the first one.
  [Running in production](production.md) explains the difference.

To keep the load off the main database entirely, point quickhouse at a read
replica. [Big tables and busy databases](performance.md) covers that and more.

### Read-only sessions

A database user that can only read is the main guard against a `source_query`
that writes, for example one that calls a function with side effects. On top of
that, you can ask PostgreSQL for a read-only session, which refuses any write
whatever the user is allowed to do. `Postgres(...)` has no argument for it,
so add it to the connection string:

```python
src = qh.Postgres("postgresql://etl:etl@localhost:5432/etl"
                  "?options=-c%20default_transaction_read_only%3Don")
```

Reads work as before. A query that would write or take locks, such as
`SELECT * FROM stores FOR UPDATE`, now fails before anything is copied:

```text
postgres error: ERROR [25006] cannot execute SELECT FOR UPDATE in a read-only transaction
```

The separate `host=`, `user=` fields can't carry this option, so use a
connection string.

## MySQL

MySQL takes the same two forms, a connection string or separate fields:

```python
src = qh.MySQL("mysql://etl:etl@localhost:3306/etl")
```

### Encrypted connections

MySQL has no `sslmode`, so you ask for TLS with `require_tls=True`. On AWS RDS
you also pass the same certificate bundle:

```python
src = qh.MySQL(
    host="inventory-db.abc123.ap-southeast-1.rds.amazonaws.com",
    user="reporting",
    password=os.environ["INVENTORY_DB_PASSWORD"],
    database="erp",
    require_tls=True,
    ca_cert_file="global-bundle.pem",
)
```

quickhouse checks the server's certificate, and the two errors you are likely
to meet point straight at the fix:

- `invalid peer certificate: UnknownIssuer` means quickhouse doesn't trust the
  authority that signed the certificate. Pass the right `ca_cert_file`.
- `certificate not valid for name "..."` means you connected by a name that
  isn't on the certificate, such as an IP address or `localhost` through a
  tunnel. Connect using the hostname the certificate was issued for.

Client certificates work as for PostgreSQL: `client_cert_file` and
`client_key_file`, together (PEM or DER).

### A read-only session

MySQL has a switch for the read-only session described for
[PostgreSQL](#read-only-sessions):

```python
src = qh.MySQL("mysql://etl:etl@localhost:3306/etl", read_only=True)
```

Every connection quickhouse opens then refuses statements that would write,
and a `source_query` such as `SELECT * FROM stock_moves FOR UPDATE` fails as
the run starts, having written nothing:

```text
mysql error: Server error: `ERROR 25006 (1792): Cannot execute statement in a READ ONLY transaction.'
```

It needs MySQL 5.7.20 or later. An older server fails the connection rather
than reading without it.

### Trap 1: tinyint(1) columns that aren't true/false

MySQL has no real boolean type. `BOOL` is just another name for `tinyint(1)`,
so quickhouse reads every `tinyint(1)` column as true/false. Usually that is
right. But Daily Grind's inventory system stores a priority level from 0 to 3
in a `tinyint(1)` column. Sync it:

```python
src = qh.MySQL("mysql://etl:etl@localhost:3306/etl")
dst = qh.ClickHouse("http://localhost:8123", database="analytics")

result = qh.sync(src, dst, source_table="stock_moves", dest_table="stock_moves",
                 mode="incremental", watermark="write_date", key=["id"])
for w in result.warnings:
    print(w.kind, w.column, w.count)
```

```text
collapsed_bool priority 500
```

500 rows held a 2 or a 3, and each one landed as `true`. The difference between
"urgent" and "normal" is gone. quickhouse can't undo that afterwards, which is
why it warns you during the run.

The fix is `tinyint1_as_bool=False`, which reads `tinyint(1)` as the small
integer it really is (`Int8` in ClickHouse). Because the table already exists
with a true/false column, you also need to rebuild it, so drop it:

```bash
docker compose exec clickhouse clickhouse-client --query "DROP TABLE analytics.stock_moves"
```

```python
result = qh.sync(src, dst, source_table="stock_moves", dest_table="stock_moves",
                 mode="incremental", watermark="write_date", key=["id"],
                 tinyint1_as_bool=False)
print(result.rows_written)
for w in result.warnings:
    print(w.kind, w.sample)
```

```text
1000
recreated_destination 2026-10-25 06:10:36
```

All 1,000 rows are copied again, and `priority` now holds 0, 1, 2 and 3.
quickhouse still had the saved position from the first run, but because it had
to create the table again, it read from the start instead. The
`recreated_destination` warning says so, with the old position as its
`sample`.

```{tip}
Set `tinyint1_as_bool=False` from the start for apps known to store numbers in
`tinyint(1)`, such as Odoo. To make the warning stop a run instead of just
reporting it, see [Catching bad data](data-quality.md).
```

### Trap 2: TIMESTAMP columns on a server that isn't in UTC

MySQL has two date-and-time types, and they behave differently:

- `DATETIME` stores the date and time exactly as written. It has no time zone.
- `TIMESTAMP` stores an exact moment, but *shows* it in the time zone of the
  connection.

quickhouse stores both as UTC timestamps. For a `TIMESTAMP` that only works
when the connection's time zone is UTC. Daily Grind's inventory server runs on
Jakarta time (`+07:00`). A delivery received at 01:00 UTC is shown as 08:00, and
lands in ClickHouse as 08:00 UTC: seven hours off. quickhouse spots this and
warns you once per column, with the offset as the warning's `sample`:

```text
shifted_timestamp received_at UTC+07:00
```

The fix is `utc_session=True`, which makes every connection use UTC. The
delivery then lands as 01:00 UTC, as it should:

```python
src = qh.MySQL(host="inventory-db.internal", user="reporting",
               password=os.environ["INVENTORY_DB_PASSWORD"], database="erp",
               utc_session=True)
```

`utc_session` is off by default because turning it on moves values in an
existing pipeline. Before you switch:

- **Rows already copied** keep the old, shifted value until they are written
  again. A full refresh rewrites them all.
- **A saved watermark from a `TIMESTAMP` column** moves too. On a server east of
  UTC, like Jakarta, it jumps ahead by the offset, and rows inside that jump are
  skipped. For the first run after the switch, set `lookback_seconds` to at
  least the offset (`25200` seconds for `+07:00`), or start over with a new
  `state_key`.
- **`NOW()` and `CURDATE()`** inside a `source_query` return UTC values too.

`DATETIME` columns are not affected either way. If you would rather keep a
`TIMESTAMP` as the local wall-clock time on purpose, store it without a time
zone using `type_overrides={"received_at": "DATETIME"}`.
[Type mapping](../guide/type-mapping.md) has the details.

### Trap 3: zero dates

Older MySQL apps often store "no date" as `0000-00-00`, which isn't a real
date. quickhouse turns these, and dates outside the years 1900 to 2299 that
ClickHouse can store, into `NULL` instead of failing the run. Each affected
column gets a `coerced_date` warning with a count:

```text
coerced_date contract_end 2
coerced_date last_audit_at 1
```

## BigQuery as a source

BigQuery can be a source as well as a destination. Daily Grind's marketing
team keeps ad spend in `marketing.campaign_spend`, and store managers want it
next to their sales in ClickHouse:

```python
src = qh.BigQuery("daily-grind-data", credentials_file="bq-reader.json")
dst = qh.ClickHouse("http://localhost:8123", database="analytics")

qh.sync(src, dst,
        source_table="marketing.campaign_spend",
        dest_table="campaign_spend",
        mode="full",
        key=["campaign_id", "spend_date"],
        numeric_as_decimal="Decimal(18, 2)")
```

- **Name tables as `dataset.table`**, or `project.dataset.table` for a table in
  another project.
- **Credentials** come from a key file (`credentials_file`), the key's contents
  (`credentials_json`, for example from a secrets manager), or, if you pass
  neither, Application Default Credentials such as
  `gcloud auth application-default login`.
- **BigQuery `NUMERIC` lands as a float** unless you ask otherwise, which can
  turn `32.9` into `32.89999999999999`. For money, set `numeric_as_decimal` (as
  above) or a per-column `type_overrides`.
- **BigQuery does the parallel work itself.** `parallelism` is only a hint,
  `read_max_rows_per_sec` doesn't apply, and neither does `column_transforms`.

## ClickHouse as a source

The loyalty app writes raw events into a ClickHouse database called `raw`. The
cleaned-up copy that dashboards read lives in `analytics`. Copying between them
is an ordinary sync, with the same `ClickHouse` class on both sides:

```python
src = qh.ClickHouse("http://localhost:8123", database="raw")
dst = qh.ClickHouse("http://localhost:8123", database="analytics")

qh.sync(src, dst,
        source_table="app_sessions",
        dest_table="app_sessions",
        mode="incremental", watermark="updated_at", key=["session_id"],
        exclude=["screens"])
```

The source and destination can be on the same server or on different
clusters. `source_table` can also be written `"raw.app_sessions"` to read from
a database other than the descriptor's.

```{tip}
A `ReplacingMergeTree` table may still hold old versions of some rows (see
[Keeping tables up to date](incremental.md)). quickhouse checks the source
table's engine and reads such a table with `FINAL`, so only the newest version
of each row is copied, and the log says so. A `source_query` is sent as
written, so add `FINAL` to it yourself, for example
`source_query="SELECT * FROM raw.app_sessions FINAL"`. To copy every stored
version instead, pass `final=False` to the source's `qh.ClickHouse(...)`.
```

ClickHouse-only types survive the copy. In the example, `UUID`, `IPv4`,
`Enum8`, `LowCardinality(String)`, `FixedString(3)` and
`DateTime64(3, 'Asia/Jakarta')` columns all arrive with the same types. Copied
to BigQuery instead, `UUID`, IP, enum and fixed-string columns become `STRING`.

Some types can't be read yet: `Array`, `Map`, `Tuple`, `Nested`, `JSON`, the
256-bit integers and `Decimal256`. The run stops with an error that names the
column and its type, here `screens` and `Array(String)`, and suggests the two
fixes. That's why the example excludes `screens`. To keep it, turn it into text
in a query:

```python
qh.sync(src, dst, dest_table="app_sessions_flat", mode="full", key=["session_id"],
        source_query="""
            SELECT session_id, platform, arrayStringConcat(screens, ',') AS screens
            FROM raw.app_sessions
        """)
```

Two ClickHouse-specific settings: `statement_timeout_secs` on the source
descriptor sets the server's `max_execution_time`, and `settings={...}` passes
any other ClickHouse setting. Values in `settings` always win.

## Recap

- Use separate fields, or `quote()`, for passwords with special characters.
- TLS: `sslmode=require` for PostgreSQL, `require_tls=True` for MySQL, and
  AWS's bundle as `ca_cert_file` on RDS.
- For a session that refuses writes, use `read_only=True` on MySQL and
  `default_transaction_read_only` in a PostgreSQL connection string.
- Watch for MySQL's `collapsed_bool`, `shifted_timestamp` and `coerced_date`
  warnings. The fixes are `tinyint1_as_bool=False` and `utc_session=True`.
- To rebuild an incremental table, drop it. The next run reads everything
  again and raises `recreated_destination`.
- BigQuery tables are named `dataset.table`. ClickHouse to ClickHouse keeps
  ClickHouse-only types, but arrays and maps must be excluded or converted.

## Next

[Pulling from HTTP APIs](http-apis.md): load CleverTap, AppsFlyer and any other
REST or CSV endpoint.
