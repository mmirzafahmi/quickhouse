# Sources

Every transfer is `sync(source, target, ...)`. A **source** is a `Postgres`,
`MySQL`, `BigQuery`, `ClickHouse`, `CleverTap`, or `AppsFlyer` connection
descriptor. The `BigQuery` and `ClickHouse` classes each also work as a
[destination](../destinations/index.md); everything else about the call is
identical regardless of which engines you use.

```python
import quickhouse as qh

qh.Postgres("postgresql://user:pw@host:5432/db")
qh.MySQL("mysql://user:pw@host:3306/db", require_tls=True)
qh.BigQuery("my-gcp-project")                       # source_table="dataset.table"
qh.ClickHouse("http://ch-a:8123", database="raw")   # ClickHouse reads too
```

A DataFrame you already hold is not a descriptor — it goes through
[`from_pandas()`](dataframes.md) instead, which takes the frame and a target:

```python
qh.from_pandas(df, qh.ClickHouse("http://host:8123", database="analytics"),
               dest_table="orders", mode="full", key=["id"])
```

For the exact constructor signatures see the [API reference](../../api.md).

```{raw} html
<div class="qh-modes">
  <a class="qh-mode qh-mode--current" href="databases.html">
    <div class="qh-mode__name">Databases</div>
    <div class="qh-mode__desc">PostgreSQL, MySQL, BigQuery and ClickHouse, read over their native wire protocols.</div>
  </a>
  <a class="qh-mode" href="http-apis.html">
    <div class="qh-mode__name">HTTP APIs</div>
    <div class="qh-mode__desc">CleverTap and AppsFlyer, with a schema you declare up front.</div>
  </a>
  <a class="qh-mode" href="dataframes.html">
    <div class="qh-mode__name">DataFrames</div>
    <div class="qh-mode__desc">pandas, polars or pyarrow, already in memory &mdash; via <code>from_pandas()</code>.</div>
  </a>
</div>
```

## Authentication

Databases accept a DSN string **or** discrete fields (pass one or the other, not
both). The discrete fields are percent-encoded and assembled into a DSN, so
special characters in a password survive:

```python
qh.Postgres("postgresql://user:pw@host:5432/db")            # DSN
qh.Postgres(host="host", port=5432, user="u",               # discrete fields
            password="p@ss/word", database="shop")
```

TLS:

```{raw} html
<div class="qh-params">
  <div>
    <div>
      <div class="qh-params__name">sslmode</div>
      <div class="qh-params__type">DSN parameter &mdash; PostgreSQL</div>
    </div>
    <p class="qh-params__desc">Standard PostgreSQL DSN parameter: <code>disable</code> | <code>prefer</code> (default) | <code>require</code>. <code>require</code> verifies the certificate and the host name, as libpq's <code>verify-full</code> does; <code>verify-ca</code> and <code>verify-full</code> themselves are rejected. Only a DSN can set it: the discrete fields always <code>prefer</code>.</p>
  </div>
  <div>
    <div>
      <div class="qh-params__name">require_tls</div>
      <div class="qh-params__type">bool &mdash; MySQL</div>
    </div>
    <p class="qh-params__desc">MySQL has no <code>sslmode</code> convention, so require TLS explicitly with <code>require_tls=True</code>. It replaces any TLS settings in the DSN (<code>verify_ca=false</code> included) and verifies the certificate in full.</p>
  </div>
  <div>
    <div>
      <div class="qh-params__name">ca_cert_file</div>
      <div class="qh-params__type">str, optional</div>
    </div>
    <p class="qh-params__desc">Add a private CA (e.g. AWS RDS's regional bundle) &mdash; trusted in addition to the public CA store. On MySQL it turns TLS on, as <code>require_tls</code> does; on PostgreSQL it doesn't, so pair it with <code>sslmode=require</code>.</p>
  </div>
  <div>
    <div>
      <div class="qh-params__name">client_cert_file / client_key_file</div>
      <div class="qh-params__type">str, optional &mdash; mTLS</div>
    </div>
    <p class="qh-params__desc">Client-certificate auth for Postgres and MySQL. Set <strong>together</strong> (both PEM; passing only one is a config error).</p>
  </div>
</div>
```

```python
qh.Postgres(
    "postgresql://user@host:5432/db?sslmode=require",
    ca_cert_file="rds-ca.pem",
    client_cert_file="client.crt",
    client_key_file="client.key",   # mTLS: both files, or neither
)
```

**Read-only sessions.** A read-only database user is the guard against a
`source_query` that writes. On top of one, ask the server for a read-only
session, so it refuses a write whatever the user's grants allow:

```python
qh.MySQL("mysql://reader:pw@host:3306/db", read_only=True)
qh.Postgres("postgresql://reader:pw@host:5432/db"
            "?options=-c%20default_transaction_read_only%3Don")
```

`read_only=True` adds `transaction_read_only = ON` to the `SET SESSION` every
MySQL connection of the run starts with, so a statement that would write, such
as a `source_query` calling a function that inserts, or `SELECT ... FOR
UPDATE`, fails with error 1792. It needs MySQL 5.7.20 or later; on an older
server the connection fails rather than reading without it. PostgreSQL has no
keyword for it: the DSN's `options` parameter sends
`default_transaction_read_only=on` with the connection, and a write fails with
SQLSTATE `25006`. The discrete `host=` fields can't carry it, so use a DSN.

**BigQuery** authenticates with a service-account key file, inline JSON
contents, or Application Default Credentials (ADC):

```{raw} html
<div class="qh-params">
  <div>
    <div>
      <div class="qh-params__name">credentials_file</div>
      <div class="qh-params__type">str, optional</div>
    </div>
    <p class="qh-params__desc">Path to a service-account key file, e.g. <code>"key.json"</code>.</p>
  </div>
  <div>
    <div>
      <div class="qh-params__name">credentials_json</div>
      <div class="qh-params__type">str, optional</div>
    </div>
    <p class="qh-params__desc">Inline service-account JSON contents (e.g. <code>os.environ["SA_JSON"]</code>, from a secrets manager). Takes precedence over <code>credentials_file</code>.</p>
  </div>
</div>
```

Neither set: falls back to Application Default Credentials. The same
credentials work whether `BigQuery` is used as a source or a
[destination](../destinations/bigquery.md).

**ClickHouse** authenticates with the same `user` / `password` it does as a
[destination](../destinations/clickhouse.md) — one class, either role:

```python
qh.ClickHouse("https://host:8443", database="raw", user="reader", password="…")
```

```{toctree}
:hidden:

databases
http-apis
dataframes
```
