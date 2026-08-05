# Sources

Every transfer is `sync(source, target, ...)`. A **source** is a `Postgres`,
`MySQL`, `BigQuery`, `CleverTap`, or `AppsFlyer` connection descriptor. The same
`BigQuery` class also works as a [destination](../destinations/index.md); everything
else about the call is identical regardless of which engines you use.

```python
import quickhouse as qh

qh.Postgres("postgresql://user:pw@host:5432/db")
qh.MySQL("mysql://user:pw@host:3306/db", require_tls=True)
qh.BigQuery("my-gcp-project")                       # source_table="dataset.table"
```

For the exact constructor signatures see the [API reference](../../api.md).

```{raw} html
<div class="qh-modes">
  <a class="qh-mode qh-mode--current" href="databases.html">
    <div class="qh-mode__name">Databases</div>
    <div class="qh-mode__desc">PostgreSQL, MySQL and BigQuery, read over their native wire protocols.</div>
  </a>
  <a class="qh-mode" href="http-apis.html">
    <div class="qh-mode__name">HTTP APIs</div>
    <div class="qh-mode__desc">CleverTap and AppsFlyer, with a schema you declare up front.</div>
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
    <p class="qh-params__desc">Standard PostgreSQL DSN parameter: <code>disable</code> | <code>prefer</code> (default) | <code>require</code>.</p>
  </div>
  <div>
    <div>
      <div class="qh-params__name">require_tls</div>
      <div class="qh-params__type">bool &mdash; MySQL</div>
    </div>
    <p class="qh-params__desc">MySQL has no <code>sslmode</code> convention, so require TLS explicitly with <code>require_tls=True</code>.</p>
  </div>
  <div>
    <div>
      <div class="qh-params__name">ca_cert_file</div>
      <div class="qh-params__type">str, optional</div>
    </div>
    <p class="qh-params__desc">Add a private CA (e.g. AWS RDS's regional bundle) &mdash; trusted in addition to the public CA store.</p>
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

```{toctree}
:hidden:

databases
http-apis
```
