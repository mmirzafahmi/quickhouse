# Command-line interface

For cron/CI jobs you can drive a transfer from a TOML file instead of writing
Python:

```bash
pip install "quickhouse[cli]"   # the extra is only needed on Python < 3.11
quickhouse run job.toml
quickhouse --version
```

## The job file

The job file has three tables — `[source]`, `[target]`, `[sync]`. `type` picks
the engine; every other key maps to that descriptor's constructor / to
`sync()`'s arguments. String values are passed through `os.path.expandvars`, so
`${ENV_VAR}` lets you keep credentials out of the file.

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

## Recognized `type` values

`[source]`
: `postgres`, `mysql`, `bigquery`, `clevertap`, `appsflyer`

`[target]`
: `clickhouse`, `bigquery`

Any other key in a table maps straight to the matching constructor argument (see
[Sources & destinations](guide/sources.md) and the [API reference](api.md)). A
misspelled or missing key produces a clear config error naming the section.

The `[sync]` table's keys are exactly the keyword arguments of
{func}`quickhouse.sync`.

See [`examples/job.toml`](https://github.com/mmirzafahmi/quickhouse/blob/main/examples/job.toml)
in the repository for a complete file.
