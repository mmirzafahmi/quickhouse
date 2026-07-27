# Examples

Runnable, self-contained scripts live in the
[`examples/`](https://github.com/mmirzafahmi/quickhouse/tree/main/examples)
directory of the repository. Each reads its configuration from environment
variables and documents them in its module docstring.

| Script | What it shows |
|---|---|
| `postgres_to_clickhouse.py` | The minimal call — full-refresh a Postgres table into ClickHouse, with a progress callback. |
| `incremental_sync.py` | Watermark-based incremental sync (idempotent; run it twice). |
| `mysql_to_bigquery.py` | Same `sync()`, different engines — MySQL → BigQuery. |
| `clevertap_bronze_append.py` | HTTP API source with a declared schema + `mode="append"` bronze landing. |

## Running against the local stack

The Postgres/ClickHouse/MySQL examples default to the services in the repo's
`docker-compose.yml`:

```bash
pip install quickhouse
docker compose up -d          # starts Postgres, MySQL, ClickHouse, MinIO
python examples/postgres_to_clickhouse.py
```

The BigQuery and CleverTap examples talk to real cloud services — see each
script's docstring for the credentials and environment variables they need.

## Postgres → ClickHouse

```{literalinclude} ../examples/postgres_to_clickhouse.py
:language: python
```

## Incremental sync

```{literalinclude} ../examples/incremental_sync.py
:language: python
```

## MySQL → BigQuery

```{literalinclude} ../examples/mysql_to_bigquery.py
:language: python
```

## CleverTap bronze append

```{literalinclude} ../examples/clevertap_bronze_append.py
:language: python
```
