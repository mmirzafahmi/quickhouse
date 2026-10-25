# Set up a practice environment

The tutorials are easier to follow when you can run the examples yourself. This
page starts PostgreSQL, MySQL and ClickHouse on your own machine and fills them
with Daily Grind's sample data. It takes about five minutes.

**What you need:** Python 3.9 or newer, [Docker](https://docs.docker.com/get-docker/)
with Docker Compose, and a copy of the
[quickhouse repository](https://github.com/mmirzafahmi/quickhouse) (for its
`docker-compose.yml`).

```{note}
You can skip this page if you already have a database and a ClickHouse server
to practise on. Just swap in your own connection details as you read.
```

## 1. Install quickhouse

```bash
python -m venv .venv
source .venv/bin/activate          # on Windows: .venv\Scripts\activate
pip install "quickhouse[pandas,progress]"
```

The two extras add pandas support (used in [Loading DataFrames](dataframes.md))
and a progress bar. Check that it worked:

```bash
python -c "import quickhouse; print(quickhouse.version())"
```

## 2. Start the databases

From the root of the quickhouse repository:

```bash
docker compose up -d
```

This starts four services:

| Service | Address | Login |
| --- | --- | --- |
| PostgreSQL | `localhost:5432` | user `etl`, password `etl`, database `etl` |
| MySQL | `localhost:3306` | user `etl`, password `etl`, database `etl` |
| ClickHouse | `http://localhost:8123` | user `default`, no password |
| MinIO (S3-compatible storage) | `http://localhost:9010` (web console on `9011`) | user `minioadmin`, password `minioadmin` |

Give them a few seconds to start. `docker compose ps` shows `healthy` when they
are ready.

## 3. Load the ordering app's data into PostgreSQL

Save this as `seed_postgres.sql`. It creates Daily Grind's `stores`,
`customers` and `orders` tables and fills them with 50,000 orders spread over
the last five weeks:

```sql
CREATE TABLE stores (
    id        integer PRIMARY KEY,
    name      text    NOT NULL,
    city      text    NOT NULL,
    opened_on date    NOT NULL
);

INSERT INTO stores VALUES
    (1, 'Central Station', 'Jakarta',  '2021-03-01'),
    (2, 'Riverside Mall',  'Bandung',  '2022-07-15'),
    (3, 'Airport T3',      'Jakarta',  '2023-01-10'),
    (4, 'Old Town',        'Surabaya', '2024-05-20');

CREATE TABLE customers (
    id         bigint      PRIMARY KEY,
    full_name  text        NOT NULL,
    email      text        NOT NULL,
    phone      text,
    tier       text        NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

INSERT INTO customers
SELECT g,
       'Customer ' || g,
       'customer' || g || '@example.com',
       '+62-800-' || lpad(g::text, 4, '0'),
       (ARRAY['bronze', 'silver', 'gold'])[1 + g % 3],
       now() - interval '400 days' + (g || ' hours')::interval,
       now() - interval '400 days' + (g || ' hours')::interval
FROM generate_series(1, 500) AS g;

CREATE TABLE orders (
    id          bigint         PRIMARY KEY,
    store_id    integer        NOT NULL,
    customer_id bigint,
    status      text           NOT NULL,
    total       numeric(12, 2) NOT NULL,
    created_at  timestamptz    NOT NULL,
    updated_at  timestamptz    NOT NULL
);

INSERT INTO orders
SELECT g,
       1 + g % 4,
       1 + g % 500,
       (ARRAY['paid', 'paid', 'paid', 'refunded'])[1 + g % 4],
       round((2 + random() * 8)::numeric, 2),
       now() - ((50000 - g) || ' minutes')::interval,
       now() - ((50000 - g) || ' minutes')::interval
FROM generate_series(1, 50000) AS g;

CREATE INDEX ON orders (updated_at);
CREATE INDEX ON customers (updated_at);
```

Load it:

```bash
docker compose exec -T postgres psql -U etl -d etl < seed_postgres.sql
```

## 4. Load the inventory system's data into MySQL

Save this as `seed_mysql.sql`. It creates the `stock_moves` table with 1,000
stock movements:

```sql
CREATE TABLE stock_moves (
    id          bigint         NOT NULL PRIMARY KEY,
    product_id  int            NOT NULL,
    store_id    int            NOT NULL,
    qty         decimal(12, 3) NOT NULL,
    priority    tinyint(1)     NOT NULL DEFAULT 0,
    create_date datetime       NOT NULL,
    write_date  datetime       NOT NULL,
    KEY (write_date)
);

INSERT INTO stock_moves
WITH RECURSIVE n AS (SELECT 1 AS i UNION ALL SELECT i + 1 FROM n WHERE i < 1000)
SELECT i,
       100 + i % 25,
       1 + i % 4,
       ROUND(1 + RAND() * 20, 3),
       i % 4,
       NOW() - INTERVAL (1000 - i) HOUR,
       NOW() - INTERVAL (1000 - i) HOUR
FROM n;
```

Load it:

```bash
docker compose exec -T mysql mysql -uetl -petl etl < seed_mysql.sql
```

(The `priority` column holds the numbers 0 to 3 even though it is declared
`tinyint(1)`. That is on purpose: [Reading from databases](databases.md) shows
why it matters.)

## 5. Create the ClickHouse database

quickhouse creates tables for you, but not databases. Create the `analytics`
database that the tutorials write into:

```bash
docker compose exec clickhouse clickhouse-client --query "CREATE DATABASE IF NOT EXISTS analytics"
```

## 6. Check that everything is connected

Save this as `check.py` and run it with `python check.py`:

```python
import quickhouse as qh

src = qh.Postgres("postgresql://etl:etl@localhost:5432/etl")
dst = qh.ClickHouse("http://localhost:8123", database="analytics")

result = qh.sync(src, dst, source_table="stores", dest_table="stores", mode="full")
print(result.rows_written, "rows copied")
```

You should see `4 rows copied`. If you get a connection error instead, check
that `docker compose ps` lists every service as running.

## Running a query in ClickHouse

The tutorials often ask you to look at a table in ClickHouse. The quickest way
is the built-in client:

```bash
docker compose exec clickhouse clickhouse-client --query "SELECT * FROM analytics.stores"
```

You can also open `http://localhost:8123/play` in a browser and type queries
there.

## Starting over

To throw everything away and start fresh:

```bash
docker compose down -v      # -v also deletes the data volumes
docker compose up -d
```

Then repeat steps 3 to 5.

## Next

You're ready. Go to [Your first sync](first-sync.md).
