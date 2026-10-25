# Choosing what to copy

**What you'll learn**

- How to copy only some columns, and how to rename them.
- How to keep personal data out of your analytics database.
- How to copy the result of a SQL query instead of a table.
- How to change a column's type or value on the way across.

**Before you start:** read [Your first sync](first-sync.md). The examples reuse
its `src` and `dst`:

```python
import quickhouse as qh

src = qh.Postgres("postgresql://etl:etl@localhost:5432/etl")
dst = qh.ClickHouse("http://localhost:8123", database="analytics")
```

## The task

Three teams at Daily Grind have asked for data, each with a condition:

- **Marketing** wants the `customers` table to study loyalty tiers, but the
  privacy policy says email addresses and phone numbers must not leave the
  ordering app.
- **The dashboard team** wants shorter, clearer column names.
- **Finance** doesn't want individual orders at all. They want one row per
  store per day, with the number of orders and the revenue.

## Leave columns out

`exclude` drops the columns you name. Everything else is copied:

```python
qh.sync(
    src, dst,
    source_table="customers",
    dest_table="customers",
    mode="full",
    key=["id"],
    exclude=["email", "phone"],
)
```

The ClickHouse table has no `email` or `phone` column at all. quickhouse never
reads them, so they don't pass through your network or your logs.

`include` works the other way round: only the columns you name are copied.
Use it when you need a few columns from a wide table:

```python
qh.sync(
    src, dst,
    source_table="orders",
    dest_table="orders_slim",
    mode="full",
    key=["id"],
    include=["id", "store_id", "total", "created_at"],
)
```

```{tip}
For personal data, `include` is the safer choice. If a developer later adds a
`date_of_birth` column to `customers`, an `exclude` list would let it through.
An `include` list would not.
```

## Rename columns

`rename` maps a source column name to the name you want in the destination.
The `customers` table from the previous example has a `full_name` column and no
`name`, and quickhouse only chooses a table's columns when it creates the table
(more on that [below](#change-a-columns-type)). So drop it first:

```bash
docker compose exec clickhouse clickhouse-client --query "DROP TABLE analytics.customers"
```

```python
result = qh.sync(
    src, dst,
    source_table="customers",
    dest_table="customers",
    mode="full",
    key=["id"],
    exclude=["email", "phone"],
    rename={"full_name": "name"},
)
```

The destination now has a `name` column holding what the source calls
`full_name`.

If you skip the drop, the run still succeeds, but it can't put `name` anywhere,
and the existing `full_name` column gets no value. The result carries a warning
for each (see [Catching bad data](data-quality.md)):

```python
for w in result.warnings:
    print(w.kind, w.column)
```

```text
unsourced_destination_column full_name
dropped_source_column full_name
```

`unsourced_destination_column` names the destination column that nothing
fills: after this full refresh, every `full_name` in ClickHouse is empty.
`dropped_source_column` names the source column that was left out, before its
rename. `evolve_schema=True` would add the `name` column instead (see
[Designing ClickHouse tables](clickhouse-tables.md#when-the-source-gets-a-new-column)).

When you rename a column, the other options refer to it by different names:

- `include` and `exclude` use the **source** name, because they decide what to
  read.
- `key` uses the **destination** name, because it describes the new table.

So if you rename `id` to `order_id`, the key becomes `order_id`:

```python
qh.sync(
    src, dst,
    source_table="orders",
    dest_table="orders_renamed",
    mode="full",
    rename={"id": "order_id"},
    key=["order_id"],
)
```

## Name the source table

`source_table` accepts the forms each database understands:

| Source | Example |
| --- | --- |
| PostgreSQL | `"orders"` or, with a schema, `"public.orders"` |
| MySQL | `"orders"` |
| ClickHouse | `"orders"` or `"other_db.orders"` |
| BigQuery | `"dataset.table"` or `"project.dataset.table"` |

## Copy the result of a query

Finance wants a daily summary rather than raw orders. Instead of
`source_table`, pass `source_query` with any `SELECT` the source database can
run:

```python
qh.sync(
    src, dst,
    dest_table="daily_sales",
    mode="full",
    key=["day", "store_id"],
    source_query="""
        SELECT date(created_at)            AS day,
               store_id,
               count(*)                    AS orders,
               sum(total)::numeric(14, 2)  AS revenue
        FROM orders
        WHERE status = 'paid'
        GROUP BY 1, 2
    """,
)
```

The query runs on PostgreSQL, so you write it in PostgreSQL's SQL. quickhouse
reads the columns the query returns and creates `daily_sales` to match.

Two details in that query are worth copying:

- **Every column has a name** (`AS day`, `AS orders`). quickhouse uses these
  names for the destination columns.
- **The sum is cast back to `numeric(14, 2)`.** In PostgreSQL, `sum()` of a
  `numeric(12, 2)` column returns a plain `numeric` with no precision. Without a
  declared precision, quickhouse stores it as `Float64`, and money stored as a
  float picks up tiny rounding errors (`32.9` becomes `32.89999999999999`). With
  the cast, `revenue` lands as an exact `Decimal(14, 2)`.

```{note}
A `source_query` is read as one stream, while a `source_table` can be split into
ranges and read in parallel. For small results like this summary it doesn't
matter. For a big query, see `partition_source_expr` in
[Big tables and busy databases](performance.md).
```

## Change a column's type

quickhouse picks a sensible destination type for every column (see
[Type mapping](../guide/type-mapping.md) for the full list). When you want a
different one, use `type_overrides`. The value is the destination's own type
name. (It only takes effect when quickhouse creates the table, as the note
below explains, so to try it, first drop the `analytics.orders` table from
[Your first sync](first-sync.md).)

```python
qh.sync(
    src, dst,
    source_table="orders",
    dest_table="orders",
    mode="full",
    key=["id"],
    type_overrides={
        "total": "Decimal(18, 2)",      # more room for large totals
        "created_at": "DATETIME",       # a plain date-time with no time zone
    },
)
```

Common reasons to override a type:

- **Money and quantities** that must stay exact: pick a `Decimal(P, S)` that
  fits your largest value.
- **Time zones**: `"DATETIME"` stores a wall-clock time without a time zone,
  instead of the default UTC timestamp.
- **Repeated short strings**: on ClickHouse, `"LowCardinality(String)"` stores a
  column like `tier` (only `bronze`, `silver` or `gold`) much more compactly.

To make *every* decimal column exact at once, including ones with no declared
precision, use `numeric_as_decimal`:

```python
qh.sync(
    src, dst,
    dest_table="store_revenue",
    mode="full",
    key=["store_id"],
    source_query="SELECT store_id, sum(total) AS revenue FROM orders GROUP BY 1",
    numeric_as_decimal="Decimal(18, 2)",
)
```

A value too big for the decimal type you chose becomes `NULL`, and the run
reports a `coerced_decimal` warning. Choose a type with room to spare.

```{important}
Options that shape the destination table, like `type_overrides` and `key`, only
take effect when quickhouse **creates** the table. If the table already exists,
quickhouse keeps its existing column types. To change them, drop the
destination table and let the next sync create it again.

This works for a table filled by an incremental sync too. quickhouse remembers
how far that sync got, but when it has to create the table again it starts
from the beginning and copies every row, with a `recreated_destination` warning
on the result. [Keeping tables up to date](incremental.md) explains where it
keeps its place.
```

## Change a column's value

Sometimes you want to change the data itself on the way across.
`column_transforms` applies a SQL expression to a column inside the source's
`SELECT`. Marketing would like to know which email providers customers use, but
not the addresses themselves:

```python
qh.sync(
    src, dst,
    source_table="customers",
    dest_table="customers_masked",
    mode="full",
    key=["id"],
    column_transforms={
        "email": "lower(split_part(email, '@', 2))",   # keep only the domain
        "phone": "left(phone, 7) || '****'",           # mask the last digits
    },
    rename={"email": "email_domain"},
)
```

```text
251   Customer 251   example.com   +62-800****   gold   ...
252   Customer 252   example.com   +62-800****   bronze ...
```

The expression runs on the source database, so the full address never leaves
it. Like `source_query`, it is written in the source's own SQL.

Why not just write a `source_query`? Because `column_transforms` keeps
`source_table`, so quickhouse can still split the table into ranges and read it
in parallel. Use it for per-column tweaks on big tables. It works with
PostgreSQL, MySQL and ClickHouse sources, and is marked experimental.

### When the transform changes the type

A transform changes the value but, on its own, not the type. Say the dashboard
wants a true/false `is_refunded` column instead of the `status` text. The
expression `status = 'refunded'` returns a boolean, so you tell quickhouse twice:

```python
qh.sync(
    src, dst,
    source_table="orders",
    dest_table="orders_flags",
    mode="full",
    key=["id"],
    column_transforms={"status": "status = 'refunded'"},
    column_transform_types={"status": "Boolean"},   # how to read the new value
    type_overrides={"status": "Bool"},              # what type to store it as
    rename={"status": "is_refunded"},
)
```

- `column_transform_types` tells quickhouse how to **read** the transformed
  value. The choices are `"Boolean"`, `"Int16"`, `"Int32"`, `"Int64"`,
  `"UInt32"`, `"Float32"`, `"Float64"`, `"Utf8"`, `"Binary"` and `"Date32"`.
- `type_overrides` tells it what column type to **create** in the destination.
  Without it, the column keeps the source's type (text), and you get the words
  `true` and `false` instead of a real boolean.

For dates, times and decimals, use `type_overrides` alone. It already carries
the time zone or precision those types need.

## Recap

- `include` copies only the listed columns. `exclude` drops the listed columns.
  For personal data, prefer `include`.
- `rename` gives columns new names. `include`/`exclude` use source names; `key`
  uses destination names.
- `source_query` copies the result of any `SELECT`. Name every column, and cast
  sums of money back to a fixed precision.
- `type_overrides` and `numeric_as_decimal` choose destination types, but only
  when the table is first created.
- `column_transforms` changes values inside the source query while keeping
  parallel reads.

## Next

[Keeping tables up to date](incremental.md): stop copying everything every time,
and copy only what changed.
