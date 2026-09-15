# Type mapping

quickhouse maps each source type to a sensible destination type automatically:
integers to integers, floats to floats, text/JSON/UUID to strings, dates and
timestamps across as-is, and booleans preserved. Nullable source columns stay
nullable in the destination.

A few deliberate choices are worth knowing.

## Arbitrary-precision decimals

`numeric` / `DECIMAL` / `NUMERIC` default to **`Float64`**, since precision can't
be recovered from the type alone. Pin an exact type with `type_overrides` and the
value is decoded exactly (no `Float64` round-trip), not just declared with the
right destination type:

```python
qh.sync(..., type_overrides={"qty": "Decimal(18, 3)"})
```

- A value that doesn't fit the declared precision, or is `NaN`/`Infinity`
  (PostgreSQL `numeric` only), coerces to `NULL` with a warning.
- `P <= 38` is supported. `P > 38` (`Decimal256`) is rejected as a config error
  up front, rather than silently falling back to `Float64`.

## `TIME` columns

`TIME` columns transfer as canonical **text** into a `String` column — ClickHouse
has no time-of-day type.

## MySQL `DATETIME` / `TIMESTAMP`

These map to a **UTC-aware timestamp** (BigQuery `TIMESTAMP`, ClickHouse
`DateTime64(6, 'UTC')`) — the wall-clock value is read as UTC, matching how a
`TIMESTAMP` column expects it and what the legacy pandas/`to_gbq` path stored.

To land a column as a **naive** BigQuery `DATETIME` (or ClickHouse
`DateTime64(6)`) instead, opt out per column:

```python
qh.sync(..., type_overrides={"created_at": "DATETIME"})
```

This flips the actual wire encoding, not just the declared type, so it works on
the Storage Write path too.

PostgreSQL keeps the distinction natively: `timestamptz` → UTC-aware,
`timestamp` → naive.

## ClickHouse as a source

Read back from ClickHouse, a type that has no counterpart in the other engines
is kept rather than flattened: `UUID`, `IPv4`/`IPv6`, `Enum8`/`Enum16`,
`FixedString(N)` and `LowCardinality(...)` are recreated as themselves at a
ClickHouse destination (and land as `STRING` in BigQuery). `Decimal(P, S)` is
read exactly, without the `Float64` round-trip the other engines' unparameterised
decimals take. `Date`, `Date32`, `DateTime` and `DateTime64(P[, tz])` all resolve
to a UTC-aware timestamp, since a ClickHouse datetime is an absolute instant
whatever timezone its type names.

`Array`, `Map`, `Tuple`, `Nested`, `JSON`, the 256-bit integers and `Decimal256`
aren't readable yet — each is a clear error naming the column. Cast it to
`String` in a `source_query`, or `exclude` it.

## DataFrames

`from_pandas` normalises what it safely can and refuses the rest by name. The
conversions are applied in Python, before anything reaches the engine:

| incoming | becomes | note |
|---|---|---|
| `datetime64[ns]` (and `[s]`/`[ms]`) | `DateTime64(6)` | quickhouse standardises on microseconds. Real sub-microsecond digits **raise** rather than truncating — round with `.dt.floor('us')` |
| tz-aware, named zone | `DateTime64(6, 'Asia/Jakarta')` | carried through |
| tz-aware, fixed offset (`+07:00`) | `DateTime64(6, 'UTC')` + warning | ClickHouse's `DateTime64` takes a zone *name*; the instant is unchanged |
| naive `datetime64` | `DateTime64(6)` — **naive** | an unplaced wall clock stays unplaced, as with PostgreSQL `timestamp` |
| `category` | the value type | dictionary-decoded; expands in memory |
| `Int64`/`boolean`/`string` (nullable extension dtypes) | `Nullable(Int64)` etc. | stays an integer, so ids above 2^53 remain exact |
| `Decimal` objects | `Decimal(P, S)` | precision inferred from the values present — pin it with `type_overrides` for a stable DDL across runs |
| `float16` | `Float32` | lossless |
| `date64`, `time32`/`time64` | `Date32`, `String` | TIME as text, matching every other source |
| `large_string`, `string_view`, `large_binary` | `String`, `String` | the `pd.ArrowDtype` / polars family |

Refused, naming the column and the fix: values outside the 1900–2299 window
(checked with one vectorised pass, rather than letting the server reject the
insert partway through), nested `list`/`struct`/`map`, `Decimal256`, durations
and intervals, all-null columns, duplicate column names, and non-string column
names.

Note `NaN` in a float column is a *value*, not a null: ClickHouse stores a NaN,
BigQuery converts it to NULL. Use `pd.NA` (or a nullable dtype) if you mean
missing.

## Out-of-range and zero dates

Out-of-range dates, and MySQL zero-dates like `0000-00-00`, coerce to `NULL`
with a warning rather than failing the transfer.

## Column value transforms

`column_transforms` *(experimental)* applies a per-column SQL value transform in
the source `SELECT`, over `source_table=` (so range partitioning is preserved,
unlike `source_query=`). It changes the value, not the resolved type — pair it
with `type_overrides` if the type must change too. PostgreSQL, MySQL and
ClickHouse only.

```python
qh.sync(..., source_table="orders",
        column_transforms={"ts": "ts AT TIME ZONE 'UTC'", "amt": "ROUND(amt, 9)"})
```

## Schema evolution

`evolve_schema=True` auto-`ADD COLUMN` (as Nullable) when the source has a column
the destination lacks, instead of erroring. ADD-only — it never drops or retypes
a column. Default `False`.

## Not yet supported

Arrays and composite (`RECORD`/`STRUCT`) types aren't mapped yet. For API sources,
point a `JSON`/`STRING` column at a nested object via its `path` to land it as
compact JSON text.
