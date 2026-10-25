# Type mapping

quickhouse maps each source type to a sensible destination type automatically:
integers to integers, floats to floats, text/JSON/UUID to strings, dates and
timestamps across as-is, and booleans preserved. Nullable source columns stay
nullable in the destination.

A few deliberate choices are worth knowing.

## Arbitrary-precision decimals

A decimal column that **declares** its precision — PostgreSQL `numeric(P, S)`,
MySQL `DECIMAL(P, S)`, an API source's declared `NUMERIC` (as `Decimal(38, 9)`)
— lands as the exact **`Decimal(P, S)`** (BigQuery `NUMERIC`) and is decoded
exactly, with no `Float64` round-trip. That covers
`P <= 38`; wider types stay `Float64`, as does an unconstrained PostgreSQL
`numeric`, whose type carries no precision to use. BigQuery sources are
unchanged: their `NUMERIC` stays `Float64` unless you ask for more.

Three limits keep the default from breaking a table that already exists:

- **An existing column keeps its type.** The exact type applies to a table or
  column created fresh, and to a column that is already a decimal. A column
  created as `Float64` by an earlier version keeps receiving floats. To move it
  to a decimal, change the column (or recreate the table) yourself.
- **A new column follows the table.** When `evolve_schema` adds a column to a
  table whose other declared-precision columns are all `Float64`, the new one is
  `Float64` too, so one table never mixes the two (ClickHouse has no arithmetic
  or common type across `Decimal` and `Float64`). A table that already mixes
  them gets a `decimal_mapping_mixed` warning naming both sets.
- **BigQuery `NUMERIC` holds at most 9 fractional and 29 integer digits.** A
  declared type past either stays `FLOAT64`; pin a type with `type_overrides` if
  you need it exact.

Pin an exact type yourself with `type_overrides`, for one column, or with
`numeric_as_decimal`, for every decimal column including unconstrained ones:

```python
qh.sync(..., type_overrides={"qty": "Decimal(18, 3)"})
qh.sync(..., numeric_as_decimal="Decimal(38, 9)")
```

`numeric_as_decimal="Float64"` restores the old mapping: every decimal column
lands as `Float64`.

- A value that doesn't fit the declared precision, or is `NaN`/`Infinity`
  (PostgreSQL `numeric` only), coerces to `NULL` with a warning.
- `P <= 38` is supported. `P > 38` (`Decimal256`) is rejected as a config error
  up front, rather than silently falling back to `Float64`.

## `TIME` columns

`TIME` columns transfer as canonical **text** into a `String` column — ClickHouse
has no time-of-day type.

## MySQL `DATETIME` / `TIMESTAMP`

These map to a **UTC-aware timestamp** (BigQuery `TIMESTAMP`, ClickHouse
`DateTime64(6, 'UTC')`): the wall-clock value MySQL sends is read as UTC, which
is also what the legacy pandas/`to_gbq` path stored.

**A `TIMESTAMP` is only right when the session runs in UTC.** MySQL stores a
`TIMESTAMP` as an instant but sends it rendered in the session's time zone. On a
server whose zone isn't UTC, every value lands shifted by the offset: at
`+07:00`, the instant `01:00 UTC` lands as `08:00 UTC`. quickhouse reports each
such column with a `shifted_timestamp` warning. Pass `utc_session=True` to read
them as the instants they store:

```python
qh.MySQL("mysql://user:pw@host:3306/db", utc_session=True)
```

This runs every connection with `SET time_zone = '+00:00'`, which also changes
what `NOW()` and `CURDATE()` return inside a `source_query`. `DATETIME` columns
carry no zone and read the same either way.

Switching an existing pipeline moves every `TIMESTAMP` value by the offset:

- **Rows already landed** keep the shifted value until they are written again.
  A full refresh rewrites them.
- **A cursor saved from a `TIMESTAMP` watermark** moves too. On a server east of
  UTC it jumps ahead by the offset, and rows inside that jump are skipped. Give
  the first incremental run after the switch a `lookback_seconds` of at least
  the offset (`25200` for `+07:00`), or a new `state_key`.

To land a column as a **naive** BigQuery `DATETIME` (or ClickHouse
`DateTime64(6)`) instead, opt out per column:

```python
qh.sync(..., type_overrides={"created_at": "DATETIME"})
```

A naive `TIMESTAMP` column keeps the wall-clock time the session renders it in,
so it isn't reported as `shifted_timestamp`.

This flips the actual wire encoding, not just the declared type, so it works on
the Storage Write path too. On ClickHouse, `DATETIME` and `TIMESTAMP` create
`DateTime64(6)` and `DateTime64(6, 'UTC')`, keeping microseconds; ClickHouse's
own `DateTime`, spelled that way, still means second precision.

PostgreSQL keeps the distinction natively: `timestamptz` → UTC-aware,
`timestamp` → naive.

## ClickHouse as a source

Read back from ClickHouse, a type that has no counterpart in the other engines
is kept rather than flattened: `UUID`, `IPv4`/`IPv6`, `Enum8`/`Enum16`,
`FixedString(N)` and `LowCardinality(...)` are recreated as themselves at a
ClickHouse destination (and land as `STRING` in BigQuery). `Decimal(P, S)` is
read exactly, without the `Float64` round-trip the other engines' unparameterised
decimals take. `Date`, `Date32`, `DateTime` and `DateTime64(P[, tz])` keep their
declared types at a ClickHouse destination, time zone and precision included:
a `DateTime64(3, 'Asia/Jakarta')` column is created as
`DateTime64(3, 'Asia/Jakarta')`. In BigQuery, `Date` and `Date32` land as
`DATE`, and `DateTime` and `DateTime64` as a UTC-aware `TIMESTAMP`, since a
ClickHouse datetime is an absolute instant whatever timezone its type names;
`type_overrides={"col": "DATETIME"}` is the per-column opt-out to a naive one.

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

A column that holds only nulls, with nothing to infer a type from (an `object`
column of `None`s, or Arrow's `null` type from pyarrow or polars), becomes a
nullable string, `Nullable(String)` on ClickHouse, with a `QuickhouseWarning`.
Give it a dtype, such as `df["note"].astype("string")`, to choose its type
yourself.

Refused, naming the column and the fix: values outside the 1900–2299 window
(checked with one vectorised pass, rather than letting the server reject the
insert partway through), nested `list`/`struct`/`map`, `Decimal256`, durations
and intervals, duplicate column names, and non-string column names.

`NaN` in a float column depends on the library. pandas uses `NaN` as its
missing-value marker, so in a pandas float column it arrives as `NULL`. In a
pyarrow or polars float column it is a *value*, not a null: ClickHouse stores a
NaN, BigQuery converts it to NULL. Use `None` there (or `pd.NA`, or a nullable
dtype, in pandas) if you mean missing.

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
the destination lacks. ADD-only — it never drops or retypes a column. Default
`False`, and then a ClickHouse destination drops such a column from every row it
writes. The run reports it: each such source column (after
`rename`/`include`/`exclude`) raises `dropped_source_column` before anything is
written. Turning `evolve_schema` on later adds the column but leaves it `NULL`
on the rows synced in the meantime, since the cursor has passed them, so pass
`fail_on_warnings={"dropped_source_column"}` to stop the run instead. The fix is
`evolve_schema=True`, or `exclude=` for a column the destination doesn't need.
BigQuery refuses such a write rather than drop the column, and so does a
ClickHouse incremental run that stages (for `validate=`, a retried `MergeTree`
or `delete_stale_in_window`): neither raises the warning.

The other direction is reported too: a destination column that no source column
feeds (after `rename`/`include`/`exclude`) raises
`unsourced_destination_column`, once per column, before anything is written.
The two destinations treat such a column differently:

- **ClickHouse** writes whole rows. Each row a sync writes carries the column's
  type default (`NULL` when `Nullable`), and in a `ReplacingMergeTree` it
  replaces the stored row, so a value already there is lost.
- **BigQuery**'s `MERGE` updates only the columns the source provides. Updated
  rows keep the value; inserted rows get `NULL`.

A full refresh rebuilds every row without it on either destination. A column
the destination fills itself (a `DEFAULT`, or a ClickHouse `MATERIALIZED`,
`ALIAS` or `EPHEMERAL` expression) isn't reported. To stop the run instead,
pass `fail_on_warnings={"unsourced_destination_column"}`. The fix is to select
the column in the source, or to drop it from the destination.

## Not yet supported

Arrays and composite (`RECORD`/`STRUCT`) types aren't mapped yet. For API sources,
point a `JSON`/`STRING` column at a nested object via its `path` to land it as
compact JSON text.
