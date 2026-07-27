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

## Out-of-range and zero dates

Out-of-range dates, and MySQL zero-dates like `0000-00-00`, coerce to `NULL`
with a warning rather than failing the transfer.

## Column value transforms

`column_transforms` *(experimental)* applies a per-column SQL value transform in
the source `SELECT`, over `source_table=` (so range partitioning is preserved,
unlike `source_query=`). It changes the value, not the resolved type — pair it
with `type_overrides` if the type must change too. PostgreSQL and MySQL only.

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
