# CSV files

```{versionadded} 0.21.0
```

A `Csv` destination writes each table as a folder of CSV part files on a local
path. Every source works into it, with the same `sync()` call:

```python
import quickhouse as qh

dst = qh.Csv("/data/exports")                     # or compression="gzip"
qh.sync(qh.Postgres(dsn), dst, dest_table="orders",
        source_table="orders", mode="full")
```

`Csv` is a destination only; it can't be a `source=`. It writes to a local path
(or anything mounted as one); S3 and GCS paths aren't supported yet.

| Parameter | Default | |
| --- | --- | --- |
| `path` | | The root folder, a `str` or `os.PathLike`. Created on the first write. |
| `delimiter` | `","` | One ASCII character other than `"`, CR or LF. `"\t"` writes tab-separated files. |
| `header` | `True` | Write the column names as the first row of every part. |
| `compression` | `None` | `None` for `.csv` parts, or `"gzip"` for `.csv.gz` (one gzip member per part). |
| `max_rows_per_file` | `1_000_000` | A part closes and the next one opens after this many rows. |

A bad value fails when the `Csv` is built, with a `ValueError`.

## Layout

```text
/data/exports/
  _quickhouse_state.json                append cursors (see below)
  orders/
    _quickhouse_manifest.json           columns, format, rows per part
    part-1761480000123456789-00000.csv
    part-1761480000123456789-00001.csv
```

- Each `dest_table` is a folder. A run writes one or more parts named
  `part-<run_id>-<n>.csv` (`.csv.gz` with gzip), where `run_id` is the run's
  start in nanoseconds since 1970, so the parts sort in the order the runs wrote
  them.
- Every part has its own header row (unless `header=False`), so each one reads
  alone, and the folder reads as one table.
- A parallel read writes at least one part per partition that returns rows, and
  each part closes after `max_rows_per_file` rows.
- `_quickhouse_manifest.json` lists the columns in order (with the Arrow type
  each was written from), the delimiter, header and compression, and each
  part's file name, row count and size. quickhouse reads it to count rows and to
  check an append; a reader can use it too, but doesn't need to.

## Modes

### Full refresh

`mode="full"` writes the new parts into a staging folder beside the table's
(`<dest_table>_quickhouse_tmp_<id>`, after `staging_suffix=`), with their own
manifest, then swaps that folder into place and deletes the old one.

- On Linux the swap is one `renameat2(RENAME_EXCHANGE)`: the table's path never
  stops existing, and a reader sees the old folder or the new one, never a mix.
- Elsewhere (macOS, Windows, or a filesystem without the exchange) it is two
  renames: the path is missing for an instant, but never holds a mix.
- A run that fails before the swap removes its staging folder and leaves the
  table as it was.

A run with no rows writes one part holding only the header (with
`header=True`), so the folder still says what its columns are. The [shrink guard](../sync-modes.md#full-refresh)
applies as on any destination: a refresh that would leave fewer rows than the
manifest counts is refused unless `allow_full_refresh_shrink=True`. A full
refresh writes whatever columns this run reads, so it is also how a table's
columns change.

### Append

`mode="append"` adds this run's parts to the folder. They are written into a
staging folder first and moved into the table's folder once every row is read,
then added to its manifest. So a reader never sees a part being written, and a
run that fails partway leaves no new parts behind.

With `watermark=`, an append reads only the rows past a saved cursor, from any
source, database ones included:

```python
qh.sync(qh.Postgres(dsn), dst, dest_table="events", source_table="events",
        mode="append", watermark="created_at")
```

The cursor works as on the other destinations' state table, keyed by
`state_key` (by default the `source_table`) and `dest_table`, and
`seed_watermark=`, `skip_to_max=` and `advance_watermark=` apply. It is kept in
`<path>/_quickhouse_state.json` (`state_table_name=` + `.json`), written to a
temporary file and renamed into place. `qh.state_keys(dst)` lists the cursors;
`qh.compact_state(dst)` has nothing to do (the file holds one entry per cursor)
and returns 0. Delete a table's folder and its next append starts over from the
beginning, with a `recreated_destination` warning, as a dropped table does.

An append can't update or remove rows: nothing is deduplicated, so a row read
twice is written twice. `lookback_seconds` (which reads a margin again on
purpose) is refused in append mode, and the cursor is never moved back to
re-read a margin. A row whose watermark is NULL is left out and reported as
`null_watermark`: no cursor can hold it, so it would be written again on every
run.

An append must match the folder: the same columns in the same order, the same
delimiter, header and compression. Otherwise it is refused before anything is
read, naming the difference. `evolve_schema` isn't supported. To change the
columns, run `mode="full"` once (it rewrites the folder with the new ones), or
append to a new `dest_table`.

### Incremental

`mode="incremental"` is refused before anything runs: a CSV file can't update
or delete rows, so there is nothing to upsert into. Use `mode="append"` with
`watermark=` to add only the rows past a saved cursor.

## Format

UTF-8 text with RFC 4180 quoting:

| Value | Written as |
| --- | --- |
| NULL | an empty field: `1,,3` |
| empty string, empty bytes | `""`, so it reads apart from a NULL |
| a field holding the delimiter, `"`, CR or LF | quoted, each `"` doubled: `"a,b ""q"""` |
| date | `2024-01-02` |
| timestamp, timezone-aware | ISO 8601 in UTC with `Z`: `2024-01-02T03:04:05.123456Z` |
| timestamp, naive | ISO 8601, no offset: `2024-01-02T03:04:05.123456` |
| decimal | its exact text at its scale: `12.50` |
| float | the shortest text that reads back as the same value: `0.1`, `1e20`, `NaN`, `inf` |
| boolean | `true` / `false` |
| binary | base64, standard alphabet, padded (`aGk=` for `b"hi"`), as BigQuery exports `BYTES` |

Rows end with `\n`. A timestamp has as many fractional digits as its unit holds:
6 for the microseconds every source decodes to. A timezone-aware value in
another zone is written as its UTC instant. Which source types are aware or
naive is on the [type mapping](../type-mapping.md) page; a MySQL `DATETIME`, for
one, is UTC-aware by default.

A declared `numeric(P, S)` / `DECIMAL(P, S)` is written exactly. An
unconstrained PostgreSQL `numeric` is a float by default, as on every
destination; `numeric_as_decimal="Decimal(38, 9)"` writes it as exact decimal
text instead.

Dates and timestamps outside the years 1900 to 2299 are written empty, with a
`coerced_date` warning, as on every destination (see
[Out-of-range and zero dates](../type-mapping.md#out-of-range-and-zero-dates)).
In a table with one column, a NULL row is an empty line, which some readers skip
(pandas: pass `skip_blank_lines=False`).

### Reading it back

```python
import glob
parts = sorted(glob.glob("/data/exports/orders/part-*.csv"))

# pyarrow: types inferred, and NULL kept apart from ""
import pyarrow as pa, pyarrow.csv as pacsv
opts = pacsv.ConvertOptions(strings_can_be_null=True, quoted_strings_can_be_null=False)
table = pa.concat_tables(pacsv.read_csv(p, convert_options=opts) for p in parts)

# polars: NULL and "" kept apart by default; dates parsed on request
import polars as pl
df = pl.read_csv("/data/exports/orders/part-*.csv", try_parse_dates=True)

# pandas: numbers and booleans by default; both NULL and "" read as NaN
import pandas as pd
df = pd.concat(pd.read_csv(p, parse_dates=["created_at"]) for p in parts)
```

DuckDB reads the folder with `read_csv('/data/exports/orders/part-*.csv')`.
gzip parts read the same way: pyarrow, polars, pandas and DuckDB decompress a
`.gz` file themselves.

## Options

Options that can't apply to CSV files are refused, naming each, rather than
ignored:

- `engine`, `order_by`, `partition_by`, `primary_key`: they shape a database
  table, and a CSV folder has none.
- `not_null`: every CSV field can be empty.
- `merge_prune_partition_by`, `merge_prune_key_range=False`,
  `merge_prune_key_list_max`, `delete_stale_in_window`: they tune or extend an
  incremental `MERGE`, and CSV never merges or deletes rows.
- `type_overrides`: a CSV file has no column types. To write an unconstrained
  numeric exactly use `numeric_as_decimal`; to change a column's text, transform
  it in `source_query` or `column_transforms`.
- `evolve_schema`: see [Append](#append).
- `validate=`: the Great Expectations gate runs against a staging table in the
  destination database.

`Csv` takes no `archive=`: it is files already. `reconcile_keys()` and
`reconcile_values()` refuse a CSV destination.

`key` is allowed. Nothing is deduplicated on it; it only picks the column a
parallel read splits on, when `partition_column` is unset. Every column can be
empty, so a row whose key is NULL is written with the key empty, not left out
(there is no `null_key` or `null_defaulted` warning on a CSV destination).
`fail_on_warnings` works as anywhere: a full refresh stops before its swap, and
an append before its parts move in and its cursor is saved.

## Safety

- quickhouse replaces or adds to a folder only when it holds a
  `_quickhouse_manifest.json`, or nothing at all. A `dest_table` that names a
  folder of other files is refused, not deleted.
- A `dest_table` (and `staging_suffix`, `state_table_name`) must be a single
  path component that doesn't start with `.`.
- The state file and each manifest are written to a temporary file and renamed
  into place, and writes to them are serialized per `path`: by a lock in the
  process and, on Unix, an exclusive `flock` on the `path` folder, so syncs of
  different tables into one `path` from several processes don't lose a cursor.
  On Windows only the in-process lock applies.
- Parts and manifests are synced to disk (`fsync`) before they are renamed into
  place.
