# Changelog

All notable changes to **quickhouse** are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While quickhouse is pre-1.0 the public API may change between minor versions;
any breaking change is called out explicitly.

## [Unreleased]

## [0.13.0] - 2026-07-31

### Changed — please read before upgrading
Two changes reject configurations that earlier versions accepted. Both were
silently destroying data, which is why they're now errors rather than warnings,
but each can turn a currently-"working" call into a startup failure:

- **A row-merging ClickHouse engine with no `key`/`order_by` is now a config
  error.** If you have a `sync()` that creates a table in `mode="incremental"`
  (which defaults to `ReplacingMergeTree`) without passing `key` or `order_by`,
  it will now fail up front instead of creating a table whose first background
  merge collapses it to a single row. Any table already created that way is
  already damaged — the error is telling you about a latent bug, not causing
  one. See the entry under *Fixed* below.
- **`mode="full"` against an existing ClickHouse table no longer rewrites its
  engine/`ORDER BY`/`PARTITION BY`** from the `engine`/`order_by`/`partition_by`
  arguments (or their defaults, when omitted). Full-refresh staging clones the
  destination's actual DDL instead. If you were relying on a full refresh to
  change an existing table's structure, drop the table first — passing different
  arguments no longer has that effect.

Also note, for `write_method="storage_write"` users: writes now go to a
per-call *committed* stream rather than the `_default` stream. This is what makes
the exactly-once fix possible (`_default` cannot carry offsets) and needs no
config change, but it is a different BigQuery API call
(`CreateWriteStream` rather than `GetWriteStream`) on the same write permission
— worth a smoke test on a throwaway dataset before a large run.

### Fixed
- **BigQuery writes could permanently duplicate rows.** Both write paths retry a
  transient failure, and "transient" includes the case where the server
  committed the rows but the acknowledgement was lost — so a retry rewrote them.
  Nothing caught it afterwards: `insertAll` sent `insert_id: None`, disabling
  BigQuery's own row-level dedup; the Storage Write path appended to the
  offset-less `_default` stream; and the incremental `MERGE` read its staging
  table directly, where `WHEN NOT MATCHED THEN INSERT` fires once per source row
  (BigQuery only rejects a *target* row matched more than once). A key not yet in
  the destination was therefore inserted twice, the watermark still advanced, the
  sync still reported success, and no later run could repair it — a subsequent
  merge updates both copies identically. Now: `insertAll` sends a deterministic
  per-row `insert_id` (stable across a request's retries, distinct between rows);
  the Storage Write path appends at an explicit offset on a committed stream, so
  a re-append the server already has comes back `ALREADY_EXISTS` and is treated
  as the success it is; and the `MERGE` deduplicates staging by `key` first, so a
  duplicate reaching staging by any route still can't reach the destination.
  The dedup keeps the highest-watermark row per key, which also fixes a source
  batch legitimately holding two rows for one key — previously a hard
  "MERGE must match at most one source row" failure, now last-write-wins,
  matching what `ReplacingMergeTree(<watermark>)` does with the same input.
- **A multi-line `source_query` broke incremental sync into BigQuery.** The
  GoogleSQL escaping for the hand-built watermark statements escaped backslashes
  and quotes but not newlines, and a quoted (non-triple) GoogleSQL literal can't
  carry a raw newline. Since the default state key *is* the `source_query` text,
  any query written across several lines produced an unclosed string literal.
  Confusingly ordered, too: `read_last_watermark` returns `Ok(None)` while the
  state table doesn't exist yet, so run 1 read the whole table and succeeded,
  and only run 2 onward failed — permanently, with the cursor frozen. Newlines
  and carriage returns are now escaped in both copies of that function.
- **ClickHouse incremental sync with no `key`/`order_by` could empty the table.**
  Incremental mode defaults to `ReplacingMergeTree`, and `key` was only mandatory
  for a destination that stages its incremental writes (BigQuery) — so a
  ClickHouse table created without either got
  `ReplacingMergeTree(<watermark>) ORDER BY (tuple())`. That's a genuinely empty
  sorting key, and a row-merging engine treats rows equal on the sorting key as
  the same row: with no columns in it, every row in a part compares equal, and
  the first background merge collapses the part to a single row. Runs up to that
  point all reported success with matching row counts. `create_table` now refuses
  the combination, naming `key`/`order_by`, for every engine whose merge
  semantics depend on the sorting key (`Replacing`/`Collapsing`/
  `VersionedCollapsing`/`Summing`/`AggregatingMergeTree`, including
  `Replicated*` and any explicit parameter list). Plain `MergeTree` keeps every
  row, so an unsorted table there is still allowed.
- **Passing both `source_table` and `source_query` decoded data with the wrong
  types.** `source_table`'s documented contract is "ignored if `source_query` is
  set", and `copy_sql`/`max_watermark`/`select_sql` all honour it — but the
  schema probe alone inverted the precedence, resolving column types from the
  bare table while the rows came from the query. PostgreSQL's binary `COPY`
  carries no per-field type tag, so the query's bytes were decoded against the
  table's OIDs, and because the field readers take a byte-count prefix, a widened
  column was silently truncated rather than erroring (`id::bigint` over an `int4`
  column read the high 4 bytes of each `i64` — every value `0`). The probe now
  follows the query, matching every other consumer, and warns that
  `source_table` is being ignored. When both are set the table is no longer
  consulted for `NOT NULL` either, since a join in the query can make a
  `NOT NULL` base column nullable in the result.
- The ClickHouse sink re-ran the `_quickhouse_state` chunk-resume migration
  `ALTER` on *every* `sync()`, even when the table already had the columns (it
  always does since 0.5). On a replicated engine (e.g. ClickHouse Cloud) an
  `ADD COLUMN IF NOT EXISTS` that changes nothing still bumps the table's
  metadata version, so this churned a cluster-wide counter and raced concurrent
  syncs into `517 CANNOT_ASSIGN_ALTER`, aborting otherwise-healthy runs
  (including no-op ones). `ensure_state_table` now probes `system.columns`
  first and issues the `ALTER` only when a column is genuinely absent — at most
  once per table, and never for a table created by 0.5+. No data or cursor
  impact; the abort happened before any write.
- BigQuery-source incremental sync against a numeric/boolean watermark failed
  with `INT64 > STRING` (etc.) on every run once a cursor was persisted: the
  upper bound (this run's snapshot max) was CAST-typed but the lower bound
  (the persisted cursor) was still emitted as a bare quoted STRING literal.
  Both bounds are now typed consistently.
- A nullable incremental `watermark` column silently excluded every row with
  a `NULL` value there, forever — `WHERE watermark > x` never matches NULL,
  but the transfer still reported success. A PostgreSQL/MySQL source now
  warns (with the row count) when this is detected.
- `mode="full"` against an *existing* ClickHouse destination silently
  replaced its engine/`ORDER BY`/`PARTITION BY` with whatever `engine`/
  `order_by`/`partition_by` happened to be passed (or their defaults, if
  omitted) — the swap adopts staging's DDL, and staging was always rebuilt
  from those arguments rather than the destination's actual structure.
  Full-refresh staging now clones the existing destination's DDL instead;
  `evolve_schema=True` is what now adds a genuinely new source column to it
  (previously added for free, an inconsistency with every other schema-drift
  path). Deliberately changing an existing table's DDL now requires dropping
  it first. The same "schema follows the destination" fix also applies to
  **incremental** mode's staging table (BigQuery only — the sole destination
  that stages for incremental): it previously rebuilt staging fresh from the
  resolved source schema on every run, which could drift from whatever
  `type_overrides`/`column_transform_types` the destination was actually
  created with on an earlier run and break the incremental `MERGE`.
- Nullability for DDL generated from scratch was always resolved from the
  source schema, with no way to force a column `NOT NULL` unless it was also
  a `key`/`order_by`/`primary_key` column — a column used only in
  `partition_by`'s expression (e.g. `toYYYYMM(create_date)`) could end up
  `Nullable(...)` (e.g. every BigQuery column not explicitly `REQUIRED`),
  which ClickHouse rejects as a partition key. New `not_null=[...]` forces it.
- `column_transforms` paired with `type_overrides` to change a column's type
  (as `column_transforms`' own doc suggested) didn't actually work:
  `type_overrides` only changes the declared *destination* type string, not
  the Arrow type this crate decodes the source's wire data as — so the
  column was still decoded (and could silently misdecode) as its original
  type. New `column_transform_types={col: "<Arrow type>"}` declares the
  transformed decode type explicitly.

### Added
- `validate=` on `sync()`: an optional **preventive data-quality gate**. The
  transfer loads its per-run staging table as usual, then — *before* promoting
  it (the atomic swap for a full refresh, or the `MERGE`/insert for an
  incremental) — runs a validation callback against the staging table; if it
  raises, the promotion is aborted, staging is dropped, and `sync()` fails, so
  rejected data never reaches the destination. Pass a new `quickhouse.Validation`
  (a [Great Expectations](https://greatexpectations.io/) suite run against the
  staging table via a GX SQL datasource you point at the destination) or any
  `callable(info) -> None` that raises to reject. Optional dependency:
  `pip install quickhouse[quality]`. Works in **full-refresh and incremental**
  mode, into **either destination** — a ClickHouse incremental sync (which
  normally inserts directly, with no staging) is transparently routed through a
  staging table when a gate is attached, then promoted via `INSERT … SELECT`
  (`ReplacingMergeTree` dedups the promoted rows as usual). Append mode and
  `chunk_rows` (keyset resumable reads) commit directly with no single staging
  table to gate, so they raise a clear config error rather than silently
  skipping the validation.
- `watermark_source_expr` (PostgreSQL/MySQL sources): a raw SQL expression
  used in place of `watermark` when building the incremental filter and the
  boundary-max probe, leaving the projected `watermark` output untouched.
  Fixes a full-scan-on-every-run case: when `source_query` computes
  `watermark` via a transform (a cast, a timezone shift, ...) rather than a
  bare pass-through of an indexed base-table column, the generated
  `WHERE watermark > $1` binds to that computed value, not the underlying
  column, so no index can serve it — confirmed on a 49.9M-row table (594x
  planner cost; 1/5 runs succeeding under hot-standby query cancellation vs.
  5/5 once filtered on the raw column instead).
- `numeric_as_decimal="Decimal(P, S)"` on `sync()`: decode **every**
  arbitrary-precision decimal source column (PostgreSQL `numeric`, MySQL
  `DECIMAL`, BigQuery `NUMERIC`) exactly, rather than through the default lossy
  `Float64` round-trip that reproduces a stored `32.9` as `32.89999999999999`.
  Exact decoding was already reachable per column via
  `type_overrides={col: "Decimal(P,S)"}` — the problem being that it has to be
  remembered for every affected column in every table, and forgetting one is
  silent (confirmed in production: 2,457 of 179,478 sampled rows of one Odoo
  `numeric` column already carry exactly this noise, and 7 of 734,047 rows of
  another). A per-column `type_overrides` entry still wins. **Not the default**,
  because it changes the destination column type — against a table already
  created with a `Float64`/`FLOAT64` column you'd be writing a decimal into a
  float. Choose `S` for the column's real range: a value that doesn't fit is
  coerced to NULL (counted and warned about, as an explicit override already
  was). `P > 38` needs `Decimal256`, still unsupported.
- `tinyint1_as_bool=False` on `sync()` (MySQL sources): read a `tinyint(1)`
  column as the integer it is (`Int8`, or `UInt8` when UNSIGNED) instead of a
  boolean. MySQL has no boolean type — `BOOL` is an alias for `tinyint(1)` — so
  display width is the only signal, and this crate followed that convention
  unconditionally. It doesn't hold universally: schemas that store genuine small
  integers in a `tinyint(1)` (Odoo, for one) had every non-zero value decoded to
  `true` and written as `1`, destroying the difference between `2` and `3`. This
  caused a real production incident across 9 columns in 8 tables, and
  `type_overrides` cannot repair it — the boolean decoder flattens the value
  before the declared destination type is relevant. Left at the default `True`,
  any value outside `{0, 1}` is now counted and warned about at the end of the
  read, instead of passing unnoticed.

## [0.12.1] - 2026-07-27

### Changed
- Internal refactor: the destination layer is now an object-safe **`Sink` trait**
  (`#[async_trait]`, dispatched as `Arc<dyn Sink>`) instead of a closed enum.
  Each built-in destination (ClickHouse, BigQuery) is a trait impl, and the
  engine-specific capabilities (staged-merge upsert, chunked-resume cursor) are
  overridable trait methods with safe defaults — so a new destination implements
  only what it supports. Exported from `quickhouse-core` (`Sink`, `build_sink`)
  as an extension seam for external Rust crates implementing custom engines.
  No change to the Python API or to any transfer's behavior (byte-identical;
  the full existing test suite is unchanged and green).

## [0.12.0] - 2026-07-27

### Added
- A generic **`HttpApi`** source for arbitrary REST/JSON or CSV endpoints (the
  config-driven complement to the purpose-built CleverTap/AppsFlyer sources):
  GET/POST with caller-supplied headers (auth), `{from}`/`{to}` date
  substitution in the URL/body, JSON (records array at a dotted `records_path`)
  or CSV bodies, and optional cursor pagination (`next_cursor_path` +
  `cursor_param`). Writes to BigQuery or ClickHouse; declared-schema columns and
  the incremental/append/lookback machinery are shared with the other API
  sources.

## [0.11.0] - 2026-07-27

### Added
- HTTP API sources (CleverTap, AppsFlyer) can now write to a **ClickHouse**
  destination, not just BigQuery — the transfer flows through the same `Sink`
  abstraction. (BigQuery-specific type-name seeding is skipped for ClickHouse,
  which takes its column types from the resolved Arrow/ClickHouse mapping.)

### Removed
- The API-source "BigQuery destination only" restriction.

### Added
- mTLS (client-certificate auth) for the PostgreSQL and MySQL sources: set
  `client_cert_file` + `client_key_file` together on `Postgres`/`MySQL`
  (Postgres via rustls `with_client_auth_cert`; MySQL via mysql_async
  `ClientIdentity`). Additive; omitting them keeps the prior no-client-auth
  behavior. Passing only one is a clear config error.

## [0.9.0] - 2026-07-27

### Added
- Richer authentication (all additive; existing calls unchanged):
  - `Postgres`/`MySQL` accept discrete `host`/`port`/`user`/`password`/`database`
    fields as an alternative to the DSN string (percent-encoded and assembled
    into a DSN; pass one or the other, not both).
  - `BigQuery` accepts inline `credentials_json` (service-account key contents,
    e.g. from a secrets manager) alongside `credentials_file`/ADC; it takes
    precedence when both are set.

## [0.8.0] - 2026-07-27

### Added
- Configurable internal names via new `sync()` arguments (defaults unchanged, so
  existing calls are byte-identical): `state_table_name` (default
  `_quickhouse_state`), `staging_suffix` (default `_quickhouse_tmp`), and
  `application_name` (default `quickhouse`, the PostgreSQL `application_name`).
- A command-line runner: `quickhouse run job.toml` (and `quickhouse --version`),
  installed as a console script. TOML job files have `[source]`/`[target]`/`[sync]`
  tables with `${ENV_VAR}` expansion. New `[cli]` extra pulls the TOML parser on
  Python < 3.11 (3.11+ uses stdlib `tomllib`).

## [0.7.2] - 2026-07-26

### Added
- `examples/` directory with runnable end-to-end scripts (Postgres → ClickHouse,
  incremental, MySQL → BigQuery, CleverTap append → BigQuery).
- Community health files: issue and pull-request templates, `CODE_OF_CONDUCT.md`.
- A documented stability & versioning policy, and *experimental* markers on the
  sharper-edged knobs (`chunk_rows`, `merge_prune_partition_by`,
  `delete_stale_in_window`, `storage_write`, `column_transforms`).

### Changed
- CI now runs the full test suite (including the MySQL and S3-archival suites)
  across Python 3.9 and 3.12, and enforces `cargo fmt` + `clippy` gates.
- Neutralized ERP-specific example naming in the docs and benchmark (generic
  order-line schema; `created_at`/`updated_at` in the pruning examples).

## [0.7.1] - 2026-07-26

### Added
- `CHANGELOG.md` and `SECURITY.md`.
- README: source ↔ destination compatibility matrix, a "when to use / when not
  to use" section, and a pre-1.0 stability note.

### Changed
- Corrected the prebuilt-wheel platform list in the README to match the release
  pipeline (Linux x86_64, macOS Apple Silicon, Windows x64; other platforms build
  from the sdist).
- Richer PyPI/crates metadata (per-minor Python + `Typing :: Typed` classifiers,
  `Documentation`/`Changelog` URLs, refreshed descriptions).

### Fixed
- Stale `.gitignore` rules that referenced the pre-rename `etlhouse` package path
  (locally built `python/quickhouse/` extension artifacts are now ignored).

## [0.7.0] - 2026-07-26

### Added
- HTTP API sources: `mode="append"` bronze-landing writes (insert without
  staging/merge/swap), a `lookback_days` rolling re-pull window, and a
  window-scoped `delete_stale_in_window` (`MERGE … WHEN NOT MATCHED BY SOURCE`,
  requires `merge_prune_partition_by`).

## [0.6.1] - 2026-07-26

### Fixed
- CleverTap top-level `ts` is a packed `yyyyMMddHHmmSS` integer (not epoch
  seconds); it no longer overflows to a silent `NULL` in declared
  TIMESTAMP/DATETIME/DATE columns.

### Added
- Warnings when a declared date/time column parses to `NULL` for every source
  value, and when a full-refresh would shrink an existing API destination.

## [0.6.0] - 2026-07-26

### Added
- CleverTap and AppsFlyer HTTP API sources → BigQuery, with a caller-declared
  output schema (`ApiColumn`).

## [0.5.0] - 2026-07-26

### Added
- Keyset resumable reads (`chunk_rows`), source retry/backoff
  (`retry_max_attempts`), declarative `column_transforms`, exact
  Decimal128 → BigQuery `NUMERIC`, and destination schema evolution
  (`evolve_schema`).

### Fixed
- MySQL `TEXT` columns now map to `STRING` (not `BYTES`); a null incremental
  watermark no longer crashes the Arrow schema check.

## [0.4.0] - 2026-07-25

### Added
- Incremental cursor control (`state_key`, `seed_watermark`/`skip_to_max`,
  `advance_watermark`) and cheaper MERGE pruning (`merge_prune_partition_by`).
- Golden decode-matrix tests for the Postgres and BigQuery type paths.

## [0.3.5] - 2026-07-25

### Fixed
- MySQL decoder now emits a UTC-aware timestamp array (regression from 0.3.4 that
  broke MySQL → BigQuery `TIMESTAMP`).

## [0.3.4] - 2026-07-25

### Added
- `read_max_rows_per_sec` source-read throttling to keep bulk exports gentle on a
  production database.

### Fixed
- MySQL `DATETIME`/`TIMESTAMP` now map to BigQuery `TIMESTAMP` (UTC-aware).

## [0.3.3] - 2026-07-24

### Fixed
- BigQuery staging tables now use a per-run-unique name, fixing streaming-insert
  failures on rapid re-runs/retries (and cleaning up staging on the error path).

## [0.3.2] - 2026-07-23

### Added
- Optional S3/Parquet data-lake archival of every synced batch (ClickHouse
  destinations).

## [0.3.1] - 2026-07-22

### Fixed
- BigQuery date-range and SQL-escaping bugs; exact decimal precision via
  `type_overrides`.

## [0.3.0] - 2026-07-22

### Added
- **BigQuery as a destination** (in addition to source), and an opt-in BigQuery
  Storage Write API path.

### Fixed
- Replaced the copy-job swap that could silently empty a BigQuery full-refresh
  destination.

## [0.2.4] - 2026-07-20

### Fixed
- Hardened date/time handling for legacy data; usage-focused documentation pass.

## [0.2.3] - 2026-07-17

### Fixed
- Release/version-metadata correction (no functional change).

## [0.2.2] - 2026-07-17

### Added
- Byte-budgeted memory pipeline, streaming zstd uploads, and insert retry/backoff.

## [0.2.1] - 2026-07-17

### Changed
- **Renamed the project `etlhouse` → `quickhouse`** (first release under the new
  name).

## [0.2.0] - 2026-07-17

### Added
- MySQL and BigQuery **sources**, a tqdm progress bar, and structured sync logging.

## [0.1.1] - 2026-07-16

### Added
- TLS support for PostgreSQL connections.

## [0.1.0] - 2026-07-16

### Added
- Initial release: parallel, bounded-memory PostgreSQL → ClickHouse transfer with
  automatic DDL, full-refresh and incremental modes, and type mapping.

[Unreleased]: https://github.com/mmirzafahmi/quickhouse/compare/v0.13.0...HEAD
[0.13.0]: https://github.com/mmirzafahmi/quickhouse/compare/v0.12.1...v0.13.0
[0.12.1]: https://github.com/mmirzafahmi/quickhouse/compare/v0.12.0...v0.12.1
[0.12.0]: https://github.com/mmirzafahmi/quickhouse/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/mmirzafahmi/quickhouse/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/mmirzafahmi/quickhouse/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/mmirzafahmi/quickhouse/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/mmirzafahmi/quickhouse/compare/v0.7.2...v0.8.0
[0.7.2]: https://github.com/mmirzafahmi/quickhouse/compare/v0.7.1...v0.7.2
[0.7.1]: https://github.com/mmirzafahmi/quickhouse/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/mmirzafahmi/quickhouse/compare/v0.6.1...v0.7.0
[0.6.1]: https://github.com/mmirzafahmi/quickhouse/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/mmirzafahmi/quickhouse/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/mmirzafahmi/quickhouse/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/mmirzafahmi/quickhouse/compare/v0.3.5...v0.4.0
[0.3.5]: https://github.com/mmirzafahmi/quickhouse/compare/v0.3.4...v0.3.5
[0.3.4]: https://github.com/mmirzafahmi/quickhouse/compare/v0.3.3...v0.3.4
[0.3.3]: https://github.com/mmirzafahmi/quickhouse/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/mmirzafahmi/quickhouse/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/mmirzafahmi/quickhouse/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/mmirzafahmi/quickhouse/compare/v0.2.4...v0.3.0
[0.2.4]: https://github.com/mmirzafahmi/quickhouse/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/mmirzafahmi/quickhouse/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/mmirzafahmi/quickhouse/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/mmirzafahmi/quickhouse/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/mmirzafahmi/quickhouse/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/mmirzafahmi/quickhouse/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/mmirzafahmi/quickhouse/releases/tag/v0.1.0
