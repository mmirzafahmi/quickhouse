# Changelog

All notable changes to **quickhouse** are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While quickhouse is pre-1.0 the public API may change between minor versions;
any breaking change is called out explicitly.

## [Unreleased]

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

[Unreleased]: https://github.com/mmirzafahmi/quickhouse/compare/v0.12.0...HEAD
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
