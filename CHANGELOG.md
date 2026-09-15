# Changelog

All notable changes to **quickhouse** are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While quickhouse is pre-1.0 the public API may change between minor versions;
any breaking change is called out explicitly.

## [Unreleased]

## [0.16.0] — 2026-09-15

ClickHouse could be written to but not read from, which made the one transfer a
ClickHouse shop most obviously wants — moving a table from one cluster or
database to another — the only one quickhouse couldn't do. It can now, and it
turns out to be the *simplest* source in the crate rather than the fourth
variation on a wire decoder: ClickHouse speaks Arrow natively in both
directions.

### Added — ClickHouse as a source

- **`quickhouse.ClickHouse(...)` now works as `sync()`'s `source=` as well as
  its `target=`**, the way `BigQuery` already did. That makes a cross-cluster or
  cross-database ClickHouse copy, and publishing a ClickHouse mart into
  BigQuery, ordinary transfers rather than something to script by hand:

  ```python
  src = qh.ClickHouse("http://ch-a:8123", database="raw")
  dst = qh.ClickHouse("http://ch-b:8123", database="analytics")
  qh.sync(src, dst, dest_table="orders", source_table="orders",
          mode="incremental", watermark="updated_at", key=["id"], parallelism=4)
  ```

  Everything the Postgres and MySQL sources support works here: range
  partitioning across `parallelism` connections, full-refresh and incremental
  modes, `chunk_rows` resumable reads, `lookback_seconds`,
  `partition_source_expr` / `watermark_source_expr`, `column_transforms`,
  `read_max_rows_per_sec`, the S3 Parquet archive, and `reconcile_keys`.

- **No new wire decoder.** ClickHouse serves `FORMAT ArrowStream`, so reads go
  straight into the Arrow IPC decoder the rest of the crate already moves rows
  in — the one source that needs no hand-written protocol decoding. What makes
  that safe is that every projected column is `CAST` server-side to a ClickHouse
  type whose Arrow output type is pinned: ClickHouse's Arrow writer emits `Date`
  as `UINT16`, `DateTime` as `UINT32`, `Enum8` as its backing `INT8`,
  `FixedString` as `FIXED_SIZE_BINARY`, and a `DateTime64(P)` at `P`'s own time
  unit, none of which the destination schema expects. The cast target is derived
  from the *destination* column, so `type_overrides`, `numeric_as_decimal` and
  `column_transform_types` are honoured with no extra machinery.

- **Types that only ClickHouse has survive a ClickHouse → ClickHouse copy.**
  `UUID`, `IPv4`/`IPv6`, `Enum8`/`Enum16`, `FixedString(N)` and
  `LowCardinality(...)` are recreated as themselves at the destination rather
  than flattened to `String` (and land as `STRING` in BigQuery).
  `LowCardinality(Nullable(T))` keeps its dictionary encoding, with the
  nullability lifted out to where the rest of the pipeline expects it.
  `Decimal(P, S)` is read exactly — no `Float64` round-trip, unlike the other
  engines' unparameterised decimals. `Date`, `Date32`, `DateTime` and
  `DateTime64(P[, tz])` all resolve UTC-aware, since a ClickHouse datetime is an
  absolute instant whatever timezone its type names;
  `type_overrides={"col": "DATETIME"}` is the per-column opt-out, as it is for
  MySQL.

- **Unsupported types fail loudly, naming the column.** `Array`, `Map`, `Tuple`,
  `Nested`, `JSON`, the 256-bit integers and `Decimal256` are rejected during
  schema resolution with the workaround in the message (`exclude=`, or a
  `source_query` that casts to `String`) — never silently dropped. And here
  `exclude=["col"]` genuinely works: the ClickHouse source drops an unmappable
  column that the transfer was never going to carry instead of failing the run.
  (The error's advice has always been false for the other sources, where column
  selection happens well after schema resolution has already errored — unchanged
  here, but ClickHouse is where it bites, since `Array` and `Map` columns are
  ordinary in a real schema rather than exotic.)

- **`NOT NULL` survives a ClickHouse → ClickHouse copy.** Every other source's
  decoder can turn an otherwise-valid value into NULL — a zero-date, a year
  outside ClickHouse's window, a decimal past its declared precision — so the
  planner widens every `Date`/`DateTime`/`Decimal` destination column to
  `Nullable(...)` defensively. This source reads Arrow the server already
  produced, from values already inside ClickHouse's representable window,
  through a `CAST` that is exact or a hard error; `transform::plan_with` lets it
  say so, and those columns stay as declared.

- Three smaller things the source has to get right, each of which would
  otherwise be a quiet wrong answer rather than an error:
  - The schema probe is `DESCRIBE (SELECT * FROM t)`, not `DESCRIBE TABLE t`.
    The latter also reports `MATERIALIZED`, `ALIAS` and `EPHEMERAL` columns,
    which `SELECT *` does not return — resolving those would leave the
    destination with columns no row ever fills.
  - ClickHouse's `max()`/`min()` over zero rows return the column type's
    *default* (the epoch, or `0`) rather than SQL NULL, so the watermark and
    partition-bound probes count rows alongside the aggregate. Without that, an
    empty source persists a 1970 watermark as though it had genuinely read up
    to there.
  - A response body that ends mid-IPC-message is an error, not a short read. A
    server aborting after the `200` has already been sent arrives as a clean
    EOF, which would otherwise look like a successful partial transfer — and get
    swapped into place.

- `quickhouse.ClickHouse(...)` gains `statement_timeout_secs` (the server's
  `max_execution_time`, read-path only), matching the other sources. Like
  theirs, it is a ceiling on the whole transfer rather than on the query; use
  `read_idle_timeout_secs` for the source-stalled case. `settings={...}` still
  wins over it.

- The CLI job file accepts `type = "clickhouse"` under `[source]`, and
  `examples/clickhouse_to_clickhouse.py` shows the round trip.

- `tests/test_clickhouse_source.py` runs the whole path against a live
  ClickHouse 24.8 (the one in `docker-compose.yml`) — full refresh across four
  read partitions, incremental idempotency, the empty-source watermark,
  `chunk_rows` keyset chunking at a row count deliberately not a multiple of the
  chunk size, `lookback_seconds` re-reading a row restated exactly at the
  committed watermark, a `source_query` with a `column_transforms` value
  transform, the unsupported-type error and its `exclude=` escape hatch,
  `reconcile_keys`, and the type round trip column by column. Two of those tests caught real bugs before this shipped:
  the missing LZ4 feature and the `Nullable(LowCardinality(...))` DDL below.

### Fixed

- **A nullable `LowCardinality` column generated invalid DDL.**
  `ColumnType::clickhouse_type()` wrapped it as
  `Nullable(LowCardinality(String))`, which ClickHouse rejects outright
  (`Code: 43 ILLEGAL_TYPE_OF_ARGUMENT`) — the only legal spelling puts `Nullable`
  on the inside. Reachable before this release through
  `type_overrides={col: "LowCardinality(String)"}` on any nullable column, and
  unavoidably by every nullable low-cardinality column read from a ClickHouse
  source.

### Changed

- The `arrow` dependency gains the `ipc_compression` feature. ClickHouse
  compresses its `FORMAT ArrowStream` buffers with LZ4 by default, and without
  the feature every read of a ClickHouse source fails on the first buffer with
  *"lz4 IPC decompression requires the lz4 feature"*. No new crate enters the
  dependency graph — `lz4_flex` and `zstd` were already there. Writers are
  unaffected: IPC compression is opt-in per `IpcWriteOptions`, and the ClickHouse
  sink doesn't ask for it.

## [0.15.0] — 2026-08-31

Four things a production fleet could see and quickhouse could not: a destination
that silently diverges from a source that deletes rows, a timeout knob that
measures the wrong subsystem, a MERGE prune that is correct and useless at the
same time, and every silent-corruption signal reaching the caller only as log
text.

### Verified against live services

Every path below was exercised against a real service before release, which is
new: 0.14.1's `MERGE` fix shipped with its live tests unrun, and 0.14.0's
outage was caused by a change no live test covered.

- **BigQuery** — the four live `MERGE`/reconcile tests now pass against a real
  dataset. They had **never once been executed**, and running them surfaced two
  bugs in the harness itself: the live sink bypassed the entry points that
  install rustls' `CryptoProvider` (so every test panicked before a request left
  the process), and the assertions compared `insert_batches`' approximate
  wire-**bytes** return against a row count of 2. Both fixed; the suite now
  covers the default-on key-range prune, the new key-list prune and its
  over-limit fallback, the partition prune with `WHEN NOT MATCHED BY SOURCE`,
  the clustering probe, and `reconcile_keys`' `distinct_keys`/`delete_keys`.
- **CleverTap** — 146,864 records across 32 pages at 100.00% coverage against an
  independent raw walk of the same window.
- **PostgreSQL/MySQL → ClickHouse** — the window-scoped delete and
  `reconcile_keys` measure/repair cycle, end to end against live services
  (`tests/test_reconcile.py`), including the assertion that an ordinary
  incremental sync does *not* remove a hard-deleted row.

### Fixed — CleverTap paging (the contract is no longer disputed)

- **The CleverTap events source could not read a single page, and the two
  contract defects behind the "reads one page" symptom are both confirmed and
  fixed.** Verified live against sg1 on 2026-08-31; the captured chain is
  committed as fixtures in `crates/quickhouse-core/tests/fixtures/clevertap/`
  and the tests now run against those rather than against hand-written JSON that
  restated the code's own assumptions.

  Three separate defects, each independently fatal:

  1. **The cursor was percent-encoded before being sent.** CleverTap hands it
     back *already* encoded — an observed cursor is ~1,900 characters of
     alphanumerics plus `%2B`, `%2F` and `%3D` and nothing else. Encoding it
     again turns `%2B` into `%252B`, and the vendor answers every such request
     with HTTP **200** carrying
     `{"status":"fail","error":"Incorrect Usage","code":3}`. This was a total
     outage of the source, not a truncation: **no page ever loaded.** The
     encoding was added for a sound-sounding reason — an opaque base64-ish
     token "routinely contains `+`, `/` and `=`" — which is true of the decoded
     token and never of the wire form.
  2. **The next cursor was read from `cursor` on data pages, which key it
     `next_cursor`.** A data page carries no `cursor` key at all, so the parsed
     cursor was always `None`. (The *create* response does key it `cursor` —
     that asymmetry is real, and both are now accepted in both places.)
  3. **`status == "success"` was treated as terminal.** Every real page says
     `"success"`, final or not; `"partial"` is never sent. The chain ends when
     `next_cursor` is absent, and only then — the real terminal page of a
     32-page export is literally `{"status":"success"}`, with no `records` key.

  Defects 2 and 3 produce the identical symptom, which is why a short table
  could never say which was at fault and why the rule was deliberately frozen
  pending evidence. Measured on the captured account, the old rule would have
  read **4,991 of 146,852 records (3.40%)** for one event-day and reported it a
  clean success. After the fix, the same window reads 146,864 records across 32
  pages at 100.00% coverage against an independent raw walk.

- **Two vendor states that arrive as HTTP 200 with a `fail` body are now
  retried instead of failing the transfer**: `code: 2` ("export still
  materialising") and an `error` of "Too many requests". Bounded retries against
  the same cursor. Both are asserted by the production audit; neither was
  reproduced during capture, so this is the defensive reading — it costs a
  bounded wait if they never occur and prevents a spurious hard failure if they
  do.

- **A cursor that stops advancing now ends the chain with a loud warning**
  rather than spinning forever.

### Added — CleverTap benchmarking and contract tooling

- **`benchmarks/bench_clevertap.py`** benchmarks the source against a real
  account in two lanes over the same window: a tolerant raw walk that
  establishes ground truth, and `sync()` timed against it. The headline is
  **coverage**, not throughput, because a truncated read produces a
  great-looking rows/s figure that describes nothing — at less than full
  coverage it says so and points at the capture tooling. Reads credentials from
  Secret Manager or the environment, never printing them.
- **`crates/quickhouse-core/tests/fixtures/clevertap/capture.py` now works.**
  Three bugs kept it from ever writing a fixture: it double-encoded the cursor
  like the source did; its scrubber substituted an *email-shaped* placeholder
  for emails and a *10-digit* placeholder for digit runs, so its own leak check
  rejected what it had just written; and long identifiers arriving as JSON
  numbers or floats bypassed the string-only scrubber (a float's shortest
  round-trip rendering reads as a 16-digit run). It also now redacts opaque
  tokens by *shape* — a real capture surfaced a 64-character `push_token` that
  every key-name and digit rule waved through — and truncates `records` to a
  couple of rows (recording the true count as `_records_total`), so a fixture
  proving a cursor key is 44 KB rather than 3 MB.

### Added — deletes and reconciliation

- **`reconcile_keys()`: measure, and optionally repair, drift against a source
  that hard-deletes.** An incremental sync is insert-and-update only. It finds
  rows whose watermark moved; a row the source *deleted* has no watermark to
  move, so nothing about it ever reaches the destination again and it stays
  there forever. For an ERP cancelling reservations, a queue draining, or a
  "soft delete" that is really a `DELETE`, the destination drifts
  one-directionally and without bound, and there was no supported way to detect
  it or repair it.

  The drift also hides well, because it is lopsided: a measured production table
  carried **+1.30% phantom rows against +0.0168% on a quantity sum**, so every
  COUNT-based model over-reported materially while every SUM-based one looked
  fine. Row counts alone do not tell you the exposure.

  `reconcile_keys()` reads the source's keyset over a bounded window, reads the
  destination's over the same window, and reports `orphan_keys` (in the
  destination, gone from the source — the drift) and `missing_keys` (in the
  source, absent from the destination — sync lag, or an incomplete load). With
  `delete=True` it removes the orphans and reports `rows_deleted`. Measuring is
  the default because it is the part worth running continuously; deleting is the
  part worth approving.

  Guards, because a reconcile is only as good as its window: `delete=True`
  requires a window (an unbounded delete is a full refresh with a race in it, not
  a reconcile); `max_delete_keys` refuses to act above a ceiling you set; and a
  diff that finds *no keys in common at all* is refused outright rather than
  reported as 100% drift — genuine drift is one-directional, so total
  disagreement means the two sides rendered the key differently or the window
  predicates disagreed, not that everything is deletable.

  PostgreSQL and MySQL sources, ClickHouse and BigQuery destinations,
  single-column keys.

- **`delete_stale_in_window=True` now works for a ClickHouse destination**, not
  just BigQuery — the in-sync half of the same problem. BigQuery expresses it as
  a `WHEN NOT MATCHED BY SOURCE` clause inside its own `MERGE` (atomic with the
  upsert, but BigQuery reports one combined affected-row count for the statement,
  so no separate delete count is attributable). ClickHouse runs a lightweight
  `DELETE FROM dest WHERE <window> AND key NOT IN (SELECT key FROM staging)` just
  before the staged rows are promoted: this forces the run to stage (ClickHouse
  incremental otherwise inserts directly, and the delete needs a materialised
  batch to subtract from), is *not* atomic with the insert, and reports the exact
  count. It still requires `merge_prune_partition_by` to scope the window — an
  unscoped delete would remove the destination's entire history outside the
  batch. Setting the flag against a ClickHouse destination previously did
  nothing at all, silently.

### Added — timeouts that measure what their names say

- **`read_idle_timeout_secs` (default `0` = off): fail when *no source rows
  arrive* for that long.** `statement_timeout_secs` reads as a cap on query
  duration, but quickhouse streams the source result set straight into the
  destination, so the statement producing it stays open from the first row read
  to the last one written. The server therefore counts read + decode +
  destination write + backpressure against it. Measured here: a **0.64s**
  server-side sequential scan (EXPLAIN ANALYZE, all shared-buffer hits) failed
  **9 consecutive times** against a 90s `statement_timeout_secs`, because the
  BigQuery Storage Write path was throttling that day — and it failed as
  `57014 canceling statement due to statement timeout`, sending an operator
  hunting for a slow query and a missing index that did not exist. Retries were
  structurally unable to help: every attempt restarts from zero against the same
  ceiling.

  The new timer wraps the awaits on the source stream and nothing else. Time
  spent decoding, inserting, or blocked on the memory budget does not count, so
  a slow destination cannot trip it — which is what makes it safe to set
  tightly — while a genuinely hung source does. The resulting error is
  classified transient, so `retry_max_attempts` retries the whole transfer.
  PostgreSQL, MySQL and BigQuery source reads.

- **`statement_timeout_secs`' docstring now says what it actually bounds.** It
  is a ceiling on the whole streamed transfer, not on the source statement, and
  it says so — along with the reason a retry cannot rescue a table that crosses
  it.

### Added — MERGE pruning that binds

- **`merge_prune_key_list_max` (default `0` = off): bound a BigQuery `MERGE` to
  the batch's exact key list instead of its key range.** `merge_prune_key_range`
  (on by default since 0.14.0) is sound and it is a tautology — a destination row
  can only match a key the batch holds. But its *effectiveness* collapses when
  the changed rows are scattered across the key space rather than clustered at
  the top of it, which is the normal shape for any table whose rows are updated
  after insert.

  Measured across 7 days of production MERGEs: 12,718 statements, 1.451 TiB
  billed, 12.98 hours of MERGE time. One table — correctly clustered on its merge
  key — still scanned roughly half of itself per run and accounted for 2.34 of
  those hours, because its updates span the whole id range and so `[MIN, MAX]`
  covers nearly the entire table. The prune was correct and useless at once.

  When the staging batch holds at most `merge_prune_key_list_max` distinct key
  values, the bound becomes `T.k IN (v1, v2, …)` instead. Same tautology, no
  immutability contract, and it does not degrade when the keys are scattered.
  Costs one small extra query per merge; if the batch exceeds the ceiling the
  list is abandoned and the range bound is used, so the limit really bounds the
  generated statement. Single-column keys only (a composite would need an
  `IN UNNEST([STRUCT(…)])` form whose pruning behaviour is not the same), and
  skipped under `delete_stale_in_window` for the same reason the range bound is.
  Off by default: 0.14.0 shipped a default-on prune change that broke every
  BigQuery merge, and this one earns its default in the field first.

- **quickhouse now warns when it MERGEs into a destination that is not clustered
  by the merge key.** There the key bound has nothing to prune with and every run
  scans the whole table — invisible from the caller's side until it appears on a
  bill. In the same 7 days, one unclustered 11.3 GiB table scanned ~10.4 GiB per
  run across 42 runs: **0.428 TiB**, the single largest MERGE cost measured.
  quickhouse generates clustered DDL itself, so it knows what good looks like;
  this only fires for a destination created some other way. Surfaces as a
  `TransferWarning` with kind `"unclustered_merge_target"`.

### Added — `TransferResult` you can act on

- **`TransferResult.warnings: list[TransferWarning]`.** Every silent-corruption
  signal quickhouse already computes reached the caller only as log text, where
  an orchestrator could not act on it: a Dagster asset succeeded while a column
  rotted. Each warning carries `.kind` (stable, machine-readable), `.column`,
  `.count`, `.sample` and `.message`, aggregated per `(kind, column)` across the
  run and ordered most-affected first. Nothing is raised — these are values, and
  the caller decides which should fail their pipeline.

  The kinds: `collapsed_bool` (a MySQL `tinyint(1)` outside `{0,1}` flattened,
  losing e.g. 2 vs 3), `null_watermark` (rows excluded from this and every future
  incremental run, permanently — the most dangerous one), `coerced_decimal`,
  `coerced_date`, `coerced_scalar`, `full_refresh_shrink`, and
  `unclustered_merge_target`.

- **Coercions are now attributed per column, not just counted per table.** The
  four decoders tracked a handful of scalar totals, which was enough to log a
  sentence and not enough to act on — "9 columns across 8 tables were genuine
  small integers flattened to `1`" cannot be reconstructed from a per-table
  total. The column index was already in hand at every increment site, so
  attribution is free; the log lines now name the columns too.

- **`TransferResult.read_secs` / `.stage_secs` / `.promote_secs`.**
  `duration_secs` alone could not tell a caller whether to tune the source query,
  the insert path, or the destination DDL — that had to be reconstructed from
  BigQuery job metadata after the fact, and not at all for a ClickHouse
  destination. `read_secs` is the time spent *waiting on source rows* (the source
  awaits alone, excluding decode, insert and backpressure), summed across
  parallel readers; `stage_secs` is the streaming phase's wall time;
  `promote_secs` is the swap / `MERGE` / insert-select / delete / watermark
  persist that follows.

- **`TransferResult.rows_deleted`**, for the ClickHouse window-scoped delete and
  a `reconcile_keys` repair. `0` for a BigQuery `delete_stale_in_window`, which
  performs its delete inside the `MERGE` and reports only one combined
  affected-row count — there is no way to attribute the delete portion, so this
  reports `0` rather than a number that is really the merge's total.


### Changed — BREAKING
- **`mode=` is now required.** It used to default to `"full"`, which REPLACES the
  destination table wholesale — the most destructive of the three modes, handed
  to anyone who did not think about the argument. `sync()` now raises unless you
  name `"full"`, `"incremental"` or `"append"`.
- **A full refresh that would shrink the destination is now refused.**
  `mode="full"` resolves to `atomic_swap` — ClickHouse `EXCHANGE TABLES`,
  BigQuery `TRUNCATE` + `INSERT ... SELECT` — and **neither is partition-aware**.
  A run covering one partition therefore destroyed every other partition,
  atomically, and exited successfully. This was previously a `tracing::warn!` on
  the API path only (and nothing at all on the DB path), so it never stopped
  anything: a Postgres full refresh scoped to one month, swapped into a
  monthly-partitioned table, lost the other eleven by the identical code path.
  The new `guard_full_refresh_shrink` runs before *every* swap site. Set
  `allow_full_refresh_shrink=True` for a shrink that is genuinely intended, or
  use `mode="incremental"` with `key=` / `mode="append"` to add rather than
  replace. A row count the sink cannot report is logged and does not block.

### Added
- CleverTap contract-capture tooling, because the paging contract is disputed
  (see below): `crates/quickhouse-core/tests/fixtures/clevertap/capture.py`
  captures and scrubs real pages into fixtures, and
  `examples/clevertap_contract_probe.py` walks a whole export chain and reports
  what each candidate termination rule *would* have read, as a percentage of the
  real chain. Both read-only.

### Fixed
- **CleverTap/HTTP-API error paths could panic on a non-ASCII response body.**
  Both sources built their error message with `&body[..body.len().min(200)]`,
  which slices a `String` at a fixed *byte* offset and panics outright when that
  offset falls inside a multi-byte UTF-8 sequence. Any sufficiently long vendor
  error message in a non-Latin script crashed the process instead of producing
  the error it was assembling. Replaced with a shared `source::body_head` that
  truncates bytes and decodes lossily.
- **CleverTap paging cursors are now percent-encoded.** The data-page URL was
  built by interpolation (`?cursor={cursor}`); an opaque vendor cursor
  containing `+`, `/`, `=` or `&` was silently corrupted on the wire — `+`
  decodes server-side as a space, and `&` truncates the parameter outright.
  Now built with `Url::query_pairs_mut`, matching `appsflyer::report_url`.
- **A CleverTap page whose `records` field is present but not an array is now an
  error** rather than a silent zero-row page. The previous
  `.and_then(as_array).unwrap_or_default()` turned a vendor protocol change into
  an invisible empty read that the caller reported as a clean success.

### Changed
- **CleverTap page errors now carry `code` and the full `error` value.** A
  `{"status":"fail","code":2}` response used to render as `unexpected status
  'fail' ` — a dangling empty field, with the one value indicating whether the
  condition is retryable dropped entirely, along with any non-string `error`.
  The response body head is now echoed too.
- **The CleverTap paging loop logs what it read and why it stopped**: page
  count, records per page, and the termination reason. It warns when it stops
  while still holding an unfollowed cursor, and when an export ends after a
  single page — the signature of a paging failure rather than an empty day.
  Every exit from this loop was previously a bare `break` that logged nothing.

### Documentation
- **The CleverTap paging contract is recorded as disputed and unverified.** The
  module previously asserted a contract "verified live against the sg1 region";
  a later production audit asserts a materially different one for the same
  region (`next_cursor` rather than `cursor`, `status:"success"` on every page,
  `code:2` as retryable, HTTP-200-shaped throttling). Neither is reproducible
  from this repository — there is no captured page or fixture, and the unit
  tests were hand-written from the same assumption as the code they test. The
  termination rule is deliberately left unchanged pending a captured raw page
  rather than swapped for an equally unverified one.
- Removed stale "BigQuery-only destination" claims left over from before
  `bc1ab45` (API sources gained ClickHouse support). One of them was
  load-bearing: `decode_api.rs` justified its *absence* of a `ch_range` clamp on
  the destination being BigQuery, which stopped being true. That is now recorded
  as a KNOWN GAP — an API source declaring a DATE/TIMESTAMP value outside
  ClickHouse's representable window is passed through rather than nulled with a
  counter, unlike every other decode path.
- Deleted an orphaned comment on `api_columns_of` describing a destination
  rejection (`ensure_api_dest_supported`) that was removed a release earlier.

## [0.14.1] - 2026-08-20

A single-defect patch release. 0.14.0's `merge_prune_key_range` shipped on by
default emitting SQL BigQuery refuses, so **every** BigQuery `MERGE` failed from
the moment it was deployed. Upgrade straight past 0.14.0 if you write to
BigQuery in `mode="incremental"`.

### Fixed
- **`merge_prune_key_range` (default `True` since 0.14.0) broke every BigQuery
  `MERGE`.** It expressed its bound as two scalar subqueries over the staging
  table inside the `ON` clause, and BigQuery rejects a subquery that references a
  table in a join predicate: `Unsupported subquery with table in join predicate`.
  That is an *analysis*-time rejection, so it did not depend on the data — every
  incremental transfer to BigQuery failed on its first merge after 0.14.0 went
  out, zero-row no-op runs included. Bounds are now resolved from the staging
  batch up front and pasted in as literals, which is legal in a join predicate
  and is also the form BigQuery prunes partitions and clustering blocks on, so
  the feature does what it was written to do. The literals are rendered by
  BigQuery itself (`FORMAT('%T', …)`): every type comes back in exact constant
  syntax, and the `MIN`/`MAX` that picks a bound is evaluated by the same engine,
  over the same table, as the `BETWEEN` that uses it.

  No data was lost by the outage: the watermark is persisted only *after* a
  successful merge, so each failed run left its cursor untouched and the next run
  re-read the same window. Catch-up is automatic on upgrade. If you worked around
  it by passing `merge_prune_key_range=False` on BigQuery-target syncs, you can
  drop the override — it is no longer doing anything for you.
- **`merge_prune_partition_by` had the identical defect, latent since 0.4.0**,
  both in the `ON` clause and in the `WHEN NOT MATCHED BY SOURCE` condition that
  scopes `delete_stale_in_window`. It went unnoticed only because it is opt-in;
  anyone who had adopted it would have hit this in 0.4.0. Both knobs now use the
  literal form.
- An empty staging batch (or a bound column that is all-`NULL` in it) leaves
  `MIN`/`MAX` NULL and now emits no bound at all rather than an unsatisfiable
  one. With `delete_stale_in_window` the `WHEN NOT MATCHED BY SOURCE` clause is
  dropped in that case — never emitted unscoped, which would delete the
  destination's whole history — matching what the old form evaluated to on an
  empty batch.

### Added
- **Live BigQuery `MERGE` tests that execute the generated statement**, covering
  key-range pruning, the partition-prune + `delete_stale_in_window` pair, and the
  zero-row case. Skipped unless `QUICKHOUSE_BQ_PROJECT` and
  `QUICKHOUSE_BQ_DATASET` name a dataset the test may create and drop tables in.
  Plus a credential-free assertion that no merge condition ever contains a
  subquery, in any combination of the prune knobs. The 0.14.0 form was rejected
  when BigQuery analysed it, so no assertion on the statement's text could have
  caught it — only handing it to BigQuery, even as a dry run, can.

### Changed
- A merge with pruning active now runs one extra small query first: `MIN`/`MAX`
  over the bounded columns of the staging table, which holds only the delta. Set
  `merge_prune_key_range=False`, and leave `merge_prune_partition_by` unset, to
  skip it.

## [0.14.0] - 2026-08-20

A performance release. Three independent measurements agreed that the reader was
never the bottleneck — quickhouse's entire extract-load-merge into ClickHouse
finished faster than a purpose-built Arrow reader took to *extract* the same
slice — so nothing here touches the read transport. What it does instead is turn
on parallelism that was structurally switched off, stop the BigQuery `MERGE`
full-scanning a table quickhouse itself clustered, and get the decode loop off
the async reactor.

### Changed — please read before upgrading
Four defaults changed. None of them reject a configuration that used to work,
but each changes what a call does:

- **`BigQuery(write_method=...)` now defaults to `"storage_write"`** instead of
  `"insert_all"`. The legacy `insertAll` path bills $0.01 per 200 MiB against
  Storage Write's $0.025/GB with 2 TiB/month free — roughly double, and slower.
  Storage Write uses a different API call on the same write permission
  (`CreateWriteStream`), so smoke-test a throwaway dataset before a large run,
  and pass `write_method="insert_all"` explicitly to keep the old path.
- **`parallelism` now defaults to `0`, meaning "derive from the host"** (CPUs
  available to this process, container CPU quota included) rather than a fixed
  `4`. An explicit value is still honoured exactly as before. Note this only
  raises a ceiling: whether a read actually fans out depends on it being
  partitionable at all — see `partition_source_expr` below.
- **Inserts are now coalesced.** Decoded batches accumulate to `insert_bytes`
  (new, default 32 MiB) before being sent, instead of one insert per batch.
  `batch_bytes` had silently been setting both, so at its 4 MiB default a
  19.4M-row table meant 900+ HTTP round-trips and 900+ new ClickHouse parts —
  against ClickHouse's own guidance of fewer, larger inserts, since part count
  drives background merge work that on Cloud competes with query memory. Set
  `insert_bytes=0` to restore one-insert-per-batch. This does not raise peak
  memory; see *Performance* below for why.
- **The generated BigQuery `MERGE` now bounds its destination scan to the
  staging batch's key range** (`merge_prune_key_range`, new, default `True`).
  Unlike `merge_prune_partition_by` this needs no immutability contract and is
  therefore safe to have on: bounding on the *join key* is a tautology, not an
  assumption — a destination row can only match by holding a staging row's exact
  key value, which is inside that batch's own `[MIN, MAX]` by construction — so
  no configuration exists in which it changes which rows merge. It is
  automatically skipped when `delete_stale_in_window` is set, where narrowing the
  `ON` clause would quietly reduce "replace this window" to "replace this key
  range". Pass `merge_prune_key_range=False` to restore the unbounded join.

### Added
- **`partition_source_expr` — parallel reads for `source_query` transfers.**
  This was the single largest unrealised speedup in the project. Range
  partitioning needs a key column it can probe `MIN`/`MAX` on and bound with an
  indexable predicate, and `source_query` hides that column behind its own
  projection, so *any* custom-query transfer silently ran single-stream however
  large it was — and since a `CAST` can only live in `source_query`, that covered
  essentially every non-trivial table. `parallelism` was simply inert for them.
  Have `source_query` additionally project the raw, indexed key column under a
  second name (e.g. `id AS id_raw`) and set `partition_source_expr="id_raw"`;
  the planner then probes and bounds on that bare pass-through column, exactly
  as `watermark_source_expr` already does for the incremental filter. Two costs
  are documented on the parameter: the probe now runs against the wrapped query,
  and an expression that is missing or non-integer is a hard error rather than a
  silent fallback — an explicitly requested fan-out that quietly collapses to one
  stream is the bug this exists to fix. Leaving it unset keeps the old
  single-stream behaviour, now logged rather than invisible.
- **`ClickHouse(settings={...})` — arbitrary ClickHouse settings passthrough**,
  sent as query parameters on every request (DDL, inserts, reads, swaps). Server
  behaviour that only ClickHouse can decide was previously unreachable from the
  client at any price. One concrete case it fixes today: on a Cloud service with
  lagging replicas, quickhouse's own post-swap row-count guard can read a stale
  replica, see 0 rows, and fail a run that in fact succeeded — now fixable from
  the caller with `{"select_sequential_consistency": "1"}`.
- **`max_memory_fraction` — size the memory ceiling from the host** instead of an
  absolute byte count, read from the cgroup limit where there is one. An absolute
  ceiling is the wrong *unit* once containers share a VM: four containers capped
  at 4 GiB on one 16 GiB box each need to size against their own 4 GiB, and only
  the container knows which it is. If the host won't report a limit (notably off
  Linux), `max_memory_bytes` is kept as configured rather than silently becoming
  unbounded.
- **`ClickHouse(insert_dedup_token=True)` — exactly-once inserts, opt-in.**
  Attaches a generated `insert_deduplication_token` that is unique per insert and
  identical across that insert's own retries, so a retry whose original the
  server had already committed is discarded rather than duplicating rows. It ships
  **off by default** on purpose: ClickHouse deduplicates per *block*, not per
  request, and a single insert large enough to be split server-side shares one
  token across its blocks — if that makes later blocks look like duplicates of
  the first they are dropped silently, which is data loss rather than a visible
  error. Coalesced inserts make multi-block inserts more likely. Verify on a
  `Replicated*MergeTree` with a large insert, comparing row counts, before
  enabling it in production.

### Performance
- **Decode no longer runs on the async reactor.** `CopyDecoder::feed` is a
  two-pass per-tuple parse into Arrow builders — the CPU-heaviest step in the
  pipeline — and it ran inline on the Tokio worker that owns the socket. That
  worker could poll nothing else for the duration of every parse, and the `COPY`
  stream sat idle between chunks instead of draining. The parse now runs on the
  blocking pool while the next chunk is pulled off the socket concurrently. One
  consequence worth knowing: `read_max_rows_per_sec` now bites one chunk later
  than it used to, since a chunk is already in hand when the throttle is checked.
  It still bounds the sustained rate. (The MySQL path is unchanged — it decodes
  per row between awaits, so it has no long inline CPU block to move.)
- **The Arrow IPC payload is serialized incrementally rather than materialized.**
  Compression already streamed; the serialization built a complete `Vec<u8>`
  first. Harmless at 4 MiB batches, which is why it had never mattered — but it
  would have become the dominant allocation of the pipeline the moment inserts
  were coalesced to tens of MiB, multiplied by `parallelism`. Peak serialization
  memory is now flat in the payload size, which is what lets `insert_bytes` grow
  without RSS following it, and keeps `max_memory_bytes` the honest single number
  it claims to be.

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

[Unreleased]: https://github.com/mmirzafahmi/quickhouse/compare/v0.16.0...HEAD
[0.16.0]: https://github.com/mmirzafahmi/quickhouse/compare/v0.15.0...v0.16.0
[0.15.0]: https://github.com/mmirzafahmi/quickhouse/compare/v0.14.1...v0.15.0
[0.14.1]: https://github.com/mmirzafahmi/quickhouse/compare/v0.14.0...v0.14.1
[0.14.0]: https://github.com/mmirzafahmi/quickhouse/compare/v0.13.0...v0.14.0
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
