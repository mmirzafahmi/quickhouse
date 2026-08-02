# Sync modes

`sync()` runs in one of three modes, chosen with `mode=`.

```{raw} html
<div class="qh-modes">
  <a class="qh-mode qh-mode--current" href="#full-refresh" aria-current="page">
    <div class="qh-mode__name">full</div>
    <div class="qh-mode__desc">Reload the whole table, swap it in atomically. The default.</div>
  </a>
  <a class="qh-mode" href="#incremental">
    <div class="qh-mode__name">incremental</div>
    <div class="qh-mode__desc">Only rows past the watermark. Idempotent; dedup on <code>key</code>.</div>
  </a>
  <a class="qh-mode" href="#append-bronze-landing">
    <div class="qh-mode__name">append</div>
    <div class="qh-mode__desc">Bronze landing for HTTP sources — no staging, no dedup.</div>
  </a>
</div>
```

## Full refresh

`mode="full"` (the default) reloads the whole table into a staging table, then
swaps it into place atomically — a crash mid-run never leaves the destination
partial. The watermark is unused and ignored in this mode, and the returned
`new_watermark` is `None`.

For a **BigQuery destination** the swap runs as a query (a billed scan of the
staged data), not a free copy job — BigQuery's copy jobs can silently skip rows
still sitting in a table's streaming buffer, so a real query is what keeps this
correct rather than just fast.

```{admonition} One accepted tradeoff on the ClickHouse path
:class: note
An insert retried after a *lost acknowledgment* (not after a crash — the
transfer is still running) can duplicate one batch's rows in the staging table,
since `mode="full"` has no engine-level dedup like `ReplacingMergeTree`. Rare,
and harmless for `key`-based incremental syncs, but worth knowing if you see an
unexpected small over-count on a full refresh right after a transient network
blip.
```

## Incremental

`mode="incremental"` tracks a high-water mark (the `watermark` column) in a
small state table in the destination and copies only newer rows. Re-running with
no new data does nothing.

```python
qh.sync(
    src, dst,
    dest_table="orders", source_table="orders",
    mode="incremental",
    watermark="updated_at",   # required
    key=["id"],
)
```

Updated rows (same `key`, newer `watermark`) are **deduplicated on `key`**, and
this is where the destinations differ:

- **ClickHouse** dedupes lazily via `ReplacingMergeTree` at merge time.
- **BigQuery** has no engine-level equivalent, so writes are staged and then
  upserted via a `MERGE` statement matched on `key` — which is therefore
  **required** for BigQuery incremental syncs (unlike everywhere else, where it
  is optional). The MERGE bills for bytes scanned but is naturally idempotent.

### Catching late-arriving rows

For daily syncs that need to catch late-arriving or edited rows, set
`lookback_seconds` to re-scan a trailing window of the watermark (e.g.
`3 * 86400` for the last three days). The dedup above keeps that overlap from
creating duplicates. Requires `key` or `order_by`, and a date/timestamp
watermark. `0` (default) disables lookback.

### Cursor control

A few knobs make the incremental cursor robust in the real world (all
incremental-mode only):

```{raw} html
<div class="qh-params">
  <div>
    <div>
      <div class="qh-params__name">state_key</div>
      <div class="qh-params__type">Optional[str] = None</div>
    </div>
    <p class="qh-params__desc">Pins the cursor's identity in the internal <code>_quickhouse_state</code> table. By default it's keyed by the source table (or <code>source_query</code> text) + destination &mdash; so editing a <code>source_query</code>'s <code>WHERE</code> would silently start a fresh full pull, and two syncs into one destination tracking different <code>watermark</code> columns would clobber each other's cursor. Set <code>state_key="orders:updated_at"</code> to give each a stable, distinct identity.</p>
  </div>
  <div>
    <div>
      <div class="qh-params__name">seed_watermark</div>
      <div class="qh-params__type">Optional[str] = None</div>
    </div>
    <p class="qh-params__desc">Seeds the cursor on the <strong>first</strong> run only, then self-retires, to an explicit floor value. Mutually exclusive with <code>skip_to_max</code>; safe to leave set.</p>
  </div>
  <div>
    <div>
      <div class="qh-params__name">skip_to_max</div>
      <div class="qh-params__type">bool = False</div>
    </div>
    <p class="qh-params__desc">Seeds the cursor on the <strong>first</strong> run only, then self-retires, to the source's current <code>MAX(watermark)</code>, reading almost nothing &mdash; for when the destination already holds complete data from a prior pipeline and a full first pull would be a waste. Mutually exclusive with <code>seed_watermark</code>; safe to leave set.</p>
  </div>
  <div>
    <div>
      <div class="qh-params__name">advance_watermark</div>
      <div class="qh-params__type">bool = True</div>
    </div>
    <p class="qh-params__desc">Set to <code>False</code> to read and merge a window <em>without</em> moving the cursor &mdash; for loading a historical backfill without rewinding your regular schedule. The computed watermark is still returned in <code>TransferResult.new_watermark</code>.</p>
  </div>
  <div>
    <div>
      <div class="qh-params__name">chunk_rows</div>
      <div class="qh-params__type">Optional[int] = None &mdash; experimental</div>
    </div>
    <p class="qh-params__desc">Reads the source in keyset-ordered chunks of <code>N</code> rows, committing the cursor per chunk so a mid-read failure resumes instead of restarting. <strong>ClickHouse destination + incremental only</strong>, and the keyset column (<code>partition_column</code>, else the first <code>key</code>) must be a <strong>unique, NOT NULL integer</strong>. Single-stream (<code>parallelism</code> is ignored). <code>None</code> (default) = one read.</p>
  </div>
</div>
```

### MERGE cost on large BigQuery tables

By default an incremental `MERGE` scans the whole destination table (it joins on
`key` only), so upserting a few delta rows into a huge partitioned table bills
the whole table each run.

```python
qh.sync(..., mode="incremental", key=["id"],
        merge_prune_partition_by="created_at")   # bound the scan
```

`merge_prune_partition_by="<col>"` bounds the scan to the staging batch's range
and lets BigQuery prune partitions.

```{admonition} Only for an immutable column
:class: danger
Point it **only** at a column whose value never changes for a given `key` — a
`created_at`/inserted-at column that is also the partition column. Do **not**
point it at an `updated_at` column: an updated row's new value lands in a
different partition than the existing row, so pruning would miss it and *insert a
duplicate key* instead of updating. quickhouse can't detect mutability, so this
is a deliberate opt-in; the default full scan is always correct.
```

`delete_stale_in_window=True` (BigQuery incremental) additionally `DELETE`s
destination rows inside the merged window that are absent from the source pull
("replace this window"). It **requires** `merge_prune_partition_by` — the DELETE
is scoped to that immutable column's staging range so it never touches history
outside the batch (a hard error otherwise).

(append-bronze-landing)=
## Append (bronze landing)

`mode="append"` (HTTP API sources only) inserts each window's rows straight into
the destination with **no** staging/merge/swap and no dedup — a bronze-landing
write for when you run your own consolidation MERGE downstream. `watermark`
drives the resume window; `key` isn't required. It avoids per-run
table-metadata churn (BigQuery's ~5-ops/10s/table limit) since there's no
staging create/swap.

```python
qh.sync(
    qh.CleverTap(...),
    qh.BigQuery("my-project", dataset_id="bronze"),
    dest_table="clevertap_events_raw",
    mode="append", watermark="ts",
)
```

## Staging tables

Both full and incremental modes stage through a per-run-unique table
(`{dest}_quickhouse_tmp_<id>`) that's dropped when the run finishes, including on
failure. The unique name is what makes rapid re-runs and whole-call retries safe
on BigQuery, whose streaming ingestion rejects writes into a table recently
recreated under the same name. You can override the suffix with
`staging_suffix=` and the state table name with `state_table_name=` if your
table-naming policy requires it.
