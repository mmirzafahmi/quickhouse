# Sync modes

`sync()` runs in one of three modes, chosen with `mode=`.

```{raw} html
<div class="qh-modes">
  <a class="qh-mode qh-mode--current" href="#full-refresh" aria-current="page">
    <div class="qh-mode__name">full</div>
    <div class="qh-mode__desc">Reload the whole table, swap it in atomically. Replaces what is there.</div>
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

`mode="full"` reloads the whole table into a staging table, then swaps it into
place atomically — a crash mid-run never leaves the destination partial. The
watermark is unused and ignored in this mode, and the returned `new_watermark`
is `None`.

```{warning}
**`mode` used to default to `"full"`. Since 0.15.0 it is required**, because a
full refresh *replaces* the destination and neither sink's swap is
partition-aware: a run covering one partition destroys every other partition,
atomically and with a success exit. Relatedly, a full refresh that would leave
the destination with **fewer** rows than it had is now refused — pass
`allow_full_refresh_shrink=True` if the source genuinely did lose rows, or use
`mode="incremental"` with `key=` / `mode="append"` to add rather than replace.
```

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

`merge_prune_key_range=True` (the default since 0.14.0) additionally bounds the
scan to the batch's `[MIN, MAX]` on the merge `key` itself. That one needs no
immutability contract — it's a tautology, since a destination row can only match
by holding a key the batch contains.

But a range only prunes as well as the changed keys cluster. On an append-only
table the delta sits at the top of the key space and the range is narrow; on a
table whose rows are *updated after insert* — a user profile, an order status, a
voucher redemption — the changed keys are scattered across the whole key space,
`[MIN, MAX]` covers nearly everything, and the bound is correct and useless at
the same time. Measured on one such table, correctly clustered on its merge key:
still ~half the table scanned per run.

`merge_prune_key_list_max=N` (new in 0.15.0, default `0` = off) is the tighter
form. When the batch holds at most `N` distinct key values, the bound becomes
that exact list — `T.k IN (v1, v2, …)` — which BigQuery prunes just as well on a
clustered table and which does *not* degrade when the keys are scattered.

```python
qh.sync(..., mode="incremental", key=["id"],
        merge_prune_key_list_max=5_000)
```

Same tautology, no new contract. It costs one small extra query per merge, and
if the batch turns out to hold more distinct keys than `N` the list is abandoned
and the range bound is used — so `N` really is a ceiling on statement size.
Single-column `key` only.

```{admonition} A bound only helps if the table is clustered by the key
:class: warning
None of this prunes anything on a destination that isn't clustered by the merge
key — the statement scans the whole table, every run, silently. quickhouse's own
generated DDL clusters by `key`, so this bites tables created some other way; a
measured 11.3 GiB unclustered table scanned ~10.4 GiB per run, 42 times in a
week. Since 0.15.0 quickhouse **warns** in that case, as a `TransferWarning` with
kind `"unclustered_merge_target"`.
```

(delete-stale-in-window)=
### Converging with a source that deletes rows

An incremental sync is insert-and-update only. It finds rows whose watermark
moved; a row the source **deleted** has no watermark to move, so nothing about it
ever reaches the destination again and it stays there forever. Against any source
that hard-deletes as a matter of routine — an ERP cancelling reservations, a
queue draining, a "soft delete" that is really a `DELETE` — the destination
drifts one-directionally and without bound.

It also hides well, because the distortion is lopsided. A measured production
table carried **+1.30% phantom rows against +0.0168% on a quantity sum**: every
COUNT-based model over-reported materially while every SUM-based one looked fine.
Row counts alone will not tell you your exposure.

There are two halves to fixing it.

**Inside a sync — `delete_stale_in_window=True`.** Additionally deletes
destination rows inside the merged window that the source pull no longer has
("replace this window"). It **requires** `merge_prune_partition_by` — the delete
is scoped to that immutable column's staging range so it never touches history
outside the batch (a hard error otherwise).

```python
qh.sync(..., mode="incremental", key=["id"],
        merge_prune_partition_by="create_date",
        delete_stale_in_window=True)
```

- **BigQuery** expresses it as a `WHEN NOT MATCHED BY SOURCE` clause inside the
  same `MERGE`, so it is atomic with the upsert. BigQuery reports one combined
  affected-row count for the statement, so `rows_deleted` stays `0` there.
- **ClickHouse** (new in 0.15.0) runs a lightweight `DELETE FROM dest WHERE
  <window> AND key NOT IN (SELECT key FROM staging)` just before the staged rows
  are promoted. That forces the run to stage, is *not* atomic with the insert,
  and reports the exact count on `TransferResult.rows_deleted`.

**Outside a sync — `reconcile_keys()`.** The flag above only converges the window
a sync happened to touch. To answer "how far apart are these two right now?" for
an arbitrary window, on its own schedule, diff the keysets:

```python
r = qh.reconcile_keys(
    src, dst, dest_table="stock_move_line",
    source_table="stock_move_line",
    key="id",
    window="create_date >= '2026-07-01' AND create_date < '2026-08-01'",
)
print(r.orphan_keys, r.missing_keys)   # measures; deletes nothing
```

`orphan_keys` are in the destination and gone from the source — the drift.
`missing_keys` are in the source and absent from the destination — ordinary sync
lag if small, an incomplete load if not. Pass `delete=True` to remove the
orphans and get `rows_deleted` back.

Measuring is the default because it's the part worth running continuously;
deleting is the part worth approving. A reconcile is only as good as its window,
so three guards apply: `delete=True` requires a window, `max_delete_keys` refuses
to act above a ceiling you set, and a diff that finds *no keys in common at all*
is refused outright — genuine drift is one-directional, so total disagreement
means the two sides rendered the key differently or the window predicates
disagreed, not that everything is deletable.

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
