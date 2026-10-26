# Destinations

Every transfer is `sync(source, target, ...)`. A **destination** is a
`ClickHouse` or `BigQuery` connection descriptor, or a `Csv` folder on a local
path. `ClickHouse` and `BigQuery` also work as a
[source](../sources/index.md); everything else about the call is
identical regardless of which engines you use.

```python
import quickhouse as qh

qh.ClickHouse("http://host:8123", database="analytics")
qh.BigQuery("my-gcp-project", dataset_id="analytics")
qh.Csv("/data/exports")                       # new in 0.21.0
```

For the exact constructor signatures see the [API reference](../../api.md).

```{raw} html
<div class="qh-modes">
  <a class="qh-mode qh-mode--current" href="clickhouse.html">
    <div class="qh-mode__name">ClickHouse</div>
    <div class="qh-mode__desc">MergeTree-family DDL, atomic swap. Optional Parquet backup of every synced batch to GCS or S3.</div>
  </a>
  <a class="qh-mode" href="bigquery.html">
    <div class="qh-mode__name">BigQuery</div>
    <div class="qh-mode__desc">MERGE-based upsert. Requires a dataset_id; insert_all or Storage Write transport.</div>
  </a>
  <a class="qh-mode" href="csv.html">
    <div class="qh-mode__name">CSV files</div>
    <div class="qh-mode__desc">A folder of part files per table on a local path. Full refresh and append; gzip optional.</div>
  </a>
</div>
```

## Destination DDL

The DDL knobs (`engine`, `partition_by`, `order_by`, `primary_key`, `key`) are
interpreted per destination:

- **ClickHouse** — they shape the `MergeTree`-family DDL.
- **BigQuery** — `engine` is ignored; `partition_by` must be a bare
  `DATE`/`DATETIME`/`TIMESTAMP` column name (not a SQL expression);
  `order_by`/`key` become clustering columns (at most 4 total; `key` leads for
  an incremental table, so its `MERGE` can prune, and `order_by` leads
  otherwise).
- **CSV** — none apply: `engine`, `partition_by`, `order_by` and `primary_key`
  are refused, and `key` only picks the column a parallel read splits on. See
  [CSV files](csv.md#options).

quickhouse creates the table for you (`create_if_missing=True` by default) with
a schema derived from the source. See [Type mapping](../type-mapping.md) for how
source types become destination types.

```{toctree}
:hidden:

clickhouse
bigquery
csv
```
