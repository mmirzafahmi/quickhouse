# Destinations

Every transfer is `sync(source, target, ...)`. A **destination** is a
`ClickHouse` or `BigQuery` connection descriptor. The same `BigQuery` class
also works as a [source](../sources/index.md); everything else about the call is
identical regardless of which engines you use.

```python
import quickhouse as qh

qh.ClickHouse("http://host:8123", database="analytics")
qh.BigQuery("my-gcp-project", dataset_id="analytics")
```

For the exact constructor signatures see the [API reference](../../api.md).

```{raw} html
<div class="qh-modes">
  <a class="qh-mode qh-mode--current" href="clickhouse.html">
    <div class="qh-mode__name">ClickHouse</div>
    <div class="qh-mode__desc">MergeTree-family DDL, atomic swap. Optional S3 archive of every synced batch.</div>
  </a>
  <a class="qh-mode" href="bigquery.html">
    <div class="qh-mode__name">BigQuery</div>
    <div class="qh-mode__desc">MERGE-based upsert. Requires a dataset_id; insert_all or Storage Write transport.</div>
  </a>
</div>
```

## Destination DDL

The DDL knobs (`engine`, `partition_by`, `order_by`, `primary_key`, `key`) are
interpreted per destination:

- **ClickHouse** — they shape the `MergeTree`-family DDL.
- **BigQuery** — `engine` is ignored; `partition_by` must be a bare
  `DATE`/`DATETIME`/`TIMESTAMP` column name (not a SQL expression);
  `order_by`/`key` become clustering columns (at most 4 total).

quickhouse creates the table for you (`create_if_missing=True` by default) with
a schema derived from the source. See [Type mapping](../type-mapping.md) for how
source types become destination types.

```{toctree}
:hidden:

clickhouse
bigquery
```
