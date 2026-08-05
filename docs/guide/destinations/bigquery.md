# BigQuery

```{note}
`dataset_id` is **required** — it's BigQuery's equivalent of ClickHouse's
`database`.
```

`write_method` selects how rows are written: `"insert_all"`
(default; `tabledata.insertAll`, proven) or `"storage_write"` (the gRPC Storage
Write API — free and higher-throughput). Both share the same atomic-swap /
MERGE flow; only the row-insert transport differs. See
[BigQuery authentication](../sources/index.md#authentication) — the same credentials
work in either role.

```python
qh.sync(
    qh.BigQuery("my-project"),                                    # source
    qh.BigQuery("my-project", dataset_id="analytics"),           # destination
    dest_table="orders", source_table="raw.orders",
)
```

The DDL knobs a BigQuery destination accepts are on the
[Destinations index](index.md#destination-ddl).
