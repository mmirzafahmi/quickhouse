# BigQuery

```{note}
`dataset_id` is **required** — it's BigQuery's equivalent of ClickHouse's
`database`.
```

`write_method` selects how rows are written: `"storage_write"` (default since
0.14.0; the gRPC Storage Write API — free up to 2 TiB/month, then $0.025/GB) or
the legacy `"insert_all"` (`tabledata.insertAll`, billed $0.01 per 200 MiB —
roughly double — and slower). Both share the same atomic-swap / MERGE flow; only
the row-insert transport differs. See
[BigQuery authentication](../sources/index.md#authentication) — the same credentials
work in either role.

```python
qh.sync(
    qh.BigQuery("my-project"),                                    # source
    qh.BigQuery("my-project", dataset_id="analytics"),           # destination
    dest_table="orders", source_table="raw.orders", mode="full",
)
```

The DDL knobs a BigQuery destination accepts are on the
[Destinations index](index.md#destination-ddl).
