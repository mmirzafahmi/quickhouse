"""Copy a ClickHouse table into another ClickHouse database or cluster.

The same `ClickHouse` class is both a source and a target, so a cross-cluster
copy — or promoting a table from a raw database into an analytics one — is an
ordinary `sync()`. Reads use ClickHouse's own `FORMAT ArrowStream` over the HTTP
interface, so rows never leave the Arrow representation between the two servers.

Types survive the round trip: `UUID`, `IPv4`/`IPv6`, `Enum8`/`Enum16`,
`FixedString` and `LowCardinality(...)` are all recreated at the destination
rather than flattened to `String`.

Run it twice — the second run is incremental and copies only what changed.

Prerequisites
-------------
- `pip install quickhouse`
- Two reachable ClickHouse servers, or one server and two databases (the
  defaults point at the `docker-compose.yml` service for both, with different
  databases, so the example works on a single local node).

Environment variables:
    CH_SRC_URL, CH_SRC_DB      source server + database
    CH_DST_URL, CH_DST_DB      destination server + database
    CH_USER, CH_PASSWORD       credentials used for both (default/empty)
    SRC_TABLE, DEST_TABLE, WATERMARK, KEY
"""

import os

import quickhouse as qh


def main() -> None:
    user = os.getenv("CH_USER", "default")
    password = os.getenv("CH_PASSWORD", "")

    src = qh.ClickHouse(
        os.getenv("CH_SRC_URL", "http://localhost:8123"),
        database=os.getenv("CH_SRC_DB", "raw"),
        user=user,
        password=password,
    )
    dst = qh.ClickHouse(
        os.getenv("CH_DST_URL", "http://localhost:8123"),
        database=os.getenv("CH_DST_DB", "analytics"),
        user=user,
        password=password,
    )
    src_table = os.getenv("SRC_TABLE", "orders")

    result = qh.sync(
        src,
        dst,
        dest_table=os.getenv("DEST_TABLE", src_table),
        source_table=src_table,
        mode="incremental",
        watermark=os.getenv("WATERMARK", "updated_at"),
        key=[os.getenv("KEY", "id")],
        create_if_missing=True,
        # The source table is split into key ranges and read in parallel, the
        # same as a Postgres or MySQL source.
        parallelism=4,
    )
    print(result)


if __name__ == "__main__":
    main()
