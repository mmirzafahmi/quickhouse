"""End-to-end integration tests: ClickHouse -> ClickHouse.

The ClickHouse descriptor is dual-role, so these read from one table and write
to another on the same server — enough to exercise the whole source path
(`DESCRIBE` schema resolution, the server-side casts that pin the Arrow output
types, range partitioning, incremental watermarks) without a second cluster.

The type-preservation test is the one that matters most: reading Arrow back out
of ClickHouse is only safe because the SELECT casts every column to a type whose
Arrow output is fixed, and a regression there shows up as a wrong destination
type or a decode error, not as missing rows.

Run against the services in ``docker-compose.yml`` after building the module:

    docker compose up -d
    pip install -e '.[test]'
    maturin develop --release
    pytest -v tests/test_clickhouse_source.py
"""

from __future__ import annotations

import pytest

import quickhouse


def _drop(ch_client, *tables: str):
    for t in tables:
        ch_client.command(f"DROP TABLE IF EXISTS `{t}`")
        ch_client.command(f"DROP TABLE IF EXISTS `{t}_quickhouse_tmp`")


def _seed_source(ch_client, table: str, rows: int, base_ts: str = "2024-01-01 00:00:00"):
    """A source table with a mixed, deliberately ClickHouse-flavoured schema."""
    ch_client.command(f"DROP TABLE IF EXISTS `{table}`")
    ch_client.command(
        f"""
        CREATE TABLE `{table}` (
            id          Int64,
            name        LowCardinality(String),
            amount      Float64,
            qty         Nullable(Int32),
            is_active   Bool,
            note        Nullable(String),
            updated_at  DateTime
        ) ENGINE = MergeTree ORDER BY id
        """
    )
    ch_client.command(
        f"INSERT INTO `{table}` SELECT "
        f"number + 1 AS id, concat('row-', toString(number % 7)) AS name, "
        f"(number + 1) * 1.5 AS amount, "
        f"if(number % 3 = 0, NULL, toInt32(number)) AS qty, "
        f"number % 2 = 0 AS is_active, NULL AS note, "
        f"toDateTime('{base_ts}') AS updated_at "
        f"FROM numbers({rows})"
    )


def test_full_refresh_reconciles(ch_client, ch_source, ch_target, unique_name):
    src_table, dest_table = unique_name, f"{unique_name}_dst"
    n = 5000
    _seed_source(ch_client, src_table, n)
    _drop(ch_client, dest_table)
    try:
        result = quickhouse.sync(
            ch_source,
            ch_target,
            dest_table=dest_table,
            source_table=src_table,
            mode="full",
            key=["id"],
            create_if_missing=True,
            # Forces range partitioning over `id`, so the partition predicates
            # and their union are exercised rather than a single stream.
            parallelism=4,
            batch_rows=1000,
        )
        assert result.rows_written == n
        assert int(ch_client.command(f"SELECT count() FROM `{dest_table}`")) == n

        # Column-level reconciliation, including the nullable columns.
        src_sum = float(ch_client.command(f"SELECT sum(amount) FROM `{src_table}`"))
        dst_sum = float(ch_client.command(f"SELECT sum(amount) FROM `{dest_table}`"))
        assert abs(src_sum - dst_sum) < 1e-6
        assert int(ch_client.command(f"SELECT countIf(note IS NULL) FROM `{dest_table}`")) == n
        src_null_qty = int(ch_client.command(f"SELECT countIf(qty IS NULL) FROM `{src_table}`"))
        dst_null_qty = int(ch_client.command(f"SELECT countIf(qty IS NULL) FROM `{dest_table}`"))
        assert src_null_qty == dst_null_qty > 0

        # No partition is read twice and none is skipped: every id lands once.
        assert int(ch_client.command(f"SELECT uniqExact(id) FROM `{dest_table}`")) == n
    finally:
        _drop(ch_client, src_table, dest_table)


def test_types_survive_the_round_trip(ch_client, ch_source, ch_target, unique_name):
    """The types only ClickHouse has must come back as themselves.

    This is the regression guard for the whole cast-on-the-server design: every
    column below is one whose Arrow output type is *not* the obvious one, so a
    missing or wrong cast surfaces here as a changed destination type (or a
    decode failure) rather than as lost rows.
    """
    src_table, dest_table = unique_name, f"{unique_name}_dst"
    ch_client.command(f"DROP TABLE IF EXISTS `{src_table}`")
    ch_client.command(
        f"""
        CREATE TABLE `{src_table}` (
            id      Int64,
            uid     UUID,
            ip      IPv4,
            kind    Enum8('a' = 1, 'b' = 2),
            code    FixedString(4),
            tag     LowCardinality(Nullable(String)),
            price   Decimal(18, 4),
            d       Date,
            d32     Date32,
            ts      DateTime,
            ts64    DateTime64(3, 'UTC')
        ) ENGINE = MergeTree ORDER BY id
        """
    )
    ch_client.command(
        f"INSERT INTO `{src_table}` VALUES "
        f"(1, '5f2b1b2e-0000-4000-8000-000000000001', '10.0.0.7', 'b', 'ABCD', 'x', "
        f"123.4567, '2024-02-03', '2024-02-03', '2024-02-03 04:05:06', "
        f"'2024-02-03 04:05:06.123')"
    )
    _drop(ch_client, dest_table)
    try:
        quickhouse.sync(
            ch_source,
            ch_target,
            dest_table=dest_table,
            source_table=src_table,
            mode="full",
            key=["id"],
            create_if_missing=True,
        )
        types = dict(
            ch_client.query(
                "SELECT name, type FROM system.columns "
                f"WHERE database = currentDatabase() AND table = '{dest_table}'"
            ).result_rows
        )
        assert types["uid"] == "UUID"
        assert types["ip"] == "IPv4"
        assert types["kind"].startswith("Enum8(")
        assert types["code"] == "FixedString(4)"
        # Nullable is lifted out of the LowCardinality wrapper on the way in and
        # re-applied on the way out, so the dictionary encoding is preserved.
        assert types["tag"] == "LowCardinality(Nullable(String))"
        assert types["price"] == "Decimal(18, 4)"
        assert types["d"] == "Date"
        assert types["d32"] == "Date32"
        assert types["ts"] == "DateTime"
        assert types["ts64"] == "DateTime64(3, 'UTC')"
        # And NOT NULL stays NOT NULL. Every other source's decoder can turn a
        # date or an over-precision decimal into a NULL, so the planner widens
        # those columns defensively; this one can't, so it doesn't.
        assert not any(t.startswith("Nullable(") for name, t in types.items() if name != "tag")

        # And the values themselves, not just the declared types. The decimal is
        # the interesting one: it must be exact, not a Float64 round-trip.
        row = ch_client.query(
            f"SELECT toString(uid), toString(ip), toString(kind), toString(code), tag, "
            f"toString(price), toString(d), toString(d32), toString(ts), toString(ts64) "
            f"FROM `{dest_table}`"
        ).result_rows[0]
        assert row[0] == "5f2b1b2e-0000-4000-8000-000000000001"
        assert row[1] == "10.0.0.7"
        assert row[2] == "b"
        assert row[3] == "ABCD"
        assert row[4] == "x"
        assert row[5] == "123.4567"
        assert row[6] == "2024-02-03"
        assert row[7] == "2024-02-03"
        assert row[8].startswith("2024-02-03 04:05:06")
        assert row[9].startswith("2024-02-03 04:05:06.123")
    finally:
        _drop(ch_client, src_table, dest_table)


def test_incremental_is_idempotent_and_picks_up_new_rows(
    ch_client, ch_source, ch_target, unique_name
):
    src_table, dest_table = unique_name, f"{unique_name}_dst"
    _seed_source(ch_client, src_table, 100, base_ts="2024-01-01 00:00:00")
    _drop(ch_client, dest_table)
    try:
        kwargs = dict(
            dest_table=dest_table,
            source_table=src_table,
            mode="incremental",
            watermark="updated_at",
            key=["id"],
            create_if_missing=True,
        )
        first = quickhouse.sync(ch_source, ch_target, **kwargs)
        assert first.rows_written == 100

        # Re-running with nothing new must be a no-op, not a re-copy.
        second = quickhouse.sync(ch_source, ch_target, **kwargs)
        assert second.rows_written == 0
        assert int(ch_client.command(f"SELECT count() FROM `{dest_table}`")) == 100

        ch_client.command(
            f"INSERT INTO `{src_table}` SELECT number + 1000 AS id, 'new' AS name, "
            f"1.0 AS amount, NULL AS qty, true AS is_active, NULL AS note, "
            f"toDateTime('2024-06-01 00:00:00') AS updated_at FROM numbers(10)"
        )
        third = quickhouse.sync(ch_source, ch_target, **kwargs)
        assert third.rows_written == 10
        ch_client.command(f"OPTIMIZE TABLE `{dest_table}` FINAL")
        assert int(ch_client.command(f"SELECT count() FROM `{dest_table}`")) == 110
    finally:
        _drop(ch_client, src_table, dest_table)


def test_empty_source_does_not_persist_a_1970_watermark(
    ch_client, ch_source, ch_target, unique_name
):
    """ClickHouse's max() over zero rows returns the epoch, not NULL.

    Taken at face value that would commit a watermark the source never actually
    reached, so the source counts rows alongside the aggregate. The observable
    consequence is that a later row older than "now" is still picked up.
    """
    src_table, dest_table = unique_name, f"{unique_name}_dst"
    _seed_source(ch_client, src_table, 0)
    _drop(ch_client, dest_table)
    try:
        kwargs = dict(
            dest_table=dest_table,
            source_table=src_table,
            mode="incremental",
            watermark="updated_at",
            key=["id"],
            create_if_missing=True,
        )
        first = quickhouse.sync(ch_source, ch_target, **kwargs)
        assert first.rows_written == 0
        assert first.new_watermark is None

        ch_client.command(
            f"INSERT INTO `{src_table}` SELECT number + 1 AS id, 'a' AS name, 1.0 AS amount, "
            f"NULL AS qty, true AS is_active, NULL AS note, "
            f"toDateTime('2020-01-01 00:00:00') AS updated_at FROM numbers(5)"
        )
        second = quickhouse.sync(ch_source, ch_target, **kwargs)
        assert second.rows_written == 5
    finally:
        _drop(ch_client, src_table, dest_table)


def test_source_query_and_column_transform(ch_client, ch_source, ch_target, unique_name):
    src_table, dest_table = unique_name, f"{unique_name}_dst"
    _seed_source(ch_client, src_table, 50)
    _drop(ch_client, dest_table)
    try:
        quickhouse.sync(
            ch_source,
            ch_target,
            dest_table=dest_table,
            source_query=f"SELECT id, amount FROM `{src_table}` WHERE id <= 10",
            mode="full",
            key=["id"],
            create_if_missing=True,
        )
        assert int(ch_client.command(f"SELECT count() FROM `{dest_table}`")) == 10

        # A value transform is wrapped by the type-pinning cast, not replaced
        # by it: the rounded value must be what lands.
        _drop(ch_client, dest_table)
        quickhouse.sync(
            ch_source,
            ch_target,
            dest_table=dest_table,
            source_table=src_table,
            mode="full",
            key=["id"],
            create_if_missing=True,
            column_transforms={"amount": "round(amount)"},
        )
        assert int(ch_client.command(f"SELECT countIf(amount != round(amount)) FROM `{dest_table}`")) == 0
    finally:
        _drop(ch_client, src_table, dest_table)


def test_chunked_keyset_read_transfers_every_row_once(
    ch_client, ch_source, ch_target, unique_name
):
    """`chunk_rows` walks the key in bounded `WHERE id > cursor LIMIT n` steps.

    Each chunk is made durable and its cursor persisted before the next one
    starts, so the failure this guards against is an off-by-one in the cursor —
    a skipped or doubled row at every chunk boundary, which only shows up when
    the row count is not a multiple of the chunk size.
    """
    src_table, dest_table = unique_name, f"{unique_name}_dst"
    n, chunk = 250, 40  # deliberately not a multiple
    _seed_source(ch_client, src_table, n)
    _drop(ch_client, dest_table)
    try:
        result = quickhouse.sync(
            ch_source,
            ch_target,
            dest_table=dest_table,
            source_table=src_table,
            mode="incremental",
            watermark="updated_at",
            key=["id"],
            create_if_missing=True,
            chunk_rows=chunk,
        )
        assert result.rows_written == n
        ch_client.command(f"OPTIMIZE TABLE `{dest_table}` FINAL")
        assert int(ch_client.command(f"SELECT count() FROM `{dest_table}`")) == n
        assert int(ch_client.command(f"SELECT uniqExact(id) FROM `{dest_table}`")) == n
        src_ids = ch_client.command(f"SELECT sum(id) FROM `{src_table}`")
        dst_ids = ch_client.command(f"SELECT sum(id) FROM `{dest_table}`")
        assert int(src_ids) == int(dst_ids)
    finally:
        _drop(ch_client, src_table, dest_table)


def test_lookback_seconds_re_reads_across_the_watermark_boundary(
    ch_client, ch_source, ch_target, unique_name
):
    """`lookback_seconds` widens the lower bound with ClickHouse's own arithmetic.

    The interesting part is that the bound is built as
    `toDateTime64('<cursor>', 6, 'UTC') - INTERVAL n SECOND` — a same-engine
    round trip of the watermark string this source itself produced. A row
    restated at exactly the committed watermark is invisible without it.
    """
    src_table, dest_table = unique_name, f"{unique_name}_dst"
    _seed_source(ch_client, src_table, 10, base_ts="2024-03-01 12:00:00")
    _drop(ch_client, dest_table)
    try:
        kwargs = dict(
            dest_table=dest_table,
            source_table=src_table,
            mode="incremental",
            watermark="updated_at",
            key=["id"],
            create_if_missing=True,
        )
        first = quickhouse.sync(ch_source, ch_target, **kwargs)
        assert first.rows_written == 10

        # Restate a row *at* the committed watermark. A strict `>` never sees it.
        ch_client.command(
            f"ALTER TABLE `{src_table}` UPDATE amount = 999.0 WHERE id = 1 SETTINGS "
            f"mutations_sync = 2"
        )
        assert quickhouse.sync(ch_source, ch_target, **kwargs).rows_written == 0

        # With a lookback window the same run picks it up and the destination
        # converges (ReplacingMergeTree collapses the two versions).
        widened = quickhouse.sync(ch_source, ch_target, lookback_seconds=3600, **kwargs)
        assert widened.rows_written == 10
        ch_client.command(f"OPTIMIZE TABLE `{dest_table}` FINAL")
        assert int(ch_client.command(f"SELECT count() FROM `{dest_table}`")) == 10
        assert (
            float(ch_client.command(f"SELECT amount FROM `{dest_table}` WHERE id = 1")) == 999.0
        )
    finally:
        _drop(ch_client, src_table, dest_table)


def test_unsupported_type_names_the_column(ch_client, ch_source, ch_target, unique_name):
    src_table, dest_table = unique_name, f"{unique_name}_dst"
    ch_client.command(f"DROP TABLE IF EXISTS `{src_table}`")
    ch_client.command(
        f"CREATE TABLE `{src_table}` (id Int64, tags Array(String)) "
        f"ENGINE = MergeTree ORDER BY id"
    )
    ch_client.command(f"INSERT INTO `{src_table}` VALUES (1, ['a', 'b'])")
    _drop(ch_client, dest_table)
    try:
        with pytest.raises(RuntimeError) as excinfo:
            quickhouse.sync(
                ch_source,
                ch_target,
                dest_table=dest_table,
                source_table=src_table,
                mode="full",
                key=["id"],
                create_if_missing=True,
            )
        msg = str(excinfo.value)
        assert "tags" in msg and "Array(String)" in msg

        # ...and excluding it makes the same transfer work.
        quickhouse.sync(
            ch_source,
            ch_target,
            dest_table=dest_table,
            source_table=src_table,
            mode="full",
            key=["id"],
            exclude=["tags"],
            create_if_missing=True,
        )
        assert int(ch_client.command(f"SELECT count() FROM `{dest_table}`")) == 1
    finally:
        _drop(ch_client, src_table, dest_table)


def test_reconcile_keys_measures_drift_from_a_clickhouse_source(
    ch_client, ch_source, ch_target, unique_name
):
    src_table, dest_table = unique_name, f"{unique_name}_dst"
    _seed_source(ch_client, src_table, 20)
    _drop(ch_client, dest_table)
    try:
        quickhouse.sync(
            ch_source,
            ch_target,
            dest_table=dest_table,
            source_table=src_table,
            mode="full",
            key=["id"],
            create_if_missing=True,
        )
        # A hard delete at the source is exactly what an incremental sync can't
        # see; reconcile_keys is what finds it.
        ch_client.command(f"ALTER TABLE `{src_table}` DELETE WHERE id <= 3")
        ch_client.command(f"OPTIMIZE TABLE `{src_table}` FINAL")

        result = quickhouse.reconcile_keys(
            ch_source,
            ch_target,
            dest_table=dest_table,
            key="id",
            source_table=src_table,
        )
        assert result.orphan_keys == 3
        assert result.missing_keys == 0
    finally:
        _drop(ch_client, src_table, dest_table)
