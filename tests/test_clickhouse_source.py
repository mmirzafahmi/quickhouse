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

import time

import pytest

import quickhouse
from conftest import CH_DB, CH_PASSWORD, CH_URL, CH_USER


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


def test_resume_marker_without_an_upper_bound_resumes_and_is_cleared(
    ch_client, ch_source, ch_target, unique_name
):
    """A chunk-resume marker saved with no frozen upper bound must not pin one.

    A chunked run that had no upper bound (the MAX probe was skipped, or MAX was
    NULL) saves `chunk_upper = ''`. That used to come back as a real bound, so
    every later run filtered on `updated_at <= ''` and failed or read nothing,
    and nothing ever cleared the marker. The marker is injected here exactly as
    such an interrupted run leaves it.
    """
    src_table, dest_table = unique_name, f"{unique_name}_dst"
    n = 250
    _seed_source(ch_client, src_table, n, base_ts="2024-03-01 12:00:00")
    _drop(ch_client, dest_table)
    kwargs = dict(
        dest_table=dest_table,
        source_table=src_table,
        mode="incremental",
        watermark="updated_at",
        key=["id"],
        create_if_missing=True,
        chunk_rows=40,
    )
    try:
        assert quickhouse.sync(ch_source, ch_target, **kwargs).rows_written == n
        ch_client.command(
            "INSERT INTO _quickhouse_state "
            "(source_table, dest_table, last_watermark, rows, chunk_cursor, chunk_upper) "
            f"VALUES ('{src_table}', '{dest_table}', '2024-03-01 12:00:00', 0, '100', '')"
        )

        # Resumes past id 100 up to the live MAX: nothing new, and no error.
        assert quickhouse.sync(ch_source, ch_target, **kwargs).rows_written == 0
        marker = ch_client.command(
            f"SELECT chunk_cursor FROM _quickhouse_state FINAL "
            f"WHERE source_table = '{src_table}' AND dest_table = '{dest_table}' "
            f"ORDER BY run_ts DESC LIMIT 1"
        )
        assert marker == "", "a finished run must clear the resume marker"

        # With the marker gone, a change to a key below the old cursor is read.
        ch_client.command(
            f"ALTER TABLE `{src_table}` UPDATE updated_at = toDateTime('2024-03-02 00:00:00'), "
            f"amount = 999.0 WHERE id = 5 SETTINGS mutations_sync = 2"
        )
        assert quickhouse.sync(ch_source, ch_target, **kwargs).rows_written == 1
    finally:
        _drop(ch_client, src_table, dest_table)


def test_lookback_seconds_re_reads_across_the_watermark_boundary(
    ch_client, ch_source, ch_target, unique_name
):
    """`lookback_seconds` widens the lower bound with ClickHouse's own arithmetic.

    The interesting part is that the bound is built as
    `CAST('<cursor>' AS <watermark type>) - INTERVAL n SECOND` — a same-engine
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


def test_lookback_on_a_zoned_datetime_reads_the_cursor_in_that_zone(
    ch_client, ch_source, ch_target, unique_name
):
    """A `DateTime('Asia/Jakarta')` watermark with a lookback must not skip rows.

    `toString(max(col))` renders the cursor as Jakarta wall-clock text. Reading
    it back as UTC put the lower bound 7 hours after the real cursor, so with a
    lookback shorter than the offset the window was empty: every run after the
    first read nothing while the cursor still advanced.
    """
    src_table, dest_table = unique_name, f"{unique_name}_dst"
    ch_client.command(f"DROP TABLE IF EXISTS `{src_table}`")
    ch_client.command(
        f"CREATE TABLE `{src_table}` (id Int64, updated_at DateTime('Asia/Jakarta')) "
        f"ENGINE = MergeTree ORDER BY id"
    )
    ch_client.command(
        f"INSERT INTO `{src_table}` SELECT number + 1, "
        f"toDateTime('2024-03-01 12:00:00', 'UTC') + number * 60 FROM numbers(10)"
    )
    _drop(ch_client, dest_table)
    try:
        kwargs = dict(
            dest_table=dest_table,
            source_table=src_table,
            mode="incremental",
            watermark="updated_at",
            key=["id"],
            create_if_missing=True,
            lookback_seconds=3600,
        )
        assert quickhouse.sync(ch_source, ch_target, **kwargs).rows_written == 10

        # Five new rows two hours after the cursor (12:09 UTC). The one-hour
        # lookback also re-reads the first ten, which all sit within it.
        ch_client.command(
            f"INSERT INTO `{src_table}` SELECT number + 11, "
            f"toDateTime('2024-03-01 14:00:00', 'UTC') + number * 60 FROM numbers(5)"
        )
        assert quickhouse.sync(ch_source, ch_target, **kwargs).rows_written == 15
        ch_client.command(f"OPTIMIZE TABLE `{dest_table}` FINAL")
        assert int(ch_client.command(f"SELECT count() FROM `{dest_table}`")) == 15
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


def test_read_idle_timeout_secs_times_the_wait_for_the_first_block(
    ch_client, ch_source, ch_target, unique_name
):
    """Issue #41, from ClickHouse: the server sends the response headers with
    the first block, so the request waits as long as the query takes to
    produce it, and that wait wasn't timed. It trips the timer now, and a
    read that outlasts it counts it in read_secs."""
    src, dst = f"{unique_name}_src", unique_name
    _seed_source(ch_client, src, 100)
    _drop(ch_client, dst)
    # sleep() takes at most 3 seconds per block.
    stalled = "SELECT o.* FROM `{}` o CROSS JOIN (SELECT sleep({}) AS z) s"
    kw = dict(dest_table=dst, mode="full", key=["id"], create_if_missing=True)
    try:
        started = time.monotonic()
        with pytest.raises(RuntimeError, match="read_idle_timeout_secs"):
            quickhouse.sync(
                ch_source, ch_target, source_query=stalled.format(src, 3),
                read_idle_timeout_secs=1, **kw,
            )
        assert time.monotonic() - started < 3, "it waited out the stall"

        r = quickhouse.sync(
            ch_source, ch_target, source_query=stalled.format(src, 2),
            read_idle_timeout_secs=10, **kw,
        )
        assert r.rows_written == 100
        assert r.read_secs >= 1.5, r.read_secs
    finally:
        _drop(ch_client, src, dst)


def test_a_null_key_is_left_out_with_a_null_key_warning(
    ch_client, ch_source, ch_target, unique_name
):
    """Issue #49: a Nullable source column used as the key failed the write
    with an Arrow error on its first NULL. The row is left out now, and
    null_key counts it."""
    src, dst = f"{unique_name}_src", unique_name
    _drop(ch_client, src, dst)
    ch_client.command(
        f"CREATE TABLE `{src}` (id Int64, code Nullable(String)) ENGINE = MergeTree ORDER BY id"
    )
    ch_client.command(f"INSERT INTO `{src}` VALUES (1, 'a'), (2, NULL), (3, 'c')")
    try:
        r = quickhouse.sync(
            ch_source, ch_target, source_table=src, dest_table=dst, mode="full", key=["code"],
            create_if_missing=True,
        )
        assert (r.rows_read, r.rows_written) == (3, 2)
        assert [(w.column, w.count) for w in r.warnings if w.kind == "null_key"] == [("code", 1)]
        rows = ch_client.query(f"SELECT id, code FROM `{dst}` ORDER BY id").result_rows
        assert rows == [(1, "a"), (3, "c")]
    finally:
        _drop(ch_client, src, dst)


def test_a_nullable_watermark_reads_into_a_replacing_merge_tree(
    ch_client, ch_source, ch_target, unique_name
):
    """Issue #49: the read casts each column to its destination type, and a
    ReplacingMergeTree's version column (the watermark) can't be NULL, so it
    cast to the non-Nullable type. ClickHouse evaluates that cast on rows the
    `IS NOT NULL` filter goes on to drop, and the read failed with
    `Code: 349 CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN`. The cast keeps NULL
    now: the filter leaves those rows out, and null_watermark counts them."""
    src, dst = f"{unique_name}_src", unique_name
    _drop(ch_client, src, dst)
    ch_client.command(
        f"CREATE TABLE `{src}` (id Int64, v Int64, updated_at Nullable(DateTime)) "
        "ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64"
    )
    # NULL on every 97th row: 21 of 2,000.
    ch_client.command(
        f"INSERT INTO `{src}` SELECT number + 1, number, "
        "if(number % 97 = 0, NULL, toDateTime('2024-01-01 00:00:00') + number) "
        "FROM numbers(2000)"
    )
    try:
        r = quickhouse.sync(
            ch_source, ch_target, source_table=src, dest_table=dst, mode="incremental",
            watermark="updated_at", key=["id"], create_if_missing=True, state_key=dst,
        )
        assert (r.rows_read, r.rows_written) == (1979, 1979)
        null = [(w.column, w.count) for w in r.warnings if w.kind == "null_watermark"]
        assert null == [("updated_at", 21)], r.warnings
        assert ch_client.command(f"SELECT count() FROM `{dst}`") == 1979
    finally:
        _drop(ch_client, src, dst)


# --- #39: a source table whose engine merges rows is read with FINAL --------


def _ch_source(**kw):
    """The ``ch_source`` fixture's descriptor, with extra keywords."""
    return quickhouse.ClickHouse(
        CH_URL, database=CH_DB, user=CH_USER, password=CH_PASSWORD, **kw
    )


def _seed_versions(ch_client, table, engine="ReplacingMergeTree(ver)", order_by="id"):
    """Ten keys, two of them (3 and 7) with a newer version that no merge has
    removed: ``count()`` is 12 and, on a merging engine, ``count() FINAL`` is 10.

    Merges are stopped on this table only, so each insert stays its own part.
    The newer versions also move ``grp``, a column outside the sorting key;
    ``region`` is the same in every version of a key, so it can lead one.
    """
    ch_client.command(f"DROP TABLE IF EXISTS `{table}`")
    ch_client.command(
        f"""
        CREATE TABLE `{table}` (
            id          Int64,
            grp         Int32,
            region      String,
            name        String,
            updated_at  DateTime,
            ver         UInt32
        ) ENGINE = {engine} ORDER BY {order_by}
        """
    )
    ch_client.command(f"SYSTEM STOP MERGES `{table}`")
    ch_client.command(
        f"INSERT INTO `{table}` SELECT number + 1, toInt32(number % 3), "
        f"concat('r', toString((number + 1) % 2)), 'v1', "
        f"toDateTime('2024-01-01 00:00:00'), 1 FROM numbers(10)"
    )
    ch_client.command(
        f"INSERT INTO `{table}` VALUES "
        f"(3, 1, 'r1', 'v2', '2024-02-01 00:00:00', 2), "
        f"(7, 2, 'r1', 'v2', '2024-02-01 00:00:00', 2)"
    )
    assert int(ch_client.command(f"SELECT count() FROM `{table}`")) == 12


def _full(src, dst, **kw):
    return dict(
        source_table=src, dest_table=dst, mode="full", key=["id"],
        create_if_missing=True, **kw,
    )


def test_replacing_source_full_copy_reads_only_the_newest_versions(
    ch_client, ch_source, ch_target, unique_name, capfd
):
    """Issue #39: 12 rows were written for 10 keys, into a MergeTree where they
    never collapse."""
    src, dst = unique_name, f"{unique_name}_dst"
    _seed_versions(ch_client, src)
    _drop(ch_client, dst)
    try:
        capfd.readouterr()
        r = quickhouse.sync(ch_source, ch_target, **_full(src, dst, parallelism=1))
        err = capfd.readouterr().err
        assert r.rows_written == 10
        assert int(ch_client.command(f"SELECT count() FROM `{dst}`")) == 10
        rows = ch_client.query(
            f"SELECT id, name FROM `{dst}` WHERE id IN (3, 7) ORDER BY id"
        ).result_rows
        assert rows == [(3, "v2"), (7, "v2")]
        assert f"source table '{src}' is a ReplacingMergeTree" in err
        assert "reading it with FINAL" in err
    finally:
        _drop(ch_client, src, dst)


def test_replacing_source_incremental_reads_final_and_takes_its_cursor_from_it(
    ch_client, ch_source, ch_target, unique_name
):
    """The read and its MAX(watermark) probe both see the merged rows.

    Key 5 also has a stale version (a lower ``ver``) with a later
    ``updated_at``: the stored MAX is 2024-05-01, the merged one 2024-02-01. A
    cursor taken from the stored MAX would skip key 11, written later at
    2024-03-01."""
    src, dst = unique_name, f"{unique_name}_dst"
    _seed_versions(ch_client, src)
    ch_client.command(
        f"INSERT INTO `{src}` VALUES (5, 1, 'r1', 'stale', '2024-05-01 00:00:00', 0)"
    )
    _drop(ch_client, dst)
    kw = dict(
        source_table=src, dest_table=dst, mode="incremental", watermark="updated_at",
        key=["id"], create_if_missing=True,
    )
    try:
        first = quickhouse.sync(ch_source, ch_target, **kw)
        assert first.rows_written == 10
        assert first.new_watermark.startswith("2024-02-01"), first.new_watermark

        ch_client.command(
            f"INSERT INTO `{src}` VALUES (11, 0, 'r1', 'v1', '2024-03-01 00:00:00', 1)"
        )
        second = quickhouse.sync(ch_source, ch_target, **kw)
        assert second.rows_written == 1

        rows = dict(
            ch_client.query(f"SELECT id, name FROM `{dst}` FINAL ORDER BY id").result_rows
        )
        assert sorted(rows) == list(range(1, 12))
        assert rows[3] == rows[7] == "v2"
        assert rows[5] == "v1"
    finally:
        _drop(ch_client, src, dst)


def test_replacing_source_parallel_range_read_reads_final(
    ch_client, ch_source, ch_target, unique_name, capfd
):
    """Ranges on a sorting-key column hold every version of a key in one
    range, so the read stays parallel: four ranges over id 1..10."""
    src, dst = unique_name, f"{unique_name}_dst"
    _seed_versions(ch_client, src)
    _drop(ch_client, dst)
    try:
        capfd.readouterr()
        r = quickhouse.sync(ch_source, ch_target, **_full(src, dst, parallelism=4))
        err = capfd.readouterr().err
        assert "computed 4 partition(s)" in err
        assert r.rows_written == 10
        assert int(ch_client.command(f"SELECT count() FROM `{dst}`")) == 10
        assert int(ch_client.command(f"SELECT uniqExact(id) FROM `{dst}`")) == 10
    finally:
        _drop(ch_client, src, dst)


def test_final_range_read_moves_off_a_column_outside_the_sorting_key(
    ch_client, ch_source, ch_target, unique_name, capfd
):
    """A range over a column outside the sorting key would merge the whole
    table once per range. The read splits on the sorting key's first column
    instead, or runs in one stream when that one isn't an integer."""
    src, src2, dst = unique_name, f"{unique_name}_b", f"{unique_name}_dst"
    _seed_versions(ch_client, src)
    _seed_versions(ch_client, src2, order_by="(region, id)")
    _drop(ch_client, dst)
    try:
        capfd.readouterr()
        r = quickhouse.sync(
            ch_source, ch_target, **_full(src, dst, parallelism=4, partition_column="grp")
        )
        err = capfd.readouterr().err
        assert "splits on 'id' instead of 'grp'" in err
        assert "computed 4 partition(s)" in err
        assert r.rows_written == 10
        assert int(ch_client.command(f"SELECT count() FROM `{dst}`")) == 10

        _drop(ch_client, dst)
        r = quickhouse.sync(
            ch_source, ch_target, **_full(src2, dst, parallelism=4, partition_column="grp")
        )
        err = capfd.readouterr().err
        assert f"'{src2}' is read in one stream" in err
        assert "computed 1 partition(s)" in err
        assert r.rows_written == 10
        assert int(ch_client.command(f"SELECT count() FROM `{dst}`")) == 10
    finally:
        _drop(ch_client, src, src2, dst)


def test_final_false_reads_a_replacing_source_as_stored(
    ch_client, ch_target, unique_name, capfd
):
    """final=False keeps the behaviour before #39: every stored row is read."""
    src, dst = unique_name, f"{unique_name}_dst"
    _seed_versions(ch_client, src)
    _drop(ch_client, dst)
    try:
        capfd.readouterr()
        r = quickhouse.sync(
            _ch_source(final=False), ch_target, **_full(src, dst, parallelism=4)
        )
        err = capfd.readouterr().err
        assert r.rows_written == 12
        assert int(ch_client.command(f"SELECT count() FROM `{dst}`")) == 12
        assert "with FINAL" not in err
    finally:
        _drop(ch_client, src, dst)


def test_merge_tree_source_is_read_without_final(
    ch_client, ch_source, ch_target, unique_name, capfd
):
    """A plain MergeTree keeps every row, and ClickHouse refuses FINAL on one
    (ILLEGAL_FINAL), so the default reads it as stored, and final=True fails
    before anything is written, naming the option."""
    src, dst = unique_name, f"{unique_name}_dst"
    _seed_versions(ch_client, src, engine="MergeTree")
    _drop(ch_client, dst)
    try:
        capfd.readouterr()
        r = quickhouse.sync(ch_source, ch_target, **_full(src, dst, parallelism=4))
        err = capfd.readouterr().err
        assert r.rows_written == 12
        assert "with FINAL" not in err

        _drop(ch_client, dst)
        with pytest.raises(RuntimeError, match=r"final=True.*ILLEGAL_FINAL"):
            quickhouse.sync(_ch_source(final=True), ch_target, **_full(src, dst))
        assert not int(ch_client.command(f"EXISTS TABLE `{dst}`"))
    finally:
        _drop(ch_client, src, dst)


def test_reconcile_keys_reads_a_merging_source_with_final(
    ch_client, ch_source, ch_target, unique_name
):
    """A ReplacingMergeTree(ver, is_deleted) deletes a key by writing a newer
    version with is_deleted = 1, which only FINAL drops. Read as stored, the
    source still held the key, so reconcile_keys never reported its orphan."""
    src, dst = unique_name, f"{unique_name}_dst"
    ch_client.command(f"DROP TABLE IF EXISTS `{src}`")
    ch_client.command(
        f"CREATE TABLE `{src}` (id Int64, name String, ver UInt32, is_deleted UInt8) "
        f"ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY id"
    )
    ch_client.command(f"SYSTEM STOP MERGES `{src}`")
    ch_client.command(f"INSERT INTO `{src}` SELECT number + 1, 'v1', 1, 0 FROM numbers(10)")
    _drop(ch_client, dst)
    try:
        r = quickhouse.sync(ch_source, ch_target, **_full(src, dst))
        assert r.rows_written == 10
        ch_client.command(f"INSERT INTO `{src}` VALUES (2, 'v1', 2, 1)")

        result = quickhouse.reconcile_keys(
            ch_source, ch_target, dest_table=dst, key="id", source_table=src
        )
        assert result.orphan_keys == 1
        assert result.orphan_sample == ["2"]
        assert result.missing_keys == 0
    finally:
        _drop(ch_client, src, dst)


def test_source_query_over_a_replacing_table_is_sent_as_written(
    ch_client, ch_source, ch_target, unique_name, capfd
):
    """FINAL goes only into quickhouse's own reads of source_table: a
    source_query is the caller's SQL, so it reads what it says."""
    src, dst = unique_name, f"{unique_name}_dst"
    _seed_versions(ch_client, src)
    _drop(ch_client, dst)
    kw = dict(dest_table=dst, mode="full", key=["id"], create_if_missing=True)
    try:
        r = quickhouse.sync(ch_source, ch_target, source_query=f"SELECT * FROM `{src}`", **kw)
        assert r.rows_written == 12

        _drop(ch_client, dst)
        capfd.readouterr()
        r = quickhouse.sync(
            _ch_source(final=True), ch_target,
            source_query=f"SELECT * FROM `{src}` FINAL", **kw,
        )
        err = capfd.readouterr().err
        assert r.rows_written == 10
        assert "final=True does not change source_query" in err
    finally:
        _drop(ch_client, src, dst)
