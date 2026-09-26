"""Integration tests for the 0.15 delete/reconcile paths and the structured
``TransferResult``.

These need a live PostgreSQL and ClickHouse (``docker compose up -d``); the
fixtures in ``conftest.py`` skip the module if either is unreachable.

The reason this file exists rather than a unit test: the whole point of
``delete_stale_in_window`` and ``reconcile_keys`` is that rows *disappear* from
the destination. That is a property of the generated SQL meeting a real engine —
ClickHouse's lightweight ``DELETE``, its ``NOT IN`` against a subquery, the
scalar ``min``/``max`` window bound — and none of it is proven by asserting on
statement text.
"""

from __future__ import annotations

import quickhouse as qh
from conftest import CH_DB


def _ids(ch_client, table):
    got = ch_client.query(f"SELECT DISTINCT id FROM {CH_DB}.{table} ORDER BY id").result_rows
    return [r[0] for r in got]


def _seed_source(pg_conn, table):
    """Five rows across two days, so a window can select a subset."""
    pg_conn.execute(f"DROP TABLE IF EXISTS {table}")
    pg_conn.execute(
        f"""
        CREATE TABLE {table} (
            id          bigint PRIMARY KEY,
            create_date timestamp NOT NULL,
            updated_at  timestamp NOT NULL,
            qty         integer NOT NULL
        )
        """
    )
    pg_conn.execute(
        f"""
        INSERT INTO {table} (id, create_date, updated_at, qty) VALUES
            (1, '2026-07-01 00:00', '2026-07-01 00:00', 10),
            (2, '2026-07-01 01:00', '2026-07-01 01:00', 20),
            (3, '2026-07-01 02:00', '2026-07-01 02:00', 30),
            (4, '2026-07-02 00:00', '2026-07-02 00:00', 40),
            (5, '2026-08-01 00:00', '2026-08-01 00:00', 50)
        """
    )


def test_clickhouse_delete_stale_in_window_removes_hard_deleted_rows(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """The P0 case: a source row is hard-deleted, and an ordinary incremental
    sync can never remove it. With ``delete_stale_in_window`` it does."""
    src = f"{unique_name}_src"
    _seed_source(pg_conn, src)

    common = dict(
        source_table=src,
        dest_table=unique_name,
        mode="incremental",
        watermark="updated_at",
        key=["id"],
        order_by=["id"],
        create_if_missing=True,
    )

    first = qh.sync(pg_source, ch_target, **common)
    assert first.rows_written == 5
    assert first.rows_deleted == 0
    assert _ids(ch_client, unique_name) == [1, 2, 3, 4, 5]

    # The source hard-deletes id=2 and edits id=3. A plain incremental re-run
    # sees only id=3's new watermark; nothing at all tells it about id=2.
    pg_conn.execute(f"DELETE FROM {src} WHERE id = 2")
    pg_conn.execute(f"UPDATE {src} SET qty = 999, updated_at = now() WHERE id = 3")

    plain = qh.sync(pg_source, ch_target, **common)
    assert plain.rows_deleted == 0
    assert _ids(ch_client, unique_name) == [1, 2, 3, 4, 5], (
        "an ordinary incremental sync must NOT remove the deleted row — that is "
        "the drift this feature exists to fix"
    )

    # Now with the window delete. `advance_watermark=False` re-reads the July
    # window rather than only what changed since the last cursor, so the batch
    # is the window's full contents — which is what "replace this window" needs.
    windowed = qh.sync(
        pg_source,
        ch_target,
        source_query=(
            f"SELECT id, create_date, updated_at, qty FROM {src} "
            "WHERE create_date >= '2026-07-01' AND create_date < '2026-08-01'"
        ),
        dest_table=unique_name,
        mode="incremental",
        watermark="updated_at",
        key=["id"],
        seed_watermark="1970-01-01 00:00:00",
        advance_watermark=False,
        state_key=f"{unique_name}:window",
        merge_prune_partition_by="create_date",
        delete_stale_in_window=True,
    )
    assert windowed.rows_deleted == 1, windowed
    assert _ids(ch_client, unique_name) == [1, 3, 4, 5]

    # The delete is *scoped*: id=5 lives outside the window and survives, which
    # is the whole safety property.
    assert 5 in _ids(ch_client, unique_name)


def test_reconcile_keys_measures_then_repairs(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    src = f"{unique_name}_src"
    _seed_source(pg_conn, src)
    qh.sync(
        pg_source,
        ch_target,
        source_table=src,
        dest_table=unique_name,
        mode="full",
        order_by=["id"],
        create_if_missing=True,
    )
    assert _ids(ch_client, unique_name) == [1, 2, 3, 4, 5]

    pg_conn.execute(f"DELETE FROM {src} WHERE id IN (2, 4)")
    window = "create_date >= '2026-07-01' AND create_date < '2026-08-01'"

    # Measure only — the default, and the mode worth scheduling.
    measured = qh.reconcile_keys(
        pg_source,
        ch_target,
        unique_name,
        key="id",
        source_table=src,
        window=window,
    )
    assert measured.source_keys == 2  # ids 1, 3 inside the window
    assert measured.dest_keys == 4  # ids 1, 2, 3, 4
    assert measured.orphan_keys == 2
    assert measured.missing_keys == 0
    assert measured.rows_deleted == 0
    assert sorted(measured.orphan_sample) == ["2", "4"]
    assert _ids(ch_client, unique_name) == [1, 2, 3, 4, 5], "measuring must not mutate"

    # Repair.
    repaired = qh.reconcile_keys(
        pg_source,
        ch_target,
        unique_name,
        key="id",
        source_table=src,
        window=window,
        delete=True,
    )
    assert repaired.orphan_keys == 2
    assert repaired.rows_deleted == 2
    # id=5 is outside the window and untouched, even though the source still
    # has it — a scoped reconcile says nothing about rows it did not look at.
    assert _ids(ch_client, unique_name) == [1, 3, 5]


def test_reconcile_refuses_to_delete_without_a_window(pg_source, ch_target, unique_name):
    """An unbounded delete is a full refresh with a race in it, not a reconcile."""
    try:
        qh.reconcile_keys(
            pg_source, ch_target, unique_name, key="id", source_table="whatever", delete=True
        )
    except RuntimeError as e:
        assert "requires a window" in str(e), e
    else:
        raise AssertionError("an unbounded delete must be refused")


def test_reconcile_max_delete_keys_is_a_real_ceiling(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    src = f"{unique_name}_src"
    _seed_source(pg_conn, src)
    qh.sync(
        pg_source,
        ch_target,
        source_table=src,
        dest_table=unique_name,
        mode="full",
        order_by=["id"],
        create_if_missing=True,
    )
    pg_conn.execute(f"DELETE FROM {src} WHERE id IN (1, 2, 3)")
    window = "create_date >= '2026-07-01' AND create_date < '2026-08-01'"
    try:
        qh.reconcile_keys(
            pg_source,
            ch_target,
            unique_name,
            key="id",
            source_table=src,
            window=window,
            delete=True,
            max_delete_keys=2,
        )
    except RuntimeError as e:
        assert "max_delete_keys" in str(e), e
    else:
        raise AssertionError("3 orphans against a ceiling of 2 must be refused")
    # ...and nothing was deleted on the way to refusing.
    assert _ids(ch_client, unique_name) == [1, 2, 3, 4, 5]


def test_transfer_result_carries_phase_timings(pg_conn, pg_source, ch_target, unique_name):
    src = f"{unique_name}_src"
    _seed_source(pg_conn, src)
    r = qh.sync(
        pg_source,
        ch_target,
        source_table=src,
        dest_table=unique_name,
        mode="full",
        order_by=["id"],
        create_if_missing=True,
    )
    # Every phase is populated and the parts are bounded by the whole. (Not a
    # strict sum: read overlaps stage by design, and both exclude setup.)
    assert r.read_secs >= 0.0
    assert r.stage_secs > 0.0
    assert r.promote_secs > 0.0
    assert r.stage_secs + r.promote_secs <= r.duration_secs + 0.5
    assert r.warnings == []
    assert repr(r).startswith("TransferResult(")


def test_null_watermark_rows_surface_as_a_structured_warning(
    pg_conn, pg_source, ch_target, unique_name
):
    """The most dangerous silent failure in the tool: rows excluded from this
    and every future incremental run, with the transfer reporting success."""
    src = f"{unique_name}_src"
    pg_conn.execute(f"DROP TABLE IF EXISTS {src}")
    pg_conn.execute(
        f"CREATE TABLE {src} (id bigint PRIMARY KEY, updated_at timestamp, qty integer)"
    )
    pg_conn.execute(
        f"INSERT INTO {src} VALUES (1, '2026-07-01', 10), (2, NULL, 20), (3, NULL, 30)"
    )
    r = qh.sync(
        pg_source,
        ch_target,
        source_table=src,
        dest_table=unique_name,
        mode="incremental",
        watermark="updated_at",
        key=["id"],
        order_by=["id"],
        create_if_missing=True,
    )
    warnings = {w.kind: w for w in r.warnings}
    assert "null_watermark" in warnings, r.warnings
    w = warnings["null_watermark"]
    assert w.column == "updated_at"
    assert w.count == 2
    assert "silently excluded" in w.message
    # The run still succeeded — which is exactly why the caller needs this as
    # data rather than as a log line.
    assert r.rows_written == 1


def test_reconcile_keeps_padded_char_keys(pg_conn, ch_client, pg_source, ch_target, unique_name):
    """A char(n) key reaches the destination padded ('XYZ       '), but the
    source keyset rendered it with ::text, which strips the padding. Every short
    key then looked like an orphan and delete=True removed live rows."""
    src = f"{unique_name}_src"
    pg_conn.execute(f"DROP TABLE IF EXISTS {src}")
    pg_conn.execute(f"CREATE TABLE {src} (code char(10) PRIMARY KEY, qty integer)")
    pg_conn.execute(f"INSERT INTO {src} VALUES ('ABCDEFGHIJ', 1), ('XYZ', 2)")
    qh.sync(pg_source, ch_target, source_table=src, dest_table=unique_name, mode="full", key=["code"])

    result = qh.reconcile_keys(
        pg_source,
        ch_target,
        unique_name,
        key="code",
        source_table=src,
        window="code <> ''",
        delete=True,
    )
    assert result.orphan_keys == 0
    assert result.rows_deleted == 0
    assert int(ch_client.command(f"SELECT count() FROM {CH_DB}.{unique_name}")) == 2


def test_reconcile_keeps_keys_that_differ_only_in_case(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """MySQL's DISTINCT ran under the connection's case-insensitive collation,
    so 'aB3x' and 'Ab3X' (distinct under the column's utf8mb4_bin) came back as
    one key and reconcile deleted the other from the destination."""
    src = f"{unique_name}_src"
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        cur.execute(
            f"CREATE TABLE `{src}` (code VARCHAR(12) COLLATE utf8mb4_bin PRIMARY KEY, qty INT)"
        )
        cur.execute(f"INSERT INTO `{src}` VALUES ('aB3x', 1), ('Ab3X', 2)")
    mysql_conn.commit()
    qh.sync(mysql_source, ch_target, source_table=src, dest_table=unique_name, mode="full", key=["code"])

    result = qh.reconcile_keys(
        mysql_source,
        ch_target,
        unique_name,
        key="code",
        source_table=src,
        window="code <> ''",
        delete=True,
    )
    assert result.source_keys == 2
    assert result.orphan_keys == 0
    assert result.rows_deleted == 0
    assert int(ch_client.command(f"SELECT count() FROM {CH_DB}.{unique_name}")) == 2
