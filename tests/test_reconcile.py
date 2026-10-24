"""Integration tests for the 0.15 delete/reconcile paths, the structured
``TransferResult``, and ``reconcile_values``' per-column comparison.

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


def _drift(result):
    """`reconcile_values` columns by name, as (mismatched, null_in_dest_only,
    null_in_source_only, sorted sample)."""
    return {
        c.column: (c.mismatched, c.null_in_dest_only, c.null_in_source_only, sorted(c.sample))
        for c in result.columns
    }


def test_reconcile_values_finds_value_drift_and_nulls_a_sync_wrote_from_postgres(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """#28 end to end: a source_query that has lost a column makes an
    incremental run write it as NULL over values the source still holds. Key
    counts match throughout; only a per-column comparison sees it."""
    src = f"{unique_name}_src"
    pg_conn.execute(f"DROP TABLE IF EXISTS {src}")
    pg_conn.execute(
        f"""
        CREATE TABLE {src} (
            id          bigint PRIMARY KEY,
            amount      numeric(15, 2) NOT NULL,
            state       text,
            qty         integer NOT NULL,
            updated_at  timestamp NOT NULL
        )
        """
    )
    pg_conn.execute(
        f"""
        INSERT INTO {src} VALUES
            (1, 10.10, 'posted', 1, '2026-07-01 00:00:00.123456'),
            (2, 20.20, 'posted', 2, '2026-07-01 01:00:00'),
            (3, 30.30, 'draft',  3, '2026-07-01 02:00:00'),
            (4, 40.40, 'posted', 4, '2026-07-01 03:00:00'),
            (5, 50.50, NULL,     5, '2026-07-01 04:00:00'),
            (6, 60.60, 'posted', 6, '2026-08-01 00:00:00')
        """
    )
    common = dict(
        dest_table=unique_name,
        mode="incremental",
        watermark="updated_at",
        key=["id"],
        state_key=src,
        create_if_missing=True,
    )
    qh.sync(pg_source, ch_target, source_table=src, **common)

    def reconcile():
        return qh.reconcile_values(
            pg_source, ch_target, unique_name, key="id", source_table=src, window="id <= 5"
        )

    clean = reconcile()
    assert clean.compared_keys == 5
    assert [c.column for c in clean.columns] == ["amount", "state", "qty", "updated_at"]
    assert all(c.mismatched == 0 for c in clean.columns), clean.columns
    assert clean.skipped_columns == []

    # Hold the ReplacingMergeTree's parts apart, so each rewritten row sits
    # beside its original: the comparison has to read the newest through FINAL.
    ch_client.command(f"SYSTEM STOP MERGES {CH_DB}.{unique_name}")
    try:
        pg_conn.execute(f"UPDATE {src} SET updated_at = now() WHERE id IN (2, 3)")
        # The #28 run: the query no longer carries `state`.
        qh.sync(
            pg_source,
            ch_target,
            source_query=f"SELECT id, amount, qty, updated_at FROM {src}",
            **common,
        )
        # A source edit no watermark saw: plain value drift.
        pg_conn.execute(f"UPDATE {src} SET qty = 999 WHERE id = 4")
        assert int(ch_client.command(f"SELECT count() FROM {CH_DB}.{unique_name}")) == 8

        drifted = reconcile()
    finally:
        ch_client.command(f"SYSTEM START MERGES {CH_DB}.{unique_name}")

    assert drifted.source_rows == 5
    assert drifted.dest_rows == 5, "FINAL collapses the rewritten rows"
    assert drifted.compared_keys == 5
    assert (drifted.missing_keys, drifted.orphan_keys, drifted.duplicate_keys) == (0, 0, 0)
    assert _drift(drifted) == {
        "amount": (0, 0, 0, []),
        "state": (2, 2, 0, ["2", "3"]),
        "qty": (1, 0, 0, ["4"]),
        "updated_at": (0, 0, 0, []),
    }


def test_reconcile_values_reports_a_float_truncated_into_a_decimal_unless_tolerated(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """ClickHouse truncates a Float64 written into a Decimal column, so 0.29
    lands in Decimal(15, 2) as 0.28. A float8 column overridden to a Decimal
    goes down exactly that path."""
    src = f"{unique_name}_src"
    pg_conn.execute(f"DROP TABLE IF EXISTS {src}")
    pg_conn.execute(f"CREATE TABLE {src} (id bigint PRIMARY KEY, price float8 NOT NULL)")
    pg_conn.execute(f"INSERT INTO {src} VALUES (1, 0.29), (2, 0.57), (3, 0.5), (4, 32.9)")
    qh.sync(
        pg_source,
        ch_target,
        source_table=src,
        dest_table=unique_name,
        mode="full",
        order_by=["id"],
        create_if_missing=True,
        type_overrides={"price": "Decimal(15, 2)"},
    )
    stored = ch_client.query(
        f"SELECT toString(price) FROM {CH_DB}.{unique_name} ORDER BY id"
    ).result_rows
    assert [r[0] for r in stored] == ["0.28", "0.56", "0.5", "32.9"]

    def price(**kwargs):
        r = qh.reconcile_values(
            pg_source, ch_target, unique_name, key="id", source_table=src, window="id > 0", **kwargs
        )
        (column,) = r.columns
        return column

    exact = price()
    assert exact.mismatched == 2
    assert sorted(exact.sample) == ["1", "2"]
    assert exact.null_in_dest_only == 0
    # One step of the column's scale covers every truncation, named per column
    # or given once for every numeric column.
    assert price(tolerance={"price": 0.01}).mismatched == 0
    assert price(tolerance=0.01).mismatched == 0


def test_reconcile_values_from_mysql_into_a_table_another_loader_created(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """#28's setting: the ClickHouse table predates the sync, with every column
    and its own types. `tinyint(1)` went into a Bool and a UInt8, text into
    non-Nullable columns (where a NULL is stored as ''), and a DATETIME(6) into
    a second-precision DateTime. None of that is drift; values a sync wrote
    over, or never wrote, are."""
    src = f"{unique_name}_src"
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        cur.execute(
            f"""
            CREATE TABLE `{src}` (
                id          BIGINT PRIMARY KEY,
                is_active   TINYINT(1) NOT NULL,
                is_open     TINYINT(1) NOT NULL,
                state       VARCHAR(20) NOT NULL,
                note        VARCHAR(50) NULL,
                qty         INT NOT NULL,
                updated_at  DATETIME(6) NOT NULL
            )
            """
        )
        cur.execute(
            f"""
            INSERT INTO `{src}` VALUES
                (1, 1, 1, 'posted', NULL, 10, '2026-07-01 00:00:00.123456'),
                (2, 2, 2, 'posted', 'n2', 20, '2026-07-01 01:00:00.500000'),
                (3, 0, 0, 'draft',  'n3', 30, '2026-07-01 02:00:00'),
                (4, 1, 1, 'posted', NULL, 40, '2026-07-01 03:00:00'),
                (5, 1, 0, 'cancel', 'n5', 50, '2026-07-01 04:00:00')
            """
        )
    mysql_conn.commit()
    ch_client.command(
        f"""
        CREATE TABLE {CH_DB}.{unique_name} (
            id          Int64,
            is_active   Bool,
            is_open     UInt8,
            state       LowCardinality(String),
            note        String,
            qty         Int32,
            updated_at  DateTime('UTC')
        ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY id
        """
    )
    common = dict(
        dest_table=unique_name,
        mode="incremental",
        watermark="updated_at",
        key=["id"],
        state_key=src,
        tinyint1_as_bool=False,
    )
    qh.sync(mysql_source, ch_target, source_table=src, **common)

    def reconcile():
        return qh.reconcile_values(
            mysql_source, ch_target, unique_name, key="id", source_table=src, window="id > 0"
        )

    clean = reconcile()
    assert clean.compared_keys == 5
    assert [c.column for c in clean.columns] == [
        "is_active",
        "is_open",
        "state",
        "note",
        "qty",
        "updated_at",
    ]
    assert all(c.mismatched == 0 for c in clean.columns), clean.columns

    ch_client.command(f"SYSTEM STOP MERGES {CH_DB}.{unique_name}")
    try:
        with mysql_conn.cursor() as cur:
            cur.execute(f"UPDATE `{src}` SET updated_at = '2026-09-01 00:00:00' WHERE id IN (3, 5)")
        mysql_conn.commit()
        # The #28 run: `state` has dropped out of the query, so the column,
        # which can't hold NULL, is reset to ''.
        qh.sync(
            mysql_source,
            ch_target,
            source_query=f"SELECT id, is_active, is_open, note, qty, updated_at FROM `{src}`",
            **common,
        )
        with mysql_conn.cursor() as cur:
            cur.execute(f"UPDATE `{src}` SET qty = 999 WHERE id = 2")
        mysql_conn.commit()
        assert int(ch_client.command(f"SELECT count() FROM {CH_DB}.{unique_name}")) == 7

        drifted = reconcile()
    finally:
        ch_client.command(f"SYSTEM START MERGES {CH_DB}.{unique_name}")

    assert drifted.dest_rows == 5
    assert drifted.compared_keys == 5
    assert (drifted.missing_keys, drifted.orphan_keys, drifted.duplicate_keys) == (0, 0, 0)
    assert _drift(drifted) == {
        "is_active": (0, 0, 0, []),
        "is_open": (0, 0, 0, []),
        "state": (2, 2, 0, ["3", "5"]),
        "note": (0, 0, 0, []),
        "qty": (1, 0, 0, ["2"]),
        "updated_at": (0, 0, 0, []),
    }


def test_reconcile_values_refuses_a_bigquery_destination(pg_source, unique_name):
    """Not supported yet, and said so before anything is read."""
    try:
        qh.reconcile_values(
            pg_source,
            qh.BigQuery("some-project", dataset_id="analytics"),
            unique_name,
            key="id",
            source_table="whatever",
            window="id > 0",
        )
    except RuntimeError as e:
        assert "BigQuery destination" in str(e), e
    else:
        raise AssertionError("a BigQuery destination must be refused")
