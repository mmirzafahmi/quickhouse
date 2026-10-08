"""End-to-end integration tests: MySQL -> ClickHouse.

Mirrors test_sync.py's PostgreSQL coverage (full refresh reconciliation,
incremental idempotency, column mapping) against a MySQL source instead.

Run against the services in ``docker-compose.yml`` after building the module:

    docker compose up -d
    pip install -e '.[test]'
    maturin develop --release
    pytest -v
"""

from __future__ import annotations

import contextlib
import datetime as dt
import threading
import time

import pytest

import quickhouse
from conftest import (
    MYSQL_DB,
    MYSQL_DSN,
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_ROOT_PASSWORD,
    MYSQL_USER,
)


def _seed_table(mysql_conn, table: str, rows: int, base_ts: str = "2024-01-01 00:00:00"):
    """Create and populate a table with mixed types + a NULL column."""
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"""
            CREATE TABLE `{table}` (
                id          BIGINT PRIMARY KEY,
                name        TEXT,
                amount      DOUBLE,
                qty         INT,
                is_active   BOOLEAN,
                note        TEXT,          -- left NULL to exercise Nullable
                write_date  DATETIME NOT NULL
            )
            """
        )
        cur.executemany(
            f"INSERT INTO `{table}` (id, name, amount, qty, is_active, write_date) "
            f"VALUES (%s, %s, %s, %s, %s, %s)",
            [(i, f"row-{i}", i * 1.5, i, i % 2 == 0, base_ts) for i in range(1, rows + 1)],
        )


def _drop_ch(ch_client, table: str):
    ch_client.command(f"DROP TABLE IF EXISTS `{table}`")
    ch_client.command(f"DROP TABLE IF EXISTS `{table}_quickhouse_tmp`")


def test_full_refresh_reconciles(mysql_conn, ch_client, mysql_source, ch_target, unique_name):
    table = unique_name
    n = 5000
    _seed_table(mysql_conn, table, n)
    _drop_ch(ch_client, table)
    try:
        result = quickhouse.sync(
            mysql_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="full",
            key=["id"],
            create_if_missing=True,
            parallelism=4,
            batch_rows=1000,
        )
        assert result.rows_written == n

        ch_count = ch_client.command(f"SELECT count() FROM `{table}`")
        assert int(ch_count) == n

        # Column-level reconciliation.
        mysql_sum = _mysql_scalar(mysql_conn, f"SELECT SUM(amount) FROM `{table}`")
        ch_sum = float(ch_client.command(f"SELECT sum(amount) FROM `{table}`"))
        assert abs(float(mysql_sum) - ch_sum) < 1e-6

        # NULL column round-trips.
        ch_nulls = ch_client.command(f"SELECT countIf(note IS NULL) FROM `{table}`")
        assert int(ch_nulls) == n

        # Boolean mapped correctly (TINYINT(1) -> Bool, not a plain int).
        ch_type = ch_client.command(
            f"SELECT type FROM system.columns "
            f"WHERE database = currentDatabase() AND table = '{table}' AND name = 'is_active'"
        )
        assert "Bool" in ch_type
    finally:
        _drop_ch(ch_client, table)


def test_wide_decimal_is_rounded_into_a_narrower_override(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """MySQL sends DECIMAL(65,30) padded to all 30 fraction digits, so
    1000000000.5 arrives as 40 digits. It fits Decimal(38, 9) but used to
    overflow while being parsed and land as NULL."""
    table = unique_name
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, v DECIMAL(65, 30))")
        cur.execute(f"INSERT INTO `{table}` VALUES (1, 1000000000.5), (2, 0.1234567894)")
    mysql_conn.commit()
    _drop_ch(ch_client, table)
    try:
        quickhouse.sync(
            mysql_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="full",
            key=["id"],
            type_overrides={"v": "Decimal(38, 9)"},
        )
        rows = ch_client.query(f"SELECT id, toString(v) FROM `{table}` ORDER BY id").result_rows
        assert rows == [(1, "1000000000.5"), (2, "0.123456789")]
    finally:
        _drop_ch(ch_client, table)


def test_full_refresh_coerces_zero_dates_to_null(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """Regression test: MySQL's classic 0000-00-00 zero-date and partial-zero
    dates (e.g. 2024-00-15) used to crash the whole decode with 'invalid
    MySQL date' / 'invalid MySQL datetime' instead of just nulling the one
    unrepresentable value. Must coerce to NULL and let the transfer complete."""
    table = unique_name
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"""
            CREATE TABLE `{table}` (
                id          BIGINT PRIMARY KEY,
                event_date  DATE NULL,
                event_at    DATETIME NULL
            )
            """
        )
        cur.execute("SELECT @@SESSION.sql_mode")
        (orig_mode,) = cur.fetchone()
        try:
            # MySQL 8's default strict sql_mode rejects zero-date literals on
            # insert; relax it just for this seed so the server accepts what
            # real legacy data commonly already contains.
            cur.execute("SET SESSION sql_mode = ''")
            cur.executemany(
                f"INSERT INTO `{table}` (id, event_date, event_at) VALUES (%s, %s, %s)",
                [
                    (1, "2024-05-01", "2024-05-01 10:00:00"),  # valid
                    (2, "0000-00-00", "0000-00-00 00:00:00"),  # full zero-date
                    (3, "2024-00-15", "2024-05-00 10:00:00"),  # partial-zero
                ],
            )
        finally:
            cur.execute("SET SESSION sql_mode = %s", (orig_mode,))
    _drop_ch(ch_client, table)
    try:
        result = quickhouse.sync(
            mysql_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="full",
            key=["id"],
            create_if_missing=True,
        )
        assert result.rows_written == 3

        null_dates = ch_client.command(f"SELECT countIf(event_date IS NULL) FROM `{table}`")
        assert int(null_dates) == 2
        null_datetimes = ch_client.command(f"SELECT countIf(event_at IS NULL) FROM `{table}`")
        assert int(null_datetimes) == 2

        valid_date = ch_client.command(f"SELECT event_date FROM `{table}` WHERE id = 1")
        assert str(valid_date) == "2024-05-01"
    finally:
        _drop_ch(ch_client, table)


def test_full_refresh_not_null_zero_dates_promote_column_to_nullable(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """Regression test for bug report 06: a NOT NULL MySQL DATE/DATETIME
    column resolves as non-nullable from the source's own constraint, but a
    legacy zero-date in it still gets coerced to NULL at decode time (same as
    the nullable case above). Previously this produced a *second*, more
    confusing failure than the original bug: 'arrow error: Invalid argument
    error: Column ... is declared as non-nullable but contains null values',
    since the NOT NULL destination column didn't accept the coerced NULL.
    The destination column must be auto-promoted to Nullable so the coercion
    that already works for nullable columns works here too."""
    table = unique_name
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"""
            CREATE TABLE `{table}` (
                id            BIGINT PRIMARY KEY,
                created_date  DATETIME NOT NULL
            )
            """
        )
        cur.execute("SELECT @@SESSION.sql_mode")
        (orig_mode,) = cur.fetchone()
        try:
            cur.execute("SET SESSION sql_mode = ''")
            cur.executemany(
                f"INSERT INTO `{table}` (id, created_date) VALUES (%s, %s)",
                [
                    (1, "2024-05-01 10:00:00"),  # valid
                    (2, "0000-00-00 00:00:00"),  # zero-date, despite NOT NULL
                ],
            )
        finally:
            cur.execute("SET SESSION sql_mode = %s", (orig_mode,))
    _drop_ch(ch_client, table)
    try:
        result = quickhouse.sync(
            mysql_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="full",
            key=["id"],
            create_if_missing=True,
        )
        assert result.rows_written == 2

        col_type = ch_client.command(
            f"SELECT type FROM system.columns "
            f"WHERE database = currentDatabase() AND table = '{table}' AND name = 'created_date'"
        )
        assert "Nullable" in col_type, f"NOT NULL source column must be promoted to Nullable: {col_type}"

        null_count = ch_client.command(f"SELECT countIf(created_date IS NULL) FROM `{table}`")
        assert int(null_count) == 1
        valid = ch_client.command(f"SELECT created_date FROM `{table}` WHERE id = 1")
        assert str(valid) == "2024-05-01 10:00:00.000000"
    finally:
        _drop_ch(ch_client, table)


def test_full_refresh_coerces_out_of_range_dates_to_null(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """Regression test: a valid MySQL DATE/DATETIME whose year is outside
    ClickHouse's Date32/DateTime64 window ([1900-01-01, 2299-12-31]) used to
    abort the entire transfer at insert time with 'VALUE_IS_OUT_OF_RANGE_OF_
    DATA_TYPE'. Legacy tables routinely hold the '9999-12-31' "never expires"
    sentinel and pre-1900 dates, so an out-of-range value must be coerced to
    NULL (like a zero-date) and let the transfer complete."""
    table = unique_name
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"""
            CREATE TABLE `{table}` (
                id          BIGINT PRIMARY KEY,
                event_date  DATE NULL,
                event_at    DATETIME NULL
            )
            """
        )
        cur.executemany(
            f"INSERT INTO `{table}` (id, event_date, event_at) VALUES (%s, %s, %s)",
            [
                (1, "2024-05-01", "2024-05-01 10:00:00"),  # in range
                (2, "9999-12-31", "9999-12-31 23:59:59"),  # far-future sentinel
                (3, "1000-01-01", "1000-01-01 00:00:00"),  # MySQL min, below CH
                (4, "1899-12-31", "1899-12-31 23:59:59"),  # just below CH window
            ],
        )
    _drop_ch(ch_client, table)
    try:
        result = quickhouse.sync(
            mysql_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="full",
            key=["id"],
            create_if_missing=True,
        )
        assert result.rows_written == 4

        null_dates = ch_client.command(f"SELECT countIf(event_date IS NULL) FROM `{table}`")
        assert int(null_dates) == 3
        null_datetimes = ch_client.command(f"SELECT countIf(event_at IS NULL) FROM `{table}`")
        assert int(null_datetimes) == 3

        # The one in-range row round-trips untouched.
        valid_date = ch_client.command(f"SELECT event_date FROM `{table}` WHERE id = 1")
        assert str(valid_date) == "2024-05-01"
    finally:
        _drop_ch(ch_client, table)


def test_full_refresh_ignores_watermark_with_all_null_column(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """Regression test: passing `watermark` alongside mode="full" used to
    eagerly run MAX(watermark) even though full mode discards the result —
    and MAX() over an all-NULL column returns SQL NULL, which crashed the
    whole process (mysql_common panics converting NULL directly to String
    instead of erroring). Full mode must skip the watermark lookup entirely."""
    table = unique_name
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"""
            CREATE TABLE `{table}` (
                id            BIGINT PRIMARY KEY,
                name          TEXT,
                created_date  DATETIME NULL
            )
            """
        )
        cur.executemany(
            f"INSERT INTO `{table}` (id, name, created_date) VALUES (%s, %s, NULL)",
            [(i, f"row-{i}") for i in range(1, 51)],
        )
    _drop_ch(ch_client, table)
    try:
        result = quickhouse.sync(
            mysql_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="full",
            watermark="created_date",  # set but irrelevant for full mode
            key=["id"],
            create_if_missing=True,
        )
        assert result.rows_written == 50
    finally:
        _drop_ch(ch_client, table)


def test_time_column_stored_as_text(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """MySQL TIME maps to a ClickHouse String and round-trips as canonical
    [-]HH:MM:SS[.ffffff] text — including the negative and >24h durations
    (range +/-838:59:59) that no time-of-day type can represent. Previously the
    column carried an Arrow Time64 physical type into a String destination,
    silently storing bogus epoch-relative datetimes."""
    table = unique_name
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, t TIME NULL)")
        cur.executemany(
            f"INSERT INTO `{table}` (id, t) VALUES (%s, %s)",
            [(1, "10:30:00"), (2, "-05:00:00"), (3, "838:59:59"), (4, None)],
        )
    _drop_ch(ch_client, table)
    try:
        result = quickhouse.sync(
            mysql_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="full",
            key=["id"],
            create_if_missing=True,
        )
        assert result.rows_written == 4

        col_type = ch_client.command(
            f"SELECT type FROM system.columns "
            f"WHERE database = currentDatabase() AND table = '{table}' AND name = 't'"
        )
        assert "String" in col_type

        vals = {
            row[0]: row[1]
            for row in ch_client.query(f"SELECT id, t FROM `{table}` ORDER BY id").result_rows
        }
        assert vals[1] == "10:30:00"
        assert vals[2] == "-05:00:00"
        assert vals[3] == "838:59:59"  # 34d 22h -> 838 accumulated hours
        assert vals[4] is None
    finally:
        _drop_ch(ch_client, table)


def test_incremental_missing_watermark_column_errors_clearly(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """A watermark column absent from the source must fail with a clear config
    error naming the available columns — not the cryptic driver error the
    MAX(watermark) probe raised before (MySQL 1054 'Unknown column ... in
    field list'). Common trigger: one watermark reused across a batch of tables
    where this one lacks it."""
    table = unique_name
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, name TEXT)")
        cur.execute(f"INSERT INTO `{table}` (id, name) VALUES (1, 'a')")
    _drop_ch(ch_client, table)
    try:
        with pytest.raises(RuntimeError, match=r"watermark column 'created_date' not found"):
            quickhouse.sync(
                mysql_source,
                ch_target,
                dest_table=table,
                source_table=table,
                mode="incremental",
                watermark="created_date",  # not a column of this table
                key=["id"],
                create_if_missing=True,
            )
    finally:
        _drop_ch(ch_client, table)


def test_incremental_appends_and_is_idempotent(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    table = unique_name
    _seed_table(mysql_conn, table, 100, base_ts="2024-01-01 00:00:00")
    _drop_ch(ch_client, table)
    try:
        # First incremental run backfills everything (no prior watermark).
        r1 = quickhouse.sync(
            mysql_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="incremental",
            watermark="write_date",
            key=["id"],
            create_if_missing=True,
            engine="ReplacingMergeTree",
            order_by=["id"],
            parallelism=2,
            batch_rows=50,
        )
        assert r1.rows_written == 100

        # Add newer rows.
        with mysql_conn.cursor() as cur:
            cur.executemany(
                f"INSERT INTO `{table}` (id, name, amount, qty, is_active, write_date) "
                f"VALUES (%s, %s, %s, %s, %s, %s)",
                [
                    (i, f"row-{i}", i * 1.5, i, True, "2024-02-01 00:00:00")
                    for i in range(101, 151)
                ],
            )

        r2 = quickhouse.sync(
            mysql_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="incremental",
            watermark="write_date",
            key=["id"],
            parallelism=2,
            batch_rows=50,
        )
        assert r2.rows_written == 50  # only the new rows

        # Re-running with no new data changes nothing.
        r3 = quickhouse.sync(
            mysql_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="incremental",
            watermark="write_date",
            key=["id"],
        )
        assert r3.rows_written == 0

        total = ch_client.command(f"SELECT count() FROM `{table}` FINAL")
        assert int(total) == 150
    finally:
        _drop_ch(ch_client, table)


def test_column_mapping(mysql_conn, ch_client, mysql_source, ch_target, unique_name):
    table = unique_name
    _seed_table(mysql_conn, table, 10)
    _drop_ch(ch_client, table)
    try:
        quickhouse.sync(
            mysql_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="full",
            key=["id"],
            exclude=["note"],
            rename={"amount": "amt"},
            parallelism=1,
        )
        cols = ch_client.command(
            f"SELECT groupArray(name) FROM system.columns "
            f"WHERE database = currentDatabase() AND table = '{table}'"
        )
        assert "amt" in cols
        assert "note" not in cols
    finally:
        _drop_ch(ch_client, table)


def test_full_refresh_decimal_override_preserves_exact_precision(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """MySQL counterpart of the same regression test in test_sync.py:
    `type_overrides={"col": "Decimal(P,S)"}` now decodes the DECIMAL/
    NEWDECIMAL value exactly instead of through a lossy Float64 round-trip.
    Also covers rounding (override scale narrower than the column's own
    declared scale) and precision overflow on a NOT NULL column (proving
    it's forced nullable, so the coercion doesn't abort the transfer)."""
    table = unique_name
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"""
            CREATE TABLE `{table}` (
                id            BIGINT PRIMARY KEY,
                amount_wide   DECIMAL(30, 10),
                amount_round  DECIMAL(10, 4) NOT NULL,
                amount_narrow DECIMAL(10, 2) NOT NULL
            )
            """
        )
        cur.executemany(
            f"INSERT INTO `{table}` (id, amount_wide, amount_round, amount_narrow) "
            f"VALUES (%s, %s, %s, %s)",
            [
                (1, "123456789012345.6789012345", "12.3450", "12.34"),
                (2, None, "1.0000", "12345.67"),
            ],
        )
    _drop_ch(ch_client, table)
    try:
        result = quickhouse.sync(
            mysql_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="full",
            key=["id"],
            create_if_missing=True,
            type_overrides={
                "amount_wide": "Decimal(30, 10)",
                "amount_round": "Decimal(10, 2)",
                "amount_narrow": "Decimal(5, 2)",
            },
        )
        assert result.rows_written == 2

        col_types = {
            row[0]: row[1]
            for row in ch_client.query(
                f"SELECT name, type FROM system.columns "
                f"WHERE database = currentDatabase() AND table = '{table}'"
            ).result_rows
        }
        assert "Nullable" in col_types["amount_wide"] and "Decimal(30, 10)" in col_types["amount_wide"]
        assert "Nullable" in col_types["amount_round"] and "Decimal(10, 2)" in col_types["amount_round"]
        # amount_narrow is NOT NULL in MySQL but must be forced Nullable in
        # the destination: a Decimal128 value can be coerced to NULL on
        # overflow (see types::may_coerce_to_null).
        assert "Nullable" in col_types["amount_narrow"] and "Decimal(5, 2)" in col_types["amount_narrow"]

        rows = {
            row[0]: (row[1], row[2], row[3])
            for row in ch_client.query(
                f"SELECT id, toString(amount_wide), toString(amount_round), toString(amount_narrow) "
                f"FROM `{table}` ORDER BY id"
            ).result_rows
        }
        assert rows[1] == ("123456789012345.6789012345", "12.35", "12.34")
        assert rows[2][0] is None, "source NULL stays NULL"
        # ClickHouse's toString(Decimal) strips trailing zeros (verified live:
        # toString(toDecimal64('1.0000', 4)) -> "1", not "1.0000") -- the
        # stored value is still exactly 1.00 at scale 2, this is purely a
        # display convention, not a precision loss.
        assert rows[2][1] == "1"
        assert rows[2][2] is None, "12345.67 overflows Decimal(5,2)'s max of 999.99"
    finally:
        _drop_ch(ch_client, table)


def _mysql_scalar(mysql_conn, sql: str):
    with mysql_conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def test_declared_decimal_lands_as_exact_decimal(
    mysql_conn, ch_client, mysql_source, ch_target_zstd, unique_name
):
    """MySQL DECIMAL(P, S) always declares its precision, so it lands as the
    exact Decimal(P, S), signed or UNSIGNED. DECIMAL(65, 30) is past
    Decimal128's 38 digits and stays Float64."""
    table = unique_name
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, amount DECIMAL(15, 2) NOT NULL, "
            "fee DECIMAL(10, 4) UNSIGNED, wide DECIMAL(65, 30))"
        )
        cur.execute(
            f"INSERT INTO `{table}` VALUES (1, 32.90, 0.1250, 1.5), "
            "(2, -1234567890123.45, NULL, NULL)"
        )
    _drop_ch(ch_client, table)
    try:
        quickhouse.sync(
            mysql_source, ch_target_zstd, dest_table=table, source_table=table,
            mode="full", key=["id"], create_if_missing=True,
        )
        types = dict(
            ch_client.query(
                "SELECT name, type FROM system.columns "
                f"WHERE database = currentDatabase() AND table = '{table}'"
            ).result_rows
        )
        assert types["amount"] == "Nullable(Decimal(15, 2))"
        assert types["fee"] == "Nullable(Decimal(10, 4))"
        assert types["wide"] == "Nullable(Float64)"
        rows = ch_client.query(
            f"SELECT id, toString(amount), toString(fee) FROM `{table}` ORDER BY id"
        ).result_rows
        assert rows == [(1, "32.9", "0.125"), (2, "-1234567890123.45", None)]
    finally:
        _drop_ch(ch_client, table)


def test_collapsed_tinyint1_names_the_column_it_flattened(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """MySQL's ``BOOL`` is an alias for ``tinyint(1)``, so display width is the
    only hint quickhouse has — and a schema that doesn't follow that convention
    (Odoo, for one) stores genuine small integers there. Those get flattened to
    0/1, irreversibly, and ``type_overrides`` cannot repair it after the fact.

    The count alone was never enough to act on: "9 columns across 8 tables were
    genuine small integers" is a statement you can only make from per-column
    attribution, which is what this asserts.
    """
    table = unique_name
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"""
            CREATE TABLE `{table}` (
                id       BIGINT PRIMARY KEY,
                x_state  TINYINT(1) NOT NULL,   -- real small integers
                flag     TINYINT(1) NOT NULL,   -- an honest boolean
                write_date DATETIME NOT NULL
            )
            """
        )
        cur.execute(
            f"INSERT INTO `{table}` VALUES "
            "(1, 0, 0, '2024-01-01'), (2, 1, 1, '2024-01-01'), "
            "(3, 2, 0, '2024-01-01'), (4, 3, 1, '2024-01-01'), (5, 7, 0, '2024-01-01')"
        )
    ch_client.command(f"DROP TABLE IF EXISTS `{table}`")
    try:
        result = quickhouse.sync(
            mysql_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="full",
            order_by=["id"],
            create_if_missing=True,
        )
        by_kind = {(w.kind, w.column): w for w in result.warnings}
        # Three values outside {0, 1}, all in x_state.
        assert ("collapsed_bool", "x_state") in by_kind, result.warnings
        assert by_kind[("collapsed_bool", "x_state")].count == 3
        # ...and the honest boolean column is not implicated.
        assert ("collapsed_bool", "flag") not in by_kind, result.warnings
        # The run still succeeded, which is exactly why this has to be data.
        assert result.rows_written == 5

        # Re-reading with the opt-out produces no warning and keeps the values.
        ch_client.command(f"DROP TABLE IF EXISTS `{table}`")
        kept = quickhouse.sync(
            mysql_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="full",
            order_by=["id"],
            create_if_missing=True,
            tinyint1_as_bool=False,
        )
        assert kept.warnings == []
        got = ch_client.query(f"SELECT x_state FROM `{table}` ORDER BY id").result_rows
        assert [r[0] for r in got] == [0, 1, 2, 3, 7]
    finally:
        ch_client.command(f"DROP TABLE IF EXISTS `{table}`")
        with mysql_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")


def _stream_cursor_sync(mysql_source, ch_target, table, **kw):
    """Incremental sync whose MAX probe is skipped (``probe_max_cost=1.0`` on an
    unindexed watermark), so the cursor is taken from the read stream."""
    return quickhouse.sync(
        mysql_source,
        ch_target,
        dest_table=table,
        source_table=table,
        mode="incremental",
        watermark="write_date",
        key=["id"],
        create_if_missing=True,
        engine="ReplacingMergeTree",
        order_by=["id"],
        lookback_seconds=60,
        probe_max_cost=1.0,
        **kw,
    )


def _latest_cursor(ch_client, table):
    return ch_client.command(
        "SELECT last_watermark FROM _quickhouse_state FINAL "
        f"WHERE dest_table = '{table}' ORDER BY run_ts DESC LIMIT 1"
    )


def _add_newer_rows(mysql_conn, table):
    with mysql_conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO `{table}` (id, name, amount, qty, is_active, write_date) "
            f"VALUES (%s, %s, %s, %s, %s, %s)",
            [(i, f"row-{i}", i * 1.5, i, True, "2024-02-01 00:00:00") for i in range(101, 151)],
        )


def test_stream_cursor_is_saved_in_a_form_mysql_can_compare(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """Issue #2: a cursor folded from the read stream was saved as
    ``...+00``, which MySQL can't parse as a DATETIME. The next run's lower
    bound became NULL, it read 0 rows and succeeded, and so did every run after
    it."""
    table = unique_name
    _seed_table(mysql_conn, table, 100, base_ts="2024-01-01 00:00:00")
    _drop_ch(ch_client, table)
    try:
        r1 = _stream_cursor_sync(mysql_source, ch_target, table)
        assert r1.rows_written == 100
        assert "unindexed_watermark" in {w.kind for w in r1.warnings}, r1.warnings
        cursor = _latest_cursor(ch_client, table)
        assert cursor == "2024-01-01 00:00:00.000000", cursor

        _add_newer_rows(mysql_conn, table)
        r2 = _stream_cursor_sync(mysql_source, ch_target, table)
        # The 50 new rows, plus the 100 run-1 rows the 60s lookback re-reads
        # (they sit exactly on the cursor). Before the fix this was 0.
        assert r2.rows_written == 150
        assert int(ch_client.command(f"SELECT count() FROM `{table}` FINAL")) == 150
        assert _latest_cursor(ch_client, table) == "2024-02-01 00:00:00.000000"
    finally:
        _drop_ch(ch_client, table)


def test_a_saved_utc_offset_cursor_is_healed_on_read(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """State written by 0.18-0.20.1 still holds ``...+00`` cursors. Reading one
    back strips the zero offset (without converting it), so a frozen table
    resumes on upgrade with no manual state edit. Covers the bare-literal
    ``lookback_seconds=0`` form as well as the ``CAST(...)`` lookback form."""
    table = unique_name
    _seed_table(mysql_conn, table, 100, base_ts="2024-01-01 00:00:00")
    _drop_ch(ch_client, table)
    try:
        assert _stream_cursor_sync(mysql_source, ch_target, table).rows_written == 100
        state_key = ch_client.command(
            f"SELECT source_table FROM _quickhouse_state WHERE dest_table = '{table}' LIMIT 1"
        )
        for frozen in ("2024-01-01 00:00:00.000000+00", "2024-01-01 00:00:00+00:00"):
            ch_client.command(
                "INSERT INTO _quickhouse_state "
                "(source_table, dest_table, last_watermark, rows, chunk_cursor, chunk_upper) "
                f"VALUES ('{state_key}', '{table}', '{frozen}', 0, '', '')"
            )
            with mysql_conn.cursor() as cur:
                cur.execute(f"DELETE FROM `{table}` WHERE id > 100")
            _add_newer_rows(mysql_conn, table)
            # 50 new rows + the 100 on the cursor that the lookback re-reads.
            assert _stream_cursor_sync(mysql_source, ch_target, table).rows_written == 150, frozen
            assert _latest_cursor(ch_client, table) == "2024-02-01 00:00:00.000000"

        # The MAX-probe path with lookback_seconds=0 compares the bare literal.
        ch_client.command(
            "INSERT INTO _quickhouse_state "
            "(source_table, dest_table, last_watermark, rows, chunk_cursor, chunk_upper) "
            f"VALUES ('{state_key}', '{table}', '2024-01-01 00:00:00.000000+00', 0, '', '')"
        )
        r = quickhouse.sync(
            mysql_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="incremental",
            watermark="write_date",
            key=["id"],
            probe_max_cost=0.0,
        )
        assert r.rows_written == 50
    finally:
        _drop_ch(ch_client, table)


def _column_types(ch_client, table):
    return dict(
        ch_client.query(
            "SELECT name, type FROM system.columns "
            f"WHERE database = currentDatabase() AND table = '{table}'"
        ).result_rows
    )


def test_a_decimal_column_added_to_a_float_table_follows_the_table(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """Issue #5: on a table created under the pre-0.20.1 Float64 default,
    ``evolve_schema`` adds a new DECIMAL(P, S) column as Float64 like its
    siblings, not as the one Decimal in a table of floats."""
    table = unique_name
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, "
            "price DECIMAL(15, 2), discount DECIMAL(15, 2))"
        )
        cur.execute(f"INSERT INTO `{table}` VALUES (1, 32.90, 1.50)")
    _drop_ch(ch_client, table)
    ch_client.command(
        f"CREATE TABLE `{table}` (id Int64, price Nullable(Float64)) "
        "ENGINE = MergeTree ORDER BY id"
    )
    try:
        r = quickhouse.sync(
            mysql_source, ch_target, dest_table=table, source_table=table,
            mode="full", evolve_schema=True,
        )
        assert _column_types(ch_client, table)["discount"] == "Nullable(Float64)"
        assert "decimal_mapping_mixed" not in {w.kind for w in r.warnings}, r.warnings
        rows = ch_client.query(f"SELECT price - discount FROM `{table}`").result_rows
        assert rows == [(31.4,)]
    finally:
        _drop_ch(ch_client, table)


def test_a_table_that_already_mixes_decimals_and_floats_is_reported(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """A table 0.20.1 already left mixed has no single convention to follow: a
    new column keeps the Decimal default, and the run says which columns
    disagree."""
    table = unique_name
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, "
            "price DECIMAL(15, 2), discount DECIMAL(15, 2), tax DECIMAL(15, 2))"
        )
        cur.execute(f"INSERT INTO `{table}` VALUES (1, 32.90, 1.50, 0.25)")
    _drop_ch(ch_client, table)
    ch_client.command(
        f"CREATE TABLE `{table}` (id Int64, price Nullable(Float64), "
        "discount Nullable(Decimal(15, 2))) ENGINE = MergeTree ORDER BY id"
    )
    try:
        r = quickhouse.sync(
            mysql_source, ch_target, dest_table=table, source_table=table,
            mode="full", evolve_schema=True,
        )
        assert _column_types(ch_client, table)["tax"] == "Nullable(Decimal(15, 2))"
        mixed = [w for w in r.warnings if w.kind == "decimal_mapping_mixed"]
        assert len(mixed) == 1, r.warnings
        assert mixed[0].column is None
        assert "discount" in mixed[0].message and "price" in mixed[0].message
    finally:
        _drop_ch(ch_client, table)


def _cursor_warnings(result):
    return [w for w in result.warnings if w.kind.startswith("watermark_")]


def _insert_state(ch_client, table, cursor):
    state_key = ch_client.command(
        f"SELECT source_table FROM _quickhouse_state WHERE dest_table = '{table}' LIMIT 1"
    )
    ch_client.command(
        "INSERT INTO _quickhouse_state "
        "(source_table, dest_table, last_watermark, rows, chunk_cursor, chunk_upper) "
        f"VALUES ('{state_key}', '{table}', '{cursor}', 0, '', '')"
    )
    return state_key


def test_a_lower_bound_that_evaluates_to_null_fails_the_run(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """Issue #3 (A): outside strict mode MySQL turns an unparseable
    ``CAST(... AS DATETIME)`` into NULL, so ``write_date > NULL`` matched
    nothing and the run succeeded with 0 rows, as did every run after it. It
    now fails before reading, naming the state key and the cursor."""
    table = unique_name
    _seed_table(mysql_conn, table, 100, base_ts="2024-01-01 00:00:00")
    _drop_ch(ch_client, table)
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="write_date",
        key=["id"], create_if_missing=True, engine="ReplacingMergeTree", order_by=["id"],
        lookback_seconds=60,
    )
    try:
        assert quickhouse.sync(mysql_source, ch_target, **kw).rows_written == 100
        state_key = _insert_state(ch_client, table, "2024-13-45 00:00:00")
        _add_newer_rows(mysql_conn, table)
        with pytest.raises(Exception, match="evaluates to NULL on the source") as err:
            quickhouse.sync(mysql_source, ch_target, **kw)
        assert f"state_key '{state_key}'" in str(err.value)
        assert "2024-13-45 00:00:00" in str(err.value)
        # Failed, so nothing moved: the bad cursor is still there to repair.
        assert _latest_cursor(ch_client, table) == "2024-13-45 00:00:00"
    finally:
        _drop_ch(ch_client, table)


def test_a_cursor_ahead_of_the_source_is_reported(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """Issue #3 (C): a cursor past the source's MAX (shifted by a time-zone
    conversion, or seeded from another table) is reported. An ordinary quiet
    run, where the MAX equals the cursor, reports nothing."""
    table = unique_name
    _seed_table(mysql_conn, table, 100, base_ts="2024-01-01 00:00:00")
    _drop_ch(ch_client, table)
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="write_date",
        key=["id"], create_if_missing=True, engine="ReplacingMergeTree", order_by=["id"],
        probe_max_cost=0.0,
    )
    try:
        assert quickhouse.sync(mysql_source, ch_target, **kw).rows_written == 100
        quiet = quickhouse.sync(mysql_source, ch_target, **kw)
        assert quiet.rows_written == 0
        assert _cursor_warnings(quiet) == [], quiet.warnings

        _insert_state(ch_client, table, "2024-01-02 00:00:00")
        r = quickhouse.sync(mysql_source, ch_target, **kw)
        assert r.rows_written == 0
        (ahead,) = _cursor_warnings(r)
        assert ahead.kind == "watermark_ahead_of_source"
        assert ahead.column == "write_date"
        assert ahead.sample == "2024-01-02 00:00:00"
        # The probe's MAX is saved as the cursor, so the next run is quiet.
        assert _latest_cursor(ch_client, table) == "2024-01-01 00:00:00"
        assert _cursor_warnings(quickhouse.sync(mysql_source, ch_target, **kw)) == []
    finally:
        _drop_ch(ch_client, table)


def test_chunk_rows_takes_a_source_querys_keyset_nullability_from_mysql(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """Issue #7, MySQL half: the result metadata of a source_query carries
    NOT_NULL_FLAG per column, cleared on the nullable side of a LEFT JOIN, so
    a NOT NULL primary key is accepted for chunk_rows and a keyset the query
    can null is refused."""
    table, other = unique_name, f"{unique_name}_b"
    _seed_table(mysql_conn, table, 100)
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{other}`")
        cur.execute(f"CREATE TABLE `{other}` (id BIGINT PRIMARY KEY, k BIGINT)")
        cur.execute(f"INSERT INTO `{other}` SELECT id, id FROM `{table}`")
    kw = dict(
        dest_table=table, mode="incremental", watermark="write_date", key=["id"],
        create_if_missing=True, engine="ReplacingMergeTree", order_by=["id"], chunk_rows=30,
    )
    _drop_ch(ch_client, table)
    try:
        projected = f"SELECT id, CAST(name AS CHAR) AS name, write_date FROM `{table}`"
        r = quickhouse.sync(mysql_source, ch_target, source_query=projected, **kw)
        assert r.rows_written == 100

        _drop_ch(ch_client, table)
        nullable_side = (
            f"SELECT t.id, t.write_date FROM `{other}` o LEFT JOIN `{table}` t ON t.id = o.k"
        )
        with pytest.raises(Exception, match="keyset_not_null=True"):
            quickhouse.sync(mysql_source, ch_target, source_query=nullable_side, **kw)
    finally:
        _drop_ch(ch_client, table)
        with mysql_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{other}`")


def test_an_indexed_watermark_is_probed_not_swept(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """MySQL plans MAX() over an indexed column as "Select tables optimized
    away", with neither a cost nor an access path. quickhouse read that plan as
    a probe it could not price: every incremental run on a primary key or an
    indexed column reported unindexed_watermark and swept the whole key range
    in windows, opening a connection for each."""
    table = unique_name
    _seed_table(mysql_conn, table, 100)
    _drop_ch(ch_client, table)
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="id",
        key=["id"], create_if_missing=True, engine="ReplacingMergeTree", order_by=["id"],
    )
    try:
        first = quickhouse.sync(mysql_source, ch_target, **kw)
        assert first.rows_written == 100
        _add_newer_rows(mysql_conn, table)
        second = quickhouse.sync(mysql_source, ch_target, **kw)
        assert second.rows_written == 50
        for r in (first, second):
            assert "unindexed_watermark" not in {w.kind for w in r.warnings}, r.warnings
    finally:
        _drop_ch(ch_client, table)
        with mysql_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")


@contextlib.contextmanager
def _server_time_zone(zone: str):
    """Give new connections a default time zone of `zone`, as on a server
    configured for local time. Needs root, which docker-compose.yml has."""
    pymysql = pytest.importorskip("pymysql")
    try:
        root = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT, user="root",
            password=MYSQL_ROOT_PASSWORD, autocommit=True,
        )
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"changing the server time zone needs MySQL root: {e}")
    try:
        with root.cursor() as cur:
            cur.execute("SELECT @@GLOBAL.time_zone")
            (before,) = cur.fetchone()
            cur.execute("SET GLOBAL time_zone = %s", (zone,))
        try:
            yield
        finally:
            with root.cursor() as cur:
                cur.execute("SET GLOBAL time_zone = %s", (before,))
    finally:
        root.close()


def test_a_timestamp_lands_shifted_unless_the_session_is_utc(
    mysql_conn, ch_client, ch_target, unique_name
):
    """MySQL renders a TIMESTAMP in the session's time zone, and quickhouse
    stores that wall-clock time as UTC. On a server at +07:00, the instant
    01:00 UTC landed as 08:00 UTC, silently. The run now names the column, and
    utc_session=True reads the instant itself. A column overridden to naive
    DATETIME asked for the wall-clock time, so it is not reported; a DATETIME
    column carries no zone and reads the same either way."""
    table = unique_name
    instant = int(dt.datetime(2026, 9, 30, 1, tzinfo=dt.timezone.utc).timestamp())
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, at TIMESTAMP(6) NULL, "
            "local_at TIMESTAMP(6) NULL, civil DATETIME(6) NULL)"
        )
        # FROM_UNIXTIME and the TIMESTAMP store convert through the same
        # session zone, so both columns hold the instant whatever that zone is.
        cur.execute(
            f"INSERT INTO `{table}` VALUES "
            "(1, FROM_UNIXTIME(%s), FROM_UNIXTIME(%s), '2026-09-30 01:00:00')",
            (instant, instant),
        )
    _drop_ch(ch_client, table)

    def run(source):
        result = quickhouse.sync(
            source, ch_target, dest_table=table, source_table=table, mode="full",
            key=["id"], create_if_missing=True, type_overrides={"local_at": "DATETIME"},
        )
        landed = ch_client.query(
            f"SELECT toString(at), toString(local_at), toString(civil) FROM `{table}`"
        ).result_rows
        shifted = {(w.column, w.sample) for w in result.warnings if w.kind == "shifted_timestamp"}
        return landed, shifted

    try:
        with _server_time_zone("+07:00"):
            landed, shifted = run(quickhouse.MySQL(MYSQL_DSN))
            assert landed == [(
                "2026-09-30 08:00:00.000000",
                "2026-09-30 08:00:00.000000",
                "2026-09-30 01:00:00.000000",
            )]
            assert shifted == {("at", "UTC+07:00")}

            landed, shifted = run(quickhouse.MySQL(MYSQL_DSN, utc_session=True))
            assert landed == [(
                "2026-09-30 01:00:00.000000",
                "2026-09-30 01:00:00.000000",
                "2026-09-30 01:00:00.000000",
            )]
            assert shifted == set()
    finally:
        _drop_ch(ch_client, table)
        with mysql_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")


def test_an_all_null_watermark_is_reported_on_every_run(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """Issue #15: when every row's watermark is NULL there is no cursor to
    save, so each run re-reads the whole table. The NULL-count probe that
    raises null_watermark is skipped as too costly on a big table
    (``probe_max_cost=1.0`` stands in for that here), so the read itself has
    to say so, on every run."""
    table = unique_name
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, updated_date DATETIME NULL)"
        )
        cur.executemany(
            f"INSERT INTO `{table}` (id) VALUES (%s)", [(i,) for i in range(1, 201)]
        )
    _drop_ch(ch_client, table)
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="updated_date",
        # No version column: ReplacingMergeTree(updated_date) can't hold a NULL.
        key=["id"], create_if_missing=True, engine="ReplacingMergeTree()", order_by=["id"],
    )

    def nulls(result):
        return [(w.column, w.count) for w in result.warnings if w.kind == "null_watermark"]

    try:
        for _ in range(2):
            r = quickhouse.sync(
                mysql_source, ch_target, lookback_seconds=60, probe_max_cost=1.0, **kw
            )
            assert r.rows_read == 200, "no cursor, so every run reads it all"
            assert r.new_watermark is None
            assert nulls(r) == [("updated_date", 200)], r.warnings

        # With the probe affordable it reports the column itself, and the
        # read doesn't add a second count on top of it.
        r = quickhouse.sync(mysql_source, ch_target, probe_max_cost=0.0, **kw)
        assert r.rows_read == 200
        assert nulls(r) == [("updated_date", 200)], r.warnings
    finally:
        _drop_ch(ch_client, table)
        with mysql_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")


def test_a_first_read_into_a_replacing_merge_tree_leaves_out_null_watermarks(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """Issue #27, MySQL: with the MAX probe skipped, a first read has no bound
    and read rows whose watermark is NULL, which a ReplacingMergeTree's
    version column can't hold: the insert failed. It leaves them out now, as
    a read bounded by the MAX does."""
    table = unique_name
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, updated_date DATETIME NULL)"
        )
        cur.executemany(
            f"INSERT INTO `{table}` (id, updated_date) VALUES (%s, %s)",
            [(i, None if 150 <= i <= 160 else "2024-01-01 00:00:00") for i in range(1, 201)],
        )
    _drop_ch(ch_client, table)
    try:
        r = quickhouse.sync(
            mysql_source, ch_target, dest_table=table, source_table=table,
            mode="incremental", watermark="updated_date", key=["id"], create_if_missing=True,
            lookback_seconds=60, probe_max_cost=1.0,
        )
        assert r.rows_written == 189
        assert {w.kind for w in r.warnings} & {"null_watermark", "null_check_skipped"}
    finally:
        _drop_ch(ch_client, table)
        with mysql_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")


def test_a_source_query_that_filters_out_the_newest_rows_raises_no_false_warning(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """Issue #23: a source_query that filters out the newest rows has a MAX
    below a correct cursor. That is no reason to warn about a shifted cursor
    when source_table shows the cursor is within the table. The cursor still
    goes back to that MAX, as it always has: a re-read at most, where keeping
    a cursor that really is ahead would skip rows."""
    table = unique_name
    _seed_table(mysql_conn, table, 100, base_ts="2024-01-01 00:00:00")
    with mysql_conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO `{table}` (id, name, write_date) VALUES (%s, %s, %s)",
            [(i, f"row-{i}", "2024-03-01 00:00:00") for i in range(101, 111)],
        )
    _drop_ch(ch_client, table)
    query = f"SELECT id, name, write_date FROM `{table}` WHERE id <= 100"
    kw = dict(
        dest_table=table, source_query=query, mode="incremental", watermark="write_date",
        key=["id"], create_if_missing=True, engine="ReplacingMergeTree", order_by=["id"],
        probe_max_cost=0.0, state_key=f"{table}:filtered",
    )
    try:
        assert quickhouse.sync(mysql_source, ch_target, source_table=table, **kw).rows_written == 100

        # Past the filtered MAX (01-01), within the table's (03-01).
        _insert_state(ch_client, table, "2024-02-01 00:00:00")
        r = quickhouse.sync(mysql_source, ch_target, source_table=table, **kw)
        assert _cursor_warnings(r) == [], r.warnings
        assert _latest_cursor(ch_client, table) == "2024-01-01 00:00:00"

        # Without source_table nothing tells the filter from a shifted cursor:
        # the warning names the filter as a possible cause.
        _insert_state(ch_client, table, "2024-02-01 00:00:00")
        r = quickhouse.sync(mysql_source, ch_target, **kw)
        (ahead,) = _cursor_warnings(r)
        assert ahead.kind == "watermark_ahead_of_source"
        assert "source_query" in ahead.message and "set source_table" in ahead.message.lower()
        assert _latest_cursor(ch_client, table) == "2024-01-01 00:00:00"

        # Past the table's own MAX: a real cursor problem, handled as before.
        _insert_state(ch_client, table, "2024-04-01 00:00:00")
        r = quickhouse.sync(mysql_source, ch_target, source_table=table, **kw)
        (ahead,) = _cursor_warnings(r)
        assert ahead.kind == "watermark_ahead_of_source"
        assert "saves the source's MAX" in ahead.message
        assert _latest_cursor(ch_client, table) == "2024-01-01 00:00:00"
    finally:
        _drop_ch(ch_client, table)
        with mysql_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")


def test_a_sweep_longer_than_the_lookback_rereads_rows_updated_mid_sweep(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """Issue #13: a windowed sweep reads one window after another. A row in a
    window already read is updated mid-sweep, then a row in a later window,
    and the stream cursor lands on the later update. Once the sweep outlasts
    lookback_seconds, the next run's lower bound is past the first update and
    that row was never read again. The cursor now moves back by what the sweep
    took beyond the lookback, so the next run reads it."""
    pymysql = pytest.importorskip("pymysql")
    table = unique_name
    rows = 6_000
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, "
            "updated_date DATETIME(6) NOT NULL, payload VARCHAR(32) NOT NULL)"
        )
        cur.executemany(
            f"INSERT INTO `{table}` VALUES (%s, '2026-01-01 00:00:00', 'v0')",
            [(i,) for i in range(1, rows + 1)],
        )
    _drop_ch(ch_client, table)
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="updated_date",
        key=["id"], create_if_missing=True, engine="ReplacingMergeTree()", order_by=["id"],
        lookback_seconds=1, state_key=f"{table}:updated_date", parallelism=1,
        # Stands in for a MAX(updated_date) too costly to probe: the read is
        # swept, and the cursor comes from the rows read.
        probe_max_cost=1.0,
        read_window_rows=300,
    )
    # 20 windows of 300 keys at 1,000 rows/s: a ~6 s sweep. The pace is kept
    # per full batch, so batches have to be smaller than a window.
    slow = dict(read_max_rows_per_sec=1_000, batch_rows=50)

    def payload(i):
        return ch_client.command(f"SELECT payload FROM `{table}` FINAL WHERE id = {i}")

    def update(i, value):
        # Its own connection: the fixture's is not safe to share across threads.
        conn = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD,
            database=MYSQL_DB, autocommit=True,
        )
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE `{table}` SET updated_date = NOW(6), payload = %s WHERE id = %s",
                (value, i),
            )
        conn.close()

    try:
        quickhouse.sync(mysql_source, ch_target, **kw)
        result = {}
        sweep = threading.Thread(
            target=lambda: result.update(r=quickhouse.sync(mysql_source, ch_target, **kw, **slow))
        )
        sweep.start()
        time.sleep(1.5)
        update(100, "v1-early")  # its window was read in the first half second
        time.sleep(1.5)
        update(rows - 10, "v1-late")  # its window is read last
        sweep.join()
        assert payload(rows - 10) == "v1-late", "the sweep should have read the late update"
        assert result["r"].stage_secs > 3, result["r"].stage_secs

        quickhouse.sync(mysql_source, ch_target, **kw)
        assert payload(100) == "v1-early"
    finally:
        _drop_ch(ch_client, table)
        with mysql_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")


def _connections(mysql_conn) -> int:
    """MySQL's count of connection attempts since start: the difference across
    a sync is how many connections it opened."""
    with mysql_conn.cursor() as cur:
        cur.execute("SHOW GLOBAL STATUS LIKE 'Connections'")
        return int(cur.fetchone()[1])


def _new_parts(ch_client, table) -> int:
    ch_client.command("SYSTEM FLUSH LOGS")
    return ch_client.command(
        "SELECT count() FROM system.part_log WHERE database = currentDatabase() "
        f"AND table = '{table}' AND event_type = 'NewPart'"
    )


def test_an_indexed_watermark_with_many_nulls_is_read_in_one_pass(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name, capfd
):
    """Issue #16, case 1: on an indexed, nullable watermark with mostly NULLs,
    MAX comes from the index while the NULL count prices above the gate. The
    costly count used to report unindexed_watermark and sweep a read that was a
    range scan. Now only the skipped count is reported."""
    table = unique_name
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, updated_date DATETIME NULL, "
            "KEY (updated_date))"
        )
        cur.executemany(
            f"INSERT INTO `{table}` VALUES (%s, %s)",
            [(i, "2026-01-01 00:00:00" if i <= 100 else None) for i in range(1, 2001)],
        )
    _drop_ch(ch_client, table)
    try:
        capfd.readouterr()
        r = quickhouse.sync(
            mysql_source, ch_target, dest_table=table, source_table=table,
            mode="incremental", watermark="updated_date", key=["id"],
            create_if_missing=True, engine="ReplacingMergeTree()", order_by=["id"],
            probe_max_cost=1.0, read_window_rows=100, parallelism=1,
        )
        kinds = {w.kind for w in r.warnings}
        assert "unindexed_watermark" not in kinds, r.warnings
        assert "null_check_skipped" in kinds, r.warnings
        assert "windowed read on" not in capfd.readouterr().err
        # `WHERE updated_date <= MAX` matches no NULL, as ever.
        assert r.rows_written == 100
    finally:
        _drop_ch(ch_client, table)
        with mysql_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")


def _seed_filtered(mysql_conn, table, ids):
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, updated_date DATETIME NOT NULL, "
            "is_test TINYINT NOT NULL, payload VARCHAR(32) NOT NULL, KEY (updated_date))"
        )
        _add_filtered(mysql_conn, table, ids)


def _add_filtered(mysql_conn, table, ids, is_test=0):
    with mysql_conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO `{table}` VALUES (%s, "
            "TIMESTAMPADD(SECOND, %s, '2026-01-01 00:00:00'), %s, 'p')",
            [(i, i % 100_000, is_test) for i in ids],
        )


def test_a_filtered_source_query_probes_max_on_the_table(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name, capfd
):
    """Issue #16, case 2 with both set: MAX(id) through `WHERE is_test = 0`
    can't come from the primary key, so it priced as a scan and switched on a
    sweep of a read that is a range scan. With source_table set the MAX is the
    table's own. It only bounds the read, though: the cursor is the largest id
    actually read, so a test row the filter excludes (id 1,000,000,000) can't
    carry the cursor past rows not read yet."""
    table = unique_name
    _seed_filtered(mysql_conn, table, range(1, 3001))
    _add_filtered(mysql_conn, table, [1_000_000_000], is_test=1)
    _drop_ch(ch_client, table)
    kw = dict(
        dest_table=table, source_table=table,
        source_query=f"SELECT id, updated_date, payload FROM `{table}` WHERE is_test = 0",
        mode="incremental", watermark="id", key=["id"], create_if_missing=True,
        engine="ReplacingMergeTree()", order_by=["id"], probe_max_cost=1.0,
        read_window_rows=100, parallelism=1, state_key=f"{table}:pk",
    )
    try:
        capfd.readouterr()
        r = quickhouse.sync(mysql_source, ch_target, **kw)
        assert r.rows_written == 3000
        assert r.new_watermark == "3000", "the cursor is what was read, not the table's MAX"
        assert "unindexed_watermark" not in {w.kind for w in r.warnings}, r.warnings
        assert "windowed read on" not in capfd.readouterr().err

        _add_filtered(mysql_conn, table, range(3001, 3011))
        r = quickhouse.sync(mysql_source, ch_target, **kw)
        assert r.rows_written == 10, "rows past the cursor but below the junk row still land"
        assert r.new_watermark == "3010"
        assert ch_client.command(f"SELECT count() FROM `{table}` FINAL") == 3010
    finally:
        _drop_ch(ch_client, table)
        with mysql_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")


def test_a_key_watermark_through_a_filtered_query_is_not_swept_once_it_has_a_cursor(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name, capfd
):
    """Issue #16, case 2 with a primary-key watermark and only source_query:
    MAX(id) through the filter is a scan, and with lookback_seconds=0 it runs
    anyway. The sweep added a second full scan for its key bounds, every run.
    With a cursor the read is `id > cursor`, the very range a window bounds,
    so a run pays for the MAX at most."""
    table = unique_name
    _seed_filtered(mysql_conn, table, range(1, 2001))
    _drop_ch(ch_client, table)
    kw = dict(
        dest_table=table,
        source_query=f"SELECT id, updated_date, payload FROM `{table}` WHERE is_test = 0",
        mode="incremental", watermark="id", key=["id"], create_if_missing=True,
        engine="ReplacingMergeTree()", order_by=["id"], probe_max_cost=1.0,
        read_window_rows=100, parallelism=1, state_key=f"{table}:pk",
    )
    try:
        assert quickhouse.sync(mysql_source, ch_target, **kw).rows_written == 2000
        _add_filtered(mysql_conn, table, range(2001, 2021))
        capfd.readouterr()
        r = quickhouse.sync(mysql_source, ch_target, **kw)
        assert r.rows_written == 20
        assert "windowed read on" not in capfd.readouterr().err
        unindexed = next(w for w in r.warnings if w.kind == "unindexed_watermark")
        assert "set source_table as well" in unindexed.message, unindexed.message
    finally:
        _drop_ch(ch_client, table)
        with mysql_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")


def test_key_watermark_partitions_split_the_rows_above_the_cursor(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name, capfd
):
    """Issue #24: with the watermark as the partition key, the partitions split
    the table's whole [MIN, MAX], so every new row landed in the last one and
    the others opened a connection to read nothing. They now split what is
    above the cursor. A steady-state run also opens no connection beyond the
    setup one and the read: the lower-bound check runs on the setup's."""
    table = unique_name
    _seed_table(mysql_conn, table, 100)
    _drop_ch(ch_client, table)
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="id",
        key=["id"], create_if_missing=True, engine="ReplacingMergeTree", order_by=["id"],
    )
    try:
        quickhouse.sync(mysql_source, ch_target, parallelism=2, **kw)
        _add_newer_rows(mysql_conn, table)  # ids 101..150
        capfd.readouterr()
        r = quickhouse.sync(mysql_source, ch_target, parallelism=2, **kw)
        assert r.rows_written == 50
        err = capfd.readouterr().err
        assert "partition 'range-0' complete: 25 rows" in err, err
        assert "partition 'range-1' complete: 25 rows" in err, err

        before = _connections(mysql_conn)
        r = quickhouse.sync(mysql_source, ch_target, parallelism=1, **kw)
        assert r.rows_written == 0
        assert _connections(mysql_conn) - before == 2, "setup + read, nothing else"
    finally:
        _drop_ch(ch_client, table)


def test_a_windowed_sweep_shares_one_connection_and_one_insert(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """Issue #20: each window of a sweep opened its own connection and flushed
    its own insert, so 18 updated rows spread over the key range cost 20
    MySQL connections and 18 ClickHouse parts where a single pass took 4 and
    2. The windows now share one connection and one insert buffer."""
    table = unique_name
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, updated_date DATETIME(6) NOT NULL)"
        )
        cur.executemany(
            f"INSERT INTO `{table}` VALUES (%s, TIMESTAMPADD(SECOND, %s, '2026-01-01'))",
            [(i, i) for i in range(1, 2001)],
        )
    _drop_ch(ch_client, table)
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="updated_date",
        key=["id"], create_if_missing=True, engine="ReplacingMergeTree()", order_by=["id"],
        lookback_seconds=1, probe_max_cost=1.0, read_window_rows=100, parallelism=1,
    )
    try:
        quickhouse.sync(mysql_source, ch_target, **kw)
        with mysql_conn.cursor() as cur:
            cur.execute(
                f"UPDATE `{table}` SET updated_date = NOW(6) WHERE id IN "
                f"({', '.join(str(i) for i in range(50, 2000, 110))})"
            )
        parts, conns = _new_parts(ch_client, table), _connections(mysql_conn)
        r = quickhouse.sync(mysql_source, ch_target, **kw)
        assert r.rows_written >= 18
        assert _connections(mysql_conn) - conns == 2, "setup + one for the whole sweep"
        assert _new_parts(ch_client, table) - parts == 1, "one insert for the whole sweep"
    finally:
        _drop_ch(ch_client, table)
        with mysql_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")


def test_fail_on_warnings_stops_the_cursor_and_the_swap(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """Issue #17: raising on a warning after sync() returns can't stop the
    cursor, which is saved by then, so a retry starts past the rows the
    warning was about. fail_on_warnings stops the run before that."""
    table = unique_name
    _seed_table(mysql_conn, table, 10)
    _drop_ch(ch_client, table)
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="write_date",
        key=["id"], create_if_missing=True, engine="ReplacingMergeTree", order_by=["id"],
        probe_max_cost=0.0, state_key=f"{table}:wd",
    )

    def cursor():
        return ch_client.command(
            "SELECT last_watermark FROM _quickhouse_state FINAL "
            f"WHERE source_table = '{table}:wd' ORDER BY run_ts DESC LIMIT 1"
        )

    try:
        quickhouse.sync(mysql_source, ch_target, **kw)
        before = cursor()
        with mysql_conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO `{table}` (id, name, is_active, write_date) "
                "VALUES (11, 'row-11', 2, '2024-03-01 00:00:00')"
            )
        with pytest.raises(RuntimeError, match="fail_on_warnings: collapsed_bool") as err:
            quickhouse.sync(mysql_source, ch_target, fail_on_warnings={"collapsed_bool"}, **kw)
        assert "column 'is_active'" in str(err.value)
        assert "cursor was not saved" in str(err.value)
        assert cursor() == before, "the cursor did not move"
        # Once the cause is accepted, the next run reads the same range again.
        r = quickhouse.sync(mysql_source, ch_target, **kw)
        assert r.rows_written == 1
        assert {w.kind for w in r.warnings} == {"collapsed_bool"}

        # A full refresh stops before its swap: the destination is untouched.
        with mysql_conn.cursor() as cur:
            cur.execute(f"UPDATE `{table}` SET name = 'changed' WHERE id = 1")
        # The incremental runs left an unmerged copy of row 11, which the
        # shrink guard counts.
        full = dict(kw, mode="full", allow_full_refresh_shrink=True)
        with pytest.raises(RuntimeError, match="untouched"):
            quickhouse.sync(mysql_source, ch_target, fail_on_warnings=["collapsed_bool"], **full)
        assert ch_client.command(f"SELECT name FROM `{table}` FINAL WHERE id = 1") == "row-1"

        with pytest.raises(RuntimeError, match="unknown warning kind"):
            quickhouse.sync(mysql_source, ch_target, fail_on_warnings={"collapsed"}, **kw)
    finally:
        _drop_ch(ch_client, table)


def _seed_wide(mysql_conn, table, rows):
    """`rows` rows of 1 KB each: wide enough that MySQL is still sending the
    result while a throttled read drains it, so killing the connection
    breaks the read rather than the idle socket after it."""
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, payload VARCHAR(1024) NOT NULL)"
        )
        cur.execute("SET SESSION cte_max_recursion_depth = 1000000")
        cur.execute(
            f"INSERT INTO `{table}` WITH RECURSIVE seq (n) AS "
            f"(SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < {rows}) "
            "SELECT n, REPEAT('x', 1000) FROM seq"
        )


def _kill_reader_after(mysql_conn, delay):
    """Kill quickhouse's read connection after `delay` seconds, as a replica
    restart would, from a thread of its own: every connection of the test
    user but this test's own."""
    pymysql = pytest.importorskip("pymysql")
    with mysql_conn.cursor() as cur:
        cur.execute("SELECT CONNECTION_ID()")
        (fixture,) = cur.fetchone()

    def kill():
        time.sleep(delay)
        conn = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD,
            database=MYSQL_DB, autocommit=True,
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM information_schema.processlist "
                "WHERE user = %s AND id NOT IN (%s, CONNECTION_ID())",
                (MYSQL_USER, fixture),
            )
            killed.extend(thread for (thread,) in cur.fetchall())
            for thread in killed:
                cur.execute(f"KILL CONNECTION {thread}")
        conn.close()

    killed = []
    t = threading.Thread(target=kill)
    t.start()
    return t, killed


def test_a_retry_into_a_plain_mergetree_writes_each_row_once(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """Issue #14: a retried attempt re-inserts what the failed one wrote, and a
    plain MergeTree keeps every copy: 280,000 rows for 150,000 ids, with
    rows_written saying 150,000. Retries now stage each attempt for such an
    engine, so the failed one leaves nothing behind."""
    table = unique_name
    rows = 40_000
    _seed_wide(mysql_conn, table, rows)
    _drop_ch(ch_client, table)
    ch_client.command(
        f"CREATE TABLE `{table}` (id Int64, payload String) ENGINE = MergeTree ORDER BY id"
    )
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="id",
        key=["id"], create_if_missing=True, parallelism=1, batch_rows=1000, insert_bytes=0,
        read_max_rows_per_sec=10_000, retry_max_attempts=3, state_key=f"{table}:pk",
    )
    try:
        killer, killed = _kill_reader_after(mysql_conn, 1.5)
        r = quickhouse.sync(mysql_source, ch_target, **kw)
        killer.join()
        assert killed, "the read should have been cut off part-way"
        got = ch_client.query(f"SELECT count(), uniqExact(id) FROM `{table}`").result_rows[0]
        assert got == (rows, rows), got
        assert r.rows_written == rows
        assert r.rows_written_failed_attempts == 0
        assert "retried_after_partial_write" not in {w.kind for w in r.warnings}
    finally:
        _drop_ch(ch_client, table)
        with mysql_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")


def test_a_retry_after_a_partial_write_says_so(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """Issue #14, ReplacingMergeTree: the copies a retry writes collapse at the
    next merge, but count() runs high until then and rows_written covered the
    last attempt only. The retry is now reported, with the rows."""
    table = unique_name
    rows = 40_000
    _seed_wide(mysql_conn, table, rows)
    _drop_ch(ch_client, table)
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="id",
        key=["id"], create_if_missing=True, engine="ReplacingMergeTree", order_by=["id"],
        parallelism=1, batch_rows=1000, insert_bytes=0, read_max_rows_per_sec=10_000,
        retry_max_attempts=3, state_key=f"{table}:pk",
    )
    try:
        killer, killed = _kill_reader_after(mysql_conn, 1.5)
        r = quickhouse.sync(mysql_source, ch_target, **kw)
        killer.join()
        assert killed, "the read should have been cut off part-way"
        (retried,) = [w for w in r.warnings if w.kind == "retried_after_partial_write"]
        assert r.rows_written == rows
        assert 0 < r.rows_written_failed_attempts < rows
        assert retried.count == r.rows_written_failed_attempts
        assert ch_client.command(f"SELECT uniqExact(id) FROM `{table}` FINAL") == rows
    finally:
        _drop_ch(ch_client, table)
        with mysql_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")


def test_a_table_max_never_becomes_the_cursor_of_an_excluded_watermark(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    """Review of #16: with the watermark left out of the transfer there is no
    read to take the cursor from, and source_table's unfiltered MAX is no
    cursor: a test row the filter excludes, dated 2099, would carry it past
    every real row. source_query's own MAX is probed instead."""
    table = unique_name
    _seed_filtered(mysql_conn, table, range(1, 101))
    with mysql_conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO `{table}` VALUES (1000000, '2099-01-01 00:00:00', 1, 'junk')"
        )
    _drop_ch(ch_client, table)
    kw = dict(
        dest_table=table, source_table=table,
        source_query=f"SELECT id, updated_date, payload FROM `{table}` WHERE is_test = 0",
        mode="incremental", watermark="updated_date", key=["id"], create_if_missing=True,
        engine="ReplacingMergeTree()", order_by=["id"], probe_max_cost=1.0,
        exclude=["updated_date"], state_key=f"{table}:ud", parallelism=1,
    )
    try:
        r = quickhouse.sync(mysql_source, ch_target, **kw)
        assert r.rows_written == 100
        assert r.new_watermark is not None and not r.new_watermark.startswith("2099"), (
            r.new_watermark
        )
        _add_filtered(mysql_conn, table, range(101, 111))
        assert quickhouse.sync(mysql_source, ch_target, **kw).rows_written >= 10
    finally:
        _drop_ch(ch_client, table)
        with mysql_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
