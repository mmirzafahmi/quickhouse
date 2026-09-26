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

import pytest

import quickhouse


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
