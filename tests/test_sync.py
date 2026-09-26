"""End-to-end integration tests: PostgreSQL -> ClickHouse.

Run against the services in ``docker-compose.yml`` after building the module:

    docker compose up -d
    pip install -e '.[test]'
    maturin develop --release
    pytest -v
"""

from __future__ import annotations

from datetime import datetime, timedelta

import quickhouse


def _seed_table(pg_conn, table: str, rows: int, base_ts: str = "2024-01-01 00:00:00"):
    """Create and populate a table with mixed types + a NULL column."""
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(
            f"""
            CREATE TABLE "{table}" (
                id          bigint PRIMARY KEY,
                name        text,
                amount      double precision,
                qty         integer,
                is_active   boolean,
                note        text,          -- left NULL to exercise Nullable
                write_date  timestamp NOT NULL
            )
            """
        )
        with cur.copy(
            f'COPY "{table}" (id, name, amount, qty, is_active, write_date) FROM STDIN'
        ) as copy:
            for i in range(1, rows + 1):
                copy.write_row((i, f"row-{i}", i * 1.5, i, i % 2 == 0, base_ts))


def _drop_ch(ch_client, table: str):
    ch_client.command(f"DROP TABLE IF EXISTS `{table}`")
    ch_client.command(f"DROP TABLE IF EXISTS `{table}_quickhouse_tmp`")


def test_full_refresh_reconciles(pg_conn, ch_client, pg_source, ch_target, unique_name):
    table = unique_name
    n = 5000
    _seed_table(pg_conn, table, n)
    _drop_ch(ch_client, table)
    try:
        result = quickhouse.sync(
            pg_source,
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
        pg_sum = _pg_scalar(pg_conn, f'SELECT sum(amount) FROM "{table}"')
        ch_sum = float(ch_client.command(f"SELECT sum(amount) FROM `{table}`"))
        assert abs(pg_sum - ch_sum) < 1e-6

        # NULL column round-trips.
        ch_nulls = ch_client.command(f"SELECT countIf(note IS NULL) FROM `{table}`")
        assert int(ch_nulls) == n
    finally:
        _drop_ch(ch_client, table)


def test_full_refresh_coerces_out_of_range_dates_to_null(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Regression test: a valid PostgreSQL date/timestamp whose year is outside
    ClickHouse's Date32/DateTime64 window ([1900-01-01, 2299-12-31]) used to
    abort the whole transfer at insert time (VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE).
    PostgreSQL's date range is far wider than ClickHouse's, so this is reachable
    with ordinary data; out-of-range values must coerce to NULL and complete."""
    table = unique_name
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(
            f"""
            CREATE TABLE "{table}" (
                id          bigint PRIMARY KEY,
                event_date  date,
                event_at    timestamp
            )
            """
        )
        with cur.copy(f'COPY "{table}" (id, event_date, event_at) FROM STDIN') as copy:
            copy.write_row((1, "2024-05-01", "2024-05-01 10:00:00"))  # in range
            copy.write_row((2, "3000-01-01", "3000-01-01 00:00:00"))  # far future
            copy.write_row((3, "1000-01-01", "1000-01-01 00:00:00"))  # far past
    _drop_ch(ch_client, table)
    try:
        result = quickhouse.sync(
            pg_source,
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
        null_ts = ch_client.command(f"SELECT countIf(event_at IS NULL) FROM `{table}`")
        assert int(null_ts) == 2

        valid = ch_client.command(f"SELECT event_date FROM `{table}` WHERE id = 1")
        assert str(valid) == "2024-05-01"
    finally:
        _drop_ch(ch_client, table)


def test_time_column_stored_as_text(pg_conn, ch_client, pg_source, ch_target, unique_name):
    """PostgreSQL TIME maps to a ClickHouse String and round-trips as canonical
    HH:MM:SS[.ffffff] text. Previously the column carried an Arrow Time64
    physical type into a String destination, which stored a bogus value."""
    table = unique_name
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, t time, t2 time)')
        with cur.copy(f'COPY "{table}" (id, t, t2) FROM STDIN') as copy:
            copy.write_row((1, "10:30:00", "23:59:59.123456"))
            copy.write_row((2, "00:00:00", None))
    _drop_ch(ch_client, table)
    try:
        result = quickhouse.sync(
            pg_source,
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
            f"WHERE database = currentDatabase() AND table = '{table}' AND name = 't'"
        )
        assert "String" in col_type

        assert str(ch_client.command(f"SELECT t FROM `{table}` WHERE id = 1")) == "10:30:00"
        assert str(ch_client.command(f"SELECT t2 FROM `{table}` WHERE id = 1")) == "23:59:59.123456"
        assert int(ch_client.command(f"SELECT countIf(t2 IS NULL) FROM `{table}`")) == 1
    finally:
        _drop_ch(ch_client, table)


def test_uuid_column_round_trips(pg_conn, ch_client, pg_source, ch_target, unique_name):
    """Binary COPY sends a uuid as 16 raw bytes. It used to be decoded as UTF-8
    text, so any table with a uuid column failed with "invalid utf8"."""
    table = unique_name
    ids = ["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "ffffffff-ffff-ffff-ffff-ffffffffffff"]
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id uuid PRIMARY KEY, other uuid)')
        cur.execute(f'INSERT INTO "{table}" VALUES (%s, %s), (%s, NULL)', (ids[0], ids[0], ids[1]))
    _drop_ch(ch_client, table)
    try:
        result = quickhouse.sync(
            pg_source, ch_target, dest_table=table, source_table=table, mode="full", key=["id"]
        )
        assert result.rows_written == 2
        rows = ch_client.query(
            f"SELECT toString(id), toString(other) FROM `{table}` ORDER BY id"
        ).result_rows
        assert rows == [(ids[0], ids[0]), (ids[1], None)]
    finally:
        _drop_ch(ch_client, table)


def test_column_transforms_are_decoded_in_their_own_type(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """A transformed column used to keep its source column's type for decoding.
    A jsonb extraction lost its first character, a numeric rounding of a float8
    column came out as a denormal, and a time cast to text was read as
    microseconds — all silently."""
    table = unique_name
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(
            f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, payload jsonb, ratio float8, t time)'
        )
        cur.execute(
            f"""INSERT INTO "{table}" VALUES (1, '{{"user_id": "12345"}}', 0.1234, '14:30:00')"""
        )
    _drop_ch(ch_client, table)
    try:
        quickhouse.sync(
            pg_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="full",
            key=["id"],
            column_transforms={
                "payload": "payload->>'user_id'",
                "ratio": "ROUND(ratio::numeric, 2)",
                "t": "CAST(t AS TEXT)",
            },
        )
        row = ch_client.query(f"SELECT payload, ratio, t FROM `{table}`").result_rows[0]
        assert row == ("12345", 0.12, "14:30:00")
    finally:
        _drop_ch(ch_client, table)


def test_decimal_with_more_digits_than_fit_is_rounded_not_nulled(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """An unconstrained numeric with 46 significant digits fits Decimal(38, 9)
    once rounded, but decoding used to add up every digit first, overflow, and
    land the value as NULL."""
    table = unique_name
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, v numeric)')
        cur.execute(
            f'INSERT INTO "{table}" VALUES (1, (\'1.\' || repeat(\'6\', 45))::numeric), (2, 2.5)'
        )
    _drop_ch(ch_client, table)
    try:
        quickhouse.sync(
            pg_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="full",
            key=["id"],
            type_overrides={"v": "Decimal(38, 9)"},
        )
        rows = ch_client.query(f"SELECT id, toString(v) FROM `{table}` ORDER BY id").result_rows
        assert rows == [(1, "1.666666667"), (2, "2.5")]
    finally:
        _drop_ch(ch_client, table)


def test_full_refresh_zstd_with_tight_memory_budget(
    pg_conn, ch_client, pg_source, ch_target_zstd, unique_name
):
    """zstd compression (the new default codec) reconciles exactly, and a
    deliberately tight memory ceiling at high parallelism still completes —
    exercising the streaming-compressed upload path and the MemoryBudget
    backpressure together."""
    table = unique_name
    n = 20000
    _seed_table(pg_conn, table, n)
    _drop_ch(ch_client, table)
    try:
        result = quickhouse.sync(
            pg_source,
            ch_target_zstd,
            dest_table=table,
            source_table=table,
            mode="full",
            key=["id"],
            create_if_missing=True,
            parallelism=8,
            batch_rows=1000,
            # 2 MiB ceiling forces backpressure across the 8 partitions.
            max_memory_bytes=2 * 1024 * 1024,
        )
        assert result.rows_written == n

        ch_count = ch_client.command(f"SELECT count() FROM `{table}`")
        assert int(ch_count) == n

        pg_sum = _pg_scalar(pg_conn, f'SELECT sum(amount) FROM "{table}"')
        ch_sum = float(ch_client.command(f"SELECT sum(amount) FROM `{table}`"))
        assert abs(pg_sum - ch_sum) < 1e-3
    finally:
        _drop_ch(ch_client, table)


def test_incremental_appends_and_is_idempotent(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    table = unique_name
    _seed_table(pg_conn, table, 100, base_ts="2024-01-01 00:00:00")
    _drop_ch(ch_client, table)
    try:
        # First incremental run backfills everything (no prior watermark).
        r1 = quickhouse.sync(
            pg_source,
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
        with pg_conn.cursor() as cur:
            with cur.copy(
                f'COPY "{table}" (id, name, amount, qty, is_active, write_date) FROM STDIN'
            ) as copy:
                for i in range(101, 151):
                    copy.write_row((i, f"row-{i}", i * 1.5, i, True, "2024-02-01 00:00:00"))

        r2 = quickhouse.sync(
            pg_source,
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
            pg_source,
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


def test_incremental_lookback_reprocesses_row_whose_watermark_moved_forward(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """lookback_seconds widens the tracked watermark's lower bound so a run
    re-includes a trailing window of already-synced rows -- the mechanism
    that makes a "resync the last N days" pattern safe. Simulates a row
    whose write_date moves forward (a late edit) but stays behind the
    previous run's high-water mark, so a plain incremental rerun's
    `write_date > last` filter never sees it; lookback_seconds must catch it,
    and ReplacingMergeTree/FINAL must upsert it rather than duplicate it."""
    table = unique_name
    base = datetime(2024, 1, 1)
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(
            f"""
            CREATE TABLE "{table}" (
                id          bigint PRIMARY KEY,
                amount      double precision,
                write_date  timestamp NOT NULL
            )
            """
        )
        with cur.copy(f'COPY "{table}" (id, amount, write_date) FROM STDIN') as copy:
            for i in range(1, 101):
                copy.write_row((i, i * 1.5, base + timedelta(seconds=i)))
    _drop_ch(ch_client, table)
    try:
        r1 = quickhouse.sync(
            pg_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="incremental",
            watermark="write_date",
            key=["id"],
            create_if_missing=True,
            engine="ReplacingMergeTree",
            order_by=["id"],
        )
        assert r1.rows_written == 100

        # id=1's write_date moves forward (1s -> 60s) but stays well behind
        # the persisted high-water mark (100s, from id=100).
        with pg_conn.cursor() as cur:
            cur.execute(
                f'UPDATE "{table}" SET amount = 99999.0, write_date = %s WHERE id = 1',
                (base + timedelta(seconds=60),),
            )

        r2 = quickhouse.sync(
            pg_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="incremental",
            watermark="write_date",
            key=["id"],
        )
        assert r2.rows_written == 0, "without lookback the moved-forward-but-still-behind row is invisible"
        stale = ch_client.command(f"SELECT amount FROM `{table}` FINAL WHERE id = 1")
        assert float(stale) == 1.5, "the update hasn't propagated yet"

        r3 = quickhouse.sync(
            pg_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="incremental",
            watermark="write_date",
            lookback_seconds=45,
            key=["id"],
        )
        # Lower bound becomes (last=100s) - 45s = 55s: re-reads ids 56..100
        # (45 rows, write_date 56s..100s) plus id=1 (now at 60s) = 46 rows.
        assert r3.rows_written == 46

        total = ch_client.command(f"SELECT count() FROM `{table}` FINAL")
        assert int(total) == 100, "upserted, not duplicated"
        updated = ch_client.command(f"SELECT amount FROM `{table}` FINAL WHERE id = 1")
        assert float(updated) == 99999.0
        unaffected = ch_client.command(f"SELECT amount FROM `{table}` FINAL WHERE id = 60")
        assert float(unaffected) == 90.0
    finally:
        _drop_ch(ch_client, table)


def test_column_mapping(pg_conn, ch_client, pg_source, ch_target, unique_name):
    table = unique_name
    _seed_table(pg_conn, table, 10)
    _drop_ch(ch_client, table)
    try:
        quickhouse.sync(
            pg_source,
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
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Regression test (bug report): `type_overrides={"col": "Decimal(P,S)"}`
    previously only changed the destination DDL type -- the value itself
    still went through a lossy Float64 round-trip before ever reaching
    ClickHouse, silently corrupting exact NUMERIC values beyond ~15-17
    significant digits despite the destination column being correctly typed.
    Also covers PostgreSQL numeric's NaN/Infinity/-Infinity sentinels (only
    NaN was previously even checked) and precision overflow on a NOT NULL
    column (proving it's forced nullable, so the coercion doesn't abort the
    whole transfer with an Arrow schema-consistency error)."""
    table = unique_name
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        # amount_wide is a bare (unconstrained) numeric: PostgreSQL rejects
        # Infinity/-Infinity outright in a precision/scale-constrained
        # numeric(P,S) column ("numeric field overflow ... cannot hold an
        # infinite value") even though NaN is accepted there -- confirmed
        # live against this project's postgres:16 container.
        cur.execute(
            f"""
            CREATE TABLE "{table}" (
                id            bigint PRIMARY KEY,
                amount_wide   numeric,
                amount_narrow numeric(10, 2) NOT NULL
            )
            """
        )
        cur.execute(
            f"""
            INSERT INTO "{table}" (id, amount_wide, amount_narrow) VALUES
                (1, 123456789012345.6789012345, 12.34),
                (2, 'NaN', 1.00),
                (3, 'Infinity', 2.00),
                (4, '-Infinity', 3.00),
                (5, NULL, 12345.67)
            """
        )
    _drop_ch(ch_client, table)
    try:
        result = quickhouse.sync(
            pg_source,
            ch_target,
            dest_table=table,
            source_table=table,
            mode="full",
            key=["id"],
            create_if_missing=True,
            type_overrides={"amount_wide": "Decimal(30, 10)", "amount_narrow": "Decimal(5, 2)"},
        )
        assert result.rows_written == 5

        col_types = {
            row[0]: row[1]
            for row in ch_client.query(
                f"SELECT name, type FROM system.columns "
                f"WHERE database = currentDatabase() AND table = '{table}'"
            ).result_rows
        }
        assert "Nullable" in col_types["amount_wide"] and "Decimal(30, 10)" in col_types["amount_wide"]
        # amount_narrow is NOT NULL in Postgres but must be forced Nullable in
        # the destination: a Decimal128 value can be coerced to NULL on
        # overflow (see types::may_coerce_to_null).
        assert "Nullable" in col_types["amount_narrow"] and "Decimal(5, 2)" in col_types["amount_narrow"]

        rows = {
            row[0]: (row[1], row[2])
            for row in ch_client.query(
                f"SELECT id, toString(amount_wide), toString(amount_narrow) FROM `{table}` ORDER BY id"
            ).result_rows
        }
        assert rows[1] == ("123456789012345.6789012345", "12.34")
        assert rows[2][0] is None, "NaN must coerce to NULL"
        assert rows[3][0] is None, "Infinity must coerce to NULL"
        assert rows[4][0] is None, "-Infinity must coerce to NULL"
        assert rows[5][0] is None, "source NULL stays NULL"
        assert rows[5][1] is None, "12345.67 overflows Decimal(5,2)'s max of 999.99"
    finally:
        _drop_ch(ch_client, table)


def _staging_orphans(ch_client, table: str) -> list[str]:
    """Any leftover `{table}_quickhouse_tmp*` staging tables in the current DB.
    (`table` is a test-generated unique_name, so f-string interpolation here is
    safe from injection.)"""
    return [
        row[0]
        for row in ch_client.query(
            f"SELECT name FROM system.tables "
            f"WHERE database = currentDatabase() AND name LIKE '{table}_quickhouse_tmp%' ORDER BY name"
        ).result_rows
    ]


def test_full_refresh_leaves_no_staging_orphan(pg_conn, ch_client, pg_source, ch_target, unique_name):
    """A successful full refresh must drop its per-run staging table — with
    per-run-unique staging names (bug 10 fix) nothing else ever reclaims them,
    so a leak here would accumulate forever."""
    table = unique_name
    _seed_table(pg_conn, table, 100)
    _drop_ch(ch_client, table)
    try:
        quickhouse.sync(
            pg_source, ch_target, dest_table=table, source_table=table,
            mode="full", key=["id"], create_if_missing=True, parallelism=4,
        )
        orphans = _staging_orphans(ch_client, table)
        assert orphans == [], f"staging table(s) left behind after a successful run: {orphans}"
    finally:
        _drop_ch(ch_client, table)
        for name in _staging_orphans(ch_client, table):
            ch_client.command(f"DROP TABLE IF EXISTS `{name}`")


def test_rapid_successive_full_refreshes_both_succeed(pg_conn, ch_client, pg_source, ch_target, unique_name):
    """Two back-to-back full refreshes into the same destination both succeed
    and reconcile. This is the pattern bug 10 broke on BigQuery (fixed-name
    staging drop+recreate blocked streaming on the immediate second run);
    ClickHouse shares the exact staging-name code path, so this exercises the
    per-run-unique-name plumbing end-to-end against a real sink."""
    table = unique_name
    _drop_ch(ch_client, table)
    try:
        _seed_table(pg_conn, table, 100)
        r1 = quickhouse.sync(
            pg_source, ch_target, dest_table=table, source_table=table,
            mode="full", key=["id"], create_if_missing=True,
        )
        assert r1.rows_written == 100

        # Immediate second run (different data) into the same dest.
        _seed_table(pg_conn, table, 150)
        r2 = quickhouse.sync(
            pg_source, ch_target, dest_table=table, source_table=table,
            mode="full", key=["id"], create_if_missing=True,
        )
        assert r2.rows_written == 150
        assert int(ch_client.command(f"SELECT count() FROM `{table}`")) == 150
        assert _staging_orphans(ch_client, table) == []
    finally:
        _drop_ch(ch_client, table)
        for name in _staging_orphans(ch_client, table):
            ch_client.command(f"DROP TABLE IF EXISTS `{name}`")


def _pg_scalar(pg_conn, sql: str):
    with pg_conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]
