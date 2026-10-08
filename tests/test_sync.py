"""End-to-end integration tests: PostgreSQL -> ClickHouse.

Run against the services in ``docker-compose.yml`` after building the module:

    docker compose up -d
    pip install -e '.[test]'
    maturin develop --release
    pytest -v
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

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


def test_rows_of_every_size_survive_copy_chunking(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """PostgreSQL sends a binary COPY as one message per row, and quickhouse
    regroups the messages into multi-row chunks before decoding them. Every row
    must still arrive byte-for-byte, on both PostgreSQL read paths (one-shot and
    keyset-chunked): short rows, NULLs, and rows bigger than a whole chunk (a
    300 KiB value is a single message over the 256 KiB chunk ceiling)."""
    table = unique_name
    n = 20_000
    big = {i: f"{i:08d}" + "x" * (300 * 1024) for i in (1, 2, 5_000, 12_345, n)}

    def body(i):
        return big.get(i) or (None if i % 7 == 0 else f"row-{i}")

    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(
            f'CREATE TABLE "{table}" '
            "(id bigint PRIMARY KEY, body text, write_date timestamp NOT NULL)"
        )
        with cur.copy(f'COPY "{table}" (id, body, write_date) FROM STDIN') as copy:
            for i in range(1, n + 1):
                copy.write_row((i, body(i), "2024-01-01 00:00:00"))
    expected = [(i, body(i)) for i in range(1, n + 1)]

    read_paths = {
        "one_shot": dict(mode="full", parallelism=2),
        "keyset": dict(
            mode="incremental",
            watermark="write_date",
            chunk_rows=3_000,
            engine="ReplacingMergeTree",
            order_by=["id"],
        ),
    }
    for label, kwargs in read_paths.items():
        dest = f"{table}_{label}"
        _drop_ch(ch_client, dest)
        try:
            result = quickhouse.sync(
                pg_source,
                ch_target,
                dest_table=dest,
                source_table=table,
                key=["id"],
                create_if_missing=True,
                **kwargs,
            )
            assert result.rows_written == n, label
            got = ch_client.query(f"SELECT id, body FROM `{dest}` ORDER BY id").result_rows
            assert got == expected, label
        finally:
            _drop_ch(ch_client, dest)


def _ch_types(ch_client, table: str) -> dict:
    return dict(
        ch_client.query(
            "SELECT name, type FROM system.columns "
            f"WHERE database = currentDatabase() AND table = '{table}'"
        ).result_rows
    )


def _seed_numeric_table(pg_conn, table: str):
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(
            f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, price numeric(15, 2), ratio numeric)'
        )
        cur.execute(
            f'INSERT INTO "{table}" VALUES (1, 32.90, 0.1), (2, 1234567890123.45, 2.5), '
            "(3, NULL, NULL)"
        )


def test_declared_numeric_lands_as_exact_decimal(
    pg_conn, ch_client, pg_source, ch_target_zstd, unique_name
):
    """A numeric(P, S) column lands as the exact Decimal(P, S): ClickHouse holds
    32.9, not the 32.89999999999999 a Float64 round-trip produces. An
    unconstrained numeric carries no precision, so it stays Float64."""
    table = unique_name
    _seed_numeric_table(pg_conn, table)
    _drop_ch(ch_client, table)
    try:
        quickhouse.sync(
            pg_source, ch_target_zstd, dest_table=table, source_table=table,
            mode="full", key=["id"], create_if_missing=True,
        )
        types = _ch_types(ch_client, table)
        assert types["price"] == "Nullable(Decimal(15, 2))"
        assert types["ratio"] == "Nullable(Float64)"
        rows = ch_client.query(f"SELECT id, toString(price) FROM `{table}` ORDER BY id").result_rows
        assert rows == [(1, "32.9"), (2, "1234567890123.45"), (3, None)]
    finally:
        _drop_ch(ch_client, table)


def test_existing_float_column_keeps_its_type(
    pg_conn, ch_client, pg_source, ch_target_zstd, unique_name
):
    """A table an earlier version created holds numeric(P, S) as Float64. Later
    runs keep writing floats into it instead of changing its type underneath
    whatever reads it; on BigQuery a decimal payload aimed at a FLOAT64 column
    would be rejected outright."""
    table = unique_name
    _seed_numeric_table(pg_conn, table)
    _drop_ch(ch_client, table)
    ch_client.command(
        f"CREATE TABLE `{table}` (id Int64, price Nullable(Float64), ratio Nullable(Float64)) "
        "ENGINE = ReplacingMergeTree ORDER BY id"
    )
    try:
        for mode in ("full", "incremental"):
            quickhouse.sync(
                pg_source, ch_target_zstd, dest_table=table, source_table=table,
                mode=mode, key=["id"], watermark="id" if mode == "incremental" else None,
            )
            assert _ch_types(ch_client, table)["price"] == "Nullable(Float64)", mode
        total = ch_client.command(f"SELECT round(sum(price), 2) FROM `{table}` FINAL")
        assert float(total) == 1234567890156.35
    finally:
        _drop_ch(ch_client, table)


def test_numeric_as_decimal_float64_restores_the_old_mapping(
    pg_conn, ch_client, pg_source, ch_target_zstd, unique_name
):
    table = unique_name
    _seed_numeric_table(pg_conn, table)
    _drop_ch(ch_client, table)
    try:
        quickhouse.sync(
            pg_source, ch_target_zstd, dest_table=table, source_table=table,
            mode="full", key=["id"], create_if_missing=True, numeric_as_decimal="Float64",
        )
        assert _ch_types(ch_client, table)["price"] == "Nullable(Float64)"
    finally:
        _drop_ch(ch_client, table)


def test_lz4_compressed_inserts_land_intact(
    pg_conn, ch_client, pg_source, ch_target_lz4, unique_name
):
    table = unique_name
    n = 5000
    _seed_table(pg_conn, table, n)
    _drop_ch(ch_client, table)
    try:
        result = quickhouse.sync(
            pg_source, ch_target_lz4, dest_table=table, source_table=table,
            mode="full", key=["id"], create_if_missing=True,
        )
        assert result.rows_written == n
        assert int(ch_client.command(f"SELECT count() FROM `{table}`")) == n
        pg_sum = _pg_scalar(pg_conn, f'SELECT sum(amount) FROM "{table}"')
        ch_sum = float(ch_client.command(f"SELECT sum(amount) FROM `{table}`"))
        assert abs(pg_sum - ch_sum) < 1e-6
    finally:
        _drop_ch(ch_client, table)


def _pg_scalar(pg_conn, sql: str):
    with pg_conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def test_quiet_incremental_runs_raise_no_cursor_warnings(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Issue #3's checks compare the MAX probe with the saved cursor. A run with
    nothing new must say nothing, including on a ``timestamptz``, whose MAX
    renders with the session's offset, and with a lookback."""
    table = unique_name
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(
            f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, write_date timestamptz NOT NULL)'
        )
        cur.execute(
            f"INSERT INTO \"{table}\" SELECT g, '2024-01-01 00:00:00+00' "
            "FROM generate_series(1, 10) g"
        )
    for lookback in (0, 3600):
        dest = f"{table}_{lookback}"
        _drop_ch(ch_client, dest)
        kw = dict(
            dest_table=dest, source_table=table, mode="incremental", watermark="write_date",
            key=["id"], create_if_missing=True, engine="ReplacingMergeTree", order_by=["id"],
            lookback_seconds=lookback, probe_max_cost=0.0,
        )
        try:
            assert quickhouse.sync(pg_source, ch_target, **kw).rows_written == 10
            quiet = quickhouse.sync(pg_source, ch_target, **kw)
            kinds = {w.kind for w in quiet.warnings}
            assert not {"watermark_not_advanced", "watermark_ahead_of_source"} & kinds, (
                lookback,
                quiet.warnings,
            )
        finally:
            _drop_ch(ch_client, dest)


def _chunked_query_sync(pg_source, ch_target, dest, query, **kw):
    return quickhouse.sync(
        pg_source,
        ch_target,
        dest_table=dest,
        source_query=query,
        mode="incremental",
        watermark="write_date",
        key=["id"],
        create_if_missing=True,
        engine="ReplacingMergeTree",
        order_by=["id"],
        chunk_rows=40,
        **kw,
    )


def test_chunk_rows_accepts_a_source_query_whose_keyset_is_a_not_null_column(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Issue #7: every column of a PostgreSQL source_query resolves as
    nullable, which refused chunk_rows outright. A keyset that is a plain
    reference to a NOT NULL column, in a query with no outer join, is now
    proven from the query itself, and chunks like source_table does."""
    table = unique_name
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(
            f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, qty numeric(12, 3), '
            "write_date timestamptz NOT NULL)"
        )
        cur.execute(
            f"INSERT INTO \"{table}\" SELECT g, g / 7.0, '2024-01-01 00:00:00+00' "
            "FROM generate_series(1, 250) g"
        )
    query = (
        f'SELECT "id", CAST(ROUND("qty", 9) AS TEXT) AS "qty", '
        f'("write_date" AT TIME ZONE \'UTC\') AS "write_date" FROM "{table}"'
    )
    _drop_ch(ch_client, table)
    try:
        r = _chunked_query_sync(pg_source, ch_target, table, query)
        assert r.rows_written == 250
        assert int(ch_client.command(f"SELECT count() FROM `{table}` FINAL")) == 250
        # Committed a marker per chunk on the way, and cleared it at the end.
        assert ch_client.command(
            f"SELECT chunk_cursor FROM _quickhouse_state FINAL WHERE dest_table = '{table}' "
            "ORDER BY run_ts DESC LIMIT 1"
        ) == ""
    finally:
        _drop_ch(ch_client, table)
        with pg_conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')


def test_chunk_rows_refuses_a_keyset_a_query_can_null_unless_asserted(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """PostgreSQL reports a column's table straight through the nullable side
    of a LEFT JOIN and through GROUPING SETS, so the NOT NULL constraint alone
    proves nothing there: both stay refused, and the error names the explicit
    assertion, which is then accepted."""
    table, other = unique_name, f"{unique_name}_b"
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}", "{other}"')
        cur.execute(
            f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, write_date timestamp NOT NULL)'
        )
        cur.execute(f'CREATE TABLE "{other}" (id bigint PRIMARY KEY, k bigint)')
        cur.execute(
            f"INSERT INTO \"{table}\" SELECT g, '2024-01-01' FROM generate_series(1, 50) g"
        )
        cur.execute(f'INSERT INTO "{other}" SELECT g, g FROM generate_series(1, 50) g')
    nullable_side = (
        f'SELECT t.id, t.write_date FROM "{other}" o LEFT JOIN "{table}" t ON t.id = o.k'
    )
    grouping_sets = (
        f'SELECT id, max(write_date) AS write_date FROM "{table}" '
        "GROUP BY GROUPING SETS ((id), ())"
    )
    _drop_ch(ch_client, table)
    try:
        for query in (nullable_side, grouping_sets):
            with pytest.raises(Exception, match="keyset_not_null=True"):
                _chunked_query_sync(pg_source, ch_target, table, query)
        # Every o.k matches, so asserting it is true here.
        r = _chunked_query_sync(pg_source, ch_target, table, nullable_side, keyset_not_null=True)
        assert r.rows_written == 50
    finally:
        _drop_ch(ch_client, table)
        with pg_conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}", "{other}"')


def test_numeric_decodes_to_the_correctly_rounded_float(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Issue #8: an unconstrained numeric landed one ulp off the nearest
    double (-0.03 as -0.030000000000000002). It now equals PostgreSQL's own
    ::float8, bit for bit."""
    table = unique_name
    values = ["-0.03", "0.12", "-0.06", "32.9", "0.1", "123456789.123456789", "1e-20"]
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id int PRIMARY KEY, v numeric)')
        for i, v in enumerate(values):
            cur.execute(f'INSERT INTO "{table}" VALUES (%s, %s::numeric)', (i, v))
        cur.execute(f'SELECT id, v::float8 FROM "{table}" ORDER BY id')
        expected = cur.fetchall()
    _drop_ch(ch_client, table)
    try:
        quickhouse.sync(
            pg_source, ch_target, dest_table=table, source_table=table, mode="full",
            key=["id"], create_if_missing=True,
        )
        got = ch_client.query(f"SELECT id, v FROM `{table}` ORDER BY id").result_rows
        assert [(i, float(v)) for i, v in got] == expected
    finally:
        _drop_ch(ch_client, table)


def test_portable_datetime_overrides_keep_microseconds_on_clickhouse(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Issue #10: type_overrides {"c": "DATETIME"} created ClickHouse's
    second-precision DateTime alias and dropped the microseconds. The portable
    names now create DateTime64(6) / DateTime64(6, 'UTC'); ClickHouse's own
    DateTime still means seconds."""
    table = unique_name
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(
            f'CREATE TABLE "{table}" (id int PRIMARY KEY, a timestamp, b timestamp, c timestamp)'
        )
        cur.execute(
            f"INSERT INTO \"{table}\" VALUES (1, '2026-09-21 17:21:00.002835', "
            "'2026-09-21 17:21:00.002835', '2026-09-21 17:21:00.002835')"
        )
    _drop_ch(ch_client, table)
    try:
        quickhouse.sync(
            pg_source, ch_target, dest_table=table, source_table=table, mode="full",
            key=["id"], create_if_missing=True,
            type_overrides={"a": "DATETIME", "b": "TIMESTAMP", "c": "DateTime"},
        )
        types = _ch_types(ch_client, table)
        assert types["a"] == "Nullable(DateTime64(6))", types
        assert types["b"] == "Nullable(DateTime64(6, 'UTC'))", types
        assert types["c"] == "Nullable(DateTime)", types
        row = ch_client.query(
            f"SELECT toString(a), toString(b), toString(c) FROM `{table}`"
        ).result_rows[0]
        assert row == (
            "2026-09-21 17:21:00.002835",
            "2026-09-21 17:21:00.002835",
            "2026-09-21 17:21:00",
        )
    finally:
        _drop_ch(ch_client, table)


def test_an_engine_version_column_from_a_source_query_is_created_non_nullable(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Issue #11: every source_query column resolves as nullable, so a
    computed ReplacingMergeTree(ver) column was declared Nullable and the
    CREATE failed with Code 169. It is now forced non-nullable, as a sort key
    is, with or without an explicit not_null."""
    table = unique_name
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id int PRIMARY KEY, write_date timestamp)')
        cur.execute(f"INSERT INTO \"{table}\" VALUES (1, '2024-01-01'), (2, NULL)")
    query = (
        "SELECT id, (COALESCE(write_date, TIMESTAMP '1987-12-01') + INTERVAL '7 hours') "
        f'AS write_date FROM "{table}"'
    )
    try:
        for i, extra in enumerate(({}, {"not_null": ["write_date"]})):
            dest = f"{table}_{i}"
            _drop_ch(ch_client, dest)
            r = quickhouse.sync(
                pg_source, ch_target, dest_table=dest, source_query=query, mode="incremental",
                watermark="id", key=["id"], order_by=["id"], create_if_missing=True,
                engine="ReplacingMergeTree(`write_date`)", **extra,
            )
            assert r.rows_written == 2
            assert _ch_types(ch_client, dest)["write_date"] == "DateTime64(6)", extra
    finally:
        for i in range(2):
            _drop_ch(ch_client, f"{table}_{i}")
        with pg_conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')


def test_a_chunk_marker_repeated_from_an_earlier_read_still_lands(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Regression: a replicated ClickHouse table drops an insert whose block it
    has seen before. A chunk marker written while re-reading a window repeats,
    byte for byte, the marker an earlier read of that window wrote, so it was
    dropped: the latest state row stayed the earlier run's cleared marker, and
    the retry after a failure started over from the first chunk. Every state
    write now carries a dedup token of its own.

    ClickHouse Cloud leaves a column default out of the block's hash; a local
    MergeTree doesn't, so `run_ts` (a default) would keep every row distinct
    here. This state table pins `run_ts` to a constant and turns on a dedup
    window, which makes the local server drop a repeated row the way Cloud does.
    """
    table = unique_name
    state = f"{table}_state"
    n, chunk = 1000, 100
    _seed_table(pg_conn, table, n)
    _drop_ch(ch_client, table)
    ch_client.command(f"DROP TABLE IF EXISTS `{state}`")
    ch_client.command(
        f"CREATE TABLE `{state}` (source_table String, dest_table String, "
        f"last_watermark String, rows UInt64, chunk_cursor String DEFAULT '', "
        f"chunk_upper String DEFAULT '', "
        f"run_ts DateTime64(3) DEFAULT toDateTime64('2000-01-01 00:00:00', 3)) "
        f"ENGINE = ReplacingMergeTree(run_ts) ORDER BY (source_table, dest_table) "
        f"SETTINGS non_replicated_deduplication_window = 1000"
    )
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="id",
        key=["id"], chunk_rows=chunk, advance_watermark=False, create_if_missing=True,
        engine="ReplacingMergeTree", order_by=["id"], state_table_name=state,
    )
    try:
        # A full read of the window, then a second one that fails in chunk 5 of
        # 10, after committing the same 4 markers the first read did.
        assert quickhouse.sync(pg_source, ch_target, **kw).rows_read == n
        ch_client.command(f"ALTER TABLE `{table}` ADD CONSTRAINT stop_here CHECK id <= 450")
        with pytest.raises(RuntimeError, match="stop_here"):
            quickhouse.sync(pg_source, ch_target, **kw)
        ch_client.command(f"ALTER TABLE `{table}` DROP CONSTRAINT stop_here")

        assert quickhouse.sync(pg_source, ch_target, **kw).rows_read == n - 400, (
            "the retry must resume after the 4 committed chunks, not start over"
        )
    finally:
        _drop_ch(ch_client, table)
        ch_client.command(f"DROP TABLE IF EXISTS `{state}`")


def test_a_rolling_source_query_filter_raises_no_false_warning(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Issue #23, PostgreSQL: a quiet table read through a source_query whose
    filter has dropped the newest rows. Its MAX sits below the saved cursor,
    which raised watermark_ahead_of_source about time zones (10-01 against
    09-21 in production). With source_table set the cursor is checked against
    the unfiltered table: no warning. It still goes back to the MAX, which
    costs a re-read at most."""
    table = unique_name
    _seed_table(pg_conn, table, 100, base_ts="2024-01-01 00:00:00")
    with pg_conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO \"{table}\" (id, name, write_date) "
            "SELECT g, 'new', '2024-03-01 00:00:00' FROM generate_series(101, 110) g"
        )
    _drop_ch(ch_client, table)
    state_key = f"{table}:rolling"
    kw = dict(
        dest_table=table, source_table=table,
        source_query=f'SELECT id, name, write_date FROM "{table}" WHERE id <= 100',
        mode="incremental", watermark="write_date", key=["id"], create_if_missing=True,
        engine="ReplacingMergeTree", order_by=["id"], probe_max_cost=0.0,
        lookback_seconds=3600, state_key=state_key,
    )

    def cursor():
        return ch_client.command(
            "SELECT last_watermark FROM _quickhouse_state FINAL "
            f"WHERE source_table = '{state_key}' ORDER BY run_ts DESC LIMIT 1"
        )

    try:
        assert quickhouse.sync(pg_source, ch_target, **kw).rows_written == 100
        ch_client.command(
            "INSERT INTO _quickhouse_state "
            "(source_table, dest_table, last_watermark, rows, chunk_cursor, chunk_upper) "
            f"VALUES ('{state_key}', '{table}', '2024-02-01 00:00:00', 0, '', '')"
        )
        for _ in range(2):
            r = quickhouse.sync(pg_source, ch_target, **kw)
            assert not [w for w in r.warnings if w.kind.startswith("watermark_")], r.warnings
            assert cursor() == "2024-01-01 00:00:00"
    finally:
        _drop_ch(ch_client, table)


def _chunked_stream_table(pg_conn, ch_client, table):
    """200 rows keyed by an integer primary key, with a watermark no index
    serves, so its MAX prices as a scan and the cursor comes from the rows
    read."""
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(
            f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, v int, write_date timestamp NOT NULL)'
        )
        cur.execute(
            f"INSERT INTO \"{table}\" SELECT g, 0, '2024-01-01' FROM generate_series(1, 200) g"
        )
    _drop_ch(ch_client, table)


def _latest_state(ch_client, state_key):
    return ch_client.query(
        "SELECT last_watermark, chunk_cursor, chunk_upper FROM _quickhouse_state FINAL "
        f"WHERE source_table = '{state_key}' ORDER BY run_ts DESC LIMIT 1"
    ).result_rows[0]


def test_a_resumed_chunked_stream_read_saves_the_cursor_its_marker_froze(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Review of #13: a chunked read whose cursor comes from the rows read
    (MAX too costly), interrupted, and resumed by a later sync. The resume
    reads only the chunks after the marker, so a cursor taken from them would
    pass over a row in an earlier chunk that changed in between: id=30 below,
    updated to 02-01 while id=180, read by the resume, went to 03-01. Each
    marker records the cursor the interrupted read could have saved, and the
    resume reads up to it and saves it."""
    table = unique_name
    _chunked_stream_table(pg_conn, ch_client, table)
    state_key = f"{table}:chunked"
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="write_date",
        key=["id"], create_if_missing=True, engine="ReplacingMergeTree", order_by=["id"],
        chunk_rows=50, probe_max_cost=1.0, lookback_seconds=60, state_key=state_key,
    )
    try:
        assert quickhouse.sync(pg_source, ch_target, **kw).rows_written == 200
        committed = _latest_state(ch_client, state_key)[0]
        # Interrupted in its third chunk, after committing two.
        ch_client.command(f"ALTER TABLE `{table}` ADD CONSTRAINT stop_here CHECK id <= 100")
        with pytest.raises(RuntimeError, match="stop_here"):
            quickhouse.sync(pg_source, ch_target, **kw)
        ch_client.command(f"ALTER TABLE `{table}` DROP CONSTRAINT stop_here")
        last, marker, upper = _latest_state(ch_client, state_key)
        assert (last, marker) == (committed, "100")
        assert upper == "2024-01-01 00:00:00.000000", "the cursor the read could have saved"

        with pg_conn.cursor() as cur:
            cur.execute(f"UPDATE \"{table}\" SET v = 1, write_date = '2024-02-01' WHERE id = 30")
            cur.execute(f"UPDATE \"{table}\" SET v = 1, write_date = '2024-03-01' WHERE id = 180")
        r = quickhouse.sync(pg_source, ch_target, **kw)
        assert r.rows_read == 99, "the resume reads past the marker, up to its bound"
        assert r.new_watermark == upper
        assert _latest_state(ch_client, state_key) == (upper, "", "")

        quickhouse.sync(pg_source, ch_target, **kw)
        assert ch_client.command(f"SELECT sum(v) FROM `{table}` FINAL WHERE id IN (30, 180)") == 2, (
            "the row changed in a chunk the interrupted read had committed lands, and so does "
            "the one past the resume's bound"
        )
    finally:
        _drop_ch(ch_client, table)


def test_a_resume_past_a_marker_with_no_bound_keeps_the_committed_cursor(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """A marker written by 0.20.6, or before the interrupted read saw a
    watermark, records no bound for a stream-derived cursor. The resume can't
    tell what changed in the chunks before it, so it keeps the committed
    cursor, and the next run reads everything changed since."""
    table = unique_name
    _chunked_stream_table(pg_conn, ch_client, table)
    state_key = f"{table}:chunked"
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="write_date",
        key=["id"], create_if_missing=True, engine="ReplacingMergeTree", order_by=["id"],
        chunk_rows=50, probe_max_cost=1.0, lookback_seconds=60, state_key=state_key,
    )
    try:
        assert quickhouse.sync(pg_source, ch_target, **kw).rows_written == 200
        committed = _latest_state(ch_client, state_key)[0]
        with pg_conn.cursor() as cur:
            cur.execute(f"UPDATE \"{table}\" SET v = 1, write_date = '2024-02-01' WHERE id = 30")
            cur.execute(f"UPDATE \"{table}\" SET v = 1, write_date = '2024-03-01' WHERE id = 180")
        ch_client.command(
            "INSERT INTO _quickhouse_state "
            "(source_table, dest_table, last_watermark, rows, chunk_cursor, chunk_upper) "
            f"VALUES ('{state_key}', '{table}', '{committed}', 100, '100', '')"
        )
        r = quickhouse.sync(pg_source, ch_target, **kw)
        assert r.rows_read == 100
        assert r.new_watermark is None, "the cursor stays where it was committed"
        assert _latest_state(ch_client, state_key) == (committed, "", ""), (
            "and the resume marker is cleared"
        )

        quickhouse.sync(pg_source, ch_target, **kw)
        assert ch_client.command(f"SELECT v FROM `{table}` FINAL WHERE id = 30") == 1, (
            "the row changed in a chunk the interrupted attempt had read lands"
        )
    finally:
        _drop_ch(ch_client, table)



def test_a_resume_past_a_marker_with_no_bound_records_none_with_a_max_of_its_own(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Review of #13: a resume past a marker with no bound, in a run that
    probes the MAX (an index added, say, or probe_max_cost=0), bounds its read
    by that fresh MAX. Recorded in its own markers, it would read as a bound
    the interrupted read froze, and a later resume would save it as the
    cursor, past rows in the first chunks that changed in between. It records
    none, so that later resume keeps the committed cursor too."""
    table = unique_name
    _chunked_stream_table(pg_conn, ch_client, table)
    state_key = f"{table}:chunked"
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="write_date",
        key=["id"], create_if_missing=True, engine="ReplacingMergeTree", order_by=["id"],
        chunk_rows=50, lookback_seconds=60, state_key=state_key,
    )
    try:
        assert quickhouse.sync(pg_source, ch_target, probe_max_cost=1.0, **kw).rows_written == 200
        committed = _latest_state(ch_client, state_key)[0]
        with pg_conn.cursor() as cur:
            cur.execute(f"UPDATE \"{table}\" SET v = 1, write_date = '2024-02-01' WHERE id = 30")
            cur.execute(f"UPDATE \"{table}\" SET v = 1, write_date = '2024-03-01' WHERE id = 180")
        ch_client.command(
            "INSERT INTO _quickhouse_state "
            "(source_table, dest_table, last_watermark, rows, chunk_cursor, chunk_upper) "
            f"VALUES ('{state_key}', '{table}', '{committed}', 100, '100', '')"
        )
        # Resumed with the MAX probed, and interrupted again after one chunk.
        ch_client.command(f"ALTER TABLE `{table}` ADD CONSTRAINT stop_here CHECK id <= 150")
        with pytest.raises(RuntimeError, match="stop_here"):
            quickhouse.sync(pg_source, ch_target, probe_max_cost=0.0, **kw)
        ch_client.command(f"ALTER TABLE `{table}` DROP CONSTRAINT stop_here")
        assert _latest_state(ch_client, state_key) == (committed, "150", "")

        r = quickhouse.sync(pg_source, ch_target, probe_max_cost=0.0, **kw)
        assert r.new_watermark is None
        assert _latest_state(ch_client, state_key) == (committed, "", "")
        quickhouse.sync(pg_source, ch_target, probe_max_cost=0.0, **kw)
        assert ch_client.command(f"SELECT v FROM `{table}` FINAL WHERE id = 30") == 1
    finally:
        _drop_ch(ch_client, table)


def test_a_resumed_first_read_lands_rows_with_no_watermark(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Review of #13: a first read whose cursor comes from the rows read has
    no bound, so it lands rows whose watermark is NULL. Resumed, it is held to
    its marker's bound, and `watermark <= bound` matches no NULL: the rows
    past the marker would never land. The resume reads them too. (Not into a
    ReplacingMergeTree, whose version column, the watermark, can't be NULL.)"""
    table = unique_name
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, v int, write_date timestamp)')
    _drop_ch(ch_client, table)
    state_key = f"{table}:chunked"
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="write_date",
        key=["id"], create_if_missing=True, engine="MergeTree", order_by=["id"],
        chunk_rows=50, probe_max_cost=1.0, lookback_seconds=60, state_key=state_key,
    )
    try:
        # An empty source: the destination is created, and no cursor saved.
        assert quickhouse.sync(pg_source, ch_target, **kw).new_watermark is None
        with pg_conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO \"{table}\" SELECT g, 0, "
                "CASE WHEN g BETWEEN 150 AND 160 THEN NULL ELSE timestamp '2024-01-01' END "
                "FROM generate_series(1, 200) g"
            )
        ch_client.command(f"ALTER TABLE `{table}` ADD CONSTRAINT stop_here CHECK id <= 100")
        with pytest.raises(RuntimeError, match="stop_here"):
            quickhouse.sync(pg_source, ch_target, **kw)
        ch_client.command(f"ALTER TABLE `{table}` DROP CONSTRAINT stop_here")
        upper = _latest_state(ch_client, state_key)[2]
        assert upper == "2024-01-01 00:00:00.000000"

        r = quickhouse.sync(pg_source, ch_target, **kw)
        assert r.rows_read == 100
        assert r.new_watermark == upper
        assert ch_client.command(f"SELECT count() FROM `{table}`") == 200
        assert ch_client.command(f"SELECT count() FROM `{table}` WHERE write_date IS NULL") == 11
    finally:
        _drop_ch(ch_client, table)



def test_a_resume_that_reads_only_rows_with_no_watermark_saves_its_bound(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """The resume above, with every row past the marker NULL: it saves its
    marker's bound all the same, so the read-side null_watermark, which says
    there is no cursor to save, isn't raised (and can't fail the run)."""
    table = unique_name
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, v int, write_date timestamp)')
    _drop_ch(ch_client, table)
    state_key = f"{table}:chunked"
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="write_date",
        key=["id"], create_if_missing=True, engine="MergeTree", order_by=["id"],
        chunk_rows=50, probe_max_cost=1.0, lookback_seconds=60, state_key=state_key,
        fail_on_warnings={"null_watermark"},
    )
    try:
        assert quickhouse.sync(pg_source, ch_target, **kw).new_watermark is None
        with pg_conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO \"{table}\" SELECT g, 0, "
                "CASE WHEN g > 100 THEN NULL ELSE timestamp '2024-01-01' END "
                "FROM generate_series(1, 200) g"
            )
        ch_client.command(f"ALTER TABLE `{table}` ADD CONSTRAINT stop_here CHECK id <= 100")
        with pytest.raises(RuntimeError, match="stop_here"):
            quickhouse.sync(pg_source, ch_target, **kw)
        ch_client.command(f"ALTER TABLE `{table}` DROP CONSTRAINT stop_here")
        upper = _latest_state(ch_client, state_key)[2]

        r = quickhouse.sync(pg_source, ch_target, **kw)
        assert r.rows_read == 100
        assert not [w for w in r.warnings if w.kind == "null_watermark"], r.warnings
        assert r.new_watermark == upper == "2024-01-01 00:00:00.000000"
        assert _latest_state(ch_client, state_key) == (upper, "", "")
    finally:
        _drop_ch(ch_client, table)



def test_a_date_cursor_from_a_chunked_read_across_midnight_keeps_that_day(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Issue #26: a DATE is the day of a change, up to a day before it. A
    chunked read whose cursor comes from the rows read, crossing midnight,
    saw a row changed after midnight (id 200, dated 01-02), so the newest
    DATE read is 01-02. A row read before midnight and changed again that
    day is dated 01-01, below `'01-02' - lookback` for a lookback of a day or
    less, and was never read again. The cursor now goes back a day."""
    table = unique_name
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, v int, d date NOT NULL)')
        cur.execute(
            f"INSERT INTO \"{table}\" SELECT g, 0, "
            "CASE WHEN g = 200 THEN date '2024-01-02' ELSE date '2024-01-01' END "
            "FROM generate_series(1, 200) g"
        )
    _drop_ch(ch_client, table)
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="d",
        key=["id"], create_if_missing=True, engine="ReplacingMergeTree", order_by=["id"],
        chunk_rows=50, probe_max_cost=1.0, lookback_seconds=3600, state_key=f"{table}:date",
    )
    try:
        r = quickhouse.sync(pg_source, ch_target, **kw)
        assert r.rows_written == 200
        assert r.new_watermark == "2024-01-01", "moved back a day from 01-02"

        with pg_conn.cursor() as cur:
            cur.execute(f"UPDATE \"{table}\" SET v = 1 WHERE id = 10")
        quickhouse.sync(pg_source, ch_target, **kw)
        assert ch_client.command(f"SELECT v FROM `{table}` FINAL WHERE id = 10") == 1
    finally:
        _drop_ch(ch_client, table)


def _null_watermark_table(pg_conn, ch_client, table):
    """200 rows with no index on `write_date`, NULL on ids 150-160."""
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, v int, write_date timestamp)')
        cur.execute(
            f"INSERT INTO \"{table}\" SELECT g, 0, "
            "CASE WHEN g BETWEEN 150 AND 160 THEN NULL ELSE timestamp '2024-01-01' END "
            "FROM generate_series(1, 200) g"
        )
    _drop_ch(ch_client, table)


def test_a_first_read_into_a_replacing_merge_tree_leaves_out_null_watermarks(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Issue #27: a ReplacingMergeTree's version column is the watermark, so
    it can't be NULL. A first read bounded by the MAX leaves rows with a NULL
    watermark out; one whose MAX probe was skipped had no bound at all, read
    them, and failed the insert with an Arrow error. It leaves them out too
    now, and says so."""
    table = unique_name
    _null_watermark_table(pg_conn, ch_client, table)
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="write_date",
        key=["id"], create_if_missing=True, lookback_seconds=60, state_key=table,
    )
    try:
        r = quickhouse.sync(pg_source, ch_target, probe_max_cost=1.0, **kw)
        assert r.rows_written == 189
        kinds = {w.kind for w in r.warnings}
        assert kinds & {"null_watermark", "null_check_skipped"}, r.warnings
        # As a read bounded by the MAX lands.
        _drop_ch(ch_client, table)
        r = quickhouse.sync(
            pg_source, ch_target, probe_max_cost=0.0, **{**kw, "state_key": f"{table}:max"}
        )
        assert r.rows_written == 189
    finally:
        _drop_ch(ch_client, table)


def test_a_resumed_first_read_into_a_replacing_merge_tree_leaves_out_null_watermarks(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Issue #27, resumed: the read it finishes left NULL watermarks out, so
    the resume does too, rather than read them and fail the insert."""
    table = unique_name
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, v int, write_date timestamp)')
    _drop_ch(ch_client, table)
    state_key = f"{table}:chunked"
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="write_date",
        key=["id"], create_if_missing=True, engine="ReplacingMergeTree", order_by=["id"],
        chunk_rows=50, probe_max_cost=1.0, lookback_seconds=60, state_key=state_key,
    )
    try:
        assert quickhouse.sync(pg_source, ch_target, **kw).new_watermark is None
        with pg_conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO \"{table}\" SELECT g, 0, "
                "CASE WHEN g BETWEEN 150 AND 160 THEN NULL ELSE timestamp '2024-01-01' END "
                "FROM generate_series(1, 200) g"
            )
        ch_client.command(f"ALTER TABLE `{table}` ADD CONSTRAINT stop_here CHECK id <= 100")
        with pytest.raises(RuntimeError, match="stop_here"):
            quickhouse.sync(pg_source, ch_target, **kw)
        ch_client.command(f"ALTER TABLE `{table}` DROP CONSTRAINT stop_here")

        r = quickhouse.sync(pg_source, ch_target, **kw)
        assert r.rows_read == 89
        assert ch_client.command(f"SELECT count() FROM `{table}` FINAL") == 189
    finally:
        _drop_ch(ch_client, table)
