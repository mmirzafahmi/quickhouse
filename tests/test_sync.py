"""End-to-end integration tests: PostgreSQL -> ClickHouse.

Run against the services in ``docker-compose.yml`` after building the module:

    docker compose up -d
    pip install -e '.[test]'
    maturin develop --release
    pytest -v
"""

from __future__ import annotations

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


def _pg_scalar(pg_conn, sql: str):
    with pg_conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]
