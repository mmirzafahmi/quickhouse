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
