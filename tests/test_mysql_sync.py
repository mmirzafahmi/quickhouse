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

import etlhouse


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
    ch_client.command(f"DROP TABLE IF EXISTS `{table}_etlhouse_tmp`")


def test_full_refresh_reconciles(mysql_conn, ch_client, mysql_source, ch_target, unique_name):
    table = unique_name
    n = 5000
    _seed_table(mysql_conn, table, n)
    _drop_ch(ch_client, table)
    try:
        result = etlhouse.sync(
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


def test_incremental_appends_and_is_idempotent(
    mysql_conn, ch_client, mysql_source, ch_target, unique_name
):
    table = unique_name
    _seed_table(mysql_conn, table, 100, base_ts="2024-01-01 00:00:00")
    _drop_ch(ch_client, table)
    try:
        # First incremental run backfills everything (no prior watermark).
        r1 = etlhouse.sync(
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

        r2 = etlhouse.sync(
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
        r3 = etlhouse.sync(
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
        etlhouse.sync(
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


def _mysql_scalar(mysql_conn, sql: str):
    with mysql_conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]
