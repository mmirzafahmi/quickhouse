"""Integration tests: the incremental cursor state table.

Every incremental run appends a cursor row to `_quickhouse_state`, and nothing
removed old ones: on BigQuery the table only grows, and keys renamed long ago
keep their rows for good (issue #21). These tests pin the helpers that compact
the table and list its keys, against ClickHouse, and the per-process caching
of the table's setup (issue #24).

Run against docker-compose.yml -- see test_sync.py's module docstring.
"""

from __future__ import annotations

import quickhouse


def _seed_state(ch_client, state):
    """A state table with two live keys of several rows each and one idle key,
    written in one insert, so no background merge folds them before the test
    looks (a merge needs two parts)."""
    ch_client.command(f"DROP TABLE IF EXISTS `{state}`")
    ch_client.command(
        f"CREATE TABLE `{state}` (source_table String, dest_table String, "
        "last_watermark String, rows UInt64, chunk_cursor String DEFAULT '', "
        "chunk_upper String DEFAULT '', run_ts DateTime64(3) DEFAULT now64(3)) "
        "ENGINE = ReplacingMergeTree(run_ts) ORDER BY (source_table, dest_table)"
    )
    # optimize_on_insert would fold each key's rows within the one insert.
    ch_client.command(
        f"INSERT INTO `{state}` (source_table, dest_table, last_watermark, rows, run_ts) "
        "SETTINGS optimize_on_insert = 0 VALUES "
        "('orders', 'orders', '2026-10-01 00:00:00', 1, now64(3) - INTERVAL 3 HOUR), "
        "('orders', 'orders', '2026-10-02 00:00:00', 1, now64(3) - INTERVAL 2 HOUR), "
        "('orders', 'orders', '2026-10-03 00:00:00', 1, now64(3) - INTERVAL 30 MINUTE), "
        "('lines', 'lines', '41', 1, now64(3) - INTERVAL 2 HOUR), "
        "('lines', 'lines', '42', 1, now64(3) - INTERVAL 1 HOUR), "
        "('orders_old_key', 'orders', '2026-01-01 00:00:00', 1, now64(3) - INTERVAL 40 DAY)"
    )


def test_compact_state_keeps_the_newest_row_per_key(ch_client, ch_target, unique_name):
    state = f"{unique_name}_state"
    _seed_state(ch_client, state)
    try:
        keys = quickhouse.state_keys(ch_target, state_table_name=state)
        assert [(k.state_key, k.last_watermark, k.state_rows) for k in keys] == [
            ("orders_old_key", "2026-01-01 00:00:00", 1),
            ("lines", "42", 2),
            ("orders", "2026-10-03 00:00:00", 3),
        ], "oldest first, each with its newest cursor"

        assert quickhouse.compact_state(ch_target, state_table_name=state) == 3
        after = quickhouse.state_keys(ch_target, state_table_name=state)
        assert [(k.state_key, k.last_watermark, k.state_rows) for k in after] == [
            ("orders_old_key", "2026-01-01 00:00:00", 1),
            ("lines", "42", 1),
            ("orders", "2026-10-03 00:00:00", 1),
        ], "every cursor reads the same afterwards"
        assert quickhouse.compact_state(ch_target, state_table_name=state) == 0

        idle = quickhouse.state_keys(ch_target, state_table_name=state, idle_days=30)
        assert [k.state_key for k in idle] == ["orders_old_key"]
        assert quickhouse.state_keys(ch_target, state_table_name=f"{state}_missing") == []
    finally:
        ch_client.command(f"DROP TABLE IF EXISTS `{state}`")


def test_the_state_table_is_set_up_once_per_process_and_again_if_dropped(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Issue #24: `CREATE TABLE IF NOT EXISTS` and a column probe ran on every
    sync. They now run once per process, and a state table dropped by hand is
    created again by the next sync instead of failing it."""
    table, state = unique_name, f"{unique_name}_state"
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, write_date timestamp NOT NULL)')
        cur.execute(
            f'INSERT INTO "{table}" SELECT g, \'2024-01-01\' FROM generate_series(1, 10) g'
        )
    ch_client.command(f"DROP TABLE IF EXISTS `{table}`")
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="write_date",
        key=["id"], create_if_missing=True, engine="ReplacingMergeTree", order_by=["id"],
        state_table_name=state,
    )

    def creates():
        ch_client.command("SYSTEM FLUSH LOGS")
        return ch_client.command(
            "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' "
            f"AND query LIKE 'CREATE TABLE IF NOT EXISTS%{state}%'"
        )

    try:
        assert quickhouse.sync(pg_source, ch_target, **kw).rows_written == 10
        assert creates() == 1
        assert quickhouse.sync(pg_source, ch_target, **kw).rows_written == 0
        assert creates() == 1, "set up once, not on every sync"

        ch_client.command(f"DROP TABLE `{state}`")
        r = quickhouse.sync(pg_source, ch_target, **kw)
        assert r.rows_written == 10, "no cursor left, so it reads from the start"
        assert ch_client.command(f"EXISTS TABLE `{state}`") == 1
        assert quickhouse.sync(pg_source, ch_target, **kw).rows_written == 0
    finally:
        ch_client.command(f"DROP TABLE IF EXISTS `{table}`")
        ch_client.command(f"DROP TABLE IF EXISTS `{state}`")
