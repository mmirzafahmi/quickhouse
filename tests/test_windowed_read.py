"""Integration tests: reading in bounded key windows.

When the watermark column has no usable index, `WHERE wm > x` is a sequential
scan. On a hot standby a scan longer than `max_standby_streaming_delay` is
cancelled with SQLSTATE 40001, and retrying cannot converge because every
attempt restarts the same scan against the same window -- measured on a real
replica, reads of this shape took 75-162s and several never completed in three
attempts.

quickhouse instead sweeps the read in bounded key ranges. These tests pin the
property the whole design rests on: a windowed read returns EXACTLY the rows a
single-pass read would, at any window size.

Run against docker-compose.yml -- see test_sync.py's module docstring.
"""

from __future__ import annotations

import quickhouse


def _seed(pg_conn, table: str, rows: int, start_id: int = 1, gap: int = 1):
    """`gap > 1` leaves holes in the key space, so windows land on absent keys."""
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(
            f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, name text, '
            f"write_date timestamp)"
        )
        with cur.copy(f'COPY "{table}" (id, name, write_date) FROM STDIN') as copy:
            for i in range(rows):
                copy.write_row(
                    (start_id + i * gap, f"row-{i}", "2024-01-01 00:00:00")
                )


def _drop_ch(ch_client, table: str):
    ch_client.command(f"DROP TABLE IF EXISTS `{table}`")
    ch_client.command(f"DROP TABLE IF EXISTS `{table}_quickhouse_tmp`")


def _sync(pg_source, ch_target, table, **kw):
    kw.setdefault("create_if_missing", True)
    kw.setdefault("engine", "ReplacingMergeTree")
    kw.setdefault("order_by", ["id"])
    return quickhouse.sync(
        pg_source, ch_target, dest_table=table, source_table=table,
        mode="incremental", watermark="write_date", key=["id"], **kw,
    )


def _rows(ch_client, table):
    return ch_client.query(
        f"SELECT count(), uniqExact(id), sum(id) FROM `{table}` FINAL"
    ).result_rows[0]


def test_windowed_read_matches_a_single_pass_read(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """The core invariant, at several window widths.

    `probe_max_cost=1.0` forces the gate on for a small fixture; a table this
    size is genuinely cheap, so the planner would otherwise (correctly) decline
    to window it.
    """
    table = unique_name
    _seed(pg_conn, table, rows=500)
    _drop_ch(ch_client, table)
    try:
        # Baseline: one pass, gate off.
        _sync(pg_source, ch_target, table, probe_max_cost=0.0)
        expected = _rows(ch_client, table)
        assert expected[0] == 500

        for width in (1000, 97, 10, 1):
            _drop_ch(ch_client, table)
            _sync(
                pg_source, ch_target, table,
                probe_max_cost=1.0, read_window_rows=width, lookback_seconds=86_400,
            )
            got = _rows(ch_client, table)
            assert got == expected, f"window width {width} changed the result: {got}"
    finally:
        _drop_ch(ch_client, table)


def test_window_boundaries_landing_on_real_keys_lose_nothing(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """The classic off-by-one: `(lo, hi]` must not drop or duplicate the key
    that sits exactly on a boundary."""
    table = unique_name
    # Dense ids 1..100 with width 10 puts a boundary on 10, 20, 30 ... exactly.
    _seed(pg_conn, table, rows=100)
    _drop_ch(ch_client, table)
    try:
        _sync(
            pg_source, ch_target, table,
            probe_max_cost=1.0, read_window_rows=10, lookback_seconds=86_400,
        )
        count, uniq, total = _rows(ch_client, table)
        assert (count, uniq) == (100, 100)
        assert total == sum(range(1, 101)), "a boundary key was lost or duplicated"
    finally:
        _drop_ch(ch_client, table)


def test_sparse_key_space_is_swept_completely(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Windows are key ranges, not row counts, so gaps in the id space mean
    some windows match nothing. The sweep must still reach the end."""
    table = unique_name
    _seed(pg_conn, table, rows=50, start_id=1, gap=1000)  # ids 1, 1001, 2001 ...
    _drop_ch(ch_client, table)
    try:
        _sync(
            pg_source, ch_target, table,
            probe_max_cost=1.0, read_window_rows=250, lookback_seconds=86_400,
        )
        count, uniq, _ = _rows(ch_client, table)
        assert (count, uniq) == (50, 50)
    finally:
        _drop_ch(ch_client, table)


def test_source_query_is_windowed_too(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Production reads through a source_query -- the shape that defeated an
    earlier catalog-based approach entirely."""
    table = unique_name
    _seed(pg_conn, table, rows=300)
    _drop_ch(ch_client, table)
    try:
        q = (f'SELECT "id", "write_date", CAST("name" AS TEXT) AS "name" '
             f'FROM "{table}"')
        quickhouse.sync(
            pg_source, ch_target, dest_table=table, source_query=q,
            mode="incremental", watermark="write_date", key=["id"],
            create_if_missing=True, engine="ReplacingMergeTree", order_by=["id"],
            probe_max_cost=1.0, read_window_rows=50, lookback_seconds=86_400,
        )
        count, uniq, _ = _rows(ch_client, table)
        assert (count, uniq) == (300, 300)
    finally:
        _drop_ch(ch_client, table)


def test_gate_off_reads_in_one_pass(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """probe_max_cost=0 disables the gate, so nothing is windowed and the
    behaviour is exactly what it was before this feature."""
    table = unique_name
    _seed(pg_conn, table, rows=120)
    _drop_ch(ch_client, table)
    try:
        r = _sync(pg_source, ch_target, table, probe_max_cost=0.0)
        assert "unindexed_watermark" not in {w.kind for w in r.warnings}
        assert _rows(ch_client, table)[0] == 120
    finally:
        _drop_ch(ch_client, table)


def test_source_table_supplies_bounds_alongside_source_query(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Passing both lets the sweep read its key range from the table's index.

    A source_query that embeds its own unindexed filter makes the bounds probe
    inherit that filter, so the probe costs what the read costs -- on a hot
    standby it gets cancelled and windowing silently never activates. The bounds
    only need to cover the key space (a superset is fine; empty windows are
    cheap), so the table is the right relation to ask.
    """
    table = unique_name
    _seed(pg_conn, table, rows=400)
    _drop_ch(ch_client, table)
    try:
        q = (f'SELECT "id", "write_date", CAST("name" AS TEXT) AS "name" '
             f'FROM "{table}" WHERE write_date >= \'2000-01-01\'')
        quickhouse.sync(
            pg_source, ch_target, dest_table=table,
            source_query=q, source_table=table,
            mode="incremental", watermark="write_date", key=["id"],
            create_if_missing=True, engine="ReplacingMergeTree", order_by=["id"],
            probe_max_cost=1.0, read_window_rows=50, lookback_seconds=86_400,
        )
        count, uniq, total = _rows(ch_client, table)
        assert (count, uniq) == (400, 400)
        assert total == sum(range(1, 401)), "windowed sweep lost or duplicated rows"
    finally:
        _drop_ch(ch_client, table)
