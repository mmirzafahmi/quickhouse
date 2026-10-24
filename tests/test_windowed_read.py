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

import re

import pytest

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


def _sweep(err):
    """`(windows, shrinks)` from the log line a finished sweep writes."""
    m = re.search(r"windowed read complete: (\d+) window\(s\), (\d+) shrink\(s\)", err)
    assert m, f"the read was not swept:\n{err}"
    return int(m.group(1)), int(m.group(2))


def _backfill(table, lo, hi, **kw):
    """A bounded backfill as a caller runs one: a primary-key range, a cursor
    that stays where it is, and a state key of its own."""
    return dict(
        dest_table=table, mode="incremental", watermark="id", key=["id"],
        advance_watermark=False, state_key=f"{table}:backfill:{lo}-{hi}",
        create_if_missing=True, engine="ReplacingMergeTree", order_by=["id"], **kw,
    )


def _state_rows(ch_client, state_key):
    """The rows a run saved under `state_key`: cursors and resume markers."""
    if not ch_client.command("EXISTS TABLE _quickhouse_state"):
        return 0
    return ch_client.command(
        f"SELECT count() FROM _quickhouse_state WHERE source_table = '{state_key}'"
    )


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


def test_a_chunked_read_is_never_swept(
    pg_conn, ch_client, pg_source, ch_target, unique_name, capfd
):
    """A sweep bypassed the keyset loop of a chunk_rows read whose MAX probe
    priced too high: no chunk markers, and on a staged destination (BigQuery)
    every row went into the template table each chunk is cloned from, which
    is then dropped. A chunked read is bounded per chunk already."""
    table = unique_name
    _seed(pg_conn, table, rows=300)
    _drop_ch(ch_client, table)
    try:
        capfd.readouterr()
        r = _sync(
            pg_source, ch_target, table, probe_max_cost=1.0, read_window_rows=50,
            lookback_seconds=86_400, chunk_rows=100,
        )
        err = capfd.readouterr().err
        assert r.rows_written == 300
        assert "keyset chunked read starting" in err
        assert "windowed read on" not in err
        assert _rows(ch_client, table)[:2] == (300, 300)
    finally:
        _drop_ch(ch_client, table)


def test_a_primary_key_backfill_can_force_the_sweep(
    pg_conn, ch_client, pg_source, ch_target, unique_name, capfd
):
    """Issue #29: a backfill on the primary key has a cheap MAX probe, so it
    was always read in one pass, which a hot standby cancels once it outlives
    max_standby_streaming_delay -- on every retry, since each one restarts the
    same scan. window_bounds sweeps it in windows whatever the probe costs."""
    table = unique_name
    _seed(pg_conn, table, rows=1000)
    _drop_ch(ch_client, table)
    q = (f'SELECT "id", "name", "write_date" FROM "{table}" '
         f'WHERE "id" >= 201 AND "id" <= 800')
    kw = _backfill(table, 201, 800, source_query=q, read_window_rows=100)
    try:
        capfd.readouterr()
        r = quickhouse.sync(pg_source, ch_target, **kw)
        assert "windowed read on" not in capfd.readouterr().err, "a cheap probe: one pass"
        assert r.rows_written == 600
        _drop_ch(ch_client, table)

        r = quickhouse.sync(pg_source, ch_target, window_bounds=(201, 800), **kw)
        err = capfd.readouterr().err
        # (200, 300], (300, 400], ..., (700, 800]
        assert _sweep(err)[0] == 6, err
        assert r.rows_written == 600
        assert _rows(ch_client, table) == (600, 600, sum(range(201, 801)))
        assert r.new_watermark == "800"
        assert _state_rows(ch_client, kw["state_key"]) == 0, "the cursor stays unsaved"
    finally:
        _drop_ch(ch_client, table)


def test_window_bounds_are_the_range_the_sweep_reads(
    pg_conn, ch_client, pg_source, ch_target, unique_name, capfd
):
    """The bounds stand in for the MIN/MAX probe, and they are the range read:
    from source_table alone, a backfill of ids 201..800 reads those rows and no
    others, in windows over [201, 800] rather than the table's [1, 1000]."""
    table = unique_name
    _seed(pg_conn, table, rows=1000)
    _drop_ch(ch_client, table)
    kw = _backfill(table, 201, 800, source_table=table, read_window_rows=100)
    try:
        capfd.readouterr()
        r = quickhouse.sync(pg_source, ch_target, window_bounds=(201, 800), **kw)
        err = capfd.readouterr().err
        assert 'windowed read on "id" over [201, 800]' in err, err
        assert _sweep(err)[0] == 6
        assert r.rows_written == 600
        assert _rows(ch_client, table) == (600, 600, sum(range(201, 801)))
    finally:
        _drop_ch(ch_client, table)


def test_windowed_without_bounds_sweeps_the_whole_key_range(
    pg_conn, ch_client, pg_source, ch_target, unique_name, capfd
):
    """windowed=True alone probes the key's MIN/MAX for its bounds, so the sweep
    reads exactly what one pass would, and the cursor moves as usual."""
    table = unique_name
    _seed(pg_conn, table, rows=500)
    _drop_ch(ch_client, table)
    try:
        capfd.readouterr()
        r = quickhouse.sync(
            pg_source, ch_target, dest_table=table, source_table=table,
            mode="incremental", watermark="id", key=["id"], create_if_missing=True,
            engine="ReplacingMergeTree", order_by=["id"], state_key=f"{table}:pk",
            windowed=True, read_window_rows=100,
        )
        err = capfd.readouterr().err
        assert 'windowed read on "id" over [1, 500]' in err, err
        assert _sweep(err)[0] == 5
        assert _rows(ch_client, table) == (500, 500, sum(range(1, 501)))
        assert r.new_watermark == "500"
    finally:
        _drop_ch(ch_client, table)


def test_a_cancelled_window_of_a_backfill_is_read_again_narrower(
    pg_conn, ch_client, pg_source, ch_target, unique_name, capfd
):
    """Why a backfill wants the sweep on a hot standby: a statement the source
    cancels is read again at half the width until one fits. A function in
    source_query raises SQLSTATE 40001 once one statement has read more than
    140 rows, as a standby cancels any statement that outlives
    max_standby_streaming_delay. One pass fails on every retry; the sweep
    shrinks, finishes, and lands every row once."""
    table = unique_name
    guard = f"{table}_cancel_past"
    _seed(pg_conn, table, rows=600, start_id=10, gap=10)  # ids 10, 20, ..., 6000
    with pg_conn.cursor() as cur:
        # Counted per statement: every read is a transaction of its own.
        cur.execute(f"""
            CREATE FUNCTION "{guard}"(n int) RETURNS boolean
            LANGUAGE plpgsql VOLATILE AS $$
            DECLARE
              seen int := coalesce(nullif(current_setting('qh.seen', true), ''), '0')::int + 1;
            BEGIN
              PERFORM set_config('qh.seen', seen::text, true);
              IF seen > n THEN
                RAISE EXCEPTION 'canceling statement due to conflict with recovery'
                  USING ERRCODE = '40001';
              END IF;
              RETURN true;
            END $$""")
    _drop_ch(ch_client, table)
    q = f'SELECT "id", "name", "write_date" FROM "{table}" WHERE "{guard}"(140)'
    kw = _backfill(table, 10, 6000, source_query=q, retry_max_attempts=2)
    try:
        with pytest.raises(RuntimeError, match="conflict with recovery"):
            quickhouse.sync(pg_source, ch_target, **kw)

        capfd.readouterr()
        # Windows of 1,000 keys (100 rows) at the narrowest, 4,000 at the widest.
        r = quickhouse.sync(
            pg_source, ch_target, window_bounds=(10, 6000), read_window_rows=4000, **kw
        )
        windows, shrinks = _sweep(capfd.readouterr().err)
        assert shrinks > 0 and windows > 1, (windows, shrinks)
        assert r.rows_written == 600
        assert _rows(ch_client, table) == (600, 600, sum(range(10, 6001, 10)))
    finally:
        _drop_ch(ch_client, table)
        with pg_conn.cursor() as cur:
            cur.execute(f'DROP FUNCTION IF EXISTS "{guard}"(int)')


def test_a_forced_sweep_refuses_a_key_it_cannot_window(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Asked for a sweep, a key that can't bound one is an error before
    anything is written, not the one-pass read the caller set out to avoid."""
    table = unique_name
    _seed(pg_conn, table, rows=10)
    _drop_ch(ch_client, table)
    try:
        with pytest.raises(RuntimeError, match="must be an integer"):
            quickhouse.sync(
                pg_source, ch_target, dest_table=table, source_table=table,
                mode="incremental", watermark="id", key=["name"], windowed=True,
                create_if_missing=True, engine="ReplacingMergeTree", order_by=["name"],
            )
        assert not ch_client.command(f"EXISTS TABLE `{table}`")
    finally:
        _drop_ch(ch_client, table)
