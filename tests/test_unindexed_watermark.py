"""Integration tests: what an unindexed watermark column changes.

An incremental run probes the watermark column three times -- a `MAX(watermark)`
snapshot bound, a `count(*) WHERE watermark IS NULL` completeness check, and the
filter itself. With an index those are cheap; without one each is a full
sequential scan of the whole table, every run (measured at 56.7s and 57.2s on a
14.9 GB table, against plan costs of 2.06 and 0.64 where the column is indexed).

quickhouse asks the planner what each probe would cost (EXPLAIN, never ANALYZE)
and skips those above `probe_max_cost`. These tests pin the observable
consequences: which warnings are raised, which are deliberately NOT raised, that
detection works THROUGH a source_query, and that skipping changes nothing about
which rows move.

Run against docker-compose.yml -- see test_sync.py's module docstring.
"""

from __future__ import annotations

import quickhouse


def _kinds(result):
    return {w.kind for w in result.warnings}


def _warning(result, kind):
    return next((w for w in result.warnings if w.kind == kind), None)


def _seed_nullable_wm(pg_conn, table: str, rows: int, nulls: int = 0, indexed: bool = False):
    """Table whose watermark is NULLABLE, optionally with an index on it.

    The index is the single variable these tests turn: everything else about
    the two tables is identical.
    """
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(
            f"""
            CREATE TABLE "{table}" (
                id          bigint PRIMARY KEY,
                name        text,
                write_date  timestamp          -- deliberately NULLABLE
            )
            """
        )
        if indexed:
            cur.execute(f'CREATE INDEX "{table}_wm_idx" ON "{table}" (write_date)')
        with cur.copy(f'COPY "{table}" (id, name, write_date) FROM STDIN') as copy:
            for i in range(1, rows + 1):
                copy.write_row((i, f"row-{i}", "2024-01-01 00:00:00"))
            # Rows a `WHERE write_date > x` predicate can never match.
            for i in range(rows + 1, rows + 1 + nulls):
                copy.write_row((i, f"null-{i}", None))


def _drop_ch(ch_client, table: str):
    ch_client.command(f"DROP TABLE IF EXISTS `{table}`")
    ch_client.command(f"DROP TABLE IF EXISTS `{table}_quickhouse_tmp`")


def _sync(pg_source, ch_target, table, **kw):
    kw.setdefault("create_if_missing", True)
    kw.setdefault("engine", "ReplacingMergeTree")
    kw.setdefault("order_by", ["id"])
    return quickhouse.sync(
        pg_source,
        ch_target,
        dest_table=table,
        source_table=table,
        mode="incremental",
        watermark="write_date",
        key=["id"],
        **kw,
    )


def test_indexed_watermark_still_reports_null_rows(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """The pre-existing guarantee, which must not regress.

    With an index on the watermark the completeness check is cheap, so it still
    runs -- and still catches rows that a `>` predicate would silently exclude
    forever. This is the condition NullWatermark exists for.
    """
    table = unique_name
    _seed_nullable_wm(pg_conn, table, rows=50, nulls=7, indexed=True)
    _drop_ch(ch_client, table)
    try:
        r = _sync(pg_source, ch_target, table)
        assert "null_watermark" in _kinds(r), r.warnings
        assert _warning(r, "null_watermark").count == 7
        # An indexed watermark is not a cost problem, so no cost warning.
        assert "unindexed_watermark" not in _kinds(r), r.warnings
        assert r.rows_written == 50
    finally:
        _drop_ch(ch_client, table)


def test_unindexed_watermark_first_run_skips_a_count_it_cannot_afford(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """This reverses an earlier decision, deliberately.

    Paying for the completeness count on a first run was defensible in
    isolation: the read touches the whole table anyway, and a pipeline that
    starts out excluding NULL-watermark rows excludes them forever. But on a
    large table with an unindexed watermark, read from a hot standby, that scan
    does not merely cost -- it never finishes. Measured on a real replica it was
    cancelled with 40001 on three consecutive attempts, so the first run failed
    before reading anything and no first run could ever complete to make a later
    one cheap. A check that cannot run is not a check.

    What replaces it is honesty: the skip is reported, so nobody reads a missing
    warning as a clean bill of health. The rows still transfer.
    """
    table = unique_name
    _seed_nullable_wm(pg_conn, table, rows=50, nulls=7, indexed=False)
    _drop_ch(ch_client, table)
    try:
        r = _sync(pg_source, ch_target, table, probe_max_cost=1.0)
        # The count did not run, so no count is reported ...
        assert "null_watermark" not in _kinds(r), r.warnings
        # ... but the reason is, and it says the check was skipped.
        assert "unindexed_watermark" in _kinds(r), r.warnings
        # A first run has no committed cursor to filter against, so every row
        # transfers, NULL watermarks included. Skipping the count costs the
        # report, not the data -- it is later runs, filtering on `wm > x`, where
        # those rows go missing, which is what the warning is there to say.
        assert r.rows_written == 50
    finally:
        _drop_ch(ch_client, table)


def test_unindexed_watermark_ongoing_run_skips_and_says_so(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """On an ongoing run the count is pure overhead on every schedule tick.

    Skipping it is the point -- it is a full sequential scan, measured at 56.65s
    on a 14.9 GB table -- but a skipped check reported as a clean one is exactly
    how the NullWatermark condition goes unnoticed. So the warning must say in
    so many words that the check did not run.
    """
    table = unique_name
    _seed_nullable_wm(pg_conn, table, rows=50, nulls=7, indexed=False)
    _drop_ch(ch_client, table)
    try:
        first = _sync(pg_source, ch_target, table, probe_max_cost=1.0)
        assert first.new_watermark is not None, "need a persisted cursor to go ongoing"

        r = _sync(pg_source, ch_target, table, probe_max_cost=1.0)
        assert "unindexed_watermark" in _kinds(r), r.warnings
        w = _warning(r, "unindexed_watermark")
        assert w.column == "write_date"
        assert "SKIPPED" in w.message, w.message
        assert "CREATE INDEX CONCURRENTLY" in w.message, w.message
        # The count was not paid for, so the condition cannot be reported.
        assert "null_watermark" not in _kinds(r), r.warnings
    finally:
        _drop_ch(ch_client, table)


def test_unindexed_watermark_still_syncs_correctly_across_runs(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Skipping the NULL count must not change which rows move.

    Includes a BACKDATED insert -- rows whose watermark is older than wall-clock
    but newer than the previous run's cursor. This is the case that killed an
    earlier attempt to replace MAX(watermark) with a source clock: the clock
    advanced the cursor to wall-clock time and these rows were then silently
    skipped forever. Keeping MAX means the cursor tracks the data, so they land.
    """
    table = unique_name
    _seed_nullable_wm(pg_conn, table, rows=100, indexed=False)
    _drop_ch(ch_client, table)
    try:
        r1 = _sync(pg_source, ch_target, table, probe_max_cost=1.0)
        assert "unindexed_watermark" in _kinds(r1), r1.warnings
        assert r1.rows_written == 100
        assert r1.new_watermark is not None

        with pg_conn.cursor() as cur:
            with cur.copy(f'COPY "{table}" (id, name, write_date) FROM STDIN') as copy:
                for i in range(101, 151):
                    # Newer than the seeded rows, but far behind wall-clock.
                    copy.write_row((i, f"row-{i}", "2024-06-01 00:00:00"))

        r2 = _sync(pg_source, ch_target, table, probe_max_cost=1.0)
        assert r2.rows_written == 50, "backdated rows must still be picked up"

        got = ch_client.query(
            f"SELECT count(), uniqExact(id) FROM `{table}` FINAL"
        ).result_rows[0]
        assert got == (150, 150), f"expected 150 distinct rows, got {got}"
    finally:
        _drop_ch(ch_client, table)


def test_non_nullable_unindexed_watermark_warns_without_the_skip_note(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """A NOT NULL watermark never ran the count, so nothing was skipped.

    The cost warning still applies -- the filter is a full scan either way --
    but it must not claim a completeness check was lost when there was none.
    """
    table = unique_name
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(
            f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, write_date timestamp NOT NULL)'
        )
        with cur.copy(f'COPY "{table}" (id, write_date) FROM STDIN') as copy:
            for i in range(1, 21):
                copy.write_row((i, "2024-01-01 00:00:00"))
    _drop_ch(ch_client, table)
    try:
        r = _sync(pg_source, ch_target, table, probe_max_cost=1.0)
        w = _warning(r, "unindexed_watermark")
        assert w is not None, r.warnings
        assert "SKIPPED the check" not in w.message, w.message
        assert r.rows_written == 20
    finally:
        _drop_ch(ch_client, table)


def _cast_source_query(table: str) -> str:
    """The production shape: a cast-projecting SELECT, never a bare table.

    This is how every real pipeline in this repo calls quickhouse, and it is
    what defeated the previous catalog-based detection: with no `source_table`
    there is no relation to look an index up on, so the check silently reported
    "indexed" and skipped nothing. The planner has no such blind spot.
    """
    return (
        f'SELECT "id", "write_date", CAST("name" AS TEXT) AS "name" FROM "{table}"'
    )


def test_source_query_unindexed_watermark_is_still_detected(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """The defect this design replaced: detection must survive a source_query."""
    table = unique_name
    _seed_nullable_wm(pg_conn, table, rows=60, nulls=0, indexed=False)
    _drop_ch(ch_client, table)
    try:
        # First run establishes a cursor (and still pays for the count).
        quickhouse.sync(
            pg_source, ch_target, dest_table=table,
            source_query=_cast_source_query(table),
            mode="incremental", watermark="write_date", key=["id"],
            create_if_missing=True, engine="ReplacingMergeTree", order_by=["id"],
            lookback_seconds=3600, probe_max_cost=1.0,
        )
        # Ongoing run: the gate must fire even though there is no source_table.
        r = quickhouse.sync(
            pg_source, ch_target, dest_table=table,
            source_query=_cast_source_query(table),
            mode="incremental", watermark="write_date", key=["id"],
            lookback_seconds=3600, probe_max_cost=1.0,
        )
        assert "unindexed_watermark" in _kinds(r), r.warnings
        w = _warning(r, "unindexed_watermark")
        assert w.column == "write_date"
        # The message must quote the planner's own estimate, not just "skipped".
        assert "cost" in w.message.lower(), w.message
    finally:
        _drop_ch(ch_client, table)


def test_zero_probe_max_cost_restores_unconditional_probing(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """The documented escape hatch."""
    table = unique_name
    _seed_nullable_wm(pg_conn, table, rows=40, nulls=5, indexed=False)
    _drop_ch(ch_client, table)
    try:
        _sync(pg_source, ch_target, table, probe_max_cost=0.0)
        r = _sync(pg_source, ch_target, table, probe_max_cost=0.0)
        assert "unindexed_watermark" not in _kinds(r), r.warnings
    finally:
        _drop_ch(ch_client, table)


def test_stream_cursor_lands_backdated_rows_exactly_once(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """With the MAX probe skipped, the cursor comes from the rows read.

    The backdated insert is the case that killed an earlier attempt to use a
    source clock as the bound: it advanced the cursor to wall-clock time, and
    rows written with an older watermark were then skipped forever. A cursor
    derived from the stream tracks the data instead.
    """
    table = unique_name
    _seed_nullable_wm(pg_conn, table, rows=100, indexed=False)
    _drop_ch(ch_client, table)
    try:
        r1 = _sync(pg_source, ch_target, table, lookback_seconds=3600, probe_max_cost=1.0)
        assert r1.rows_written == 100
        assert r1.new_watermark is not None, "a stream cursor must still be persisted"
        # Taken from the data, not from the clock.
        assert r1.new_watermark.startswith("2024-01-01"), r1.new_watermark

        with pg_conn.cursor() as cur:
            with cur.copy(f'COPY "{table}" (id, name, write_date) FROM STDIN') as copy:
                for i in range(101, 151):
                    copy.write_row((i, f"row-{i}", "2024-06-01 00:00:00"))

        r2 = _sync(pg_source, ch_target, table, lookback_seconds=3600, probe_max_cost=1.0)
        # The lookback legitimately re-reads the trailing window, and every
        # seeded row shares one timestamp, so this is >= 50 rather than == 50.
        # The invariant that matters is the deduplicated set below.
        assert r2.rows_written >= 50, "backdated rows must still be picked up"
        assert r2.new_watermark.startswith("2024-06-01"), r2.new_watermark
        got = ch_client.query(
            f"SELECT count(), uniqExact(id) FROM `{table}` FINAL"
        ).result_rows[0]
        assert got == (150, 150), f"expected 150 distinct rows, got {got}"
    finally:
        _drop_ch(ch_client, table)


def test_stream_cursor_needs_a_lookback(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """Without a trailing re-scan the MAX scan is paid for rather than skipped."""
    table = unique_name
    _seed_nullable_wm(pg_conn, table, rows=30, indexed=False)
    _drop_ch(ch_client, table)
    try:
        _sync(pg_source, ch_target, table, lookback_seconds=0, probe_max_cost=1.0)
        r = _sync(pg_source, ch_target, table, lookback_seconds=0, probe_max_cost=1.0)
        w = _warning(r, "unindexed_watermark")
        assert w is not None, r.warnings
        assert "lookback_seconds > 0" in w.message, w.message
        # MAX still ran, so the cursor is the column max, not a stream artefact.
        assert r.new_watermark is not None
    finally:
        _drop_ch(ch_client, table)


def test_stream_derived_watermark_refuses_a_transformed_column(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """When the MAX probe is skipped as too costly, the cursor is taken from
    the read stream's own (decoded, transformed) values. The incremental
    filter, though, always compares against the raw column — never against
    column_transforms, which can be arbitrary SQL a WHERE cannot generally
    invert. That domain mismatch used to persist a cursor no later run's
    filter could correctly compare against, silently skipping rows."""
    table = unique_name
    _seed_nullable_wm(pg_conn, table, rows=50, nulls=0, indexed=False)
    _drop_ch(ch_client, table)
    try:
        try:
            _sync(
                pg_source,
                ch_target,
                table,
                lookback_seconds=3600,
                probe_max_cost=1.0,
                column_transforms={"write_date": "write_date + interval '1 hour'"},
            )
            assert False, "expected a RuntimeError"
        except RuntimeError as e:
            msg = str(e)
            assert "column_transforms" in msg, msg
            assert "write_date" in msg, msg
        # int(), because clickhouse_connect returns EXISTS as an int on some
        # driver versions and a str on others; the assertion is about the
        # table not existing, not about which of those we got.
        assert int(ch_client.command(f"EXISTS TABLE `{table}`")) == 0
    finally:
        _drop_ch(ch_client, table)


def test_skip_to_max_refuses_rather_than_full_scanning_when_the_probe_is_skipped(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """skip_to_max exists so the first run reads "(almost) nothing" instead of
    a doomed full pull. When the MAX probe itself is too costly to run, there
    is no snapshot to seed from — it used to fall through to a plain, unseeded
    first run (the exact full scan skip_to_max exists to avoid) and say
    nothing. It must refuse instead."""
    table = unique_name
    _seed_nullable_wm(pg_conn, table, rows=50, nulls=0, indexed=False)
    _drop_ch(ch_client, table)
    try:
        try:
            _sync(
                pg_source,
                ch_target,
                table,
                skip_to_max=True,
                lookback_seconds=3600,
                probe_max_cost=1.0,
            )
            assert False, "expected a RuntimeError"
        except RuntimeError as e:
            msg = str(e)
            assert "skip_to_max" in msg, msg
            assert "probe_max_cost" in msg, msg
        # The refusal happens before the destination table is even created.
        # int(), because clickhouse_connect returns EXISTS as an int on some
        # driver versions and a str on others; the assertion is about the
        # table not existing, not about which of those we got.
        assert int(ch_client.command(f"EXISTS TABLE `{table}`")) == 0

    finally:
        _drop_ch(ch_client, table)
