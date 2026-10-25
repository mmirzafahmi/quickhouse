"""Live integration tests: what 0.20.7 changed on a BigQuery destination.

Like ``test_bigquery_chunked.py`` these need a real dataset you can write to,
and skip otherwise (the default, and in CI):

    QUICKHOUSE_BQ_DATASET=my_scratch_dataset pytest tests/test_bigquery_live.py -v

Credentials come from Application Default Credentials; ``QUICKHOUSE_BQ_PROJECT``
overrides the project they resolve to. Every test writes its own state table,
so a shared ``_quickhouse_state`` is never touched. Every table a test creates
is removed afterwards.
"""

from __future__ import annotations

import os

import pytest

import quickhouse

BQ_DATASET = os.environ.get("QUICKHOUSE_BQ_DATASET")
BQ_PROJECT = os.environ.get("QUICKHOUSE_BQ_PROJECT")

pytestmark = pytest.mark.skipif(
    not BQ_DATASET,
    reason="set QUICKHOUSE_BQ_DATASET to a writable dataset to run the live BigQuery tests",
)


@pytest.fixture
def bq():
    bigquery = pytest.importorskip("google.cloud.bigquery")
    return bigquery.Client(project=BQ_PROJECT)


@pytest.fixture
def bq_target():
    return quickhouse.BigQuery(project_id=BQ_PROJECT, dataset_id=BQ_DATASET)


@pytest.fixture
def dest(bq, pg_conn, unique_name):
    """A destination table name, with every table it leaves behind removed:
    the destination, its staging tables and its own state table."""
    yield unique_name
    for t in bq.list_tables(f"{bq.project}.{BQ_DATASET}"):
        if t.table_id.startswith(unique_name):
            bq.delete_table(t, not_found_ok=True)
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{unique_name}"')


def _fq(bq, table):
    return f"`{bq.project}.{BQ_DATASET}.{table}`"


def _rows(bq, sql):
    return list(bq.query(sql).result())


def _seed(pg_conn, table, ids, ts="2024-01-01 00:00:00"):
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(
            f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, amount numeric, '
            "write_date timestamp NOT NULL)"
        )
        with cur.copy(f'COPY "{table}" (id, amount, write_date) FROM STDIN') as copy:
            for i in ids:
                copy.write_row((i, i * 1.5, ts))


def _add(pg_conn, table, ids, ts):
    with pg_conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO \"{table}\" SELECT g, g * 1.5, '{ts}' "
            f"FROM generate_series({ids.start}, {ids.stop - 1}) g"
        )


def _sync(pg_source, bq_target, table, **kw):
    kw.setdefault("source_table", table)
    kw.setdefault("state_table_name", f"{table}_state")
    return quickhouse.sync(
        pg_source,
        bq_target,
        dest_table=table,
        mode="incremental",
        watermark="write_date",
        key=["id"],
        create_if_missing=True,
        **kw,
    )


def _existing_dest(bq, table):
    """The destination an earlier version's state refers to. Saved state with
    no destination means the table was dropped, and the run starts over
    (#36), so a test of what an earlier version left creates both."""
    bigquery = pytest.importorskip("google.cloud.bigquery")
    bq.create_table(
        bigquery.Table(
            f"{bq.project}.{BQ_DATASET}.{table}",
            schema=[
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("amount", "FLOAT"),
                bigquery.SchemaField("write_date", "DATETIME", mode="REQUIRED"),
            ],
        )
    )


def _staging(bq, table):
    return [
        t.table_id
        for t in bq.list_tables(f"{bq.project}.{BQ_DATASET}")
        if t.table_id.startswith(f"{table}_quickhouse_tmp")
    ]


def test_a_new_state_table_is_clustered_by_its_key(bq, pg_conn, pg_source, bq_target, dest):
    """Issue #21: every cursor read filters on (source_table, dest_table), and
    an unclustered state table made each read scan the whole table."""
    _seed(pg_conn, dest, range(1, 11))
    assert _sync(pg_source, bq_target, dest).rows_written == 10
    assert bq.get_table(f"{bq.project}.{BQ_DATASET}.{dest}_state").clustering_fields == [
        "source_table",
        "dest_table",
    ]


def test_an_unclustered_state_table_is_clustered_in_place(
    bq, pg_conn, pg_source, bq_target, dest
):
    """Issue #21: a state table created by an earlier version is clustered by
    the next sync, keeping its rows, and the cursor it holds is read."""
    bigquery = pytest.importorskip("google.cloud.bigquery")
    state = f"{bq.project}.{BQ_DATASET}.{dest}_state"
    old = bigquery.Table(
        state,
        schema=[
            bigquery.SchemaField("source_table", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("dest_table", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("last_watermark", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("rows", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("chunk_cursor", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("chunk_upper", "STRING", mode="NULLABLE"),
        ],
    )
    bq.create_table(old)
    _existing_dest(bq, dest)
    assert bq.get_table(state).clustering_fields is None
    # What an earlier version left: this destination's cursor, and another's.
    bq.query(
        f"INSERT INTO `{state}` (source_table, dest_table, last_watermark, `rows`, run_ts) "
        f"VALUES ('{dest}', '{dest}', '2024-01-01 00:00:00', 10, CURRENT_TIMESTAMP()), "
        "('other_key', 'other', 'kept', 1, CURRENT_TIMESTAMP())"
    ).result()
    _seed(pg_conn, dest, range(1, 11))
    _add(pg_conn, dest, range(11, 16), "2024-02-01 00:00:00")

    assert _sync(pg_source, bq_target, dest).rows_written == 5, "the saved cursor was read"
    assert bq.get_table(state).clustering_fields == ["source_table", "dest_table"]
    kept = _rows(bq, f"SELECT last_watermark FROM `{state}` WHERE source_table = 'other_key'")
    assert [r.last_watermark for r in kept] == ["kept"]


def test_compact_state_keeps_the_newest_row_per_key(bq, pg_conn, pg_source, bq_target, dest):
    """Issue #21: the BigQuery state table only grows. compact_state deletes
    every row but the newest per key, after which every cursor reads the same,
    and state_keys lists the keys, the idle ones with idle_days."""
    state_name = f"{dest}_state"
    state = f"{bq.project}.{BQ_DATASET}.{state_name}"
    _seed(pg_conn, dest, range(1, 11))
    assert _sync(pg_source, bq_target, dest).rows_written == 10
    for i, ts in enumerate(["2024-02-01", "2024-03-01"]):
        _add(pg_conn, dest, range(11 + 5 * i, 16 + 5 * i), f"{ts} 00:00:00")
        assert _sync(pg_source, bq_target, dest).rows_written == 5
    bq.query(
        f"INSERT INTO `{state}` (source_table, dest_table, last_watermark, `rows`, run_ts) VALUES "
        "('renamed_long_ago', 'old_dest', '2023-01-01 00:00:00', 1, "
        "TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 40 DAY)), "
        "('renamed_long_ago', 'old_dest', '2022-12-01 00:00:00', 1, "
        "TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 50 DAY))"
    ).result()

    keys = {k.state_key: k for k in quickhouse.state_keys(bq_target, state_table_name=state_name)}
    assert keys[dest].state_rows == 3
    assert keys[dest].last_watermark == "2024-03-01 00:00:00"
    assert keys["renamed_long_ago"].state_rows == 2
    assert keys["renamed_long_ago"].last_watermark == "2023-01-01 00:00:00"
    idle = quickhouse.state_keys(bq_target, state_table_name=state_name, idle_days=30)
    assert [k.state_key for k in idle] == ["renamed_long_ago"]

    assert quickhouse.compact_state(bq_target, state_table_name=state_name) == 3
    after = {k.state_key: k for k in quickhouse.state_keys(bq_target, state_table_name=state_name)}
    assert after[dest].state_rows == 1
    assert after[dest].last_watermark == "2024-03-01 00:00:00"
    assert after["renamed_long_ago"].state_rows == 1
    assert after["renamed_long_ago"].last_watermark == "2023-01-01 00:00:00"
    assert quickhouse.compact_state(bq_target, state_table_name=state_name) == 0

    # The compacted cursor is the one the next sync reads.
    _add(pg_conn, dest, range(21, 24), "2024-04-01 00:00:00")
    assert _sync(pg_source, bq_target, dest).rows_written == 3
    assert quickhouse.compact_state(bq_target, state_table_name=f"{dest}_missing") == 0


def test_fail_on_warnings_runs_no_merge(bq, pg_conn, pg_source, bq_target, dest):
    """Issue #17: on a staged destination the check runs before the MERGE, so
    a fatal warning leaves the destination untouched and the cursor unsaved,
    and the next run reads the same rows again."""
    state = _fq(bq, f"{dest}_state")
    kw = dict(numeric_as_decimal="Decimal(10,2)")
    _seed(pg_conn, dest, range(1, 6))
    assert _sync(pg_source, bq_target, dest, **kw).rows_written == 5
    runs = _rows(bq, f"SELECT COUNT(*) AS n FROM {state}")[0].n
    with pg_conn.cursor() as cur:
        # Twelve integer digits: past Decimal(10,2), so it becomes NULL.
        cur.execute(
            f"UPDATE \"{dest}\" SET amount = 123456789012.5, write_date = '2024-02-01' "
            "WHERE id = 1"
        )
    with pytest.raises(RuntimeError, match="fail_on_warnings: coerced_decimal") as err:
        _sync(pg_source, bq_target, dest, fail_on_warnings={"coerced_decimal"}, **kw)
    assert "destination is untouched" in str(err.value)
    amount = _rows(bq, f"SELECT amount FROM {_fq(bq, dest)} WHERE id = 1")[0].amount
    assert float(amount) == 1.5, "no MERGE ran"
    assert _rows(bq, f"SELECT COUNT(*) AS n FROM {state}")[0].n == runs, "no cursor saved"
    assert _staging(bq, dest) == [], "the staged rows were dropped"

    r = _sync(pg_source, bq_target, dest, **kw)
    assert r.rows_written == 1, "the same range is read again"
    assert {w.kind for w in r.warnings} >= {"coerced_decimal"}
    assert _rows(bq, f"SELECT amount FROM {_fq(bq, dest)} WHERE id = 1")[0].amount is None


def test_an_unclustered_destination_is_reported_on_every_merge(
    bq, pg_conn, pg_source, bq_target, dest
):
    """Issue #24: the clustering lookup behind unclustered_merge_target is now
    read once per process, but the warning is still raised on every MERGE."""
    bigquery = pytest.importorskip("google.cloud.bigquery")
    bq.create_table(
        bigquery.Table(
            f"{bq.project}.{BQ_DATASET}.{dest}",
            schema=[
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("amount", "FLOAT"),
                bigquery.SchemaField("write_date", "DATETIME", mode="REQUIRED"),
            ],
        )
    )
    _seed(pg_conn, dest, range(1, 6))
    first = _sync(pg_source, bq_target, dest)
    _add(pg_conn, dest, range(6, 9), "2024-02-01 00:00:00")
    second = _sync(pg_source, bq_target, dest)
    assert second.rows_written == 3
    for r in (first, second):
        assert "unclustered_merge_target" in {w.kind for w in r.warnings}, r.warnings


def test_a_full_refresh_swaps_in_through_insert_all(bq, pg_conn, pg_source, dest):
    """Issue #19 moved every BigQuery job submit onto a retried path, the
    full refresh's swap and the clone included; insertAll is the other write
    method. A refresh lands exactly the source's rows."""
    target = quickhouse.BigQuery(
        project_id=BQ_PROJECT, dataset_id=BQ_DATASET, write_method="insert_all"
    )
    _seed(pg_conn, dest, range(1, 51))
    kw = dict(dest_table=dest, source_table=dest, mode="full", create_if_missing=True)
    assert quickhouse.sync(pg_source, target, **kw).rows_written == 50
    with pg_conn.cursor() as cur:
        cur.execute(f'DELETE FROM "{dest}" WHERE id > 40')
    r = quickhouse.sync(pg_source, target, allow_full_refresh_shrink=True, **kw)
    assert r.rows_written == 40
    got = _rows(bq, f"SELECT COUNT(*) AS n, MAX(id) AS hi FROM {_fq(bq, dest)}")[0]
    assert (got.n, got.hi) == (40, 40)
    assert _staging(bq, dest) == []


@pytest.fixture
def bq_source():
    return quickhouse.BigQuery(project_id=BQ_PROJECT)


def _bq_table(bq, table, rows):
    """A BigQuery source table of `(id, updated_at)` rows."""
    bq.query(
        f"CREATE OR REPLACE TABLE {_fq(bq, table)} (id INT64 NOT NULL, updated_at DATETIME) "
        f"AS SELECT * FROM UNNEST([{rows}])"
    ).result()


def test_a_bigquery_source_with_no_watermark_reports_it(
    bq, bq_source, ch_client, ch_target, dest
):
    """Issue #15 on the BigQuery-source flow: every row's watermark is NULL,
    so there is no cursor to save and every run reads them all again."""
    _bq_table(
        bq,
        dest,
        ", ".join(f"STRUCT({i} AS id, CAST(NULL AS DATETIME) AS updated_at)" for i in range(1, 6)),
    )
    ch_client.command(f"DROP TABLE IF EXISTS `{dest}`")
    kw = dict(
        dest_table=dest, source_table=f"{BQ_DATASET}.{dest}", mode="incremental",
        watermark="updated_at", key=["id"], create_if_missing=True,
        engine="ReplacingMergeTree()", order_by=["id"], state_key=f"{dest}:bq",
    )
    try:
        for _ in range(2):
            r = quickhouse.sync(bq_source, ch_target, **kw)
            assert r.rows_read == 5
            assert [(w.kind, w.count) for w in r.warnings if w.kind == "null_watermark"] == [
                ("null_watermark", 5)
            ], r.warnings
    finally:
        ch_client.command(f"DROP TABLE IF EXISTS `{dest}`")


def test_a_bigquery_source_with_no_watermark_into_a_replacing_merge_tree_says_so(
    bq, bq_source, ch_client, ch_target, dest
):
    """Issue #27 on the BigQuery-source flow: into a ReplacingMergeTree
    versioned by the watermark, which can't hold a NULL, the rows with no
    watermark are left out rather than failing the insert, and counted (the
    MAX is NULL only when every watermark is, and BigQuery counts for free)."""
    _bq_table(
        bq,
        dest,
        ", ".join(f"STRUCT({i} AS id, CAST(NULL AS DATETIME) AS updated_at)" for i in range(1, 6)),
    )
    ch_client.command(f"DROP TABLE IF EXISTS `{dest}`")
    kw = dict(
        dest_table=dest, source_table=f"{BQ_DATASET}.{dest}", mode="incremental",
        watermark="updated_at", key=["id"], create_if_missing=True, state_key=f"{dest}:bq",
    )
    try:
        for _ in range(2):
            r = quickhouse.sync(bq_source, ch_target, **kw)
            assert r.rows_written == 0
            assert [(w.kind, w.count) for w in r.warnings if w.kind == "null_watermark"] == [
                ("null_watermark", 5)
            ], r.warnings
    finally:
        ch_client.command(f"DROP TABLE IF EXISTS `{dest}`")


def test_a_bigquery_source_query_below_the_cursor_names_the_filter(
    bq, bq_source, ch_client, ch_target, dest
):
    """Issue #23 on the BigQuery-source flow: a source_query's MAX below the
    cursor may only be its filter, which the warning now says. The cursor
    still goes back to that MAX: a re-read at most."""
    _bq_table(
        bq,
        dest,
        ", ".join(
            f"STRUCT({i} AS id, DATETIME '{'2024-01-01' if i <= 5 else '2024-03-01'} 00:00:00' "
            "AS updated_at)"
            for i in range(1, 11)
        ),
    )
    ch_client.command(f"DROP TABLE IF EXISTS `{dest}`")
    state_key = f"{dest}:bqq"
    kw = dict(
        dest_table=dest,
        source_query=f"SELECT id, updated_at FROM `{bq.project}.{BQ_DATASET}.{dest}` WHERE id <= 5",
        mode="incremental", watermark="updated_at", key=["id"], create_if_missing=True,
        engine="ReplacingMergeTree()", order_by=["id"], state_key=state_key,
    )

    def cursor():
        return ch_client.command(
            "SELECT last_watermark FROM _quickhouse_state FINAL "
            f"WHERE source_table = '{state_key}' ORDER BY run_ts DESC LIMIT 1"
        )

    try:
        assert quickhouse.sync(bq_source, ch_target, **kw).rows_written == 5
        ch_client.command(
            "INSERT INTO _quickhouse_state "
            "(source_table, dest_table, last_watermark, rows, chunk_cursor, chunk_upper) "
            f"VALUES ('{state_key}', '{dest}', '2024-02-01 00:00:00', 0, '', '')"
        )
        r = quickhouse.sync(bq_source, ch_target, **kw)
        (ahead,) = [w for w in r.warnings if w.kind == "watermark_ahead_of_source"]
        assert "source_query alone" in ahead.message, ahead.message
        assert r.new_watermark is not None and r.new_watermark.startswith("2024-01-01")
        assert cursor().startswith("2024-01-01")
    finally:
        ch_client.command(f"DROP TABLE IF EXISTS `{dest}`")


def test_a_chunked_read_whose_max_is_too_costly_lands_every_row(
    bq, pg_conn, pg_source, bq_target, dest
):
    """A chunk_rows read whose MAX(watermark) priced above probe_max_cost was
    switched to a windowed sweep, which bypassed the chunk loop: every row
    went into the template staging table each chunk is cloned from, and was
    dropped with it, while the cursor advanced past them. Measured on 0.20.6:
    rows_written=120, 0 rows in BigQuery. Every such run since chunk_rows came
    to BigQuery in 0.20.2."""
    _seed(pg_conn, dest, range(1, 121))
    r = _sync(
        pg_source, bq_target, dest, chunk_rows=50, probe_max_cost=1.0,
        lookback_seconds=86_400, read_window_rows=30,
    )
    assert r.rows_written == 120
    got = _rows(bq, f"SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS k FROM {_fq(bq, dest)}")[0]
    assert (got.n, got.k) == (120, 120)
    assert _staging(bq, dest) == []


def test_a_resume_marker_from_0_20_8_resumes_and_another_reads_does_not(
    bq, pg_conn, pg_source, bq_target, dest
):
    """Issue #32 on BigQuery. A state table from 0.20.8 has no chunk_read
    column, so its in-flight marker can't say which read wrote it: the next
    chunked run resumes it as before, and the table gains the column. A
    marker another read wrote is not resumed: the run reads from the saved
    cursor and reports it."""
    bigquery = pytest.importorskip("google.cloud.bigquery")
    state = f"{bq.project}.{BQ_DATASET}.{dest}_state"
    old = bigquery.Table(
        state,
        schema=[
            bigquery.SchemaField("source_table", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("dest_table", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("last_watermark", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("rows", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("chunk_cursor", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("chunk_upper", "STRING", mode="NULLABLE"),
        ],
    )
    old.clustering_fields = ["source_table", "dest_table"]
    bq.create_table(old)
    _existing_dest(bq, dest)
    columns = "(source_table, dest_table, last_watermark, `rows`, run_ts, chunk_cursor, chunk_upper"
    # What 0.20.8 left after committing the chunks up to id 100.
    bq.query(
        f"INSERT INTO `{state}` {columns}) VALUES "
        f"('{dest}', '{dest}', '', 0, CURRENT_TIMESTAMP(), '100', '2024-01-01 00:00:00')"
    ).result()
    _seed(pg_conn, dest, range(1, 301))

    r = _sync(pg_source, bq_target, dest, chunk_rows=50)
    assert r.rows_read == 200, "resumed past id 100"
    assert "abandoned_resume_marker" not in {w.kind for w in r.warnings}, r.warnings
    assert "chunk_read" in [f.name for f in bq.get_table(state).schema]

    # A marker another read wrote, past the saved cursor.
    bq.query(
        f"INSERT INTO `{state}` {columns}, chunk_read) VALUES "
        f"('{dest}', '{dest}', '2024-01-01 00:00:00', 0, CURRENT_TIMESTAMP(), '250', "
        "'2024-01-01 00:00:00', '0000000000000000')"
    ).result()
    _add(pg_conn, dest, range(301, 311), "2024-02-01 00:00:00")
    r = _sync(pg_source, bq_target, dest, chunk_rows=50)
    assert r.rows_read == 10, "read from the saved cursor, not past the other read's marker"
    (abandoned,) = [w for w in r.warnings if w.kind == "abandoned_resume_marker"]
    assert abandoned.sample == "250"


def test_a_destination_column_the_source_does_not_feed_is_reported_unless_it_has_a_default(
    bq, pg_conn, pg_source, bq_target, dest
):
    """Issue #28 on BigQuery. The MERGE sets only the columns the source
    feeds: a destination-only column is NULL on the rows it inserts, and the
    run says so before writing. One with a DEFAULT gets its default there, so
    it isn't reported."""
    bigquery = pytest.importorskip("google.cloud.bigquery")
    bq.create_table(
        bigquery.Table(
            f"{bq.project}.{BQ_DATASET}.{dest}",
            schema=[
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("amount", "FLOAT"),
                bigquery.SchemaField("write_date", "DATETIME", mode="REQUIRED"),
                bigquery.SchemaField("legacy", "STRING"),
                bigquery.SchemaField(
                    "loaded_at", "TIMESTAMP", default_value_expression="CURRENT_TIMESTAMP()"
                ),
            ],
        )
    )
    _seed(pg_conn, dest, range(1, 6))
    r = _sync(pg_source, bq_target, dest)
    assert r.rows_written == 5
    unsourced = [w.column for w in r.warnings if w.kind == "unsourced_destination_column"]
    assert unsourced == ["legacy"], r.warnings
    got = _rows(bq, f"SELECT COUNTIF(legacy IS NULL) AS legacy_null, "
                    f"COUNTIF(loaded_at IS NULL) AS loaded_null FROM {_fq(bq, dest)}")[0]
    assert (got.legacy_null, got.loaded_null) == (5, 0)


def test_a_dropped_destination_is_rebuilt_whole_on_bigquery(
    bq, pg_conn, pg_source, bq_target, dest
):
    """Issue #36 on BigQuery: dropping the destination left its cursor in the
    state table, so the next run recreated it with only the rows past that
    cursor. The run now reads as a first run, and says so."""
    _seed(pg_conn, dest, range(1, 11))
    assert _sync(pg_source, bq_target, dest).rows_written == 10
    bq.delete_table(_fq(bq, dest).strip("`"))
    _add(pg_conn, dest, range(11, 12), "2024-02-01 00:00:00")

    r = _sync(pg_source, bq_target, dest)
    assert r.rows_written == 11, "every row, not only the one past the old cursor"
    (recreated,) = [w for w in r.warnings if w.kind == "recreated_destination"]
    assert recreated.sample is not None and recreated.sample.startswith("2024-01-01")
    assert _rows(bq, f"SELECT COUNT(*) AS n FROM {_fq(bq, dest)}")[0].n == 11


def test_a_new_incremental_destination_is_clustered_by_its_key_first(
    bq, pg_conn, pg_source, bq_target, dest
):
    """Issue #44: the generated clustering put order_by before key, so the
    MERGE's key bound pruned nothing, and quickhouse raised
    unclustered_merge_target against a table it had created itself."""
    _seed(pg_conn, dest, range(1, 6))
    first = _sync(pg_source, bq_target, dest, order_by=["write_date"])
    assert bq.get_table(_fq(bq, dest).strip("`")).clustering_fields == ["id", "write_date"]
    _add(pg_conn, dest, range(6, 9), "2024-02-01 00:00:00")
    second = _sync(pg_source, bq_target, dest, order_by=["write_date"])
    assert second.rows_written == 3
    for r in (first, second):
        assert "unclustered_merge_target" not in {w.kind for w in r.warnings}, r.warnings


def test_a_new_full_refresh_destination_keeps_order_by_first(
    bq, pg_conn, pg_source, bq_target, dest
):
    """Issue #44 changed only an incremental table: a full refresh runs no
    MERGE, so order_by still leads its clustering."""
    _seed(pg_conn, dest, range(1, 6))
    r = quickhouse.sync(
        pg_source,
        bq_target,
        dest_table=dest,
        source_table=dest,
        mode="full",
        order_by=["write_date"],
        key=["id"],
        create_if_missing=True,
    )
    assert r.rows_written == 5
    assert bq.get_table(_fq(bq, dest).strip("`")).clustering_fields == ["write_date", "id"]


def test_a_null_key_is_left_out_of_a_merge_into_a_nullable_key_column(
    bq, pg_conn, pg_source, bq_target, dest
):
    """Issue #49: into an existing table whose key column is NULLABLE, a NULL
    key never matches in the MERGE, so each run that read the row inserted it
    again. It is left out now, and null_key counts it."""
    bigquery = pytest.importorskip("google.cloud.bigquery")
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{dest}"')
        cur.execute(f'CREATE TABLE "{dest}" (code text, n int, write_date timestamp NOT NULL)')
        cur.execute(
            f"INSERT INTO \"{dest}\" VALUES ('a', 1, '2024-01-01'), (NULL, 2, '2024-01-01'), "
            "('c', 3, '2024-01-01')"
        )
    bq.create_table(
        bigquery.Table(
            f"{bq.project}.{BQ_DATASET}.{dest}",
            schema=[
                bigquery.SchemaField("code", "STRING"),
                bigquery.SchemaField("n", "INTEGER"),
                bigquery.SchemaField("write_date", "DATETIME"),
            ],
        )
    )
    kw = dict(
        dest_table=dest, source_table=dest, mode="incremental", watermark="write_date",
        key=["code"], state_table_name=f"{dest}_state",
    )

    def null_keys(r):
        return [(w.column, w.count) for w in r.warnings if w.kind == "null_key"]

    r = quickhouse.sync(pg_source, bq_target, **kw)
    assert (r.rows_read, r.rows_written) == (3, 2)
    assert null_keys(r) == [("code", 1)]
    # The row changes, so the next run reads it again, and leaves it out again.
    with pg_conn.cursor() as cur:
        cur.execute(f"UPDATE \"{dest}\" SET write_date = '2024-02-01' WHERE code IS NULL")
    r = quickhouse.sync(pg_source, bq_target, **kw)
    assert (r.rows_read, r.rows_written) == (1, 0)
    assert null_keys(r) == [("code", 1)]
    got = _rows(bq, f"SELECT COUNT(*) AS n, COUNTIF(code IS NULL) AS null_codes FROM {_fq(bq, dest)}")
    assert (got[0].n, got[0].null_codes) == (2, 0)


def test_a_bigquery_source_leaves_out_a_null_key(bq, bq_source, ch_client, ch_target, dest):
    """Issue #49 on the BigQuery-source flow: a NULL in the key column failed
    the write with an Arrow error. The row is left out, and null_key counts
    it."""
    bq.query(
        f"CREATE OR REPLACE TABLE {_fq(bq, dest)} (code STRING, n INT64) AS "
        "SELECT * FROM UNNEST([STRUCT('a' AS code, 1 AS n), STRUCT(NULL, 2), STRUCT('c', 3)])"
    ).result()
    ch_client.command(f"DROP TABLE IF EXISTS `{dest}`")
    try:
        r = quickhouse.sync(
            bq_source, ch_target, dest_table=dest, source_table=f"{BQ_DATASET}.{dest}",
            mode="full", key=["code"], create_if_missing=True,
        )
        assert (r.rows_read, r.rows_written) == (3, 2)
        assert [(w.column, w.count) for w in r.warnings if w.kind == "null_key"] == [("code", 1)]
        assert ch_client.command(f"SELECT count() FROM `{dest}`") == 2
    finally:
        ch_client.command(f"DROP TABLE IF EXISTS `{dest}`")


def test_a_type_override_an_existing_column_ignores_is_reported_on_bigquery(
    bq, pg_conn, pg_source, bq_target, dest
):
    """Issue #50 on a BigQuery destination: type_overrides applies only when
    quickhouse creates the table, so an override naming a column the existing
    table types differently is reported before writing. One naming the type
    the column has, in BigQuery's other spelling (FLOAT64 for FLOAT), is not."""
    _existing_dest(bq, dest)
    _seed(pg_conn, dest, range(1, 6))
    with pytest.raises(RuntimeError, match="fail_on_warnings: ignored_type_override") as err:
        _sync(
            pg_source, bq_target, dest, type_overrides={"amount": "NUMERIC"},
            fail_on_warnings={"ignored_type_override"},
        )
    assert "on column 'amount'" in str(err.value)
    assert "'FLOAT'" in str(err.value) and "'NUMERIC'" in str(err.value)
    assert "(count" not in str(err.value)

    r = _sync(
        pg_source, bq_target, dest,
        type_overrides={"amount": "FLOAT64", "id": "INT64", "write_date": "DATETIME"},
    )
    assert r.rows_written == 5
    assert [w for w in r.warnings if w.kind == "ignored_type_override"] == [], r.warnings
    types = {f.name: f.field_type for f in bq.get_table(_fq(bq, dest).strip("`")).schema}
    assert types["amount"] == "FLOAT"


def test_a_type_override_an_existing_column_ignores_is_reported_from_a_bigquery_source(
    bq, bq_source, ch_client, ch_target, dest
):
    """Issue #50 on the BigQuery-source flow, into ClickHouse."""
    _bq_table(
        bq,
        dest,
        ", ".join(
            f"STRUCT({i} AS id, DATETIME '2024-01-01 00:00:00' AS updated_at)" for i in range(1, 4)
        ),
    )
    ch_client.command(f"DROP TABLE IF EXISTS `{dest}`")
    kw = dict(
        dest_table=dest, source_table=f"{BQ_DATASET}.{dest}", mode="full", key=["id"],
        create_if_missing=True,
    )
    try:
        first = quickhouse.sync(bq_source, ch_target, **kw)
        assert [w for w in first.warnings if w.kind == "ignored_type_override"] == []
        r = quickhouse.sync(
            bq_source, ch_target, type_overrides={"id": "Int32", "updated_at": "DATETIME"}, **kw
        )
        assert r.rows_written == 3
        got = [(w.column, w.sample) for w in r.warnings if w.kind == "ignored_type_override"]
        assert got == [("id", "Int64")], r.warnings
    finally:
        ch_client.command(f"DROP TABLE IF EXISTS `{dest}`")
