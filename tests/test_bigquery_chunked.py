"""Live integration tests: PostgreSQL -> BigQuery with ``chunk_rows`` (issue #4).

BigQuery has no emulator for the Storage Write API or ``MERGE``, so these run
only against a real dataset you can write to, and skip otherwise (the default,
and in CI):

    QUICKHOUSE_BQ_DATASET=my_scratch_dataset pytest tests/test_bigquery_chunked.py -v

Credentials come from Application Default Credentials; ``QUICKHOUSE_BQ_PROJECT``
overrides the project they resolve to. Reading results back needs the
``google-cloud-bigquery`` package. Every table a test creates, and its rows in
``_quickhouse_state``, are removed afterwards. The tables are a few hundred
rows, so a run bills a handful of tiny ``MERGE`` statements.
"""

from __future__ import annotations

import os

import pytest

import quickhouse

try:
    from google.api_core.exceptions import NotFound
except ImportError:  # the live tests skip without google-cloud-bigquery anyway
    NotFound = Exception

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
    """A destination table name, with everything it leaves behind removed."""
    yield unique_name
    for t in bq.list_tables(f"{bq.project}.{BQ_DATASET}"):
        if t.table_id.startswith(unique_name):
            bq.delete_table(t, not_found_ok=True)
    try:
        bq.query(
            f"DELETE FROM {_fq(bq, '_quickhouse_state')} WHERE dest_table = '{unique_name}'"
        ).result()
    except NotFound:
        pass  # no test in this run used the default state table
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{unique_name}"')


def _fq(bq, table):
    return f"`{bq.project}.{BQ_DATASET}.{table}`"


def _scalar(bq, sql):
    return list(bq.query(sql).result())[0][0]


def _seed(pg_conn, table, ids, ts="2024-01-01 00:00:00"):
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(
            f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, amount double precision, '
            "write_date timestamp NOT NULL)"
        )
        with cur.copy(f'COPY "{table}" (id, amount, write_date) FROM STDIN') as copy:
            for i in ids:
                copy.write_row((i, i * 1.5, ts))


def _sync(pg_source, bq_target, table, **kw):
    kw.setdefault("source_table", table)
    kw.setdefault("chunk_rows", 40)
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


def _landed(bq, table):
    """(rows, distinct ids) in the destination."""
    row = list(bq.query(f"SELECT COUNT(*), COUNT(DISTINCT id) FROM {_fq(bq, table)}").result())[0]
    return row[0], row[1]


def _state(bq, table, state_key=None):
    """This destination's state rows, oldest first."""
    where = f"dest_table = '{table}'"
    if state_key:
        where += f" AND source_table = '{state_key}'"
    return list(
        bq.query(
            "SELECT source_table, last_watermark, chunk_cursor, chunk_upper "
            f"FROM {_fq(bq, '_quickhouse_state')} WHERE {where} ORDER BY run_ts"
        ).result()
    )


def _leftover_staging(bq, table):
    return [
        t.table_id
        for t in bq.list_tables(f"{bq.project}.{BQ_DATASET}")
        if t.table_id.startswith(f"{table}_quickhouse_tmp")
    ]


def test_every_row_lands_once_and_the_cursor_advances_per_chunk(
    bq, pg_conn, pg_source, bq_target, dest
):
    _seed(pg_conn, dest, range(1, 251))
    r = _sync(pg_source, bq_target, dest)
    assert r.rows_written == 250
    assert _landed(bq, dest) == (250, 250)

    state = _state(bq, dest)
    markers = [s.chunk_cursor for s in state if s.chunk_cursor]
    # 40-row chunks: six full ones and a short seventh, each committed.
    assert markers == ["40", "80", "120", "160", "200", "240", "250"]
    # The finished run clears the marker and commits the source MAX.
    assert state[-1].chunk_cursor is None
    assert state[-1].last_watermark == "2024-01-01 00:00:00"
    assert _leftover_staging(bq, dest) == []

    # Nothing new: a quiet run moves nothing.
    assert _sync(pg_source, bq_target, dest).rows_written == 0


def test_an_interrupted_run_resumes_at_the_next_chunk(bq, pg_conn, pg_source, bq_target, dest):
    _seed(pg_conn, dest, range(1, 251))
    assert _sync(pg_source, bq_target, dest).rows_written == 250
    with pg_conn.cursor() as cur:
        cur.execute(f"UPDATE \"{dest}\" SET write_date = '2024-02-01', amount = -1")
    # What a run cut short after its third chunk leaves: the old committed
    # cursor, and a marker at id 120 within the window up to the new MAX.
    state_key = _state(bq, dest)[-1].source_table
    bq.query(
        f"INSERT INTO {_fq(bq, '_quickhouse_state')} "
        "(source_table, dest_table, last_watermark, `rows`, run_ts, chunk_cursor, chunk_upper) "
        f"VALUES ('{state_key}', '{dest}', '2024-01-01 00:00:00', 120, CURRENT_TIMESTAMP(), "
        "'120', '2024-02-01 00:00:00')"
    ).result()

    r = _sync(pg_source, bq_target, dest)
    # Ids 121..250 only: nothing at or below the marker is read again.
    assert r.rows_written == 130
    assert _scalar(bq, f"SELECT COUNT(*) FROM {_fq(bq, dest)} WHERE amount = -1") == 130
    assert _scalar(bq, f"SELECT MIN(id) FROM {_fq(bq, dest)} WHERE amount = -1") == 121
    assert _landed(bq, dest) == (250, 250)
    last = _state(bq, dest)[-1]
    assert last.chunk_cursor is None
    assert last.last_watermark == "2024-02-01 00:00:00"


def test_another_sync_into_the_same_table_does_not_move_the_chunked_cursor(
    bq, pg_conn, pg_source, bq_target, dest
):
    _seed(pg_conn, dest, range(1, 101))
    assert _sync(pg_source, bq_target, dest, state_key="chunked").rows_written == 100
    with pg_conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO \"{dest}\" SELECT g, g * 1.5, '2024-03-01' FROM generate_series(101, 150) g"
        )
    # A second sync lands only the newest ids, under its own state_key.
    other = _sync(
        pg_source,
        bq_target,
        dest,
        state_key="other",
        source_table=None,
        source_query=f'SELECT * FROM "{dest}" WHERE id > 130',
        chunk_rows=None,
    )
    assert other.rows_written == 20
    # The chunked sync still reads everything past its own cursor.
    assert _sync(pg_source, bq_target, dest, state_key="chunked").rows_written == 50
    assert _landed(bq, dest) == (150, 150)


def test_gaps_in_the_key_keep_chunks_bounded(bq, pg_conn, pg_source, bq_target, dest):
    ids = [*range(1, 31), *range(1_000, 1_051), *range(90_000, 90_019)]
    _seed(pg_conn, dest, ids)
    assert _sync(pg_source, bq_target, dest).rows_written == len(ids)
    assert _landed(bq, dest) == (len(ids), len(ids))
    markers = [int(s.chunk_cursor) for s in _state(bq, dest) if s.chunk_cursor]
    # Keyset, not arithmetic: each chunk is the next 40 ids that exist.
    assert markers == [ids[39], ids[79], ids[-1]]


def test_a_state_table_from_an_earlier_version_gains_the_chunk_columns(
    bq, pg_conn, pg_source, bq_target, dest
):
    """A state table created before 0.20.2 has no chunk columns. The first
    chunked run reads it without error (there can be no marker yet), adds the
    columns in place, keeps the rows it holds, and resumes from them."""
    bigquery = pytest.importorskip("google.cloud.bigquery")
    state_table = f"{dest}_state"
    old = bigquery.Table(
        f"{bq.project}.{BQ_DATASET}.{state_table}",
        schema=[
            bigquery.SchemaField("source_table", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("dest_table", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("last_watermark", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("rows", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
        ],
    )
    bq.create_table(old)
    _seed(pg_conn, dest, range(1, 101))
    kw = dict(state_table_name=state_table, chunk_rows=None)
    # An earlier, unchunked run: its cursor lands in the old-schema table.
    assert _sync(pg_source, bq_target, dest, **kw).rows_written == 100
    assert {f.name for f in bq.get_table(old).schema} >= {"chunk_cursor", "chunk_upper"}
    # (The migration already ran on that first incremental run; put the old
    # schema back to take the chunked run through it as an upgrade would.)
    bq.delete_table(old)
    bq.create_table(old)
    bq.query(
        f"INSERT INTO {_fq(bq, state_table)} (source_table, dest_table, last_watermark, `rows`, "
        f"run_ts) VALUES ('{dest}', '{dest}', '2024-01-01 00:00:00', 100, CURRENT_TIMESTAMP())"
    ).result()

    with pg_conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO \"{dest}\" SELECT g, g * 1.5, '2024-02-01' FROM generate_series(101, 190) g"
        )
    kw["chunk_rows"] = 40
    assert _sync(pg_source, bq_target, dest, **kw).rows_written == 90
    assert _landed(bq, dest) == (190, 190)
    rows = list(
        bq.query(
            f"SELECT last_watermark, chunk_cursor FROM {_fq(bq, state_table)} ORDER BY run_ts"
        ).result()
    )
    assert rows[0].last_watermark == "2024-01-01 00:00:00"  # kept
    assert [r.chunk_cursor for r in rows if r.chunk_cursor] == ["140", "180", "190"]
    assert rows[-1].chunk_cursor is None
    assert rows[-1].last_watermark == "2024-02-01 00:00:00"
