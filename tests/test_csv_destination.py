"""The CSV destination (new in 0.21.0): every source into ``quickhouse.Csv``.

Needs the PostgreSQL, MySQL and ClickHouse fixture servers (see
``docker-compose.yml``) for the sources; the output goes under pytest's
``tmp_path``.

    docker compose up -d
    maturin develop
    pytest tests/test_csv_destination.py -v
"""

from __future__ import annotations

import csv
import datetime as dt
import glob
import gzip
import io
import json
import os
import shutil
import sys
import threading
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import quickhouse as qh

from conftest import PG_DSN

# A source nothing listens on: a run refused before anything runs never
# notices.
DEAD_PG = qh.Postgres("postgresql://etl:etl@127.0.0.1:1/etl")


# ---- helpers ----


def _pg_table(pg_conn, table: str, ddl: str, rows_sql: str | None = None) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" ({ddl})')
        if rows_sql:
            cur.execute(f'INSERT INTO "{table}" {rows_sql}')


def _pg_drop(pg_conn, table: str) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')


def _parts(root, table: str) -> list[str]:
    return sorted(glob.glob(os.path.join(str(root), table, "part-*")))


def _manifest(root, table: str) -> dict:
    with open(os.path.join(str(root), table, "_quickhouse_manifest.json")) as f:
        return json.load(f)


def _read_text(path: str) -> str:
    if path.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
            return f.read()
    with open(path, encoding="utf-8", newline="") as f:
        return f.read()


def _rows(root, table: str, delimiter: str = ",", header: bool = True) -> tuple[list, list]:
    """(header, rows) across every part, read with the csv module; each part
    repeats the header."""
    head, rows = None, []
    for p in _parts(root, table):
        records = list(csv.reader(io.StringIO(_read_text(p)), delimiter=delimiter))
        if header:
            assert head in (None, records[0]), f"{p} has another header"
            head, records = records[0], records[1:]
        rows.extend(records)
    return head, rows


def _root_entries(root) -> list[str]:
    return sorted(os.listdir(str(root)))


def _state(root) -> dict:
    with open(os.path.join(str(root), "_quickhouse_state.json")) as f:
        return json.load(f)


# ---- PostgreSQL ----


def test_a_full_refresh_writes_each_type_as_documented(pg_conn, pg_source, unique_name, tmp_path):
    table = unique_name
    _pg_table(
        pg_conn,
        table,
        "id int PRIMARY KEY, name text, d date, ts timestamp, tstz timestamptz, "
        "amount numeric(12,2), flag boolean, raw bytea, f float8",
        """VALUES
          (1, 'plain', '2024-01-02', '2024-01-02 03:04:05.123456', '2024-01-02 03:04:05+07',
           12.50, true, '\\x6869', 0.1),
          (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
          (3, '', '1999-12-31', '1999-12-31 23:59:59', '1999-12-31 23:59:59+00',
           -0.05, false, '\\x', 1e20),
          (4, E'a,b "q"\\nline2', '2024-02-29', NULL, NULL, 0, NULL, NULL, -1.5)""",
    )
    try:
        result = qh.sync(
            pg_source, qh.Csv(tmp_path), dest_table=table, source_table=table, mode="full"
        )
        assert result.rows_written == 4
        assert result.warnings == []
        head, rows = _rows(tmp_path, table)
        assert head == ["id", "name", "d", "ts", "tstz", "amount", "flag", "raw", "f"]
        by_id = {r[0]: r for r in rows}
        assert by_id["1"] == [
            "1",
            "plain",
            "2024-01-02",
            "2024-01-02T03:04:05.123456",
            "2024-01-01T20:04:05.000000Z",  # +07 written as the UTC instant
            "12.50",
            "true",
            "aGk=",  # base64 of b"hi"
            "0.1",
        ]
        assert by_id["3"] == [
            "3",
            "",
            "1999-12-31",
            "1999-12-31T23:59:59.000000",
            "1999-12-31T23:59:59.000000Z",
            "-0.05",
            "false",
            "",
            "1e20",
        ]
        assert by_id["4"][1] == 'a,b "q"\nline2'
        assert by_id["4"][5] == "0.00"
        # The csv module reads NULL and "" alike; the text keeps them apart: a
        # NULL is nothing at all, an empty string (or empty bytes) is "".
        text = "".join(_read_text(p) for p in _parts(tmp_path, table))
        assert "\n2,,,,,,,,\n" in text
        assert '\n3,"",1999-12-31,' in text and ',false,"",1e20\n' in text
        # ...and a reader told to, such as pyarrow, reads them back apart.
        pacsv = pytest.importorskip("pyarrow.csv")
        opts = pacsv.ConvertOptions(strings_can_be_null=True, quoted_strings_can_be_null=False)
        back = {
            r["id"]: r
            for p in _parts(tmp_path, table)
            for r in pacsv.read_csv(p, convert_options=opts).to_pylist()
        }
        assert back[2]["name"] is None and back[3]["name"] == ""
        assert back[1]["d"] == dt.date(2024, 1, 2)
        assert str(back[1]["tstz"]) == "2024-01-01 20:04:05+00:00"
        assert back[1]["ts"].tzinfo is None
        m = _manifest(tmp_path, table)
        assert [c["name"] for c in m["columns"]] == head
        assert m["rows"] == 4 and sum(p["rows"] for p in m["parts"]) == 4
        assert (m["delimiter"], m["header"], m["compression"]) == (",", True, None)
        # Nothing but the table's folder: no staging folder, no state file
        # (a full refresh keeps no cursor).
        assert _root_entries(tmp_path) == [table]
    finally:
        _pg_drop(pg_conn, table)


def test_pandas_reads_the_parts_with_its_defaults(pg_conn, pg_source, unique_name, tmp_path):
    pd = pytest.importorskip("pandas")
    table = unique_name
    _pg_table(
        pg_conn,
        table,
        "id int, name text, amount numeric(10,2), flag boolean, d date, ts timestamptz",
        """VALUES (1, 'x', 1.25, true, '2024-01-02', '2024-01-02 03:04:05+00'),
                  (2, NULL, NULL, false, NULL, NULL)""",
    )
    try:
        qh.sync(pg_source, qh.Csv(tmp_path), dest_table=table, source_table=table, mode="full")
        df = pd.concat(pd.read_csv(p, parse_dates=["d", "ts"]) for p in _parts(tmp_path, table))
        df = df.sort_values("id").reset_index(drop=True)
        assert df["id"].tolist() == [1, 2]
        assert df["amount"].iloc[0] == 1.25 and pd.isna(df["amount"].iloc[1])
        assert df["flag"].tolist() == [True, False]
        assert df["d"].iloc[0] == pd.Timestamp("2024-01-02")
        assert df["ts"].iloc[0] == pd.Timestamp("2024-01-02 03:04:05", tz="UTC")
        assert pd.isna(df["name"].iloc[1])
    finally:
        _pg_drop(pg_conn, table)


def test_a_full_refresh_replaces_the_earlier_parts(pg_conn, pg_source, unique_name, tmp_path):
    table = unique_name
    _pg_table(pg_conn, table, "id int, v text", "SELECT g, 'v' || g FROM generate_series(1, 5) g")
    dst = qh.Csv(tmp_path, max_rows_per_file=2)
    try:
        qh.sync(pg_source, dst, dest_table=table, source_table=table, mode="full")
        first = _parts(tmp_path, table)
        assert len(first) == 3  # 5 rows, 2 per part
        with pg_conn.cursor() as cur:
            cur.execute(f"INSERT INTO \"{table}\" SELECT g, 'w' || g FROM generate_series(6, 8) g")
        result = qh.sync(pg_source, dst, dest_table=table, source_table=table, mode="full")
        assert result.rows_written == 8
        second = _parts(tmp_path, table)
        assert len(second) == 4
        assert not set(first) & set(second), "an old part survived the refresh"
        assert sorted(int(r[0]) for r in _rows(tmp_path, table)[1]) == list(range(1, 9))
        assert _manifest(tmp_path, table)["rows"] == 8
        assert _root_entries(tmp_path) == [table]
    finally:
        _pg_drop(pg_conn, table)


def test_the_shrink_guard_refuses_a_smaller_full_refresh(pg_conn, pg_source, unique_name, tmp_path):
    table = unique_name
    _pg_table(pg_conn, table, "id int", "SELECT generate_series(1, 10)")
    dst = qh.Csv(tmp_path)
    try:
        qh.sync(pg_source, dst, dest_table=table, source_table=table, mode="full")
        before = _parts(tmp_path, table)
        with pg_conn.cursor() as cur:
            cur.execute(f'DELETE FROM "{table}" WHERE id > 3')
        with pytest.raises(RuntimeError, match="refusing full-refresh") as err:
            qh.sync(pg_source, dst, dest_table=table, source_table=table, mode="full")
        msg = str(err.value)
        assert "10 row(s) with only 3" in msg
        # CSV can't upsert, so the way out it names is append.
        assert 'mode="append"' in msg and "incremental" not in msg
        assert _parts(tmp_path, table) == before
        assert _manifest(tmp_path, table)["rows"] == 10
        assert _root_entries(tmp_path) == [table]
        result = qh.sync(
            pg_source,
            dst,
            dest_table=table,
            source_table=table,
            mode="full",
            allow_full_refresh_shrink=True,
        )
        assert [(w.kind, w.count) for w in result.warnings] == [("full_refresh_shrink", 7)]
        assert _manifest(tmp_path, table)["rows"] == 3
    finally:
        _pg_drop(pg_conn, table)


def test_append_with_a_watermark_resumes_from_its_cursor(pg_conn, pg_source, unique_name, tmp_path):
    table = unique_name
    _pg_table(
        pg_conn,
        table,
        "id int PRIMARY KEY, updated_at timestamp NOT NULL",
        "SELECT g, timestamp '2024-01-01' + g * interval '1 hour' FROM generate_series(1, 3) g",
    )
    dst = qh.Csv(tmp_path)
    kw = dict(dest_table=table, source_table=table, mode="append", watermark="updated_at")
    try:
        first = qh.sync(pg_source, dst, **kw)
        assert first.rows_written == 3
        assert first.new_watermark.startswith("2024-01-01 03:00:00")
        assert len(_parts(tmp_path, table)) == 1
        with pg_conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO \"{table}\" SELECT g, timestamp '2024-01-01' + g * interval '1 hour' "
                "FROM generate_series(4, 5) g"
            )
        second = qh.sync(pg_source, dst, **kw)
        assert second.rows_written == 2, "the second run read past the cursor"
        third = qh.sync(pg_source, dst, **kw)
        assert third.rows_written == 0
        parts = _parts(tmp_path, table)
        assert len(parts) == 2, "an empty append adds no part"
        ids = sorted(int(r[0]) for r in _rows(tmp_path, table)[1])
        assert ids == [1, 2, 3, 4, 5]
        assert _manifest(tmp_path, table)["rows"] == 5
        # The cursor lives in the state file, keyed like a state table's.
        [entry] = _state(tmp_path)["keys"]
        assert (entry["state_key"], entry["dest_table"]) == (table, table)
        assert entry["last_watermark"].startswith("2024-01-01 05:00:00")
        [key] = qh.state_keys(dst)
        assert key.last_watermark == entry["last_watermark"] and key.state_rows == 1
        assert qh.compact_state(dst) == 0
        assert _root_entries(tmp_path) == ["_quickhouse_state.json", table]
    finally:
        _pg_drop(pg_conn, table)


def test_an_append_leaves_out_rows_with_no_watermark(pg_conn, pg_source, unique_name, tmp_path):
    """A row whose watermark is NULL can't be held behind a cursor, so an
    append would write it again on every run: it is left out and reported
    (null_watermark), even when no row has a watermark yet."""
    table = unique_name
    _pg_table(pg_conn, table, "id int, updated_at timestamp", "VALUES (1, NULL), (2, NULL)")
    dst = qh.Csv(tmp_path)
    kw = dict(dest_table=table, source_table=table, mode="append", watermark="updated_at")
    try:
        for _ in range(2):
            result = qh.sync(pg_source, dst, **kw)
            assert result.rows_written == 0
            assert [(w.kind, w.count) for w in result.warnings] == [("null_watermark", 2)]
        with pg_conn.cursor() as cur:
            cur.execute(f"INSERT INTO \"{table}\" VALUES (3, '2024-01-01')")
        assert qh.sync(pg_source, dst, **kw).rows_written == 1
        assert qh.sync(pg_source, dst, **kw).rows_written == 0
        assert [r[0] for r in _rows(tmp_path, table)[1]] == ["3"]
    finally:
        _pg_drop(pg_conn, table)


def test_a_deleted_folder_starts_its_append_over(pg_conn, pg_source, unique_name, tmp_path):
    """The cursor belongs to the folder it was saved for: with the folder gone,
    the next append reads from the start and says so (recreated_destination),
    as a dropped table does on the other destinations."""
    table = unique_name
    _pg_table(
        pg_conn,
        table,
        "id int, updated_at timestamp",
        "SELECT g, timestamp '2024-01-01' + g * interval '1 minute' FROM generate_series(1, 3) g",
    )
    dst = qh.Csv(tmp_path)
    kw = dict(dest_table=table, source_table=table, mode="append", watermark="updated_at")
    try:
        assert qh.sync(pg_source, dst, **kw).rows_written == 3
        assert qh.sync(pg_source, dst, **kw).rows_written == 0
        shutil.rmtree(tmp_path / table)
        result = qh.sync(pg_source, dst, **kw)
        assert result.rows_written == 3
        assert [w.kind for w in result.warnings] == ["recreated_destination"]
        assert sorted(int(r[0]) for r in _rows(tmp_path, table)[1]) == [1, 2, 3]
    finally:
        _pg_drop(pg_conn, table)


def test_an_append_with_other_columns_is_refused(pg_conn, pg_source, unique_name, tmp_path):
    table = unique_name
    _pg_table(
        pg_conn,
        table,
        "id int, name text, updated_at timestamp",
        "VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-02')",
    )
    dst = qh.Csv(tmp_path)
    kw = dict(dest_table=table, source_table=table, mode="append", watermark="updated_at")
    try:
        qh.sync(pg_source, dst, **kw)
        before = (_parts(tmp_path, table), _state(tmp_path))
        with pg_conn.cursor() as cur:
            cur.execute(f"INSERT INTO \"{table}\" VALUES (3, 'c', '2024-01-03')")
        with pytest.raises(RuntimeError, match="can't append") as err:
            qh.sync(pg_source, dst, exclude=["name"], **kw)
        msg = str(err.value)
        assert "doesn't write column(s) name" in msg
        assert "evolve_schema isn't supported for a CSV destination" in msg
        with pytest.raises(RuntimeError, match="another order"):
            qh.sync(
                pg_source,
                dst,
                dest_table=table,
                source_query=f'SELECT name, id, updated_at FROM "{table}"',
                mode="append",
                watermark="updated_at",
                state_key=table,
            )
        with pytest.raises(RuntimeError, match='delimiter ","'):
            qh.sync(pg_source, qh.Csv(tmp_path, delimiter=";"), **kw)
        assert (_parts(tmp_path, table), _state(tmp_path)) == before
        assert [n for n in _root_entries(tmp_path)] == ["_quickhouse_state.json", table]
    finally:
        _pg_drop(pg_conn, table)


def test_incremental_is_refused_before_anything_runs(tmp_path):
    with pytest.raises(RuntimeError) as err:
        qh.sync(
            DEAD_PG,
            qh.Csv(tmp_path / "out"),
            dest_table="t",
            source_table="t",
            mode="incremental",
            watermark="updated_at",
            key=["id"],
        )
    msg = str(err.value)
    assert "not supported for a CSV destination" in msg
    assert "can't update" in msg
    assert 'mode="append" with watermark=' in msg
    assert not (tmp_path / "out").exists()


def test_options_a_csv_destination_cannot_apply_are_refused(tmp_path):
    with pytest.raises(RuntimeError) as err:
        qh.sync(
            DEAD_PG,
            qh.Csv(tmp_path / "out"),
            dest_table="t",
            source_table="t",
            mode="full",
            engine="MergeTree",
            order_by=["id"],
            type_overrides={"id": "String"},
            not_null=["id"],
            evolve_schema=True,
            validate=lambda info: None,
        )
    msg = str(err.value)
    for option in ("engine=", "order_by=", "type_overrides=", "not_null=", "evolve_schema=", "validate="):
        assert option in msg, msg
    assert not (tmp_path / "out").exists()
    with pytest.raises(RuntimeError, match="not supported for a CSV destination"):
        qh.reconcile_keys(DEAD_PG, qh.Csv(tmp_path), dest_table="t", key="id", source_table="t")
    with pytest.raises(RuntimeError, match="not supported for a CSV destination"):
        qh.reconcile_values(
            DEAD_PG, qh.Csv(tmp_path), dest_table="t", key="id", window="id > 0", source_table="t"
        )


def test_a_bad_descriptor_value_fails_at_construction(tmp_path):
    with pytest.raises(ValueError, match="delimiter"):
        qh.Csv(tmp_path, delimiter=";;")
    with pytest.raises(ValueError, match="delimiter"):
        qh.Csv(tmp_path, delimiter='"')
    with pytest.raises(ValueError, match="compression"):
        qh.Csv(tmp_path, compression="zstd")
    with pytest.raises(ValueError, match="max_rows_per_file"):
        qh.Csv(tmp_path, max_rows_per_file=0)
    with pytest.raises(TypeError):
        qh.Csv(tmp_path, archive=qh.backup(destination="s3", bucket="b"))
    assert repr(qh.Csv("out", delimiter="\t", compression="gzip")) == (
        "Csv(path='out', delimiter='\\t', header=True, compression='gzip', "
        "max_rows_per_file=1000000)"
    )


def test_gzip_another_delimiter_and_no_header(pg_conn, pg_source, unique_name, tmp_path):
    table = unique_name
    _pg_table(pg_conn, table, "id int, s text", "VALUES (1, 'a;b'), (2, 'c,d'), (3, NULL)")
    try:
        qh.sync(
            pg_source,
            qh.Csv(tmp_path / "gz", compression="gzip"),
            dest_table=table,
            source_table=table,
            mode="full",
        )
        [part] = _parts(tmp_path / "gz", table)
        assert part.endswith(".csv.gz")
        with open(part, "rb") as f:
            assert f.read(2) == b"\x1f\x8b"
        head, rows = _rows(tmp_path / "gz", table)
        assert head == ["id", "s"] and sorted(rows) == [["1", "a;b"], ["2", "c,d"], ["3", ""]]
        assert _manifest(tmp_path / "gz", table)["compression"] == "gzip"

        qh.sync(
            pg_source,
            qh.Csv(tmp_path / "semi", delimiter=";"),
            dest_table=table,
            source_table=table,
            mode="full",
        )
        [part] = _parts(tmp_path / "semi", table)
        text = _read_text(part)
        # Only the delimiter in use is quoted for.
        assert '1;"a;b"\n' in text and "2;c,d\n" in text
        assert sorted(_rows(tmp_path / "semi", table, delimiter=";")[1])[0] == ["1", "a;b"]

        qh.sync(
            pg_source,
            qh.Csv(tmp_path / "nohead", header=False),
            dest_table=table,
            source_table=table,
            mode="full",
        )
        [part] = _parts(tmp_path / "nohead", table)
        assert not _read_text(part).startswith("id")
        _, rows = _rows(tmp_path / "nohead", table, header=False)
        assert sorted(rows) == [["1", "a;b"], ["2", "c,d"], ["3", ""]]
        m = _manifest(tmp_path / "nohead", table)
        assert m["header"] is False and [c["name"] for c in m["columns"]] == ["id", "s"]
    finally:
        _pg_drop(pg_conn, table)


def test_a_run_that_fails_partway_leaves_no_new_parts(pg_conn, pg_source, unique_name, tmp_path):
    """The read fails after thousands of rows have been written into staging
    (a division by zero at id 5000): the folder and the cursor stay as they
    were, and no staging folder is left behind."""
    table = unique_name
    _pg_table(
        pg_conn,
        table,
        "id int, updated_at timestamp",
        "SELECT g, timestamp '2024-01-01' + g * interval '1 second' FROM generate_series(1, 3) g",
    )
    query = f'SELECT id, updated_at, 1 / (id - 5000) AS boom FROM "{table}"'
    small = dict(batch_rows=100, insert_bytes=1)
    dst = qh.Csv(tmp_path, max_rows_per_file=500)
    try:
        for mode in ("full", "append"):
            dest = f"{table}_{mode}"
            kw = dict(dest_table=dest, source_query=query, mode=mode, state_key=dest, **small)
            if mode == "append":
                kw["watermark"] = "updated_at"
            first = qh.sync(pg_source, dst, **kw)
            assert first.rows_written == 3
            before = (_parts(tmp_path, dest), _manifest(tmp_path, dest))
            state_before = _state(tmp_path) if mode == "append" else None
            with pg_conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO \"{table}\" SELECT g, timestamp '2024-01-01' + g * interval "
                    "'1 second' FROM generate_series(4, 6000) g"
                )
            with pytest.raises(RuntimeError, match="division by zero"):
                qh.sync(pg_source, dst, **kw)
            assert (_parts(tmp_path, dest), _manifest(tmp_path, dest)) == before
            if mode == "append":
                assert _state(tmp_path) == state_before
            assert not [n for n in _root_entries(tmp_path) if "_quickhouse_tmp" in n]
            with pg_conn.cursor() as cur:
                cur.execute(f'DELETE FROM "{table}" WHERE id > 3')
    finally:
        _pg_drop(pg_conn, table)


def test_fail_on_warnings_stops_before_the_swap_and_before_the_append(
    pg_conn, pg_source, unique_name, tmp_path
):
    """A date the decoders can't represent (outside 1900-2299, as on every
    destination) is written empty with a coerced_date warning: named in
    fail_on_warnings, it stops a full refresh before its swap and an append
    before its parts move in and its cursor is saved."""
    table = unique_name
    _pg_table(
        pg_conn,
        table,
        "id int, d date, updated_at timestamp",
        "VALUES (1, '2024-01-01', '2024-01-01 00:00:01')",
    )
    dst = qh.Csv(tmp_path)
    try:
        qh.sync(pg_source, dst, dest_table=table, source_table=table, mode="full")
        app = f"{table}_app"
        akw = dict(dest_table=app, source_table=table, mode="append", watermark="updated_at")
        qh.sync(pg_source, dst, **akw)
        full_before = _parts(tmp_path, table)
        app_before, state_before = _parts(tmp_path, app), _state(tmp_path)
        with pg_conn.cursor() as cur:
            cur.execute(f"INSERT INTO \"{table}\" VALUES (2, '3000-01-01', '2024-01-01 00:00:02')")
        with pytest.raises(RuntimeError, match="coerced_date") as err:
            qh.sync(
                pg_source,
                dst,
                dest_table=table,
                source_table=table,
                mode="full",
                fail_on_warnings=["coerced_date"],
            )
        assert "dropped before the swap" in str(err.value)
        assert _parts(tmp_path, table) == full_before
        with pytest.raises(RuntimeError, match="coerced_date") as err:
            qh.sync(pg_source, dst, fail_on_warnings=["coerced_date"], **akw)
        assert "Nothing was appended" in str(err.value)
        assert "cursor was not saved" in str(err.value)
        assert _parts(tmp_path, app) == app_before
        assert _state(tmp_path) == state_before
        assert not [n for n in _root_entries(tmp_path) if "_quickhouse_tmp" in n]
        # Without it, the row lands with the date empty, and the warning says so.
        result = qh.sync(pg_source, dst, **akw)
        assert [w.kind for w in result.warnings] == ["coerced_date"]
        assert sorted(_rows(tmp_path, app)[1]) == [
            ["1", "2024-01-01", "2024-01-01T00:00:01.000000"],
            ["2", "", "2024-01-01T00:00:02.000000"],
        ]
    finally:
        _pg_drop(pg_conn, table)


def test_a_null_key_is_written_empty_not_left_out(pg_conn, pg_source, unique_name, tmp_path):
    """Every CSV column can be empty: a key with a NULL is neither left out
    (null_key) nor defaulted (null_defaulted), and parallel reads split on it."""
    table = unique_name
    _pg_table(pg_conn, table, "id int, v text", "SELECT g, 'v' FROM generate_series(1, 2000) g")
    with pg_conn.cursor() as cur:
        cur.execute(f"INSERT INTO \"{table}\" VALUES (NULL, 'no key')")
    try:
        result = qh.sync(
            pg_source,
            qh.Csv(tmp_path),
            dest_table=table,
            source_table=table,
            mode="full",
            key=["id"],
            parallelism=2,
        )
        assert result.rows_written == 2001
        assert result.warnings == []
        rows = _rows(tmp_path, table)[1]
        assert ["", "no key"] in rows
        assert len(_parts(tmp_path, table)) >= 2, "each parallel range writes its own part"
    finally:
        _pg_drop(pg_conn, table)


def test_a_folder_of_other_files_is_never_replaced(pg_conn, pg_source, unique_name, tmp_path):
    table = unique_name
    _pg_table(pg_conn, table, "id int", "VALUES (1)")
    (tmp_path / table).mkdir()
    (tmp_path / table / "notes.txt").write_text("mine")
    try:
        with pytest.raises(RuntimeError, match="no _quickhouse_manifest.json"):
            qh.sync(pg_source, qh.Csv(tmp_path), dest_table=table, source_table=table, mode="full")
        assert (tmp_path / table / "notes.txt").read_text() == "mine"
        assert _root_entries(tmp_path) == [table]
    finally:
        _pg_drop(pg_conn, table)


# ---- the other sources ----


def test_mysql_into_csv(mysql_conn, mysql_source, unique_name, tmp_path):
    table = unique_name
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"CREATE TABLE `{table}` (id INT PRIMARY KEY, name VARCHAR(20), "
            "amount DECIMAL(8,3), at DATETIME(6), updated_at DATETIME)"
        )
        cur.execute(
            f"INSERT INTO `{table}` VALUES (1, 'a', 1.5, '2024-01-02 03:04:05.5', "
            "'2024-01-01 00:00:01'), (2, '', NULL, NULL, '2024-01-01 00:00:02')"
        )
    dst = qh.Csv(tmp_path)
    try:
        qh.sync(mysql_source, dst, dest_table=table, source_table=table, mode="full")
        head, rows = _rows(tmp_path, table)
        assert head == ["id", "name", "amount", "at", "updated_at"]
        # A MySQL DATETIME lands as UTC by default (see the type mapping).
        assert sorted(rows) == [
            ["1", "a", "1.500", "2024-01-02T03:04:05.500000Z", "2024-01-01T00:00:01.000000Z"],
            ["2", "", "", "", "2024-01-01T00:00:02.000000Z"],
        ]
        app = f"{table}_app"
        kw = dict(dest_table=app, source_table=table, mode="append", watermark="updated_at")
        assert qh.sync(mysql_source, dst, **kw).rows_written == 2
        with mysql_conn.cursor() as cur:
            cur.execute(f"INSERT INTO `{table}` VALUES (3, 'c', 2, NULL, '2024-01-01 00:00:03')")
        assert qh.sync(mysql_source, dst, **kw).rows_written == 1
        assert sorted(int(r[0]) for r in _rows(tmp_path, app)[1]) == [1, 2, 3]
    finally:
        with mysql_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")


def test_clickhouse_into_csv(ch_client, ch_source, unique_name, tmp_path):
    table = unique_name
    ch_client.command(f"DROP TABLE IF EXISTS `{table}`")
    ch_client.command(
        f"CREATE TABLE `{table}` (id UInt32, name Nullable(String), "
        "amount Decimal(10, 2), at DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY id"
    )
    ch_client.command(
        f"INSERT INTO `{table}` VALUES (1, 'a', 1.25, '2024-01-02 03:04:05.123'), "
        "(2, NULL, 0, '2024-01-02 03:04:06')"
    )
    dst = qh.Csv(tmp_path)
    try:
        qh.sync(ch_source, dst, dest_table=table, source_table=table, mode="full")
        assert sorted(_rows(tmp_path, table)[1]) == [
            ["1", "a", "1.25", "2024-01-02T03:04:05.123000Z"],
            ["2", "", "0.00", "2024-01-02T03:04:06.000000Z"],
        ]
        app = f"{table}_app"
        kw = dict(dest_table=app, source_table=table, mode="append", watermark="at")
        assert qh.sync(ch_source, dst, **kw).rows_written == 2
        ch_client.command(f"INSERT INTO `{table}` VALUES (3, 'c', 3, '2024-01-02 03:04:07')")
        assert qh.sync(ch_source, dst, **kw).rows_written == 1
        assert sorted(int(r[0]) for r in _rows(tmp_path, app)[1]) == [1, 2, 3]
    finally:
        ch_client.command(f"DROP TABLE IF EXISTS `{table}`")


def test_a_dataframe_into_csv(unique_name, tmp_path):
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["a", None, ""],
            "at": pd.to_datetime(["2024-01-02 03:04:05", None, "2024-01-03 00:00:00"]),
            "amount": [Decimal("1.50"), None, Decimal("-2.25")],
        }
    )
    dst = qh.Csv(tmp_path)
    result = qh.from_pandas(df, dst, dest_table=unique_name, mode="full")
    assert result.rows_written == 3
    text = "".join(_read_text(p) for p in _parts(tmp_path, unique_name))
    assert text.splitlines()[0] == "id,name,at,amount"
    assert "1,a,2024-01-02T03:04:05.000000,1.50" in text
    assert "\n2,,,\n" in text
    assert '\n3,"",2024-01-03T00:00:00.000000,-2.25' in text
    # Append from a frame: each run adds its frame, with no cursor to keep.
    qh.from_pandas(df.iloc[:1], dst, dest_table=unique_name, mode="append")
    assert len(_rows(tmp_path, unique_name)[1]) == 4
    assert len(_parts(tmp_path, unique_name)) == 2
    pl = pytest.importorskip("polars")
    qh.from_pandas(
        pl.DataFrame({"id": [7], "name": ["p"], "at": [None], "amount": [None]}).with_columns(
            pl.col("at").cast(pl.Datetime("us")), pl.col("amount").cast(pl.Decimal(10, 2))
        ),
        dst,
        dest_table=f"{unique_name}_pl",
        mode="full",
    )
    assert _rows(tmp_path, f"{unique_name}_pl")[1] == [["7", "p", "", ""]]


@pytest.fixture
def api_url():
    """Serve a JSON body on localhost: call it with the records, get the URL.
    (As in ``test_http_api.py``.)"""
    state = {"body": b"[]"}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 — the http.server hook name
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(state["body"])))
            self.end_headers()
            self.wfile.write(state["body"])

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def serve(records) -> str:
        state["body"] = json.dumps(records).encode()
        return f"http://127.0.0.1:{server.server_address[1]}/records"

    yield serve
    server.shutdown()
    server.server_close()


def test_an_http_api_into_csv(api_url, unique_name, tmp_path):
    url = api_url(
        [
            {"code": "A1", "n": 3, "day": "2024-10-24", "ok": True},
            {"code": "", "n": None, "day": "2024-10-25", "ok": False},
        ]
    )
    columns = [("code", "STRING"), ("n", "INT64"), ("day", "DATE"), ("ok", "BOOL")]
    src = qh.HttpApi(url, columns=columns, from_date="2024-10-24", to_date="2024-10-25")
    dst = qh.Csv(tmp_path)
    result = qh.sync(src, dst, dest_table=unique_name, mode="full")
    assert result.rows_written == 2
    text = "".join(_read_text(p) for p in _parts(tmp_path, unique_name))
    assert text == 'code,n,day,ok\nA1,3,2024-10-24,true\n"",,2024-10-25,false\n'
    # Append keeps the window's end as its cursor, in the state file.
    app = f"{unique_name}_app"
    result = qh.sync(src, dst, dest_table=app, mode="append", watermark="day")
    assert result.rows_written == 2
    assert result.new_watermark == "2024-10-25"
    [entry] = _state(tmp_path)["keys"]
    assert entry["dest_table"] == app and entry["last_watermark"] == "2024-10-25"
    assert len(_parts(tmp_path, app)) == 1


# ---- the CLI ----


@pytest.mark.skipif(sys.version_info < (3, 11), reason="the CLI reads TOML with tomllib")
def test_the_cli_runs_a_job_into_a_csv_target(pg_conn, unique_name, tmp_path, capsys):
    from quickhouse.__main__ import main

    table = unique_name
    _pg_table(pg_conn, table, "id int, s text", "VALUES (1, 'x'), (2, NULL)")
    out = tmp_path / "out"
    job = tmp_path / "job.toml"
    job.write_text(
        f"""
[source]
type = "postgres"
dsn = "{PG_DSN}"

[target]
type = "csv"
path = "{out}"
compression = "gzip"

[sync]
dest_table = "{table}"
source_table = "{table}"
mode = "full"
"""
    )
    try:
        main(["run", str(job)])
        assert "rows_written=2" in capsys.readouterr().out
        [part] = _parts(out, table)
        assert part.endswith(".csv.gz")
        assert sorted(_rows(out, table)[1]) == [["1", "x"], ["2", ""]]
    finally:
        _pg_drop(pg_conn, table)
