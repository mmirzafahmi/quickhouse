"""HttpApi -> ClickHouse, against a local mock HTTP server.

The server is a ``http.server`` on a free localhost port that answers every
request with the JSON body the test hands it, so these need ClickHouse (see
``docker-compose.yml``) but no network beyond it.

    docker compose up -d
    pip install -e '.[test]'
    maturin develop --release
    pytest tests/test_http_api.py -v
"""

from __future__ import annotations

import datetime as dt
import json
import threading
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import quickhouse


@pytest.fixture
def api_url():
    """Serve a JSON body on localhost: call it with the records, get the URL."""
    state = {"body": b"[]"}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 — the http.server hook name
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(state["body"])))
            self.end_headers()
            self.wfile.write(state["body"])

        def log_message(self, *args):  # keep pytest's output clean
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


def _drop(ch_client, table: str):
    ch_client.command(f"DROP TABLE IF EXISTS `{table}`")


def _column_type(ch_client, table: str, column: str) -> str:
    return ch_client.command(
        f"SELECT type FROM system.columns WHERE database = currentDatabase() "
        f"AND table = '{table}' AND name = '{column}'"
    )


def _sync(src, ch_target, table: str):
    return quickhouse.sync(src, ch_target, dest_table=table, mode="full", create_if_missing=True)


def test_an_epoch_milliseconds_timestamp_lands_as_its_instant(
    ch_client, ch_target, api_url, unique_name
):
    """Issue #42: 13 digits are epoch milliseconds. A value outside 1900-2299
    becomes NULL with a coerced_date warning instead of landing at 2299-12-31."""
    table = unique_name
    _drop(ch_client, table)
    url = api_url(
        [
            {"code": "A1", "ts": 1729750000000},  # milliseconds
            {"code": "A2", "ts": 1729750000},  # seconds
            {"code": "A3", "ts": 1729750000123},  # milliseconds, sub-second part kept
            {"code": "A4", "ts": 631152000000},  # 1990 in ms: 12 digits, read as seconds
            {"code": "A5", "ts": "9999-12-31T00:00:00Z"},
        ]
    )
    src = quickhouse.HttpApi(
        url, columns=[("code", "STRING"), ("ts", "TIMESTAMP")], from_date="2024-10-24"
    )
    try:
        result = _sync(src, ch_target, table)
        assert result.rows_written == 5
        rows = dict(
            ch_client.query(f"SELECT code, toUnixTimestamp64Micro(ts) FROM `{table}`").result_rows
        )
        # 1729750000 s is 2024-10-24 06:06:40 UTC.
        assert rows == {
            "A1": 1729750000_000000,
            "A2": 1729750000_000000,
            "A3": 1729750000_123000,
            "A4": None,
            "A5": None,
        }
        coerced = [w for w in result.warnings if w.kind == "coerced_date"]
        assert [(w.column, w.count) for w in coerced] == [("ts", 2)]
    finally:
        _drop(ch_client, table)


def test_a_declared_date_reads_epoch_milliseconds_and_nulls_out_of_range(
    ch_client, ch_target, api_url, unique_name
):
    table = unique_name
    _drop(ch_client, table)
    url = api_url([{"code": "A1", "d": 1729750000000}, {"code": "A2", "d": "2300-01-01"}])
    src = quickhouse.HttpApi(
        url, columns=[("code", "STRING"), ("d", "DATE")], from_date="2024-10-24"
    )
    try:
        result = _sync(src, ch_target, table)
        rows = dict(ch_client.query(f"SELECT code, d FROM `{table}`").result_rows)
        assert rows == {"A1": dt.date(2024, 10, 24), "A2": None}
        coerced = [w for w in result.warnings if w.kind == "coerced_date"]
        assert [(w.column, w.count) for w in coerced] == [("d", 1)]
    finally:
        _drop(ch_client, table)


def test_a_declared_numeric_is_created_as_an_exact_decimal(
    ch_client, ch_target, api_url, unique_name
):
    """Issue #43: a declared NUMERIC was created on ClickHouse as Float64."""
    table = unique_name
    _drop(ch_client, table)
    url = api_url([{"code": "A1", "amount": "123.45"}, {"code": "A2", "amount": "0.1"}])
    src = quickhouse.HttpApi(
        url, columns=[("code", "STRING"), ("amount", "NUMERIC")], from_date="2024-10-24"
    )
    try:
        result = _sync(src, ch_target, table)
        assert result.warnings == []
        assert _column_type(ch_client, table, "amount") == "Nullable(Decimal(38, 9))"
        rows = dict(ch_client.query(f"SELECT code, amount FROM `{table}`").result_rows)
        assert rows == {"A1": Decimal("123.45"), "A2": Decimal("0.1")}
    finally:
        _drop(ch_client, table)


def test_a_declared_numeric_keeps_an_existing_float64_column(
    ch_client, ch_target, api_url, unique_name
):
    """A table an earlier release created holds Float64 there: it isn't altered,
    and the insert doesn't fail. A NUMERIC column new to that table follows it."""
    table = unique_name
    _drop(ch_client, table)
    ch_client.command(
        f"CREATE TABLE `{table}` (code Nullable(String), amount Nullable(Float64)) "
        f"ENGINE = MergeTree ORDER BY tuple()"
    )
    url = api_url([{"code": "A1", "amount": "123.45", "tax": "1.5"}])
    src = quickhouse.HttpApi(
        url,
        columns=[("code", "STRING"), ("amount", "NUMERIC"), ("tax", "NUMERIC")],
        from_date="2024-10-24",
    )
    try:
        result = quickhouse.sync(src, ch_target, dest_table=table, mode="full", evolve_schema=True)
        assert result.rows_written == 1
        assert not [w for w in result.warnings if w.kind == "decimal_mapping_mixed"]
        assert _column_type(ch_client, table, "amount") == "Nullable(Float64)"
        assert _column_type(ch_client, table, "tax") == "Nullable(Float64)"
        rows = ch_client.query(f"SELECT code, amount, tax FROM `{table}`").result_rows
        assert rows == [("A1", 123.45, 1.5)]
    finally:
        _drop(ch_client, table)


def _rows(ch_client, table: str, columns: str = "code, n"):
    return sorted(ch_client.query(f"SELECT {columns} FROM `{table}`").result_rows)


def _only(result, kind: str):
    (warning,) = [w for w in result.warnings if w.kind == kind]
    return warning


def test_a_record_with_no_key_is_left_out_with_a_null_key_warning(
    ch_client, ch_target, api_url, unique_name
):
    """Issue #49 (#45 item 8): a record with no value for a key column landed
    with the key '', so such records shared one key, and no warning said so.
    They are left out now, and null_key names the column and counts them."""
    table = unique_name
    _drop(ch_client, table)
    url = api_url(
        [{"code": "A1", "n": 1}, {"n": 2}, {"code": None, "n": 3}, {"code": "A4", "n": 4}]
    )
    src = quickhouse.HttpApi(
        url, columns=[("code", "STRING"), ("n", "INTEGER")], from_date="2024-10-24"
    )
    try:
        r = quickhouse.sync(
            src, ch_target, dest_table=table, mode="full", key=["code"], create_if_missing=True
        )
        assert _rows(ch_client, table) == [("A1", 1), ("A4", 4)]
        assert (r.rows_read, r.rows_written) == (4, 2)
        w = _only(r, "null_key")
        assert (w.column, w.count) == ("code", 2)
        assert "left out" in w.message, w.message
    finally:
        _drop(ch_client, table)


def test_a_key_that_does_not_parse_is_left_out_not_stored_as_zero(
    ch_client, ch_target, api_url, unique_name
):
    """Issue #49: an INTEGER key that doesn't parse becomes NULL, which a key
    column can't hold, so it landed as 0. The record is left out, and
    coerced_scalar no longer says the value was coerced to NULL."""
    table = unique_name
    _drop(ch_client, table)
    url = api_url([{"id": 1, "n": 1}, {"id": "x2", "n": 2}])
    src = quickhouse.HttpApi(
        url, columns=[("id", "INTEGER"), ("n", "INTEGER")], from_date="2024-10-24"
    )
    try:
        r = quickhouse.sync(
            src, ch_target, dest_table=table, mode="full", key=["id"], create_if_missing=True
        )
        assert _rows(ch_client, table, "id, n") == [(1, 1)]
        null_key = _only(r, "null_key")
        assert (null_key.column, null_key.count) == ("id", 1)
        coerced = _only(r, "coerced_scalar")
        assert (coerced.column, coerced.count) == ("id", 1)
        assert "coerced to NULL" not in coerced.message, coerced.message
        assert "left out" in coerced.message, coerced.message
    finally:
        _drop(ch_client, table)


def test_a_watermark_that_cant_be_stored_is_left_out_of_a_replacing_merge_tree(
    ch_client, ch_target, api_url, unique_name
):
    """Issue #49: `ts` is the ReplacingMergeTree's version column, which can't
    be NULL. An out-of-range value (12 digits, read as seconds) or a missing
    one landed as 1970-01-01, so a newer version of a row lost to the stored
    one. coerced_date said "coerced to NULL" and null_watermark wasn't raised.
    The rows are left out now, as a database source's read leaves them out,
    and null_watermark counts them."""
    table = unique_name
    _drop(ch_client, table)
    url = api_url(
        [
            {"code": "A1", "ts": "2024-10-24T06:00:00Z"},
            {"code": "A2", "ts": 172975000000},
            {"code": "A3"},
        ]
    )
    src = quickhouse.HttpApi(
        url, columns=[("code", "STRING"), ("ts", "TIMESTAMP")], from_date="2024-01-01"
    )
    kw = dict(
        dest_table=table, mode="incremental", watermark="ts", key=["code"],
        create_if_missing=True, state_key=table,
    )
    try:
        r = quickhouse.sync(src, ch_target, **kw)
        assert _column_type(ch_client, table, "ts").startswith("DateTime64(6")
        assert _rows(ch_client, table, "code") == [("A1",)]
        assert (r.rows_read, r.rows_written) == (3, 1)
        w = _only(r, "null_watermark")
        assert (w.column, w.count) == ("ts", 2)
        assert "left out" in w.message, w.message
        coerced = _only(r, "coerced_date")
        assert (coerced.column, coerced.count) == ("ts", 1)
        assert "coerced to NULL" not in coerced.message, coerced.message
        assert "left out" in coerced.message, coerced.message
        assert not [x for x in r.warnings if x.kind == "null_key"], r.warnings
    finally:
        _drop(ch_client, table)


def test_a_watermark_that_can_hold_null_still_lands_as_null(
    ch_client, ch_target, api_url, unique_name
):
    """Issue #49 changes nothing where the destination's watermark can be
    NULL: into a MergeTree it isn't the version column, so the row lands with
    a NULL there, and coerced_date still says so."""
    table = unique_name
    _drop(ch_client, table)
    url = api_url([{"code": "A1", "ts": "2024-10-24T06:00:00Z"}, {"code": "A2", "ts": 172975000000}])
    src = quickhouse.HttpApi(
        url, columns=[("code", "STRING"), ("ts", "TIMESTAMP")], from_date="2024-01-01"
    )
    try:
        r = quickhouse.sync(
            src, ch_target, dest_table=table, mode="incremental", watermark="ts", key=["code"],
            engine="MergeTree", create_if_missing=True, state_key=table,
        )
        rows = _rows(ch_client, table, "code, ts IS NULL")
        assert rows == [("A1", False), ("A2", True)]
        assert not [x for x in r.warnings if x.kind in {"null_watermark", "null_key"}]
        assert "coerced to NULL" in _only(r, "coerced_date").message
    finally:
        _drop(ch_client, table)


def test_fail_on_null_key_stops_an_api_full_refresh_before_the_swap(
    ch_client, ch_target, api_url, unique_name
):
    """Issue #49: null_key can stop the run. Counted while reading, it is
    checked at the swap, so the destination keeps the rows it had."""
    table = unique_name
    _drop(ch_client, table)
    columns = [("code", "STRING"), ("n", "INTEGER")]
    kw = dict(dest_table=table, mode="full", key=["code"], create_if_missing=True)
    try:
        src = quickhouse.HttpApi(
            api_url([{"code": "A1", "n": 1}]), columns=columns, from_date="2024-10-24"
        )
        quickhouse.sync(src, ch_target, **kw)
        src = quickhouse.HttpApi(
            api_url([{"code": "A1", "n": 10}, {"n": 20}]), columns=columns, from_date="2024-10-24"
        )
        with pytest.raises(RuntimeError, match="fail_on_warnings: null_key") as err:
            quickhouse.sync(src, ch_target, fail_on_warnings={"null_key"}, **kw)
        assert "untouched" in str(err.value), err.value
        assert _rows(ch_client, table) == [("A1", 1)]
    finally:
        _drop(ch_client, table)


def test_a_type_override_an_existing_column_ignores_is_reported(
    ch_client, ch_target, api_url, unique_name
):
    """Issue #50, its own repro: type_overrides applies only when quickhouse
    creates the table, and the run into an existing one said nothing. It now
    names the column, its type and the override; the same type in another
    spelling is no news."""
    table = unique_name
    _drop(ch_client, table)
    url = api_url([{"code": "A1", "amount": "123.45"}])
    src = quickhouse.HttpApi(
        url, columns=[("code", "STRING"), ("amount", "NUMERIC")], from_date="2024-01-01"
    )
    kw = dict(dest_table=table, mode="full", key=["code"])
    try:
        first = quickhouse.sync(src, ch_target, create_if_missing=True, **kw)
        assert first.warnings == []
        r = quickhouse.sync(src, ch_target, type_overrides={"amount": "Decimal(12, 2)"}, **kw)
        assert r.rows_written == 1
        (w,) = [w for w in r.warnings if w.kind == "ignored_type_override"]
        assert (w.column, w.sample, w.count) == ("amount", "Nullable(Decimal(38, 9))", 0)
        assert "'Decimal(12, 2)'" in w.message, w.message
        assert _column_type(ch_client, table, "amount") == "Nullable(Decimal(38, 9))"

        r = quickhouse.sync(src, ch_target, type_overrides={"amount": "Decimal(38,9)"}, **kw)
        assert [w for w in r.warnings if w.kind == "ignored_type_override"] == []
    finally:
        _drop(ch_client, table)
