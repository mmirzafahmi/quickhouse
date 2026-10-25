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
