"""Tests for the Great Expectations data-quality gate (``sync(validate=...)``).

The two ``test_gate_*`` tests are end-to-end (PostgreSQL -> ClickHouse) and need
the ``docker-compose.yml`` services plus ``pip install -e '.[test]'`` (which
pulls in great-expectations + clickhouse-sqlalchemy). They skip when a service
or dependency is missing. The other tests are pure Python — no services needed.

    docker compose up -d
    pip install -e '.[test]'
    maturin develop --release
    pytest tests/test_quality.py -v
"""

from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import pytest

import quickhouse

CH_HOST = os.environ.get("QUICKHOUSE_CH_HOST", "localhost")
CH_PORT = int(os.environ.get("QUICKHOUSE_CH_PORT", "8123"))
CH_DB = os.environ.get("QUICKHOUSE_CH_DB", "default")
CH_USER = os.environ.get("QUICKHOUSE_CH_USER", "default")
CH_PASSWORD = os.environ.get("QUICKHOUSE_CH_PASSWORD", "")


# ---- pure-Python tests (no services) ----


def test_missing_great_expectations_gives_clear_error(monkeypatch):
    """A ``Validation`` used without great-expectations installed raises a
    friendly ImportError pointing at the extra — not an obscure one."""
    monkeypatch.setitem(sys.modules, "great_expectations", None)
    v = quickhouse.Validation(suite=object(), context=object(), datasource="ds")
    info = SimpleNamespace(staging_table="t_stg", database="default",
                           dest_kind="clickhouse", rows_written=1)
    with pytest.raises(ImportError, match=r"quickhouse\[quality\]"):
        v(info)


def test_validate_on_append_is_rejected(ch_target):
    """Append mode has no staging table to gate (a bronze-landing direct
    insert), so quickhouse rejects a gate on it up front, before any network —
    this needs only the (non-connecting) descriptors."""
    src = quickhouse.HttpApi("https://example.invalid/data", columns=[("a", "STRING")])
    calls = []
    with pytest.raises(RuntimeError, match="append mode"):
        quickhouse.sync(
            src,
            ch_target,
            dest_table="does_not_matter",
            mode="append",
            watermark="a",
            validate=lambda info: calls.append(info),
        )
    assert calls == [], "the gate must not fire on a rejected path"


def test_validate_with_chunk_rows_is_rejected(pg_source, ch_target):
    """chunk_rows commits each chunk straight into the destination, so there's
    no single staging table to gate — rejected up front (no network needed;
    ClickHouse's HTTP client doesn't connect until a request is issued)."""
    calls = []
    with pytest.raises(RuntimeError, match="chunk_rows"):
        quickhouse.sync(
            pg_source,
            ch_target,
            dest_table="does_not_matter",
            source_table="does_not_matter",
            mode="incremental",
            watermark="ts",
            key=["id"],
            chunk_rows=1000,
            validate=lambda info: calls.append(info),
        )
    assert calls == [], "the gate must not fire on a rejected path"


# ---- end-to-end tests (PostgreSQL -> ClickHouse + Great Expectations) ----


def _gx_context_and_datasource(name: str):
    gx = pytest.importorskip("great_expectations")
    pytest.importorskip("clickhouse_sqlalchemy")
    context = gx.get_context(mode="ephemeral")
    conn = f"clickhouse+http://{CH_USER}:{CH_PASSWORD}@{CH_HOST}:{CH_PORT}/{CH_DB}"
    context.data_sources.add_sql(name=name, connection_string=conn)
    return gx, context


def _seed(pg_conn, table: str, rows: int):
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, note text)')
        with cur.copy(f'COPY "{table}" (id) FROM STDIN') as copy:
            for i in range(1, rows + 1):
                copy.write_row((i,))  # note left NULL for every row


def _drop_ch(ch_client, table: str):
    ch_client.command(f"DROP TABLE IF EXISTS `{table}`")
    ch_client.command(f"DROP TABLE IF EXISTS `{table}_quickhouse_tmp`")


def _ch_count(ch_client, table: str):
    try:
        return int(ch_client.command(f"SELECT count() FROM `{table}`"))
    except Exception:  # noqa: BLE001 — table absent
        return None


def _staging_leftovers(ch_client, table: str) -> int:
    like = f"{table}_quickhouse_tmp%"
    return int(
        ch_client.command(
            f"SELECT count() FROM system.tables WHERE database = '{CH_DB}' AND name LIKE '{like}'"
        )
    )


def test_gate_passes_and_data_lands(pg_conn, ch_client, pg_source, ch_target, unique_name):
    table = unique_name
    n = 200
    _seed(pg_conn, table, n)
    _drop_ch(ch_client, table)
    gx, context = _gx_context_and_datasource(f"ds_{table}")
    suite = gx.ExpectationSuite(name=f"suite_{table}")
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))
    try:
        result = quickhouse.sync(
            pg_source, ch_target, dest_table=table, source_table=table,
            mode="full", key=["id"], create_if_missing=True,
            validate=quickhouse.Validation(suite=suite, context=context, datasource=f"ds_{table}"),
        )
        assert result.rows_written == n
        assert _ch_count(ch_client, table) == n
        assert _staging_leftovers(ch_client, table) == 0
    finally:
        _drop_ch(ch_client, table)


def test_gate_fails_and_blocks_the_swap(pg_conn, ch_client, pg_source, ch_target, unique_name):
    table = unique_name
    n = 200
    _seed(pg_conn, table, n)  # every row has note = NULL
    _drop_ch(ch_client, table)
    gx, context = _gx_context_and_datasource(f"ds_{table}")
    suite = gx.ExpectationSuite(name=f"suite_{table}")
    # This cannot hold — 'note' is NULL for every row — so the gate must fire.
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="note"))
    seen = {}
    validate = quickhouse.Validation(
        suite=suite, context=context, datasource=f"ds_{table}",
        on_result=lambda r: seen.update(success=r.success),
    )
    try:
        with pytest.raises(RuntimeError, match="validation"):
            quickhouse.sync(
                pg_source, ch_target, dest_table=table, source_table=table,
                mode="full", key=["id"], create_if_missing=True, validate=validate,
            )
        assert seen.get("success") is False, "on_result should see the failed result"
        # The rejected rows never landed, and staging was cleaned up.
        assert (_ch_count(ch_client, table) or 0) != n
        assert _staging_leftovers(ch_client, table) == 0
    finally:
        _drop_ch(ch_client, table)


# ---- Phase 2: ClickHouse *incremental* gating (forced staging) ----


def _seed_incr(pg_conn, table: str, rows: int):
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(
            f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, note text, ts timestamp NOT NULL)'
        )
        with cur.copy(f'COPY "{table}" (id, ts) FROM STDIN') as copy:
            for i in range(1, rows + 1):
                copy.write_row((i, "2024-01-01 00:00:00"))  # note left NULL for every row


def test_gate_incremental_passes_and_data_lands(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    """A ClickHouse incremental sync normally inserts directly; with validate=
    quickhouse forces a staging table, gates it, then promotes via insert-select."""
    table = unique_name
    n = 200
    _seed_incr(pg_conn, table, n)
    _drop_ch(ch_client, table)
    gx, context = _gx_context_and_datasource(f"ds_{table}")
    suite = gx.ExpectationSuite(name=f"suite_{table}")
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))
    try:
        result = quickhouse.sync(
            pg_source, ch_target, dest_table=table, source_table=table,
            mode="incremental", watermark="ts", key=["id"], create_if_missing=True,
            validate=quickhouse.Validation(suite=suite, context=context, datasource=f"ds_{table}"),
        )
        assert result.rows_written == n
        assert _ch_count(ch_client, table) == n
        assert _staging_leftovers(ch_client, table) == 0
    finally:
        _drop_ch(ch_client, table)


def test_gate_incremental_fails_and_blocks_the_insert(
    pg_conn, ch_client, pg_source, ch_target, unique_name
):
    table = unique_name
    n = 200
    _seed_incr(pg_conn, table, n)  # every row has note = NULL
    _drop_ch(ch_client, table)
    gx, context = _gx_context_and_datasource(f"ds_{table}")
    suite = gx.ExpectationSuite(name=f"suite_{table}")
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="note"))
    try:
        with pytest.raises(RuntimeError, match="validation"):
            quickhouse.sync(
                pg_source, ch_target, dest_table=table, source_table=table,
                mode="incremental", watermark="ts", key=["id"], create_if_missing=True,
                validate=quickhouse.Validation(
                    suite=suite, context=context, datasource=f"ds_{table}"
                ),
            )
        # The dest table was created empty, and the insert-select was blocked,
        # so no rows landed; staging was cleaned up.
        assert (_ch_count(ch_client, table) or 0) == 0
        assert _staging_leftovers(ch_client, table) == 0
    finally:
        _drop_ch(ch_client, table)
