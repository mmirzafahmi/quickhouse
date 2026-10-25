"""Unit tests for the ``quickhouse run job.toml`` CLI — no services required:
every job here stops before ``sync()`` connects to anything."""

from __future__ import annotations

import sys

import pytest

from quickhouse.__main__ import _unset_vars, main

if sys.version_info < (3, 11):
    pytest.importorskip("tomli", reason="below Python 3.11 the CLI reads TOML with tomli")

_ENDPOINTS = """
[source]
type = "postgres"
dsn = "postgresql://etl:etl@127.0.0.1:1/etl"

[target]
type = "clickhouse"
url = "http://127.0.0.1:1"
database = "default"
"""


def _run(tmp_path, toml: str) -> str:
    """Run a job and return what it exited with: the one-line error."""
    job = tmp_path / "job.toml"
    job.write_text(toml)
    with pytest.raises(SystemExit) as exit_:
        main(["run", str(job)])
    return str(exit_.value.code)


def test_an_unknown_sync_key_is_named_instead_of_a_traceback(tmp_path):
    """Issue #45: ``keys = [...]`` ended in ``TypeError: sync() got an
    unexpected keyword argument 'keys'`` and a full traceback."""
    err = _run(
        tmp_path,
        _ENDPOINTS
        + """
[sync]
dest_table = "orders"
source_table = "orders"
keys = ["id"]
""",
    )
    assert err == "[sync] config error: unknown key 'keys' (did you mean 'key'?)"


def test_a_sync_value_of_the_wrong_type_is_a_one_line_config_error(tmp_path):
    err = _run(
        tmp_path,
        _ENDPOINTS
        + """
[sync]
dest_table = "orders"
source_table = "orders"
key = "id"
""",
    )
    assert err.startswith("[sync] config error: "), err
    assert "key" in err, err


def test_an_unset_braced_variable_stops_the_job_and_names_it(tmp_path, monkeypatch):
    """Issue #45: an unset ``${VAR}`` passed through as literal text, and the
    run failed later as an invalid connection string."""
    monkeypatch.delenv("QH_TEST_UNSET_DSN", raising=False)
    err = _run(
        tmp_path,
        """
[source]
type = "postgres"
dsn = "${QH_TEST_UNSET_DSN}"

[target]
type = "clickhouse"
url = "http://127.0.0.1:1"
database = "default"

[sync]
dest_table = "orders"
""",
    )
    assert "unset environment variable(s): QH_TEST_UNSET_DSN ([source] dsn)" in err, err


def test_only_unset_braced_variables_are_reported(monkeypatch):
    """A bare ``$NAME`` can be text (a ``$1`` placeholder, a ``$`` in a
    password), so an unset one stays as written, as before."""
    monkeypatch.setenv("QH_TEST_SET", "x")
    monkeypatch.delenv("QH_TEST_UNSET", raising=False)
    job = {
        "source": {"dsn": "${QH_TEST_SET}", "password": "pa$word"},
        "target": {"settings": {"role": "${QH_TEST_UNSET}"}},
        "sync": {"source_query": "SELECT $1", "key": ["${QH_TEST_UNSET}"]},
    }
    assert _unset_vars(job) == [
        "QH_TEST_UNSET ([target] settings.role)",
        "QH_TEST_UNSET ([sync] key)",
    ]
