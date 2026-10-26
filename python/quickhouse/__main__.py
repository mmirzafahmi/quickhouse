"""Command-line runner for quickhouse.

    quickhouse --version
    quickhouse run job.toml

`run` executes a single :func:`quickhouse.sync` from a TOML job file with three
tables — ``[source]``, ``[target]``, and ``[sync]``. ``type`` selects the engine
(``postgres`` | ``mysql`` | ``bigquery`` | ``clickhouse`` | ``clevertap`` |
``appsflyer`` for a source, ``clickhouse`` | ``bigquery`` | ``csv`` for a
target);
every other key maps straight to that descriptor's constructor / to ``sync()``'s
keyword arguments; a ``[sync]`` key ``sync()`` doesn't take is refused before
anything runs. String values are passed through :func:`os.path.expandvars`, so
``${ENV_VAR}`` lets you keep credentials out of the file. A ``${ENV_VAR}`` whose
variable is unset stops the job before anything runs, naming it; an unset bare
``$NAME`` stays as written, as ``expandvars`` leaves it.

Example ``job.toml``::

    [source]
    type = "postgres"
    dsn = "${PG_DSN}"

    [target]
    type = "clickhouse"
    url = "http://localhost:8123"
    database = "analytics"

    [sync]
    dest_table = "orders"
    source_table = "orders"
    mode = "incremental"
    watermark = "updated_at"
    key = ["id"]

A CSV target writes a folder of part files per table under ``path``::

    [target]
    type = "csv"
    path = "/data/exports"
    compression = "gzip"
"""

from __future__ import annotations

import argparse
import difflib
import inspect
import os
import re
import sys
from typing import Any

import quickhouse as qh

_SOURCES = {
    "postgres": qh.Postgres,
    "mysql": qh.MySQL,
    "bigquery": qh.BigQuery,
    "clickhouse": qh.ClickHouse,
    "clevertap": qh.CleverTap,
    "appsflyer": qh.AppsFlyer,
}
_TARGETS = {"clickhouse": qh.ClickHouse, "bigquery": qh.BigQuery, "csv": qh.Csv}


def _load_toml(path: str) -> dict:
    try:
        import tomllib as toml  # Python 3.11+
    except ModuleNotFoundError:
        try:
            import tomli as toml  # backport for 3.9 / 3.10
        except ModuleNotFoundError:
            sys.exit(
                "Reading a TOML job file needs a TOML parser on Python < 3.11.\n"
                "Install it with:  pip install 'quickhouse[cli]'   (or: pip install tomli)"
            )
    try:
        with open(path, "rb") as f:
            return toml.load(f)
    except FileNotFoundError:
        sys.exit(f"job file not found: {path}")
    except toml.TOMLDecodeError as e:
        sys.exit(f"invalid TOML in {path}: {e}")


# The ``${NAME}`` form, which says a variable is meant. A bare ``$NAME`` can as
# well be text (a ``$1`` placeholder, a ``$`` in a password), so an unset one
# is left alone, as ``os.path.expandvars`` leaves it.
_BRACED_VAR = re.compile(r"\$\{([^}]*)\}")


def _unset_vars(job: dict) -> list[str]:
    """Each ``${NAME}`` in the job's string values whose variable is unset, as
    ``"NAME ([section] key)"``. ``expandvars`` leaves one as literal text, and
    the run then failed far from the cause, as an invalid DSN (#45)."""
    found: list[str] = []

    def walk(value: Any, where: str) -> None:
        if isinstance(value, str):
            found.extend(
                f"{name} ({where})"
                for name in _BRACED_VAR.findall(value)
                if name not in os.environ
            )
        elif isinstance(value, list):
            for v in value:
                walk(v, where)
        elif isinstance(value, dict):
            for k, v in value.items():
                walk(v, f"{where}.{k}")

    for section, table in job.items():
        if isinstance(table, dict):
            for key, value in table.items():
                walk(value, f"[{section}] {key}")
        else:
            walk(table, section)
    return found


def _expand(value: Any) -> Any:
    """Recursively expand ${ENV_VAR} / $ENV_VAR in string values."""
    if isinstance(value, str):
        return os.path.expandvars(value)
    if isinstance(value, list):
        return [_expand(v) for v in value]
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    return value


def _descriptor(section: dict, registry: dict, role: str):
    section = dict(section)
    typ = section.pop("type", None)
    if typ is None:
        sys.exit(f"[{role}] needs a 'type' (one of: {', '.join(registry)})")
    cls = registry.get(str(typ).lower())
    if cls is None:
        sys.exit(f"[{role}] unknown type {typ!r}; expected one of: {', '.join(registry)}")
    try:
        return cls(**section)
    except (TypeError, ValueError) as e:
        # TypeError: an unknown or missing key. ValueError: a value the
        # descriptor checks as it is built (Csv's delimiter, say).
        sys.exit(f"[{role}] ({typ}) config error: {e}")


def _check_sync_keys(sync_kwargs: dict) -> None:
    """Refuse a ``[sync]`` key ``sync()`` doesn't take, naming it. Passed
    through, it ended in a ``TypeError`` traceback (#45)."""
    try:
        params = inspect.signature(qh.sync).parameters
    except (TypeError, ValueError):
        return  # no signature to check against: sync() itself says
    # source and target come from their own tables.
    known = [name for name in params if name not in ("source", "target")]
    for key in sync_kwargs:
        if key not in known:
            close = difflib.get_close_matches(key, known, n=1)
            hint = f" (did you mean {close[0]!r}?)" if close else ""
            sys.exit(f"[sync] config error: unknown key {key!r}{hint}")


def _cmd_run(args: argparse.Namespace) -> None:
    raw = _load_toml(args.job)
    unset = _unset_vars(raw)
    if unset:
        sys.exit(f"{args.job} uses unset environment variable(s): {', '.join(unset)}")
    job = _expand(raw)
    for required in ("source", "target", "sync"):
        if required not in job:
            sys.exit(f"job file must contain a [{required}] table")
    src = _descriptor(job["source"], _SOURCES, "source")
    dst = _descriptor(job["target"], _TARGETS, "target")
    sync_kwargs = job["sync"]
    if not isinstance(sync_kwargs, dict):
        sys.exit("[sync] must be a table of quickhouse.sync() keyword arguments")
    _check_sync_keys(sync_kwargs)
    try:
        result = qh.sync(src, dst, **sync_kwargs)
    except TypeError as e:
        # A value of the wrong type (`key = "id"` for a list), raised while
        # sync() reads its arguments, before anything runs.
        sys.exit(f"[sync] config error: {e}")
    print(result)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="quickhouse",
        description="Run a quickhouse transfer from a TOML job file.",
    )
    parser.add_argument("--version", action="version", version=f"quickhouse {qh.version()}")
    sub = parser.add_subparsers(dest="command")
    run = sub.add_parser("run", help="execute one sync() from a TOML job file")
    run.add_argument("job", help="path to the TOML job file")
    run.set_defaults(func=_cmd_run)

    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        sys.exit(2)
    args.func(args)


if __name__ == "__main__":
    main()
