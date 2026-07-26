"""Command-line runner for quickhouse.

    quickhouse --version
    quickhouse run job.toml

`run` executes a single :func:`quickhouse.sync` from a TOML job file with three
tables — ``[source]``, ``[target]``, and ``[sync]``. ``type`` selects the engine;
every other key maps straight to that descriptor's constructor / to ``sync()``'s
keyword arguments. String values are passed through :func:`os.path.expandvars`,
so ``${ENV_VAR}`` lets you keep credentials out of the file.

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
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any

import quickhouse as qh

_SOURCES = {
    "postgres": qh.Postgres,
    "mysql": qh.MySQL,
    "bigquery": qh.BigQuery,
    "clevertap": qh.CleverTap,
    "appsflyer": qh.AppsFlyer,
}
_TARGETS = {"clickhouse": qh.ClickHouse, "bigquery": qh.BigQuery}


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
    except TypeError as e:
        sys.exit(f"[{role}] ({typ}) config error: {e}")


def _cmd_run(args: argparse.Namespace) -> None:
    job = _expand(_load_toml(args.job))
    for required in ("source", "target", "sync"):
        if required not in job:
            sys.exit(f"job file must contain a [{required}] table")
    src = _descriptor(job["source"], _SOURCES, "source")
    dst = _descriptor(job["target"], _TARGETS, "target")
    sync_kwargs = job["sync"]
    if not isinstance(sync_kwargs, dict):
        sys.exit("[sync] must be a table of quickhouse.sync() keyword arguments")
    result = qh.sync(src, dst, **sync_kwargs)
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
