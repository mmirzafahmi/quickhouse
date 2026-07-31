"""Optional Great Expectations data-quality gate for :func:`quickhouse.sync`.

A **preventive** gate: after a transfer has fully loaded its per-run *staging*
table but *before* that staging is promoted (the atomic swap for a full refresh,
or the ``MERGE`` for a BigQuery incremental), quickhouse runs a user-supplied
Great Expectations :class:`ExpectationSuite` against the staging table. If the
suite fails, the promotion is aborted, the staging table is dropped, and
``sync()`` raises :class:`ValidationFailed` — so rejected data never reaches the
destination.

Requires ``great-expectations`` (``pip install quickhouse[quality]``); not a hard
dependency of the package — imported lazily, only when a :class:`Validation` is
actually used.

Coverage: the gate runs in **full-refresh and incremental** mode, into either
destination. A ClickHouse *incremental* sync normally inserts directly (no
staging); when a gate is attached quickhouse transparently routes it through a
staging table, then promotes it with ``INSERT … SELECT`` (``ReplacingMergeTree``
dedups the promoted rows as usual). Only ``append`` mode and ``chunk_rows``
(keyset resumable reads) commit directly with no single staging table to gate —
quickhouse raises a clear error if you attach a ``validate=`` to either.

Example
-------
>>> import great_expectations as gx
>>> import quickhouse
>>> context = gx.get_context()
>>> context.data_sources.add_sql(
...     name="analytics_ch",
...     connection_string="clickhouse+http://default:@localhost:8123/analytics",
... )
>>> suite = gx.ExpectationSuite(name="orders_quality")
>>> suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))
>>> quickhouse.sync(
...     src, dst, dest_table="orders", source_table="orders", mode="full",
...     validate=quickhouse.Validation(
...         suite=suite, context=context, datasource="analytics_ch",
...     ),
... )

The GX SQL datasource (``datasource=``) must point at the **same database /
dataset** as the destination — quickhouse only injects the per-run staging
table name into it; it does not build the connection (so your credentials stay
in your GX config, not in quickhouse).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

__all__ = ["Validation", "ValidationFailed"]


class ValidationFailed(Exception):
    """Raised when the staged data fails its :class:`Validation` suite.

    Carries the Great Expectations validation ``result`` (an
    ``ExpectationSuiteValidationResult``) for inspection / data-docs.
    """

    def __init__(self, message: str, result: object = None) -> None:
        super().__init__(message)
        self.result = result


@dataclass
class Validation:
    """A Great Expectations gate to run against the staging table before it is
    promoted. Pass it to :func:`quickhouse.sync`'s ``validate=`` parameter.

    A :class:`Validation` is itself the callable quickhouse fires: ``sync``
    invokes it with a ``StagedInfo`` once the staging table is loaded, and it
    raises :class:`ValidationFailed` if the suite fails — which aborts the
    promotion. (Any ``callable(StagedInfo) -> None`` that raises to reject also
    works as ``validate=``; this is the batteries-included one.)

    Parameters
    ----------
    suite:
        A Great Expectations ``ExpectationSuite`` you have built (its
        expectations are what gets checked).
    context:
        Your GX data context (``gx.get_context()`` — ephemeral or file-backed).
    datasource:
        Name of a GX **SQL datasource** already added to ``context`` whose
        connection points at the destination's database/dataset. quickhouse
        creates a temporary table asset on it aimed at the per-run staging
        table, validates, then removes the asset.
    on_result:
        Optional callback invoked with the GX validation result **whether it
        passes or fails** (before a failure raises) — use it to log, alert, or
        build data docs.
    """

    suite: object
    context: object
    datasource: str
    on_result: Optional[Callable[[object], None]] = None

    def __call__(self, info) -> None:
        # Defer the GX import to first call so the package (and this dataclass)
        # imports cleanly without the optional dependency installed.
        try:
            import great_expectations  # noqa: F401  (import-check only)
        except ImportError as e:
            raise ImportError(
                "quickhouse data-quality validation (validate=) requires "
                "great-expectations — install with `pip install quickhouse[quality]` "
                "or `pip install great-expectations`"
            ) from e

        result = _validate_staging(self, info.staging_table)
        if self.on_result is not None:
            self.on_result(result)
        if not result.success:
            raise ValidationFailed(
                f"data-quality suite {_suite_name(self.suite)!r} failed against staging "
                f"table {info.staging_table!r} — the destination was left unchanged",
                result=result,
            )


def _suite_name(suite) -> str:
    """Best-effort suite name for error messages (attribute differs across GX
    versions; never let a missing name break the gate)."""
    return getattr(suite, "name", None) or getattr(suite, "expectation_suite_name", None) or "suite"


def _validate_staging(v: Validation, staging_table: str):
    """Run ``v.suite`` against ``staging_table`` via ``v.datasource``.

    This is the only Great-Expectations-1.x-specific surface — a temporary,
    uniquely-named whole-table batch aimed at the staging table, validated and
    then cleaned up. Isolated here so GX API churn is contained to one place.
    """
    context = v.context
    datasource = context.data_sources.get(v.datasource)

    asset_name = f"_quickhouse_staging__{staging_table}"
    batch_def_name = f"{asset_name}__whole"

    # Point a fresh asset at the staging table (drop a stale one from a prior
    # aborted run first, so re-runs don't collide on the asset name).
    try:
        datasource.delete_asset(asset_name)
    except Exception:
        pass

    asset = datasource.add_table_asset(name=asset_name, table_name=staging_table)
    try:
        batch_definition = asset.add_batch_definition_whole_table(name=batch_def_name)
        batch = batch_definition.get_batch()
        return batch.validate(v.suite)
    finally:
        try:
            datasource.delete_asset(asset_name)
        except Exception:
            pass
