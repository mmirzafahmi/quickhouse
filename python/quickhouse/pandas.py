"""Write an in-memory DataFrame into ClickHouse or BigQuery.

``quickhouse.from_pandas(df, target, dest_table=...)`` is the front door. The
frame is converted to Arrow once, normalised to the types the Rust engine maps,
serialized as an Arrow IPC stream and handed across — after which it is an
ordinary transfer, with the same DDL generation, staging, swap, merge and
warnings as a Postgres or ClickHouse source.

Two things about this module are deliberate.

**All the dtype knowledge lives here, not in Rust.** Python is the only side that
knows the *pandas* dtype behind an Arrow type, so it is the only side that can
say "column 'ts' is datetime64[ns]; round it with ``.dt.floor('us')``" instead of
"Timestamp(Nanosecond, None) is unsupported". Rust keeps a matching closed
allow-list as the backstop.

**Normalisation is mandatory, not best-effort.** The engine resolves a
ClickHouse column type from the Arrow type alone, and its timestamp mapping pins
*any* time unit to ``DateTime64(6, tz)``. A nanosecond column that slipped
through would be declared microsecond and land 1000x off — silently. Everything
here that looks like fussiness is guarding that.

Note the module is named ``pandas`` inside the ``quickhouse`` package, so it must
never ``import pandas`` at module level — only inside the functions, which is
also what keeps the dependency optional.
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Sequence

if TYPE_CHECKING:  # pragma: no cover - typing only
    from ._quickhouse import TransferResult

__all__ = ["QuickhouseWarning", "from_pandas"]


class QuickhouseWarning(UserWarning):
    """Warnings raised by :func:`from_pandas` about the frame it was given.

    Its own class so these can be filtered — or escalated — as a group::

        warnings.simplefilter("error", quickhouse.QuickhouseWarning)
    """


_EXTRA_HINT = "install with `pip install quickhouse[pandas]`"


def _require_pyarrow():
    """Import pyarrow, or explain which extra provides it."""
    try:
        import pyarrow as pa
        import pyarrow.compute as pc
    except ImportError as e:  # pragma: no cover - environment-dependent
        raise ImportError(
            f"quickhouse.from_pandas() requires pyarrow — {_EXTRA_HINT} "
            "(or `pip install pyarrow`)"
        ) from e
    return pa, pc


# --------------------------------------------------------------------------
# Accepting the frame
# --------------------------------------------------------------------------


def _to_arrow_table(obj: Any, index: bool, pa):
    """Convert whatever was passed into a ``pyarrow.Table``.

    **pandas is checked before the generic Arrow protocols**, and that ordering
    is load-bearing rather than stylistic. A modern pandas DataFrame implements
    both ``__dataframe__`` and ``__arrow_c_stream__``, so a generic branch
    placed first would swallow every pandas frame and skip the index handling
    and column-name checks below with it — silently, since the conversion
    itself succeeds.

    Everything else comes across through the Arrow PyCapsule interface without
    quickhouse knowing what produced it, which is how polars, DuckDB and pyarrow
    work here for free.
    """
    if isinstance(obj, pa.Table):
        return obj
    if isinstance(obj, pa.RecordBatch):
        return pa.Table.from_batches([obj])
    if _is_pandas_frame(obj):
        return _pandas_to_arrow(obj, index, pa)
    if hasattr(obj, "__arrow_c_stream__"):
        return pa.table(obj)
    if hasattr(obj, "to_arrow"):  # older polars / DuckDB relations
        return pa.table(obj.to_arrow())
    if hasattr(obj, "__dataframe__"):
        from pyarrow.interchange import from_dataframe

        return from_dataframe(obj)
    raise TypeError(
        "quickhouse.from_pandas() expects a pandas.DataFrame, a pyarrow.Table or "
        "RecordBatch, a polars.DataFrame, a DuckDB relation, or any object exposing "
        f"the Arrow PyCapsule interface (__arrow_c_stream__) — got {type(obj).__name__}"
    )


def _is_pandas_frame(obj: Any) -> bool:
    """Is this a ``pandas.DataFrame``?

    Checked via ``sys.modules`` rather than an import: if the caller is holding
    a DataFrame then pandas is already imported, so this never forces the
    dependency on someone passing a polars or pyarrow frame.
    """
    import sys

    pd = sys.modules.get("pandas")
    return pd is not None and isinstance(obj, pd.DataFrame)


def _pandas_to_arrow(df, index: bool, pa):
    """pandas-specific conversion, with the checks pyarrow's own errors bury."""
    import pandas as pd

    if index:
        # `reset_index()` rather than `preserve_index=True`: the latter names the
        # column `__index_level_0__`, which then appears in the destination DDL.
        unnamed = [i for i, n in enumerate(df.index.names) if n is None]
        if unnamed:
            raise ValueError(
                "from_pandas(index=True) needs every index level to be named, because the "
                f"name becomes the destination column name — level(s) {unnamed} are unnamed. "
                "Use df.rename_axis('id') (or call df.reset_index() yourself and pass "
                "index=False)."
            )
        df = df.reset_index()
    elif _index_carries_information(df, pd):
        warnings.warn(
            "from_pandas() is dropping the frame's index, which is not a plain RangeIndex "
            f"({type(df.index).__name__}"
            + (f", name={df.index.name!r}" if df.index.name is not None else "")
            + "). pandas' own to_sql writes the index by default, so if you meant to keep "
            "it pass index=True or call df.reset_index() first.",
            QuickhouseWarning,
            stacklevel=4,
        )

    bad = [c for c in df.columns if not isinstance(c, str)]
    if bad:
        raise TypeError(
            f"every column name must be a string; got {bad[:5]!r}. A MultiIndex on the "
            "columns (or a transposed frame) produces tuples — flatten it first, e.g. "
            "df.columns = ['_'.join(c) for c in df.columns]."
        )
    dupes = sorted({c for c in df.columns if list(df.columns).count(c) > 1})
    if dupes:
        raise ValueError(
            f"duplicate column name(s) {dupes!r}: a destination table cannot hold two "
            "columns of the same name. Rename one before syncing."
        )

    try:
        return pa.Table.from_pandas(df, preserve_index=False)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TypeError(
            f"quickhouse.from_pandas(): pandas could not be converted to Arrow ({e}). "
            "This is usually an `object` column holding mixed types, or a dtype with no "
            "Arrow equivalent (complex, Period, Interval) — normalise it first, e.g. "
            "df[col] = df[col].astype(str)."
        ) from e


def _index_carries_information(df, pd) -> bool:
    return (
        isinstance(df.index, pd.MultiIndex)
        or df.index.name is not None
        or not isinstance(df.index, pd.RangeIndex)
    )


# --------------------------------------------------------------------------
# Normalisation
# --------------------------------------------------------------------------

# ClickHouse's Date32/DateTime64 representable window, mirrored from
# `crate::types::ch_range`. A value outside it is rejected by the server
# mid-insert with `Code: 321`, aborting the transfer — so catch it here, where
# the column still has a name.
_CH_MIN_YEAR = 1900
_CH_MAX_YEAR = 2299


def _normalise(table, pa, pc):
    """Coerce the table's schema into exactly the set the engine maps.

    Anything left unmappable raises, naming the column, its type and the fix.
    """
    for i in range(table.num_columns):
        field = table.schema.field(i)
        column = table.column(i)
        new = _normalise_column(field, column, pa, pc)
        if new is not None:
            table = table.set_column(
                i, pa.field(field.name, new.type, nullable=True), new
            )
    return table


def _normalise_column(field, column, pa, pc):
    """Return a replacement column, or ``None`` to leave it alone."""
    t = field.type
    name = field.name

    # A categorical arrives dictionary-encoded. Nothing downstream maps it, and
    # decoding is what `.astype(str)` would have done anyway.
    if pa.types.is_dictionary(t):
        column = column.combine_chunks().dictionary_decode()
        return _normalise_column(
            pa.field(name, column.type, nullable=True), column, pa, pc
        ) or column

    # The pd.ArrowDtype / polars large-and-view string family. Same values, a
    # wider offset type; the engine only maps the 32-bit ones.
    if pa.types.is_large_string(t) or _is_view(pa, t, "string"):
        return column.cast(pa.string())
    if (
        pa.types.is_large_binary(t)
        or pa.types.is_fixed_size_binary(t)
        or _is_view(pa, t, "binary")
    ):
        return column.cast(pa.binary())

    if pa.types.is_float16(t):
        return column.cast(pa.float32())  # widening, lossless

    if pa.types.is_date64(t):
        return column.cast(pa.date32())
    if pa.types.is_date32(t):
        _guard_ch_range(name, column, pa, pc, is_date=True)
        return None

    # ClickHouse has no time-of-day type; every other source in this crate maps
    # TIME to text, so match them rather than inventing a second convention.
    if pa.types.is_time(t):
        return column.cast(pa.string())

    if pa.types.is_timestamp(t):
        return _normalise_timestamp(name, column, t, pa, pc)

    if pa.types.is_null(t):
        warnings.warn(
            f"column {name!r} holds only nulls, so there is no type to infer from it — "
            "syncing it as a nullable string column. Give it a dtype "
            f"(df[{name!r}] = df[{name!r}].astype('string')) to choose for yourself.",
            QuickhouseWarning,
            stacklevel=5,
        )
        return column.cast(pa.string())

    if pa.types.is_decimal256(t):
        raise TypeError(
            f"column {name!r} is {t}: quickhouse has no Decimal256. Use a precision of 38 "
            "or less, a float, or a string column."
        )
    if (
        pa.types.is_list(t)
        or pa.types.is_large_list(t)
        or pa.types.is_fixed_size_list(t)
        or pa.types.is_struct(t)
        or pa.types.is_map(t)
        or pa.types.is_union(t)
    ):
        raise TypeError(
            f"column {name!r} is {t}: nested Arrow types have no destination column type. "
            f"Serialize it first, e.g. df[{name!r}] = df[{name!r}].map(json.dumps)."
        )
    if pa.types.is_duration(t) or pa.types.is_interval(t):
        raise TypeError(
            f"column {name!r} is {t}: durations and intervals have no destination column "
            "type. Convert it to a number of seconds, or to text."
        )
    return None


def _is_view(pa, t, kind: str) -> bool:
    """`string_view`/`binary_view` exist only in pyarrow >= 16."""
    checker = getattr(pa.types, f"is_{kind}_view", None)
    return bool(checker and checker(t))


def _normalise_timestamp(name, column, t, pa, pc):
    """Pin a timestamp column to microseconds, and its timezone to one
    ClickHouse will accept.

    The unit cast is `safe=True` on purpose: a nanosecond column carrying real
    sub-microsecond digits *raises* rather than quietly truncating. Losing
    precision is a decision for the caller to make explicitly.
    """
    tz = t.tz
    if tz is not None and not _is_iana_name(tz):
        # ClickHouse's DateTime64 takes a timezone *name*; a fixed offset like
        # '+07:00' is rejected outright. Converting to UTC preserves the instant
        # exactly — only the rendering timezone changes.
        warnings.warn(
            f"column {name!r} has a fixed-offset timezone ({tz!r}), which ClickHouse "
            "cannot name in a DateTime64 type — converting it to UTC. The instant is "
            "unchanged; only the displayed offset is. Use a named zone "
            "(tz_convert('Asia/Jakarta')) to keep one.",
            QuickhouseWarning,
            stacklevel=5,
        )
        tz = "UTC"

    target = pa.timestamp("us", tz=tz)
    if t != target:
        try:
            column = column.cast(target, safe=True)
        except pa.ArrowInvalid as e:
            raise ValueError(
                f"column {name!r} is {t} and carries sub-microsecond precision, which "
                "quickhouse cannot store — it standardises on microsecond timestamps. "
                f"Round it first: df[{name!r}] = df[{name!r}].dt.floor('us'). ({e})"
            ) from e
    _guard_ch_range(name, column, pa, pc, is_date=False)
    return column


def _is_iana_name(tz: str) -> bool:
    """A zone name ClickHouse can put in a type, vs. a fixed offset."""
    return "/" in tz or tz.upper() == "UTC"


def _guard_ch_range(name, column, pa, pc, *, is_date: bool):
    """Reject values outside ClickHouse's representable date window.

    One vectorised ``min_max`` per temporal column — cheap next to the transfer
    it precedes, and far better than the server's `Code: 321` mid-insert, which
    names neither the column nor the value.
    """
    bounds = pc.min_max(column)
    lo, hi = bounds["min"], bounds["max"]
    if lo.as_py() is None:  # all-null column: nothing to check
        return
    for edge in (lo.as_py(), hi.as_py()):
        if not (_CH_MIN_YEAR <= edge.year <= _CH_MAX_YEAR):
            what = "date" if is_date else "timestamp"
            raise ValueError(
                f"column {name!r} holds the {what} {edge}, outside the "
                f"[{_CH_MIN_YEAR}, {_CH_MAX_YEAR}] window ClickHouse can represent. "
                "Clamp or null those rows before syncing — the destination would "
                "otherwise reject the insert partway through, after some rows had "
                "already landed."
            )


# --------------------------------------------------------------------------
# Deduplication
# --------------------------------------------------------------------------


def _dedupe_on_key(table, key: Sequence[str], pa, pc):
    """Keep only the last row per key, the way an upsert is meant to behave.

    Incremental from a frame merges on ``key``, and neither destination can
    order duplicates *within* one batch without a version column — ClickHouse
    would pick whichever part merged last, BigQuery whichever row `ROW_NUMBER`
    happened to see first. Resolving it here makes "last one wins" true rather
    than nearly true.

    Done on the Arrow table rather than in pandas so a polars or DuckDB frame
    behaves identically.
    """
    missing = [k for k in key if k not in table.column_names]
    if missing:
        raise ValueError(
            f"key column(s) {missing!r} are not in the frame; incremental mode upserts on "
            f"key, so they have to be there. Frame columns: {table.column_names!r}"
        )
    ordinal = "__quickhouse_ordinal"
    with_ord = table.append_column(
        ordinal, pa.array(range(table.num_rows), type=pa.int64())
    )
    keep = with_ord.group_by(list(key)).aggregate([(ordinal, "max")])
    wanted = keep.column(f"{ordinal}_max")
    dropped = table.num_rows - keep.num_rows
    if dropped == 0:
        return table
    mask = pc.is_in(with_ord.column(ordinal), value_set=wanted.combine_chunks())
    warnings.warn(
        f"the frame has {dropped} row(s) sharing a key with a later row; keeping the last "
        f"of each (key={list(key)!r}). Neither destination can order duplicates within one "
        "batch without a version column, so quickhouse resolves it here rather than "
        "letting the winner be arbitrary. Pass a watermark= column to order by that "
        "instead, or dedupe the frame yourself to silence this.",
        QuickhouseWarning,
        stacklevel=3,
    )
    return with_ord.filter(mask).drop([ordinal])


# --------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------


def from_pandas(
    df: Any,
    target: Any,
    *,
    dest_table: str,
    index: bool = False,
    **sync_kwargs: Any,
) -> "TransferResult":
    """Write an in-memory DataFrame into ClickHouse or BigQuery.

    Parameters
    ----------
    df:
        A ``pandas.DataFrame``, a ``pyarrow.Table``/``RecordBatch``, a
        ``polars.DataFrame``, a DuckDB relation, or anything exposing the Arrow
        PyCapsule interface (``__arrow_c_stream__``).
    target:
        A :class:`quickhouse.ClickHouse` or :class:`quickhouse.BigQuery`
        descriptor — the same objects :func:`quickhouse.sync` takes.
    dest_table:
        Destination table name.
    index:
        Write the frame's index as column(s). ``False`` (default) drops it,
        warning if it looked meaningful. ``True`` requires every index level to
        be named. (pandas' own ``to_sql`` defaults to ``True``; quickhouse
        defaults the other way, because an unnamed index column in a warehouse
        table is almost never what was meant.)
    **sync_kwargs:
        Everything else is forwarded verbatim to :func:`quickhouse.sync` —
        ``mode``, ``key``, ``engine``, ``order_by``, ``type_overrides``,
        ``on_progress`` and the rest. Options that cannot apply to a frame
        (``source_table``, ``chunk_rows``, ``read_max_rows_per_sec``,
        ``retry_max_attempts`` and friends) are rejected by the engine with a
        message naming the knob.

    Modes
    -----
    ``mode="full"`` replaces the table through a staged atomic swap.
    ``mode="append"`` inserts straight in, with no staging and no dedup.
    ``mode="incremental"`` upserts on ``key`` — and needs **no watermark**,
    unlike a database source: the frame you passed *is* the delta. Rows sharing
    a key within the frame are deduped last-wins first, with a warning.

    Returns
    -------
    The same :class:`TransferResult` :func:`quickhouse.sync` returns, including
    ``warnings``.

    Examples
    --------
    >>> import quickhouse as qh
    >>> dst = qh.ClickHouse("http://localhost:8123", database="analytics")
    >>> qh.from_pandas(df, dst, dest_table="orders", mode="full", key=["id"])

    >>> # Upsert, no watermark needed
    >>> qh.from_pandas(df, dst, dest_table="orders", mode="incremental", key=["id"])
    """
    from ._quickhouse import sync

    pa, pc = _require_pyarrow()

    table = _to_arrow_table(df, index, pa)
    if table.num_columns == 0:
        raise ValueError("the frame has no columns; there is nothing to transfer")
    table = _normalise(table, pa, pc)

    key = sync_kwargs.get("key") or []
    if sync_kwargs.get("mode") == "incremental" and key:
        table = _dedupe_on_key(table, key, pa, pc)

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    payload = sink.getvalue().to_pybytes()
    # The intermediate table can be large; let it go before the engine allocates
    # its own decoded batches on top.
    del table, sink

    return sync(payload, target, dest_table=dest_table, **sync_kwargs)
