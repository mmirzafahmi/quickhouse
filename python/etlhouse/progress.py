"""Optional tqdm-backed progress bar for :func:`etlhouse.sync`'s ``on_progress``.

Requires ``tqdm`` (``pip install etlhouse[progress]``); not a hard dependency
of the package since it's a convenience, not core functionality.
"""

from __future__ import annotations

import contextlib
from typing import Callable, Iterator, Optional

__all__ = ["progress_bar"]


@contextlib.contextmanager
def progress_bar(total: Optional[int] = None, **tqdm_kwargs) -> Iterator[Callable[[object], None]]:
    """Context manager yielding an ``on_progress`` callback backed by tqdm.

    Pass the yielded callback straight to :func:`etlhouse.sync`'s
    ``on_progress``. The bar is closed automatically on exit, including when
    ``sync()`` raises.

    Parameters
    ----------
    total:
        Row count to show a percentage/ETA against, if known in advance
        (e.g. from a prior ``COUNT(*)``). Omit for an indeterminate bar that
        just shows a running count and rate.
    **tqdm_kwargs:
        Passed through to ``tqdm.tqdm`` (e.g. ``desc="my_table"``).

    Example
    -------
    >>> import etlhouse
    >>> with etlhouse.progress_bar() as on_progress:
    ...     etlhouse.sync(src, dst, dest_table="t", source_table="t",
    ...                   on_progress=on_progress)
    """
    try:
        from tqdm import tqdm
    except ImportError as e:
        raise ImportError(
            "etlhouse.progress_bar() requires tqdm — install with "
            "`pip install etlhouse[progress]` or `pip install tqdm`"
        ) from e

    tqdm_kwargs.setdefault("unit", "rows")
    tqdm_kwargs.setdefault("unit_scale", True)
    bar = tqdm(total=total, **tqdm_kwargs)
    last_rows = 0

    def _on_progress(p) -> None:
        nonlocal last_rows
        delta = p.rows_written - last_rows
        if delta > 0:
            bar.update(delta)
            last_rows = p.rows_written
        bar.set_postfix_str(f"{p.rows_per_sec:,.0f} rows/s")

    try:
        yield _on_progress
    finally:
        bar.close()
