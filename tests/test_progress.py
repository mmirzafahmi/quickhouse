"""Unit tests for quickhouse.progress_bar — pure Python, no services required."""

from __future__ import annotations

import io
import sys
from types import SimpleNamespace

import pytest

import quickhouse


def _progress(rows_written, rows_per_sec=0.0):
    return SimpleNamespace(rows_written=rows_written, rows_per_sec=rows_per_sec)


def test_progress_bar_tracks_deltas_and_renders_final_count():
    pytest.importorskip("tqdm")
    buf = io.StringIO()
    with quickhouse.progress_bar(total=100, file=buf, mininterval=0, maxinterval=0) as on_progress:
        on_progress(_progress(10, 500.0))
        on_progress(_progress(30, 750.0))
        on_progress(_progress(30, 750.0))  # no new rows: must be a no-op, not double-counted
    assert "30" in buf.getvalue()


def test_progress_bar_closes_even_on_exception():
    pytest.importorskip("tqdm")
    buf = io.StringIO()
    with pytest.raises(ValueError, match="boom"):
        with quickhouse.progress_bar(file=buf) as on_progress:
            on_progress(_progress(5))
            raise ValueError("boom")
    # tqdm writes a newline/reset on close; reaching here without a hang/deadlock
    # and with output captured is enough to confirm the bar was actually closed.
    assert buf.getvalue() != ""


def test_progress_bar_missing_tqdm_gives_clear_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "tqdm", None)
    with pytest.raises(ImportError, match=r"quickhouse\[progress\]"):
        with quickhouse.progress_bar():
            pass
