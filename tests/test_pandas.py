"""End-to-end integration tests: a DataFrame -> ClickHouse.

`quickhouse.from_pandas` converts the frame to Arrow in Python, normalises its
schema, and hands the engine a serialized IPC stream. Two halves are worth
guarding, and they fail in different ways:

* the **normalisation**, where a wrong answer is silent — a nanosecond column
  declared microsecond, a fixed-offset timezone that ClickHouse cannot name, an
  index quietly dropped;
* the **engine path**, which reuses the same DDL, staging, swap and merge as
  every other source, so it fails loudly if at all.

Run against the services in ``docker-compose.yml`` after building the module:

    docker compose up -d clickhouse
    pip install -e '.[test]'
    maturin develop --release
    pytest -v tests/test_pandas.py
"""

from __future__ import annotations

import datetime as dt
import decimal
import warnings

import pytest

import quickhouse

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
pc = pytest.importorskip("pyarrow.compute")


def _drop(ch_client, *tables: str):
    for t in tables:
        ch_client.command(f"DROP TABLE IF EXISTS `{t}`")


def _frame(rows: int = 5, base_ts: str = "2024-01-01"):
    return pd.DataFrame(
        {
            "id": range(1, rows + 1),
            "name": [f"row-{i}" for i in range(1, rows + 1)],
            "amount": [i * 1.5 for i in range(1, rows + 1)],
            "updated_at": pd.to_datetime([base_ts] * rows),
        }
    )


def test_full_refresh_replaces_the_table(ch_client, ch_target, unique_name):
    table = unique_name
    _drop(ch_client, table)
    try:
        result = quickhouse.from_pandas(
            _frame(500), ch_target, dest_table=table, mode="full", key=["id"]
        )
        assert result.rows_written == 500
        assert int(ch_client.command(f"SELECT count() FROM `{table}`")) == 500
        assert int(ch_client.command(f"SELECT uniqExact(id) FROM `{table}`")) == 500

        # A full refresh replaces rather than appends. Shrinking the
        # destination is refused by default (`guard_full_refresh_shrink`), so
        # this is also the check that a frame source goes through that guard
        # like every other source — opting in is what makes it legal.
        with pytest.raises(RuntimeError, match="shrinking the destination"):
            quickhouse.from_pandas(
                _frame(250), ch_target, dest_table=table, mode="full", key=["id"]
            )
        quickhouse.from_pandas(
            _frame(250),
            ch_target,
            dest_table=table,
            mode="full",
            key=["id"],
            allow_full_refresh_shrink=True,
        )
        assert int(ch_client.command(f"SELECT count() FROM `{table}`")) == 250
    finally:
        _drop(ch_client, table)


def test_append_inserts_without_dedup(ch_client, ch_target, unique_name):
    table = unique_name
    _drop(ch_client, table)
    try:
        for _ in range(2):
            quickhouse.from_pandas(
                _frame(10), ch_target, dest_table=table, mode="append", key=["id"]
            )
        # Append is a bronze landing: no staging, no merge, no dedup.
        assert int(ch_client.command(f"SELECT count() FROM `{table}`")) == 20
    finally:
        _drop(ch_client, table)


def test_incremental_upserts_on_key_without_a_watermark(ch_client, ch_target, unique_name):
    """The headline behaviour, and the one that diverges from every other source.

    A database source needs a watermark because it has to know where to resume
    reading. A frame does not: the caller already holds every row, so `key`
    alone is enough to upsert on.
    """
    table = unique_name
    _drop(ch_client, table)
    try:
        quickhouse.from_pandas(
            _frame(10), ch_target, dest_table=table, mode="incremental", key=["id"]
        )
        changed = _frame(10)
        changed.loc[0, "amount"] = 999.0
        result = quickhouse.from_pandas(
            changed, ch_target, dest_table=table, mode="incremental", key=["id"]
        )
        assert result.rows_written == 10

        ch_client.command(f"OPTIMIZE TABLE `{table}` FINAL")
        assert int(ch_client.command(f"SELECT count() FROM `{table}`")) == 10
        assert float(ch_client.command(f"SELECT amount FROM `{table}` WHERE id = 1")) == 999.0
    finally:
        _drop(ch_client, table)


def test_incremental_without_a_key_is_refused(ch_target, unique_name):
    # `key` takes over the watermark's old job of making incremental meaningful,
    # so its absence has to be an error rather than a silent append.
    with pytest.raises(RuntimeError, match="upserts on key"):
        quickhouse.from_pandas(
            _frame(2), ch_target, dest_table=unique_name, mode="incremental"
        )


def test_duplicate_keys_are_deduped_last_wins(ch_client, ch_target, unique_name):
    """Neither destination can order duplicates *within* one batch without a
    version column, so quickhouse resolves it here rather than letting the
    winner be arbitrary."""
    table = unique_name
    _drop(ch_client, table)
    df = pd.DataFrame({"id": [1, 1, 2], "amount": [1.0, 7.0, 3.0]})
    try:
        with pytest.warns(quickhouse.QuickhouseWarning, match="sharing a key"):
            quickhouse.from_pandas(
                df, ch_target, dest_table=table, mode="incremental", key=["id"]
            )
        ch_client.command(f"OPTIMIZE TABLE `{table}` FINAL")
        assert int(ch_client.command(f"SELECT count() FROM `{table}`")) == 2
        # Last one wins, not an arbitrary one.
        assert float(ch_client.command(f"SELECT amount FROM `{table}` WHERE id = 1")) == 7.0
    finally:
        _drop(ch_client, table)


def test_duplicate_keys_with_watermark_keep_the_highest_watermark_row(
    ch_client, ch_target, unique_name
):
    """Passing watermark= must decide the dedup winner, not row position.

    The rows are deliberately ordered so the *older* row comes last in the
    frame: with the old position-based dedup this kept the stale 'old' value,
    even though watermark= was passed specifically to prevent that (the
    engine's own config error for the no-watermark, no-key case says exactly
    this: "Pass watermark= as well if you want it used as the version column
    for dedup ordering.")."""
    table = unique_name
    _drop(ch_client, table)
    df = pd.DataFrame(
        {
            "id": [1, 1, 2],
            "updated_at": pd.to_datetime(["2024-02-01", "2024-01-01", "2024-01-01"]),
            "status": ["new", "old", "only"],
        }
    )
    try:
        with pytest.warns(quickhouse.QuickhouseWarning, match="sharing a key"):
            quickhouse.from_pandas(
                df,
                ch_target,
                dest_table=table,
                mode="incremental",
                key=["id"],
                watermark="updated_at",
            )
        ch_client.command(f"OPTIMIZE TABLE `{table}` FINAL")
        assert int(ch_client.command(f"SELECT count() FROM `{table}`")) == 2
        assert (
            str(ch_client.command(f"SELECT status FROM `{table}` WHERE id = 1")) == "new"
        )
    finally:
        _drop(ch_client, table)


def test_dedupe_on_key_breaks_watermark_ties_by_position():
    """A pure unit test (no database) of the tie-break: when two duplicates of
    a key share the same watermark value, position still decides — the same
    "last one wins" rule as with no watermark at all — rather than the
    ordering going arbitrary."""
    from quickhouse.pandas import _dedupe_on_key

    table = pa.table(
        {
            "id": pa.array([1, 1, 2], type=pa.int64()),
            "updated_at": pa.array([1000, 1000, 1000], type=pa.int64()),
            "status": pa.array(["earlier", "later", "only"]),
        }
    )
    out = _dedupe_on_key(table, ["id"], pa, pc, watermark="updated_at")
    got = dict(zip(out.column("id").to_pylist(), out.column("status").to_pylist()))
    assert got == {1: "later", 2: "only"}


def test_dtype_coverage(ch_client, ch_target, unique_name):
    """One column per dtype family, asserting the destination type.

    This is the regression guard for the normalisation pass: every row below is
    a type whose Arrow representation is *not* what the engine maps directly, so
    a missing rule shows up here as a wrong DDL type or a decode error.
    """
    table = unique_name
    _drop(ch_client, table)
    df = pd.DataFrame(
        {
            "id": pd.array([1, 2], dtype="int64"),
            "small": pd.array([1, 2], dtype="int16"),
            "unsigned": pd.array([1, 2], dtype="uint32"),
            "f32": pd.array([1.5, 2.5], dtype="float32"),
            "nullable_int": pd.array([1, None], dtype="Int64"),
            "flag": [True, False],
            "text": ["a", "b"],
            "categorical": pd.Categorical(["x", "y"]),
            "price": [decimal.Decimal("1.2345"), decimal.Decimal("2.0000")],
            "naive_ts": pd.to_datetime(["2024-01-01 10:00:00", "2024-01-02 10:00:00"]),
            "zoned_ts": pd.to_datetime(
                ["2024-01-01T10:00:00Z", "2024-01-02T10:00:00Z"]
            ).tz_convert("Asia/Jakarta"),
            "day": [dt.date(2024, 1, 1), dt.date(2024, 1, 2)],
        }
    )
    try:
        quickhouse.from_pandas(df, ch_target, dest_table=table, mode="full", key=["id"])
        types = dict(
            ch_client.query(
                "SELECT name, type FROM system.columns "
                f"WHERE database = currentDatabase() AND table = '{table}'"
            ).result_rows
        )
        assert types["id"] == "Int64"  # key= forces NOT NULL
        assert types["small"] == "Nullable(Int16)"
        assert types["unsigned"] == "Nullable(UInt32)"
        assert types["f32"] == "Nullable(Float32)"
        assert types["nullable_int"] == "Nullable(Int64)"
        assert types["flag"] == "Nullable(Bool)"
        assert types["text"] == "Nullable(String)"
        # A categorical is dictionary-encoded in Arrow and decoded on the way in.
        assert types["categorical"] == "Nullable(String)"
        assert types["price"].startswith("Nullable(Decimal(")
        # Naive stays naive; a named zone is carried through. Getting this wrong
        # plans a tz-aware column against naive data and fails every batch.
        assert types["naive_ts"] == "Nullable(DateTime64(6))"
        assert types["zoned_ts"] == "Nullable(DateTime64(6, 'Asia/Jakarta'))"
        assert types["day"] == "Nullable(Date32)"

        row = ch_client.query(
            f"SELECT toString(price), toString(naive_ts), toString(zoned_ts), toString(day) "
            f"FROM `{table}` WHERE id = 1"
        ).result_rows[0]
        assert row[0] == "1.2345"  # exact, not a Float64 round trip
        assert row[1].startswith("2024-01-01 10:00:00")
        assert row[3] == "2024-01-01"
    finally:
        _drop(ch_client, table)


def test_nullable_int64_survives_above_2_pow_53(ch_client, ch_target, unique_name):
    # A null-containing integer column is where a naive pandas round trip loses
    # exactness (it becomes float64). Arrow keeps it an integer all the way.
    table = unique_name
    _drop(ch_client, table)
    big = 2**53 + 1
    df = pd.DataFrame({"id": [1, 2], "n": pd.array([big, None], dtype="Int64")})
    try:
        quickhouse.from_pandas(df, ch_target, dest_table=table, mode="full", key=["id"])
        assert int(ch_client.command(f"SELECT n FROM `{table}` WHERE id = 1")) == big
    finally:
        _drop(ch_client, table)


def test_fixed_offset_timezone_is_converted_to_utc_with_a_warning(
    ch_client, ch_target, unique_name
):
    """ClickHouse's DateTime64 takes a timezone *name*; '+07:00' is rejected.

    Converting to UTC preserves the instant exactly — only the rendering zone
    changes — but it is a change the caller should hear about.
    """
    table = unique_name
    _drop(ch_client, table)
    df = pd.DataFrame(
        {"id": [1], "t": pd.to_datetime(["2024-01-01T07:00:00+07:00"])}
    )
    assert df["t"].dt.tz is not None
    try:
        with pytest.warns(quickhouse.QuickhouseWarning, match="fixed-offset"):
            quickhouse.from_pandas(df, ch_target, dest_table=table, mode="full", key=["id"])
        # 07:00+07:00 is 00:00Z — the same instant.
        stored = ch_client.command(f"SELECT toString(toTimeZone(t, 'UTC')) FROM `{table}`")
        assert stored.startswith("2024-01-01 00:00:00")
    finally:
        _drop(ch_client, table)


def test_out_of_range_timestamp_is_refused_by_column_name(ch_target, unique_name):
    # ClickHouse would reject this itself, mid-insert, after some rows had
    # already landed — and its error names neither the column nor the value.
    df = pd.DataFrame({"id": [1], "t": pd.to_datetime(["1700-01-01"])})
    with pytest.raises(ValueError, match="1900"):
        quickhouse.from_pandas(df, ch_target, dest_table=unique_name, mode="full", key=["id"])


def test_nested_and_unsupported_columns_are_refused_by_name(ch_target, unique_name):
    for df, needle in [
        (pd.DataFrame({"id": [1], "tags": [["a", "b"]]}), "nested"),
        (pd.DataFrame({"id": [1], "obj": [{"a": 1}]}), "nested"),
    ]:
        with pytest.raises(TypeError) as excinfo:
            quickhouse.from_pandas(
                df, ch_target, dest_table=unique_name, mode="full", key=["id"]
            )
        msg = str(excinfo.value)
        assert needle in msg and ("'tags'" in msg or "'obj'" in msg), msg


def test_non_string_and_duplicate_column_names_are_refused(ch_target, unique_name):
    dupes = pd.DataFrame([[1, 2]], columns=["id", "id"])
    with pytest.raises(ValueError, match="duplicate column"):
        quickhouse.from_pandas(dupes, ch_target, dest_table=unique_name, mode="full")

    numeric = pd.DataFrame({0: [1], 1: [2]})
    with pytest.raises(TypeError, match="must be a string"):
        quickhouse.from_pandas(numeric, ch_target, dest_table=unique_name, mode="full")


def test_index_is_dropped_by_default_and_warned_about(ch_client, ch_target, unique_name):
    table = unique_name
    _drop(ch_client, table)
    df = _frame(3).set_index("id")
    try:
        # pandas' own to_sql writes the index by default, so dropping it
        # silently is the predictable "lost data" report.
        with pytest.warns(quickhouse.QuickhouseWarning, match="dropping the frame's index"):
            quickhouse.from_pandas(df, ch_target, dest_table=table, mode="full")
        cols = ch_client.query(
            "SELECT name FROM system.columns "
            f"WHERE database = currentDatabase() AND table = '{table}'"
        ).result_rows
        assert "id" not in [c[0] for c in cols]

        # index=True brings it back as a real, properly-named column — never
        # pyarrow's `__index_level_0__`.
        _drop(ch_client, table)
        quickhouse.from_pandas(df, ch_target, dest_table=table, mode="full", index=True, key=["id"])
        cols = [
            c[0]
            for c in ch_client.query(
                "SELECT name FROM system.columns "
                f"WHERE database = currentDatabase() AND table = '{table}'"
            ).result_rows
        ]
        assert "id" in cols
        assert not any(c.startswith("__index_level") for c in cols)
    finally:
        _drop(ch_client, table)


def test_unnamed_index_with_index_true_is_refused(ch_target, unique_name):
    df = pd.DataFrame({"a": [1, 2]})  # plain RangeIndex, no name
    with pytest.raises(ValueError, match="index level to be named"):
        quickhouse.from_pandas(df, ch_target, dest_table=unique_name, mode="full", index=True)


def test_knobs_that_cannot_apply_to_a_frame_are_refused(ch_target, unique_name):
    # The engine is the enforcement point, so a kwarg added to sync() later is
    # still caught without the Python layer knowing about it.
    for kwargs, needle in [
        ({"source_table": "orders"}, "source_table"),
        ({"chunk_rows": 1000}, "chunk_rows"),
        ({"read_max_rows_per_sec": 100}, "read_max_rows_per_sec"),
        ({"retry_max_attempts": 3}, "retry_max_attempts"),
    ]:
        with pytest.raises(RuntimeError, match=needle):
            quickhouse.from_pandas(
                _frame(2), ch_target, dest_table=unique_name, mode="full", key=["id"], **kwargs
            )


def test_pyarrow_and_polars_frames_produce_the_same_table(ch_client, ch_target, unique_name):
    """The Arrow path is generic, so the promise is not pandas-specific."""
    pl = pytest.importorskip("polars")
    df = _frame(20)
    inputs = {
        "pandas": df,
        "pyarrow": pa.Table.from_pandas(df, preserve_index=False),
        "polars": pl.from_pandas(df),
    }
    tables = {k: f"{unique_name}_{k}" for k in inputs}
    _drop(ch_client, *tables.values())
    try:
        for label, obj in inputs.items():
            quickhouse.from_pandas(
                obj, ch_target, dest_table=tables[label], mode="full", key=["id"]
            )
        counts, sums = set(), set()
        for t in tables.values():
            counts.add(int(ch_client.command(f"SELECT count() FROM `{t}`")))
            sums.add(round(float(ch_client.command(f"SELECT sum(amount) FROM `{t}`")), 6))
        assert counts == {20}
        assert len(sums) == 1, f"the three inputs disagreed on the data: {sums}"
    finally:
        _drop(ch_client, *tables.values())


def test_empty_frame_creates_the_table_with_no_rows(ch_client, ch_target, unique_name):
    table = unique_name
    _drop(ch_client, table)
    empty = _frame(0)
    try:
        result = quickhouse.from_pandas(
            empty, ch_target, dest_table=table, mode="full", key=["id"]
        )
        assert result.rows_written == 0
        assert int(ch_client.command(f"SELECT count() FROM `{table}`")) == 0
        # The schema still has to arrive — an empty frame carries one.
        cols = ch_client.query(
            "SELECT name FROM system.columns "
            f"WHERE database = currentDatabase() AND table = '{table}'"
        ).result_rows
        assert {c[0] for c in cols} == {"id", "name", "amount", "updated_at"}
    finally:
        _drop(ch_client, table)


def test_include_and_exclude_project_the_frame(ch_client, ch_target, unique_name):
    # include/exclude become an Arrow column projection applied by the reader.
    table = unique_name
    _drop(ch_client, table)
    try:
        quickhouse.from_pandas(
            _frame(5), ch_target, dest_table=table, mode="full", key=["id"],
            exclude=["name", "updated_at"],
        )
        cols = {
            c[0]
            for c in ch_client.query(
                "SELECT name FROM system.columns "
                f"WHERE database = currentDatabase() AND table = '{table}'"
            ).result_rows
        }
        assert cols == {"id", "amount"}
        assert int(ch_client.command(f"SELECT count() FROM `{table}`")) == 5
    finally:
        _drop(ch_client, table)


def test_not_a_frame_is_refused_with_a_useful_message():
    with pytest.raises(TypeError, match="pandas.DataFrame"):
        quickhouse.from_pandas({"id": [1]}, None, dest_table="t", mode="full")


def test_narrowing_a_decimal_column_keeps_values_exact(ch_client, ch_target, unique_name):
    """A frame decimal narrowed by type_overrides went through arrow's cast,
    which (arrow 53) added one unit to every value on a same-scale narrowing
    and turned values that no longer fit into NULL without a warning."""
    table = unique_name
    amounts = [decimal.Decimal(v) for v in ("0.00", "12.34", "-5.50", "99.99", "1234567890.12")]
    frame = pa.table(
        {"id": pa.array(range(1, 6), pa.int64()), "amount": pa.array(amounts, pa.decimal128(38, 2))}
    )
    _drop(ch_client, table)
    try:
        result = quickhouse.from_pandas(
            frame,
            ch_target,
            dest_table=table,
            mode="full",
            key=["id"],
            type_overrides={"amount": "Decimal(10, 2)"},
        )
        rows = ch_client.query(f"SELECT id, toString(amount) FROM `{table}` ORDER BY id").result_rows
        assert rows == [(1, "0"), (2, "12.34"), (3, "-5.5"), (4, "99.99"), (5, None)]
        coerced = [w for w in result.warnings if w.kind == "coerced_decimal"]
        assert [(w.column, w.count) for w in coerced] == [("amount", 1)]
    finally:
        _drop(ch_client, table)
