"""Tests for the GCS Parquet data-lake archive
(``ClickHouse``/``BigQuery(..., archive=GcsArchive(...))``, or the
``backup(destination="gcs")`` factory).

**Why there is no GCS emulator here.** ``object_store`` writes GCS objects
through Google's *XML* API (``PUT {base}/{bucket}/{object}``), while
fake-gcs-server — the usual local stand-in — implements only the *JSON* upload
API (``POST /upload/storage/v1/b/{bucket}/o?uploadType=...``). A PUT to the XML
path returns ``400 invalid uploadType`` no matter how the server is
configured, so an emulator cannot exercise this path at all. MinIO is not a
substitute either: it speaks S3, which is the other backend.

So the coverage splits three ways, and the split is deliberate:

* **Client construction and credential precedence** — Rust unit tests in
  ``crates/quickhouse-core/src/archive.rs``. No network.
* **The archive plumbing itself** (that a destination or source shape reaches
  the writer at all) — ``tests/test_s3_archive.py`` against MinIO. That code
  is backend-blind: ``SendCtx.archive`` holds an ``Arc<dyn ObjectStore>`` and
  never learns which cloud it is.
* **A real GCS round trip** — the tests below marked ``live``, which run only
  when ``QUICKHOUSE_GCS_BUCKET`` names a bucket you can write to. They skip
  everywhere else, including CI.

Everything above the ``live`` marker needs no services and always runs::

    pip install -e '.[test]' && maturin develop --release
    pytest tests/test_gcs_archive.py -v

    # and, against a bucket you own (google-cloud-storage is deliberately not
    # in the `test` extra: CI never reaches these, so it would be dead weight
    # in both pyproject.toml and the CI pip line):
    pip install google-cloud-storage
    QUICKHOUSE_GCS_BUCKET=my-test-bucket pytest tests/test_gcs_archive.py -v
"""

from __future__ import annotations

import io

import pytest

import quickhouse

from conftest import CH_DB, CH_PASSWORD, CH_URL, CH_USER, GCS_BUCKET

# --------------------------------------------------------------------------
# No services required.
# --------------------------------------------------------------------------


def test_backup_factory_returns_the_right_descriptor():
    assert isinstance(quickhouse.backup(bucket="b"), quickhouse.GcsArchive)
    for name in ("gcs", "GCS", " gs ", "google"):
        assert isinstance(quickhouse.backup(destination=name, bucket="b"), quickhouse.GcsArchive)
    for name in ("s3", "S3", "aws"):
        assert isinstance(quickhouse.backup(destination=name, bucket="b"), quickhouse.S3Archive)


def test_backup_factory_rejects_unsupported_arguments():
    """These must fail at the call, not inside a ``sync()`` that has already
    started moving data."""
    with pytest.raises(ValueError, match="parquet"):
        quickhouse.backup(destination="gcs", format="csv", bucket="b")
    with pytest.raises(ValueError, match="expected 'gcs' or 's3'"):
        quickhouse.backup(destination="azure", bucket="b")


def test_backup_factory_forwards_backend_options():
    """kwargs go straight to the descriptor, so the factory cannot drift from
    it — and an option meant for the other cloud is a TypeError, not a
    silently dropped argument."""
    assert "my-lake" in repr(quickhouse.backup(destination="gcs", bucket="my-lake", prefix="qh"))
    with pytest.raises(TypeError):
        quickhouse.backup(destination="gcs", bucket="b", region="us-east-1")
    with pytest.raises(TypeError):
        quickhouse.backup(destination="s3", bucket="b", credentials_json="{}")


def test_both_destinations_accept_a_gcs_archive():
    """The archive used to be read off a ClickHouse destination only; a
    BigQuery destination silently ignored it."""
    archive = quickhouse.backup(destination="gcs", bucket="b")
    quickhouse.ClickHouse(CH_URL, database=CH_DB, archive=archive)
    quickhouse.BigQuery("some-project", dataset_id="analytics", archive=archive)


def test_archive_on_the_source_descriptor_warns(pg_conn, ch_client, pg_source, ch_target, unique_name):
    """`archive=` belongs on the destination. Set on a source it does nothing
    — archiving is a write-path option — and that used to pass in total
    silence, which is the one way a backup fails that nobody notices.

    Needs no object store: the archive is dropped before any client is built,
    so the bucket below is never contacted."""
    table = unique_name
    dest = f"{table}_copy"
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, name text)')
        with cur.copy(f'COPY "{table}" (id, name) FROM STDIN') as copy:
            for i in range(1, 11):
                copy.write_row((i, f"row-{i}"))
    for t in (table, dest):
        ch_client.command(f"DROP TABLE IF EXISTS `{t}`")
        ch_client.command(f"DROP TABLE IF EXISTS `{t}_quickhouse_tmp`")
    try:
        quickhouse.sync(
            pg_source, ch_target, dest_table=table, source_table=table,
            mode="full", key=["id"], create_if_missing=True,
        )
        src = quickhouse.ClickHouse(
            CH_URL, database=CH_DB, user=CH_USER, password=CH_PASSWORD,
            archive=quickhouse.backup(destination="gcs", bucket="never-contacted"),
        )
        result = quickhouse.sync(
            src, ch_target, dest_table=dest, source_table=table,
            mode="full", key=["id"], create_if_missing=True,
        )
        assert result.rows_written == 10
        kinds = [w.kind for w in result.warnings]
        assert "ignored_source_archive" in kinds, kinds
    finally:
        for t in (table, dest):
            ch_client.command(f"DROP TABLE IF EXISTS `{t}`")
            ch_client.command(f"DROP TABLE IF EXISTS `{t}_quickhouse_tmp`")


def test_a_dataframe_source_really_reaches_the_archive(ch_client, unique_name):
    """Regression: `run_transfer_frame` hardcoded ``archive: None``, so a
    backup configured on a DataFrame sync produced no files and no error.

    Proven without an object store by pointing the archive at a dead port: a
    wired archive must fail the sync trying to upload, while the old silently
    skipping behaviour would return success. That asymmetry is exactly the
    bug, so this test fails against the old code for the right reason.
    """
    pytest.importorskip("pyarrow")
    import pyarrow as pa

    table = unique_name
    ch_client.command(f"DROP TABLE IF EXISTS `{table}`")
    ch_client.command(f"DROP TABLE IF EXISTS `{table}_quickhouse_tmp`")
    frame = pa.table({"id": list(range(1, 21)), "name": [f"row-{i}" for i in range(1, 21)]})
    dst = quickhouse.ClickHouse(
        CH_URL, database=CH_DB, user=CH_USER, password=CH_PASSWORD,
        archive=quickhouse.S3Archive(
            bucket="unreachable", prefix="lake",
            endpoint="http://127.0.0.1:1",  # nothing listens here
            access_key_id="x", secret_access_key="y", region="us-east-1",
        ),
    )
    try:
        with pytest.raises(RuntimeError, match="archive"):
            quickhouse.from_pandas(
                frame, dst, dest_table=table,
                mode="full", key=["id"], create_if_missing=True,
            )
    finally:
        ch_client.command(f"DROP TABLE IF EXISTS `{table}`")
        ch_client.command(f"DROP TABLE IF EXISTS `{table}_quickhouse_tmp`")


def test_gcs_archive_rejects_empty_bucket(pg_source):
    # The bucket check lives in the Rust `into_config()` conversion, which only
    # runs at sync()-time (matching BigQuery's `dataset_id` requiredness
    # check) — constructing GcsArchive(...) alone validates nothing. This must
    # fail before any real connection is made.
    dst = quickhouse.ClickHouse(CH_URL, archive=quickhouse.GcsArchive(bucket=""))
    with pytest.raises(RuntimeError, match="non-empty bucket"):
        quickhouse.sync(pg_source, dst, dest_table="x", source_table="y", mode="full")


# --------------------------------------------------------------------------
# A real bucket: QUICKHOUSE_GCS_BUCKET=... pytest tests/test_gcs_archive.py
# --------------------------------------------------------------------------

live = pytest.mark.skipif(
    not GCS_BUCKET,
    reason="set QUICKHOUSE_GCS_BUCKET to a writable bucket to run the live GCS tests",
)


@pytest.fixture
def gcs_client():
    storage = pytest.importorskip("google.cloud.storage")
    return storage.Client().bucket(GCS_BUCKET)


def _seed_table(pg_conn, table: str, rows: int):
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, name text, amount double precision)')
        with cur.copy(f'COPY "{table}" (id, name, amount) FROM STDIN') as copy:
            for i in range(1, rows + 1):
                copy.write_row((i, f"row-{i}", i * 1.5))


def _drop_ch(ch_client, table: str):
    ch_client.command(f"DROP TABLE IF EXISTS `{table}`")
    ch_client.command(f"DROP TABLE IF EXISTS `{table}_quickhouse_tmp`")


def _read_parquet_objects(bucket, prefix: str):
    """Download every Parquet object under `prefix` as one combined table.
    Each blob must parse on its own, so this also proves none was left
    truncated or unfinalized."""
    pq = pytest.importorskip("pyarrow.parquet")
    import pyarrow as pa

    blobs = sorted(bucket.client.list_blobs(bucket, prefix=prefix), key=lambda b: b.name)
    tables = [pq.read_table(io.BytesIO(b.download_as_bytes())) for b in blobs]
    return [b.name for b in blobs], (pa.concat_tables(tables) if tables else None)


@live
def test_live_gcs_archive_matches_clickhouse(pg_conn, ch_client, pg_source, gcs_client, unique_name):
    table = unique_name
    n = 500
    _seed_table(pg_conn, table, n)
    _drop_ch(ch_client, table)
    prefix = f"quickhouse-test/{table}/"
    dst = quickhouse.ClickHouse(
        CH_URL, database=CH_DB, user=CH_USER, password=CH_PASSWORD,
        archive=quickhouse.backup(
            destination="gcs", format="parquet",
            bucket=GCS_BUCKET, prefix="quickhouse-test",
        ),
    )
    try:
        result = quickhouse.sync(
            pg_source, dst, dest_table=table, source_table=table,
            mode="full", key=["id"], create_if_missing=True, parallelism=1,
        )
        assert result.rows_written == n
        assert int(ch_client.command(f"SELECT count() FROM `{table}`")) == n

        names, archived = _read_parquet_objects(gcs_client, prefix)
        assert len(names) == 1, f"parallelism=1 should write exactly one file, got {names}"
        assert names[0].endswith(".parquet") and f"{table}/dt=" in names[0]
        assert archived.num_rows == n
        assert sorted(archived.column("id").to_pylist()) == list(range(1, n + 1))
        expected = sum(i * 1.5 for i in range(1, n + 1))
        assert abs(sum(archived.column("amount").to_pylist()) - expected) < 1e-6
    finally:
        _drop_ch(ch_client, table)
        for blob in list(gcs_client.client.list_blobs(gcs_client, prefix=prefix)):
            blob.delete()


@live
def test_live_gcs_archive_one_file_per_partition(pg_conn, ch_client, pg_source, gcs_client, unique_name):
    table = unique_name
    n = 2000
    _seed_table(pg_conn, table, n)
    _drop_ch(ch_client, table)
    prefix = f"quickhouse-test/{table}/"
    dst = quickhouse.ClickHouse(
        CH_URL, database=CH_DB, user=CH_USER, password=CH_PASSWORD,
        archive=quickhouse.backup(destination="gcs", bucket=GCS_BUCKET, prefix="quickhouse-test"),
    )
    try:
        assert quickhouse.sync(
            pg_source, dst, dest_table=table, source_table=table,
            mode="full", key=["id"], create_if_missing=True, parallelism=4,
        ).rows_written == n
        names, archived = _read_parquet_objects(gcs_client, prefix)
        assert len(names) == 4, f"parallelism=4 should write 4 files, got {names}"
        assert archived.num_rows == n
        assert sorted(archived.column("id").to_pylist()) == list(range(1, n + 1))
    finally:
        _drop_ch(ch_client, table)
        for blob in list(gcs_client.client.list_blobs(gcs_client, prefix=prefix)):
            blob.delete()
