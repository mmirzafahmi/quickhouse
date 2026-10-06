"""Integration tests: the optional Parquet data-lake archive, against a local
MinIO service (no AWS account needed).

Doubles as the regression suite for the *backend-blind* archive plumbing —
which source shapes and which destinations reach the writer at all. That code
holds an `Arc<dyn ObjectStore>` and never learns which cloud it is, so proving
it here proves it for GCS too. It has to live here rather than in
`test_gcs_archive.py` because MinIO is the only archive backend with a working
local emulator: `object_store` writes GCS objects through the XML API, which
fake-gcs-server does not implement.

Run against the services in ``docker-compose.yml`` after building the module:

    docker compose up -d
    pip install -e '.[test]'
    maturin develop --release
    pytest tests/test_s3_archive.py -v
"""

from __future__ import annotations

import io

import pytest

import quickhouse

from conftest import CH_DB, CH_PASSWORD, CH_URL, CH_USER, MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY


@pytest.fixture(scope="session")
def s3_client():
    boto3 = pytest.importorskip("boto3")
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    try:
        client.list_buckets()
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"MinIO unavailable at {MINIO_ENDPOINT}: {e}")
    return client


@pytest.fixture
def minio_bucket(s3_client, unique_bucket_name):
    bucket = unique_bucket_name
    s3_client.create_bucket(Bucket=bucket)
    yield bucket
    objs = s3_client.list_objects_v2(Bucket=bucket).get("Contents", [])
    for o in objs:
        s3_client.delete_object(Bucket=bucket, Key=o["Key"])
    # Only a failing test leaves one, but it would block the bucket's delete.
    for u in s3_client.list_multipart_uploads(Bucket=bucket).get("Uploads", []):
        s3_client.abort_multipart_upload(Bucket=bucket, Key=u["Key"], UploadId=u["UploadId"])
    s3_client.delete_bucket(Bucket=bucket)


def _archive_target(bucket: str, prefix: str = "lake", **kwargs):
    return quickhouse.ClickHouse(
        CH_URL,
        database=CH_DB,
        user=CH_USER,
        password=CH_PASSWORD,
        archive=quickhouse.S3Archive(
            bucket=bucket,
            prefix=prefix,
            endpoint=MINIO_ENDPOINT,
            access_key_id=MINIO_ACCESS_KEY,
            secret_access_key=MINIO_SECRET_KEY,
            region="us-east-1",
            **kwargs,
        ),
    )


def _seed_table(pg_conn, table: str, rows: int):
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, name text, amount double precision)')
        with cur.copy(f'COPY "{table}" (id, name, amount) FROM STDIN') as copy:
            for i in range(1, rows + 1):
                copy.write_row((i, f"row-{i}", i * 1.5))


def _read_parquet_objects(s3_client, bucket: str, prefix: str):
    """Download every Parquet object under `prefix` and return one combined
    pyarrow Table (each file is a valid, independent Parquet file — this
    verifies that too, since a truncated/corrupt file fails to parse)."""
    pq = pytest.importorskip("pyarrow.parquet")
    import pyarrow as pa

    keys = [o["Key"] for o in s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])]
    tables = []
    for key in keys:
        body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
        tables.append(pq.read_table(io.BytesIO(body)))
    return keys, pa.concat_tables(tables) if tables else None


def _drop_ch(ch_client, table: str):
    ch_client.command(f"DROP TABLE IF EXISTS `{table}`")
    ch_client.command(f"DROP TABLE IF EXISTS `{table}_quickhouse_tmp`")


def _dest_that_stops_after(ch_client, table: str, max_id: int):
    """A destination that rejects any row past `max_id`, so a transfer into it
    fails part-way at a known row, and accepts everything once
    `stop_here` is dropped."""
    _drop_ch(ch_client, table)
    ch_client.command(
        f"CREATE TABLE `{table}` (id Int64, name Nullable(String), amount Nullable(Float64), "
        f"CONSTRAINT stop_here CHECK id <= {max_id}) ENGINE = ReplacingMergeTree ORDER BY id"
    )


def test_archive_parquet_matches_clickhouse_single_partition(pg_conn, ch_client, pg_source, s3_client, minio_bucket, unique_name):
    table = unique_name
    n = 500
    _seed_table(pg_conn, table, n)
    _drop_ch(ch_client, table)
    dst = _archive_target(minio_bucket)
    try:
        result = quickhouse.sync(
            pg_source, dst, dest_table=table, source_table=table,
            mode="full", key=["id"], create_if_missing=True, parallelism=1,
        )
        assert result.rows_written == n

        ch_count = int(ch_client.command(f"SELECT count() FROM `{table}`"))
        assert ch_count == n

        keys, archived = _read_parquet_objects(s3_client, minio_bucket, f"lake/{table}/")
        assert len(keys) == 1, f"parallelism=1 should produce exactly one Parquet file, got {keys}"
        assert keys[0].startswith(f"lake/{table}/dt=") and keys[0].endswith(".parquet")
        assert archived.num_rows == n

        # Row-for-row correctness, not just counts.
        pg_sum = sum(i * 1.5 for i in range(1, n + 1))
        assert abs(sum(archived.column("amount").to_pylist()) - pg_sum) < 1e-6
        assert sorted(archived.column("id").to_pylist()) == list(range(1, n + 1))
    finally:
        _drop_ch(ch_client, table)


def test_archive_one_parquet_file_per_partition(pg_conn, ch_client, pg_source, s3_client, minio_bucket, unique_name):
    table = unique_name
    n = 2000
    _seed_table(pg_conn, table, n)
    _drop_ch(ch_client, table)
    dst = _archive_target(minio_bucket)
    try:
        result = quickhouse.sync(
            pg_source, dst, dest_table=table, source_table=table,
            mode="full", key=["id"], create_if_missing=True, parallelism=4,
        )
        assert result.rows_written == n

        keys, archived = _read_parquet_objects(s3_client, minio_bucket, f"lake/{table}/")
        assert len(keys) == 4, f"parallelism=4 should produce exactly 4 Parquet files (one per partition), got {keys}"
        assert archived.num_rows == n
        assert sorted(archived.column("id").to_pylist()) == list(range(1, n + 1))
    finally:
        _drop_ch(ch_client, table)


def test_archive_disabled_by_default(pg_conn, ch_client, pg_source, ch_target, unique_name):
    """No `archive=` at all -> zero effect on a plain ClickHouse sync (the
    common case, and this must never regress)."""
    table = unique_name
    _seed_table(pg_conn, table, 10)
    _drop_ch(ch_client, table)
    try:
        result = quickhouse.sync(
            pg_source, ch_target, dest_table=table, source_table=table,
            mode="full", key=["id"], create_if_missing=True,
        )
        assert result.rows_written == 10
    finally:
        _drop_ch(ch_client, table)


def test_archive_rejects_empty_bucket(pg_source):
    # The bucket check lives in the Rust `into_config()` conversion, which
    # only runs at sync()-time (matching BigQuery's `dataset_id` requiredness
    # check) — constructing ClickHouse(...)/S3Archive(...) alone never
    # validates anything. This must fail before any real connection is made.
    dst = quickhouse.ClickHouse(CH_URL, archive=quickhouse.S3Archive(bucket=""))
    with pytest.raises(RuntimeError, match="non-empty bucket"):
        quickhouse.sync(
            pg_source, dst, dest_table="x", source_table="y", mode="full"
        )


def test_archive_fires_for_a_dataframe_source(ch_client, s3_client, minio_bucket, unique_name):
    """Regression: `run_transfer_frame` hardcoded `archive: None`, so a backup
    configured on a DataFrame sync produced no files and no error — the one
    way a backup fails that nobody notices."""
    pytest.importorskip("pyarrow")
    import pyarrow as pa

    table = unique_name
    _drop_ch(ch_client, table)
    frame = pa.table({"id": list(range(1, 51)), "name": [f"row-{i}" for i in range(1, 51)]})
    try:
        result = quickhouse.from_pandas(
            frame, _archive_target(minio_bucket), dest_table=table,
            mode="full", key=["id"], create_if_missing=True,
        )
        assert result.rows_written == 50
        keys, archived = _read_parquet_objects(s3_client, minio_bucket, f"lake/{table}/")
        assert keys, "a DataFrame source must archive too, not silently skip"
        assert archived.num_rows == 50
        assert sorted(archived.column("id").to_pylist()) == list(range(1, 51))
    finally:
        _drop_ch(ch_client, table)


def test_a_resumed_chunked_run_archives_every_row(
    pg_conn, ch_client, pg_source, s3_client, minio_bucket, unique_name
):
    """Regression: a `chunk_rows` read commits its cursor after every chunk, but
    archived the whole run to one file, finished only after the last chunk. A
    run that failed part-way left no object, the run that resumed archived only
    the chunks it read itself, and the backup silently lacked every row the
    failed attempt had committed. Each chunk now gets its own file, finished
    before its cursor commits."""
    table = unique_name
    n, chunk = 1000, 100
    _seed_table(pg_conn, table, n)
    _dest_that_stops_after(ch_client, table, max_id=450)  # chunk 5 of 10 fails
    dst = _archive_target(minio_bucket)
    kw = dict(
        dest_table=table, source_table=table, mode="incremental", watermark="id",
        key=["id"], chunk_rows=chunk, advance_watermark=False, create_if_missing=False,
    )
    try:
        with pytest.raises(RuntimeError, match="stop_here"):
            quickhouse.sync(pg_source, dst, **kw)
        ch_client.command(f"ALTER TABLE `{table}` DROP CONSTRAINT stop_here")
        resumed = quickhouse.sync(pg_source, dst, **kw)
        assert resumed.rows_read == n - 400, "the retry must resume after the 4 committed chunks"

        keys, archived = _read_parquet_objects(s3_client, minio_bucket, f"lake/{table}/")
        assert sorted(archived.column("id").to_pylist()) == list(range(1, n + 1))
        # One file per chunk across both runs; the failed chunk's never finished.
        assert len(keys) == n // chunk, keys
        assert all(k.rsplit("/", 1)[1].startswith("part-keyset-") for k in keys), keys
    finally:
        _drop_ch(ch_client, table)


def test_a_failed_run_leaves_no_multipart_upload_behind(
    pg_conn, ch_client, pg_source, s3_client, minio_bucket, unique_name
):
    """Regression: a file bigger than the 10 MiB upload buffer goes up as a
    multipart upload, which becomes an object only once completed. A run that
    failed after its archive had sent parts neither completed nor aborted the
    upload, so the parts stayed in the bucket, invisible and billed, until a
    lifecycle rule removed them, if there was one. A failed run now aborts every
    upload it didn't finish.

    One chunk, past the first Parquet row group (1,048,576 rows) and so past
    10 MiB: its file is already uploading when the destination rejects a later
    row, and a chunk's file is finished only after the chunk has landed."""
    table = unique_name
    n = 1_100_000
    with pg_conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(
            f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, name text, amount double precision)'
        )
        cur.execute(
            f'INSERT INTO "{table}" SELECT g, md5(g::text), g * 1.5 '
            f"FROM generate_series(1, {n}) g"
        )
    _dest_that_stops_after(ch_client, table, max_id=1_090_000)
    try:
        with pytest.raises(RuntimeError, match="stop_here"):
            quickhouse.sync(
                pg_source, _archive_target(minio_bucket), dest_table=table, source_table=table,
                mode="incremental", watermark="id", key=["id"], chunk_rows=n,
                create_if_missing=False,
            )
        uploads = s3_client.list_multipart_uploads(Bucket=minio_bucket).get("Uploads", [])
        assert uploads == [], f"the failed run left {len(uploads)} multipart upload(s) open"
        assert not s3_client.list_objects_v2(Bucket=minio_bucket).get("Contents")
    finally:
        _drop_ch(ch_client, table)
        with pg_conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
