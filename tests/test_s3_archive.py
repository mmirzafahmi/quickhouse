"""Integration tests: the optional S3 data-lake archive for a ClickHouse
destination, against a local MinIO service (no AWS account needed).

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
        quickhouse.sync(pg_source, dst, dest_table="x", source_table="y")
