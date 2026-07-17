"""Shared pytest fixtures.

The integration tests need a live PostgreSQL, MySQL, and ClickHouse (see
``docker-compose.yml``). Connection details come from environment variables and
default to the compose setup. If a service is unreachable the whole module is
skipped rather than failed.
"""

from __future__ import annotations

import os
import uuid

import pytest

PG_DSN = os.environ.get("ETLHOUSE_PG_DSN", "postgresql://etl:etl@localhost:5432/etl")
MYSQL_DSN = os.environ.get("ETLHOUSE_MYSQL_DSN", "mysql://etl:etl@localhost:3306/etl")
MYSQL_HOST = os.environ.get("ETLHOUSE_MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("ETLHOUSE_MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("ETLHOUSE_MYSQL_USER", "etl")
MYSQL_PASSWORD = os.environ.get("ETLHOUSE_MYSQL_PASSWORD", "etl")
MYSQL_DB = os.environ.get("ETLHOUSE_MYSQL_DB", "etl")
CH_URL = os.environ.get("ETLHOUSE_CH_URL", "http://localhost:8123")
CH_HOST = os.environ.get("ETLHOUSE_CH_HOST", "localhost")
CH_PORT = int(os.environ.get("ETLHOUSE_CH_PORT", "8123"))
CH_DB = os.environ.get("ETLHOUSE_CH_DB", "default")
CH_USER = os.environ.get("ETLHOUSE_CH_USER", "default")
CH_PASSWORD = os.environ.get("ETLHOUSE_CH_PASSWORD", "")


@pytest.fixture(scope="session")
def pg_conn():
    psycopg = pytest.importorskip("psycopg")
    try:
        conn = psycopg.connect(PG_DSN, autocommit=True)
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"PostgreSQL unavailable at {PG_DSN}: {e}")
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def mysql_conn():
    pymysql = pytest.importorskip("pymysql")
    try:
        conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            autocommit=True,
        )
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"MySQL unavailable at {MYSQL_HOST}:{MYSQL_PORT}: {e}")
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def ch_client():
    cc = pytest.importorskip("clickhouse_connect")
    try:
        client = cc.get_client(
            host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database=CH_DB
        )
        client.command("SELECT 1")
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"ClickHouse unavailable at {CH_URL}: {e}")
    yield client
    client.close()


@pytest.fixture
def unique_name():
    return f"t_{uuid.uuid4().hex[:12]}"


@pytest.fixture
def pg_source():
    import etlhouse

    return etlhouse.Postgres(PG_DSN)


@pytest.fixture
def mysql_source():
    import etlhouse

    return etlhouse.MySQL(MYSQL_DSN)


@pytest.fixture
def ch_target():
    import etlhouse

    return etlhouse.ClickHouse(
        CH_URL, database=CH_DB, user=CH_USER, password=CH_PASSWORD, compression="gzip"
    )
