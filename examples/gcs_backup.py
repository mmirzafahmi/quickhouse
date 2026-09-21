"""Back a transfer up to Google Cloud Storage while it runs.

Every batch written to the destination is also streamed to GCS as Parquet —
one file per parallel partition, never fully buffered — giving a data lake
independent of the destination's own retention.

Environment:
    PG_DSN          postgresql://user:pw@host:5432/db
    CH_URL          http://localhost:8123
    CH_DB           analytics
    GCS_BUCKET      a bucket you can write to
    GCS_PREFIX      optional object-name prefix (default "quickhouse")

Credentials resolve through Application Default Credentials and the standard
SERVICE_ACCOUNT / GOOGLE_SERVICE_ACCOUNT environment, exactly as for the
BigQuery descriptor — so `gcloud auth application-default login`, a
GOOGLE_APPLICATION_CREDENTIALS key file, or a service account attached to the
VM all work with nothing set here. Pass credentials_file= / credentials_json=
to GcsArchive to override.

Run:
    python examples/gcs_backup.py
"""

import os

import quickhouse as qh

PG_DSN = os.environ.get("PG_DSN", "postgresql://etl:etl@localhost:5432/etl")
CH_URL = os.environ.get("CH_URL", "http://localhost:8123")
CH_DB = os.environ.get("CH_DB", "default")
GCS_BUCKET = os.environ["GCS_BUCKET"]
GCS_PREFIX = os.environ.get("GCS_PREFIX", "quickhouse")


def main() -> None:
    src = qh.Postgres(PG_DSN)

    # backup() picks the backend; qh.GcsArchive(bucket=...) is the same thing
    # spelled explicitly. Set it on the DESTINATION — an archive on the source
    # descriptor writes nothing and warns.
    dst = qh.ClickHouse(
        CH_URL,
        database=CH_DB,
        archive=qh.backup(
            destination="gcs",
            format="parquet",
            bucket=GCS_BUCKET,
            prefix=GCS_PREFIX,
        ),
    )

    result = qh.sync(
        src,
        dst,
        dest_table="orders",
        source_table="orders",
        mode="full",
        key=["id"],
        parallelism=4,
    )

    print(f"{result.rows_written:,} rows in {result.duration_secs:.1f}s")
    # One object per partition, under a Hive-style date/run prefix that
    # BigQuery external tables, Spark and DuckDB can all prune on.
    print(f"archived to gs://{GCS_BUCKET}/{GCS_PREFIX}/orders/dt=<date>/run=<id>/part-*.parquet")

    # A failed upload fails the sync, so reaching here means the backup landed.
    for w in result.warnings:
        print(f"warning [{w.kind}]: {w.message}")


if __name__ == "__main__":
    main()
