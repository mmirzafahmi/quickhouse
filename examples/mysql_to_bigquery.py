"""Sync a MySQL table into BigQuery.

Shows that the same `sync()` call works across engines: swap the source/target
descriptors and everything else stays the same. BigQuery is used here as the
*destination* (set `dataset_id`); the same `BigQuery` class is also a source.

Prerequisites
-------------
- `pip install quickhouse`
- A reachable MySQL (defaults match `docker-compose.yml`).
- A Google Cloud project + dataset, and credentials. quickhouse uses
  Application Default Credentials (ADC) by default; run `gcloud auth
  application-default login`, or set BQ_CREDENTIALS_FILE to a service-account
  key path. (This example talks to real BigQuery — it will bill/insert.)

Environment variables:
    MYSQL_DSN        e.g. mysql://etl:etl@localhost:3306/etl
    BQ_PROJECT       Google Cloud project id                (required)
    BQ_DATASET       destination dataset id                 (required)
    BQ_CREDENTIALS_FILE  optional service-account key path (else ADC)
    SRC_TABLE, DEST_TABLE
"""

import os
import sys

import quickhouse as qh


def main() -> None:
    project = os.getenv("BQ_PROJECT")
    dataset = os.getenv("BQ_DATASET")
    if not project or not dataset:
        sys.exit("Set BQ_PROJECT and BQ_DATASET (this example writes to real BigQuery).")

    src = qh.MySQL(os.getenv("MYSQL_DSN", "mysql://etl:etl@localhost:3306/etl"))
    dst = qh.BigQuery(
        project,
        dataset_id=dataset,
        credentials_file=os.getenv("BQ_CREDENTIALS_FILE"),  # None -> ADC
    )
    src_table = os.getenv("SRC_TABLE", "orders")

    result = qh.sync(
        src,
        dst,
        dest_table=os.getenv("DEST_TABLE", src_table),
        source_table=src_table,
        mode="incremental",
        watermark=os.getenv("WATERMARK", "updated_at"),
        key=[os.getenv("KEY", "id")],   # required for a BigQuery incremental MERGE
        create_if_missing=True,
    )
    print(result)


if __name__ == "__main__":
    main()
