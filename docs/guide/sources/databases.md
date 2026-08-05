# Databases

PostgreSQL, MySQL and BigQuery. Connection descriptors and credentials for all
three are on the [Sources index](index.md#authentication).

## BigQuery as a source

`source_table` should be `"dataset.table"` or `"project.dataset.table"`. Reads
use the BigQuery Storage Read API; `parallelism` becomes a server-side
stream-count hint, but rows are consumed on a single client connection
(BigQuery parallelizes server-side).
