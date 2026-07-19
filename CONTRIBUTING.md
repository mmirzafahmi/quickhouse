# Contributing to quickhouse

Thanks for your interest in improving quickhouse! Bug reports, new source/type
support, performance work, and documentation fixes are all welcome.

## Architecture at a glance

quickhouse is a Rust engine with a thin Python binding:

- **`crates/quickhouse-core`** — the pure-Rust engine. No Python; fully
  unit-testable on its own. This is where almost all logic lives (wire-protocol
  decoding, Arrow batching, type mapping, DDL, orchestration).
- **`crates/quickhouse-py`** — [PyO3](https://pyo3.rs) bindings compiled to a
  `cdylib` exposed as `quickhouse._quickhouse`. Keep this layer thin: parse
  Python arguments, build a `TransferConfig`, call into the core, translate
  results/errors back.
- **`python/quickhouse`** — the typed Python surface (`__init__.py`, `.pyi`
  stubs, the `progress_bar()` helper).

The data path is: source's native wire protocol → Apache Arrow `RecordBatch` →
ClickHouse `FORMAT ArrowStream`.

### Project layout

```
crates/quickhouse-core/    # pure-Rust engine (unit-testable, no Python)
  src/source/postgres.rs   # PostgreSQL: binary COPY, schema/partition queries
  src/source/mysql.rs      # MySQL: streaming SELECT, schema/partition queries
  src/source/bigquery.rs   # BigQuery: auth, schema resolution, Storage Read API
  src/decode.rs            # PostgreSQL COPY wire format -> Arrow
  src/decode_mysql.rs      # MySQL typed rows -> Arrow
  src/decode_bigquery.rs   # BigQuery typed rows -> Arrow
  src/types.rs             # per-source type -> Arrow -> ClickHouse mapping
  src/memory.rs            # MemoryBudget: total in-flight memory ceiling
  src/sink/clickhouse.rs   # ClickHouse HTTP sink (streaming compressed inserts, retry)
  src/config.rs            # TransferConfig and friends
  src/ddl.rs               # CREATE TABLE generation
  src/sync.rs              # orchestration; dispatches on the `Source` enum
crates/quickhouse-py/      # PyO3 bindings (cdylib -> quickhouse._quickhouse)
python/quickhouse/         # typed Python surface (__init__.py, .pyi stubs)
tests/                     # pytest integration tests
docker-compose.yml         # local PostgreSQL + MySQL + ClickHouse
```

## Development setup

Prerequisites:

- **Rust** toolchain, 1.75+ — install from <https://rustup.rs>
- **Python** 3.9+
- **maturin** — `pip install maturin`
- **Docker** (+ Compose) for the integration-test services

Build the extension into your active virtualenv:

```bash
pip install maturin
maturin develop --release      # compiles the Rust engine, installs into the venv
python -c "import quickhouse; print(quickhouse.version())"
```

`maturin develop` (no `--release`) builds faster but runs much slower — use
`--release` whenever you're measuring performance or running the integration
tests against real data.

Build a distributable wheel:

```bash
maturin build --release        # -> target/wheels/quickhouse-*.whl
```

## Running the tests

Rust unit tests (decoders, type map, DDL, memory budget) need no services and
are the fastest feedback loop:

```bash
cargo test -p quickhouse-core
```

Integration tests need live PostgreSQL, MySQL, and ClickHouse. They're skipped
automatically if a service is unreachable, so they never fail a machine without
Docker:

```bash
docker compose up -d           # PostgreSQL + MySQL + ClickHouse
pip install -e '.[test]'
maturin develop --release
pytest -v
```

Connection details default to the compose setup and can be overridden with
`QUICKHOUSE_*` environment variables (see `tests/conftest.py`).

## Before you open a PR

Please make sure:

```bash
cargo fmt --all                        # formatting
cargo clippy -p quickhouse-core        # no new warnings
cargo test -p quickhouse-core          # unit tests green
pytest -v                              # integration tests green (if services are up)
```

- Add a test for any behavior change — a Rust unit test in the relevant module
  and/or a pytest integration test under `tests/`. Bug fixes should come with a
  regression test that fails before the fix.
- Keep the PyO3 layer thin; put logic in `quickhouse-core` so it stays testable
  without Python.
- Match the surrounding code's style, naming, and comment density.

## Common changes

**Add a type mapping.** Map the source type in `src/types.rs` (`map_oid` for
Postgres, `mysql::map_mysql_type`, or `bigquery::map_type`) to an Arrow
`DataType` + ClickHouse type string, then handle that Arrow type in the matching
decoder's `ColBuilder` (`src/decode.rs` / `src/decode_mysql.rs` /
`src/decode_bigquery.rs`). Prefer coercing messy/out-of-range source values to
`NULL` (see how zero-dates and out-of-ClickHouse-range dates are handled) over
aborting a whole transfer.

**Add a `sync()` option.** Add the field to `TransferConfig` in
`src/config.rs` (with validation in `validate()` if needed), thread it through
`src/sync.rs`, then expose it as a keyword argument in
`crates/quickhouse-py/src/lib.rs` and document it in `python/quickhouse/*.pyi`
and the README's parameter table.

**Add a source.** This is the largest change: add a module under `src/source/`,
a decoder, type mappings in `types.rs`, a variant to the `Source`/`SourceConfig`
enums, dispatch in `src/sync.rs`, and a `#[pyclass]` in the Python bindings.
Opening an issue to discuss the shape first is a good idea.

## Releasing (maintainers)

CI (`.github/workflows/release.yml`) builds wheels for Linux (manylinux x86_64),
macOS (Intel + Apple Silicon), and Windows (x86_64) plus an sdist, then publishes
via **PyPI Trusted Publishing** (OIDC — no API tokens stored anywhere).

Cut a release by bumping the version in **both** `Cargo.toml` and
`pyproject.toml`, committing, then pushing a matching tag:

```bash
git tag v0.2.3
git push origin v0.2.3
```

Pushing the tag builds all wheels, publishes to TestPyPI automatically, then
waits for approval on the `pypi` environment before the real publish. Both the
version bump and the tag matter: the tag triggers the workflow, but PyPI rejects
a re-upload of an already-published version — so a forgotten bump silently
no-ops instead of releasing.

Verify the TestPyPI build before approving the real publish:

```bash
pip install --index-url https://test.pypi.org/simple/ \
            --extra-index-url https://pypi.org/simple/ quickhouse
```

## License

By contributing, you agree that your contributions are licensed under the
project's [MIT License](LICENSE).
