# Installation

## From PyPI

```bash
pip install quickhouse
```

Prebuilt wheels ship for **Python 3.9+** on:

- **Linux** (x86_64)
- **macOS** (Apple Silicon)
- **Windows** (x64)

On those platforms there is nothing else to install — no Rust toolchain, no JVM,
no separate service. quickhouse is an ordinary Python dependency that runs
wherever your jobs already run (cron, Airflow, Dagster, a Lambda, a plain
script).

## Optional extras

```bash
pip install "quickhouse[progress]"   # ready-made tqdm progress bar
pip install "quickhouse[cli]"         # TOML parser for `quickhouse run` on Python < 3.11
```

`quickhouse` extras:

`progress`
: Pulls in [tqdm](https://github.com/tqdm/tqdm) so you can use
  {func}`quickhouse.progress_bar` as a drop-in `on_progress` callback. See
  [Watching progress](guide/performance.md#watching-progress-and-diagnosing-failures).

`cli`
: A TOML parser (`tomli`) for the [`quickhouse run` command-line runner](cli.md),
  needed only on Python 3.9 / 3.10 (Python 3.11+ has `tomllib` in the standard
  library).

## Building from source

On platforms without a prebuilt wheel (e.g. Intel macOS, Linux aarch64) `pip`
builds from the source distribution, which needs a **Rust toolchain**
(1.75+, from [rustup.rs](https://rustup.rs)) and [maturin](https://www.maturin.rs):

```bash
pip install maturin
maturin develop --release      # compiles the Rust engine, installs into the venv
```

See [Contributing](contributing.md) for the full development setup, including the
Docker Compose stack used by the integration tests.

## Verify the install

```python
import quickhouse
print(quickhouse.version())     # prints the installed version string
```
