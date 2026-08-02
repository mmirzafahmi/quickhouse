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

```{raw} html
<div class="qh-params">
  <div>
    <div>
      <div class="qh-params__name">progress</div>
      <div class="qh-params__type">pip install "quickhouse[progress]"</div>
    </div>
    <p class="qh-params__desc">Pulls in <a href="https://github.com/tqdm/tqdm">tqdm</a> so you can use <code>quickhouse.progress_bar</code> as a drop-in <code>on_progress</code> callback. See <a href="guide/performance.html#watching-progress-and-diagnosing-failures">Watching progress</a>.</p>
  </div>
  <div>
    <div>
      <div class="qh-params__name">cli</div>
      <div class="qh-params__type">pip install "quickhouse[cli]"</div>
    </div>
    <p class="qh-params__desc">A TOML parser (<code>tomli</code>) for the <a href="cli.html"><code>quickhouse run</code> command-line runner</a>, needed only on Python 3.9 / 3.10 (Python 3.11+ has <code>tomllib</code> in the standard library).</p>
  </div>
</div>
```

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
