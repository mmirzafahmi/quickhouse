"""Sphinx configuration for the quickhouse documentation.

The public API is implemented in Rust and exposed through the compiled
extension module ``quickhouse._quickhouse`` (see ``crates/quickhouse-py``).
Its rich, human-facing docstrings, however, live in the type stub
``python/quickhouse/_quickhouse.pyi`` — the compiled module only carries the
short one-line summaries from the ``#[pyo3]`` doc comments.

To document the *stub's* docstrings without needing a Rust toolchain on the
build host, this config materializes the ``.pyi`` as an importable
pure-Python package into ``docs/_shim`` and puts it first on ``sys.path``.
``autodoc`` then introspects that shim (never the compiled extension), so the
build is fast, deterministic, and identical locally and on Read the Docs.
"""

from __future__ import annotations

import pathlib
import shutil
import sys
import tomllib

DOCS = pathlib.Path(__file__).parent.resolve()
ROOT = DOCS.parent
PKG_SRC = ROOT / "python" / "quickhouse"

# -- Single source of truth for the version -------------------------------
with (ROOT / "pyproject.toml").open("rb") as _f:
    _project = tomllib.load(_f)["project"]

release = _project["version"]
version = ".".join(release.split(".")[:2])


# -- Build the importable pure-Python shim of the package -----------------
def _build_shim() -> pathlib.Path:
    """Materialize ``quickhouse`` as pure Python so autodoc can import it
    without the compiled Rust extension. Returns the dir to add to sys.path."""
    shim_root = DOCS / "_shim"
    pkg = shim_root / "quickhouse"
    if shim_root.exists():
        shutil.rmtree(shim_root)
    pkg.mkdir(parents=True)

    # The pure-Python modules import cleanly as-is (progress.py imports tqdm
    # lazily, inside the function), so copy them verbatim.
    for name in ("__init__.py", "progress.py", "py.typed"):
        shutil.copy(PKG_SRC / name, pkg / name)

    # Turn the type stub into a real, importable module. The stub only
    # *annotates* ``__version__`` (``__version__: str``) without assigning it,
    # so append a concrete value — otherwise ``from ._quickhouse import
    # __version__`` in __init__.py would fail at import time.
    stub = (PKG_SRC / "_quickhouse.pyi").read_text(encoding="utf-8")
    stub += f'\n\n__version__ = "{release}"\n'
    (pkg / "_quickhouse.py").write_text(stub, encoding="utf-8")

    return shim_root


sys.path.insert(0, str(_build_shim()))

# CONTRIBUTING.md (included on the Contributing page) links to the repo's
# LICENSE with a root-relative path; make it resolve from docs/ by copying the
# file in at build time (gitignored — no committed duplicate).
shutil.copy(ROOT / "LICENSE", DOCS / "LICENSE")

# -- Project information --------------------------------------------------
project = "quickhouse"
author = "M Mirza Fahmi"
copyright = "2026, M Mirza Fahmi"

# -- General configuration ------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",       # NumPy-style docstrings (Parameters/Notes/...)
    "sphinx.ext.intersphinx",
    "myst_parser",               # author pages in Markdown
    "sphinx_copybutton",         # copy button on code blocks
    "sphinx_design",             # cards, grids, tabs
]

exclude_patterns = ["_build", "_shim", "Thumbs.db", ".DS_Store"]

# -- MyST (Markdown) ------------------------------------------------------
myst_enable_extensions = [
    "colon_fence",     # ::: fenced directives
    "deflist",         # definition lists
    "substitution",    # {{ version }} substitutions
    "linkify",         # bare URLs -> links
]
myst_heading_anchors = 3          # auto anchors for h1-h3 (cross-page links)
myst_substitutions = {"version": release}

# -- Autodoc / napoleon ---------------------------------------------------
autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": False,
    "member-order": "bysource",
}
autodoc_typehints = "description"     # render type hints in the body, not the signature
autodoc_class_signature = "mixed"
# Concatenate the class docstring with __init__'s, so per-constructor notes
# (e.g. the mTLS caveat on Postgres/MySQL) are included.
autoclass_content = "both"
napoleon_numpy_docstring = True
napoleon_google_docstring = False
napoleon_use_rtype = False

# Nitpicky would flood warnings for stdlib typing generics we don't own; keep
# it off but still surface genuine broken cross-references during the build.
nitpicky = False

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

# -- HTML output ----------------------------------------------------------
html_theme = "furo"
html_title = f"quickhouse {release}"
html_static_path = ["_static"]
html_css_files = ["custom.css"]
html_favicon = "_static/favicon.svg"
# Pygments is a fallback only — custom.css overrides the token colors per scheme
# to match the Console syntax palette exactly.
pygments_style = "friendly"
pygments_dark_style = "github-dark"

# ---------------------------------------------------------------------------
# "Console" design system — one theme, two schemes (light 2a / dark 1b).
# Space Grotesk + JetBrains Mono; deep-teal links on light, neon on dark, gold
# as the secondary. Furo's standard tokens carry structure; the --qh-* tokens
# carry the Console-specific accents and are consumed by custom.css.
# ---------------------------------------------------------------------------
_FONTS = {
    "--font-stack": "'Space Grotesk', system-ui, -apple-system, sans-serif",
    "--font-stack--monospace": "'JetBrains Mono', ui-monospace, 'SF Mono', Menlo, monospace",
}

_LIGHT_VARS = {
    **_FONTS,
    # brand / structure
    "--color-brand-primary": "#0b8f74",
    "--color-brand-content": "#0b8f74",
    "--color-brand-visited": "#0b8f74",
    "--color-background-primary": "#fcfdfc",
    "--color-background-secondary": "#f7faf9",
    "--color-background-hover": "#eff4f2",
    "--color-background-border": "#e4eae7",
    "--color-foreground-primary": "#0f1614",
    "--color-foreground-secondary": "#4c5b55",
    "--color-foreground-muted": "#66776f",
    "--color-foreground-border": "#d7e0dc",
    "--color-sidebar-background": "#f9fbfa",
    "--color-inline-code-background": "#eef4f1",
    "--color-api-name": "#0b8f74",
    "--color-api-pre-name": "#a06b00",
    "--color-highlight-on-target": "rgba(46, 230, 176, 0.20)",
    "--color-admonition-title--note": "#0b8f74",
    "--color-admonition-title-background--note": "rgba(46, 230, 176, 0.10)",
    # Console accents
    "--qh-neon": "#2ee6b0",
    "--qh-gold": "#d4b400",
    "--qh-eyebrow": "#0b8f74",
    "--qh-ink": "#0f1614",
    "--qh-muted": "#66776f",
    "--qh-faint": "#8a978f",
    "--qh-border": "#e4eae7",
    "--qh-panel-bg": "#f7faf9",
    "--qh-code-bg": "#ffffff",
    "--qh-code-header-bg": "#eff4f2",
    "--qh-chip-border": "#e0e7e4",
    "--qh-btn-bg": "#0f1614",
    "--qh-btn-fg": "#fcfdfc",
    "--qh-on-neon": "#04120e",
    "--qh-exp-bg": "#fdf8e6",
    "--qh-exp-border": "#e6d9a8",
    "--qh-exp-fg": "#8a6d10",
    # syntax
    "--qh-c-name": "#a06b00",
    "--qh-c-str": "#0b8f74",
    "--qh-c-kw": "#8a978f",
    "--qh-c-num": "#6d5ae0",
    "--qh-c-com": "#8a978f",
}
_DARK_VARS = {
    **_FONTS,
    "--color-brand-primary": "#2ee6b0",
    "--color-brand-content": "#2ee6b0",
    "--color-brand-visited": "#2ee6b0",
    "--color-background-primary": "#0b100f",
    "--color-background-secondary": "#12201b",
    "--color-background-hover": "#1a2422",
    "--color-background-border": "#24312d",
    "--color-foreground-primary": "#e4efea",
    "--color-foreground-secondary": "#9fb2ab",
    "--color-foreground-muted": "#7f918b",
    "--color-foreground-border": "#2d3a35",
    "--color-sidebar-background": "#0e1413",
    "--color-inline-code-background": "#12201b",
    "--color-api-name": "#2ee6b0",
    "--color-api-pre-name": "#ffe94a",
    "--color-highlight-on-target": "rgba(255, 233, 74, 0.14)",
    "--color-admonition-title--note": "#2ee6b0",
    "--color-admonition-title-background--note": "rgba(46, 230, 176, 0.12)",
    "--qh-neon": "#2ee6b0",
    "--qh-gold": "#ffe94a",
    "--qh-eyebrow": "#2ee6b0",
    "--qh-ink": "#e4efea",
    "--qh-muted": "#9fb2ab",
    "--qh-faint": "#7f918b",
    "--qh-border": "#24312d",
    "--qh-panel-bg": "#12201b",
    "--qh-code-bg": "#0f1a17",
    "--qh-code-header-bg": "#131c19",
    "--qh-chip-border": "#2d3a35",
    "--qh-btn-bg": "#2ee6b0",
    "--qh-btn-fg": "#04120e",
    "--qh-on-neon": "#04120e",
    "--qh-exp-bg": "rgba(255, 233, 74, 0.08)",
    "--qh-exp-border": "#3d3714",
    "--qh-exp-fg": "#ffe94a",
    "--qh-c-name": "#ffe94a",
    "--qh-c-str": "#2ee6b0",
    "--qh-c-kw": "#7f918b",
    "--qh-c-num": "#7fa8ff",
    "--qh-c-com": "#5f6f69",
}

html_theme_options = {
    "light_logo": "logo.svg",
    "dark_logo": "logo-dark.svg",
    "light_css_variables": _LIGHT_VARS,
    "dark_css_variables": _DARK_VARS,
    "source_repository": "https://github.com/mmirzafahmi/quickhouse/",
    "source_branch": "main",
    "source_directory": "docs/",
    "footer_icons": [
        {
            "name": "GitHub",
            "url": "https://github.com/mmirzafahmi/quickhouse",
            "html": (
                '<svg stroke="currentColor" fill="currentColor" stroke-width="0" '
                'viewBox="0 0 16 16"><path fill-rule="evenodd" d="M8 0C3.58 0 0 '
                '3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 '
                '0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01'
                '1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95'
                '0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 '
                '2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 '
                '1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 '
                '2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"></path></svg>'
            ),
            "class": "",
        },
    ],
}
