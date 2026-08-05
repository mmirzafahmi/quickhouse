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
    # lazily inside the function; quality.py imports great-expectations lazily
    # inside Validation.__call__), so copy them verbatim.
    for name in ("__init__.py", "progress.py", "quality.py", "py.typed"):
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

# DESIGN.md and the design handoff live under docs/ but are implementation
# reference for whoever edits the theme -- not published pages. Sphinx would
# otherwise read them as sources and warn that they are in no toctree, which
# is an error under -W.
exclude_patterns = [
    "_build",
    "_shim",
    "Thumbs.db",
    ".DS_Store",
    "design_handoff_*",
    "DESIGN.md",
]

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
templates_path = ["_templates"]     # _templates/page.html adds the Atlas top nav
html_css_files = ["quickhouse-atlas.css", "quickhouse-bench.css"]
html_js_files = ["quickhouse-atlas.js", "quickhouse-bench.js"]
html_favicon = "_static/favicon.svg"
# Furo renders the syntax itself; the Atlas theme owns all tokens (colors,
# fonts, both schemes) in _static/quickhouse-atlas.css. "nord" reads closest to
# the hand-marked hero in dark mode.
pygments_style = "friendly"
pygments_dark_style = "nord"

html_theme_options = {
    # Atlas: the top nav carries the wordmark, so hide it in the sidebar. One
    # teal/gold mark serves both schemes (fixed strokes read on light and dark).
    "light_logo": "quickhouse-mark.svg",
    "dark_logo": "quickhouse-mark.svg",
    "sidebar_hide_name": True,
    "navigation_with_keys": True,
    "source_repository": "https://github.com/mmirzafahmi/quickhouse/",
    "source_branch": "main",
    "source_directory": "docs/",
    "footer_icons": [],
}
