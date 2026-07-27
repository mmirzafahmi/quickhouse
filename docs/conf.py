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
pygments_style = "friendly"
pygments_dark_style = "monokai"
html_theme_options = {
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
