# DESIGN.md — quickhouse documentation site

Implementation reference for the visual design of the quickhouse docs. Written
for whoever touches `docs/` next, human or agent.

Source of truth for the intended look: `QuickHouse Docs.dc.html` (interactive
prototype, dark scheme) and `preview/index.html` (the real landing markup
rendered against the real stylesheet, no Sphinx needed).

---

## 1. Architecture

Sphinx + MyST + the Furo theme. The design is **additive** — Furo is never
forked, and its templates are extended, not replaced.

| File | Owns | Editable |
|---|---|---|
| `_static/quickhouse-atlas.css` | Design tokens, typography, landing-page components (`.qh-hero`, `.qh-slab`, `.qh-stats`, `.qh-modes`, `.qh-params`, `.qh-crumb`, `.qh-index`), Furo overrides | Yes — this is the base layer |
| `_static/quickhouse-atlas.js` | Hero code tabs, `data-qh-copy` buttons, top-nav active state | Yes |
| `_static/quickhouse-bench.css` | Everything added after Atlas: bar charts, benchmark switch, teaser carousel, flow diagram, release band, body code-block chrome, interior-page corrections | Yes |
| `_static/quickhouse-bench.js` | Bar reveal, benchmark tabs, carousel, breadcrumbs, code-block chrome, sidebar expansion, mode-card selection | Yes |
| `_templates/page.html` | Announce strip + top nav, injected above Furo's content | Yes |

```python
# conf.py — order matters; bench extends atlas
html_css_files = ["quickhouse-atlas.css", "quickhouse-bench.css"]
html_js_files  = ["quickhouse-atlas.js", "quickhouse-bench.js"]
```

### Why the split

`atlas` is the design system. `bench` is everything added while matching the
prototype. Keeping them apart means Atlas stays diffable against its original
form, and any correction to Furo's defaults is visibly a correction. **New work
goes in `bench`** unless it is a genuine token or base-typography change.

---

## 2. Rules

These are the constraints that keep the site consistent. Break them and the
next page will not match.

1. **No per-page CSS.** If a page needs a treatment, it uses a class from
   §4. Nothing in this design requires a new class.
2. **Tokens only — never a literal colour.** Every value comes from a
   `--qh-*` custom property (§3). A hardcoded hex breaks the light/dark
   switch, silently, in one scheme only.
3. **Chrome is applied by JS, not authored per page.** Breadcrumbs, code-block
   language bars and copy buttons, sidebar expansion and mode-card state are
   all derived from the DOM Sphinx already produces. Do not hand-write them
   into markdown.
4. **Prose is not part of this design.** The prototype's body copy for
   interior pages is placeholder written from outside the project. Only
   `index.md` and `guide/benchmark.md` carry content authored here.
5. **Both schemes, every time.** Furo's light/dark toggle is live. Check both.
6. **`prefers-reduced-motion` disables motion, never content.** Bars appear
   full, packets stop, the carousel stops auto-advancing — nothing disappears.
7. **Raw HTML blocks are for components only.** Reach for
   `\`\`\`{raw} html` when a documented class needs markup MyST cannot express.
   Prose, tables and code stay in markdown so search and PDF export keep
   working.

---

## 3. Tokens

Defined in `quickhouse-atlas.css` against Furo's own custom-property layer, so
they flip with the theme automatically.

| Token | Role |
|---|---|
| `--qh-accent` | Brand yellow. Fills — buttons, bars, dots, tags. |
| `--qh-accent-text` | Accent tuned for legibility on the page background. **All accent text and icons.** |
| `--qh-accent-dim` | Accent at low alpha. Borders on highlighted surfaces. |
| `--qh-ink` | Body text. |
| `--qh-ink-muted` | Secondary text, captions, labels, mono eyebrows. |
| `--qh-rule` | Hairlines: borders, dividers, spines, inactive bar tracks. |
| `--qh-surface` | Raised panels — cards, code blocks, charts. |
| `--qh-sans` | UI and headings. |
| `--qh-mono` | Code, numbers, labels, eyebrows, anything tabular. |
| `--qh-serif` | Atlas's original heading face. **Not used** — `bench` overrides headings to sans to match the prototype. |
| `--color-background-primary` | Furo's page background. Use for text *on* an accent fill. |
| `--color-background-hover` | Furo's hover tint. Inactive tracks, hover states. |

Never use `--qh-accent` for text or `--qh-accent-text` for a fill.

### Type scale

Set in `bench` under "Interior page header", overriding Atlas's serif display
sizes to match the prototype.

| Element | Size | Notes |
|---|---|---|
| `h1` | 2.35rem / 1.1 | `-0.03em`. A flex container — see §6. |
| `h2` | 1.45rem | Divider rule above, except the first on a page. |
| `h3` | 1.1rem | |
| Body | Furo default | |
| Code in blocks | 0.78rem / 1.75 | Wraps; never scrolls horizontally. |
| Mono labels, eyebrows | 0.66–0.72rem | `0.13em` tracking, uppercase. |

### Motion

| Use | Timing |
|---|---|
| Hover, selection, colour | `0.18–0.22s ease` |
| Bar growth | `0.75s cubic-bezier(0.22, 0.85, 0.25, 1)`, staggered 80/200/320ms |
| Panel cross-fade | `0.35s ease` |
| Flow packet | `2.6s linear infinite`, lanes offset 0.42s |
| Carousel dwell | `4.6s`, paused on hover and focus |

Bars grow **from the left** (`transform-origin: left`, `scaleX`) — never by
animating `width`.

---

## 4. Component catalogue

Everything the design needs already exists. Match the pattern to the intent:

| Intent | Class | Defined in | Notes |
|---|---|---|---|
| Mono eyebrow above a title | `.qh-crumb` | atlas | **Injected by JS.** Never authored. |
| A small closed set of alternatives | `.qh-modes` / `.qh-mode` | atlas | Sync modes, connector pickers. Anchor children. `.qh-mode--current` marks the active one; JS keeps it in sync. |
| Name / type / description rows | `.qh-params` | atlas | Arguments, options, subcommands. **Not** a markdown table. |
| Numeric comparison | `.qh-bars` / `.qh-bar` | bench | `--qh-w` sets length, `--qh-delay` the stagger, `.qh-bar--lead` the accent row. |
| Two views of one dataset | `.qh-bench` | bench | Tablist + panels, arrow-key navigable. |
| Rotating landing stat | `.qh-teaser` | bench | `data-title` / `data-note` per slide drive the header and caption. |
| Pipeline diagram | `.qh-flow` | bench | Decorative; `aria-hidden` on lanes and hub. |
| Release highlight | `.qh-newband` | bench | One at a time, on the landing page only. |
| Caveat or tradeoff | `{note}` / `{warning}` / `{admonition}` | Furo + atlas | `warning` when ignoring it corrupts data. |
| Body code | fenced markdown | bench chrome | Language bar + copy button added by JS. |
| Hand-marked code | `.qh-slab pre`, `.qh-newband__code pre` | atlas / bench | Landing page **only** — accent spans are hand-authored. |
| API signatures | `dl.py > dt.sig` | atlas | Autodoc. Never hand-write. |

Landing-page-only: `.qh-hero`, `.qh-slab`, `.qh-stats`, `.qh-split`,
`.qh-teaser`, `.qh-newband`, `.qh-flow`, `.qh-index`.

---

## 5. Behaviour (`quickhouse-bench.js`)

One IIFE, no dependencies, `ready()` guard, seven independent passes. Each is
defensive — a missing element skips that pass, never throws.

1. **Bar reveal** — `IntersectionObserver` adds `.is-visible` to `.qh-bars` on
   entry. Without IO support, bars start full.
2. **Benchmark switch** — tablist over `.qh-bench__panel`, arrow keys, replays
   the bar growth on switch.
3. **Teaser carousel** — auto-advance, pause on hover/focus, dot navigation,
   header and caption read from the active slide's data attributes.
4. **Breadcrumbs** — derives `<sidebar caption> / <page title>` from the
   sidebar tree and inserts `.qh-crumb` above the `h1`. Skips the landing page.
5. **Code-block chrome** — wraps each `div.highlight` in `.qh-codeblock`, reads
   the language off the `highlight-*` class, adds the bar and copy button,
   hides Furo's own hover copy button.
6. **Sidebar expansion** — checks every `toctree-checkbox` on the path to the
   current page, so a nested section stays open on its own index page.
7. **Mode-card selection** — click and keyboard select a `.qh-mode`; a
   scroll-spy (`-25% / -60%` root margin) moves the selection to whichever
   linked section is in view. Cross-page grids get click behaviour only.

### Adding a pass

Append a numbered block inside the `ready()` callback. Query defensively,
return early if the host element is absent, and never assume a page has any
particular component.

---

## 6. Furo corrections

Non-obvious fixes in `bench`. Each exists for a reason worth keeping.

- **`.qh-hero > *` needs `grid-column: 1 / -1`.** `.qh-hero` is a two-column
  grid; anything added to it silently collapses into a narrow column.
- **`h1` is `display: flex`.** Sphinx leaves a whitespace text node between the
  title and its `¶` headerlink, which renders as a leading space. Flex drops it
  so the title aligns with the breadcrumb above.
- **First `h2` loses its divider.** The between-sections rule and its 3rem of
  air are wrong directly under a page title.
- **Sidebar logo and search hidden.** The top nav already carries brand and
  search; Furo's duplicates pushed the tree down.
- **Content icon row hidden.** Furo's eye/edit/theme row is not part of the
  design.
- **Sidebar carets styled.** Furo emits a hidden checkbox and `<label>` for
  nested toctrees; Atlas never styled them, so the dropdown affordance was
  invisible.
- **`.qh-mode` needs `cursor: pointer`.** They are anchors, but nothing said
  so — no pointer, no hover movement, no caret.
- **Content gutters at 3.25rem.** Furo's default is roughly half the
  prototype's.

---

## 7. Content ownership

| Page | Content | Note |
|---|---|---|
| `index.md` | Authored here | Full landing composition. |
| `guide/benchmark.md` | Authored here | Transcribed from the author's benchmark document. Keep **Limitations** prominent and linked from the intro. |
| `guide/sources/*`, `guide/destinations/*` | Split from the originals | Verbatim. Each index carries a `.qh-modes` grid plus a hidden `{toctree}`. |
| Everything else | Pre-existing | Chrome and components only. **Do not rewrite prose.** |

### Numbers that must agree

Landing hero `TransferResult`, `guide/benchmark.md`, and the teaser carousel
all quote the same run: **299,540 rows / 0.94 s**. BigQuery cost figures are
1,000 × per-run bytes at on-demand $6.25/TiB — 28 MiB → $0.17, 122 MiB → $0.73,
200 MiB → $1.19. Change one, change all.

The announce strip, the `.qh-newband` tag and the changelog's newest entry all
claim the same headline feature. Change one, change all three.

---

## 8. Checklist

Per page:

- [ ] Breadcrumb reads `<sidebar caption> / <page title>`.
- [ ] First `h2` has no divider rule; later ones do.
- [ ] Sidebar current page is accent **text**, not a filled pill.
- [ ] Nested sections stay expanded when you are inside them.
- [ ] Every code block has a language bar and a working copy button.
- [ ] Options and arguments use `.qh-params`, not a table.
- [ ] Renders correctly in **both** schemes.
- [ ] No horizontal scroll at 1280 / 1024 / 768px.
- [ ] Nothing breaks with `prefers-reduced-motion: reduce`.
- [ ] No hardcoded colour; no per-page CSS.

Landing page also:

- [ ] Both sidebars hidden (`.qh-hero-page`).
- [ ] Hero tabs, both copy buttons, carousel dots all work.
- [ ] Bars grow on scroll; flow packets animate; band renders full-width.
- [ ] Doc index links resolve, including nested section indexes.

---

## 9. Iterating

`preview/index.html` renders the landing page's raw-HTML blocks against the
real `quickhouse-atlas.css` with no Sphinx — seconds per change instead of a
full build. `preview/interior.html` does the same for a doc page's DOM shape.
For real pages, `sphinx-autobuild docs docs/_build/html`.

If a change appears to have no effect, verify the built asset rather than the
source: `grep -c "<your selector>" docs/_build/html/_static/quickhouse-bench.css`.
Zero means Sphinx did not re-copy `_static`; a match means browser cache. On
OneDrive-backed WSL paths, `rm -rf docs/_build` fails on permissions — build
elsewhere:

```bash
sphinx-build -a -E docs /tmp/qh-docs && python -m http.server -d /tmp/qh-docs 8000
```
