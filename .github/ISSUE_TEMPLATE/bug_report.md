---
name: Bug report
about: Something isn't working as documented
title: ""
labels: bug
assignees: ""
---

<!--
SECURITY: never paste real DSNs, passwords, API tokens, or service-account keys.
Redact credentials before submitting. For a security vulnerability, do NOT open a
public issue — see SECURITY.md.
-->

## What happened

A clear description of the bug and what you expected instead.

## Reproduction

A minimal `sync(...)` call (credentials redacted) that triggers it:

```python
import quickhouse as qh
# ...
```

If it errors, paste the full error message / traceback (redacted).

## Environment

- quickhouse version: <!-- python -c "import quickhouse; print(quickhouse.version())" -->
- Python version:
- OS / architecture:
- Source → destination: <!-- e.g. PostgreSQL -> ClickHouse -->
- Mode: <!-- full / incremental / append -->

## Anything else

Logs (`RUST_LOG=quickhouse_core=debug`), data-shape notes, or context that helps.
