# Security Policy

## Supported versions

quickhouse is pre-1.0. Security fixes are applied to the **latest released
`0.x` version** only. Please upgrade to the newest release before reporting.

## Reporting a vulnerability

**Please do not open a public issue for security problems.**

Report privately through GitHub's **[Report a vulnerability](https://github.com/mmirzafahmi/quickhouse/security/advisories/new)**
flow (Security → Advisories → *Report a vulnerability*). This opens a private
advisory visible only to the maintainers.

Please include:

- affected version(s) and platform,
- a description of the issue and its impact,
- steps to reproduce (a minimal snippet is ideal), and
- any known mitigations.

You can expect an initial acknowledgement within a few days. Once a fix is
released we will publish an advisory and credit the reporter unless you prefer
to remain anonymous.

## Scope notes

quickhouse handles database credentials and connection strings. When filing a
report or a regular bug, **never paste real DSNs, passwords, API tokens, or
service-account keys** — redact them first. Connection descriptors are held only
in memory for the duration of a transfer and are never logged; credential values
are omitted from every object's `repr()` and from error messages.
