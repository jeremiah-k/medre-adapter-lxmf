# Startup-info provider: held transport facts at ready

`LxmfAdapter` now exposes an optional, duck-typed `startup_info()`
callable that core discovers with `getattr` at the adapter's ready
transition and calls under a bounded, supervised wait. The adapter does
not import core symbols for the seam, so it stays installable against
older cores that never look for it. Failure, expiry, or absence of held
facts only means no startup block; it never affects readiness, routing,
or delivery, and the provider never gates, blocks, or delays the
interface.

The provider is a pure read of state the adapter already holds — no RNS
or interface I/O, no resource acquisition, no session mutation. Row 0 is
the adapter's own LXMF delivery-destination hash prefix (first 16 hex
characters, the same convention the session's announce logs use) once
the session has registered the delivery identity. Row 1 carries the
configured connection type and the installed RNS distribution version
read via `importlib.metadata` — never a source literal; the repository's
pin-hygiene guard keeps pin values in `pyproject.toml` only.

Facts that are not held are omitted rather than probed. The delivery
hash is absent until the delivery identity registers, and fake mode
never registers one. The RNode/serial/TCP interface detail is not held
state: it lives in the Reticulum configuration that RNS owns, which the
adapter never parses, so no interface-name fact is offered. An absent
`rns` distribution simply drops the version fact. The Reticulum
configuration directory path is deliberately not reported — it is a
local filesystem path, not an interface identity. No identity or key
material is offered: the destination prefix is a published mesh
identifier that appears in announce traffic and diagnostics, and it
renders in full under core's redaction policy. Redaction remains core's
render-boundary responsibility.

Both session surfaces expose the held hash through a new read-only
`delivery_destination_hash` property (`LxmfSession` and
`WorkerSession`), a single atomic attribute read that is safe from
worker threads. `FakeLxmfAdapter` mirrors the real adapter's provider
with configured transport facts only.

Pinned by `tests/test_lxmf_startup_info.py` (fake connection mode only,
no RNS traffic, no interface access). The core-side contract is
specified in core's `docs/spec/adapter-startup-info.md`.
