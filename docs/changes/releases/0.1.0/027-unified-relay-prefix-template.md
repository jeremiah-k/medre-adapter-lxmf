# Unified relay-prefix template surface

The LXMF adapter's relay-prefix configuration key is the shared
`relay_prefix` name. The config field, validation messages, renderer reads,
renderer fixtures, and config tests use the unified key; no compatibility
alias exists. The empty-string default keeps the prefix disabled and the
validation tri-state is unchanged.

LXMF radio clients are this transport's constrained-display affordance: the
label variables `sender`, `sender_short`, and `origin_label` accept
`{nameN}` truncation suffixes in configured templates, rendering the first N
Unicode code points (N at least 1). `{sender3}` now works in a configured
`relay_prefix`. A numeric suffix on an undeclared variable passes through
unchanged, per the shared formatter contract.
