# Structured relay attribution

When metadata embedding is enabled, outbound MEDRE fields envelopes MUST include
valid shared v1 `bridge_context`, independently of the visible prefix setting.
The context MUST preserve the source's full sender claims, platform, origin label,
verification description, canonical event correlation and scoped native reference.
Route-level origin labels take precedence over the source configuration, including
explicit empty labels. Unknown verification MUST remain unknown.

The envelope retains its existing fields and schema version. Its additive context
is described in [lxmf-fields-envelope.schema.json](../schemas/lxmf-fields-envelope.schema.json).
The context's fields follow core's bridge-context schema. Invalid context MUST be
omitted without preventing message delivery. Disabling metadata embedding MUST
omit the context along with the envelope. Text-only fallback MUST omit relations
from both the existing envelope and the shared context.

Inbound context MUST be normalized and retained under
`metadata.custom.medre_envelope.bridge_context` as a remote claim. It MUST NOT
replace the actual LXMF sender, source native reference or local canonical ID,
and its relation hints MUST NOT create canonical relations. Invalid context MUST
be discarded without discarding the envelope's text message.

This metadata does not change an ordinary client's displayed native sender or
introduce puppeting. Visible attribution remains controlled by the relay prefix.
SDK and codec checks do not establish RF qualification.

The renderer MUST select a styled message's authoritative body over a stale text
alias. Unstyled legacy messages retain their text-first behavior.
