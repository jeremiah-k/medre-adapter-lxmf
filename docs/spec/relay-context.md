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

Optional `bridge_context.source.native_facts` is a single bounded public
source-namespace projection; LXMF uses the same normalized structure as Matrix
and XMPP. An LXMF recipient must never promote these hints into native sender
identity, mutation/reaction authority, or forwarding permission.

When rendering supplied source attribution, origin-label precedence MUST be the
explicit route/context label, then the attribution snapshot label, then the
configured source label. Only `None` falls through; an explicit empty label
MUST be preserved at each level.

## Visible prefix surface

The visible attribution template is the `relay_prefix` configuration key,
rendered through core's shared relay-prefix formatter. An empty string MUST
disable the prefix; the MEDRE metadata envelope remains the authoritative
provenance source either way. Prefix presentation MUST NOT rewrite canonical
sender metadata.

LXMF is a radio transport, and constrained client displays are the affordance
that motivates truncation. The label variables `sender`, `sender_short`, and
`origin_label` accept the shared `{nameN}` truncation suffix in configured
templates, rendering the first N Unicode code points of the variable's value.
The suffix counts code points, never bytes, and N MUST be at least 1
(`{sender0}` is not a truncation variable). A numeric-suffixed name whose stem
is not declared truncatable for this transport is rejected during configuration
validation — the
fixed-width `sender_id` source hash is deliberately left undeclared. The
shared relay-prefix formatter specification defines the full contract.
