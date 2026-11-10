# Bounded native source facts over LXMF fields

The custom LXMF metadata field can carry the shared v1 source-native fact
projection while keeping native source identity authoritative. New wire schema
validation and round-trip tests cover the bounded generic shape. Support for
relay claims never grants routing or signature authority.

Preserve configured origin labels when supplied attribution leaves the label
unspecified. Explicit route labels and snapshot labels, including empty labels,
retain precedence over the configured source label.
