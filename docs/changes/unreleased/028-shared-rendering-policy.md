# Share rendering policy

LXMF now shares origin-label precedence, validates relay-prefix placeholders and applies context UTF-8 limits in addition to character limits. Direct structured relations remain separate from readable fallback descriptions.

Fake adapter setup validates supplied configuration through the native configuration contract. Unsupported prefix placeholders are rejected rather than emitted literally.
