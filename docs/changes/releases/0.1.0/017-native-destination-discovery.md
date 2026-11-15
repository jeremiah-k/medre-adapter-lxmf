# Discover LXMF destination paths before native handoff

Request a native path for a valid contact missing from Reticulum's discovery
cache and wait within a bounded pre-handoff budget. Keep unresolved discovery
retryable without constructing or submitting the message. Preserve cancellation
and uncertain-handoff behavior after the native submission boundary.
