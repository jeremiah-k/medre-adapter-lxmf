# Publish local LXMF worker health

Publish provider-owned state through the optional runtime health sink.
Preserve native ownership, confirmation levels and explicit-probe compatibility.
Fence stopped and replacement lifetimes, deduplicate successful notifications
and contain observer errors without changing native operations.

Observe the owned worker leader's exit independently of pipe EOF. Preserve the
stream protocol's flow control and use the existing failure cleanup when a
descendant retains a protocol pipe.
