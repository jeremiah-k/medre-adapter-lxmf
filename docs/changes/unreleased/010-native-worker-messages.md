# Forward production LXMF messages through the native worker

Real Reticulum adapters own their SDK, identity and queue in an isolated process.
Private bounded messages carry native submissions, normalized inbound packets,
health snapshots and terminal delivery facts. Core attempt provenance remains
in the parent, including feedback received before a handoff reply or after
caller cancellation.

A private journal fences repeated attempts across worker lifetimes, preserves
uncertain outcomes and contains no plaintext message bodies. Generated identities
persist under storage_path. Concurrent native profile owners fail startup.
Filtered child environments exclude other adapters' credentials. Core retains
canonical admission, routing, outbox and delivery-feedback authority.
