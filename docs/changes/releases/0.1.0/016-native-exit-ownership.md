# Retain LXMF endpoint leases through native exit persistence

Keep queue and identity ownership until SDK process-exit handlers finish
persisting native state. Real control probes acquire the same endpoint profile
leases as message workers. Separate LXMF profiles can share a Reticulum service;
the endpoint leases do not acquire or stop that service.
