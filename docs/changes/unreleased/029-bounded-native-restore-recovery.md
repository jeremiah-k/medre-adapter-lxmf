# Bound native restore recovery qualification

The local stopped-application campaign retains its 20-second healthy deadline.
An unfinished native submission may receive a bounded observation extension
when the independent peer and MEDRE worker report complete, positive recovery
facts. Terminal failure, exhausted native attempts, missing route evidence,
detachment, and unavailable workers end recovery at the next bounded check.
An already-delivered sender cannot grant an extension; recipient handoff still
has to occur within an admitted window.

The allowance follows the declared SDK constants: two retry waits, two zero-hop
link-establishment allowances, and five outbound polling turns. At the current
pins this is 64 seconds total, including the healthy deadline. Diagnostic time
counts against that receive ceiling. The native router owns the attempt limit;
qualification does not resubmit, reset links, or change transport state.

A callback first observed after the healthy deadline requires recovery
admission, including when it arrives during diagnosis. Denied native status
precedes accepting a diagnosis-time callback. Successful asynchronous reads
also check the absolute receive ceiling; observing a result late is not proof
of on-time handoff.

Every accepted delivery must match the run's exact content nonce. Unexpected
peer deliveries remain assertion failures, including during status collection.
Deterministic regressions cover malformed recovery evidence, terminal state
transitions, deadline races, and router tick phases.

This corrects an insufficient qualification budget. It does not establish the
cause of the first replacement handshake loss or prove physical RF reliability.
The earlier twenty-second-only campaign descriptions remain historical records.
