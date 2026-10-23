# Add an owned native worker lifecycle

Add an internal typed process interface for native initialization and teardown,
with bounded readiness, deferred cancellation, process reaping and bounded
private stderr retention. Separate SDK signal ownership from the host process.

Qualify blocked constructors, real host signals, late spawn results, ignored
shutdown and native SDK initialization without interfaces. Production adapter
message and delivery-feedback wiring remains on the established session until
its worker counterpart is qualified.

- Preserve native cleanup acknowledgement independently of forced leader or
  descendant termination. Bound inherited-pipe draining and discard buffered
  configuration during teardown, including detached descendants retaining stdin.

Qualify consecutive native worker lifetimes against an opt-in private RNode
profile, requiring independently expected identity and acknowledged clean exits.
