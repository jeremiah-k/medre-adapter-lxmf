# Preserve uncertain native handoffs

Exceptions raised by the native router during or after submission produce a
non-retryable dispatch error with an explicit unknown handoff outcome. The
adapter does not infer recipient failure or resubmit a possibly accepted
message. Failures before the router call retain bounded construction retries.
