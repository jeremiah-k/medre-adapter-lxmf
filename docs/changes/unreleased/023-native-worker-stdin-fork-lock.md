# Keep protocol input locks out of SDK fork children

The native worker now reads protocol frames from a dup of file descriptor 0
instead of ``sys.stdin.buffer``. The worker parks a reader thread on that
stream for its whole life, and the SDK forks stamp-generation subprocesses
whose first bootstrap step closes ``sys.stdin``. A fork child inherited the
buffer lock held by the parked reader and deadlocked before running its job,
which stalled every LXMF send that required a stamp behind a permanently
pending delivery. The dup'd raw descriptor consumes the same protocol stream
while leaving ``sys.stdin`` and its locks free; environments without a
dupable descriptor 0 keep the previous stream. A parked-reader fork probe
pins both the working shape and the hazard it removes.
