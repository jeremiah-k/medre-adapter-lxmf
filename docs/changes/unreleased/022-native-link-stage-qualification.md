# Bound LXMF link failure observations

The independent native-peer campaign now reports a transport stage when an
inbound message misses its existing delivery deadline. Observations distinguish
shared-service disconnect, missing destination path, an unestablished direct
link and unobserved recipient handoff. The peer also reports the age of a
matching destination announce. These fields diagnose a failed stage; they do
not establish its root cause, change a send's authority, or retry a message.

Use `python scripts/qualify_native_link.py --runs 5` to repeat both native
restore directions in isolated process generations. The campaign returns a
nonzero status if any iteration fails and preserves existing 20-second native
delivery deadlines. Repeated results, including the failed stage, are required
before claiming the intermittent link issue resolved. Tests in this patch do
not prove MeshChat GUI or physical RF interoperability.

Classify missing and non-active links before recipient handoff. Derive link
usability from the pinned RNS `Link.ACTIVE` state for both direct links and
backchannels; path discovery alone does not establish a usable link.
