# Correlate native peer acceptance before receipt qualification

The shared-Reticulum restore campaign waits for an exact peer submission token
before beginning its unchanged receive deadline. A drained control pipe alone
is not native acceptance. Both inbound-first and outbound-first restore
assertions remain mandatory and no duplicate submission is introduced.

Failure diagnostics include local acceptance age, direct-link age and native
establishment timeout, expected hops, retry scheduling and outbound processing
state. These observations do not reset links, alter paths or warm the restored
endpoint. The underlying intermittent native link failure is not declared
resolved by successful repetitions or by this qualification correction.
