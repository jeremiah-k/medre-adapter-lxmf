# Preserve runtime signals across both SDK constructors

Restore the enclosing runtime's SIGINT and SIGTERM handlers around Reticulum
and LXMF construction, including failures. Retain native atexit and shared
Reticulum ownership, and surface signal preservation failures as warnings.
