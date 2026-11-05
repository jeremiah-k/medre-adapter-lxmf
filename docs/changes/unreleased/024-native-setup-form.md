# Offline native setup form for LXMF connections

`medre config native --transport lxmf` collects provider-declared connection
prompts and validates them through the production configuration loader without
importing the transport SDKs or starting a runtime. Prompts cover the
connection mode, the private storage directory, the Reticulum config
directory, the announce display name, and delivery method selection with its
propagation-node requirement. Identity material is referenced by path only:
the form never provisions, generates, or writes a Reticulum identity, and
leaving the identity prompt blank keeps the worker's persist-a-dedicated-
identity behavior. Output is a new private, exclusively created YAML file.

Native-form metadata is optional for provider discovery: on an older
compatible core without that contract the provider still registers normally,
and the operator uses the manual configuration path.
