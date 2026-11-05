"""Declarative registration for the separately distributed adapter."""

from dataclasses import replace
from typing import Any

from medre import adapter_spec as registration
from medre.adapter_spec import AdapterSpec, DestinationSpec, SymbolRef


def adapter_spec() -> AdapterSpec:
    """Declare LXMF bindings and the offline native setup form.

    The form prompts reference existing Reticulum identity material by path;
    it never provisions, generates, or writes identity material. Operators
    without an identity file leave the prompt blank so the native worker
    persists a dedicated identity in the storage path on first start, or they
    prepare one with Reticulum tooling beforehand. A missing explicit identity
    file fails worker startup rather than being replaced.
    """
    field_type = getattr(registration, "NativeSetupField", None)
    native_setup: tuple[Any, ...] = ()
    if field_type is not None and hasattr(AdapterSpec, "native_setup_fields"):
        native_setup = (
            field_type(
                "connection_type",
                "Reticulum connection mode",
                choices=("reticulum",),
            ),
            field_type(
                "identity_path",
                "Existing Reticulum identity file (blank persists a dedicated "
                "identity in storage_path)",
                required=False,
                secret=True,
                active_when=("connection_type", "reticulum"),
            ),
            field_type(
                "reticulum_config_dir",
                "Reticulum config directory (blank uses the SDK's normal discovery)",
                required=False,
                active_when=("connection_type", "reticulum"),
            ),
            field_type(
                "storage_path",
                "Private LXMF storage directory (keep it stable across restarts)",
                active_when=("connection_type", "reticulum"),
            ),
            field_type(
                "display_name",
                "Display name for LXMF announces (blank announces without a name)",
                required=False,
                active_when=("connection_type", "reticulum"),
            ),
            field_type(
                "default_delivery_method",
                "Default delivery method (blank uses direct)",
                required=False,
                choices=("direct", "opportunistic", "propagated", "paper"),
                active_when=("connection_type", "reticulum"),
            ),
            field_type(
                "outbound_propagation_node",
                "Propagation node destination hash (32 hex characters)",
                active_when=("default_delivery_method", "propagated"),
            ),
        )
    spec = AdapterSpec(
        api_version=1,
        sample_config=SymbolRef("medre.adapters.lxmf.sample", "sample_config"),
        stopped_state_declaration=SymbolRef(
            "medre.adapters.lxmf.stopped_declaration", "declaration"
        ),
        destinations=(
            DestinationSpec(
                "lxmf_destination",
                SymbolRef(
                    "medre.config.adapters.lxmf_destinations", "validate_destination"
                ),
            ),
        ),
        transport="lxmf",
        config=SymbolRef("medre.config.adapters.lxmf", "LxmfConfig"),
        runtime_config=SymbolRef(
            "medre.config.adapters.lxmf_runtime", "LxmfRuntimeConfig"
        ),
        adapter=SymbolRef("medre.adapters.lxmf.adapter", "LxmfAdapter"),
        fake_adapter=SymbolRef("medre.adapters.fakes.lxmf", "FakeLxmfAdapter"),
        renderer_factory=SymbolRef(
            "medre.adapters.lxmf.renderer", "build_lxmf_renderer"
        ),
        dependency_probe=SymbolRef("medre.adapters.lxmf.compat", "HAS_LXMF"),
        distribution="lxmf",
        import_names=("LXMF", "RNS"),
        sdk_import_roots=("RNS", "LXMF", "lxmf"),
        native_namespace_reader=SymbolRef(
            "medre.adapters.lxmf.event_shape", "lxmf_namespace"
        ),
        versioned_namespace_reader=SymbolRef(
            "medre.adapters.lxmf.event_shape", "lxmf_versioned_namespace"
        ),
        attribution_projector=SymbolRef(
            "medre.adapters.lxmf.attribution", "project_lxmf_attribution"
        ),
        support_endpoint_fields=("storage_path", "display_name"),
        support_secret_fields=("identity_path",),
        native_detection_priority=40,
    )
    return replace(spec, native_setup_fields=native_setup) if native_setup else spec
