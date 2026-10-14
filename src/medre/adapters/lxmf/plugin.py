"""Declarative registration for the separately distributed adapter."""

from medre.adapter_registry import AdapterSpec, DestinationSpec, SymbolRef


def adapter_spec() -> AdapterSpec:
    return AdapterSpec(
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
        import_names=("lxmf", "RNS"),
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
