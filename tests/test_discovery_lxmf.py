"""Discovery contract for a separately installed MEDRE transport."""
from importlib.metadata import entry_points

from medre.adapter_registry import AdapterSpec, get_adapter_spec


def test_adapter_entry_point() -> None:
    matches = [ep for ep in entry_points(group="medre.adapters") if ep.name == "lxmf"]
    assert len(matches) == 1
    spec = matches[0].load()()
    assert isinstance(spec, AdapterSpec)
    assert spec.transport == "lxmf"
    assert spec.adapter.module == "medre.adapters.lxmf.adapter"
    assert spec.config.module == "medre.config.adapters.lxmf"
    assert get_adapter_spec("lxmf") == spec


def test_declared_sdk_import_names_match_actual_module_spelling() -> None:
    """The import vocabulary must reflect LXMF's uppercase module name."""
    from medre.adapters.lxmf.plugin import adapter_spec

    spec = adapter_spec()
    assert spec.import_names == ("LXMF", "RNS")
    assert spec.dependency_probe is not None
    assert spec.dependency_probe.module == "medre.adapters.lxmf.compat"
    assert spec.dependency_probe.name == "HAS_LXMF"
