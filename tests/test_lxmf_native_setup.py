"""SDK-free native form metadata remains provider-owned and deterministic.

The offline form references existing Reticulum identity material by path. It
never provisions, generates, or writes identity material, never imports the
transport SDKs, and refuses values the pure configuration model rejects.
"""

import io
import os
import sys
from argparse import Namespace
from dataclasses import fields as model_fields

import pytest

from medre import adapter_spec as registration
from medre.adapters.lxmf.plugin import adapter_spec

_SDK_MODULE_NAMES = ("lxmf", "LXMF", "RNS")


def test_native_setup_fields_are_sdk_free_and_named_for_the_config_model():
    spec = adapter_spec()
    fields = spec.native_setup_fields
    names = {field.name for field in fields}

    assert names <= {field.name for field in model_fields(spec.config.load())}
    assert len(names) == len(fields)
    transport = next(field for field in fields if field.name == "connection_type")
    assert transport.choices == ("reticulum",)
    for field in fields:
        if field.name != "connection_type":
            assert field.active_when is not None


def test_native_setup_prompt_order_keeps_conditions_behind_controllers():
    names = [field.name for field in adapter_spec().native_setup_fields]

    assert names[0] == "connection_type"
    assert names.index("outbound_propagation_node") > names.index(
        "default_delivery_method"
    )


def test_older_core_without_native_form_still_discovers_provider(monkeypatch):
    monkeypatch.delattr(registration, "NativeSetupField", raising=False)
    spec = adapter_spec()
    assert spec.transport == "lxmf"
    assert not spec.native_setup_fields
    assert spec.config.load().__name__ == "LxmfConfig"


def test_identity_reference_is_hidden_optional_input():
    identity = next(
        field
        for field in adapter_spec().native_setup_fields
        if field.name == "identity_path"
    )
    assert identity.secret and not identity.required
    assert identity.active_when == ("connection_type", "reticulum")


def test_propagation_node_is_required_only_for_propagated_delivery():
    fields = {field.name: field for field in adapter_spec().native_setup_fields}
    node = fields["outbound_propagation_node"]
    method = fields["default_delivery_method"]

    assert node.required
    assert node.active_when == ("default_delivery_method", "propagated")
    assert not method.required
    assert method.choices == ("direct", "opportunistic", "propagated", "paper")


def test_storage_path_prompt_is_required_for_reticulum_connections():
    storage = next(
        field
        for field in adapter_spec().native_setup_fields
        if field.name == "storage_path"
    )
    assert storage.required


def _run_native_form(monkeypatch, tmp_path, answers, secret=""):
    from medre.cli import native_commands
    from medre.config._yaml import parse_yaml_config

    class Terminal(io.StringIO):
        def isatty(self):
            return True

    output = tmp_path / "connection.yaml"
    monkeypatch.setenv("MEDRE_HOME", str(tmp_path / "state"))
    monkeypatch.setattr("sys.stdin", Terminal())
    replies = iter(answers)
    monkeypatch.setattr(native_commands, "_ask", lambda *args: next(replies))
    monkeypatch.setattr(native_commands.getpass, "getpass", lambda *args: secret)
    monkeypatch.setattr("medre.cli.bridge_commands._confirm_save", lambda *args: True)
    native_commands._config_native(
        Namespace(transport="lxmf", adapter_id="bot", config=None, output=str(output))
    )
    assert not (tmp_path / "state").exists()
    return parse_yaml_config(output.read_text())["adapters"]["lxmf"]["bot"]


def test_native_form_validates_a_complete_reticulum_connection(monkeypatch, tmp_path):
    identity = tmp_path / "reticulum-identity"
    values = _run_native_form(
        monkeypatch,
        tmp_path,
        ["reticulum", "", str(tmp_path / "lxmf-storage"), "MEDRE Bot", "direct"],
        secret=str(identity),
    )
    assert values["adapter_kind"] == "real"
    assert values["connection_type"] == "reticulum"
    assert values["identity_path"] == str(identity)
    assert values["storage_path"] == str(tmp_path / "lxmf-storage")
    assert values["display_name"] == "MEDRE Bot"
    assert values["default_delivery_method"] == "direct"
    assert "reticulum_config_dir" not in values
    assert not identity.exists()


def test_native_form_writes_a_private_exclusive_output_file(monkeypatch, tmp_path):
    output = tmp_path / "connection.yaml"
    _run_native_form(
        monkeypatch,
        tmp_path,
        ["reticulum", "", str(tmp_path / "lxmf-storage"), "", ""],
    )
    assert output.exists()
    assert os.stat(output).st_mode & 0o777 == 0o600


def test_native_form_never_overwrites_an_existing_output_file(
    monkeypatch, tmp_path, capsys
):
    from medre.cli import native_commands

    class Terminal(io.StringIO):
        def isatty(self):
            return True

    output = tmp_path / "connection.yaml"
    output.write_text("keep: me\n", encoding="utf-8")
    monkeypatch.setenv("MEDRE_HOME", str(tmp_path / "state"))
    monkeypatch.setattr("sys.stdin", Terminal())
    replies = iter(["reticulum", "", str(tmp_path / "lxmf-storage"), "", ""])
    monkeypatch.setattr(native_commands, "_ask", lambda *args: next(replies))
    monkeypatch.setattr(native_commands.getpass, "getpass", lambda *args: "")
    monkeypatch.setattr("medre.cli.bridge_commands._confirm_save", lambda *args: True)
    with pytest.raises(SystemExit) as error:
        native_commands._config_native(
            Namespace(
                transport="lxmf", adapter_id="bot", config=None, output=str(output)
            )
        )
    assert error.value.code == 2
    assert "already exists" in capsys.readouterr().err.lower()
    assert output.read_text(encoding="utf-8") == "keep: me\n"


def test_native_form_refuses_to_fabricate_identity_material_when_missing(
    monkeypatch, tmp_path
):
    values = _run_native_form(
        monkeypatch,
        tmp_path,
        ["reticulum", "", str(tmp_path / "lxmf-storage"), "", ""],
        secret="",
    )
    assert values["connection_type"] == "reticulum"
    assert values["storage_path"] == str(tmp_path / "lxmf-storage")
    assert "identity_path" not in values
    planted = [
        path.name
        for path in tmp_path.rglob("*")
        if path.is_file() and "identity" in path.name.lower()
    ]
    assert planted == []


def test_native_form_records_the_identity_reference_without_echoing_it(
    monkeypatch, tmp_path, capsys
):
    identity = tmp_path / "reticulum-identity"
    _run_native_form(
        monkeypatch,
        tmp_path,
        ["reticulum", "", str(tmp_path / "lxmf-storage"), "", ""],
        secret=str(identity),
    )
    captured = capsys.readouterr()
    assert str(identity) not in captured.out
    assert str(identity) not in captured.err


def test_native_form_imports_no_transport_sdk(monkeypatch, tmp_path):
    for name in tuple(sys.modules):
        if any(
            name == root or name.startswith(f"{root}.") for root in _SDK_MODULE_NAMES
        ):
            monkeypatch.delitem(sys.modules, name, raising=False)
    _run_native_form(
        monkeypatch,
        tmp_path,
        ["reticulum", "", str(tmp_path / "lxmf-storage"), "", ""],
        secret=str(tmp_path / "reticulum-identity"),
    )
    assert not any(
        name == root or name.startswith(f"{root}.")
        for name in sys.modules
        for root in _SDK_MODULE_NAMES
    )


def test_native_form_refuses_an_invalid_propagation_node(monkeypatch, tmp_path, capsys):
    from medre.cli import native_commands

    class Terminal(io.StringIO):
        def isatty(self):
            return True

    output = tmp_path / "connection.yaml"
    monkeypatch.setenv("MEDRE_HOME", str(tmp_path / "state"))
    monkeypatch.setattr("sys.stdin", Terminal())
    replies = iter(
        [
            "reticulum",
            "",
            str(tmp_path / "lxmf-storage"),
            "",
            "propagated",
            "not-a-hash",
        ]
    )
    monkeypatch.setattr(native_commands, "_ask", lambda *args: next(replies))
    monkeypatch.setattr(native_commands.getpass, "getpass", lambda *args: "")
    monkeypatch.setattr("medre.cli.bridge_commands._confirm_save", lambda *args: True)
    with pytest.raises(SystemExit) as error:
        native_commands._config_native(
            Namespace(
                transport="lxmf", adapter_id="bot", config=None, output=str(output)
            )
        )
    assert error.value.code == 2
    assert "outbound_propagation_node" in capsys.readouterr().err
    assert not output.exists()


def test_older_core_without_native_setup_fields_still_discovers_provider(monkeypatch):
    from medre.adapters.lxmf import plugin

    class LegacyAdapterSpec:
        def __init__(self, **fields):
            assert "native_setup_fields" not in fields
            self.transport = fields["transport"]

    monkeypatch.setattr(plugin, "AdapterSpec", LegacyAdapterSpec)
    spec = plugin.adapter_spec()
    assert spec.transport == "lxmf"
