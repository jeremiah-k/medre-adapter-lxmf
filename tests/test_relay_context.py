"""LXMF carries source attribution even when its visible prefix is disabled."""

from datetime import datetime, timezone
from types import SimpleNamespace
import json
from pathlib import Path

import msgspec
import pytest
from jsonschema import Draft202012Validator

from medre.adapters.lxmf.codec import LxmfCodec
from medre.adapters.lxmf.fields import LxmfFieldsHelper, FIELD_MEDRE_ENVELOPE
from medre.adapters.lxmf.renderer import LxmfRenderer
from medre.config.adapters.lxmf import LxmfConfig
from medre.core.events import (
    CanonicalEvent,
    EventMetadata,
    EventRelation,
    NativeMetadata,
    NativeRef,
)
from medre.core.rendering.attribution import RelayAttribution
from medre.core.rendering.renderer import RenderingContext


def source_event(payload=None, relations=()):
    """Build a named native source with an exact reference and private metadata sentinel."""
    return CanonicalEvent(
        event_id="canonical-source",
        event_kind="message.created",
        schema_version=1,
        timestamp=datetime(2026, 10, 25, tzinfo=timezone.utc),
        source_adapter="radio-east",
        source_transport_id="ab" * 16,
        source_channel_id="conversation",
        parent_event_id=None,
        lineage=(),
        relations=relations,
        payload=payload or {"body": "hello"},
        metadata=EventMetadata(
            native=NativeMetadata(
                data={
                    "lxmf": {
                        "schema_version": 1,
                        "source_hash": "ab" * 16,
                        "display_name": "Peer",
                        "secret": "PRIVATE_SENTINEL",
                    }
                }
            )
        ),
        source_native_ref=NativeRef(
            adapter="radio-east", native_channel_id=None, native_message_id="original"
        ),
    )


def renderer(**kwargs):
    """Resolve the source platform and origin without requiring a visible relay prefix."""
    return LxmfRenderer(
        source_attribution={
            "radio-east": SimpleNamespace(
                platform="lxmf",
                origin_label="East",
            )
        },
        **kwargs,
    )


def receive(payload):
    # Exercise the same integer-keyed MessagePack fields used by the native worker.
    """Round-trip integer-keyed worker fields into the real receiving codec."""
    fields = msgspec.msgpack.decode(msgspec.msgpack.encode(payload["fields"]))
    return LxmfCodec("receiver", LxmfConfig(adapter_id="receiver")).decode(
        {
            "source_hash": "cd" * 16,
            "destination_hash": "ef" * 16,
            "message_id": "native-received",
            "content": payload["content"],
            "fields": fields,
            "signature_validated": True,
        }
    )


@pytest.mark.parametrize(
    "origin,expected", [(None, "East"), ("", ""), ("Route origin", "Route origin")]
)
async def test_context_survives_worker_fields_and_codec_without_replacing_sender(
    origin, expected
):
    """Preserve source claims and origin precedence while keeping receiver-native identity."""
    result = await renderer().render(
        source_event(),
        RenderingContext(
            target_adapter="lxmf-out",
            delivery_strategy="direct",
            source_origin_label=origin,
        ),
    )
    assert result.payload["content"] == "hello"
    received = receive(result.payload)
    envelope = received.metadata.custom["medre_envelope"]
    context = envelope["bridge_context"]
    assert context["source"]["sender_name"] == "Peer"
    assert context["source"]["origin_label"] == expected
    assert context["source"]["sender_verification"] == "unknown"
    assert context["source"]["canonical_event_id"] == "canonical-source"
    assert context["source"]["native_ref"]["native_message_id"] == "original"
    assert "PRIVATE_SENTINEL" not in json.dumps(envelope)
    assert received.source_transport_id == "cd" * 16
    assert received.source_adapter == "receiver"
    assert received.source_native_ref.native_message_id == "native-received"
    assert not received.relations
    schema = json.loads(
        (
            Path(__file__).parents[1] / "docs/schemas/lxmf-fields-envelope.schema.json"
        ).read_text()
    )
    Draft202012Validator(schema).validate(
        msgspec.json.decode(msgspec.json.encode(envelope))
    )


async def test_remote_context_relations_cannot_create_canonical_relations():
    """Retain remote relation hints as claims without granting them binding authority."""
    result = await renderer().render(
        source_event(),
        RenderingContext(
            target_adapter="lxmf-out",
            delivery_strategy="direct",
        ),
    )
    envelope = result.payload["fields"][FIELD_MEDRE_ENVELOPE]["medre"]
    envelope["bridge_context"]["relations"] = [
        {
            "relation_type": "reply",
            "target_event_id": "remote-target",
            "target_native_ref": None,
            "key": None,
        }
    ]
    received = receive(result.payload)
    assert received.metadata.custom["medre_envelope"]["bridge_context"]["relations"]
    assert not received.relations


@pytest.mark.parametrize("strategy", ["direct", "fallback_text"])
async def test_relation_hints_follow_delivery_strategy(strategy):
    """Carry relation hints for direct delivery and omit them from text-only fallbacks."""
    relation = EventRelation(
        relation_type="reply",
        target_event_id="target",
        target_native_ref=None,
        key=None,
        fallback_text="earlier",
    )
    result = await renderer().render(
        source_event(relations=(relation,)),
        RenderingContext(
            target_adapter="lxmf-out",
            delivery_strategy=strategy,
        ),
    )
    envelope = LxmfFieldsHelper.extract_envelope(result.payload["fields"])
    assert bool(envelope["relations"]) is (strategy == "direct")
    assert bool(envelope["bridge_context"]["relations"]) is (strategy == "direct")


async def test_disabled_embedding_omits_context():
    """Respect the metadata switch even when source attribution is available."""
    result = await renderer(metadata_embedding=False).render(
        source_event(),
        RenderingContext(
            target_adapter="lxmf-out",
            delivery_strategy="direct",
        ),
    )
    assert result.payload["fields"] == {}


@pytest.mark.parametrize(
    "invalid", [True, {"schema_version": 2}, {"schema_version": 1, "source": []}]
)
async def test_invalid_remote_context_does_not_discard_message(invalid):
    """Keep valid message text when its optional remote relay context is malformed."""
    result = await renderer().render(
        source_event(),
        RenderingContext(
            target_adapter="lxmf-out",
            delivery_strategy="direct",
        ),
    )
    result.payload["fields"][FIELD_MEDRE_ENVELOPE]["medre"]["bridge_context"] = invalid
    received = receive(result.payload)
    assert received.payload["body"] == "hello"
    assert "bridge_context" not in received.metadata.custom["medre_envelope"]


@pytest.mark.parametrize(
    "payload,expected",
    [
        ({"body": "current", "text": "stale", "formatting": {"spans": []}}, "current"),
        ({"body": "plain body", "text": "plain text"}, "plain text"),
    ],
)
async def test_styled_body_is_authoritative_while_text_field_is_selected(
    payload, expected
):
    """Prefer styled canonical body text; unstyled messages select ``text`` first."""
    result = await renderer().render(
        source_event(payload),
        RenderingContext(
            target_adapter="lxmf-out",
            delivery_strategy="direct",
        ),
    )
    assert result.payload["content"] == expected


@pytest.mark.parametrize(
    "payload",
    [
        {"body": "valid body", "text": 42},
        {"body": 42, "text": "unstyled"},
        {"body": None, "text": "unstyled", "formatting": {"version": 1, "spans": []}},
    ],
)
async def test_malformed_canonical_text_never_becomes_native_content(payload):
    """An invalid body or alias must not acquire a native LXMF wire value."""
    with pytest.raises(ValueError, match="must be text"):
        await renderer().render(
            source_event(payload),
            RenderingContext(target_adapter="lxmf-out", delivery_strategy="direct"),
        )


@pytest.mark.parametrize("portnum", [1, 1.0])
async def test_public_source_facts_survive_native_lxmf_field_roundtrip(portnum):
    """Preserve only explicitly approved radio source facts as remote claims."""
    original = source_event()
    from msgspec.structs import replace

    native = msgspec.json.decode(msgspec.json.encode(original.metadata.native.data))
    native["lxmf"]["public_facts"] = {"portnum": portnum, "meshnet_label": "East"}
    original = replace(
        original, metadata=EventMetadata(native=NativeMetadata(data=native))
    )
    rendered = await renderer().render(
        original,
        RenderingContext(target_adapter="lxmf-out", delivery_strategy="direct"),
    )
    received = receive(rendered.payload)
    facts = received.metadata.custom["medre_envelope"]["bridge_context"]["source"][
        "native_facts"
    ]
    assert facts == {
        "namespace": "lxmf",
        "fields": {"portnum": 1, "meshnet_label": "East"},
    }
    assert type(facts["fields"]["portnum"]) is int
    assert "PRIVATE_SENTINEL" not in json.dumps(received.metadata.custom)
    assert received.source_native_ref.native_message_id == "native-received"


@pytest.mark.parametrize(
    "origin,expected", [(None, "Remote"), ("", ""), ("Route", "Route")]
)
async def test_embedding_attribution_reaches_lxmf_peer_context(origin, expected):
    from medre.core.rendering.attribution import RelayAttribution

    rendered = await renderer().render(
        source_event(),
        RenderingContext(
            "direct",
            "lxmf-out",
            source_origin_label=origin,
            source_attribution=RelayAttribution(
                source_platform="meshtastic",
                source_sender_label="Alice",
                source_sender_short_label="AL",
                source_origin_label="Remote",
            ),
        ),
    )
    context = receive(rendered.payload).metadata.custom["medre_envelope"][
        "bridge_context"
    ]
    assert context["source"]["sender_name"] == "Alice"
    assert context["source"]["sender_short_name"] == "AL"
    assert context["source"]["origin_label"] == expected


@pytest.mark.parametrize("context_label", [None, "", "Route"])
@pytest.mark.parametrize("snapshot_label", [None, "", "Snapshot"])
async def test_supplied_attribution_preserves_registry_origin(
    context_label, snapshot_label
):
    """Preserve registry fallback and explicit empty labels over the actual wire."""
    sender = LxmfRenderer(
        source_attribution={
            "radio-east": SimpleNamespace(platform="lxmf", origin_label="Registry")
        }
    )
    rendered = await sender.render(
        source_event(),
        RenderingContext(
            "direct",
            "lxmf-out",
            source_origin_label=context_label,
            source_attribution=RelayAttribution(
                source_sender_label="Alice", source_origin_label=snapshot_label
            ),
        ),
    )
    context = receive(rendered.payload).metadata.custom["medre_envelope"][
        "bridge_context"
    ]
    expected = (
        context_label
        if context_label is not None
        else snapshot_label if snapshot_label is not None else "Registry"
    )
    assert context["source"]["origin_label"] == expected
    assert context["source"]["sender_name"] == "Alice"
