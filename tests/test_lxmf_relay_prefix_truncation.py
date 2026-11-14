"""Truncation suffixes on the unified ``relay_prefix`` template surface.

Custom ``relay_prefix`` templates accept ``{nameN}`` first-N-code-point
truncation for the label variables declared by this transport. LXMF
radio clients are the constrained-display affordance that motivates
truncation; identifier-like variables (``sender_id`` — the fixed-width
LXMF source hash) stay undeclared and their suffixed forms are rejected.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from medre.adapters.lxmf.renderer import LxmfRenderer
from medre.core.events import CanonicalEvent, EventMetadata
from medre.core.events.metadata import NativeMetadata
from medre.core.rendering.renderer import RenderingContext, RenderingResult
from medre_lxmf_tests.helpers.lxmf_renderer import lxmf_renderer_with_prefix


def _make_event(
    display_name: str | None = "Tad Chilly",
    short_name: str | None = None,
    text: str = "Testing from Sideband",
) -> CanonicalEvent:
    """Build a canonical event carrying LXMF native identity metadata."""
    lxmf: dict[str, object] = {
        "schema_version": 1,
        "source_hash": "ab" * 16,
        "destination_hash": None,
        "message_id": None,
        "timestamp": None,
        "title": "",
        "delivery_method": None,
        "has_fields": False,
    }
    if display_name is not None:
        lxmf["display_name"] = display_name
    if short_name is not None:
        lxmf["short_name"] = short_name
    return CanonicalEvent(
        event_id="relay-prefix-truncation",
        event_kind="message.created",
        schema_version=1,
        timestamp=datetime.now(timezone.utc),
        source_adapter="source",
        source_transport_id="ab" * 16,
        source_channel_id=None,
        parent_event_id=None,
        lineage=(),
        relations=(),
        payload={"body": text},
        metadata=EventMetadata(native=NativeMetadata(data={"lxmf": lxmf})),
    )


async def _render(
    relay_prefix: str,
    *,
    display_name: str | None = "Tad Chilly",
    short_name: str | None = None,
    text: str = "Testing from Sideband",
    ctx: RenderingContext | None = None,
) -> RenderingResult:
    """Render an event through an LXMF renderer with the given prefix."""
    renderer: LxmfRenderer = lxmf_renderer_with_prefix(relay_prefix)
    if ctx is None:
        ctx = RenderingContext(target_adapter="lxmf_node", delivery_strategy="direct")
    return await renderer.render(_make_event(display_name, short_name, text), ctx)


# ---------------------------------------------------------------------------
# Declared truncation suffixes
# ---------------------------------------------------------------------------


async def test_custom_template_truncates_sender_by_code_points() -> None:
    result = await _render("{sender3}[LXMF]: ")
    assert result.payload["content"] == "Tad[LXMF]: Testing from Sideband"
    assert result.metadata["relay_prefix_rendered"] == "Tad[LXMF]: "


async def test_truncation_counts_code_points_not_bytes() -> None:
    result = await _render("{sender2}: ", display_name="\U0001f47eIngrid")
    assert result.payload["content"] == "\U0001f47eI: Testing from Sideband"
    assert len("\U0001f47e".encode("utf-8")) == 4


async def test_value_shorter_than_suffix_renders_whole_value() -> None:
    result = await _render("{sender5}[LXMF]: ", display_name="Ana")
    assert result.payload["content"] == "Ana[LXMF]: Testing from Sideband"


async def test_origin_label_truncation_suffix() -> None:
    result = await _render(
        "{origin_label2}: ",
        text="hello",
        ctx=RenderingContext(
            target_adapter="lxmf_node",
            delivery_strategy="direct",
            source_origin_label="East Relay",
        ),
    )
    assert result.payload["content"] == "Ea: hello"


async def test_sender_short_truncation_suffix() -> None:
    result = await _render("{sender_short4}: ", short_name="tadchilly")
    assert result.payload["content"] == "tadc: Testing from Sideband"


async def test_sender_short_falls_back_to_compact_display_name() -> None:
    result = await _render("{sender_short4}: ")
    assert result.payload["content"] == "TadC: Testing from Sideband"


# ---------------------------------------------------------------------------
# Undeclared or invalid suffixes are rejected
# ---------------------------------------------------------------------------


async def test_zero_suffix_is_not_a_truncation_variable() -> None:
    with pytest.raises(ValueError, match="Invalid relay_prefix"):
        await _render("{sender0}: ")


async def test_undeclared_stem_with_suffix_rejected() -> None:
    with pytest.raises(ValueError, match="Invalid relay_prefix"):
        await _render("{sender_id3}: ")


# ---------------------------------------------------------------------------
# Behavior without numeric suffixes is unchanged
# ---------------------------------------------------------------------------


async def test_plain_variables_are_unaffected_by_declaration() -> None:
    result = await _render("[{sender}]({sender_id}) ")
    source_hash = "ab" * 16
    assert (
        result.payload["content"]
        == f"[Tad Chilly]({source_hash}) Testing from Sideband"
    )


async def test_empty_prefix_still_disables_the_prefix() -> None:
    result = await _render("")
    assert result.payload["content"] == "Testing from Sideband"
    assert "relay_prefix_rendered" not in result.metadata
