"""Tests for v0.7.4 telemetry/event separation: SourceAdapter.data_class,
registry split, class-scoped filter options, and the data_class SQL filter.
Registry-derived (no hardcoded adapter lists beyond the nwis pin). No live DB.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from central.adapter import SourceAdapter
from central.adapter_discovery import discover_adapters
from central.gui import routes

# Adapters with data_class="telemetry" (the pinned split; grow as telemetry adapters land).
_TELEMETRY = ["itd_511_cameras", "nwis", "state_511_atis_cameras", "tomtom_flow"]


# --- data_class defaults / registry split -----------------------------------

def test_base_default_is_event():
    assert SourceAdapter.data_class == "event"


def test_registry_split_11_event_1_telemetry():
    reg = discover_adapters()
    by_class = {}
    for name, cls in reg.items():
        by_class.setdefault(getattr(cls, "data_class", "event"), []).append(name)
    assert sorted(by_class.get("telemetry", [])) == _TELEMETRY
    # Everything else is event-class; the split must cover the whole registry.
    assert sorted(by_class.get("event", [])) == sorted(n for n in reg if n not in _TELEMETRY)
    assert len(by_class.get("event", [])) == len(reg) - len(_TELEMETRY)


def test_class_adapter_names():
    assert "nwis" not in routes._class_adapter_names("event")
    assert sorted(routes._class_adapter_names("telemetry")) == _TELEMETRY
    assert "usgs_quake" in routes._class_adapter_names("event")


# --- class-scoped chip-picker / legend options -------------------------------

def test_event_options_exclude_nwis():
    flat, grouped = routes._adapter_filter_options("event")
    names = {a["name"] for a in flat}
    assert "nwis" not in names
    assert len(flat) == len(discover_adapters()) - len(_TELEMETRY)
    grouped_values = {opt["value"] for _, items in grouped for opt in items}
    assert "nwis" not in grouped_values


def test_telemetry_options_only_nwis():
    flat, grouped = routes._adapter_filter_options("telemetry")
    assert sorted(a["name"] for a in flat) == _TELEMETRY
    grouped_values = [opt["value"] for _, items in grouped for opt in items]
    assert sorted(grouped_values) == _TELEMETRY


def test_colors_stable_across_classes():
    """A given adapter keeps the same color on /events and /telemetry (colors
    are keyed to the full registry, not the per-tab subset)."""
    full, _ = routes._adapter_filter_options()
    full_color = {a["name"]: a["color"] for a in full}
    ev, _ = routes._adapter_filter_options("event")
    for a in ev:
        assert a["color"] == full_color[a["name"]]


# --- data_class SQL filter (captured SQL) ------------------------------------

async def _capture(parsed):
    captured = {}

    async def fake_fetch(query, *args):
        captured["query"] = query
        captured["params"] = list(args)
        return []

    conn = MagicMock()
    conn.fetch = fake_fetch
    pool = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
    with patch("central.gui.routes.get_pool", return_value=pool):
        await routes._fetch_events(parsed)
    return captured


@pytest.mark.asyncio
async def test_class_adapters_adds_adapter_any_condition():
    parsed, _ = routes._parse_events_params({"time": "all"}, default_offset=0)
    parsed["class_adapters"] = routes._class_adapter_names("event")
    cap = await _capture(parsed)
    assert "adapter = ANY($" in cap["query"]
    assert routes._class_adapter_names("event") in cap["params"]


@pytest.mark.asyncio
async def test_no_class_adapters_no_class_condition():
    """events.json path: no class_adapters -> no extra adapter filter (all classes)."""
    parsed, _ = routes._parse_events_params({"time": "all"})  # cursor-mode, no class
    assert parsed.get("class_adapters") is None
    cap = await _capture(parsed)
    # The only adapter=ANY would come from a user filter, which we didn't set.
    assert "adapter = ANY($" not in cap["query"]
