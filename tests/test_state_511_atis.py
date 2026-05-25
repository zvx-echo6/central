"""Tests for the state_511_atis adapter (Castle Rock ATIS, Idaho).

Fixtures are real captures (one record + its matching marker per layer):
  state_511_atis_<layer>.json         -- POST /List/GetData/<Layer> .data[0:1]
  state_511_atis_markers_<layer>.json -- GET  /map/mapIcons/<Layer> matching item2

No tests/conftest isolation entry is added: dedup uses the supervisor-injected
cursors.db (inherited mixin) and discovery is stateless -- no adapter-owned cache.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from central.adapter import SourceAdapter
from central.adapters.state_511_atis import (
    LAYER_EVENT_TYPE,
    State511ATISAdapter,
    _parse_us_dt,
)
from central.config_models import AdapterConfig

FIX = Path(__file__).parent / "fixtures"
DETAIL = {lyr: json.loads((FIX / f"state_511_atis_{lyr.lower()}.json").read_text())["data"][0]
          for lyr in ("Incidents", "Closures", "Construction")}
MARK = {lyr: json.loads((FIX / f"state_511_atis_markers_{lyr.lower()}.json").read_text())["item2"][0]
        for lyr in ("Incidents", "Closures", "Construction")}


def _coords(layer):
    loc = MARK[layer]["location"]
    return (loc[0], loc[1])


def _cfg():
    return AdapterConfig(
        name="state_511_atis", enabled=True, cadence_s=300,
        settings={"states": [{"code": "ID", "base_url": "https://511.idaho.gov"}]},
        updated_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def adapter(tmp_path):
    return State511ATISAdapter(_cfg(), MagicMock(), tmp_path / "cursors.db")


def test_layer_event_type_map():
    assert LAYER_EVENT_TYPE == {"Incidents": "incident", "Closures": "closure", "Construction": "work_zone"}


def test_parse_us_dt():
    assert _parse_us_dt("5/25/26, 2:32 PM") == datetime(2026, 5, 25, 14, 32, tzinfo=timezone.utc)
    assert _parse_us_dt("") is None
    assert _parse_us_dt("not a date") is None


def test_dedup_key(adapter):
    e = adapter._build_event(DETAIL["Incidents"], _coords("Incidents"), "ID", "Incidents")
    assert e.id == "ID:Incidents:33579"


def test_build_incident(adapter):
    e = adapter._build_event(DETAIL["Incidents"], _coords("Incidents"), "ID", "Incidents")
    assert e.category == "incident.state_511_atis"
    assert e.severity == 1
    assert e.data["roadway_name"] == "US-95"
    assert e.data["county"] == "Bonner"
    assert e.data["latitude"] is not None and e.data["longitude"] is not None


def test_build_closure_full_closure_severity(adapter):
    e = adapter._build_event(DETAIL["Closures"], _coords("Closures"), "ID", "Closures")
    assert e.category == "closure.state_511_atis"
    assert e.data["is_full_closure"] is True
    assert e.severity == 3  # isFullClosure -> 3


def test_build_construction_maps_to_work_zone(adapter):
    e = adapter._build_event(DETAIL["Construction"], _coords("Construction"), "ID", "Construction")
    assert e.category == "work_zone.state_511_atis"   # layer Construction, type "Roadwork"
    assert e.severity == 1
    assert e.data["roadway_name"] == "SH-81"


def test_join_missing_coords(adapter):
    e = adapter._build_event(DETAIL["Incidents"], None, "ID", "Incidents")
    assert e.data["latitude"] is None and e.data["longitude"] is None
    assert e.geo.centroid is None  # still built, just no map point


@pytest.mark.parametrize("layer,et", [("Incidents", "incident"), ("Closures", "closure"), ("Construction", "work_zone")])
def test_subject_for(adapter, layer, et):
    e = adapter._build_event(DETAIL[layer], _coords(layer), "ID", layer)
    assert adapter.subject_for(e) == f"central.traffic.{et}.id"


def test_summary_partial_renders():
    from central.gui.routes import _derive_subject
    inner = {"layer": "Incidents", "roadway_name": "US-95", "location_description": "Ponderosa Mobile Home Park"}
    row = {"adapter": "state_511_atis", "data": {"data": {"data": inner}}}
    assert _derive_subject(row) == "Incident on US-95 — Ponderosa Mobile Home Park"


@pytest.mark.asyncio
async def test_poll_joins_and_yields(adapter):
    await adapter.startup()

    async def fake_markers(base_url, layer):
        m = MARK[layer]
        return {str(m["itemId"]): (m["location"][0], m["location"][1])}

    async def fake_details(base_url, layer):
        return [DETAIL[layer]]

    adapter._fetch_markers = fake_markers
    adapter._fetch_details = fake_details
    events = [e async for e in adapter.poll()]
    await adapter.shutdown()
    assert len(events) == 3  # one ID state x three layers
    assert {e.category for e in events} == {
        "incident.state_511_atis", "closure.state_511_atis", "work_zone.state_511_atis",
    }
    assert all(e.adapter == "state_511_atis" for e in events)


def test_inherits_dedup_mixin():
    for m in ("is_published", "mark_published", "sweep_old_ids"):
        assert m not in State511ATISAdapter.__dict__, f"redefines {m}"
        assert getattr(State511ATISAdapter, m) is getattr(SourceAdapter, m)
