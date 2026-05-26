"""Tests for the tomtom_incidents adapter (v0.9.5).

Fixture is a real Orbis incidentDetails capture (2 incidents, varied
magnitudeOfDelay) from the Treasure Valley bbox:
  tests/fixtures/tomtom_incidents_sample.json

No conftest entry: dedup uses the supervisor-injected cursors.db (inherited
mixin); polling is stateless.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from central.adapter import SourceAdapter
from central.adapters.tomtom_incidents import (
    BBox,
    TomTomIncidentsAdapter,
    _first_vertex,
    _MAGNITUDE_SEVERITY,
)
from central.config_models import AdapterConfig

INC = json.loads((Path(__file__).parent / "fixtures" / "tomtom_incidents_sample.json").read_text())["incidents"]
BB = BBox(name="treasure_valley", min_lon=-116.85, min_lat=43.30, max_lon=-115.65, max_lat=44.10, state_code="ID")


def _cfg():
    return AdapterConfig(
        name="tomtom_incidents", enabled=True, cadence_s=1800,
        settings={"api_key_alias": "tomtom", "bboxes": [BB.model_dump()]},
        updated_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def adapter(tmp_path):
    return TomTomIncidentsAdapter(_cfg(), MagicMock(), tmp_path / "cursors.db")


@pytest.mark.parametrize("mag,sev", [(0, 1), (1, 1), (2, 2), (3, 3), (4, 4), (None, 1), (99, 1)])
def test_severity_mapping(mag, sev):
    assert _MAGNITUDE_SEVERITY.get(mag, 1) == sev


def test_dedup_key(adapter):
    e = adapter._build_event(INC[0], BB)
    assert e.id == f"ID:tomtom:{INC[0]['properties']['id']}"


def test_build_event_linestring(adapter):
    e = adapter._build_event(INC[0], BB)  # mag-0 Roadworks LineString
    assert e.category == "incident.tomtom_incidents"
    assert e.severity == 1
    assert e.data["description"] == "Roadworks"
    assert e.data["from"] == "Early Road" and e.data["to"] == "Slade Road"
    assert e.data["state_code"] == "ID"
    assert e.data["latitude"] is not None and e.data["longitude"] is not None


def test_build_event_closure_severity(adapter):
    e = adapter._build_event(INC[1], BB)  # mag-4 Closed
    assert e.data["magnitude_of_delay"] == 4
    assert e.severity == 4


def test_geo_geometry_for_linestring(adapter):
    # v0.9.3 framework: the affected-road LineString rides on geo.geometry.
    e = adapter._build_event(INC[0], BB)
    assert e.geo.geometry["type"] == "LineString"
    assert e.geo.geometry["coordinates"] == INC[0]["geometry"]["coordinates"]


def test_build_event_point():
    a = TomTomIncidentsAdapter(_cfg(), MagicMock(), Path("/tmp/unused.db"))
    inc = {"geometry": {"type": "Point", "coordinates": [-116.2, 43.6]},
           "properties": {"id": "TTI-x", "magnitudeOfDelay": 2,
                          "events": [{"description": "Accident", "code": 1}]}}
    e = a._build_event(inc, BB)
    assert e.geo.geometry["type"] == "Point"
    assert e.severity == 2
    assert e.data["latitude"] == 43.6 and e.data["longitude"] == -116.2


def test_first_vertex():
    assert _first_vertex({"type": "Point", "coordinates": [-116.2, 43.6]}) == (43.6, -116.2)
    assert _first_vertex({"type": "LineString", "coordinates": [[-116.2, 43.6], [-116.1, 43.7]]}) == (43.6, -116.2)
    assert _first_vertex(None) == (None, None)
    assert _first_vertex({"type": "Polygon", "coordinates": []}) == (None, None)


def test_subject_for_idaho(adapter):
    e = adapter._build_event(INC[0], BB)
    assert adapter.subject_for(e) == "central.traffic.incident.id"


def test_subject_unknown(adapter):
    e = adapter._build_event(INC[0], BBox(name="x", min_lon=0, min_lat=0, max_lon=0.5, max_lat=0.5, state_code=""))
    assert adapter.subject_for(e) == "central.traffic.incident.unknown"


@pytest.mark.asyncio
async def test_poll_yields_events(tmp_path):
    cs = MagicMock()
    cs.get_api_key = AsyncMock(return_value="testkey")
    adapter = TomTomIncidentsAdapter(_cfg(), cs, tmp_path / "cursors.db")
    await adapter.startup()
    adapter._fetch_bbox = AsyncMock(return_value=INC)  # bypass retry + network
    events = [e async for e in adapter.poll()]
    await adapter.shutdown()
    assert len(events) == 2
    assert all(e.adapter == "tomtom_incidents" for e in events)
    assert all(e.category == "incident.tomtom_incidents" for e in events)


@pytest.mark.asyncio
async def test_poll_skips_without_key(tmp_path):
    cs = MagicMock()
    cs.get_api_key = AsyncMock(return_value=None)
    adapter = TomTomIncidentsAdapter(_cfg(), cs, tmp_path / "cursors.db")
    await adapter.startup()
    events = [e async for e in adapter.poll()]
    await adapter.shutdown()
    assert events == []


def test_summary_partial_renders():
    from central.gui.routes import _derive_subject
    inner = {"description": "Roadworks", "from": "Early Road", "to": "Slade Road"}
    row = {"adapter": "tomtom_incidents", "data": {"data": {"data": inner}}}
    assert _derive_subject(row) == "Roadworks on Early Road → Slade Road"


def test_inherits_dedup_mixin():
    for m in ("is_published", "mark_published", "sweep_old_ids"):
        assert m not in TomTomIncidentsAdapter.__dict__, f"redefines {m}"
        assert getattr(TomTomIncidentsAdapter, m) is getattr(SourceAdapter, m)


# --- v0.9.5.1 per-bbox cadence -----------------------------------------------

def _b(name, cadence_s=None):
    return BBox(name=name, min_lon=0, min_lat=0, max_lon=0.5, max_lat=0.5,
                state_code="ID", cadence_s=cadence_s)


def _cadence_adapter(tmp_path, bboxes):
    cfg = AdapterConfig(
        name="tomtom_incidents", enabled=True, cadence_s=1800,
        settings={"api_key_alias": "tomtom", "bboxes": [b.model_dump() for b in bboxes]},
        updated_at=datetime.now(timezone.utc),
    )
    cs = MagicMock()
    cs.get_api_key = AsyncMock(return_value="testkey")
    return TomTomIncidentsAdapter(cfg, cs, tmp_path / "cursors.db")


def test_bbox_cadence_field_defaults_none():
    assert _b("x").cadence_s is None
    assert _b("y", 3600).cadence_s == 3600


def test_bbox_due_first_poll(adapter):
    assert adapter._bbox_due(_b("never", 3600), datetime.now(timezone.utc)) is True


def test_bbox_due_respects_per_bbox_cadence(adapter):
    now = datetime.now(timezone.utc)
    b60 = _b("b60", 3600)
    adapter._last_polled["b60"] = now - timedelta(minutes=31)
    assert adapter._bbox_due(b60, now) is False          # 31 < 60
    adapter._last_polled["b60"] = now - timedelta(minutes=60)
    assert adapter._bbox_due(b60, now) is True           # 60 >= 60 boundary


def test_bbox_due_fallback_to_default(adapter):
    now = datetime.now(timezone.utc)
    bd = _b("bd")  # cadence_s None -> default_cadence_s == 1800 (30 min)
    adapter._last_polled["bd"] = now - timedelta(minutes=29)
    assert adapter._bbox_due(bd, now) is False
    adapter._last_polled["bd"] = now - timedelta(minutes=31)
    assert adapter._bbox_due(bd, now) is True


@pytest.mark.asyncio
async def test_poll_only_fetches_due_bboxes(tmp_path):
    # Matt's 60/90/60: seed all polled 70 min ago -> 60-min due, 90-min not.
    bboxes = [_b("treasure_valley_ext", 3600), _b("mountain_home_corridor", 5400),
              _b("magic_valley_burley", 3600)]
    a = _cadence_adapter(tmp_path, bboxes)
    await a.startup()
    seed = datetime.now(timezone.utc) - timedelta(minutes=70)
    for b in bboxes:
        a._last_polled[b.name] = seed
    fetched = []

    async def fake_fetch(bbox):
        fetched.append(bbox.name)
        return []

    a._fetch_bbox = fake_fetch
    [e async for e in a.poll()]
    await a.shutdown()
    assert set(fetched) == {"treasure_valley_ext", "magic_valley_burley"}  # 60-min due
    assert "mountain_home_corridor" not in fetched                          # 90-min not due
    assert a._last_polled["treasure_valley_ext"] > seed                     # advanced
    assert a._last_polled["mountain_home_corridor"] == seed                 # untouched


@pytest.mark.asyncio
async def test_failed_fetch_does_not_update_last_polled(tmp_path):
    import aiohttp
    a = _cadence_adapter(tmp_path, [_b("bf", 3600)])
    await a.startup()

    async def boom(bbox):
        raise aiohttp.ClientError("upstream down")

    a._fetch_bbox = boom
    [e async for e in a.poll()]
    await a.shutdown()
    assert "bf" not in a._last_polled  # failed fetch -> not recorded -> still due next cycle
