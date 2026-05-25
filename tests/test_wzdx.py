"""Tests for the WZDx adapter.

Fixtures are real captures trimmed to representative features:
  wzdx_utah_sample.json -- curl https://udottraffic.utah.gov/wzdx/udot/v40/data
      | jq '{road_event_feed_info, type, features: .features[0:1]}'
      (LineString, vehicle_impact "unknown", has event_status, no lanes)
  wzdx_iowa_sample.json -- curl https://iowa-atms.cloud-q-free.com/api/rest/dataprism/wzdx/wzdxfeed
      | jq '{... , features: [<all-lanes-open feature>, <some-lanes-closed feature>]}'
      (MultiPoint, lanes + types_of_work, no event_status)

No tests/conftest isolation entry is added: WZDx dedup uses the supervisor-
injected cursors.db and registry discovery is stateless, so there is no
adapter-owned cache to redirect (unlike nwis's NWIS_CACHE_DB_PATH).
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from central.adapters.wzdx import (
    _DEFAULT_SEVERITY,
    _VEHICLE_IMPACT_SEVERITY,
    WZDxAdapter,
    _eligible,
    _flatten_geometry,
)
from central.config_models import AdapterConfig

FIX = Path(__file__).parent / "fixtures"
UTAH = json.loads((FIX / "wzdx_utah_sample.json").read_text())
IOWA = json.loads((FIX / "wzdx_iowa_sample.json").read_text())


def _cfg(settings=None):
    return AdapterConfig(
        name="wzdx", enabled=True, cadence_s=600,
        settings=settings or {}, updated_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def adapter(tmp_path):
    return WZDxAdapter(_cfg(), MagicMock(), tmp_path / "cursors.db")


@pytest.mark.parametrize("row,keep", [
    ({"format": "geojson", "active": True, "needapikey": False, "version": "4.1"}, True),
    ({"format": "geojson", "active": True, "needapikey": False, "version": "4"}, True),
    ({"format": "json", "active": True, "needapikey": False, "version": "4.1"}, False),
    ({"format": "geojson", "active": False, "needapikey": False, "version": "4.1"}, False),
    ({"format": "geojson", "active": True, "needapikey": True, "version": "4.1"}, False),
    ({"format": "geojson", "active": True, "needapikey": False, "version": "3.1"}, False),
    ({"format": "geojson", "active": True, "needapikey": False, "version": "CWZ 1.0"}, False),
])
def test_eligible_filter(row, keep):
    assert _eligible(row) is keep


def test_dedup_key(adapter):
    eu = adapter._build_event(UTAH["features"][0], {"feedname": "udot", "state": "utah"})
    ei = adapter._build_event(IOWA["features"][0], {"feedname": "idot", "state": "iowa"})
    assert eu.id == "UDOT-Construction:2365_eastbound"
    assert ei.id == "IowaDOT-WZDx:OpenTMS-Event22920571864-1"


@pytest.mark.parametrize("vi,sev", [
    ("all-lanes-closed", 3), ("some-lanes-closed", 2), ("all-lanes-open", 1),
    ("unknown", 1), (None, 1),
])
def test_severity(vi, sev):
    assert _VEHICLE_IMPACT_SEVERITY.get(vi, _DEFAULT_SEVERITY) == sev


@pytest.mark.parametrize("geom,expect", [
    ({"type": "LineString", "coordinates": [[-111.8, 40.7], [-111.6, 40.6]]}, (40.7, -111.8)),
    ({"type": "MultiPoint", "coordinates": [[-93.5, 40.7]]}, (40.7, -93.5)),
    ({"type": "Point", "coordinates": [-93.5, 40.7]}, (40.7, -93.5)),
    (None, (None, None)),
    ({"type": "Polygon", "coordinates": []}, (None, None)),
])
def test_flatten_geometry(geom, expect):
    assert _flatten_geometry(geom) == expect


def test_build_utah_shape(adapter):
    e = adapter._build_event(UTAH["features"][0], {"feedname": "udot", "state": "utah"})
    assert e.category == "work_zone.wzdx"
    assert e.severity == 1  # vehicle_impact "unknown"
    assert e.data["latitude"] is not None
    assert e.data["event_status"] == "active"  # Utah carries it


def test_build_iowa_shape(adapter):
    e0 = adapter._build_event(IOWA["features"][0], {"feedname": "idot", "state": "iowa"})
    e1 = adapter._build_event(IOWA["features"][1], {"feedname": "idot", "state": "iowa"})
    assert e0.severity == 1  # all-lanes-open
    assert e1.severity == 2  # some-lanes-closed
    assert e0.data["event_status"] is None  # Iowa lacks it -> no raise


def test_subject_from_registry(adapter):
    e = adapter._build_event(UTAH["features"][0], {"feedname": "udot", "state": "utah"})
    assert adapter.subject_for(e) == "central.traffic.work_zone.ut"


def test_subject_unknown(adapter):
    e = adapter._build_event(UTAH["features"][0], {"feedname": "x", "state": "n/a"})
    assert adapter.subject_for(e) == "central.traffic.work_zone.unknown"


def test_subject_geocoder_fallback(adapter):
    e = adapter._build_event(UTAH["features"][0], {"feedname": "x", "state": "n/a"})
    e.data["_enriched"] = {"geocoder": {"state": "Idaho"}}
    assert adapter.subject_for(e) == "central.traffic.work_zone.id"


def test_event_type_split(adapter):
    # Mirrors routes.py split_part(category, '.', 1) -> GUI event_type.
    e = adapter._build_event(UTAH["features"][0], {"feedname": "udot", "state": "utah"})
    assert e.category.split(".")[0] == "work_zone"


@pytest.mark.asyncio
async def test_poll_yields_events(adapter):
    await adapter.startup()
    registry = [
        {"format": "geojson", "active": True, "needapikey": False, "version": "4", "feedname": "udot", "state": "utah", "url": {"url": "u"}},
        {"format": "geojson", "active": True, "needapikey": False, "version": "4", "feedname": "idot", "state": "iowa", "url": {"url": "i"}},
        {"format": "json", "active": True, "needapikey": False, "version": "4", "feedname": "skip", "state": "ohio", "url": {"url": "s"}},
    ]
    adapter._fetch_registry = AsyncMock(return_value=registry)

    async def fake_feed(row):
        return {"udot": UTAH, "idot": IOWA}.get(row["feedname"], {"features": []})["features"]

    adapter._fetch_feed = fake_feed
    events = [e async for e in adapter.poll()]
    await adapter.shutdown()
    # Utah 1 + Iowa 2 = 3; the json feed is dropped by _discover.
    assert len(events) == 3
    assert {e.adapter for e in events} == {"wzdx"}


def test_summary_partial_renders_subject():
    # End-to-end through the real _event_summaries/wzdx.html partial selection.
    from central.gui.routes import _derive_subject
    flat = {"road_names": ["I-80"], "direction": "eastbound"}
    row = {"adapter": "wzdx", "data": {"data": {"data": flat}}}
    assert _derive_subject(row) == "Work zone on I-80 eastbound"
