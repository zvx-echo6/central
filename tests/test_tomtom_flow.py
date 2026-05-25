"""Tests for the tomtom_flow adapter + shared decode module (v0.9.3).

Fixture is a real Orbis vector flow tile (Boise z10/181/374):
  tests/fixtures/tomtom_flow_orbis.pbf
  -- curl 'https://api.tomtom.com/maps/orbis/traffic/tile/flow/10/181/374.pbf?key=…&apiVersion=1'

No tests/conftest isolation entry: dedup uses the supervisor-injected cursors.db
(inherited mixin); polling is stateless.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from central.adapter import SourceAdapter
from central.adapters.tomtom_flow import TomTomFlowAdapter
from central.archive import _build_geom_sql
from central.config_models import AdapterConfig
from central.tomtom_flow_parse import (
    _local_to_lonlat,
    decode_flow_tile,
    severity_from_relative_speed,
)

FIX = Path(__file__).parent / "fixtures" / "tomtom_flow_orbis.pbf"
PBF = FIX.read_bytes()
AT = datetime(2026, 5, 25, 22, 3, tzinfo=timezone.utc)


def _cfg(tiles=None):
    return AdapterConfig(
        name="tomtom_flow", enabled=True, cadence_s=300,
        settings={"api_key_alias": "tomtom", "tiles": tiles or [{"z": 10, "x": 181, "y": 374}]},
        updated_at=datetime.now(timezone.utc),
    )


@pytest.mark.parametrize("rs,sev", [
    (1.0, 1), (0.75, 1), (0.74, 2), (0.5, 2), (0.49, 3), (0.25, 3), (0.24, 4), (0.0, 4), (None, 1),
])
def test_severity_from_relative_speed(rs, sev):
    assert severity_from_relative_speed(rs) == sev


def test_local_to_lonlat_boise_tile():
    # tile z10/181/374; local (0,0)=bottom-left -> SW corner ~ Boise area
    lon, lat = _local_to_lonlat(2048, 2048, 10, 181, 374, 4096)  # tile center
    assert -116.4 < lon < -116.0 and 43.4 < lat < 43.8


def test_decode_flow_tile_real_fixture():
    evs = decode_flow_tile(PBF, 10, 181, 374, AT)
    assert len(evs) > 50
    e = evs[0]
    assert e.category == "flow.tomtom_flow"
    assert e.adapter == "tomtom_flow"
    assert e.geo.geometry["type"] in ("LineString", "MultiLineString")
    assert e.data["road_category"] in ("motorway", "trunk", "primary", "secondary")
    assert e.data["tile_z"] == 10 and e.data["tile_x"] == 181 and e.data["tile_y"] == 374
    # vertices georeferenced near Boise
    v = e.geo.geometry["coordinates"]
    while isinstance(v[0], list):
        v = v[0]
    assert -117 < v[0] < -116 and 43 < v[1] < 44


def test_dedup_key_minute_bucketed():
    evs = decode_flow_tile(PBF, 10, 181, 374, AT)
    assert evs[0].id == "10/181/374:0:2026-05-25T22:03"
    later = decode_flow_tile(PBF, 10, 181, 374, datetime(2026, 5, 25, 22, 4, tzinfo=timezone.utc))
    assert later[0].id != evs[0].id  # different minute -> different id


def test_subject_for():
    adapter = TomTomFlowAdapter(_cfg(), MagicMock(), Path("/tmp/unused.db"))
    e = decode_flow_tile(PBF, 10, 181, 374, AT)[0]
    assert adapter.subject_for(e) == "central.traffic_flow.10.181.374"


def test_archive_prefers_geo_geometry():
    line = {"type": "LineString", "coordinates": [[-116.2, 43.6], [-116.1, 43.7]]}
    # geometry present -> returned verbatim (not bbox/centroid)
    out = _build_geom_sql({"geometry": line, "centroid": [-116.2, 43.6], "bbox": [-116.3, 43.5, -116.0, 43.8]})
    assert json.loads(out) == line
    # no geometry -> falls back to centroid Point (regression guard)
    out2 = _build_geom_sql({"centroid": [-116.2, 43.6]})
    assert json.loads(out2)["type"] == "Point"


@pytest.mark.asyncio
async def test_poll_yields_segments(tmp_path):
    cs = MagicMock()
    cs.get_api_key = AsyncMock(return_value="testkey")
    adapter = TomTomFlowAdapter(_cfg(), cs, tmp_path / "cursors.db")
    await adapter.startup()
    adapter._fetch_tile = AsyncMock(return_value=PBF)  # bypass retry + network
    events = [e async for e in adapter.poll()]
    await adapter.shutdown()
    assert len(events) > 50
    assert all(e.adapter == "tomtom_flow" for e in events)
    assert all(e.category == "flow.tomtom_flow" for e in events)


@pytest.mark.asyncio
async def test_poll_skips_without_key(tmp_path):
    cs = MagicMock()
    cs.get_api_key = AsyncMock(return_value=None)
    adapter = TomTomFlowAdapter(_cfg(), cs, tmp_path / "cursors.db")
    await adapter.startup()
    events = [e async for e in adapter.poll()]
    await adapter.shutdown()
    assert events == []  # no key -> no fetch, clean skip


def test_summary_partial_renders():
    from central.gui.routes import _derive_subject
    inner = {"road_category": "primary", "relative_speed": 0.11}
    row = {"adapter": "tomtom_flow", "data": {"data": {"data": inner}}}
    assert _derive_subject(row) == "Traffic flow (primary) — 11% of free-flow"


def test_inherits_dedup_mixin():
    for m in ("is_published", "mark_published", "sweep_old_ids"):
        assert m not in TomTomFlowAdapter.__dict__, f"redefines {m}"
        assert getattr(TomTomFlowAdapter, m) is getattr(SourceAdapter, m)
