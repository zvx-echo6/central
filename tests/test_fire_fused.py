"""Fused fire view (v0.9.14): WFIGS perimeter + nearby FIRMS hotspots.

The spatial join itself (ST_DWithin / time window) is PostGIS and is verified on
real data, not here -- the suite has no PostGIS test DB (mock_conn only). These
tests cover the Python response-shaping, the bbox parse, the R/T constants, and
that the query is parameterized with R, T, and the optional bbox.
"""
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from central.gui.routes import (
    FIRE_FUSE_RADIUS_M,
    FIRE_FUSE_WINDOW_H,
    _fused_bbox,
    _shape_fused_fire,
    _shape_unconfirmed_hotspot,
    fire_fused,
)

T = datetime(2026, 5, 27, 0, 0, tzinfo=timezone.utc)


def test_constants():
    assert FIRE_FUSE_RADIUS_M == 1000
    assert FIRE_FUSE_WINDOW_H == 72


class TestFusedBbox:
    def test_valid_returns_w_s_e_n(self):
        assert _fused_bbox({"north": "44.5", "south": "41.8",
                            "east": "-111.0", "west": "-117.5"}) == (-117.5, 41.8, -111.0, 44.5)

    def test_missing_returns_none(self):
        assert _fused_bbox({"north": "44.5"}) is None
        assert _fused_bbox({}) is None

    def test_out_of_range_returns_none(self):
        # east<west (degenerate) and lat>90
        assert _fused_bbox({"north": "44", "south": "42", "east": "-118", "west": "-111"}) is None
        assert _fused_bbox({"north": "99", "south": "42", "east": "-111", "west": "-118"}) is None

    def test_nonnumeric_returns_none(self):
        assert _fused_bbox({"north": "x", "south": "1", "east": "2", "west": "0"}) is None


def test_shape_fused_fire():
    row = {
        "id": "perim1", "time": T, "incident_name": "Summit Creek",
        "irwin_id": "{ABC}", "acres": 1234.8, "cause": "Natural",
        "geometry": {"type": "Polygon", "coordinates": []},
        "hotspot_count": 90, "max_frp": 12.5,
        "hotspots": [{"geometry": {"type": "Polygon"}, "frp": "1.2",
                      "confidence": "nominal", "satellite": "N", "time": "2026-05-26T..."}],
    }
    out = _shape_fused_fire(row)
    assert out["incident_name"] == "Summit Creek"
    assert out["hotspot_count"] == 90
    assert out["max_frp"] == 12.5
    assert out["geometry"]["type"] == "Polygon"
    assert len(out["hotspots"]) == 1
    assert out["time"] == T.isoformat()


def test_shape_fused_fire_null_hotspots():
    row = {"id": "p", "time": T, "incident_name": None, "irwin_id": None,
           "acres": None, "cause": None, "geometry": None,
           "hotspot_count": 0, "max_frp": None, "hotspots": None}
    out = _shape_fused_fire(row)
    assert out["hotspots"] == []
    assert out["hotspot_count"] == 0


def test_shape_unconfirmed_hotspot():
    row = {"id": "h1", "time": T, "geometry": {"type": "Polygon"},
           "frp": "0.7", "confidence": "nominal", "satellite": "N"}
    out = _shape_unconfirmed_hotspot(row)
    assert out["id"] == "h1"
    assert out["satellite"] == "N"
    assert out["geometry"]["type"] == "Polygon"


def _mock_pool(conn):
    pool = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
    return pool


def _confirmed_row():
    return {"id": "p1", "time": T, "incident_name": "Summit Creek", "irwin_id": "{X}",
            "acres": 1234.8, "cause": "Natural", "geometry": {"type": "Polygon"},
            "hotspot_count": 2, "max_frp": 9.9,
            "hotspots": [{"geometry": {"type": "Polygon"}, "frp": "1", "confidence": "n",
                          "satellite": "N", "time": "2026-05-26T00:00:00+00:00"}]}


def _unconfirmed_row():
    return {"id": "h9", "time": T, "geometry": {"type": "Polygon"},
            "frp": "2.0", "confidence": "high", "satellite": "1"}


@pytest.mark.asyncio
async def test_endpoint_returns_fires_and_unconfirmed():
    req = MagicMock()
    req.state.operator = MagicMock(id=1, username="admin")
    req.query_params = {}
    conn = AsyncMock()
    conn.fetch.side_effect = [[_confirmed_row()], [_unconfirmed_row(), _unconfirmed_row()]]
    with patch("central.gui.routes.get_pool", return_value=_mock_pool(conn)):
        resp = await fire_fused(req)
    assert resp.status_code == 200
    body = json.loads(resp.body)
    assert len(body["fires"]) == 1
    assert body["fires"][0]["incident_name"] == "Summit Creek"
    assert len(body["unconfirmed"]) == 2


@pytest.mark.asyncio
async def test_endpoint_binds_R_T_and_no_bbox_params():
    req = MagicMock()
    req.query_params = {}
    conn = AsyncMock()
    conn.fetch.side_effect = [[], []]
    with patch("central.gui.routes.get_pool", return_value=_mock_pool(conn)):
        await fire_fused(req)
    # Both queries bound with exactly (R, T) and no bbox.
    first = conn.fetch.call_args_list[0].args
    assert first[1] == FIRE_FUSE_RADIUS_M and first[2] == FIRE_FUSE_WINDOW_H
    assert len(first) == 3  # sql, R, T


@pytest.mark.asyncio
async def test_endpoint_appends_bbox_params_when_valid():
    req = MagicMock()
    req.query_params = {"north": "44.5", "south": "41.8", "east": "-111.0", "west": "-117.5"}
    conn = AsyncMock()
    conn.fetch.side_effect = [[], []]
    with patch("central.gui.routes.get_pool", return_value=_mock_pool(conn)):
        await fire_fused(req)
    first = conn.fetch.call_args_list[0].args
    assert first[1:] == (FIRE_FUSE_RADIUS_M, FIRE_FUSE_WINDOW_H, -117.5, 41.8, -111.0, 44.5)
    assert "ST_MakeEnvelope" in first[0]
