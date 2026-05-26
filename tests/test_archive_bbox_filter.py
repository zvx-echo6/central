"""Archive-level monitoring-area bbox filter (v0.9.12).

Events whose geometry falls entirely outside the system monitoring area are
dropped at archive INSERT time; null-geom events and border-straddlers are kept.
The filter is fail-open: an unparseable geometry is archived (with a warning),
never dropped.
"""
import json

import pytest
from unittest.mock import AsyncMock, MagicMock

from central.archive import ArchiveConsumer, MonitoringArea, _classify_geom

IDAHO = MonitoringArea(north=44.5, south=41.8, east=-111.0, west=-117.5)


def _pt(lon, lat):
    return json.dumps({"type": "Point", "coordinates": [lon, lat]})


class TestClassifyGeom:
    def test_null_geom_always_kept(self):
        assert _classify_geom(None, IDAHO) == "null-geom"

    def test_no_area_keeps_everything(self):
        assert _classify_geom(_pt(-114.0, 43.5), None) == "no-area"

    def test_in_bounds_kept(self):
        assert _classify_geom(_pt(-114.0, 43.5), IDAHO) == "in-bounds"

    def test_out_of_bounds_dropped(self):
        assert _classify_geom(_pt(-74.0, 40.7), IDAHO) == "out-of-bounds"

    def test_border_straddling_polygon_kept(self):
        # Spans the western edge (west=-117.5): partly out, partly in -> kept.
        poly = json.dumps({
            "type": "Polygon",
            "coordinates": [[[-119, 42], [-116, 42], [-116, 43], [-119, 43], [-119, 42]]],
        })
        assert _classify_geom(poly, IDAHO) == "in-bounds"

    def test_point_exactly_on_border_kept(self):
        assert _classify_geom(_pt(-117.5, 43.0), IDAHO) == "in-bounds"

    def test_unparseable_geom_kept(self):
        assert _classify_geom("{not valid json", IDAHO) == "invalid-geom"

    def test_unknown_geom_type_kept(self):
        assert _classify_geom(json.dumps({"type": "Nonsense"}), IDAHO) == "invalid-geom"


def _make_msg(envelope):
    msg = MagicMock()
    msg.data = json.dumps(envelope).encode()
    msg.ack = AsyncMock()
    return msg


def _envelope(adapter, lon, lat):
    return {
        "id": f"{adapter}:evt1",
        "data": {
            "adapter": adapter,
            "category": "test",
            "time": "2026-05-26T00:00:00Z",
            "geo": {"centroid": [lon, lat]},
        },
    }


class TestProcessMessageFilter:
    @pytest.mark.asyncio
    async def test_out_of_bounds_dropped_and_counted(self):
        c = ArchiveConsumer("nats://x", "postgresql://x")
        c._monitoring_area = IDAHO
        conn = AsyncMock()
        msg = _make_msg(_envelope("wzdx", -74.0, 40.7))
        await c._process_message(msg, conn)
        conn.execute.assert_not_called()
        msg.ack.assert_awaited_once()
        assert c._dropped == {"wzdx": 1}

    @pytest.mark.asyncio
    async def test_in_bounds_inserted(self):
        c = ArchiveConsumer("nats://x", "postgresql://x")
        c._monitoring_area = IDAHO
        conn = AsyncMock()
        msg = _make_msg(_envelope("nws", -114.0, 43.5))
        await c._process_message(msg, conn)
        conn.execute.assert_awaited_once()
        msg.ack.assert_awaited_once()
        assert c._dropped == {}

    @pytest.mark.asyncio
    async def test_null_geom_inserted(self):
        c = ArchiveConsumer("nats://x", "postgresql://x")
        c._monitoring_area = IDAHO
        conn = AsyncMock()
        env = {"id": "swpc:1", "data": {
            "adapter": "swpc_alerts", "category": "space",
            "time": "2026-05-26T00:00:00Z"}}
        await c._process_message(_make_msg(env), conn)
        conn.execute.assert_awaited_once()
        assert c._dropped == {}

    @pytest.mark.asyncio
    async def test_no_area_keeps_out_of_bounds(self):
        c = ArchiveConsumer("nats://x", "postgresql://x")
        c._monitoring_area = None
        conn = AsyncMock()
        msg = _make_msg(_envelope("wzdx", -74.0, 40.7))
        await c._process_message(msg, conn)
        conn.execute.assert_awaited_once()
        assert c._dropped == {}
