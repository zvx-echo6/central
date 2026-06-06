"""Archive-level monitoring-area bbox filter integration (v0.9.12).

After v0.10.2 the ``MonitoringArea`` / ``classify_geom`` / ``build_geom_json``
primitives moved to ``central.monitoring_area`` -- their pure-unit tests live in
``test_monitoring_area.py``. This file keeps the end-to-end check that archive's
``_process_message`` still drops out-of-bounds events (ACK + counter, no INSERT)
and that null-geom / no-area paths still archive.
"""
import json

import pytest
from unittest.mock import AsyncMock, MagicMock

from central.archive import ArchiveConsumer
from central.monitoring_area import MonitoringArea

IDAHO = MonitoringArea(north=44.5, south=41.8, east=-111.0, west=-117.5)


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
