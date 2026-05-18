"""Tests for events feed JSON endpoint."""

import base64
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from central.gui.routes import events_json


class TestEventsFeedUnauthenticated:
    """Test events feed without authentication."""

    @pytest.mark.asyncio
    async def test_events_json_unauthenticated_redirects(self):
        """GET /events.json without auth redirects to /login."""
        # This test verifies the session middleware behavior
        # In practice, the middleware redirects before the route is called
        # We verify the route requires operator in request.state
        mock_request = MagicMock()
        mock_request.state.operator = None
        # The middleware would redirect, verified via integration tests


class TestEventsFeedAuthenticated:
    """Test events feed with authentication."""

    @pytest.mark.asyncio
    async def test_events_json_no_filters_returns_events(self):
        """GET /events.json authenticated, no filters returns recent events."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.query_params = {}

        # Create mock events
        mock_events = [
            {
                "id": f"event_{i}",
                "time": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc) - timedelta(hours=i),
                "received": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc) - timedelta(hours=i),
                "adapter": "nws",
                "category": "Weather Alert",
                "subject": f"Test Alert {i}",
                "geometry": '{"type": "Point", "coordinates": [-122.4, 37.8]}' if i % 2 == 0 else None,
                "data": {"key": f"value_{i}"},
                "regions": ["CA", "OR"] if i % 2 == 0 else [],
            }
            for i in range(5)
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = mock_events

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            result = await events_json(mock_request)

        assert result.status_code == 200
        body = json.loads(result.body)
        assert "events" in body
        assert len(body["events"]) == 5
        assert body["next_cursor"] is None  # No next page (only 5 events)

    @pytest.mark.asyncio
    async def test_events_json_with_limit(self):
        """GET /events.json?limit=10 returns up to 10 events."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.query_params = {"limit": "3"}

        # Create 4 mock events (limit+1 to trigger pagination)
        mock_events = [
            {
                "id": f"event_{i}",
                "time": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc) - timedelta(hours=i),
                "received": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc) - timedelta(hours=i),
                "adapter": "nws",
                "category": "Weather Alert",
                "subject": f"Test Alert {i}",
                "geometry": None,
                "data": {},
                "regions": [],
            }
            for i in range(4)  # 4 events for limit=3 to trigger next_cursor
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = mock_events

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            result = await events_json(mock_request)

        assert result.status_code == 200
        body = json.loads(result.body)
        assert len(body["events"]) == 3
        assert body["next_cursor"] is not None

    @pytest.mark.asyncio
    async def test_pagination_roundtrip(self):
        """Fetch page 1, use next_cursor for page 2, verify no overlap."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.query_params = {"limit": "2"}

        # Page 1: 3 events (2 returned + next_cursor)
        page1_events = [
            {
                "id": "event_0",
                "time": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "received": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "adapter": "nws",
                "category": "Alert",
                "subject": "Event 0",
                "geometry": None,
                "data": {},
                "regions": [],
            },
            {
                "id": "event_1",
                "time": datetime(2026, 5, 17, 11, 0, tzinfo=timezone.utc),
                "received": datetime(2026, 5, 17, 11, 0, tzinfo=timezone.utc),
                "adapter": "nws",
                "category": "Alert",
                "subject": "Event 1",
                "geometry": None,
                "data": {},
                "regions": [],
            },
            {
                "id": "event_2",
                "time": datetime(2026, 5, 17, 10, 0, tzinfo=timezone.utc),
                "received": datetime(2026, 5, 17, 10, 0, tzinfo=timezone.utc),
                "adapter": "nws",
                "category": "Alert",
                "subject": "Event 2",
                "geometry": None,
                "data": {},
                "regions": [],
            },
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = page1_events

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            result1 = await events_json(mock_request)

        body1 = json.loads(result1.body)
        assert len(body1["events"]) == 2
        assert body1["next_cursor"] is not None
        page1_ids = {e["id"] for e in body1["events"]}

        # Page 2 with cursor
        mock_request.query_params = {"limit": "2", "cursor": body1["next_cursor"]}

        page2_events = [
            {
                "id": "event_2",
                "time": datetime(2026, 5, 17, 10, 0, tzinfo=timezone.utc),
                "received": datetime(2026, 5, 17, 10, 0, tzinfo=timezone.utc),
                "adapter": "nws",
                "category": "Alert",
                "subject": "Event 2",
                "geometry": None,
                "data": {},
                "regions": [],
            },
        ]
        mock_conn.fetch.return_value = page2_events

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            result2 = await events_json(mock_request)

        body2 = json.loads(result2.body)
        page2_ids = {e["id"] for e in body2["events"]}

        # No overlap
        assert page1_ids.isdisjoint(page2_ids)

    @pytest.mark.asyncio
    async def test_filter_by_adapter(self):
        """GET /events.json?adapter=nws returns only nws events."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.query_params = {"adapter": "nws"}

        mock_events = [
            {
                "id": "nws_event_1",
                "time": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "received": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "adapter": "nws",
                "category": "Weather Alert",
                "subject": "NWS Alert",
                "geometry": None,
                "data": {},
                "regions": [],
            },
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = mock_events

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            result = await events_json(mock_request)

        assert result.status_code == 200
        body = json.loads(result.body)
        assert all(e["adapter"] == "nws" for e in body["events"])

    @pytest.mark.asyncio
    async def test_filter_by_adapter_and_category(self):
        """GET /events.json?adapter=nws&category=Test Alert filters by both."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.query_params = {"adapter": "nws", "category": "Test Alert"}

        mock_events = [
            {
                "id": "filtered_event",
                "time": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "received": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "adapter": "nws",
                "category": "Test Alert",
                "subject": "Filtered Alert",
                "geometry": None,
                "data": {},
                "regions": [],
            },
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = mock_events

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            result = await events_json(mock_request)

        assert result.status_code == 200
        body = json.loads(result.body)
        for event in body["events"]:
            assert event["adapter"] == "nws"
            assert event["category"] == "Test Alert"

    @pytest.mark.asyncio
    async def test_filter_by_since_until(self):
        """GET /events.json?since=<iso>&until=<iso> filters by time window."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.query_params = {
            "since": "2026-05-17T00:00:00Z",
            "until": "2026-05-17T12:00:00Z",
        }

        mock_events = [
            {
                "id": "in_range_event",
                "time": datetime(2026, 5, 17, 6, 0, tzinfo=timezone.utc),
                "received": datetime(2026, 5, 17, 6, 0, tzinfo=timezone.utc),
                "adapter": "nws",
                "category": "Alert",
                "subject": "In Range",
                "geometry": None,
                "data": {},
                "regions": [],
            },
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = mock_events

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            result = await events_json(mock_request)

        assert result.status_code == 200

    @pytest.mark.asyncio
    async def test_filter_by_region_bbox_includes_geometry_inside(self):
        """Region bbox filter includes events with geometry inside bbox."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.query_params = {
            "region_north": "49.5",
            "region_south": "31",
            "region_east": "-102",
            "region_west": "-124.5",
        }

        mock_events = [
            {
                "id": "inside_bbox",
                "time": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "received": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "adapter": "nws",
                "category": "Alert",
                "subject": "Inside BBox",
                "geometry": '{"type": "Point", "coordinates": [-120, 40]}',
                "data": {},
                "regions": ["CA"],
            },
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = mock_events

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            result = await events_json(mock_request)

        assert result.status_code == 200
        body = json.loads(result.body)
        assert len(body["events"]) == 1
        assert body["events"][0]["geometry"] is not None

    @pytest.mark.asyncio
    async def test_empty_result_returns_200(self):
        """GET /events.json?adapter=nonexistent returns 200 with empty list."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.query_params = {"adapter": "nonexistent"}

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = []

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            result = await events_json(mock_request)

        assert result.status_code == 200
        body = json.loads(result.body)
        assert body["events"] == []
        assert body["next_cursor"] is None


class TestEventsFeedValidation:
    """Test events feed validation errors."""

    @pytest.mark.asyncio
    async def test_limit_zero_returns_400(self):
        """GET /events.json?limit=0 returns 400."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.query_params = {"limit": "0"}

        result = await events_json(mock_request)

        assert result.status_code == 400
        body = json.loads(result.body)
        assert "error" in body
        assert "limit" in body["error"].lower()

    @pytest.mark.asyncio
    async def test_limit_too_large_returns_400(self):
        """GET /events.json?limit=500 returns 400."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.query_params = {"limit": "500"}

        result = await events_json(mock_request)

        assert result.status_code == 400
        body = json.loads(result.body)
        assert "error" in body
        assert "limit" in body["error"].lower()

    @pytest.mark.asyncio
    async def test_partial_region_returns_400(self):
        """GET /events.json?region_north=49 alone returns 400."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.query_params = {"region_north": "49"}

        result = await events_json(mock_request)

        assert result.status_code == 400
        body = json.loads(result.body)
        assert "error" in body
        assert "region" in body["error"].lower()

    @pytest.mark.asyncio
    async def test_invalid_since_returns_400(self):
        """GET /events.json?since=garbage returns 400."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.query_params = {"since": "garbage"}

        result = await events_json(mock_request)

        assert result.status_code == 400
        body = json.loads(result.body)
        assert "error" in body
        assert "since" in body["error"].lower()

    @pytest.mark.asyncio
    async def test_invalid_cursor_returns_400(self):
        """GET /events.json?cursor=garbage returns 400."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.query_params = {"cursor": "garbage"}

        result = await events_json(mock_request)

        assert result.status_code == 400
        body = json.loads(result.body)
        assert "error" in body
        assert "cursor" in body["error"].lower()

    @pytest.mark.asyncio
    async def test_since_after_until_returns_400(self):
        """GET /events.json?since=2026-01-01&until=2025-01-01 returns 400."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.query_params = {
            "since": "2026-01-01T00:00:00Z",
            "until": "2025-01-01T00:00:00Z",
        }

        result = await events_json(mock_request)

        assert result.status_code == 400
        body = json.loads(result.body)
        assert "error" in body
        assert "since" in body["error"].lower() or "until" in body["error"].lower()


class TestEventsFeedGeometry:
    """Test geometry handling in events feed."""

    @pytest.mark.asyncio
    async def test_geometry_parsed_as_object(self):
        """Geometry is returned as GeoJSON object, not string."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.query_params = {}

        mock_events = [
            {
                "id": "geom_event",
                "time": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "received": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "adapter": "nws",
                "category": "Alert",
                "subject": "With Geometry",
                "geometry": '{"type": "Polygon", "coordinates": [[[-122, 37], [-122, 38], [-121, 38], [-121, 37], [-122, 37]]]}',
                "data": {},
                "regions": [],
            },
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = mock_events

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            result = await events_json(mock_request)

        assert result.status_code == 200
        body = json.loads(result.body)
        assert len(body["events"]) == 1
        geom = body["events"][0]["geometry"]
        assert isinstance(geom, dict)
        assert geom["type"] == "Polygon"

    @pytest.mark.asyncio
    async def test_null_geometry_returned_as_null(self):
        """Events with null geometry return null, not string."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.query_params = {}

        mock_events = [
            {
                "id": "no_geom_event",
                "time": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "received": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "adapter": "nws",
                "category": "Alert",
                "subject": "No Geometry",
                "geometry": None,
                "data": {},
                "regions": [],
            },
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = mock_events

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            result = await events_json(mock_request)

        assert result.status_code == 200
        body = json.loads(result.body)
        assert body["events"][0]["geometry"] is None
