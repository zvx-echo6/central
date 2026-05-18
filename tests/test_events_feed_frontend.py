"""Tests for events feed frontend routes."""

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from central.gui.routes import events_list, events_rows, events_json


class TestEventsFeedFrontendAuthenticated:
    """Test events feed frontend with authentication."""

    @pytest.mark.asyncio
    async def test_events_no_filters_returns_html(self):
        """GET /events authenticated, no filters returns HTML with events."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.state.csrf_token = "test_csrf_token"
        mock_request.query_params = {}

        mock_events = [
            {
                "id": f"event_{i}",
                "time": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc) - timedelta(hours=i),
                "received": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc) - timedelta(hours=i),
                "adapter": "nws",
                "category": "Weather Alert",
                "subject": f"Test Alert {i}",
                "geometry": '{"type": "Point", "coordinates": [-122.4, 37.8]}' if i % 2 == 0 else None,
                "data": {},
                "regions": [],
            }
            for i in range(5)
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = mock_events
        mock_conn.fetchrow.return_value = {
            "map_tile_url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            "map_attribution": "OpenStreetMap",
        }

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock(status_code=200)

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await events_list(mock_request)

        assert result.status_code == 200
        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert "events" in context
        assert context["filter_error"] is None

    @pytest.mark.asyncio
    async def test_events_adapter_filter(self):
        """GET /events?adapter=nws returns only nws events."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.state.csrf_token = "test_csrf_token"
        mock_request.query_params = {"adapter": "nws"}

        mock_events = [
            {
                "id": "nws_event_1",
                "time": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "received": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "adapter": "nws",
                "category": "Alert",
                "subject": "NWS Alert",
                "geometry": None,
                "data": {},
                "regions": [],
            },
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = mock_events
        mock_conn.fetchrow.return_value = {
            "map_tile_url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            "map_attribution": "OpenStreetMap",
        }

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock(status_code=200)

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await events_list(mock_request)

        assert result.status_code == 200
        context = mock_templates.TemplateResponse.call_args.kwargs.get("context")
        assert context["filter_values"]["adapter"] == "nws"

    @pytest.mark.asyncio
    async def test_events_since_until_filter(self):
        """GET /events?since=...&until=... filters by time window."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.state.csrf_token = "test_csrf_token"
        mock_request.query_params = {
            "since": "2026-05-17T00:00:00",
            "until": "2026-05-17T12:00:00",
        }

        mock_events = [
            {
                "id": "in_range",
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
        mock_conn.fetchrow.return_value = {
            "map_tile_url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            "map_attribution": "OpenStreetMap",
        }

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock(status_code=200)

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await events_list(mock_request)

        assert result.status_code == 200
        # Verify filter was actually parsed and passed to template
        mock_templates.TemplateResponse.assert_called_once()
        call_kwargs = mock_templates.TemplateResponse.call_args.kwargs
        context = call_kwargs.get("context", call_kwargs)
        assert context["filter_values"]["since"] == "2026-05-17T00:00:00"
        assert context["filter_values"]["until"] == "2026-05-17T12:00:00"

    @pytest.mark.asyncio
    async def test_events_region_filter(self):
        """GET /events with full region bbox filters by location."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.state.csrf_token = "test_csrf_token"
        mock_request.query_params = {
            "region_north": "49.5",
            "region_south": "31",
            "region_east": "-102",
            "region_west": "-124.5",
        }

        mock_events = [
            {
                "id": "in_bbox",
                "time": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "received": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "adapter": "nws",
                "category": "Alert",
                "subject": "In BBox",
                "geometry": '{"type": "Point", "coordinates": [-120, 40]}',
                "data": {},
                "regions": [],
            },
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = mock_events
        mock_conn.fetchrow.return_value = {
            "map_tile_url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            "map_attribution": "OpenStreetMap",
        }

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock(status_code=200)

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await events_list(mock_request)

        assert result.status_code == 200
        # Verify region filter was actually parsed and passed to template
        mock_templates.TemplateResponse.assert_called_once()
        call_kwargs = mock_templates.TemplateResponse.call_args.kwargs
        context = call_kwargs.get("context", call_kwargs)
        assert context["filter_values"]["region_north"] == "49.5"
        assert context["filter_values"]["region_south"] == "31"
        assert context["filter_values"]["region_east"] == "-102"
        assert context["filter_values"]["region_west"] == "-124.5"

    @pytest.mark.asyncio
    async def test_events_partial_region_shows_error_banner(self):
        """GET /events with partial region shows error banner, not 400."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.state.csrf_token = "test_csrf_token"
        mock_request.query_params = {"region_north": "49"}

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {
            "map_tile_url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            "map_attribution": "OpenStreetMap",
        }

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock(status_code=200)

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await events_list(mock_request)

        # Should be 200, not 400
        assert result.status_code == 200
        context = mock_templates.TemplateResponse.call_args.kwargs.get("context")
        assert context["filter_error"] is not None
        assert "region" in context["filter_error"].lower()
        # Events should be empty due to validation error
        assert context["events"] == []

    @pytest.mark.asyncio
    async def test_events_with_limit_shows_next_button(self):
        """GET /events?limit=5 shows Next button when more events exist."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.state.csrf_token = "test_csrf_token"
        mock_request.query_params = {"limit": "5"}

        # Return 6 events (limit+1) to trigger pagination
        mock_events = [
            {
                "id": f"event_{i}",
                "time": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc) - timedelta(hours=i),
                "received": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc) - timedelta(hours=i),
                "adapter": "nws",
                "category": "Alert",
                "subject": f"Event {i}",
                "geometry": None,
                "data": {},
                "regions": [],
            }
            for i in range(6)
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = mock_events
        mock_conn.fetchrow.return_value = {
            "map_tile_url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            "map_attribution": "OpenStreetMap",
        }

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock(status_code=200)

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await events_list(mock_request)

        assert result.status_code == 200
        context = mock_templates.TemplateResponse.call_args.kwargs.get("context")
        assert context["next_cursor"] is not None
        assert len(context["events"]) == 5  # Should be trimmed to limit


class TestEventsRowsFragment:
    """Test /events/rows HTMX fragment."""

    @pytest.mark.asyncio
    async def test_events_rows_returns_fragment(self):
        """GET /events/rows returns table fragment, not full page."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.query_params = {"limit": "5"}

        mock_events = [
            {
                "id": "event_1",
                "time": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "received": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "adapter": "nws",
                "category": "Alert",
                "subject": "Event 1",
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

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock(status_code=200)

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await events_rows(mock_request)

        assert result.status_code == 200
        # Verify it uses the fragment template
        call_args = mock_templates.TemplateResponse.call_args
        assert call_args.kwargs.get("name") == "_events_rows.html"


class TestGeometrySummary:
    """Test geometry summary function."""

    def test_geometry_summary_polygon(self):
        """Polygon geometry shows point count."""
        from central.gui.routes import _geometry_summary

        geom = {
            "type": "Polygon",
            "coordinates": [[[-122, 37], [-122, 38], [-121, 38], [-121, 37], [-122, 37]]]
        }
        summary = _geometry_summary(geom)
        assert "Polygon" in summary
        assert "5 pts" in summary

    def test_geometry_summary_point(self):
        """Point geometry shows 'Point'."""
        from central.gui.routes import _geometry_summary

        geom = {"type": "Point", "coordinates": [-122.4, 37.8]}
        summary = _geometry_summary(geom)
        assert summary == "Point"

    def test_geometry_summary_linestring(self):
        """LineString geometry shows point count."""
        from central.gui.routes import _geometry_summary

        geom = {
            "type": "LineString",
            "coordinates": [[-122, 37], [-121, 38], [-120, 39]]
        }
        summary = _geometry_summary(geom)
        assert "Line" in summary
        assert "3 pts" in summary

    def test_geometry_summary_none(self):
        """None geometry shows 'None'."""
        from central.gui.routes import _geometry_summary

        summary = _geometry_summary(None)
        assert summary == "None"


class TestDataGeometryAttribute:
    """Test that rows have valid geometry data attributes."""

    @pytest.mark.asyncio
    async def test_event_with_geometry_has_valid_json(self):
        """Events with geometry have parseable JSON in data-geometry."""
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

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock(status_code=200)

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await events_rows(mock_request)

        context = mock_templates.TemplateResponse.call_args.kwargs.get("context")
        event = context["events"][0]
        # Geometry should be parsed dict, not string
        assert isinstance(event["geometry"], dict)
        assert event["geometry"]["type"] == "Polygon"

    @pytest.mark.asyncio
    async def test_event_without_geometry_has_none(self):
        """Events without geometry have None for geometry field."""
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

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock(status_code=200)

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await events_rows(mock_request)

        context = mock_templates.TemplateResponse.call_args.kwargs.get("context")
        event = context["events"][0]
        assert event["geometry"] is None


class TestCrossEndpointParity:
    """Test that /events.json and /events return the same filtered results."""

    @pytest.mark.asyncio
    async def test_category_filter_both_endpoints(self):
        """Category filter works on both /events.json and /events."""
        mock_events = [
            {
                "id": "weather_event",
                "time": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "received": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "adapter": "nws",
                "category": "Weather Alert",
                "subject": "Weather Event",
                "geometry": None,
                "data": {},
                "regions": [],
            },
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = mock_events
        mock_conn.fetchrow.return_value = {
            "map_tile_url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            "map_attribution": "OpenStreetMap",
        }

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        query_params = {"category": "Weather Alert"}

        # Test /events.json
        json_request = MagicMock()
        json_request.state.operator = MagicMock(id=1, username="admin")
        json_request.query_params = query_params

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            json_response = await events_json(json_request)

        json_data = json.loads(json_response.body)
        assert len(json_data["events"]) == 1
        assert json_data["events"][0]["category"] == "Weather Alert"

        # Test /events
        html_request = MagicMock()
        html_request.state.operator = MagicMock(id=1, username="admin")
        html_request.state.csrf_token = "test_csrf"
        html_request.query_params = query_params

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock(status_code=200)

        mock_conn.fetch.return_value = mock_events

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                await events_list(html_request)

        html_context = mock_templates.TemplateResponse.call_args.kwargs.get("context")
        assert len(html_context["events"]) == 1
        assert html_context["events"][0]["category"] == "Weather Alert"

    @pytest.mark.asyncio
    async def test_cursor_pagination_both_endpoints(self):
        """Cursor pagination works identically on both endpoints."""
        first_page = [
            {
                "id": f"event_{i}",
                "time": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc) - timedelta(hours=i),
                "received": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc) - timedelta(hours=i),
                "adapter": "nws",
                "category": "Alert",
                "subject": f"Event {i}",
                "geometry": None,
                "data": {},
                "regions": [],
            }
            for i in range(3)
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = first_page
        mock_conn.fetchrow.return_value = {
            "map_tile_url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            "map_attribution": "OpenStreetMap",
        }

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        json_request = MagicMock()
        json_request.state.operator = MagicMock(id=1, username="admin")
        json_request.query_params = {"limit": "2"}

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            json_response = await events_json(json_request)

        json_data = json.loads(json_response.body)
        json_cursor = json_data["next_cursor"]
        assert json_cursor is not None

        html_request = MagicMock()
        html_request.state.operator = MagicMock(id=1, username="admin")
        html_request.state.csrf_token = "test_csrf"
        html_request.query_params = {"limit": "2"}

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock(status_code=200)

        mock_conn.fetch.return_value = first_page

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                await events_list(html_request)

        html_context = mock_templates.TemplateResponse.call_args.kwargs.get("context")
        html_cursor = html_context["next_cursor"]

        assert json_cursor == html_cursor


class TestErrorSemantics:
    """Test error handling differences between JSON and HTML endpoints."""

    @pytest.mark.asyncio
    async def test_json_endpoint_returns_400_on_invalid_limit(self):
        """/events.json?limit=0 returns 400 JSON error."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.query_params = {"limit": "0"}

        response = await events_json(mock_request)

        assert response.status_code == 400
        data = json.loads(response.body)
        assert "error" in data

    @pytest.mark.asyncio
    async def test_html_endpoint_returns_200_with_error_banner(self):
        """/events?limit=0 returns 200 HTML with error banner."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.state.csrf_token = "test_csrf"
        mock_request.query_params = {"limit": "0"}

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {
            "map_tile_url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            "map_attribution": "OpenStreetMap",
        }

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock(status_code=200)

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await events_list(mock_request)

        assert result.status_code == 200
        context = mock_templates.TemplateResponse.call_args.kwargs.get("context")
        assert context["filter_error"] is not None
        assert "limit" in context["filter_error"].lower()
        assert context["events"] == []


class TestEventRowDataAttributes:
    """Test that _events_rows.html renders required data attributes."""

    @pytest.mark.asyncio
    async def test_row_renders_data_adapter_attribute(self):
        """Event rows include data-adapter attribute for color coding."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.state.csrf_token = "test_csrf"
        mock_request.query_params = {}

        mock_events = [
            {
                "id": "test1",
                "time": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "received": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                "adapter": "usgs_quake",
                "category": "quake.event",
                "subject": "M4.2 Earthquake",
                "geometry": None,
                "data": {},
                "regions": [],
            },
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = mock_events
        mock_conn.fetchrow.return_value = {
            "map_tile_url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            "map_attribution": "OpenStreetMap",
        }

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock(status_code=200)

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await events_list(mock_request)

        assert result.status_code == 200
        mock_templates.TemplateResponse.assert_called_once()
        context = mock_templates.TemplateResponse.call_args.kwargs.get("context")
        # The template receives events with adapter field for data-adapter attribute
        assert len(context["events"]) == 1
        assert context["events"][0]["adapter"] == "usgs_quake"
        assert context["events"][0]["category"] == "quake.event"
        assert context["events"][0]["subject"] == "M4.2 Earthquake"
