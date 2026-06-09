"""Tests for events feed frontend routes."""

import html
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from central.adapter_discovery import discover_adapters
from central.gui import templates as _gui_templates
from central.gui.routes import events_list, events_rows, events_json, _derive_subject


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
        # v0.7.1: adapter is now a multi-select; single value parses to a 1-list.
        assert context["filter_state"]["adapters"] == ["nws"]

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
        # v0.7.1: the GUI uses time presets, but legacy since/until are still
        # honored (JSON API + bookmarks); they parse without error and apply.
        assert context["filter_error"] is None

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
        assert context["filter_state"]["region_north"] == "49.5"
        assert context["filter_state"]["region_south"] == "31"
        assert context["filter_state"]["region_east"] == "-102"
        assert context["filter_state"]["region_west"] == "-124.5"

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
    async def test_events_with_limit_builds_pagination(self):
        """GET /events?limit=5 (offset-mode): pagination reflects total/next page."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.state.csrf_token = "test_csrf_token"
        mock_request.query_params = {"limit": "5"}

        # 5 rows for page 1; total_count=12 (the window count) => 3 pages.
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
                "total_count": 12,
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
        context = mock_templates.TemplateResponse.call_args.kwargs.get("context")
        pg = context["pagination"]
        assert pg["total"] == 12 and pg["total_pages"] == 3 and pg["page"] == 1
        assert pg["next_offset"] == 5 and pg["prev_offset"] is None
        assert context["next_cursor"] is None  # offset-mode (GUI) doesn't use cursor
        assert len(context["events"]) == 5


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
    async def test_pagination_modes_split_cleanly_by_endpoint(self):
        """v0.7.3: events.json keeps CURSOR pagination (next_cursor set); the
        GUI events page uses OFFSET pagination (next_cursor None + pagination)."""
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
        # events.json paginates by cursor; the GUI by offset (no cursor, has paginator).
        assert json_cursor is not None
        assert html_context["next_cursor"] is None
        assert "pagination" in html_context


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
        # `subject` is now derived from the inner payload (rendered partial),
        # not a DB pass-through, so the mock's input value is no longer echoed;
        # just confirm the field is present. See TestEventsJsonSubject for the
        # derivation contract.
        assert "subject" in context["events"][0]


# --- PR L-b: operator /events tab polish ---------------------------------


def _events_context(events):
    """Minimal context for rendering _events_rows.html as a standalone fragment."""
    n = len(events)
    return {
        "events": events,
        "next_cursor": None,
        "base_path": "/events",
        "query_string": "",
        "pagination": {
            "total": n, "offset": 0, "limit": 50, "page": 1, "total_pages": 1,
            "start": 1 if n else 0, "end": n, "prev_offset": None, "next_offset": None,
            "pages": [{"page": 1, "offset": 0, "current": True}] if n else [],
            "per_page_options": [25, 50, 100, 250],
        },
        "filter_error": None,
    }


def _event(adapter, inner=None, geometry=None):
    """Build an event dict matching _fetch_events output shape.

    `inner` populates payload->data->data (the adapter-specific payload) at
    event["data"]["data"]["data"], which the per-adapter partials read.
    """
    return {
        "id": "evt-" + adapter,
        "time": "2026-05-17T12:00:00+00:00",
        "received": "2026-05-17T12:00:00+00:00",
        "adapter": adapter,
        "category": adapter + ".test",
        "subject": "subject",
        "geometry": geometry,
        "geometry_summary": "",
        "data": {"data": {"data": inner or {}}},
        "regions": [],
    }


def _render_rows(events):
    """Render _events_rows.html through the real Jinja environment."""
    from central.gui import templates as gui_templates
    return gui_templates.env.get_template("_events_rows.html").render(
        **_events_context(events)
    )


class TestRegistryDrivenAdapterFilter:
    """(A) Adapter filter <select> is driven by discover_adapters(), no hardcoded list."""

    @pytest.mark.asyncio
    async def test_filter_options_cover_every_discovered_adapter(self):
        from central.adapter_discovery import discover_adapters
        registry = discover_adapters()

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.state.csrf_token = "test_csrf"
        mock_request.query_params = {}

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = []
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
                await events_list(mock_request)

        context = mock_templates.TemplateResponse.call_args.kwargs.get("context")

        # v0.7.4: /events shows event-class adapters only (telemetry-class, e.g.
        # nwis, moved to /telemetry). Registry-derived, sorted, no extras.
        event_names = sorted(n for n, c in registry.items()
                             if getattr(c, "data_class", "event") == "event")
        assert [a["name"] for a in context["adapters"]] == event_names
        # Each entry carries name + display_name (v0.7.1 adds a positional color).
        by_name = {a["name"]: a for a in context["adapters"]}
        for name in event_names:
            cls = registry[name]
            assert by_name[name]["display_name"] == cls.display_name
            assert by_name[name]["color"].startswith("#")


class TestPerAdapterRowPartials:
    """(C) Per-adapter row partials with registry-derived dispatch + _default fallback."""

    def test_every_discovered_adapter_has_a_partial(self):
        from central.adapter_discovery import discover_adapters
        from central.gui import templates as gui_templates
        # get_template raises TemplateNotFound if a per-adapter file is missing.
        for name in discover_adapters():
            gui_templates.env.get_template("_event_rows/%s.html" % name)

    def test_default_fallback_partial_exists(self):
        from central.gui import templates as gui_templates
        gui_templates.env.get_template("_event_rows/_default.html")

    def test_every_discovered_adapter_renders_without_error(self):
        from central.adapter_discovery import discover_adapters
        for name in discover_adapters():
            html = _render_rows([_event(name)])
            assert 'data-adapter="%s"' % name in html

    def test_unknown_adapter_falls_back_to_default(self):
        # No bespoke partial -> dispatch resolves to _default.html (no crash),
        # and the raw payload block is still rendered.
        html = _render_rows([_event("not_a_real_adapter", inner={"foo": "bar"})])
        assert 'data-adapter="not_a_real_adapter"' in html
        assert "event-data-pre" in html

    def test_usgs_quake_partial_surfaces_curated_fields(self):
        html = _render_rows([_event(
            "usgs_quake",
            inner={"magnitude": 4.2, "magType": "mb", "place": "10km N of Town", "depth": 5.0},
        )])
        assert "Magnitude" in html
        assert "4.2" in html
        assert "10km N of Town" in html

    def test_nws_partial_surfaces_curated_fields(self):
        html = _render_rows([_event(
            "nws",
            inner={"event": "Tornado Warning", "headline": "TORNADO", "severity": "Extreme"},
        )])
        assert "Tornado Warning" in html
        assert "Extreme" in html


class TestMapAllAdapterGeometry:
    """(B) Every non-null geometry reaches the map; rebind-on-swap is enabled."""

    def test_polygon_geometry_emitted_as_data_geometry(self):
        poly = {
            "type": "Polygon",
            "coordinates": [[[-104.18, 37.14], [-103.66, 37.14],
                             [-103.66, 37.43], [-104.18, 37.43], [-104.18, 37.14]]],
        }
        html = _render_rows([_event("nws", inner={"event": "x"}, geometry=poly)])
        assert "data-geometry=" in html
        assert "Polygon" in html

    def test_event_without_geometry_omits_data_geometry(self):
        html = _render_rows([_event("swpc_protons", inner={"flux": 1.0})])
        assert "data-geometry=" not in html

    def test_map_rebinds_on_swap_and_handles_degenerate_geometry(self):
        # Regression guard for the NWIS-only map: rebind must fire on HTMX swap
        # and the degenerate-geometry fallback must exist.
        import pathlib
        from central.gui import templates as gui_templates
        src = pathlib.Path(
            gui_templates.env.loader.searchpath[0], "events_list.html"
        ).read_text()
        assert "// rebindEventLayers(); // DISABLED" not in src
        assert "isDegenerate" in src


# --- PR L-c: readable Time / Location / Subject / Adapter columns ---------


def _first_row_cells(html):
    """Visible <td> text of the first event-row (before its detail row).

    Splits on the detail row's class attribute (not the bare string
    'event-detail', which also appears in the page's <style> block).
    """
    import re
    body = html.split('class="event-detail"')[0]
    return [
        re.sub(r"<[^>]+>", "", c).strip()
        for c in re.findall(r"<td[^>]*>(.*?)</td>", body, re.S)
    ]


class TestEventTimeFormat:
    """(A) Server-side 'MM-DD HH:MM UTC' formatting (24h, no seconds, no year;
    v0.7.3 dropped the year from the cell for single-line stable rows)."""

    def test_format_basic_utc(self):
        from central.gui.routes import _format_event_time
        assert _format_event_time("2026-05-21T06:00:00+00:00") == "05-21 06:00 UTC"

    def test_format_converts_offset_to_utc(self):
        from central.gui.routes import _format_event_time
        # 19:30 at -06:00 is 01:30 UTC the next day.
        assert _format_event_time("2026-05-20T19:30:00-06:00") == "05-21 01:30 UTC"

    def test_format_empty_and_none(self):
        from central.gui.routes import _format_event_time
        assert _format_event_time("") == ""
        assert _format_event_time(None) == ""

    def test_format_no_seconds_no_year_no_offset_suffix(self):
        from central.gui.routes import _format_event_time
        out = _format_event_time("2026-01-02T03:04:59+00:00")
        assert out == "01-02 03:04 UTC"
        assert ":59" not in out and "+00" not in out and "2026" not in out


class TestTableDisplayDecoration:
    """(A)/(D) _decorate_table_events adds display fields; /events.json unaffected."""

    def test_adapter_display_matches_registry(self):
        from central.adapter_discovery import discover_adapters
        from central.gui.routes import _decorate_table_events, _format_event_time
        registry = discover_adapters()
        events = [_event(name) for name in registry]
        _decorate_table_events(events)
        for ev in events:
            cls = registry[ev["adapter"]]
            assert ev["adapter_display"] == cls.display_name
            assert ev["time_human"] == _format_event_time(ev["time"])

    def test_unknown_adapter_display_falls_back_to_name(self):
        from central.gui.routes import _decorate_table_events
        events = [_event("not_a_real_adapter")]
        _decorate_table_events(events)
        assert events[0]["adapter_display"] == "not_a_real_adapter"

    @pytest.mark.asyncio
    async def test_events_json_has_no_table_only_fields(self):
        # No /events.json schema change: display fields must not leak into JSON.
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")
        mock_request.query_params = {}

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [{
            "id": "e1",
            "time": datetime(2026, 5, 21, 6, 0, tzinfo=timezone.utc),
            "received": datetime(2026, 5, 21, 6, 0, tzinfo=timezone.utc),
            "adapter": "nwis",
            "category": "hydro",
            "subject": None,
            "geometry": None,
            "data": {},
            "regions": [],
        }]
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            resp = await events_json(mock_request)

        ev = json.loads(resp.body)["events"][0]
        assert "time_human" not in ev
        assert "adapter_display" not in ev


class TestLocationColumn:
    """(B) Generic _enriched/top-level location reader — no adapter logic."""

    def _location(self, inner):
        return _first_row_cells(_render_rows([_event("usgs_quake", inner=inner)]))[2]

    def test_city_state_country(self):
        loc = self._location({"_enriched": {"geocoder": {
            "city": "Trinidad", "state": "Colorado", "country": "United States"}}})
        assert loc == "Trinidad, Colorado, United States"

    def test_state_country_when_city_null(self):
        loc = self._location({"_enriched": {"geocoder": {
            "city": None, "state": "Missouri", "country": "United States"}}})
        assert loc == "Missouri, United States"

    def test_top_level_country_fallback(self):
        # gdacs-style: no geocoder, country at top level.
        assert self._location({"country": "Austria"}) == "Austria"

    def test_top_level_state_only_fallback(self):
        # wfigs-style: state code at top level, no country.
        assert self._location({"state": "CO"}) == "CO"

    def test_landclass_fallback(self):
        loc = self._location({"_enriched": {"geocoder": {
            "city": None, "state": None, "country": None,
            "landclass": "Ridgecrest Field Office"}}})
        assert loc == "Ridgecrest Field Office"

    def test_coordinates_alone_are_not_a_location(self):
        # Bare lat/lon is a position, not a place name -> "—".
        assert self._location({"latitude": 35.3603324, "longitude": -117.7854995}) == "—"

    def test_none_when_nothing_available(self):
        assert self._location({}) == "—"


class TestSubjectColumn:
    """(C) Per-adapter one-line summaries, registry-derived dispatch + fallback."""

    def test_every_adapter_has_a_summary_partial(self):
        from central.adapter_discovery import discover_adapters
        from central.gui import templates as gui_templates
        for name in discover_adapters():
            gui_templates.env.get_template("_event_summaries/%s.html" % name)

    def test_default_summary_partial_exists(self):
        from central.gui import templates as gui_templates
        gui_templates.env.get_template("_event_summaries/_default.html")

    def test_every_adapter_summary_renders_without_error(self):
        from central.adapter_discovery import discover_adapters
        for name in discover_adapters():
            _render_rows([_event(name)])  # must not raise

    def test_usgs_quake_summary_is_plain_language(self):
        cells = _first_row_cells(_render_rows([_event(
            "usgs_quake", inner={"magnitude": 1.347, "magType": "ml",
                                 "place": "14 km W of Johannesburg, CA"})]))
        assert cells[3] == "Magnitude 1.3 — 14 km W of Johannesburg, CA"
        assert "ml" not in cells[3]  # scale code dropped

    def test_nwis_summary_drops_parameter_code(self):
        cells = _first_row_cells(_render_rows([_event(
            "nwis", inner={"parameter_code": "00060", "value": 111.0,
                           "unit_of_measure": "ft^3/s"})]))
        assert cells[3] == "Water reading: 111.0 ft^3/s"
        assert "00060" not in cells[3]  # opaque pcode dropped

    def test_unknown_adapter_summary_is_dash(self):
        cells = _first_row_cells(_render_rows([_event("not_a_real_adapter", inner={"x": 1})]))
        assert cells[3] == "—"

    def test_summary_populates_data_subject_for_popup(self):
        html = _render_rows([_event("usgs_quake", inner={"magnitude": 1.347,
                                                          "place": "near Town"})])
        assert 'data-subject="Magnitude 1.3 — near Town"' in html


class TestTableRendersThroughHTTP:
    """End-to-end HTTP render: Time and Adapter cells must be populated in the
    real response, guarding the route->template binding (not the helper alone)."""

    def _mock_pool(self):
        now = datetime(2026, 5, 21, 6, 0, tzinfo=timezone.utc)

        def _fetchrow(query, *args):
            q = " ".join(str(query).split())
            if "config.sessions" in q:
                return {"id": 1, "username": "admin", "created_at": now,
                        "password_changed_at": now, "csrf_token": "csrf"}
            if "map_tile_url" in q:
                return {"map_tile_url": "https://t/{z}/{x}/{y}.png",
                        "map_attribution": "OSM"}
            return None

        rows = [
            {"id": "evt-q", "time": now, "received": now, "adapter": "usgs_quake",
             "category": "quake", "subject": None, "geometry": None,
             "data": {"data": {"data": {"magnitude": 1.3, "place": "near Town"}}},
             "regions": []},
            {"id": "evt-w", "time": now, "received": now, "adapter": "nwis",
             "category": "hydro", "subject": None, "geometry": None,
             "data": {"data": {"data": {"value": 5.0, "unit_of_measure": "ft"}}},
             "regions": []},
        ]
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(side_effect=_fetchrow)
        mock_conn.fetch = AsyncMock(return_value=rows)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(return_value=mock_conn)
        return mock_pool

    def _client(self):
        from fastapi import FastAPI
        from starlette.testclient import TestClient
        from central.gui.middleware import SessionMiddleware
        from central.gui.routes import router
        app = FastAPI()
        app.include_router(router)
        app.add_middleware(SessionMiddleware)
        return TestClient(app, cookies={"central_session": "valid"})

    def _expected_adapter_display(self):
        from central.adapter_discovery import discover_adapters
        return discover_adapters()["usgs_quake"].display_name

    def test_events_page_time_and_adapter_cells_populated(self):
        mock_pool = self._mock_pool()
        with patch("central.gui.middleware.get_pool", return_value=mock_pool), \
             patch("central.gui.routes.get_pool", return_value=mock_pool):
            resp = self._client().get("/events")
        assert resp.status_code == 200
        cells = _first_row_cells(resp.text)
        # v0.7.3: single-line MM-DD HH:MM UTC (no year in the cell).
        assert cells[1].endswith("UTC") and "2026" not in cells[1] and len(cells[1].split()) == 3
        # Adapter cell is now a chip showing the short name; display_name is the tooltip.
        assert cells[4] == "usgs_quake"
        assert self._expected_adapter_display() in resp.text   # display_name in title=

    def test_events_rows_fragment_time_and_adapter_cells_populated(self):
        mock_pool = self._mock_pool()
        with patch("central.gui.middleware.get_pool", return_value=mock_pool), \
             patch("central.gui.routes.get_pool", return_value=mock_pool):
            resp = self._client().get("/events/rows")
        assert resp.status_code == 200
        cells = _first_row_cells(resp.text)
        assert cells[1].endswith("UTC") and "2026" not in cells[1] and len(cells[1].split()) == 3
        assert cells[4] == "usgs_quake"
        assert self._expected_adapter_display() in resp.text


# --- feat(events-json-subject): JSON subject derivation ------------------

# Representative inner adapter payloads (payload->'data'->'data'), captured from
# production -- one per registered adapter. Keyed by adapter name so the
# coverage test below fails loudly if a new adapter ships without a sample.
_SAMPLE_INNER = {
    "eonet": {"title": "Kress Wildfire, Swisher, Texas"},
    "firms": {"frp": 0.34, "confidence": "nominal"},
    "gdacs": {"title": "Green flood alert in Austria", "alertlevel": "Green"},
    "inciweb": {"title": "MTHLF Jericho Creek"},
    "nwis": {"value": 93.2, "unit_of_measure": "ft^3/s"},
    "nws": {"event": "Special Weather Statement", "severity": "Moderate"},
    "swpc_alerts": {
        "product_id": "EF3A",
        "message": (
            "Space Weather Message Code: ALTEF3\r\nSerial Number: 3691\r\n"
            "Issue Time: 2026 May 21 0509 UTC\r\n\r\nCONTINUED ALERT: "
            "Electron 2MeV Integral Flux exceeded 1000pfu"
        ),
    },
    "swpc_kindex": {"Kp": 1.0},
    "swpc_protons": {"flux": 15.06399917602539, "energy": ">=1 MeV"},
    "usgs_quake": {"magnitude": 1.009682538298, "place": "17 km W of Searles Valley, CA"},
    "wfigs_incidents": {"county": "Montezuma", "state": "CO"},
    "wfigs_perimeters": {"county": "Carbon", "state": "MT"},
    "wzdx": {"road_names": ["I-80"], "direction": "eastbound"},
    "tomtom_flow": {"road_category": "primary", "relative_speed": 0.11},
    "tomtom_incidents": {"description": "Roadworks", "from": "Early Road", "to": "Slade Road"},
    "itd_511": {"event_type_short": "work_zone", "roadway_name": "I-84"},
    "itd_511_cameras": {"location": "I-84 Mountain Home", "camera_id": 42},
    "avalanche_org": {"zone_name": "Banner Summit", "danger_name": "Considerable"},
    "celestrak_tle": {
        "norad_id": 25544,
        "satellite_name": "ISS (ZARYA)",
        "_enriched": {"orbit": {
            "inclination_deg": 51.6336,
            "mean_motion_rev_per_day": 15.49672912,
        }},
    },
    "satpass_predict": {
        "satellite_name": "ISS (ZARYA)",
        "norad_id": 25544,
        "peak_time": "2026-06-09T15:39:37+00:00",
        "max_elevation_deg": 40.3,
    },
}

# Exact expected subjects for the deterministic adapters. swpc_alerts is omitted
# (its message runs through Jinja's truncate(80)) and is checked separately.
# swpc_protons expects unescaped ">=" -- _derive_subject html.unescapes the
# autoescaped partial output so JSON consumers get plain text.
_EXPECTED_SUBJECT = {
    "eonet": "Kress Wildfire, Swisher, Texas",
    "firms": "Fire detected — 0.34 MW radiative power",
    "gdacs": "Green flood alert in Austria — Green alert",
    "inciweb": "MTHLF Jericho Creek",
    "nwis": "Water reading: 93.2 ft^3/s",
    "nws": "Special Weather Statement — Moderate",
    "swpc_kindex": "Geomagnetic activity (Kp index): 1.0",
    "swpc_protons": "Solar proton flux: 15.06 pfu at >=1 MeV",
    "usgs_quake": "Magnitude 1.0 — 17 km W of Searles Valley, CA",
    "wfigs_incidents": "Wildfire incident — Montezuma, CO",
    "wfigs_perimeters": "Wildfire perimeter — Carbon, MT",
    "wzdx": "Work zone on I-80 eastbound",
    "tomtom_flow": "Traffic flow (primary) — 11% of free-flow",
    "tomtom_incidents": "Roadworks on Early Road → Slade Road",
    "itd_511": "Road work on I-84",
    "itd_511_cameras": "Camera: I-84 Mountain Home",
    "avalanche_org": "Avalanche advisory — Banner Summit (Considerable)",
    "celestrak_tle": "TLE update: ISS (ZARYA) (NORAD 25544) — 92.9min orbit at 51.6°",
    "satpass_predict": "ISS (ZARYA) passes overhead at 15:39 UTC — max elevation 40°",
}


def _subject_event(adapter: str, inner: dict) -> dict:
    """Build a minimal event dict shaped like _fetch_events output."""
    return {"adapter": adapter, "data": {"data": {"data": inner}}}


class TestEventsJsonSubject:
    """/events.json `subject` is derived from the inner payload and carries the
    same human text as the GUI's per-adapter Subject cell (feat/events-json-subject).

    The old `payload->>'subject'` SQL was always null (the CloudEvents envelope
    has no top-level subject). Parameterized over discover_adapters() -- no
    hardcoded adapter list.
    """

    def test_sample_covers_every_registered_adapter(self):
        """No hardcoded list: samples must track the live registry exactly."""
        assert set(_SAMPLE_INNER) == set(discover_adapters())

    @pytest.mark.parametrize("adapter", sorted(discover_adapters()))
    def test_subject_non_null_per_adapter(self, adapter):
        """Every registered adapter derives a non-null subject for a real event."""
        event = _subject_event(adapter, _SAMPLE_INNER[adapter])
        assert _derive_subject(event) is not None

    @pytest.mark.parametrize("adapter", sorted(discover_adapters()))
    def test_subject_matches_rendered_partial(self, adapter):
        """Derived subject equals the adapter's own partial (unescaped) -- the
        JSON path and the GUI Subject cell never diverge."""
        event = _subject_event(adapter, _SAMPLE_INNER[adapter])
        oracle = html.unescape(
            _gui_templates.env.get_template(f"_event_summaries/{adapter}.html").render(event=event)
        ).strip()
        assert _derive_subject(event) == oracle

    @pytest.mark.parametrize("adapter", sorted(_EXPECTED_SUBJECT))
    def test_subject_exact_human_text(self, adapter):
        """Pin the human-readable subject for the deterministic adapters."""
        event = _subject_event(adapter, _SAMPLE_INNER[adapter])
        assert _derive_subject(event) == _EXPECTED_SUBJECT[adapter]

    def test_swpc_alerts_prefixes_id_and_truncates_message(self):
        """swpc_alerts subject prefixes the product id and truncates the body."""
        event = _subject_event("swpc_alerts", _SAMPLE_INNER["swpc_alerts"])
        subject = _derive_subject(event)
        assert subject is not None
        assert subject.startswith("Space weather alert EF3A: ")
        assert subject.endswith("...")

    def test_unknown_adapter_yields_none(self):
        """Unknown adapters fall back to _default.html -> no subject."""
        assert _derive_subject(_subject_event("does_not_exist", {"x": 1})) is None

    def test_missing_source_fields_yields_none(self):
        """An event lacking its adapter's source fields derives no subject."""
        assert _derive_subject(_subject_event("usgs_quake", {})) is None


class TestShowRemovedToggle:
    """v0.9.11 — tombstone visibility wiring. filter_state.show_removed drives
    the 'Show removed' checkbox in events_list.html; defaults to hidden."""

    def _pool(self):
        conn = AsyncMock()
        conn.fetch.return_value = []
        conn.fetchrow.return_value = {
            "map_tile_url": "https://t/{z}/{x}/{y}.png", "map_attribution": "OSM"}
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        return pool

    async def _filter_state(self, query_params):
        req = MagicMock()
        req.state.operator = MagicMock(id=1, username="admin")
        req.state.csrf_token = "t"
        req.query_params = query_params
        templates = MagicMock()
        templates.TemplateResponse.return_value = MagicMock(status_code=200)
        with patch("central.gui.routes._get_templates", return_value=templates):
            with patch("central.gui.routes.get_pool", return_value=self._pool()):
                await events_list(req)
        return templates.TemplateResponse.call_args.kwargs["context"]["filter_state"]

    @pytest.mark.asyncio
    async def test_default_hides_removed(self):
        assert (await self._filter_state({}))["show_removed"] is False

    @pytest.mark.asyncio
    async def test_show_removed_true_reflected(self):
        assert (await self._filter_state({"show_removed": "true"}))["show_removed"] is True
