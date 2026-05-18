"""Tests for region picker functionality."""

import json
import os
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

os.environ.setdefault("CENTRAL_DB_DSN", "postgresql://test:test@localhost/test")
os.environ.setdefault("CENTRAL_CSRF_SECRET", "testsecret12345678901234567890ab")
os.environ.setdefault("CENTRAL_NATS_URL", "nats://localhost:4222")


class TestRegionPickerInTemplate:
    """Test region picker is included in adapter edit page."""

    @pytest.mark.asyncio
    async def test_get_adapters_firms_includes_map_div(self):
        """GET /adapters/firms includes the map div with tile URL."""
        from central.gui.routes import adapters_edit_form

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_conn = AsyncMock()
        mock_conn.fetchrow.side_effect = [
            {  # Adapter row
                "name": "firms",
                "enabled": True,
                "cadence_s": 300,
                "settings": {
                    "api_key_alias": "firms",
                    "satellites": ["VIIRS_SNPP_NRT"],
                    "region": {"north": 49.5, "south": 31.0, "east": -102.0, "west": -124.5}
                },
                "paused_at": None,
                "updated_at": None,
            },
            {  # System settings row
                "map_tile_url": "https://tile.example.com/{z}/{x}/{y}.png",
                "map_attribution": "Test Attribution",
            },
        ]
        mock_conn.fetch.return_value = [{"alias": "firms"}]

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_templates.TemplateResponse.return_value = mock_response

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await adapters_edit_form(mock_request, "firms")

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))

        # Should include tile URL from config.system
        assert context["tile_url"] == "https://tile.example.com/{z}/{x}/{y}.png"
        assert context["tile_attribution"] == "Test Attribution"


class TestRegionValidation:
    """Test region coordinate validation in POST handler."""

    @pytest.mark.asyncio
    async def test_valid_region_updates_settings(self):
        """POST with valid bbox updates settings.region."""
        from central.gui.routes import adapters_edit_submit

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_form = MagicMock()
        mock_request.state.csrf_token = "test_csrf_token"
        mock_form.get.side_effect = lambda k, d="": {
            "csrf_token": "test_csrf_token",
            "cadence_s": "300",
            "api_key_alias": "firms",
            "region_north": "45.0",
            "region_south": "35.0",
            "region_east": "-100.0",
            "region_west": "-120.0",
        }.get(k, d)
        mock_form.getlist.return_value = ["VIIRS_SNPP_NRT"]
        mock_form.__contains__ = lambda self, k: k == "enabled"
        mock_request.form = AsyncMock(return_value=mock_form)

        mock_conn = AsyncMock()
        mock_conn.fetchrow.side_effect = [
            {  # Adapter row
                "name": "firms",
                "enabled": True,
                "cadence_s": 300,
                "settings": {"api_key_alias": "firms", "region": {"north": 49.5, "south": 31.0, "east": -102.0, "west": -124.5}},
                "paused_at": None,
                "updated_at": None,
            },
            {"id": 1},  # api_key exists check
        ]
        mock_conn.execute = AsyncMock()

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        captured_settings = {}

        async def capture_execute(query, *args):
            if "UPDATE config.adapters" in query:
                captured_settings["settings"] = args[2]

        mock_conn.execute.side_effect = capture_execute

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            with patch("central.gui.routes.write_audit", new_callable=AsyncMock):
                result = await adapters_edit_submit(mock_request, "firms")

        assert result.status_code == 302
        assert captured_settings["settings"]["region"]["north"] == 45.0
        assert captured_settings["settings"]["region"]["south"] == 35.0
        assert captured_settings["settings"]["region"]["east"] == -100.0
        assert captured_settings["settings"]["region"]["west"] == -120.0

    @pytest.mark.asyncio
    async def test_north_less_than_south_shows_error(self):
        """POST with north <= south shows validation error."""
        from central.gui.routes import adapters_edit_submit

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_form = MagicMock()
        mock_request.state.csrf_token = "test_csrf_token"
        mock_form.get.side_effect = lambda k, d="": {
            "csrf_token": "test_csrf_token",
            "cadence_s": "300",
            "api_key_alias": "firms",
            "region_north": "30.0",  # Less than south!
            "region_south": "35.0",
            "region_east": "-100.0",
            "region_west": "-120.0",
        }.get(k, d)
        mock_form.getlist.return_value = ["VIIRS_SNPP_NRT"]
        mock_form.__contains__ = lambda self, k: k == "enabled"
        mock_request.form = AsyncMock(return_value=mock_form)

        mock_conn = AsyncMock()
        mock_conn.fetchrow.side_effect = [
            {
                "name": "firms",
                "enabled": True,
                "cadence_s": 300,
                "settings": {"api_key_alias": "firms", "region": {"north": 49.5, "south": 31.0, "east": -102.0, "west": -124.5}},
                "paused_at": None,
                "updated_at": None,
            },
            {"id": 1},  # api_key exists
            {"map_tile_url": None, "map_attribution": None},  # system settings for re-render
        ]
        mock_conn.fetch.return_value = [{"alias": "firms"}]

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_templates.TemplateResponse.return_value = mock_response

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await adapters_edit_submit(mock_request, "firms")

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert "region" in context["errors"]
        assert "latitude" in context["errors"]["region"].lower()

    @pytest.mark.asyncio
    async def test_west_greater_than_east_shows_error(self):
        """POST with west >= east shows validation error."""
        from central.gui.routes import adapters_edit_submit

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_form = MagicMock()
        mock_request.state.csrf_token = "test_csrf_token"
        mock_form.get.side_effect = lambda k, d="": {
            "csrf_token": "test_csrf_token",
            "cadence_s": "300",
            "api_key_alias": "firms",
            "region_north": "45.0",
            "region_south": "35.0",
            "region_east": "-130.0",  # Less than west!
            "region_west": "-120.0",
        }.get(k, d)
        mock_form.getlist.return_value = ["VIIRS_SNPP_NRT"]
        mock_form.__contains__ = lambda self, k: k == "enabled"
        mock_request.form = AsyncMock(return_value=mock_form)

        mock_conn = AsyncMock()
        mock_conn.fetchrow.side_effect = [
            {
                "name": "firms",
                "enabled": True,
                "cadence_s": 300,
                "settings": {"api_key_alias": "firms", "region": {"north": 49.5, "south": 31.0, "east": -102.0, "west": -124.5}},
                "paused_at": None,
                "updated_at": None,
            },
            {"id": 1},
            {"map_tile_url": None, "map_attribution": None},
        ]
        mock_conn.fetch.return_value = [{"alias": "firms"}]

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_templates.TemplateResponse.return_value = mock_response

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await adapters_edit_submit(mock_request, "firms")

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert "region" in context["errors"]
        assert "longitude" in context["errors"]["region"].lower()

    @pytest.mark.asyncio
    async def test_latitude_out_of_bounds_shows_error(self):
        """POST with lat > 90 shows validation error."""
        from central.gui.routes import adapters_edit_submit

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_form = MagicMock()
        mock_request.state.csrf_token = "test_csrf_token"
        mock_form.get.side_effect = lambda k, d="": {
            "csrf_token": "test_csrf_token",
            "cadence_s": "300",
            "api_key_alias": "firms",
            "region_north": "95.0",  # > 90!
            "region_south": "35.0",
            "region_east": "-100.0",
            "region_west": "-120.0",
        }.get(k, d)
        mock_form.getlist.return_value = ["VIIRS_SNPP_NRT"]
        mock_form.__contains__ = lambda self, k: k == "enabled"
        mock_request.form = AsyncMock(return_value=mock_form)

        mock_conn = AsyncMock()
        mock_conn.fetchrow.side_effect = [
            {
                "name": "firms",
                "enabled": True,
                "cadence_s": 300,
                "settings": {"api_key_alias": "firms", "region": {"north": 49.5, "south": 31.0, "east": -102.0, "west": -124.5}},
                "paused_at": None,
                "updated_at": None,
            },
            {"id": 1},
            {"map_tile_url": None, "map_attribution": None},
        ]
        mock_conn.fetch.return_value = [{"alias": "firms"}]

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_templates.TemplateResponse.return_value = mock_response

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await adapters_edit_submit(mock_request, "firms")

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert "region" in context["errors"]


class TestRegionAuditLog:
    """Test region changes are captured in audit log."""

    @pytest.mark.asyncio
    async def test_audit_captures_region_change(self):
        """Audit log captures region change in before/after settings."""
        from central.gui.routes import adapters_edit_submit

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_form = MagicMock()
        mock_request.state.csrf_token = "test_csrf_token"
        mock_form.get.side_effect = lambda k, d="": {
            "csrf_token": "test_csrf_token",
            "cadence_s": "300",
            "api_key_alias": "firms",
            "region_north": "45.0",
            "region_south": "35.0",
            "region_east": "-100.0",
            "region_west": "-120.0",
        }.get(k, d)
        mock_form.getlist.return_value = ["VIIRS_SNPP_NRT"]
        mock_form.__contains__ = lambda self, k: k == "enabled"
        mock_request.form = AsyncMock(return_value=mock_form)

        mock_conn = AsyncMock()
        mock_conn.fetchrow.side_effect = [
            {
                "name": "firms",
                "enabled": True,
                "cadence_s": 300,
                "settings": {
                    "api_key_alias": "firms",
                    "region": {"north": 49.5, "south": 31.0, "east": -102.0, "west": -124.5}
                },
                "paused_at": None,
                "updated_at": None,
            },
            {"id": 1},
        ]
        mock_conn.execute = AsyncMock()

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        captured_audit = {}

        async def capture_audit(conn, action, operator_id=None, target=None, before=None, after=None):
            captured_audit["before"] = before
            captured_audit["after"] = after

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            with patch("central.gui.routes.write_audit", side_effect=capture_audit):
                result = await adapters_edit_submit(mock_request, "firms")

        # Before should have old region
        assert captured_audit["before"]["settings"]["region"]["north"] == 49.5
        # After should have new region
        assert captured_audit["after"]["settings"]["region"]["north"] == 45.0
        assert captured_audit["after"]["settings"]["region"]["south"] == 35.0
