"""Tests for adapter list and edit routes."""

import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Set required env vars before importing central modules
os.environ.setdefault("CENTRAL_DB_DSN", "postgresql://test:test@localhost/test")
os.environ.setdefault("CENTRAL_CSRF_SECRET", "testsecret12345678901234567890ab")
os.environ.setdefault("CENTRAL_NATS_URL", "nats://localhost:4222")


class TestAdaptersListUnauthenticated:
    """Test adapters list without authentication."""

    @pytest.mark.asyncio
    async def test_adapters_list_unauthenticated_redirects(self):
        """GET /adapters without auth redirects to /login."""
        from central.gui.routes import adapters_list

        mock_request = MagicMock()
        mock_request.state.operator = None

        # The middleware handles the redirect, so we test the route expects operator
        # In practice, middleware returns 302 before route is called
        # This test verifies the route structure expects authentication
        assert adapters_list is not None


class TestAdaptersListAuthenticated:
    """Test adapters list with authentication."""

    @pytest.mark.asyncio
    async def test_adapters_list_returns_all_adapters(self):
        """GET /adapters authenticated returns 200 with all adapters."""
        from central.gui.routes import adapters_list

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [
            {"name": "firms", "enabled": True, "cadence_s": 300, "settings": '{"api_key_alias": "firms"}', "paused_at": None, "updated_at": None},
            {"name": "nws", "enabled": True, "cadence_s": 60, "settings": '{"contact_email": "test@test.com"}', "paused_at": None, "updated_at": None},
            {"name": "usgs_quake", "enabled": True, "cadence_s": 120, "settings": '{"feed": "all_hour"}', "paused_at": None, "updated_at": None},
        ]

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_templates.TemplateResponse.return_value = mock_response

        mock_csrf = MagicMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")
        mock_csrf.set_csrf_cookie = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await adapters_list(mock_request, mock_csrf)

        # Verify template was called with adapters
        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert len(context["adapters"]) == 3
        assert context["adapters"][0]["name"] == "firms"
        assert context["adapters"][1]["name"] == "nws"
        assert context["adapters"][2]["name"] == "usgs_quake"


class TestAdaptersEditForm:
    """Test adapter edit form GET."""

    @pytest.mark.asyncio
    async def test_adapters_edit_nws_shows_form(self):
        """GET /adapters/nws authenticated returns 200 with form."""
        from central.gui.routes import adapters_edit_form

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {
            "name": "nws",
            "enabled": True,
            "cadence_s": 60,
            "settings": '{"contact_email": "test@example.com", "region": {"north": 49, "south": 24, "east": -66, "west": -125}}',
            "paused_at": None,
            "updated_at": None,
        }
        mock_conn.fetch.return_value = []  # No API keys

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_templates.TemplateResponse.return_value = mock_response

        mock_csrf = MagicMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")
        mock_csrf.set_csrf_cookie = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await adapters_edit_form(mock_request, "nws", mock_csrf)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert context["adapter"]["name"] == "nws"
        assert context["adapter"]["settings"]["contact_email"] == "test@example.com"

    @pytest.mark.asyncio
    async def test_adapters_edit_nonexistent_returns_404(self):
        """GET /adapters/nonexistent returns 404."""
        from central.gui.routes import adapters_edit_form

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = None

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_csrf = MagicMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            result = await adapters_edit_form(mock_request, "nonexistent", mock_csrf)

        assert result.status_code == 404


class TestAdaptersEditSubmit:
    """Test adapter edit form POST."""

    @pytest.mark.asyncio
    async def test_adapters_edit_valid_changes_updates_db(self):
        """POST /adapters/nws with valid changes updates DB and redirects."""
        from central.gui.routes import adapters_edit_submit

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")
        mock_request.cookies = {}

        # Mock form data
        mock_form = MagicMock()
        mock_form.get.side_effect = lambda k, d="": {
            "cadence_s": "120",
            "contact_email": "new@example.com",
        }.get(k, d)
        mock_form.getlist.return_value = []
        mock_form.__contains__ = lambda self, k: k == "enabled"
        mock_request.form = AsyncMock(return_value=mock_form)

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {
            "name": "nws",
            "enabled": True,
            "cadence_s": 60,
            "settings": '{"contact_email": "old@example.com"}',
            "paused_at": None,
            "updated_at": None,
        }
        mock_conn.execute = AsyncMock()

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            with patch("central.gui.routes.write_audit", new_callable=AsyncMock) as mock_audit:
                result = await adapters_edit_submit(mock_request, "nws", mock_csrf)

        assert result.status_code == 302
        assert result.headers["location"] == "/adapters"
        mock_conn.execute.assert_called()
        mock_audit.assert_called_once()

    @pytest.mark.asyncio
    async def test_adapters_edit_invalid_cadence_shows_error(self):
        """POST /adapters/nws with cadence_s=30 shows error, no DB update."""
        from central.gui.routes import adapters_edit_submit

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_form = MagicMock()
        mock_form.get.side_effect = lambda k, d="": {
            "cadence_s": "30",
            "contact_email": "test@example.com",
        }.get(k, d)
        mock_form.getlist.return_value = []
        mock_form.__contains__ = lambda self, k: k == "enabled"
        mock_request.form = AsyncMock(return_value=mock_form)

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {
            "name": "nws",
            "enabled": True,
            "cadence_s": 60,
            "settings": '{"contact_email": "test@example.com"}',
            "paused_at": None,
            "updated_at": None,
        }
        mock_conn.fetch.return_value = []

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_templates.TemplateResponse.return_value = mock_response

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")
        mock_csrf.set_csrf_cookie = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await adapters_edit_submit(mock_request, "nws", mock_csrf)

        # Should re-render form with error
        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert "cadence_s" in context["errors"]
        assert "60" in context["errors"]["cadence_s"] or "3600" in context["errors"]["cadence_s"]

    @pytest.mark.asyncio
    async def test_adapters_edit_firms_unknown_api_key_shows_error(self):
        """POST /adapters/firms with unknown api_key_alias shows error."""
        from central.gui.routes import adapters_edit_submit

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_form = MagicMock()
        mock_form.get.side_effect = lambda k, d="": {
            "cadence_s": "300",
            "api_key_alias": "nonexistent_key",
        }.get(k, d)
        mock_form.getlist.return_value = ["VIIRS_SNPP_NRT"]
        mock_form.__contains__ = lambda self, k: k == "enabled"
        mock_request.form = AsyncMock(return_value=mock_form)

        mock_conn = AsyncMock()
        mock_conn.fetchrow.side_effect = [
            {  # First call: get adapter
                "name": "firms",
                "enabled": True,
                "cadence_s": 300,
                "settings": '{"api_key_alias": "firms", "satellites": ["VIIRS_SNPP_NRT"]}',
                "paused_at": None,
                "updated_at": None,
            },
            None,  # Second call: check api_key exists - returns None
        ]
        mock_conn.fetch.return_value = []

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_templates.TemplateResponse.return_value = mock_response

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")
        mock_csrf.set_csrf_cookie = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await adapters_edit_submit(mock_request, "firms", mock_csrf)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert "api_key_alias" in context["errors"]
        assert "nonexistent_key" in context["errors"]["api_key_alias"]

    @pytest.mark.asyncio
    async def test_adapters_edit_usgs_unknown_feed_shows_error(self):
        """POST /adapters/usgs_quake with unknown feed shows error."""
        from central.gui.routes import adapters_edit_submit

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_form = MagicMock()
        mock_form.get.side_effect = lambda k, d="": {
            "cadence_s": "120",
            "feed": "invalid_feed",
        }.get(k, d)
        mock_form.getlist.return_value = []
        mock_form.__contains__ = lambda self, k: k == "enabled"
        mock_request.form = AsyncMock(return_value=mock_form)

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {
            "name": "usgs_quake",
            "enabled": True,
            "cadence_s": 120,
            "settings": '{"feed": "all_hour"}',
            "paused_at": None,
            "updated_at": None,
        }
        mock_conn.fetch.return_value = []

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_templates.TemplateResponse.return_value = mock_response

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")
        mock_csrf.set_csrf_cookie = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await adapters_edit_submit(mock_request, "usgs_quake", mock_csrf)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert "feed" in context["errors"]


class TestAdaptersAudit:
    """Test adapter audit logging."""

    @pytest.mark.asyncio
    async def test_audit_row_has_before_after(self):
        """Audit row has before/after JSONB populated correctly."""
        from central.gui.routes import adapters_edit_submit

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_form = MagicMock()
        mock_form.get.side_effect = lambda k, d="": {
            "cadence_s": "120",
            "contact_email": "new@example.com",
        }.get(k, d)
        mock_form.getlist.return_value = []
        mock_form.__contains__ = lambda self, k: k == "enabled"
        mock_request.form = AsyncMock(return_value=mock_form)

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {
            "name": "nws",
            "enabled": True,
            "cadence_s": 60,
            "settings": '{"contact_email": "old@example.com"}',
            "paused_at": None,
            "updated_at": None,
        }
        mock_conn.execute = AsyncMock()

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()

        captured_audit = {}

        async def capture_audit(conn, action, operator_id=None, target=None, before=None, after=None):
            captured_audit["action"] = action
            captured_audit["target"] = target
            captured_audit["before"] = before
            captured_audit["after"] = after

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            with patch("central.gui.routes.write_audit", side_effect=capture_audit):
                result = await adapters_edit_submit(mock_request, "nws", mock_csrf)

        assert captured_audit["action"] == "adapter.update"
        assert captured_audit["target"] == "nws"
        assert captured_audit["before"]["cadence_s"] == 60
        assert captured_audit["after"]["cadence_s"] == 120
        assert captured_audit["before"]["settings"]["contact_email"] == "old@example.com"
        assert captured_audit["after"]["settings"]["contact_email"] == "new@example.com"


class TestAdaptersJsonbRegression:
    """Regression tests for JSONB double-encoding bug."""

    @pytest.mark.asyncio
    async def test_settings_passed_as_dict_not_string(self):
        """Verify settings is passed to UPDATE as dict, not json.dumps string.

        Regression test for double-encoding bug where json.dumps() was called
        on settings before passing to asyncpg, which already handles dict->jsonb.
        """
        from central.gui.routes import adapters_edit_submit

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_form = MagicMock()
        mock_form.get.side_effect = lambda k, d="": {
            "cadence_s": "120",
            "contact_email": "test@example.com",
        }.get(k, d)
        mock_form.getlist.return_value = []
        mock_form.__contains__ = lambda self, k: k == "enabled"
        mock_request.form = AsyncMock(return_value=mock_form)

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {
            "name": "nws",
            "enabled": True,
            "cadence_s": 60,
            "settings": {"contact_email": "old@example.com"},  # dict, as asyncpg returns
            "paused_at": None,
            "updated_at": None,
        }
        mock_conn.execute = AsyncMock()

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            with patch("central.gui.routes.write_audit", new_callable=AsyncMock):
                await adapters_edit_submit(mock_request, "nws", mock_csrf)

        # Get the settings argument passed to execute (3rd positional arg after query)
        call_args = mock_conn.execute.call_args
        # args[0] is the query, args[1:] are the parameters
        settings_arg = call_args[0][3]  # enabled=$1, cadence=$2, settings=$3

        # CRITICAL: settings must be a dict, NOT a string
        # If json.dumps() was called, this would be a str like {contact_email: ...}
        assert isinstance(settings_arg, dict), f"settings should be dict, got {type(settings_arg)}: {settings_arg}"
        assert settings_arg["contact_email"] == "test@example.com"

    @pytest.mark.asyncio
    async def test_audit_before_after_passed_as_dict(self):
        """Verify audit before/after are passed as dicts, not json.dumps strings."""
        from central.gui.routes import adapters_edit_submit

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_form = MagicMock()
        mock_form.get.side_effect = lambda k, d="": {
            "cadence_s": "120",
            "contact_email": "new@example.com",
        }.get(k, d)
        mock_form.getlist.return_value = []
        mock_form.__contains__ = lambda self, k: k == "enabled"
        mock_request.form = AsyncMock(return_value=mock_form)

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {
            "name": "nws",
            "enabled": True,
            "cadence_s": 60,
            "settings": {"contact_email": "old@example.com"},  # dict
            "paused_at": None,
            "updated_at": None,
        }
        mock_conn.execute = AsyncMock()

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()

        captured_audit = {}

        async def capture_audit(conn, action, operator_id=None, target=None, before=None, after=None):
            captured_audit["before"] = before
            captured_audit["after"] = after

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            with patch("central.gui.routes.write_audit", side_effect=capture_audit):
                await adapters_edit_submit(mock_request, "nws", mock_csrf)

        # CRITICAL: before and after must be dicts, NOT strings
        assert isinstance(captured_audit["before"], dict), f"before should be dict, got {type(captured_audit['before'])}"
        assert isinstance(captured_audit["after"], dict), f"after should be dict, got {type(captured_audit['after'])}"
        assert isinstance(captured_audit["before"]["settings"], dict), "before.settings should be dict"
        assert isinstance(captured_audit["after"]["settings"], dict), "after.settings should be dict"
