"""Tests for the first-run setup wizard with deferred-commit pattern."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from central.gui.routes import (
    setup_operator_form,
    setup_operator_submit,
    setup_system_form,
    setup_system_submit,
    setup_keys_form,
    setup_keys_submit,
    setup_finish_form,
    setup_finish_submit,
)
from central.gui.middleware import SetupGateMiddleware
from central.gui.wizard import WizardState, get_wizard_state, set_wizard_cookie


class TestWizardStepRedirect:
    """Test wizard step redirect logic based on cookie state."""

    def test_no_cookie_redirects_to_operator(self):
        """When no wizard cookie exists, redirect to /setup/operator."""
        from central.gui.middleware import _get_wizard_redirect_from_cookie

        mock_request = MagicMock()
        mock_request.cookies = {}

        result = _get_wizard_redirect_from_cookie(mock_request, "testsecret")
        assert result == "/setup/operator"

    def test_cookie_step_2_redirects_to_system(self):
        """When wizard_step=2 in cookie, redirect to /setup/system."""
        from central.gui.wizard import get_step_route

        result = get_step_route(2)
        assert result == "/setup/system"

    def test_cookie_step_5_redirects_to_finish(self):
        """When wizard_step=5 in cookie, redirect to /setup/finish."""
        from central.gui.wizard import get_step_route

        result = get_step_route(5)
        assert result == "/setup/finish"


class TestSetupOperatorForm:
    """Test operator creation form (step 1)."""

    @pytest.mark.asyncio
    async def test_get_returns_form_without_prefill(self):
        """GET /setup/operator returns the form when no wizard cookie exists."""
        mock_request = MagicMock()
        mock_request.cookies = {}

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_settings") as mock_settings:
                mock_settings.return_value.csrf_secret = "testsecret12345678901234567890ab"
                with patch("central.gui.routes.reuse_or_generate_pre_auth_csrf", return_value=("test_token", "signed_token")):
                    result = await setup_operator_form(mock_request)

        mock_templates.TemplateResponse.assert_called_once()
        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert "csrf_token" in context and context["csrf_token"]
        assert context["error"] is None
        assert context["form_data"] is None


class TestSetupOperatorSubmit:
    """Test operator creation submission."""

    @pytest.mark.asyncio
    async def test_password_mismatch_shows_error(self):
        """POST with password mismatch re-renders with error."""
        mock_request = MagicMock()
        mock_request.cookies = {}
        mock_request.form = AsyncMock(return_value={"csrf_token": "test_csrf"})

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.validate_pre_auth_csrf", return_value=True):
                with patch("central.gui.routes.get_settings") as mock_settings:
                    mock_settings.return_value.csrf_secret = "testsecret12345678901234567890ab"
                    with patch("central.gui.routes.reuse_or_generate_pre_auth_csrf", return_value=("test_token", "signed")):
                        result = await setup_operator_submit(
                            mock_request,
                            username="testuser",
                            password="password1",
                            confirm_password="password2",
                        )

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert context["error"] == "Passwords do not match"

    @pytest.mark.asyncio
    async def test_valid_creates_wizard_cookie_and_redirects(self):
        """POST with valid data creates wizard cookie and redirects to /setup/system."""
        mock_request = MagicMock()
        mock_request.cookies = {}
        mock_request.form = AsyncMock(return_value={"csrf_token": "test_csrf"})

        with patch("central.gui.routes.validate_pre_auth_csrf", return_value=True):
            with patch("central.gui.routes.get_settings") as mock_settings:
                mock_settings.return_value.csrf_secret = "testsecret12345678901234567890ab"
                with patch("central.gui.routes.hash_password", return_value="hashed_pw"):
                    result = await setup_operator_submit(
                        mock_request,
                        username="testuser",
                        password="password123",
                        confirm_password="password123",
                    )

        assert result.status_code == 302
        assert result.headers["location"] == "/setup/system"


class TestSetupSystemForm:
    """Test system settings form (step 2)."""

    @pytest.mark.asyncio
    async def test_no_wizard_cookie_redirects_to_operator(self):
        """GET /setup/system without wizard cookie redirects to /setup/operator."""
        mock_request = MagicMock()
        mock_request.cookies = {}

        with patch("central.gui.routes.get_settings") as mock_settings:
            mock_settings.return_value.csrf_secret = "testsecret12345678901234567890ab"
            result = await setup_system_form(mock_request)

        assert result.status_code == 302
        assert result.headers["location"] == "/setup/operator"


class TestSetupGateMiddlewareWizard:
    """Test SetupGateMiddleware with wizard paths."""

    @pytest.mark.asyncio
    async def test_allows_setup_operator_when_incomplete(self):
        """SetupGateMiddleware allows /setup/operator when setup_complete=False."""
        from starlette.testclient import TestClient
        from fastapi import FastAPI

        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value={"setup_complete": False})
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=mock_conn)

        with patch("central.gui.middleware.get_pool", return_value=mock_pool):
            app = FastAPI()

            @app.get("/setup/operator")
            async def setup_operator():
                return {"message": "operator form"}

            app.add_middleware(SetupGateMiddleware)
            client = TestClient(app)

            response = client.get("/setup/operator")
            assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_redirects_all_setup_paths_when_complete(self):
        """SetupGateMiddleware redirects /setup/* to / when setup_complete=True."""
        from starlette.testclient import TestClient
        from fastapi import FastAPI

        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value={"setup_complete": True})
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=mock_conn)

        with patch("central.gui.middleware.get_pool", return_value=mock_pool):
            app = FastAPI()

            @app.get("/")
            async def index():
                return {"message": "home"}

            @app.get("/setup/operator")
            async def setup_operator():
                return {"message": "operator"}

            app.add_middleware(SetupGateMiddleware)
            client = TestClient(app, follow_redirects=False)

            response = client.get("/setup/operator")
            assert response.status_code == 302
            assert response.headers["location"] == "/"
