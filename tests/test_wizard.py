"""Tests for the first-run setup wizard."""

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
    setup_adapters_form,
    setup_adapters_submit,
    setup_finish_form,
    setup_finish_submit,
)
from central.gui.middleware import SetupGateMiddleware, _get_wizard_redirect_step


class TestWizardStepRedirect:
    """Test wizard step redirect logic."""

    @pytest.mark.asyncio
    async def test_no_operators_redirects_to_operator(self):
        """When no operators exist, redirect to /setup/operator."""
        mock_conn = AsyncMock()
        mock_conn.fetchval.side_effect = [0]  # No operators

        result = await _get_wizard_redirect_step(mock_conn)
        assert result == "/setup/operator"

    @pytest.mark.asyncio
    async def test_default_tile_url_redirects_to_system(self):
        """When map_tile_url is default, redirect to /setup/system."""
        mock_conn = AsyncMock()
        mock_conn.fetchval.side_effect = [1]  # Has operator
        mock_conn.fetchrow.return_value = {
            "map_tile_url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
        }

        result = await _get_wizard_redirect_step(mock_conn)
        assert result == "/setup/system"

    @pytest.mark.asyncio
    async def test_no_adapters_touched_redirects_to_keys(self):
        """When no adapters have been updated, redirect to /setup/keys."""
        mock_conn = AsyncMock()
        mock_conn.fetchval.side_effect = [1, 0]  # Has operator, no adapters touched
        mock_conn.fetchrow.return_value = {
            "map_tile_url": "https://custom.example.com/{z}/{x}/{y}.png"
        }

        result = await _get_wizard_redirect_step(mock_conn)
        assert result == "/setup/keys"

    @pytest.mark.asyncio
    async def test_all_steps_complete_redirects_to_finish(self):
        """When all steps done, redirect to /setup/finish."""
        mock_conn = AsyncMock()
        mock_conn.fetchval.side_effect = [1, 1]  # Has operator, adapters touched
        mock_conn.fetchrow.return_value = {
            "map_tile_url": "https://custom.example.com/{z}/{x}/{y}.png"
        }

        result = await _get_wizard_redirect_step(mock_conn)
        assert result == "/setup/finish"


class TestSetupOperatorForm:
    """Test operator creation form (step 1)."""

    @pytest.mark.asyncio
    async def test_get_returns_form(self):
        """GET /setup/operator returns the form when no operator exists."""
        mock_request = MagicMock()

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock()

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = None  # No operator exists

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")
        mock_csrf.set_csrf_cookie = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await setup_operator_form(mock_request, mock_csrf)

        mock_templates.TemplateResponse.assert_called_once()
        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert context["csrf_token"] == "token"
        assert context["error"] is None
        assert context["existing_operator"] is None

    @pytest.mark.asyncio
    async def test_get_returns_confirmation_when_operator_exists(self):
        """GET /setup/operator shows confirmation when operator already exists."""
        mock_request = MagicMock()

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_response.body = b"Operator Already Configured"
        mock_templates.TemplateResponse.return_value = mock_response

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {"username": "admin"}  # Operator exists

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")
        mock_csrf.set_csrf_cookie = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await setup_operator_form(mock_request, mock_csrf)

        mock_templates.TemplateResponse.assert_called_once()
        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert context["existing_operator"] == {"username": "admin"}
        assert context["error"] is None


class TestSetupOperatorSubmit:
    """Test operator creation submission."""

    @pytest.mark.asyncio
    async def test_password_mismatch_shows_error(self):
        """POST with password mismatch re-renders with error."""
        mock_request = MagicMock()
        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock()

        mock_conn = AsyncMock()
        mock_conn.fetchval.return_value = 0  # No existing operators

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")
        mock_csrf.set_csrf_cookie = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await setup_operator_submit(
                    mock_request,
                    username="admin",
                    password="password123",
                    confirm_password="different",
                    csrf_protect=mock_csrf,
                )

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert context["error"] == "Passwords do not match"

    @pytest.mark.asyncio
    async def test_valid_creates_operator_and_redirects(self):
        """POST with valid data creates operator and redirects to /setup/system."""
        mock_request = MagicMock()

        mock_conn = AsyncMock()
        mock_conn.fetchval.return_value = 0  # No existing operators
        mock_conn.fetchrow.side_effect = [
            {"id": 1},  # INSERT RETURNING id
            {"session_lifetime_days": 90},  # system settings
        ]

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            with patch("central.gui.routes.hash_password", return_value="hashed"):
                with patch("central.gui.routes.create_session", new_callable=AsyncMock) as mock_session:
                    mock_session.return_value = ("session_token", datetime.now())
                    with patch("central.gui.routes.write_audit", new_callable=AsyncMock):
                        result = await setup_operator_submit(
                            mock_request,
                            username="admin",
                            password="password123",
                            confirm_password="password123",
                            csrf_protect=mock_csrf,
                        )

        assert result.status_code == 302
        assert result.headers["location"] == "/setup/system"

    @pytest.mark.asyncio
    async def test_post_when_operator_exists_shows_confirmation(self):
        """POST when operator exists returns 200 with confirmation, no insert."""
        mock_request = MagicMock()

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_templates.TemplateResponse.return_value = mock_response

        mock_conn = AsyncMock()
        mock_conn.fetchval.return_value = 1  # Operator already exists
        mock_conn.fetchrow.return_value = {"username": "existing_admin"}

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")
        mock_csrf.set_csrf_cookie = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                with patch("central.gui.routes.write_audit", new_callable=AsyncMock) as mock_audit:
                    result = await setup_operator_submit(
                        mock_request,
                        username="newadmin",
                        password="password123",
                        confirm_password="password123",
                        csrf_protect=mock_csrf,
                    )

        # Should return 200, not 500 or redirect
        assert result.status_code == 200

        # Should render confirmation state
        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert context["existing_operator"] == {"username": "existing_admin"}

        # Should NOT call write_audit (no insert happened)
        mock_audit.assert_not_called()


class TestSetupSystemForm:
    """Test system settings form (step 2)."""

    @pytest.mark.asyncio
    async def test_unauthenticated_redirects_to_operator(self):
        """GET /setup/system without auth redirects to /setup/operator."""
        mock_request = MagicMock()
        mock_request.state.operator = None

        mock_csrf = MagicMock()

        result = await setup_system_form(mock_request, mock_csrf)
        assert result.status_code == 302
        assert result.headers["location"] == "/setup/operator"

    @pytest.mark.asyncio
    async def test_authenticated_returns_form(self):
        """GET /setup/system with auth returns the form."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock()

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {
            "map_tile_url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            "map_attribution": "&copy; OpenStreetMap contributors",
        }

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")
        mock_csrf.set_csrf_cookie = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await setup_system_form(mock_request, mock_csrf)

        mock_templates.TemplateResponse.assert_called_once()


class TestSetupSystemSubmit:
    """Test system settings submission."""

    @pytest.mark.asyncio
    async def test_missing_placeholders_shows_error(self):
        """POST without {z},{x},{y} placeholders shows error."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")

        form_data = MagicMock()
        form_data.get = lambda k, default="": {
            "map_tile_url": "https://example.com/tiles",
            "map_attribution": "Test",
        }.get(k, default)
        mock_request.form = AsyncMock(return_value=form_data)

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock()

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {
            "map_tile_url": "",
            "map_attribution": "",
        }

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")
        mock_csrf.set_csrf_cookie = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await setup_system_submit(mock_request, mock_csrf)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert "map_tile_url" in context["errors"]

    @pytest.mark.asyncio
    async def test_valid_updates_and_redirects(self):
        """POST with valid data updates system and redirects to /setup/keys."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")

        form_data = MagicMock()
        form_data.get = lambda k, default="": {
            "map_tile_url": "https://example.com/{z}/{x}/{y}.png",
            "map_attribution": "Test Attribution",
        }.get(k, default)
        mock_request.form = AsyncMock(return_value=form_data)

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {
            "map_tile_url": "old_url",
            "map_attribution": "old_attr",
        }
        mock_conn.execute = AsyncMock()

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            with patch("central.gui.routes.write_audit", new_callable=AsyncMock):
                result = await setup_system_submit(mock_request, mock_csrf)

        assert result.status_code == 302
        assert result.headers["location"] == "/setup/keys"


class TestSetupKeysForm:
    """Test API keys form (step 3)."""

    @pytest.mark.asyncio
    async def test_unauthenticated_redirects_to_operator(self):
        """GET /setup/keys without auth redirects to /setup/operator."""
        mock_request = MagicMock()
        mock_request.state.operator = None

        mock_csrf = MagicMock()

        result = await setup_keys_form(mock_request, mock_csrf)
        assert result.status_code == 302
        assert result.headers["location"] == "/setup/operator"


class TestSetupKeysSubmit:
    """Test API keys submission."""

    @pytest.mark.asyncio
    async def test_next_action_redirects_to_adapters(self):
        """POST with action=next redirects to /setup/adapters."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")

        form_data = MagicMock()
        form_data.get = lambda k, default="": {"action": "next"}.get(k, default)
        mock_request.form = AsyncMock(return_value=form_data)

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()

        # No need to mock get_pool since action="next" returns before it's called
        result = await setup_keys_submit(mock_request, mock_csrf)
        assert result.status_code == 302
        assert result.headers["location"] == "/setup/adapters"

    @pytest.mark.asyncio
    async def test_add_key_creates_and_rerenders(self):
        """POST with action=add creates key and re-renders with success."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")

        form_data = MagicMock()
        form_data.get = lambda k, default="": {
            "action": "add",
            "alias": "testkey",
            "plaintext_key": "secret123",
        }.get(k, default)
        mock_request.form = AsyncMock(return_value=form_data)

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock()

        mock_conn = AsyncMock()
        mock_conn.fetchrow.side_effect = [
            None,  # No existing key
            {"created_at": datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)},
        ]
        mock_conn.fetch.side_effect = [
            [],  # First list
            [{"alias": "testkey", "created_at": datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)}],  # After insert
        ]

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")
        mock_csrf.set_csrf_cookie = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                with patch("central.crypto.encrypt", return_value=b"encrypted"):
                    with patch("central.gui.routes.write_audit", new_callable=AsyncMock):
                        result = await setup_keys_submit(mock_request, mock_csrf)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert context["success"] == "API key 'testkey' added successfully."


class TestSetupAdaptersForm:
    """Test adapters configuration form (step 4)."""

    @pytest.mark.asyncio
    async def test_unauthenticated_redirects_to_operator(self):
        """GET /setup/adapters without auth redirects to /setup/operator."""
        mock_request = MagicMock()
        mock_request.state.operator = None

        mock_csrf = MagicMock()

        result = await setup_adapters_form(mock_request, mock_csrf)
        assert result.status_code == 302
        assert result.headers["location"] == "/setup/operator"


class TestSetupFinishForm:
    """Test finish page (step 5)."""

    @pytest.mark.asyncio
    async def test_unauthenticated_redirects_to_operator(self):
        """GET /setup/finish without auth redirects to /setup/operator."""
        mock_request = MagicMock()
        mock_request.state.operator = None

        mock_csrf = MagicMock()

        result = await setup_finish_form(mock_request, mock_csrf)
        assert result.status_code == 302
        assert result.headers["location"] == "/setup/operator"

    @pytest.mark.asyncio
    async def test_authenticated_shows_summary(self):
        """GET /setup/finish with auth shows summary."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock()

        mock_conn = AsyncMock()
        mock_conn.fetchval.side_effect = [1, 2]  # 1 operator, 2 keys
        mock_conn.fetchrow.return_value = {"map_tile_url": "https://example.com/{z}/{x}/{y}.png"}
        mock_conn.fetch.return_value = [
            {"name": "nws", "enabled": True, "cadence_s": 300},
            {"name": "firms", "enabled": False, "cadence_s": 600},
        ]

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")
        mock_csrf.set_csrf_cookie = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await setup_finish_form(mock_request, mock_csrf)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert context["operator_count"] == 1
        assert context["key_count"] == 2
        assert len(context["adapters"]) == 2


class TestSetupFinishSubmit:
    """Test setup completion."""

    @pytest.mark.asyncio
    async def test_marks_setup_complete_and_redirects(self):
        """POST /setup/finish marks setup_complete=true and redirects to /."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            with patch("central.gui.routes.write_audit", new_callable=AsyncMock) as mock_audit:
                result = await setup_finish_submit(mock_request, mock_csrf)

        assert result.status_code == 302
        assert result.headers["location"] == "/"
        mock_conn.execute.assert_called_once()
        mock_audit.assert_called_once()


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
    async def test_redirects_base_setup_to_wizard_step(self):
        """SetupGateMiddleware redirects /setup to appropriate wizard step."""
        from starlette.testclient import TestClient
        from fastapi import FastAPI

        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value={"setup_complete": False})
        mock_conn.fetchval = AsyncMock(return_value=0)  # No operators
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=mock_conn)

        with patch("central.gui.middleware.get_pool", return_value=mock_pool):
            app = FastAPI()

            @app.get("/setup")
            async def setup():
                return {"message": "base setup"}

            @app.get("/setup/operator")
            async def setup_operator():
                return {"message": "operator"}

            app.add_middleware(SetupGateMiddleware)
            client = TestClient(app, follow_redirects=False)

            response = client.get("/setup")
            assert response.status_code == 302
            assert response.headers["location"] == "/setup/operator"

    @pytest.mark.asyncio
    async def test_redirects_login_to_setup_when_incomplete(self):
        """SetupGateMiddleware redirects /login to /setup when setup_complete=False."""
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

            @app.get("/login")
            async def login():
                return {"message": "login"}

            @app.get("/setup")
            async def setup():
                return {"message": "setup"}

            app.add_middleware(SetupGateMiddleware)
            client = TestClient(app, follow_redirects=False)

            response = client.get("/login")
            assert response.status_code == 302
            assert response.headers["location"] == "/setup"

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
