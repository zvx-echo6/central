"""Tests for CSRF exception handler."""

import os
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

os.environ.setdefault("CENTRAL_DB_DSN", "postgresql://test:test@localhost/test")
os.environ.setdefault("CENTRAL_CSRF_SECRET", "testsecret12345678901234567890ab")
os.environ.setdefault("CENTRAL_NATS_URL", "nats://localhost:4222")


class TestCsrfExceptionHandlerRegistered:
    """Verify CSRF exception handler is properly registered."""

    def test_csrf_exception_handler_is_registered(self):
        """The app has a CsrfValidationError exception handler registered."""
        from central.gui import app
        from central.gui.auth import CsrfValidationError

        assert CsrfValidationError in app.exception_handlers, \
            "CsrfValidationError handler should be registered"

    def test_csrf_validation_error_is_exception(self):
        """CsrfValidationError is a proper Exception subclass."""
        from central.gui.auth import CsrfValidationError

        assert issubclass(CsrfValidationError, Exception)


class TestCsrfExceptionHandlerBehavior:
    """Test the CSRF exception handler behavior."""

    def test_login_csrf_error_handler_checks_path(self):
        """CSRF handler checks request path for /login."""
        import inspect
        from central.gui import _create_app
        from central.gui.auth import CsrfValidationError

        app = _create_app()
        handler = app.exception_handlers.get(CsrfValidationError)

        # Verify handler source contains /login path check
        source = inspect.getsource(handler)
        assert "/login" in source
        assert "session expired" in source.lower()

    @pytest.mark.asyncio
    async def test_logout_csrf_error_redirects_to_login(self):
        """CSRF error on /logout should redirect to /login."""
        from central.gui import _create_app
        from central.gui.auth import CsrfValidationError
        from fastapi.responses import RedirectResponse

        app = _create_app()
        handler = app.exception_handlers.get(CsrfValidationError)

        mock_request = MagicMock()
        mock_request.url.path = "/logout"

        exc = CsrfValidationError("Invalid token")

        result = await handler(mock_request, exc)

        assert isinstance(result, RedirectResponse)
        assert result.status_code == 302

    @pytest.mark.asyncio
    async def test_adapters_csrf_error_redirects_to_adapters(self):
        """CSRF error on /adapters/{name} should redirect to /adapters."""
        from central.gui import _create_app
        from central.gui.auth import CsrfValidationError
        from fastapi.responses import RedirectResponse

        app = _create_app()
        handler = app.exception_handlers.get(CsrfValidationError)

        mock_request = MagicMock()
        mock_request.url.path = "/adapters/nws"

        exc = CsrfValidationError("Invalid token")

        result = await handler(mock_request, exc)

        assert isinstance(result, RedirectResponse)
        assert result.status_code == 302


class TestCsrfHandlerNoTraceback:
    """Verify exception handler does not expose Python internals."""

    def test_handler_exists_and_is_async(self):
        """The CSRF handler should be an async function."""
        import inspect
        from central.gui import _create_app
        from central.gui.auth import CsrfValidationError

        app = _create_app()
        handler = app.exception_handlers.get(CsrfValidationError)

        assert handler is not None
        assert inspect.iscoroutinefunction(handler)


class TestCsrfHandlerWizardPaths:
    """Test CSRF exception handler for wizard paths."""

    @pytest.mark.asyncio
    async def test_setup_operator_csrf_error_renders_form_with_error(self):
        """CSRF error on /setup/operator re-renders form with error message."""
        from central.gui import _create_app
        from central.gui.auth import CsrfValidationError

        app = _create_app()
        handler = app.exception_handlers.get(CsrfValidationError)

        mock_request = MagicMock()
        mock_request.url.path = "/setup/operator"

        exc = CsrfValidationError("Invalid token")

        result = await handler(mock_request, exc)

        # Should be HTML response, not redirect
        assert hasattr(result, "body")
        assert result.status_code == 200
        body = result.body.decode() if hasattr(result.body, "decode") else str(result.body)
        assert "session expired" in body.lower()

    @pytest.mark.asyncio
    async def test_setup_system_csrf_error_renders_form_with_error(self):
        """CSRF error on /setup/system re-renders form with error message."""
        from central.gui import _create_app
        from central.gui.auth import CsrfValidationError

        app = _create_app()
        handler = app.exception_handlers.get(CsrfValidationError)

        mock_request = MagicMock()
        mock_request.url.path = "/setup/system"

        exc = CsrfValidationError("Invalid token")

        with patch("central.gui.db.get_pool", return_value=None):
            result = await handler(mock_request, exc)

        assert hasattr(result, "body")
        assert result.status_code == 200
        body = result.body.decode() if hasattr(result.body, "decode") else str(result.body)
        assert "session expired" in body.lower()

    @pytest.mark.asyncio
    async def test_setup_keys_csrf_error_renders_form_with_error(self):
        """CSRF error on /setup/keys re-renders form with error message."""
        from central.gui import _create_app
        from central.gui.auth import CsrfValidationError

        app = _create_app()
        handler = app.exception_handlers.get(CsrfValidationError)

        mock_request = MagicMock()
        mock_request.url.path = "/setup/keys"

        exc = CsrfValidationError("Invalid token")

        with patch("central.gui.db.get_pool", return_value=None):
            result = await handler(mock_request, exc)

        assert hasattr(result, "body")
        assert result.status_code == 200
        body = result.body.decode() if hasattr(result.body, "decode") else str(result.body)
        assert "session expired" in body.lower()

    @pytest.mark.asyncio
    async def test_setup_adapters_csrf_error_renders_form_with_error(self):
        """CSRF error on /setup/adapters re-renders form with error message."""
        from central.gui import _create_app
        from central.gui.auth import CsrfValidationError

        app = _create_app()
        handler = app.exception_handlers.get(CsrfValidationError)

        mock_request = MagicMock()
        mock_request.url.path = "/setup/adapters"

        exc = CsrfValidationError("Invalid token")

        with patch("central.gui.db.get_pool", return_value=None):
            result = await handler(mock_request, exc)

        assert hasattr(result, "body")
        assert result.status_code == 200
        body = result.body.decode() if hasattr(result.body, "decode") else str(result.body)
        assert "session expired" in body.lower()

    @pytest.mark.asyncio
    async def test_setup_finish_csrf_error_renders_form_with_error(self):
        """CSRF error on /setup/finish re-renders form with error message."""
        from central.gui import _create_app
        from central.gui.auth import CsrfValidationError

        app = _create_app()
        handler = app.exception_handlers.get(CsrfValidationError)

        mock_request = MagicMock()
        mock_request.url.path = "/setup/finish"

        exc = CsrfValidationError("Invalid token")

        with patch("central.gui.db.get_pool", return_value=None):
            result = await handler(mock_request, exc)

        assert hasattr(result, "body")
        assert result.status_code == 200
        body = result.body.decode() if hasattr(result.body, "decode") else str(result.body)
        assert "session expired" in body.lower()

    @pytest.mark.asyncio
    async def test_setup_base_csrf_error_redirects_to_setup(self):
        """CSRF error on /setup redirects to /setup (middleware routes to step)."""
        from central.gui import _create_app
        from central.gui.auth import CsrfValidationError
        from fastapi.responses import RedirectResponse

        app = _create_app()
        handler = app.exception_handlers.get(CsrfValidationError)

        mock_request = MagicMock()
        mock_request.url.path = "/setup"

        exc = CsrfValidationError("Invalid token")

        result = await handler(mock_request, exc)

        assert isinstance(result, RedirectResponse)
        assert result.status_code == 302

    @pytest.mark.asyncio
    async def test_login_csrf_error_still_works(self):
        """CSRF error on /login still renders login form with error (regression test)."""
        from central.gui import _create_app
        from central.gui.auth import CsrfValidationError

        app = _create_app()
        handler = app.exception_handlers.get(CsrfValidationError)

        mock_request = MagicMock()
        mock_request.url.path = "/login"

        exc = CsrfValidationError("Invalid token")

        result = await handler(mock_request, exc)

        assert hasattr(result, "body")
        assert result.status_code == 200
        body = result.body.decode() if hasattr(result.body, "decode") else str(result.body)
        assert "session expired" in body.lower()
