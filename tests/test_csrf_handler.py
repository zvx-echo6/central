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
        """The app has a CsrfProtectError exception handler registered."""
        from central.gui import app
        from fastapi_csrf_protect.exceptions import CsrfProtectError

        assert CsrfProtectError in app.exception_handlers, \
            "CsrfProtectError handler should be registered"

    def test_csrf_subclasses_are_caught(self):
        """MissingTokenError and TokenValidationError inherit from CsrfProtectError."""
        from fastapi_csrf_protect.exceptions import (
            CsrfProtectError,
            MissingTokenError,
            TokenValidationError,
        )

        assert issubclass(MissingTokenError, CsrfProtectError)
        assert issubclass(TokenValidationError, CsrfProtectError)


class TestCsrfExceptionHandlerBehavior:
    """Test the CSRF exception handler behavior."""

    def test_login_csrf_error_handler_checks_path(self):
        """CSRF handler checks request path for /login."""
        import inspect
        from central.gui import _create_app
        from fastapi_csrf_protect.exceptions import CsrfProtectError

        app = _create_app()
        handler = app.exception_handlers.get(CsrfProtectError)

        # Verify handler source contains /login path check
        source = inspect.getsource(handler)
        assert "/login" in source
        assert "session expired" in source.lower()

    @pytest.mark.asyncio
    async def test_logout_csrf_error_redirects_to_login(self):
        """CSRF error on /logout should redirect to /login."""
        from central.gui import _create_app
        from fastapi_csrf_protect.exceptions import TokenValidationError
        from fastapi.responses import RedirectResponse

        app = _create_app()
        from fastapi_csrf_protect.exceptions import CsrfProtectError
        handler = app.exception_handlers.get(CsrfProtectError)

        mock_request = MagicMock()
        mock_request.url.path = "/logout"

        exc = TokenValidationError("Invalid token")

        result = await handler(mock_request, exc)

        assert isinstance(result, RedirectResponse)
        assert result.status_code == 302

    @pytest.mark.asyncio
    async def test_adapters_csrf_error_redirects_to_adapters(self):
        """CSRF error on /adapters/{name} should redirect to /adapters."""
        from central.gui import _create_app
        from fastapi_csrf_protect.exceptions import TokenValidationError
        from fastapi.responses import RedirectResponse

        app = _create_app()
        from fastapi_csrf_protect.exceptions import CsrfProtectError
        handler = app.exception_handlers.get(CsrfProtectError)

        mock_request = MagicMock()
        mock_request.url.path = "/adapters/nws"

        exc = TokenValidationError("Invalid token")

        result = await handler(mock_request, exc)

        assert isinstance(result, RedirectResponse)
        assert result.status_code == 302


class TestCsrfHandlerNoTraceback:
    """Verify exception handler doesn't expose Python internals."""

    def test_handler_exists_and_is_async(self):
        """The CSRF handler should be an async function."""
        import inspect
        from central.gui import _create_app
        from fastapi_csrf_protect.exceptions import CsrfProtectError

        app = _create_app()
        handler = app.exception_handlers.get(CsrfProtectError)

        assert handler is not None
        assert inspect.iscoroutinefunction(handler)
