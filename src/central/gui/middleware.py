"""Middleware for Central GUI."""

import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response

from central.gui.auth import get_session
from central.gui.db import get_pool

logger = logging.getLogger(__name__)

# Paths that don't require setup to be complete
SETUP_EXEMPT_PATHS = {"/setup", "/health"}
SETUP_EXEMPT_PREFIXES = ("/static/",)

# Paths that don't require authentication
AUTH_EXEMPT_PATHS = {"/setup", "/login", "/health"}
AUTH_EXEMPT_PREFIXES = ("/static/",)


def _is_exempt(path: str, exempt_paths: set, exempt_prefixes: tuple) -> bool:
    """Check if a path is exempt from a check."""
    if path in exempt_paths:
        return True
    for prefix in exempt_prefixes:
        if path.startswith(prefix):
            return True
    return False


class SetupGateMiddleware(BaseHTTPMiddleware):
    """Redirect to /setup if setup is not complete."""

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path

        # Check setup status from database
        pool = get_pool()
        if pool is None:
            # Pool not initialized yet
            return await call_next(request)

        setup_complete = False
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT setup_complete FROM config.system WHERE id = true"
                )
                setup_complete = row["setup_complete"] if row else False
        except Exception:
            logger.warning("Failed to check setup status", exc_info=True)
            # On error, allow the request through
            return await call_next(request)

        if not setup_complete:
            # Setup not complete - only allow exempt paths
            if not _is_exempt(path, SETUP_EXEMPT_PATHS, SETUP_EXEMPT_PREFIXES):
                return RedirectResponse(url="/setup", status_code=302)
        else:
            # Setup complete - redirect /setup to /
            if path == "/setup":
                return RedirectResponse(url="/", status_code=302)

        return await call_next(request)


class SessionMiddleware(BaseHTTPMiddleware):
    """Load session from cookie and attach operator to request.state."""

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path

        # Initialize operator to None
        request.state.operator = None

        # Try to load session from cookie
        session_token = request.cookies.get("central_session")
        if session_token:
            pool = get_pool()
            if pool is not None:
                try:
                    async with pool.acquire() as conn:
                        operator = await get_session(conn, session_token)
                        request.state.operator = operator
                except Exception:
                    logger.warning("Failed to load session", exc_info=True)
                    request.state.operator = None

        # Check if auth is required
        if not _is_exempt(path, AUTH_EXEMPT_PATHS, AUTH_EXEMPT_PREFIXES):
            if request.state.operator is None:
                return RedirectResponse(url="/login", status_code=302)

        return await call_next(request)
