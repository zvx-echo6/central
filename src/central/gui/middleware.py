"""Middleware for Central GUI."""

import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response

from central.gui.auth import get_session
from central.gui.db import get_pool

logger = logging.getLogger(__name__)

# Paths that don't require setup to be complete
SETUP_EXEMPT_PREFIXES = ("/static/", "/setup", "/api/traffic/flow/")

# Paths that don't require authentication
AUTH_EXEMPT_PATHS = {"/setup/operator", "/login", "/health"}
AUTH_EXEMPT_PREFIXES = ("/static/", "/setup/", "/api/traffic/flow/")

# Browser-noise paths that trigger CSRF race conditions
BROWSER_NOISE_PATHS = {
    "/favicon.ico",
    "/apple-touch-icon.png",
    "/apple-touch-icon-precomposed.png",
    "/robots.txt",
}


def _is_exempt(path: str, exempt_paths: set, exempt_prefixes: tuple) -> bool:
    """Check if a path is exempt from a check."""
    if path in exempt_paths:
        return True
    for prefix in exempt_prefixes:
        if path.startswith(prefix):
            return True
    return False


def _get_wizard_redirect_from_cookie(request: Request, csrf_secret: str) -> str:
    """Determine wizard redirect step from cookie state."""
    from central.gui.wizard import get_wizard_state, get_step_route

    state = get_wizard_state(request, csrf_secret)
    if state is None:
        return "/setup/operator"
    return get_step_route(state.wizard_step)


class SetupGateMiddleware(BaseHTTPMiddleware):
    """Redirect to /setup if setup is not complete."""

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path

        # Short-circuit browser-noise requests that cause CSRF races
        if path in BROWSER_NOISE_PATHS:
            return Response(status_code=204)

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
            # Setup not complete - only allow setup paths and static/health
            if path.startswith("/setup"):
                # Allow all /setup/* paths
                # But /setup with no subpath should redirect to appropriate step
                if path == "/setup" or path == "/setup/":
                    try:
                        from central.bootstrap_config import get_settings
                        settings = get_settings()
                        redirect_step = _get_wizard_redirect_from_cookie(
                            request, settings.csrf_secret
                        )
                        return RedirectResponse(url=redirect_step, status_code=302)
                    except Exception:
                        logger.warning("Failed to determine wizard step", exc_info=True)
                        return RedirectResponse(url="/setup/operator", status_code=302)
                return await call_next(request)
            elif path == "/health" or path.startswith("/static/"):
                return await call_next(request)
            elif path == "/login":
                # During setup, login redirects to /setup
                return RedirectResponse(url="/setup", status_code=302)
            else:
                # All other paths redirect to /setup
                return RedirectResponse(url="/setup", status_code=302)
        else:
            # Setup complete - redirect /setup* to /
            if path.startswith("/setup"):
                return RedirectResponse(url="/", status_code=302)

        return await call_next(request)


class SessionMiddleware(BaseHTTPMiddleware):
    """Load session from cookie and attach operator + csrf_token to request.state."""

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path

        # Short-circuit browser-noise requests (already handled by SetupGateMiddleware,
        # but this protects if middleware order changes)
        if path in BROWSER_NOISE_PATHS:
            return Response(status_code=204)

        # Initialize state
        request.state.operator = None
        request.state.csrf_token = None

        # Try to load session from cookie
        session_token = request.cookies.get("central_session")
        if session_token:
            pool = get_pool()
            if pool is not None:
                try:
                    async with pool.acquire() as conn:
                        result = await get_session(conn, session_token)
                        if result is not None:
                            operator, csrf_token = result
                            request.state.operator = operator
                            request.state.csrf_token = csrf_token
                except Exception:
                    logger.warning("Failed to load session", exc_info=True)
                    request.state.operator = None
                    request.state.csrf_token = None

        # Check if auth is required - setup paths are exempt during wizard
        if not _is_exempt(path, AUTH_EXEMPT_PATHS, AUTH_EXEMPT_PREFIXES):
            if request.state.operator is None:
                return RedirectResponse(url="/login", status_code=302)

        return await call_next(request)
