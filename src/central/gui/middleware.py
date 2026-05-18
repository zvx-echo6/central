"""Middleware for Central GUI."""

import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response

from central.gui.auth import get_session
from central.gui.db import get_pool

logger = logging.getLogger(__name__)

# Paths that don't require setup to be complete
SETUP_EXEMPT_PREFIXES = ("/static/", "/setup")

# Paths that don't require authentication
AUTH_EXEMPT_PATHS = {"/setup/operator", "/login", "/health"}
AUTH_EXEMPT_PREFIXES = ("/static/",)


def _is_exempt(path: str, exempt_paths: set, exempt_prefixes: tuple) -> bool:
    """Check if a path is exempt from a check."""
    if path in exempt_paths:
        return True
    for prefix in exempt_prefixes:
        if path.startswith(prefix):
            return True
    return False


async def _get_wizard_redirect_step(conn) -> str:
    """Determine which wizard step to redirect to based on DB state."""
    # Check if any operators exist
    op_count = await conn.fetchval("SELECT COUNT(*) FROM config.operators")
    if op_count == 0:
        return "/setup/operator"

    # Check if system settings have been configured (map_tile_url not default)
    sys_row = await conn.fetchrow(
        "SELECT map_tile_url FROM config.system WHERE id = true"
    )
    default_tile = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
    if sys_row is None or sys_row["map_tile_url"] == default_tile:
        return "/setup/system"

    # Keys step is optional, so check adapters have been reviewed
    # We consider adapters reviewed if any adapter has a non-null updated_at
    # (meaning it was explicitly saved during setup)
    adapters_touched = await conn.fetchval(
        "SELECT COUNT(*) FROM config.adapters WHERE updated_at IS NOT NULL"
    )
    if adapters_touched == 0:
        # Go to keys first, then adapters
        return "/setup/keys"

    # All steps done, go to finish
    return "/setup/finish"


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
            # Setup not complete - only allow setup paths and static/health
            if path.startswith("/setup"):
                # Allow all /setup/* paths (handler will enforce auth)
                # But /setup with no subpath should redirect to appropriate step
                if path == "/setup" or path == "/setup/":
                    try:
                        async with pool.acquire() as conn:
                            redirect_step = await _get_wizard_redirect_step(conn)
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
