"""Central GUI — FastAPI + Jinja2 + HTMX."""

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

logger = logging.getLogger(__name__)

# Template and static directories
GUI_DIR = Path(__file__).parent
TEMPLATES_DIR = GUI_DIR / "templates"
STATIC_DIR = GUI_DIR / "static"

# Jinja2 templates instance (shared with routes)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Shutdown event and cleanup task
_shutdown_event: asyncio.Event | None = None
_cleanup_task: asyncio.Task | None = None

# Lazy app singleton
_app: FastAPI | None = None


def _configure_csrf() -> None:
    """Configure CSRF protection. Must be called before app starts."""
    from fastapi_csrf_protect import CsrfProtect
    from pydantic import BaseModel
    from central.bootstrap_config import get_settings

    class CsrfSettings(BaseModel):
        secret_key: str
        token_location: str = "body"
        token_key: str = "csrf_token"

    @CsrfProtect.load_config
    def get_csrf_config():
        settings = get_settings()
        return CsrfSettings(secret_key=settings.csrf_secret)


async def _session_cleanup_loop() -> None:
    """Periodically clean up expired sessions."""
    global _shutdown_event

    from central.gui.db import get_pool

    if _shutdown_event is None:
        return

    while not _shutdown_event.is_set():
        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=3600)
        except asyncio.TimeoutError:
            try:
                pool = get_pool()
                if pool:
                    async with pool.acquire() as conn:
                        result = await conn.execute(
                            "DELETE FROM config.sessions WHERE expires_at < now()"
                        )
                        deleted = result.split()[-1] if result else "0"
                        if int(deleted) > 0:
                            logger.info("Session cleanup", extra={"deleted": deleted})
            except Exception:
                logger.warning("Session cleanup failed", exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    global _shutdown_event, _cleanup_task

    from central.bootstrap_config import get_settings
    from central.gui.db import close_pool, init_pool
    from central.gui.nats import close_nats, init_nats

    settings = get_settings()

    # Initialize database pool
    await init_pool(settings.db_dsn)

    # Initialize NATS connection
    await init_nats(settings.nats_url)

    # Start session cleanup task
    _shutdown_event = asyncio.Event()
    _cleanup_task = asyncio.create_task(_session_cleanup_loop())

    logger.info("Central GUI started")

    yield

    # Shutdown
    if _shutdown_event:
        _shutdown_event.set()
    if _cleanup_task:
        try:
            await asyncio.wait_for(_cleanup_task, timeout=5.0)
        except asyncio.TimeoutError:
            _cleanup_task.cancel()

    await close_nats()
    await close_pool()
    logger.info("Central GUI stopped")


def _create_app() -> FastAPI:
    """Create the FastAPI application."""
    from central.gui.middleware import SessionMiddleware, SetupGateMiddleware
    from central.gui.routes import router

    # Configure CSRF before creating app
    _configure_csrf()

    app = FastAPI(
        title="Central GUI",
        lifespan=lifespan,
    )

    # Add middleware (order matters - first added runs last)
    app.add_middleware(SessionMiddleware)
    app.add_middleware(SetupGateMiddleware)

    # Mount static files
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    # Include routes
    app.include_router(router)

    # CSRF exception handler - return friendly error instead of 500
    from fastapi_csrf_protect.exceptions import CsrfProtectError
    from fastapi.responses import RedirectResponse

    @app.exception_handler(CsrfProtectError)
    async def csrf_exception_handler(request, exc: CsrfProtectError):
        from fastapi_csrf_protect import CsrfProtect
        from central.gui.db import get_pool

        csrf_protect = CsrfProtect()
        csrf_token, signed_token = csrf_protect.generate_csrf_tokens()
        error_msg = "Your session expired. Please try again."

        if request.url.path == "/login":
            response = templates.TemplateResponse(
                request=request,
                name="login.html",
                context={"csrf_token": csrf_token, "error": error_msg},
            )
            csrf_protect.set_csrf_cookie(signed_token, response)
            return response

        elif request.url.path == "/setup":
            # /setup is a redirect path now, not a form
            return RedirectResponse("/setup", status_code=302)

        elif request.url.path == "/setup/operator":
            response = templates.TemplateResponse(
                request=request,
                name="setup_operator.html",
                context={"csrf_token": csrf_token, "error": error_msg, "form_data": None},
            )
            csrf_protect.set_csrf_cookie(signed_token, response)
            return response

        elif request.url.path == "/setup/system":
            pool = get_pool()
            system = {
                "map_tile_url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
                "map_attribution": "&copy; OpenStreetMap contributors",
            }
            if pool:
                try:
                    async with pool.acquire() as conn:
                        row = await conn.fetchrow(
                            "SELECT map_tile_url, map_attribution FROM config.system WHERE id = true"
                        )
                        if row:
                            system = {
                                "map_tile_url": row["map_tile_url"],
                                "map_attribution": row["map_attribution"],
                            }
                except Exception:
                    pass
            response = templates.TemplateResponse(
                request=request,
                name="setup_system.html",
                context={
                    "csrf_token": csrf_token,
                    "error": error_msg,
                    "errors": None,
                    "form_data": None,
                    "system": system,
                },
            )
            csrf_protect.set_csrf_cookie(signed_token, response)
            return response

        elif request.url.path == "/setup/keys":
            pool = get_pool()
            keys = []
            if pool:
                try:
                    async with pool.acquire() as conn:
                        rows = await conn.fetch(
                            "SELECT alias, created_at FROM config.api_keys ORDER BY alias"
                        )
                        keys = [{"alias": row["alias"], "created_at": row["created_at"]} for row in rows]
                except Exception:
                    pass
            response = templates.TemplateResponse(
                request=request,
                name="setup_keys.html",
                context={
                    "csrf_token": csrf_token,
                    "keys": keys,
                    "errors": None,
                    "form_data": None,
                    "success": None,
                    "error": error_msg,
                },
            )
            csrf_protect.set_csrf_cookie(signed_token, response)
            return response

        elif request.url.path == "/setup/adapters":
            pool = get_pool()
            adapters = []
            api_keys = []
            tile_url = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
            tile_attribution = "&copy; OpenStreetMap contributors"
            if pool:
                try:
                    async with pool.acquire() as conn:
                        rows = await conn.fetch(
                            "SELECT name, enabled, cadence_s, settings FROM config.adapters ORDER BY name"
                        )
                        for row in rows:
                            settings = row["settings"] or {}
                            adapters.append({
                                "name": row["name"],
                                "enabled": row["enabled"],
                                "cadence_s": row["cadence_s"],
                                "settings": settings,
                            })
                        key_rows = await conn.fetch(
                            "SELECT alias FROM config.api_keys ORDER BY alias"
                        )
                        api_keys = [{"alias": k["alias"]} for k in key_rows]
                        sys_row = await conn.fetchrow(
                            "SELECT map_tile_url, map_attribution FROM config.system WHERE id = true"
                        )
                        if sys_row:
                            tile_url = sys_row["map_tile_url"]
                            tile_attribution = sys_row["map_attribution"]
                except Exception:
                    pass

            # Import helper functions for valid values
            from central.gui.routes import _get_valid_satellites, _get_valid_feeds

            response = templates.TemplateResponse(
                request=request,
                name="setup_adapters.html",
                context={
                    "csrf_token": csrf_token,
                    "adapters": adapters,
                    "api_keys": api_keys,
                    "valid_satellites": _get_valid_satellites(),
                    "valid_feeds": sorted(_get_valid_feeds()),
                    "tile_url": tile_url,
                    "tile_attribution": tile_attribution,
                    "error": error_msg,
                    "errors": None,
                    "form_data": None,
                },
            )
            csrf_protect.set_csrf_cookie(signed_token, response)
            return response

        elif request.url.path == "/setup/finish":
            pool = get_pool()
            operator_count = 0
            key_count = 0
            system = {"map_tile_url": ""}
            adapters = []
            if pool:
                try:
                    async with pool.acquire() as conn:
                        operator_count = await conn.fetchval("SELECT COUNT(*) FROM config.operators")
                        key_count = await conn.fetchval("SELECT COUNT(*) FROM config.api_keys")
                        sys_row = await conn.fetchrow(
                            "SELECT map_tile_url FROM config.system WHERE id = true"
                        )
                        if sys_row:
                            system = {"map_tile_url": sys_row["map_tile_url"]}
                        rows = await conn.fetch(
                            "SELECT name, enabled, cadence_s FROM config.adapters ORDER BY name"
                        )
                        adapters = [
                            {"name": row["name"], "enabled": row["enabled"], "cadence_s": row["cadence_s"]}
                            for row in rows
                        ]
                except Exception:
                    pass
            response = templates.TemplateResponse(
                request=request,
                name="setup_finish.html",
                context={
                    "csrf_token": csrf_token,
                    "operator_count": operator_count,
                    "key_count": key_count,
                    "system": system,
                    "adapters": adapters,
                    "error": error_msg,
                },
            )
            csrf_protect.set_csrf_cookie(signed_token, response)
            return response

        elif request.url.path == "/logout":
            return RedirectResponse("/login", status_code=302)

        elif request.url.path == "/change-password":
            response = templates.TemplateResponse(
                request=request,
                name="change_password.html",
                context={"csrf_token": csrf_token, "error": error_msg},
            )
            csrf_protect.set_csrf_cookie(signed_token, response)
            return response

        elif request.url.path.startswith("/adapters/"):
            # Redirect back to adapters list
            return RedirectResponse("/adapters", status_code=302)

        else:
            # Fallback: redirect to login
            return RedirectResponse("/login", status_code=302)

    return app


def __getattr__(name: str) -> Any:
    """Lazy attribute access for app singleton."""
    global _app
    if name == "app":
        if _app is None:
            _app = _create_app()
        return _app
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def main() -> None:
    """Entry point for central-gui command."""
    import logging.config

    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s %(levelname)s %(name)s: %(message)s",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
            },
        },
        "root": {
            "level": "INFO",
            "handlers": ["console"],
        },
    })

    uvicorn.run(
        "central.gui:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
    )


if __name__ == "__main__":
    main()
