"""Central GUI — FastAPI + Jinja2 + HTMX."""

from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from central.gui.routes import router

# Template and static directories
GUI_DIR = Path(__file__).parent
TEMPLATES_DIR = GUI_DIR / "templates"
STATIC_DIR = GUI_DIR / "static"

# Jinja2 templates instance (shared with routes)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Central",
        description="Central Data Hub GUI",
        docs_url=None,  # Disable Swagger UI for now
        redoc_url=None,  # Disable ReDoc for now
    )

    # Mount static files if directory exists and has content
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    # Include routes
    app.include_router(router)

    return app


# Application instance
app = create_app()


def main() -> None:
    """Entry point for central-gui console script."""
    uvicorn.run(app, host="127.0.0.1", port=8000)
