"""Tests for setup gate middleware."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from starlette.testclient import TestClient
from fastapi import FastAPI

from central.gui.middleware import SetupGateMiddleware


class TestSetupGateMiddleware:
    """Tests for SetupGateMiddleware."""

    @pytest.mark.asyncio
    async def test_allows_setup_subpath_when_incomplete(self):
        """SetupGateMiddleware allows /setup/operator when setup_complete=False."""
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
                return {"message": "operator"}

            app.add_middleware(SetupGateMiddleware)
            client = TestClient(app)

            response = client.get("/setup/operator")
            assert response.status_code == 200
            assert response.json() == {"message": "operator"}

    @pytest.mark.asyncio
    async def test_redirects_setup_base_to_wizard_step(self):
        """SetupGateMiddleware redirects /setup to wizard step when incomplete."""
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
                return {"message": "setup"}

            @app.get("/setup/operator")
            async def setup_operator():
                return {"message": "operator"}

            app.add_middleware(SetupGateMiddleware)
            client = TestClient(app, follow_redirects=False)

            response = client.get("/setup")
            assert response.status_code == 302
            assert response.headers["location"] == "/setup/operator"

    @pytest.mark.asyncio
    async def test_allows_health_when_incomplete(self):
        """SetupGateMiddleware allows /health regardless of setup state."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value={"setup_complete": False})
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=mock_conn)

        with patch("central.gui.middleware.get_pool", return_value=mock_pool):
            app = FastAPI()

            @app.get("/health")
            async def health():
                return {"status": "ok"}

            app.add_middleware(SetupGateMiddleware)
            client = TestClient(app)

            response = client.get("/health")
            assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_redirects_other_routes_when_incomplete(self):
        """SetupGateMiddleware redirects non-setup routes when setup_complete=False."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value={"setup_complete": False})
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=mock_conn)

        with patch("central.gui.middleware.get_pool", return_value=mock_pool):
            app = FastAPI()

            @app.get("/")
            async def index():
                return {"message": "home"}

            @app.get("/setup")
            async def setup():
                return {"message": "setup"}

            app.add_middleware(SetupGateMiddleware)
            client = TestClient(app, follow_redirects=False)

            response = client.get("/")
            assert response.status_code == 302
            assert response.headers["location"] == "/setup"

    @pytest.mark.asyncio
    async def test_allows_all_routes_when_complete(self):
        """SetupGateMiddleware allows all routes when setup_complete=True."""
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

            app.add_middleware(SetupGateMiddleware)
            client = TestClient(app)

            response = client.get("/")
            assert response.status_code == 200
            assert response.json() == {"message": "home"}

    @pytest.mark.asyncio
    async def test_allows_static_when_incomplete(self):
        """SetupGateMiddleware allows /static routes when setup_complete=False."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value={"setup_complete": False})
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=mock_conn)

        with patch("central.gui.middleware.get_pool", return_value=mock_pool):
            app = FastAPI()

            @app.get("/static/test.css")
            async def static():
                return "css"

            app.add_middleware(SetupGateMiddleware)
            client = TestClient(app)

            response = client.get("/static/test.css")
            assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_redirects_setup_when_complete(self):
        """SetupGateMiddleware redirects /setup/* to / when setup_complete=True."""
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

            @app.get("/setup")
            async def setup():
                return {"message": "setup"}

            @app.get("/setup/operator")
            async def setup_operator():
                return {"message": "operator"}

            app.add_middleware(SetupGateMiddleware)
            client = TestClient(app, follow_redirects=False)

            # Both /setup and /setup/operator should redirect to /
            response = client.get("/setup")
            assert response.status_code == 302
            assert response.headers["location"] == "/"

            response = client.get("/setup/operator")
            assert response.status_code == 302
            assert response.headers["location"] == "/"
