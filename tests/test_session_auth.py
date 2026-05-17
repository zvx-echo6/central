"""Tests for session authentication middleware."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone
from starlette.testclient import TestClient
from fastapi import FastAPI, Request

from central.gui.middleware import SessionMiddleware
from central.gui.auth import Operator


class TestSessionMiddleware:
    """Tests for SessionMiddleware."""

    @pytest.mark.asyncio
    async def test_no_cookie_sets_none_on_exempt_path(self):
        """SessionMiddleware sets operator=None when no session cookie on exempt path."""
        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock()

        with patch("central.gui.middleware.get_pool", return_value=mock_pool):
            app = FastAPI()

            @app.get("/health")
            async def health(request: Request):
                return {"operator": getattr(request.state, "operator", "missing")}

            app.add_middleware(SessionMiddleware)
            client = TestClient(app)

            response = client.get("/health")
            assert response.status_code == 200
            assert response.json()["operator"] is None

    @pytest.mark.asyncio
    async def test_valid_cookie_sets_operator_on_exempt_path(self):
        """SessionMiddleware sets operator when valid session cookie on exempt path."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value={
            "id": 1,
            "username": "admin",
            "created_at": datetime.now(timezone.utc),
            "password_changed_at": datetime.now(timezone.utc),
        })
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=mock_conn)

        with patch("central.gui.middleware.get_pool", return_value=mock_pool):
            app = FastAPI()

            @app.get("/health")
            async def health(request: Request):
                op = getattr(request.state, "operator", None)
                if op:
                    return {"username": op.username}
                return {"operator": None}

            app.add_middleware(SessionMiddleware)
            client = TestClient(app, cookies={"central_session": "valid-token"})

            response = client.get("/health")
            assert response.status_code == 200
            assert response.json()["username"] == "admin"

    @pytest.mark.asyncio
    async def test_no_cookie_redirects_on_protected_path(self):
        """SessionMiddleware redirects to /login when no cookie on protected path."""
        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock()

        with patch("central.gui.middleware.get_pool", return_value=mock_pool):
            app = FastAPI()

            @app.get("/")
            async def index(request: Request):
                return {"message": "home"}

            @app.get("/login")
            async def login():
                return {"message": "login"}

            app.add_middleware(SessionMiddleware)
            client = TestClient(app, follow_redirects=False)

            response = client.get("/")
            assert response.status_code == 302
            assert response.headers["location"] == "/login"

    @pytest.mark.asyncio
    async def test_valid_cookie_allows_protected_path(self):
        """SessionMiddleware allows protected path with valid session."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value={
            "id": 1,
            "username": "admin",
            "created_at": datetime.now(timezone.utc),
            "password_changed_at": datetime.now(timezone.utc),
        })
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=mock_conn)

        with patch("central.gui.middleware.get_pool", return_value=mock_pool):
            app = FastAPI()

            @app.get("/")
            async def index(request: Request):
                op = request.state.operator
                return {"message": "home", "user": op.username}

            app.add_middleware(SessionMiddleware)
            client = TestClient(app, cookies={"central_session": "valid-token"})

            response = client.get("/")
            assert response.status_code == 200
            assert response.json()["user"] == "admin"

    @pytest.mark.asyncio
    async def test_invalid_cookie_redirects_on_protected_path(self):
        """SessionMiddleware redirects when session is invalid/expired."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value=None)  # No session found
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=mock_conn)

        with patch("central.gui.middleware.get_pool", return_value=mock_pool):
            app = FastAPI()

            @app.get("/")
            async def index(request: Request):
                return {"operator": getattr(request.state, "operator", "missing")}

            @app.get("/login")
            async def login():
                return {"message": "login"}

            app.add_middleware(SessionMiddleware)
            client = TestClient(app, cookies={"central_session": "expired-token"}, follow_redirects=False)

            response = client.get("/")
            assert response.status_code == 302
            assert response.headers["location"] == "/login"

    @pytest.mark.asyncio
    async def test_middleware_handles_db_error(self):
        """SessionMiddleware handles database errors gracefully on exempt path."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(side_effect=Exception("DB error"))
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=mock_conn)

        with patch("central.gui.middleware.get_pool", return_value=mock_pool):
            app = FastAPI()

            @app.get("/health")
            async def health(request: Request):
                return {"operator": getattr(request.state, "operator", "missing")}

            app.add_middleware(SessionMiddleware)
            client = TestClient(app, cookies={"central_session": "some-token"})

            response = client.get("/health")
            # Should not crash, just set operator to None
            assert response.status_code == 200
            assert response.json()["operator"] is None
