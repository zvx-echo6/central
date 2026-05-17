"""Tests for GUI scaffold."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from central.gui.routes import router


class TestHealthEndpoint:
    """Tests for /health endpoint."""

    def test_health_returns_200(self):
        """Health endpoint returns 200 OK."""
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_returns_status_ok(self):
        """Health endpoint returns status ok JSON."""
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)
        response = client.get("/health")
        assert response.json() == {"status": "ok"}
