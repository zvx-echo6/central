"""Tests for GUI scaffold."""

from fastapi.testclient import TestClient

from central.gui import app


client = TestClient(app)


class TestHealthEndpoint:
    """Tests for /health endpoint."""

    def test_health_returns_200(self):
        """Health endpoint returns 200 OK."""
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_returns_status_ok(self):
        """Health endpoint returns status ok JSON."""
        response = client.get("/health")
        assert response.json() == {"status": "ok"}


class TestIndexEndpoint:
    """Tests for / endpoint."""

    def test_index_returns_200(self):
        """Index endpoint returns 200 OK."""
        response = client.get("/")
        assert response.status_code == 200

    def test_index_returns_html(self):
        """Index endpoint returns HTML content."""
        response = client.get("/")
        assert "text/html" in response.headers["content-type"]

    def test_index_contains_placeholder(self):
        """Index page contains the placeholder text."""
        response = client.get("/")
        assert "Central" in response.text
        assert "coming soon" in response.text.lower()
