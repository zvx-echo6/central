"""Shared fixtures for auth tests."""

import asyncio
import tempfile
from pathlib import Path
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from central.bootstrap_config import Settings


@pytest.fixture(autouse=True)
def isolate_enrichment_cache(tmp_path, monkeypatch):
    """Redirect the supervisor's enrichment cache off the production path.

    `central.supervisor.ENRICHMENT_CACHE_DB_PATH` defaults to
    /var/lib/central/enrichment_cache.db. Constructing a Supervisor opens it,
    so without this fixture the suite writes to (or, for any user without write
    access to /var/lib/central, fails on) the live cache. Point it at a
    per-test temp dir so no test ever touches the production path.
    """
    import central.supervisor as supervisor_mod

    monkeypatch.setattr(
        supervisor_mod,
        "ENRICHMENT_CACHE_DB_PATH",
        tmp_path / "enrichment_cache.db",
    )


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_settings():
    """Create mock settings for testing."""
    return Settings(
        db_dsn="postgresql://test:test@localhost/test",
        nats_url="nats://localhost:4222",
        csrf_secret="test-csrf-secret-for-testing-only-32chars",
    )


@pytest.fixture
def mock_pool():
    """Create a mock database pool."""
    pool = MagicMock()
    pool.acquire = MagicMock()
    pool.close = AsyncMock()
    return pool


@pytest.fixture
def mock_conn():
    """Create a mock database connection."""
    conn = MagicMock()
    conn.fetchrow = AsyncMock()
    conn.fetchval = AsyncMock()
    conn.execute = AsyncMock()
    return conn


# CSRF fixtures for route tests

@pytest.fixture
def bypass_pre_auth_csrf():
    """Patch pre-auth CSRF validation to always pass.
    
    Use for tests of pre-auth routes: /login, /setup/operator
    """
    with patch("central.gui.routes.validate_pre_auth_csrf", return_value=True):
        with patch("central.gui.routes.generate_pre_auth_csrf", return_value=("test_csrf_token", "test_signed_token")):
            yield


@pytest.fixture
def bypass_session_csrf():
    """Create a mock request with session CSRF properly configured.
    
    Use for tests of authenticated routes that check request.state.csrf_token.
    Returns a configured mock_request.
    """
    request = MagicMock()
    request.state.csrf_token = "test_csrf_token_12345"
    request.state.operator = MagicMock()
    request.state.operator.id = 1
    request.state.operator.username = "testuser"
    
    # Mock form() to return dict with matching CSRF token
    form_data = {"csrf_token": "test_csrf_token_12345"}
    
    async def mock_form():
        return form_data
    
    request.form = mock_form
    request._form_data = form_data  # Allow tests to modify form data
    
    return request


@pytest.fixture
def patch_route_settings():
    """Patch get_settings in routes module."""
    with patch("central.gui.routes.get_settings") as mock:
        mock.return_value.csrf_secret = "test-csrf-secret-for-testing-only-32chars"
        yield mock
