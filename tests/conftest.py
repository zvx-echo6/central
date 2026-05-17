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
