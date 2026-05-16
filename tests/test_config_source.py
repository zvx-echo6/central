"""Tests for configuration source abstraction."""

import base64
import os
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio

from central.config_source import (
    ConfigSource,
    DbConfigSource,
)
from central.crypto import KEY_SIZE, clear_key_cache

# Test database DSN
TEST_DB_DSN = os.environ.get(
    "CENTRAL_TEST_DB_DSN",
    "postgresql://central_test:testpass@localhost/central_test",
)


@pytest.fixture(scope="session")
def master_key_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create a master key file for the test session."""
    key = os.urandom(KEY_SIZE)
    key_path = tmp_path_factory.mktemp("keys") / "master.key"
    key_path.write_text(base64.b64encode(key).decode())
    return key_path


@pytest.fixture(autouse=True)
def setup_master_key(master_key_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure master key path for all tests."""
    clear_key_cache()
    monkeypatch.setenv("CENTRAL_DB_DSN", TEST_DB_DSN)
    monkeypatch.setenv("CENTRAL_MASTER_KEY_PATH", str(master_key_path))


@pytest_asyncio.fixture
async def db_conn() -> asyncpg.Connection:
    """Get a direct database connection for setup/teardown."""
    conn = await asyncpg.connect(TEST_DB_DSN)
    yield conn
    await conn.close()


@pytest_asyncio.fixture
async def clean_config_schema(db_conn: asyncpg.Connection) -> None:
    """Ensure config schema exists and is clean before each test."""
    await db_conn.execute("CREATE SCHEMA IF NOT EXISTS config")
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS config.adapters (
            name           TEXT PRIMARY KEY,
            enabled        BOOLEAN NOT NULL DEFAULT true,
            cadence_s      INTEGER NOT NULL,
            settings       JSONB NOT NULL DEFAULT '{}'::jsonb,
            paused_at      TIMESTAMPTZ,
            updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    await db_conn.execute("DELETE FROM config.adapters")


class TestDbConfigSource:
    """Tests for database-backed config source."""

    @pytest_asyncio.fixture
    async def db_source(self, clean_config_schema: None) -> DbConfigSource:
        """Create a DbConfigSource for testing."""
        source = await DbConfigSource.create(TEST_DB_DSN)
        yield source
        await source.close()

    @pytest.mark.asyncio
    async def test_list_enabled_adapters_empty(self, db_source: DbConfigSource) -> None:
        """list_enabled_adapters returns empty list when no adapters."""
        adapters = await db_source.list_enabled_adapters()
        assert adapters == []

    @pytest.mark.asyncio
    async def test_list_enabled_adapters(
        self, db_source: DbConfigSource, db_conn: asyncpg.Connection
    ) -> None:
        """list_enabled_adapters returns only enabled, non-paused adapters."""
        # Insert test adapters
        await db_conn.execute("""
            INSERT INTO config.adapters (name, enabled, cadence_s, settings)
            VALUES
                ('enabled_adapter', true, 60, '{"key": "value"}'::jsonb),
                ('disabled_adapter', false, 60, '{}'::jsonb),
                ('paused_adapter', true, 60, '{}'::jsonb)
        """)
        await db_conn.execute("""
            UPDATE config.adapters
            SET paused_at = now()
            WHERE name = 'paused_adapter'
        """)

        adapters = await db_source.list_enabled_adapters()

        assert len(adapters) == 1
        assert adapters[0].name == "enabled_adapter"

    @pytest.mark.asyncio
    async def test_get_adapter(
        self, db_source: DbConfigSource, db_conn: asyncpg.Connection
    ) -> None:
        """get_adapter returns correct adapter config."""
        await db_conn.execute("""
            INSERT INTO config.adapters (name, enabled, cadence_s, settings)
            VALUES ('test_adapter', true, 120, '{"states": ["ID"]}'::jsonb)
        """)

        adapter = await db_source.get_adapter("test_adapter")

        assert adapter is not None
        assert adapter.name == "test_adapter"
        assert adapter.cadence_s == 120
        assert adapter.settings == {"states": ["ID"]}

    @pytest.mark.asyncio
    async def test_get_nonexistent_adapter(self, db_source: DbConfigSource) -> None:
        """get_adapter returns None for nonexistent adapter."""
        adapter = await db_source.get_adapter("does_not_exist")
        assert adapter is None

    @pytest.mark.asyncio
    async def test_implements_protocol(self, db_source: DbConfigSource) -> None:
        """DbConfigSource implements ConfigSource protocol."""
        assert isinstance(db_source, ConfigSource)
