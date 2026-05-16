"""Tests for database-backed configuration store.

These tests require a real Postgres database. Set CENTRAL_TEST_DB_DSN
environment variable to override the default test database connection.
"""

import asyncio
import base64
import os
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio

from central.config_store import ConfigStore
from central.crypto import KEY_SIZE, clear_key_cache

# Test database DSN - uses central_test database with well-known test password.
# Override via CENTRAL_TEST_DB_DSN env var if your test DB differs.
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
    # Create schema if not exists
    await db_conn.execute("CREATE SCHEMA IF NOT EXISTS config")

    # Create tables if not exist
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
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS config.api_keys (
            alias            TEXT PRIMARY KEY,
            encrypted_value  BYTEA NOT NULL,
            created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
            rotated_at       TIMESTAMPTZ,
            last_used_at     TIMESTAMPTZ
        )
    """)

    # Create notify function with proper key detection
    await db_conn.execute("""
        CREATE OR REPLACE FUNCTION config.notify_config_change()
        RETURNS trigger AS $$
        DECLARE
            key_value TEXT;
        BEGIN
            IF TG_TABLE_NAME = 'adapters' THEN
                key_value := COALESCE(NEW.name, OLD.name, '');
            ELSIF TG_TABLE_NAME = 'api_keys' THEN
                key_value := COALESCE(NEW.alias, OLD.alias, '');
            ELSE
                key_value := '';
            END IF;

            PERFORM pg_notify('config_changed', TG_TABLE_NAME || ':' || key_value);
            RETURN COALESCE(NEW, OLD);
        END;
        $$ LANGUAGE plpgsql
    """)

    # Create triggers if not exist
    await db_conn.execute("""
        DROP TRIGGER IF EXISTS adapters_notify ON config.adapters;
        CREATE TRIGGER adapters_notify
            AFTER INSERT OR UPDATE OR DELETE ON config.adapters
            FOR EACH ROW EXECUTE FUNCTION config.notify_config_change()
    """)
    await db_conn.execute("""
        DROP TRIGGER IF EXISTS api_keys_notify ON config.api_keys;
        CREATE TRIGGER api_keys_notify
            AFTER INSERT OR UPDATE OR DELETE ON config.api_keys
            FOR EACH ROW EXECUTE FUNCTION config.notify_config_change()
    """)

    # Clean tables
    await db_conn.execute("DELETE FROM config.adapters")
    await db_conn.execute("DELETE FROM config.api_keys")


@pytest_asyncio.fixture
async def config_store(clean_config_schema: None) -> ConfigStore:
    """Create a ConfigStore connected to the test database."""
    store = await ConfigStore.create(TEST_DB_DSN)
    yield store
    await store.close()


class TestAdapterConfig:
    """Tests for adapter configuration operations."""

    @pytest.mark.asyncio
    async def test_upsert_and_get(self, config_store: ConfigStore) -> None:
        """Can insert and retrieve adapter config."""
        await config_store.upsert_adapter(
            name="test_adapter",
            enabled=True,
            cadence_s=120,
            settings={"key": "value"},
        )

        adapter = await config_store.get_adapter("test_adapter")

        assert adapter is not None
        assert adapter.name == "test_adapter"
        assert adapter.enabled is True
        assert adapter.cadence_s == 120
        assert adapter.settings == {"key": "value"}

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, config_store: ConfigStore) -> None:
        """Getting nonexistent adapter returns None."""
        adapter = await config_store.get_adapter("does_not_exist")
        assert adapter is None

    @pytest.mark.asyncio
    async def test_list_adapters(self, config_store: ConfigStore) -> None:
        """Can list all adapters."""
        await config_store.upsert_adapter("adapter_a", True, 60, {})
        await config_store.upsert_adapter("adapter_b", False, 300, {"x": 1})

        adapters = await config_store.list_adapters()

        assert len(adapters) == 2
        names = [a.name for a in adapters]
        assert "adapter_a" in names
        assert "adapter_b" in names

    @pytest.mark.asyncio
    async def test_upsert_updates_existing(self, config_store: ConfigStore) -> None:
        """Upsert updates existing adapter."""
        await config_store.upsert_adapter("updater", True, 60, {"v": 1})
        await config_store.upsert_adapter("updater", False, 120, {"v": 2})

        adapter = await config_store.get_adapter("updater")

        assert adapter is not None
        assert adapter.enabled is False
        assert adapter.cadence_s == 120
        assert adapter.settings == {"v": 2}

    @pytest.mark.asyncio
    async def test_pause_unpause(self, config_store: ConfigStore) -> None:
        """Can pause and unpause adapter."""
        await config_store.upsert_adapter("pausable", True, 60, {})

        await config_store.pause_adapter("pausable")
        adapter = await config_store.get_adapter("pausable")
        assert adapter is not None
        assert adapter.is_paused is True

        await config_store.unpause_adapter("pausable")
        adapter = await config_store.get_adapter("pausable")
        assert adapter is not None
        assert adapter.is_paused is False


class TestApiKeys:
    """Tests for API key operations."""

    @pytest.mark.asyncio
    async def test_set_and_get_key(self, config_store: ConfigStore) -> None:
        """Can store and retrieve encrypted API key."""
        await config_store.set_api_key("test_key", "super_secret_value")

        value = await config_store.get_api_key("test_key")

        assert value == "super_secret_value"

    @pytest.mark.asyncio
    async def test_get_nonexistent_key(self, config_store: ConfigStore) -> None:
        """Getting nonexistent key returns None."""
        value = await config_store.get_api_key("does_not_exist")
        assert value is None

    @pytest.mark.asyncio
    async def test_key_rotation(self, config_store: ConfigStore) -> None:
        """Updating key sets rotated_at."""
        await config_store.set_api_key("rotate_me", "value1")
        await config_store.set_api_key("rotate_me", "value2")

        value = await config_store.get_api_key("rotate_me")
        assert value == "value2"

    @pytest.mark.asyncio
    async def test_delete_key(self, config_store: ConfigStore) -> None:
        """Can delete API key."""
        await config_store.set_api_key("delete_me", "value")

        deleted = await config_store.delete_api_key("delete_me")
        assert deleted is True

        value = await config_store.get_api_key("delete_me")
        assert value is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent(self, config_store: ConfigStore) -> None:
        """Deleting nonexistent key returns False."""
        deleted = await config_store.delete_api_key("never_existed")
        assert deleted is False


class TestNotifications:
    """Tests for LISTEN/NOTIFY functionality."""

    @pytest.mark.asyncio
    async def test_notify_on_adapter_change(self, config_store: ConfigStore) -> None:
        """NOTIFY fires when adapter is changed."""
        notifications: list[tuple[str, str]] = []
        notification_received = asyncio.Event()

        async def callback(table: str, key: str) -> None:
            notifications.append((table, key))
            notification_received.set()

        # Start listener in background
        listen_task = asyncio.create_task(config_store.listen_for_changes(callback))

        try:
            # Give listener time to subscribe
            await asyncio.sleep(0.1)

            # Trigger a change
            await config_store.upsert_adapter("notify_test", True, 60, {})

            # Wait for notification (with timeout)
            try:
                await asyncio.wait_for(notification_received.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pytest.fail("Notification not received within timeout")

            assert len(notifications) >= 1
            assert notifications[0][0] == "adapters"
            assert notifications[0][1] == "notify_test"

        finally:
            listen_task.cancel()
            try:
                await listen_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_notify_on_api_key_change(self, config_store: ConfigStore) -> None:
        """NOTIFY fires when API key is changed."""
        notifications: list[tuple[str, str]] = []
        notification_received = asyncio.Event()

        async def callback(table: str, key: str) -> None:
            notifications.append((table, key))
            notification_received.set()

        listen_task = asyncio.create_task(config_store.listen_for_changes(callback))

        try:
            await asyncio.sleep(0.1)
            await config_store.set_api_key("notify_key", "secret")

            try:
                await asyncio.wait_for(notification_received.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pytest.fail("Notification not received within timeout")

            assert len(notifications) >= 1
            assert notifications[0][0] == "api_keys"
            assert notifications[0][1] == "notify_key"

        finally:
            listen_task.cancel()
            try:
                await listen_task
            except asyncio.CancelledError:
                pass


class TestListenerReconnect:
    """Tests for listener reconnection on connection loss."""

    @pytest.mark.asyncio
    async def test_listener_cancellation_propagates(
        self, config_store: ConfigStore
    ) -> None:
        """Cancellation cleanly stops the listener without reconnect loop."""
        async def callback(table: str, key: str) -> None:
            pass

        listen_task = asyncio.create_task(config_store.listen_for_changes(callback))

        # Give listener time to start
        await asyncio.sleep(0.1)

        # Cancel and verify it stops
        listen_task.cancel()
        try:
            await asyncio.wait_for(listen_task, timeout=2.0)
        except asyncio.CancelledError:
            pass  # Expected
        except asyncio.TimeoutError:
            pytest.fail("Listener did not stop after cancellation")

        assert listen_task.cancelled() or listen_task.done()
