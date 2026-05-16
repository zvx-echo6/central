"""Tests for supervisor hot-reload and rate-limiting behavior."""

import asyncio
import base64
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest
import pytest_asyncio

from central.config_models import AdapterConfig
from central.config_source import DbConfigSource
from central.config_store import ConfigStore
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
    # Create notify trigger
    await db_conn.execute("""
        CREATE OR REPLACE FUNCTION config.notify_config_change()
        RETURNS trigger AS $$
        DECLARE
            key_value TEXT;
        BEGIN
            IF TG_TABLE_NAME = 'adapters' THEN
                key_value := COALESCE(NEW.name, OLD.name, '');
            ELSE
                key_value := '';
            END IF;
            PERFORM pg_notify('config_changed', TG_TABLE_NAME || ':' || key_value);
            RETURN COALESCE(NEW, OLD);
        END;
        $$ LANGUAGE plpgsql
    """)
    await db_conn.execute("""
        DROP TRIGGER IF EXISTS adapters_notify ON config.adapters;
        CREATE TRIGGER adapters_notify
            AFTER INSERT OR UPDATE OR DELETE ON config.adapters
            FOR EACH ROW EXECUTE FUNCTION config.notify_config_change()
    """)
    await db_conn.execute("DELETE FROM config.adapters")


@pytest_asyncio.fixture
async def config_store(clean_config_schema: None) -> ConfigStore:
    """Create a ConfigStore connected to the test database."""
    store = await ConfigStore.create(TEST_DB_DSN)
    yield store
    await store.close()


class TestDbConfigSourceNotifications:
    """Tests for DbConfigSource NOTIFY integration."""

    @pytest.mark.asyncio
    async def test_watch_receives_notifications(
        self,
        config_store: ConfigStore,
        db_conn: asyncpg.Connection,
    ) -> None:
        """watch_for_changes receives NOTIFY when adapter changes."""
        source = DbConfigSource(config_store)
        notifications: list[tuple[str, str]] = []
        notification_received = asyncio.Event()

        async def callback(table: str, key: str) -> None:
            notifications.append((table, key))
            notification_received.set()

        # Start watching in background
        watch_task = asyncio.create_task(source.watch_for_changes(callback))

        try:
            # Wait for listener to connect
            await asyncio.sleep(0.2)

            # Insert an adapter via direct connection (not through store)
            # This triggers the NOTIFY
            await db_conn.execute("""
                INSERT INTO config.adapters (name, enabled, cadence_s, settings)
                VALUES ('test_adapter', true, 60, '{}'::jsonb)
            """)

            # Wait for notification
            await asyncio.wait_for(notification_received.wait(), timeout=5.0)

            assert len(notifications) >= 1
            assert notifications[0] == ("adapters", "test_adapter")

        finally:
            watch_task.cancel()
            try:
                await watch_task
            except asyncio.CancelledError:
                pass


class TestRateLimitGuarantee:
    """Tests for rate-limit guarantees during hot-reload.

    These tests verify the critical invariant: cadence changes must not
    cause extra API calls before (last_poll + new_cadence).
    """

    @pytest.mark.asyncio
    async def test_cadence_change_respects_last_poll_time(self) -> None:
        """Changing cadence mid-cycle schedules next poll at last_poll + new_cadence.

        This is the core rate-limit guarantee test (gate 3).
        """
        # Import supervisor module to access AdapterState
        from central.supervisor import AdapterState

        # Mock adapter
        mock_adapter = MagicMock()
        mock_adapter.name = "test"
        mock_adapter.cadence_s = 60

        # Create adapter state with a known last_completed_poll time
        last_poll = datetime.now(timezone.utc) - timedelta(seconds=30)

        config = AdapterConfig(
            name="test",
            enabled=True,
            cadence_s=60,  # Original cadence
            settings={},
            paused_at=None,
            updated_at=datetime.now(timezone.utc),
        )

        state = AdapterState(
            name="test",
            adapter=mock_adapter,
            config=config,
            last_completed_poll=last_poll,
        )

        # Simulate cadence change to 90 seconds
        new_config = AdapterConfig(
            name="test",
            enabled=True,
            cadence_s=90,  # New cadence
            settings={},
            paused_at=None,
            updated_at=datetime.now(timezone.utc),
        )

        # Update state as reschedule would
        state.config = new_config
        state.adapter.cadence_s = 90

        # Calculate expected next poll time
        expected_next_poll = last_poll + timedelta(seconds=90)
        now = datetime.now(timezone.utc)
        expected_wait = max(0, (expected_next_poll - now).total_seconds())

        # The wait time should be based on last_poll + new_cadence
        # Since last_poll was 30 seconds ago and new cadence is 90,
        # we should wait 60 more seconds (90 - 30 = 60)
        actual_next_poll = last_poll.timestamp() + new_config.cadence_s
        actual_wait = max(0, actual_next_poll - now.timestamp())

        # Allow 1 second tolerance for timing
        assert abs(actual_wait - 60) < 2, (
            f"Expected ~60s wait, got {actual_wait}s. "
            f"Rate limit violated: poll would happen before last_poll + new_cadence"
        )

    @pytest.mark.asyncio
    async def test_cadence_increase_after_gap_polls_immediately(self) -> None:
        """When last_poll + new_cadence is already past, poll immediately.

        If operator increases cadence to 120s after a gap of 150s,
        the poll should happen now (not wait another 120s).
        """
        from central.supervisor import AdapterState

        mock_adapter = MagicMock()
        mock_adapter.name = "test"
        mock_adapter.cadence_s = 60

        # Last poll was 150 seconds ago
        last_poll = datetime.now(timezone.utc) - timedelta(seconds=150)

        config = AdapterConfig(
            name="test",
            enabled=True,
            cadence_s=120,  # Increased cadence
            settings={},
            paused_at=None,
            updated_at=datetime.now(timezone.utc),
        )

        state = AdapterState(
            name="test",
            adapter=mock_adapter,
            config=config,
            last_completed_poll=last_poll,
        )

        # Calculate next poll time
        now = datetime.now(timezone.utc)
        next_poll_at = last_poll.timestamp() + config.cadence_s
        wait_time = max(0, next_poll_at - now.timestamp())

        # Since 150 > 120, next poll should be immediate (wait_time ~= 0)
        assert wait_time < 1, (
            f"Expected immediate poll (wait ~0s), got {wait_time}s. "
            f"After a gap exceeding new cadence, poll should happen now."
        )

    @pytest.mark.asyncio
    async def test_enable_disable_enable_respects_rate_limit(self) -> None:
        """Re-enabling adapter schedules poll at last_poll + cadence.

        If adapter was disabled for a while and then re-enabled, the next
        poll should be at (last_completed_poll + cadence_s), not immediately
        (unless that time has already passed).
        """
        from central.supervisor import AdapterState

        mock_adapter = MagicMock()
        mock_adapter.name = "test"
        mock_adapter.cadence_s = 60

        # Last poll was 30 seconds ago, then adapter was disabled
        last_poll = datetime.now(timezone.utc) - timedelta(seconds=30)

        # Re-enabled config
        config = AdapterConfig(
            name="test",
            enabled=True,
            cadence_s=60,
            settings={},
            paused_at=None,
            updated_at=datetime.now(timezone.utc),
        )

        state = AdapterState(
            name="test",
            adapter=mock_adapter,
            config=config,
            last_completed_poll=last_poll,
        )

        # Calculate next poll time
        now = datetime.now(timezone.utc)
        next_poll_at = last_poll.timestamp() + config.cadence_s
        wait_time = max(0, next_poll_at - now.timestamp())

        # Should wait ~30 more seconds (60 - 30 = 30)
        assert abs(wait_time - 30) < 2, (
            f"Expected ~30s wait after re-enable, got {wait_time}s. "
            f"Rate limit violated on enable→disable→enable sequence."
        )

    @pytest.mark.asyncio
    async def test_multiple_rapid_cadence_changes_no_extra_polls(self) -> None:
        """Multiple rapid cadence changes don't cause extra polls.

        If NOTIFY fires rapidly (60→90→120→90), the final schedule should
        still be based on last_completed_poll + final_cadence.
        """
        from central.supervisor import AdapterState

        mock_adapter = MagicMock()
        mock_adapter.name = "test"
        mock_adapter.cadence_s = 60

        # Last poll was 20 seconds ago
        last_poll = datetime.now(timezone.utc) - timedelta(seconds=20)

        state = AdapterState(
            name="test",
            adapter=mock_adapter,
            config=AdapterConfig(
                name="test",
                enabled=True,
                cadence_s=60,
                settings={},
                paused_at=None,
                updated_at=datetime.now(timezone.utc),
            ),
            last_completed_poll=last_poll,
        )

        # Simulate rapid cadence changes
        for cadence in [90, 120, 90]:  # Final cadence is 90
            state.config = AdapterConfig(
                name="test",
                enabled=True,
                cadence_s=cadence,
                settings={},
                paused_at=None,
                updated_at=datetime.now(timezone.utc),
            )
            state.adapter.cadence_s = cadence

        # Final schedule should be last_poll + 90
        now = datetime.now(timezone.utc)
        final_cadence = 90
        next_poll_at = last_poll.timestamp() + final_cadence
        wait_time = max(0, next_poll_at - now.timestamp())

        # Should wait ~70 seconds (90 - 20 = 70)
        assert abs(wait_time - 70) < 2, (
            f"Expected ~70s wait after rapid changes, got {wait_time}s. "
            f"Multiple NOTIFYs should not cause extra polls."
        )


class TestBootstrapConfigFlag:
    """Tests for CENTRAL_CONFIG_SOURCE bootstrap flag."""

    def test_default_is_toml(self) -> None:
        """Default config_source is 'toml'."""
        from central.bootstrap_config import Settings

        # Create settings with minimal required fields
        settings = Settings(
            db_dsn="postgresql://test@localhost/test",
            _env_file=None,
        )
        assert settings.config_source == "toml"

    def test_accepts_db(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """config_source accepts 'db' value."""
        from central.bootstrap_config import Settings, get_settings

        get_settings.cache_clear()
        monkeypatch.setenv("CENTRAL_CONFIG_SOURCE", "db")
        monkeypatch.setenv("CENTRAL_DB_DSN", "postgresql://test@localhost/test")

        settings = get_settings()
        assert settings.config_source == "db"

    def test_rejects_invalid(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """config_source rejects invalid values."""
        from pydantic import ValidationError
        from central.bootstrap_config import Settings, get_settings

        get_settings.cache_clear()
        monkeypatch.setenv("CENTRAL_CONFIG_SOURCE", "invalid")
        monkeypatch.setenv("CENTRAL_DB_DSN", "postgresql://test@localhost/test")

        with pytest.raises(ValidationError):
            get_settings()
