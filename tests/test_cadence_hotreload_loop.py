"""Integration tests for cadence hot-reload exercising the ACTUAL running loop.

These tests run _run_adapter_loop and verify that cancel_event.set() properly
interrupts the sleeping loop. They are designed to:

- FAIL on unfixed code (cancel_event.set() inside lock delays signal delivery)
- PASS on fixed code (signal delivered after lock release)

Key difference from existing tests: these tests actually run the loop and
observe real poll timing, rather than testing AdapterState math in isolation.
"""

import asyncio
import base64
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from central.config_models import AdapterConfig
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


class MockConfigSource:
    """Mock ConfigSource for testing Supervisor without DB."""

    def __init__(self) -> None:
        self._adapters: dict[str, AdapterConfig] = {}

    def set_adapter(self, config: AdapterConfig | None, name: str | None = None) -> None:
        if config is None:
            if name:
                self._adapters.pop(name, None)
        else:
            self._adapters[config.name] = config

    async def list_enabled_adapters(self) -> list[AdapterConfig]:
        return [a for a in self._adapters.values() if a.enabled and not a.is_paused]

    async def get_adapter(self, name: str) -> AdapterConfig | None:
        return self._adapters.get(name)

    async def watch_for_changes(self, callback) -> None:
        return

    async def close(self) -> None:
        pass


class FastMockNWSAdapter:
    """Mock NWSAdapter that completes polls instantly and tracks timing."""

    def __init__(self, *, config, cursor_db_path) -> None:
        self.config = config
        self.cadence_s = config.cadence_s
        self.states = set(s.upper() for s in config.states)
        self.poll_times: list[datetime] = []
        self._published_ids: set[str] = set()

    async def startup(self) -> None:
        pass

    async def shutdown(self) -> None:
        pass

    async def poll(self) -> AsyncIterator:
        """Record poll time and yield nothing (no events)."""
        self.poll_times.append(datetime.now(timezone.utc))
        return
        yield  # Make this an async generator

    def is_published(self, event_id: str) -> bool:
        return event_id in self._published_ids

    def mark_published(self, event_id: str) -> None:
        self._published_ids.add(event_id)

    def bump_last_seen(self, event_id: str) -> None:
        pass

    def sweep_old_ids(self) -> int:
        return 0


@pytest.fixture
def mock_nats():
    """Mock NATS connection."""
    nc = MagicMock()
    nc.publish = AsyncMock()
    nc.drain = AsyncMock()
    nc.close = AsyncMock()
    js = MagicMock()
    js.publish = AsyncMock()
    nc.jetstream = MagicMock(return_value=js)
    return nc


class TestCadenceHotReloadLoop:
    """Tests that exercise the ACTUAL running loop with cancel_event signaling."""

    @pytest.mark.asyncio
    async def test_cadence_decrease_wakes_loop_immediately(
        self, mock_nats, tmp_path: Path
    ) -> None:
        """Test 1: Cadence decrease (60->30) - THE BUG WE ARE FIXING.

        This test MUST FAIL on unfixed code where cancel_event.set() is
        called inside the lock, causing delayed signal delivery.

        - Start adapter with 60s cadence
        - Let first poll complete
        - Change cadence to 30s via _on_config_change
        - Assert next poll fires at ~last_poll+30s, NOT last_poll+60s

        On unfixed code: loop sleeps full 60s, poll at T+60
        On fixed code: loop wakes immediately, recalculates, polls at T+30
        """
        from central.supervisor import Supervisor

        config_source = MockConfigSource()
        initial_config = AdapterConfig(
            name="nws",
            enabled=True,
            cadence_s=60,
            settings={"states": ["ID"], "contact_email": "test@test.com"},
            paused_at=None,
            updated_at=datetime.now(timezone.utc),
        )
        config_source.set_adapter(initial_config)

        supervisor = Supervisor(
            config_source=config_source,
            nats_url="nats://localhost:4222",
            cloudevents_config=None,
        )
        supervisor._nc = mock_nats
        supervisor._js = mock_nats.jetstream()

        # Track the mock adapter instance
        adapter_instance = None

        def capture_adapter(*, config, cursor_db_path):
            nonlocal adapter_instance
            adapter_instance = FastMockNWSAdapter(config=config, cursor_db_path=cursor_db_path)
            return adapter_instance

        with patch("central.supervisor.NWSAdapter", capture_adapter):
            # Start adapter - this creates the loop task
            await supervisor._start_adapter(initial_config)

            state = supervisor._adapter_states.get("nws")
            assert state is not None
            assert state.task is not None

            # Wait for first poll to complete
            await asyncio.sleep(0.1)
            assert len(adapter_instance.poll_times) >= 1, "First poll should complete"
            first_poll_time = adapter_instance.poll_times[-1]

            # Now the loop is sleeping for 60 seconds. Change cadence to 30s.
            new_config = AdapterConfig(
                name="nws",
                enabled=True,
                cadence_s=30,  # Decreased from 60
                settings={"states": ["ID"], "contact_email": "test@test.com"},
                paused_at=None,
                updated_at=datetime.now(timezone.utc),
            )
            config_source.set_adapter(new_config)

            # This should wake the loop via cancel_event
            await supervisor._on_config_change("adapters", "nws")

            # Wait for second poll - should happen at first_poll + 30s
            # Since we just changed cadence, if the fix works, the loop should
            # wake up, recalculate, and either poll immediately (if 30s passed)
            # or wait the remaining time.
            #
            # For this test, we wait up to 35s and verify the poll happens
            # around the 30s mark, not the 60s mark.
            start_wait = datetime.now(timezone.utc)
            timeout = 35  # Should complete well before 60s

            while len(adapter_instance.poll_times) < 2:
                await asyncio.sleep(0.5)
                elapsed = (datetime.now(timezone.utc) - start_wait).total_seconds()
                if elapsed > timeout:
                    break

            # Verify second poll happened
            assert len(adapter_instance.poll_times) >= 2, (
                f"Second poll did not happen within {timeout}s. "
                f"Bug: cancel_event.set() did not wake the sleeping loop. "
                f"Poll times: {adapter_instance.poll_times}"
            )

            second_poll_time = adapter_instance.poll_times[1]
            interval = (second_poll_time - first_poll_time).total_seconds()

            # The interval should be ~30s (new cadence), not 60s (old cadence)
            # Allow some tolerance for test execution overhead
            assert interval < 40, (
                f"Poll interval was {interval:.1f}s, expected ~30s. "
                f"Bug: loop used old cadence instead of new cadence after reschedule."
            )

            # Cleanup
            supervisor._shutdown_event.set()
            state.cancel_event.set()
            if state.task:
                state.task.cancel()
                try:
                    await state.task
                except asyncio.CancelledError:
                    pass

    @pytest.mark.asyncio
    async def test_cadence_increase_extends_wait(
        self, mock_nats, tmp_path: Path
    ) -> None:
        """Test 2: Cadence increase (10->20) extends wait correctly.

        - Start adapter with 10s cadence
        - Let first poll complete
        - Immediately change cadence to 20s
        - Assert next poll fires at ~last_poll+20s, not last_poll+10s
        """
        from central.supervisor import Supervisor

        config_source = MockConfigSource()
        initial_config = AdapterConfig(
            name="nws",
            enabled=True,
            cadence_s=10,  # Short for faster test
            settings={"states": ["ID"], "contact_email": "test@test.com"},
            paused_at=None,
            updated_at=datetime.now(timezone.utc),
        )
        config_source.set_adapter(initial_config)

        supervisor = Supervisor(
            config_source=config_source,
            nats_url="nats://localhost:4222",
            cloudevents_config=None,
        )
        supervisor._nc = mock_nats
        supervisor._js = mock_nats.jetstream()

        adapter_instance = None

        def capture_adapter(*, config, cursor_db_path):
            nonlocal adapter_instance
            adapter_instance = FastMockNWSAdapter(config=config, cursor_db_path=cursor_db_path)
            return adapter_instance

        with patch("central.supervisor.NWSAdapter", capture_adapter):
            await supervisor._start_adapter(initial_config)
            state = supervisor._adapter_states.get("nws")

            # Wait for first poll
            await asyncio.sleep(0.1)
            assert len(adapter_instance.poll_times) >= 1
            first_poll_time = adapter_instance.poll_times[-1]

            # Change cadence to 20s (increase)
            new_config = AdapterConfig(
                name="nws",
                enabled=True,
                cadence_s=20,
                settings={"states": ["ID"], "contact_email": "test@test.com"},
                paused_at=None,
                updated_at=datetime.now(timezone.utc),
            )
            config_source.set_adapter(new_config)
            await supervisor._on_config_change("adapters", "nws")

            # Wait 12 seconds - should NOT poll yet (20s cadence)
            await asyncio.sleep(12)

            # Should still be at 1 poll
            assert len(adapter_instance.poll_times) == 1, (
                f"Poll happened too early! Expected 1 poll, got {len(adapter_instance.poll_times)}. "
                f"Cadence increase should extend wait time."
            )

            # Wait remaining time plus buffer
            await asyncio.sleep(10)

            # Now should have second poll
            assert len(adapter_instance.poll_times) >= 2, (
                f"Second poll did not happen at 20s mark. "
                f"Poll times: {adapter_instance.poll_times}"
            )

            second_poll_time = adapter_instance.poll_times[1]
            interval = (second_poll_time - first_poll_time).total_seconds()

            # Should be ~20s
            assert 18 < interval < 25, (
                f"Poll interval was {interval:.1f}s, expected ~20s."
            )

            # Cleanup
            supervisor._shutdown_event.set()
            state.cancel_event.set()
            if state.task:
                state.task.cancel()
                try:
                    await state.task
                except asyncio.CancelledError:
                    pass

    @pytest.mark.asyncio
    async def test_enable_disable_enable_gap_exceeds_cadence(
        self, mock_nats, tmp_path: Path
    ) -> None:
        """Test 3: Enable->disable->enable with gap > cadence polls immediately.

        - Start adapter, complete one poll at T1
        - Disable adapter
        - Wait > cadence_s
        - Re-enable
        - Assert poll fires immediately (gap exceeded cadence)
        """
        from central.supervisor import Supervisor

        config_source = MockConfigSource()
        config = AdapterConfig(
            name="nws",
            enabled=True,
            cadence_s=2,  # Short cadence for faster test
            settings={"states": ["ID"], "contact_email": "test@test.com"},
            paused_at=None,
            updated_at=datetime.now(timezone.utc),
        )
        config_source.set_adapter(config)

        supervisor = Supervisor(
            config_source=config_source,
            nats_url="nats://localhost:4222",
            cloudevents_config=None,
        )
        supervisor._nc = mock_nats
        supervisor._js = mock_nats.jetstream()

        adapter_instances = []

        def capture_adapter(*, config, cursor_db_path):
            inst = FastMockNWSAdapter(config=config, cursor_db_path=cursor_db_path)
            adapter_instances.append(inst)
            return inst

        with patch("central.supervisor.NWSAdapter", capture_adapter):
            await supervisor._start_adapter(config)
            state = supervisor._adapter_states.get("nws")

            # Wait for first poll
            await asyncio.sleep(0.1)
            first_adapter = adapter_instances[0]
            assert len(first_adapter.poll_times) >= 1

            # Disable adapter
            disabled_config = AdapterConfig(
                name="nws",
                enabled=False,
                cadence_s=2,
                settings={"states": ["ID"], "contact_email": "test@test.com"},
                paused_at=None,
                updated_at=datetime.now(timezone.utc),
            )
            config_source.set_adapter(disabled_config)
            await supervisor._on_config_change("adapters", "nws")

            # Wait longer than cadence
            await asyncio.sleep(3)

            # Re-enable
            reenabled_config = AdapterConfig(
                name="nws",
                enabled=True,
                cadence_s=2,
                settings={"states": ["ID"], "contact_email": "test@test.com"},
                paused_at=None,
                updated_at=datetime.now(timezone.utc),
            )
            config_source.set_adapter(reenabled_config)

            reenable_time = datetime.now(timezone.utc)
            await supervisor._on_config_change("adapters", "nws")

            # Wait a bit for immediate poll
            await asyncio.sleep(0.5)

            new_state = supervisor._adapter_states.get("nws")
            assert new_state is not None

            # Check that a poll happened quickly after re-enable
            # The new adapter instance should have polled
            if len(adapter_instances) > 1:
                new_adapter = adapter_instances[-1]
                assert len(new_adapter.poll_times) >= 1, (
                    "Poll should happen immediately when gap > cadence"
                )
                poll_delay = (new_adapter.poll_times[0] - reenable_time).total_seconds()
                assert poll_delay < 1, (
                    f"Poll took {poll_delay:.1f}s after re-enable, expected immediate"
                )

            # Cleanup
            supervisor._shutdown_event.set()
            if new_state.task:
                new_state.cancel_event.set()
                new_state.task.cancel()
                try:
                    await new_state.task
                except asyncio.CancelledError:
                    pass

    @pytest.mark.asyncio
    async def test_enable_disable_enable_gap_within_cadence(
        self, mock_nats, tmp_path: Path
    ) -> None:
        """Test 4: Enable->disable->enable with gap < cadence waits.

        - Start adapter with 10s cadence, complete poll at T1
        - Disable adapter
        - Re-enable quickly (within cadence window)
        - Assert next poll fires at T1 + cadence_s, NOT immediately
        """
        from central.supervisor import Supervisor

        config_source = MockConfigSource()
        config = AdapterConfig(
            name="nws",
            enabled=True,
            cadence_s=10,  # 10 second cadence
            settings={"states": ["ID"], "contact_email": "test@test.com"},
            paused_at=None,
            updated_at=datetime.now(timezone.utc),
        )
        config_source.set_adapter(config)

        supervisor = Supervisor(
            config_source=config_source,
            nats_url="nats://localhost:4222",
            cloudevents_config=None,
        )
        supervisor._nc = mock_nats
        supervisor._js = mock_nats.jetstream()

        adapter_instances = []

        def capture_adapter(*, config, cursor_db_path):
            inst = FastMockNWSAdapter(config=config, cursor_db_path=cursor_db_path)
            adapter_instances.append(inst)
            return inst

        with patch("central.supervisor.NWSAdapter", capture_adapter):
            await supervisor._start_adapter(config)
            state = supervisor._adapter_states.get("nws")

            # Wait for first poll
            await asyncio.sleep(0.1)
            first_adapter = adapter_instances[0]
            assert len(first_adapter.poll_times) >= 1
            first_poll_time = first_adapter.poll_times[-1]

            # Disable adapter quickly
            disabled_config = AdapterConfig(
                name="nws",
                enabled=False,
                cadence_s=10,
                settings={"states": ["ID"], "contact_email": "test@test.com"},
                paused_at=None,
                updated_at=datetime.now(timezone.utc),
            )
            config_source.set_adapter(disabled_config)
            await supervisor._on_config_change("adapters", "nws")

            # Re-enable immediately (within cadence window)
            await asyncio.sleep(0.5)
            reenabled_config = AdapterConfig(
                name="nws",
                enabled=True,
                cadence_s=10,
                settings={"states": ["ID"], "contact_email": "test@test.com"},
                paused_at=None,
                updated_at=datetime.now(timezone.utc),
            )
            config_source.set_adapter(reenabled_config)
            await supervisor._on_config_change("adapters", "nws")

            new_state = supervisor._adapter_states.get("nws")
            assert new_state is not None

            # Wait 3 seconds - should NOT poll yet (still within 10s cadence)
            await asyncio.sleep(3)

            # The new adapter instance should not have polled yet
            if len(adapter_instances) > 1:
                new_adapter = adapter_instances[-1]
                assert len(new_adapter.poll_times) == 0, (
                    f"Poll happened too early! Gap < cadence should wait. "
                    f"Polls: {new_adapter.poll_times}"
                )

            # Wait for remaining time (about 7 more seconds)
            await asyncio.sleep(8)

            # Now should have polled
            if len(adapter_instances) > 1:
                new_adapter = adapter_instances[-1]
                assert len(new_adapter.poll_times) >= 1, (
                    "Poll should have happened by now (10s cadence elapsed)"
                )

            # Cleanup
            supervisor._shutdown_event.set()
            if new_state.task:
                new_state.cancel_event.set()
                new_state.task.cancel()
                try:
                    await new_state.task
                except asyncio.CancelledError:
                    pass
