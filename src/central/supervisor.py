"""Central supervisor - adapter scheduler and event publisher."""

import asyncio
import json
import logging
import signal
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import nats
from nats.js import JetStreamContext

from central.adapter import SourceAdapter
from central.adapters.nws import NWSAdapter
from central.cloudevents_wire import wrap_event
from central.config_models import AdapterConfig
from central.config_source import ConfigSource, DbConfigSource
from central.config_store import ConfigStore
from central.bootstrap_config import get_settings
from central.models import subject_for_event
from central.stream_manager import StreamManager

CURSOR_DB_PATH = Path("/var/lib/central/cursors.db")

# Stream subject mappings
STREAM_SUBJECTS = {
    "CENTRAL_WX": ["central.wx.>"],
    "CENTRAL_META": ["central.meta.>"],
}

# Recompute interval for stream max_bytes (1 hour)
STREAM_RECOMPUTE_INTERVAL_S = 3600


class JsonFormatter(logging.Formatter):
    """JSON log formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            log_obj["exc"] = self.formatException(record.exc_info)
        if hasattr(record, "extra"):
            log_obj.update(record.extra)
        for key in record.__dict__:
            if key not in (
                "name", "msg", "args", "created", "filename", "funcName",
                "levelname", "levelno", "lineno", "module", "msecs",
                "pathname", "process", "processName", "relativeCreated",
                "stack_info", "exc_info", "exc_text", "thread", "threadName",
                "taskName", "message",
            ):
                log_obj[key] = record.__dict__[key]
        return json.dumps(log_obj)


def setup_logging() -> None:
    """Configure JSON logging to stdout."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.root.handlers = [handler]
    logging.root.setLevel(logging.INFO)


logger = logging.getLogger("central.supervisor")


@dataclass
class AdapterState:
    """Runtime state for a scheduled adapter."""

    name: str
    adapter: SourceAdapter
    config: AdapterConfig
    task: asyncio.Task[None] | None = None
    last_completed_poll: datetime | None = None
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def is_running(self) -> bool:
        """Check if adapter loop is currently running."""
        return self.task is not None and not self.task.done()


class Supervisor:
    """Main supervisor process."""

    def __init__(
        self,
        config_source: ConfigSource,
        config_store: ConfigStore,
        nats_url: str,
        cloudevents_config: Any = None,
    ) -> None:
        self._config_source = config_source
        self._config_store = config_store
        self._nats_url = nats_url
        self._cloudevents_config = cloudevents_config
        self._nc: nats.NATS | None = None
        self._js: JetStreamContext | None = None
        self._stream_manager: StreamManager | None = None
        self._adapter_states: dict[str, AdapterState] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._shutdown_event = asyncio.Event()
        self._start_time = datetime.now(timezone.utc)
        self._config_watch_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        """Connect to NATS."""
        self._nc = await nats.connect(self._nats_url)
        self._js = self._nc.jetstream()
        self._stream_manager = StreamManager(self._js)
        logger.info("Connected to NATS", extra={"url": self._nats_url})

    async def disconnect(self) -> None:
        """Disconnect from NATS."""
        if self._nc:
            await self._nc.drain()
            await self._nc.close()
            self._nc = None
            self._js = None
            self._stream_manager = None
        logger.info("Disconnected from NATS")

    async def _publish_meta(self, subject: str, data: dict[str, Any]) -> None:
        """Publish a meta event (no Nats-Msg-Id)."""
        if not self._nc:
            return
        payload = json.dumps(data).encode()
        await self._nc.publish(subject, payload)

    async def _publish_event(self, subject: str, envelope: dict[str, Any], msg_id: str) -> None:
        """Publish an event with dedup header."""
        if not self._js:
            return
        payload = json.dumps(envelope).encode()
        await self._js.publish(
            subject,
            payload,
            headers={"Nats-Msg-Id": msg_id},
        )

    def _create_adapter(self, config: AdapterConfig) -> SourceAdapter:
        """Create an adapter instance based on config name."""
        if config.name == "nws":
            return NWSAdapter(config=config, cursor_db_path=CURSOR_DB_PATH)
        else:
            raise ValueError(f"Unknown adapter type: {config.name}")

    async def _run_adapter_loop(self, state: AdapterState) -> None:
        """Run an adapter poll loop with rate-limit aware scheduling."""
        while not self._shutdown_event.is_set():
            # Calculate next poll time based on rate-limit guarantee
            now = datetime.now(timezone.utc)

            if state.last_completed_poll is not None:
                next_poll_at = state.last_completed_poll.timestamp() + state.config.cadence_s
                wait_time = max(0, next_poll_at - now.timestamp())
            else:
                # First poll - run immediately
                wait_time = 0

            if wait_time > 0:
                logger.debug(
                    "Waiting for next poll",
                    extra={
                        "adapter": state.name,
                        "wait_s": wait_time,
                        "next_poll": datetime.fromtimestamp(
                            now.timestamp() + wait_time, tz=timezone.utc
                        ).isoformat(),
                    },
                )
                # Wait for either timeout or cancel signal
                try:
                    await asyncio.wait_for(
                        state.cancel_event.wait(),
                        timeout=wait_time,
                    )
                    # Cancel event was set - check if we should exit or reschedule
                    if self._shutdown_event.is_set():
                        break
                    # Clear the cancel event and re-evaluate schedule
                    state.cancel_event.clear()
                    continue
                except asyncio.TimeoutError:
                    pass

            # Check shutdown before polling
            if self._shutdown_event.is_set():
                break

            # Check if adapter is still enabled
            if not state.config.enabled or state.config.is_paused:
                logger.info(
                    "Adapter disabled/paused, stopping loop",
                    extra={"adapter": state.name},
                )
                break

            poll_start = datetime.now(timezone.utc)
            try:
                async for event in state.adapter.poll():
                    # Dedup check
                    if state.adapter.is_published(event.id):
                        state.adapter.bump_last_seen(event.id)
                        continue

                    # Build CloudEvent (uses defaults if no config provided)
                    envelope, msg_id = wrap_event(event, self._cloudevents_config)

                    subject = subject_for_event(event)

                    # Publish
                    await self._publish_event(subject, envelope, msg_id)
                    state.adapter.mark_published(event.id)

                    logger.info(
                        "Published event",
                        extra={"id": event.id, "subject": subject, "category": event.category}
                    )

                # Publish success status
                await self._publish_meta(
                    f"central.meta.adapter.{state.name}.status",
                    {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}
                )

                # Mark poll completion time for rate limiting
                state.last_completed_poll = datetime.now(timezone.utc)

            except Exception as e:
                logger.exception("Adapter poll failed", extra={"adapter": state.name})
                await self._publish_meta(
                    f"central.meta.adapter.{state.name}.status",
                    {
                        "ok": False,
                        "error": str(e),
                        "ts": datetime.now(timezone.utc).isoformat()
                    }
                )
                # Still mark completion time to avoid tight retry loops
                state.last_completed_poll = datetime.now(timezone.utc)

            # Sweep old IDs
            swept = state.adapter.sweep_old_ids()
            if swept > 0:
                logger.info("Swept old published IDs", extra={"count": swept})

    async def _start_adapter(self, config: AdapterConfig) -> None:
        """Start an adapter based on its configuration.

        If the adapter was previously stopped (state exists but task is not running),
        reuses the existing state to preserve last_completed_poll for rate limiting.
        """
        existing_state = self._adapter_states.get(config.name)

        if existing_state is not None:
            if existing_state.is_running:
                logger.warning(
                    "Adapter already running",
                    extra={"adapter": config.name},
                )
                return

            # Adapter was stopped - restart with preserved state
            # Update config and restart the adapter
            existing_state.config = config
            existing_state.cancel_event.clear()

            # Reinitialize the adapter with new config
            existing_state.adapter = self._create_adapter(config)
            await existing_state.adapter.startup()

            # Start the loop task
            existing_state.task = asyncio.create_task(
                self._run_adapter_loop(existing_state)
            )

            # Calculate next poll time for logging
            if existing_state.last_completed_poll:
                next_poll_at = datetime.fromtimestamp(
                    existing_state.last_completed_poll.timestamp() + config.cadence_s,
                    tz=timezone.utc,
                )
                if next_poll_at <= datetime.now(timezone.utc):
                    next_poll_at = datetime.now(timezone.utc)
            else:
                next_poll_at = datetime.now(timezone.utc)

            logger.info(
                "Adapter restarted",
                extra={
                    "adapter": config.name,
                    "cadence_s": config.cadence_s,
                    "preserved_last_poll": existing_state.last_completed_poll.isoformat()
                    if existing_state.last_completed_poll
                    else None,
                    "next_poll": next_poll_at.isoformat(),
                },
            )
            return

        # New adapter - create fresh state
        try:
            adapter = self._create_adapter(config)
        except ValueError as e:
            logger.warning(str(e), extra={"adapter": config.name})
            return

        await adapter.startup()

        state = AdapterState(
            name=config.name,
            adapter=adapter,
            config=config,
        )
        state.task = asyncio.create_task(self._run_adapter_loop(state))
        self._adapter_states[config.name] = state

        logger.info(
            "Adapter started",
            extra={
                "adapter": config.name,
                "cadence_s": config.cadence_s,
            },
        )

    async def _stop_adapter(self, name: str) -> None:
        """Stop a running adapter but preserve state for potential restart.

        The adapter state (including last_completed_poll) is preserved so that
        if the adapter is re-enabled, the rate-limit guarantee is maintained.
        Use _remove_adapter() to fully remove an adapter from tracking.
        """
        state = self._adapter_states.get(name)
        if state is None:
            return

        if not state.is_running:
            # Already stopped
            return

        # Signal the loop to stop
        state.cancel_event.set()

        if state.task:
            state.task.cancel()
            try:
                await state.task
            except asyncio.CancelledError:
                pass
            state.task = None

        await state.adapter.shutdown()
        logger.info(
            "Adapter stopped",
            extra={
                "adapter": name,
                "preserved_last_poll": state.last_completed_poll.isoformat()
                if state.last_completed_poll
                else None,
            },
        )

    async def _remove_adapter(self, name: str) -> None:
        """Fully remove an adapter, dropping all preserved state.

        Called when an adapter is deleted from the database (not just disabled).
        """
        state = self._adapter_states.pop(name, None)
        if state is None:
            return

        # Stop if running
        if state.is_running:
            state.cancel_event.set()
            if state.task:
                state.task.cancel()
                try:
                    await state.task
                except asyncio.CancelledError:
                    pass

            await state.adapter.shutdown()

        logger.info(
            "Adapter removed",
            extra={"adapter": name},
        )

    async def _reschedule_adapter(
        self,
        name: str,
        new_config: AdapterConfig,
    ) -> AdapterState | None:
        """Reschedule an adapter with new configuration.

        Maintains rate-limit guarantee: next poll at
        (last_completed_poll + new_cadence_s), not now + new_cadence_s.

        Returns the AdapterState to signal, or None if no signal needed.
        The caller must signal cancel_event AFTER releasing any locks to
        ensure immediate event delivery to the sleeping loop.
        """
        state = self._adapter_states.get(name)
        if state is None:
            # Adapter not running - just start it
            await self._start_adapter(new_config)
            return None

        if not state.is_running:
            # Adapter stopped - restart it
            await self._start_adapter(new_config)
            return None

        old_cadence = state.config.cadence_s
        new_cadence = new_config.cadence_s

        # Update config
        state.config = new_config

        # Apply config to adapter (generic - each adapter handles its own settings)
        await state.adapter.apply_config(new_config)

        # Calculate next poll time for logging
        if state.last_completed_poll:
            next_poll_at = datetime.fromtimestamp(
                state.last_completed_poll.timestamp() + new_cadence,
                tz=timezone.utc,
            )
        else:
            next_poll_at = datetime.now(timezone.utc)

        logger.info(
            "Rescheduled adapter",
            extra={
                "adapter": name,
                "old_cadence_s": old_cadence,
                "new_cadence_s": new_cadence,
                "next_poll": next_poll_at.isoformat(),
            },
        )

        # Return state so caller can signal OUTSIDE any locks.
        # This ensures immediate event delivery to the sleeping loop.
        return state

    async def _ensure_streams(self) -> None:
        """Ensure all configured streams exist with correct settings."""
        if not self._stream_manager:
            return

        streams = await self._config_store.list_streams()
        for stream_config in streams:
            subjects = STREAM_SUBJECTS.get(stream_config.name, [])
            if not subjects:
                logger.warning(
                    "No subjects configured for stream",
                    extra={"stream": stream_config.name},
                )
                continue

            try:
                await self._stream_manager.ensure_stream(
                    stream_config.name,
                    subjects,
                    stream_config,
                )
            except Exception as e:
                logger.error(
                    "Failed to ensure stream",
                    extra={"stream": stream_config.name, "error": str(e)},
                )

    async def _handle_stream_change(self, stream_name: str) -> None:
        """Handle a stream configuration change."""
        if not self._stream_manager:
            return

        stream_config = await self._config_store.get_stream(stream_name)
        if stream_config is None:
            logger.warning(
                "Stream config not found",
                extra={"stream": stream_name},
            )
            return

        try:
            # Apply retention settings
            await self._stream_manager.apply_retention(stream_name, stream_config)

            # Immediate recompute of max_bytes
            new_max_bytes = await self._stream_manager.recompute_max_bytes(
                stream_name, stream_config.max_age_s
            )

            # Update database (won't trigger NOTIFY due to column filter)
            await self._config_store.update_stream_max_bytes(stream_name, new_max_bytes)

            logger.info(
                "Stream retention updated",
                extra={
                    "stream": stream_name,
                    "max_age_s": stream_config.max_age_s,
                    "new_max_bytes": new_max_bytes,
                },
            )
        except Exception as e:
            logger.error(
                "Failed to handle stream change",
                extra={"stream": stream_name, "error": str(e)},
            )

    async def _stream_retention_recompute_loop(self) -> None:
        """Periodically recompute max_bytes for all streams."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=STREAM_RECOMPUTE_INTERVAL_S,
                )
                # Shutdown requested
                break
            except asyncio.TimeoutError:
                pass

            # Recompute for all streams
            if not self._stream_manager:
                continue

            streams = await self._config_store.list_streams()
            for stream_config in streams:
                if not stream_config.managed_max_bytes:
                    continue

                try:
                    new_max_bytes = await self._stream_manager.recompute_max_bytes(
                        stream_config.name, stream_config.max_age_s
                    )

                    # Only update if change > 10%
                    change_ratio = abs(new_max_bytes - stream_config.max_bytes) / max(stream_config.max_bytes, 1)
                    if change_ratio > 0.10:
                        await self._config_store.update_stream_max_bytes(
                            stream_config.name, new_max_bytes
                        )
                        await self._stream_manager.apply_retention(
                            stream_config.name,
                            await self._config_store.get_stream(stream_config.name),
                        )
                        logger.info(
                            "Recomputed stream max_bytes",
                            extra={
                                "stream": stream_config.name,
                                "old_max_bytes": stream_config.max_bytes,
                                "new_max_bytes": new_max_bytes,
                                "change_ratio": change_ratio,
                            },
                        )
                except Exception as e:
                    logger.error(
                        "Failed to recompute stream max_bytes",
                        extra={"stream": stream_config.name, "error": str(e)},
                    )

    async def _on_config_change(self, table: str, key: str) -> None:
        """Handle a configuration change notification.

        Called when NOTIFY fires for config changes.
        """
        # Handle stream changes
        if table == "streams":
            stream_name = key
            logger.info(
                "Stream config change received",
                extra={"stream": stream_name},
            )
            await self._handle_stream_change(stream_name)
            return

        if table != "adapters":
            return

        adapter_name = key
        logger.info(
            "Config change received",
            extra={"table": table, "key": key},
        )

        # Track state that needs signaling after lock release
        state_to_signal: AdapterState | None = None

        async with self._lock:
            # Fetch the current config for this adapter
            new_config = await self._config_source.get_adapter(adapter_name)
            current_state = self._adapter_states.get(adapter_name)

            if new_config is None:
                # Adapter was deleted - fully remove, don't just stop
                if current_state:
                    await self._remove_adapter(adapter_name)
                    logger.info(
                        "Adapter deleted, removed",
                        extra={"adapter": adapter_name},
                    )
                return

            if not new_config.enabled or new_config.is_paused:
                # Adapter disabled or paused - stop but preserve state
                if current_state and current_state.is_running:
                    await self._stop_adapter(adapter_name)
                    logger.info(
                        "Adapter disabled/paused, stopped",
                        extra={
                            "adapter": adapter_name,
                            "enabled": new_config.enabled,
                            "paused": new_config.is_paused,
                        },
                    )
                return

            if current_state is None or not current_state.is_running:
                # Adapter was enabled or created - start (will reuse state if exists)
                await self._start_adapter(new_config)
                logger.info(
                    "Adapter enabled, started",
                    extra={"adapter": adapter_name},
                )
            else:
                # Adapter config changed (cadence, settings)
                state_to_signal = await self._reschedule_adapter(adapter_name, new_config)

        # Signal OUTSIDE the lock to ensure immediate event delivery.
        # This fixes cadence-decrease hot-reload where the signal was
        # delayed by asyncio task scheduling while holding the lock.
        if state_to_signal is not None:
            state_to_signal.cancel_event.set()

    async def _heartbeat_loop(self) -> None:
        """Publish periodic heartbeats."""
        while not self._shutdown_event.is_set():
            uptime = (datetime.now(timezone.utc) - self._start_time).total_seconds()
            await self._publish_meta(
                "central.meta.heartbeat",
                {"ts": datetime.now(timezone.utc).isoformat(), "uptime_s": uptime}
            )
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=30
                )
            except asyncio.TimeoutError:
                pass

    async def start(self) -> None:
        """Start the supervisor."""
        await self.connect()

        # Ensure streams exist with correct configuration
        await self._ensure_streams()

        # Load and start enabled adapters
        enabled_adapters = await self._config_source.list_enabled_adapters()
        for config in enabled_adapters:
            await self._start_adapter(config)

        # Start config watcher (runs forever, calling callback on changes)
        self._config_watch_task = asyncio.create_task(
            self._config_source.watch_for_changes(self._on_config_change)
        )

        # Start heartbeat
        self._tasks.append(asyncio.create_task(self._heartbeat_loop()))

        # Start stream retention recompute loop
        self._tasks.append(asyncio.create_task(self._stream_retention_recompute_loop()))

        logger.info(
            "Supervisor started",
            extra={"adapters": list(self._adapter_states.keys())},
        )

    async def stop(self) -> None:
        """Stop the supervisor gracefully."""
        logger.info("Supervisor shutting down")
        self._shutdown_event.set()

        # Cancel config watcher
        if self._config_watch_task:
            self._config_watch_task.cancel()
            try:
                await self._config_watch_task
            except asyncio.CancelledError:
                pass

        # Cancel heartbeat and other tasks
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Remove all adapters (full cleanup)
        for name in list(self._adapter_states.keys()):
            await self._remove_adapter(name)

        # Close config source
        await self._config_source.close()

        await self.disconnect()
        logger.info("Supervisor stopped")


async def async_main() -> None:
    """Async entry point."""
    setup_logging()

    settings = get_settings()
    logger.info(
        "Config source: db",
        extra={"config_source": "db"},
    )

    # Create database config source and store
    config_source = await DbConfigSource.create(settings.db_dsn)
    config_store = await ConfigStore.create(settings.db_dsn)

    supervisor = Supervisor(
        config_source=config_source,
        config_store=config_store,
        nats_url=settings.nats_url,
        # CloudEvents uses protocol-level defaults from cloudevents_constants
        cloudevents_config=None,
    )
    logger.info(
        "CloudEvents config: defaults",
        extra={"cloudevents_source": "defaults"},
    )

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def handle_signal() -> None:
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal)

    await supervisor.start()

    # Wait for shutdown signal
    await shutdown_event.wait()

    await supervisor.stop()


def main() -> None:
    """Entry point."""
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
