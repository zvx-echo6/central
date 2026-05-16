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

from central.adapters.nws import NWSAdapter
from central.cloudevents_wire import wrap_event
from central.config import load_config, Config, NWSAdapterConfig
from central.config_models import AdapterConfig
from central.config_source import ConfigSource, create_config_source
from central.bootstrap_config import get_settings
from central.models import subject_for_event

CURSOR_DB_PATH = Path("/var/lib/central/cursors.db")


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
    adapter: NWSAdapter
    config: AdapterConfig
    task: asyncio.Task[None] | None = None
    last_completed_poll: datetime | None = None
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)

    def __post_init__(self) -> None:
        # Ensure cancel_event is created
        if self.cancel_event is None:
            self.cancel_event = asyncio.Event()


class Supervisor:
    """Main supervisor process."""

    def __init__(
        self,
        config_source: ConfigSource,
        nats_url: str,
        cloudevents_config: Any = None,
    ) -> None:
        self._config_source = config_source
        self._nats_url = nats_url
        self._cloudevents_config = cloudevents_config
        self._nc: nats.NATS | None = None
        self._js: JetStreamContext | None = None
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
        logger.info("Connected to NATS", extra={"url": self._nats_url})

    async def disconnect(self) -> None:
        """Disconnect from NATS."""
        if self._nc:
            await self._nc.drain()
            await self._nc.close()
            self._nc = None
            self._js = None
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

    def _adapter_config_to_nws_config(self, config: AdapterConfig) -> NWSAdapterConfig:
        """Convert unified AdapterConfig to NWSAdapterConfig."""
        return NWSAdapterConfig(
            enabled=config.enabled,
            cadence_s=config.cadence_s,
            states=config.settings.get("states", []),
            contact_email=config.settings.get("contact_email", ""),
        )

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

                    # Build CloudEvent
                    if self._cloudevents_config:
                        envelope, msg_id = wrap_event(event, self._cloudevents_config)
                    else:
                        # Fallback for testing
                        envelope = {"id": event.id, "data": event.data}
                        msg_id = event.id

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
        """Start an adapter based on its configuration."""
        if config.name in self._adapter_states:
            logger.warning(
                "Adapter already running",
                extra={"adapter": config.name},
            )
            return

        if config.name == "nws":
            nws_config = self._adapter_config_to_nws_config(config)
            adapter = NWSAdapter(
                config=nws_config,
                cursor_db_path=CURSOR_DB_PATH,
            )
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
        else:
            logger.warning(
                "Unknown adapter type",
                extra={"adapter": config.name},
            )

    async def _stop_adapter(self, name: str) -> None:
        """Stop a running adapter."""
        state = self._adapter_states.pop(name, None)
        if state is None:
            return

        # Signal the loop to stop
        state.cancel_event.set()

        if state.task:
            state.task.cancel()
            try:
                await state.task
            except asyncio.CancelledError:
                pass

        await state.adapter.shutdown()
        logger.info("Adapter stopped", extra={"adapter": name})

    async def _reschedule_adapter(
        self,
        name: str,
        new_config: AdapterConfig,
    ) -> None:
        """Reschedule an adapter with new configuration.

        Maintains rate-limit guarantee: next poll at
        (last_completed_poll + new_cadence_s), not now + new_cadence_s.
        """
        state = self._adapter_states.get(name)
        if state is None:
            # Adapter not running - just start it
            await self._start_adapter(new_config)
            return

        old_cadence = state.config.cadence_s
        new_cadence = new_config.cadence_s

        # Update config
        state.config = new_config

        # Update adapter's cadence
        state.adapter.cadence_s = new_cadence

        # Update adapter settings if needed (e.g., states list)
        if name == "nws":
            nws_config = self._adapter_config_to_nws_config(new_config)
            state.adapter.states = set(s.upper() for s in nws_config.states)

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

        # Signal the loop to re-evaluate its schedule
        state.cancel_event.set()

    async def _on_config_change(self, table: str, key: str) -> None:
        """Handle a configuration change notification.

        Called when NOTIFY fires for config changes.
        """
        if table != "adapters":
            return

        adapter_name = key
        logger.info(
            "Config change received",
            extra={"table": table, "key": key},
        )

        async with self._lock:
            # Fetch the current config for this adapter
            new_config = await self._config_source.get_adapter(adapter_name)
            current_state = self._adapter_states.get(adapter_name)

            if new_config is None:
                # Adapter was deleted
                if current_state:
                    await self._stop_adapter(adapter_name)
                    logger.info(
                        "Adapter deleted, stopped",
                        extra={"adapter": adapter_name},
                    )
                return

            if not new_config.enabled or new_config.is_paused:
                # Adapter disabled or paused
                if current_state:
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

            if current_state is None:
                # Adapter was enabled or created
                await self._start_adapter(new_config)
                logger.info(
                    "Adapter enabled, started",
                    extra={"adapter": adapter_name},
                )
            else:
                # Adapter config changed (cadence, settings)
                await self._reschedule_adapter(adapter_name, new_config)

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

        # Load and start enabled adapters
        enabled_adapters = await self._config_source.list_enabled_adapters()
        for config in enabled_adapters:
            await self._start_adapter(config)

        # Start config watcher (for DB source, this runs forever; for TOML, returns immediately)
        self._config_watch_task = asyncio.create_task(
            self._config_source.watch_for_changes(self._on_config_change)
        )

        # Start heartbeat
        self._tasks.append(asyncio.create_task(self._heartbeat_loop()))

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

        # Stop all adapters
        for name in list(self._adapter_states.keys()):
            await self._stop_adapter(name)

        # Close config source
        await self._config_source.close()

        await self.disconnect()
        logger.info("Supervisor stopped")


async def async_main() -> None:
    """Async entry point."""
    setup_logging()

    settings = get_settings()
    logger.info(
        "Config source: %s",
        settings.config_source,
        extra={"config_source": settings.config_source},
    )

    # Create config source based on setting
    config_source = await create_config_source(
        source_type=settings.config_source,
        dsn=settings.db_dsn,
        toml_path=settings.config_toml_path,
    )

    # Load CloudEvents config for envelope generation
    # For now, load from TOML regardless of config source
    # (CloudEvents config is not adapter-specific)
    try:
        toml_config = load_config(str(settings.config_toml_path))
        cloudevents_config = toml_config
    except Exception:
        # If TOML doesn't exist and using DB source, create minimal config
        cloudevents_config = None

    supervisor = Supervisor(
        config_source=config_source,
        nats_url=settings.nats_url,
        cloudevents_config=cloudevents_config,
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
