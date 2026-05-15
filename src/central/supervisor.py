"""Central supervisor - adapter scheduler and event publisher."""

import asyncio
import json
import logging
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import nats
from nats.js import JetStreamContext

from central.adapters.nws import NWSAdapter
from central.cloudevents_wire import wrap_event
from central.config import load_config, Config
from central.models import subject_for_event

CURSOR_DB_PATH = Path("/var/lib/central/cursors.db")
CONFIG_PATH = "/etc/central/central.toml"


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
        # Include any extra fields passed via extra={}
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


class Supervisor:
    """Main supervisor process."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self._nc: nats.NATS | None = None
        self._js: JetStreamContext | None = None
        self._adapters: list[NWSAdapter] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._shutdown_event = asyncio.Event()
        self._start_time = datetime.now(timezone.utc)

    async def connect(self) -> None:
        """Connect to NATS."""
        self._nc = await nats.connect(self.config.nats.url)
        self._js = self._nc.jetstream()
        logger.info("Connected to NATS", extra={"url": self.config.nats.url})

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

    async def _run_adapter(self, adapter: NWSAdapter) -> None:
        """Run an adapter poll loop."""
        while not self._shutdown_event.is_set():
            poll_start = datetime.now(timezone.utc)
            try:
                async for event in adapter.poll():
                    # Dedup check
                    if adapter.is_published(event.id):
                        adapter.bump_last_seen(event.id)
                        continue

                    # Build CloudEvent
                    envelope, msg_id = wrap_event(event, self.config)
                    subject = subject_for_event(event)

                    # Publish
                    await self._publish_event(subject, envelope, msg_id)
                    adapter.mark_published(event.id)

                    logger.info(
                        "Published event",
                        extra={"id": event.id, "subject": subject, "category": event.category}
                    )

                # Publish success status
                await self._publish_meta(
                    f"central.meta.adapter.{adapter.name}.status",
                    {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}
                )

            except Exception as e:
                logger.exception("Adapter poll failed", extra={"adapter": adapter.name})
                await self._publish_meta(
                    f"central.meta.adapter.{adapter.name}.status",
                    {
                        "ok": False,
                        "error": str(e),
                        "ts": datetime.now(timezone.utc).isoformat()
                    }
                )

            # Sweep old IDs
            swept = adapter.sweep_old_ids()
            if swept > 0:
                logger.info("Swept old published IDs", extra={"count": swept})

            # Sleep until next cadence
            elapsed = (datetime.now(timezone.utc) - poll_start).total_seconds()
            sleep_time = max(0, adapter.cadence_s - elapsed)
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=sleep_time
                )
            except asyncio.TimeoutError:
                pass

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

        # Initialize adapters
        if self.config.adapters.get("nws") and self.config.adapters["nws"].enabled:
            adapter = NWSAdapter(
                config=self.config.adapters["nws"],
                cursor_db_path=CURSOR_DB_PATH,
            )
            await adapter.startup()
            self._adapters.append(adapter)
            logger.info("NWS adapter initialized")

        # Start adapter tasks
        for adapter in self._adapters:
            task = asyncio.create_task(self._run_adapter(adapter))
            self._tasks.append(task)

        # Start heartbeat
        self._tasks.append(asyncio.create_task(self._heartbeat_loop()))

        logger.info("Supervisor started", extra={"adapters": [a.name for a in self._adapters]})

    async def stop(self) -> None:
        """Stop the supervisor gracefully."""
        logger.info("Supervisor shutting down")
        self._shutdown_event.set()

        # Cancel tasks
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Shutdown adapters
        for adapter in self._adapters:
            await adapter.shutdown()

        await self.disconnect()
        logger.info("Supervisor stopped")


async def async_main() -> None:
    """Async entry point."""
    setup_logging()

    config = load_config(CONFIG_PATH)
    supervisor = Supervisor(config)

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
