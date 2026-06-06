"""Central archive consumer - JetStream to TimescaleDB.

Consumes events from multiple NATS JetStream streams and archives them
to TimescaleDB. One durable consumer per stream for independent ack tracking.
"""

import asyncio
import json
import logging
import signal
import sys
from datetime import datetime, timezone
from typing import Any

import asyncpg
import nats
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, DeliverPolicy, AckPolicy
from nats.js.errors import NotFoundError

from central.bootstrap_config import get_settings
from central.monitoring_area import (
    MONITORING_AREA_REFRESH_S,
    MonitoringArea,
    build_geom_json,
    classify_geom,
    load_monitoring_area,
)
from central.streams import STREAMS as STREAM_REGISTRY

# Event-bearing streams to consume -- derived from the registry's event_bearing flag.
# CENTRAL_META is excluded because it carries status messages, not events.
STREAMS = [(s.name, s.subject_filter) for s in STREAM_REGISTRY if s.event_bearing]

BATCH_SIZE = 100
FETCH_TIMEOUT = 5.0
ACK_WAIT = 30


def consumer_name_for(stream: str) -> str:
    """Generate consumer name for a stream."""
    return f"archive-{stream.lower()}"


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


logger = logging.getLogger("central.archive")


class ArchiveConsumer:
    """Archive consumer process."""

    def __init__(self, nats_url: str, postgres_dsn: str) -> None:
        self._nats_url = nats_url
        self._postgres_dsn = postgres_dsn
        self._nc: nats.NATS | None = None
        self._js: JetStreamContext | None = None
        self._pool: asyncpg.Pool | None = None
        self._shutdown_event = asyncio.Event()
        self._monitoring_area: MonitoringArea | None = None
        self._dropped: dict[str, int] = {}

    async def connect(self) -> None:
        """Connect to NATS and PostgreSQL."""
        self._nc = await nats.connect(self._nats_url)
        self._js = self._nc.jetstream()
        logger.info("Connected to NATS", extra={"url": self._nats_url})

        self._pool = await asyncpg.create_pool(
            self._postgres_dsn,
            min_size=1,
            max_size=5,
        )
        logger.info("Connected to PostgreSQL")

    async def disconnect(self) -> None:
        """Disconnect from NATS and PostgreSQL."""
        if self._pool:
            await self._pool.close()
            self._pool = None
        if self._nc:
            await self._nc.drain()
            await self._nc.close()
            self._nc = None
            self._js = None
        logger.info("Disconnected")

    async def _load_monitoring_area(self) -> None:
        """Load (or refresh) the system monitoring-area bbox from config.system.

        On any error keep the last-known value and warn -- the filter must never
        block archiving because a config read failed."""
        if not self._pool:
            return
        try:
            async with self._pool.acquire() as conn:
                self._monitoring_area = await load_monitoring_area(conn)
        except Exception as e:
            logger.warning(
                "Could not load monitoring area; keeping previous value",
                extra={"error": str(e)},
            )

    async def _refresh_monitoring_area_loop(self) -> None:
        """Periodically refresh the monitoring area so GUI edits propagate without
        a restart, and log a rolling summary of dropped (out-of-bounds) events."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=MONITORING_AREA_REFRESH_S
                )
            except asyncio.TimeoutError:
                await self._load_monitoring_area()
                if self._dropped:
                    logger.info(
                        "bbox filter drop summary (cumulative)",
                        extra={"dropped_by_adapter": dict(self._dropped)},
                    )

    async def _cleanup_orphaned_consumer(self) -> None:
        """Remove orphaned 'archive' consumer from CENTRAL_WX if it exists.

        The old single-stream code used a consumer named 'archive' on CENTRAL_WX.
        Now we use 'archive-central_wx' instead. Clean up the old one.
        """
        if not self._js:
            return

        try:
            await self._js.consumer_info("CENTRAL_WX", "archive")
            await self._js.delete_consumer("CENTRAL_WX", "archive")
            logger.info("Removed orphaned 'archive' consumer from CENTRAL_WX")
        except NotFoundError:
            pass  # Already gone or never existed

    async def _ensure_consumer(
        self, stream_name: str, subject_filter: str, consumer_name: str
    ) -> None:
        """Ensure the durable consumer exists for a stream."""
        if not self._js:
            return

        try:
            await self._js.consumer_info(stream_name, consumer_name)
            logger.info(
                "Consumer exists",
                extra={"stream": stream_name, "consumer": consumer_name}
            )
        except NotFoundError:
            consumer_config = ConsumerConfig(
                durable_name=consumer_name,
                deliver_policy=DeliverPolicy.ALL,
                ack_policy=AckPolicy.EXPLICIT,
                ack_wait=ACK_WAIT,
                max_deliver=5,
                filter_subject=subject_filter,
            )
            await self._js.add_consumer(stream_name, consumer_config)
            logger.info(
                "Consumer created",
                extra={"stream": stream_name, "consumer": consumer_name}
            )

    async def _process_message(self, msg: Any, conn: asyncpg.Connection) -> None:
        """Process a single message and insert into database."""
        try:
            envelope = json.loads(msg.data.decode())
        except json.JSONDecodeError as e:
            logger.warning("Invalid JSON in message", extra={"error": str(e)})
            await msg.ack()
            return

        event_data = envelope.get("data", {})
        geo_data = event_data.get("geo")

        event_id = envelope.get("id")
        adapter = event_data.get("adapter", "")
        category = event_data.get("category", "")
        time_str = event_data.get("time")
        expires_str = event_data.get("expires")
        severity = event_data.get("severity")
        regions = event_data.get("geo", {}).get("regions", [])
        primary_region = event_data.get("geo", {}).get("primary_region")

        # Parse timestamps
        event_time = None
        if time_str:
            try:
                event_time = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                pass

        expires_time = None
        if expires_str:
            try:
                expires_time = datetime.fromisoformat(expires_str.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                pass

        if not event_id or not event_time:
            logger.warning(
                "Message missing required fields",
                extra={"id": event_id, "time": time_str}
            )
            await msg.ack()
            return

        geom_json = build_geom_json(geo_data)

        verdict = classify_geom(geom_json, self._monitoring_area)
        if verdict == "out-of-bounds":
            self._dropped[adapter] = self._dropped.get(adapter, 0) + 1
            logger.debug(
                "Dropped out-of-bounds event (archive bbox filter)",
                extra={"id": event_id, "adapter": adapter, "category": category},
            )
            await msg.ack()
            return
        if verdict == "invalid-geom":
            logger.warning(
                "Geom could not be evaluated for bbox filter; archiving",
                extra={"id": event_id, "adapter": adapter},
            )

        try:
            if geom_json:
                await conn.execute(
                    """
                    INSERT INTO events (id, adapter, category, time, expires, severity,
                                       geom, regions, primary_region, payload)
                    VALUES ($1, $2, $3, $4, $5, $6,
                            ST_GeomFromGeoJSON($7), $8, $9, $10)
                    ON CONFLICT (id, time) DO UPDATE SET
                        adapter = EXCLUDED.adapter,
                        category = EXCLUDED.category,
                        expires = EXCLUDED.expires,
                        severity = EXCLUDED.severity,
                        geom = EXCLUDED.geom,
                        regions = EXCLUDED.regions,
                        primary_region = EXCLUDED.primary_region,
                        payload = EXCLUDED.payload
                    """,
                    event_id, adapter, category, event_time, expires_time, severity,
                    geom_json, regions, primary_region, json.dumps(envelope)
                )
            else:
                await conn.execute(
                    """
                    INSERT INTO events (id, adapter, category, time, expires, severity,
                                       geom, regions, primary_region, payload)
                    VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, $8, $9)
                    ON CONFLICT (id, time) DO UPDATE SET
                        adapter = EXCLUDED.adapter,
                        category = EXCLUDED.category,
                        expires = EXCLUDED.expires,
                        severity = EXCLUDED.severity,
                        geom = EXCLUDED.geom,
                        regions = EXCLUDED.regions,
                        primary_region = EXCLUDED.primary_region,
                        payload = EXCLUDED.payload
                    """,
                    event_id, adapter, category, event_time, expires_time, severity,
                    regions, primary_region, json.dumps(envelope)
                )

            await msg.ack()
            logger.info("Archived event", extra={"id": event_id, "category": category})

        except Exception as e:
            logger.error(
                "Failed to insert event",
                extra={"id": event_id, "error": str(e)}
            )
            # Don't ack - let it be redelivered

    async def _consume_stream(
        self, stream_name: str, subject_filter: str, consumer_name: str
    ) -> None:
        """Consume loop for a single stream."""
        if not self._js or not self._pool:
            return

        await self._ensure_consumer(stream_name, subject_filter, consumer_name)

        sub = await self._js.pull_subscribe(
            subject_filter,
            durable=consumer_name,
            stream=stream_name,
        )

        logger.info(
            "Subscribed to stream",
            extra={"stream": stream_name, "filter": subject_filter}
        )

        while not self._shutdown_event.is_set():
            try:
                msgs = await sub.fetch(
                    batch=BATCH_SIZE,
                    timeout=FETCH_TIMEOUT,
                )

                if msgs:
                    async with self._pool.acquire() as conn:
                        for msg in msgs:
                            await self._process_message(msg, conn)

            except nats.errors.TimeoutError:
                # No messages available, continue
                pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(
                    "Error in consume loop",
                    extra={"stream": stream_name, "error": str(e)}
                )
                await asyncio.sleep(1)

        logger.info("Consume loop stopped", extra={"stream": stream_name})

    async def start(self) -> None:
        """Start the consumer."""
        await self.connect()
        await self._cleanup_orphaned_consumer()
        await self._load_monitoring_area()
        area = self._monitoring_area
        logger.info(
            "Archive consumer ready",
            extra={"monitoring_area": (
                {"north": area.north, "south": area.south,
                 "east": area.east, "west": area.west} if area else None
            )},
        )

    async def run(self) -> None:
        """Run consume loops for all streams until shutdown."""
        tasks = []
        tasks.append(
            asyncio.create_task(
                self._refresh_monitoring_area_loop(),
                name="refresh-monitoring-area",
            )
        )
        for stream_name, subject_filter in STREAMS:
            consumer_name = consumer_name_for(stream_name)
            task = asyncio.create_task(
                self._consume_stream(stream_name, subject_filter, consumer_name),
                name=f"consume-{stream_name}",
            )
            tasks.append(task)

        try:
            # Wait for all tasks; if one fails, cancel the others
            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_EXCEPTION,
            )

            # Check for exceptions in completed tasks
            for task in done:
                if task.exception():
                    logger.error(
                        "Stream consumer failed",
                        extra={"task": task.get_name(), "error": str(task.exception())}
                    )

            # Cancel any remaining tasks
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        except asyncio.CancelledError:
            # Shutdown requested, cancel all tasks
            for task in tasks:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def stop(self) -> None:
        """Stop the consumer gracefully."""
        logger.info("Archive consumer shutting down")
        self._shutdown_event.set()
        await self.disconnect()
        logger.info("Archive consumer stopped")


async def async_main() -> None:
    """Async entry point."""
    setup_logging()

    settings = get_settings()
    logger.info(
        "Archive starting",
        extra={
            "nats_url": settings.nats_url,
        },
    )

    consumer = ArchiveConsumer(
        nats_url=settings.nats_url,
        postgres_dsn=settings.db_dsn,
    )

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def handle_signal() -> None:
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal)

    await consumer.start()

    # Run consumer in background
    consume_task = asyncio.create_task(consumer.run())

    # Wait for shutdown signal
    await shutdown_event.wait()

    consumer._shutdown_event.set()
    consume_task.cancel()
    try:
        await consume_task
    except asyncio.CancelledError:
        pass

    await consumer.stop()


def main() -> None:
    """Entry point."""
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
