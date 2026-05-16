"""Central archive consumer - JetStream to TimescaleDB."""

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

from central.bootstrap_config import get_settings

CONSUMER_NAME = "archive"
STREAM_NAME = "CENTRAL_WX"
SUBJECT_FILTER = "central.wx.>"
BATCH_SIZE = 100
FETCH_TIMEOUT = 5.0
ACK_WAIT = 30


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


def _build_geom_sql(geo_data: dict[str, Any] | None) -> str | None:
    """Build PostGIS geometry from event geo data."""
    if not geo_data:
        return None

    bbox = geo_data.get("bbox")
    centroid = geo_data.get("centroid")

    if bbox and len(bbox) == 4:
        # Create polygon from bbox
        min_lon, min_lat, max_lon, max_lat = bbox
        return json.dumps({
            "type": "Polygon",
            "coordinates": [[
                [min_lon, min_lat],
                [max_lon, min_lat],
                [max_lon, max_lat],
                [min_lon, max_lat],
                [min_lon, min_lat],
            ]]
        })
    elif centroid and len(centroid) == 2:
        # Create point from centroid
        return json.dumps({
            "type": "Point",
            "coordinates": centroid
        })

    return None


class ArchiveConsumer:
    """Archive consumer process."""

    def __init__(self, nats_url: str, postgres_dsn: str) -> None:
        self._nats_url = nats_url
        self._postgres_dsn = postgres_dsn
        self._nc: nats.NATS | None = None
        self._js: JetStreamContext | None = None
        self._pool: asyncpg.Pool | None = None
        self._shutdown_event = asyncio.Event()

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

    async def _ensure_consumer(self) -> None:
        """Ensure the durable consumer exists."""
        if not self._js:
            return

        try:
            await self._js.consumer_info(STREAM_NAME, CONSUMER_NAME)
            logger.info("Consumer exists", extra={"consumer": CONSUMER_NAME})
        except nats.js.errors.NotFoundError:
            consumer_config = ConsumerConfig(
                durable_name=CONSUMER_NAME,
                deliver_policy=DeliverPolicy.ALL,
                ack_policy=AckPolicy.EXPLICIT,
                ack_wait=ACK_WAIT,
                filter_subject=SUBJECT_FILTER,
            )
            await self._js.add_consumer(STREAM_NAME, consumer_config)
            logger.info("Consumer created", extra={"consumer": CONSUMER_NAME})

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
        source = event_data.get("source", "")
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

        geom_json = _build_geom_sql(geo_data)

        try:
            if geom_json:
                await conn.execute(
                    """
                    INSERT INTO events (id, source, category, time, expires, severity,
                                       geom, regions, primary_region, payload)
                    VALUES ($1, $2, $3, $4, $5, $6,
                            ST_GeomFromGeoJSON($7), $8, $9, $10)
                    ON CONFLICT (id, time) DO UPDATE SET
                        source = EXCLUDED.source,
                        category = EXCLUDED.category,
                        expires = EXCLUDED.expires,
                        severity = EXCLUDED.severity,
                        geom = EXCLUDED.geom,
                        regions = EXCLUDED.regions,
                        primary_region = EXCLUDED.primary_region,
                        payload = EXCLUDED.payload
                    """,
                    event_id, source, category, event_time, expires_time, severity,
                    geom_json, regions, primary_region, json.dumps(envelope)
                )
            else:
                await conn.execute(
                    """
                    INSERT INTO events (id, source, category, time, expires, severity,
                                       geom, regions, primary_region, payload)
                    VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, $8, $9)
                    ON CONFLICT (id, time) DO UPDATE SET
                        source = EXCLUDED.source,
                        category = EXCLUDED.category,
                        expires = EXCLUDED.expires,
                        severity = EXCLUDED.severity,
                        geom = EXCLUDED.geom,
                        regions = EXCLUDED.regions,
                        primary_region = EXCLUDED.primary_region,
                        payload = EXCLUDED.payload
                    """,
                    event_id, source, category, event_time, expires_time, severity,
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

    async def _consume_loop(self) -> None:
        """Main consume loop."""
        if not self._js or not self._pool:
            return

        await self._ensure_consumer()

        sub = await self._js.pull_subscribe(
            SUBJECT_FILTER,
            durable=CONSUMER_NAME,
            stream=STREAM_NAME,
        )

        logger.info(
            "Subscribed to stream",
            extra={"stream": STREAM_NAME, "filter": SUBJECT_FILTER}
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
                logger.exception("Error in consume loop", extra={"error": str(e)})
                await asyncio.sleep(1)

        logger.info("Consume loop stopped")

    async def start(self) -> None:
        """Start the consumer."""
        await self.connect()
        logger.info("Archive consumer ready")

    async def run(self) -> None:
        """Run the consume loop until shutdown."""
        await self._consume_loop()

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
