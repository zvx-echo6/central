"""NATS connection for GUI."""

import logging

import nats
from nats.js import JetStreamContext

logger = logging.getLogger(__name__)

_nc: nats.NATS | None = None
_js: JetStreamContext | None = None


async def init_nats(url: str) -> JetStreamContext | None:
    """Initialize the NATS connection and JetStream context."""
    global _nc, _js
    if _nc is None:
        try:
            _nc = await nats.connect(url)
            _js = _nc.jetstream()
            logger.info("Connected to NATS", extra={"url": url})
        except Exception as e:
            logger.warning("Failed to connect to NATS", extra={"error": str(e)})
            _nc = None
            _js = None
    return _js


def get_js() -> JetStreamContext | None:
    """Get the JetStream context. Returns None if not connected."""
    return _js


async def close_nats() -> None:
    """Close the NATS connection."""
    global _nc, _js
    if _nc is not None:
        try:
            await _nc.drain()
            await _nc.close()
            logger.info("Disconnected from NATS")
        except Exception as e:
            logger.warning("Error closing NATS", extra={"error": str(e)})
        finally:
            _nc = None
            _js = None
