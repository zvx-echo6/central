"""JetStream stream manager for retention configuration."""

import logging
import re
from pathlib import Path
from typing import Any

from nats.js import JetStreamContext
from nats.js.api import StreamConfig, DiscardPolicy, RetentionPolicy

from central.config_models import StreamConfig as StreamConfigModel

logger = logging.getLogger(__name__)

# Constants
ONE_GB = 1024 * 1024 * 1024  # 1 GiB in bytes
NATS_CONFIG_PATH = Path("/etc/nats/nats-server.conf")


class StreamManager:
    """Manages JetStream stream configuration and retention."""

    def __init__(self, js: JetStreamContext) -> None:
        self._js = js
        self._server_max_file_store: int | None = None

    async def server_max_file_store_bytes(self) -> int:
        """Get the server's max_file_store setting in bytes.

        Parses the NATS server config file and caches the result.
        Returns a default of 20GB if config cannot be read.
        """
        if self._server_max_file_store is not None:
            return self._server_max_file_store

        default_value = 20 * ONE_GB  # 20GB default

        try:
            config_text = NATS_CONFIG_PATH.read_text()

            # Parse max_file_store value (supports GB/MB/KB suffixes)
            match = re.search(r'max_file_store:\s*(\d+)(GB|MB|KB|G|M|K)?', config_text, re.IGNORECASE)
            if match:
                value = int(match.group(1))
                suffix = (match.group(2) or "").upper()

                if suffix in ("GB", "G"):
                    value *= ONE_GB
                elif suffix in ("MB", "M"):
                    value *= 1024 * 1024
                elif suffix in ("KB", "K"):
                    value *= 1024
                # else: assume bytes

                self._server_max_file_store = value
                logger.info(
                    "Parsed server max_file_store",
                    extra={"max_file_store_bytes": value},
                )
                return value

            logger.warning(
                "max_file_store not found in config, using default",
                extra={"default": default_value},
            )
            self._server_max_file_store = default_value
            return default_value

        except Exception as e:
            logger.warning(
                "Failed to read NATS config, using default",
                extra={"error": str(e), "default": default_value},
            )
            self._server_max_file_store = default_value
            return default_value

    def _compute_ceiling(self, server_max: int) -> int:
        """Compute per-stream ceiling as 30% of server max_file_store."""
        return int(server_max * 0.30)

    async def ensure_stream(
        self,
        name: str,
        subjects: list[str],
        config: StreamConfigModel,
    ) -> None:
        """Ensure a stream exists with the given configuration.

        Creates the stream if it doesn't exist, or updates it if it does.
        Always enforces: discard=old, max_msgs=-1 (unlimited).
        """
        server_max = await self.server_max_file_store_bytes()
        ceiling = self._compute_ceiling(server_max)

        # Clamp max_bytes to [1GB, ceiling]
        max_bytes = max(ONE_GB, min(config.max_bytes, ceiling))

        stream_config = StreamConfig(
            name=name,
            subjects=subjects,
            retention=RetentionPolicy.LIMITS,
            discard=DiscardPolicy.OLD,
            max_age=config.max_age_s,
            max_bytes=max_bytes,
            max_msgs=-1,  # Unlimited messages
        )

        try:
            # Try to get existing stream
            existing = await self._js.stream_info(name)

            # Update if config differs
            await self._js.update_stream(config=stream_config)
            logger.info(
                "Updated stream",
                extra={
                    "stream": name,
                    "max_age_s": config.max_age_s,
                    "max_bytes": max_bytes,
                },
            )

        except Exception as e:
            if "stream not found" in str(e).lower():
                # Create new stream
                await self._js.add_stream(config=stream_config)
                logger.info(
                    "Created stream",
                    extra={
                        "stream": name,
                        "subjects": subjects,
                        "max_age_s": config.max_age_s,
                        "max_bytes": max_bytes,
                    },
                )
            else:
                raise

    async def apply_retention(self, name: str, config: StreamConfigModel) -> None:
        """Apply retention settings to an existing stream.

        Updates max_age and max_bytes. Always enforces discard=old, max_msgs=-1.
        """
        server_max = await self.server_max_file_store_bytes()
        ceiling = self._compute_ceiling(server_max)

        # Clamp max_bytes to [1GB, ceiling]
        max_bytes = max(ONE_GB, min(config.max_bytes, ceiling))

        try:
            # Get current stream config
            info = await self._js.stream_info(name)
            current = info.config

            # Build updated config
            updated = StreamConfig(
                name=name,
                subjects=current.subjects,
                retention=RetentionPolicy.LIMITS,
                discard=DiscardPolicy.OLD,
                max_age=config.max_age_s,
                max_bytes=max_bytes,
                max_msgs=-1,
            )

            await self._js.update_stream(config=updated)
            logger.info(
                "Applied retention",
                extra={
                    "stream": name,
                    "max_age_s": config.max_age_s,
                    "max_bytes": max_bytes,
                },
            )

        except Exception as e:
            logger.error(
                "Failed to apply retention",
                extra={"stream": name, "error": str(e)},
            )
            raise

    async def recompute_max_bytes(self, name: str, max_age_s: int) -> int:
        """Recompute max_bytes based on observed throughput.

        Formula: rate × max_age × 1.5 safety margin, clamped to [1GB, ceiling].

        Returns the computed max_bytes value.
        """
        server_max = await self.server_max_file_store_bytes()
        ceiling = self._compute_ceiling(server_max)

        try:
            info = await self._js.stream_info(name)
            current_bytes = info.state.bytes
            current_msgs = info.state.messages

            # Get stream age from first message
            first_seq = info.state.first_seq
            last_seq = info.state.last_seq

            if current_msgs == 0 or last_seq == 0:
                # No messages yet, use floor
                return ONE_GB

            # Estimate message age span (approximation)
            # Use stream's configured max_age as the observation window
            configured_max_age = info.config.max_age

            if configured_max_age > 0:
                # Rate = current_bytes / configured_max_age (in seconds)
                rate_per_second = current_bytes / configured_max_age
            else:
                # Fallback: assume 1 day of data
                rate_per_second = current_bytes / 86400

            # Project bytes needed for new max_age with 1.5x safety margin
            projected = int(rate_per_second * max_age_s * 1.5)

            # Clamp to [1GB, ceiling]
            result = max(ONE_GB, min(projected, ceiling))

            logger.info(
                "Recomputed max_bytes",
                extra={
                    "stream": name,
                    "current_bytes": current_bytes,
                    "rate_per_second": rate_per_second,
                    "max_age_s": max_age_s,
                    "projected": projected,
                    "result": result,
                    "ceiling": ceiling,
                },
            )

            return result

        except Exception as e:
            logger.error(
                "Failed to recompute max_bytes, using floor",
                extra={"stream": name, "error": str(e)},
            )
            return ONE_GB

    async def get_stream_stats(self, name: str) -> dict[str, Any]:
        """Get current stream statistics for monitoring."""
        try:
            info = await self._js.stream_info(name)
            return {
                "stream": name,
                "bytes": info.state.bytes,
                "messages": info.state.messages,
                "max_bytes": info.config.max_bytes,
                "max_age_s": info.config.max_age,
                "consumers": info.state.consumer_count,
            }
        except Exception as e:
            logger.error(
                "Failed to get stream stats",
                extra={"stream": name, "error": str(e)},
            )
            return {"stream": name, "error": str(e)}
