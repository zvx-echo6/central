"""Database-backed configuration store.

Provides async access to the config schema tables with support for
Postgres LISTEN/NOTIFY for real-time config change notifications.
"""

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import asyncpg

from central.config_models import AdapterConfig, EnrichmentConfig, StreamConfig
from central.crypto import decrypt, encrypt
from central.monitoring_area import MonitoringArea, load_monitoring_areas

logger = logging.getLogger(__name__)


async def _setup_json_codec(conn: asyncpg.Connection) -> None:
    """Set up JSON codec for asyncpg connection."""
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


class ConfigStore:
    """Async interface to the config schema in Postgres."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    @classmethod
    async def create(cls, dsn: str, min_size: int = 1, max_size: int = 5) -> "ConfigStore":
        """Create a ConfigStore with a new connection pool."""
        pool = await asyncpg.create_pool(
            dsn,
            min_size=min_size,
            max_size=max_size,
            init=_setup_json_codec,
        )
        return cls(pool)

    async def close(self) -> None:
        """Close the connection pool."""
        await self._pool.close()

    def get_pool(self) -> asyncpg.Pool:
        """Return the underlying connection pool.

        v0.11.1: introduced for the ``satpass_predict`` adapter which needs
        ad-hoc reads against the ``events`` table to fetch the latest TLE
        per ``norad_id`` (the supervisor's adapter scheduler doesn't pipe
        Postgres access through any of the existing ConfigStore methods).
        Single-method-surface escape hatch -- adapters acquire from the
        pool themselves, no celestrak/tle-specific shape leaks into
        ConfigStore."""
        return self._pool

    # -------------------------------------------------------------------------
    # System configuration
    # -------------------------------------------------------------------------

    async def get_monitoring_areas(self) -> list[MonitoringArea]:
        """Read the system monitoring areas from ``config.monitoring_areas`` (v0.14.0).

        Returns every configured area (empty list = keep everything). Used by
        both archive (INSERT-time filter) and supervisor (publish-time filter)
        to apply set-union bbox semantics.
        """
        async with self._pool.acquire() as conn:
            return await load_monitoring_areas(conn)

    async def get_monitoring_area(self) -> MonitoringArea | None:
        """Single-area back-compat shim (pre-v0.14.0): the first configured area.

        Reads the new ``config.monitoring_areas`` table and returns its first row
        (by name) or ``None`` when the list is empty, mirroring the old
        ``config.system`` single-bbox read for any lingering single-area caller.
        """
        areas = await self.get_monitoring_areas()
        return areas[0] if areas else None

    # -------------------------------------------------------------------------
    # Adapter configuration
    # -------------------------------------------------------------------------

    async def get_adapter(self, name: str) -> AdapterConfig | None:
        """Get configuration for a specific adapter."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT name, kind, enabled, cadence_s, settings, paused_at, updated_at
                FROM config.adapters
                WHERE name = $1
                """,
                name,
            )
        if row is None:
            return None
        return AdapterConfig(**dict(row))

    async def list_adapters(self) -> list[AdapterConfig]:
        """List all configured adapters."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT name, kind, enabled, cadence_s, settings, paused_at, updated_at
                FROM config.adapters
                ORDER BY name
                """
            )
        return [AdapterConfig(**dict(row)) for row in rows]

    async def upsert_adapter(
        self,
        name: str,
        enabled: bool,
        cadence_s: int,
        settings: dict[str, Any],
    ) -> None:
        """Insert or update an adapter configuration."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO config.adapters (name, enabled, cadence_s, settings, updated_at)
                VALUES ($1, $2, $3, $4, now())
                ON CONFLICT (name) DO UPDATE SET
                    enabled = EXCLUDED.enabled,
                    cadence_s = EXCLUDED.cadence_s,
                    settings = EXCLUDED.settings,
                    updated_at = now()
                """,
                name,
                enabled,
                cadence_s,
                settings,  # Will be encoded as JSON by the codec
            )

    async def pause_adapter(self, name: str) -> None:
        """Pause an adapter by setting paused_at."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE config.adapters
                SET paused_at = now(), updated_at = now()
                WHERE name = $1
                """,
                name,
            )

    async def unpause_adapter(self, name: str) -> None:
        """Unpause an adapter by clearing paused_at."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE config.adapters
                SET paused_at = NULL, updated_at = now()
                WHERE name = $1
                """,
                name,
            )

    # -------------------------------------------------------------------------
    # Enrichment configuration (single-row config.enrichment)
    # -------------------------------------------------------------------------

    async def get_enrichment_config(self) -> EnrichmentConfig:
        """Read the single config.enrichment row.

        Falls back to EnrichmentConfig() framework defaults if the row is
        somehow absent (it is migration-seeded, so this is belt-and-suspenders).
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT enricher_class, backend_class, backend_settings, cache_ttl_s
                FROM config.enrichment
                WHERE id = true
                """
            )
        if row is None:
            return EnrichmentConfig()
        return EnrichmentConfig(**dict(row))

    async def upsert_enrichment_config(self, config: EnrichmentConfig) -> None:
        """Write the single config.enrichment row (id = true)."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO config.enrichment
                    (id, enricher_class, backend_class, backend_settings, cache_ttl_s)
                VALUES (true, $1, $2, $3, $4)
                ON CONFLICT (id) DO UPDATE SET
                    enricher_class = EXCLUDED.enricher_class,
                    backend_class = EXCLUDED.backend_class,
                    backend_settings = EXCLUDED.backend_settings,
                    cache_ttl_s = EXCLUDED.cache_ttl_s
                """,
                config.enricher_class,
                config.backend_class,
                config.backend_settings,  # JSON-encoded by the codec
                config.cache_ttl_s,
            )

    # -------------------------------------------------------------------------
    # Stream configuration
    # -------------------------------------------------------------------------

    async def get_stream(self, name: str) -> StreamConfig | None:
        """Get configuration for a specific stream."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT name, max_age_s, max_bytes, managed_max_bytes, updated_at
                FROM config.streams
                WHERE name = $1
                """,
                name,
            )
        if row is None:
            return None
        return StreamConfig(**dict(row))

    async def list_streams(self) -> list[StreamConfig]:
        """List all configured streams."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT name, max_age_s, max_bytes, managed_max_bytes, updated_at
                FROM config.streams
                ORDER BY name
                """
            )
        return [StreamConfig(**dict(row)) for row in rows]

    # -------------------------------------------------------------------------
    # Archived-events retention (v0.9.13) -- DML on public.events, keyed by the
    # per-stream max_age_s above. Lives here because this is the supervisor's
    # Postgres gateway; the events table shares the database.
    # -------------------------------------------------------------------------

    async def delete_events_older_than(
        self, category_domains: list[str], max_age_s: int, batch: int = 10000
    ) -> int:
        """Delete archived events whose category first-token is in
        ``category_domains`` and whose ``time`` is older than ``max_age_s`` seconds.

        Deletes in batches (default 10k) so the initial bulk reclaim can't take a
        long table lock. Returns the total number of rows deleted."""
        if not category_domains:
            return 0
        total = 0
        async with self._pool.acquire() as conn:
            while True:
                # NOTE: events is a TimescaleDB hypertable -- ctid is only unique
                # WITHIN a chunk, so batching on ctid would delete same-ctid rows in
                # other chunks. Batch on the composite primary key (id, time), which
                # is globally unique.
                result = await conn.execute(
                    """
                    DELETE FROM events WHERE (id, time) IN (
                        SELECT id, time FROM events
                        WHERE split_part(category, '.', 1) = ANY($1::text[])
                          AND time < now() - ($2 * interval '1 second')
                        LIMIT $3
                    )
                    """,
                    category_domains, max_age_s, batch,
                )
                n = int(result.split()[-1])
                total += n
                if n < batch:
                    break
        return total

    async def unmapped_event_domains(self, mapped: list[str]) -> list[str]:
        """Return distinct ``events.category`` first-token domains NOT in ``mapped``.

        Used to surface category domains that no stream's retention map covers, so
        a new adapter's events can't silently dodge retention."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT split_part(category, '.', 1) AS domain
                FROM events
                WHERE split_part(category, '.', 1) <> ALL($1::text[])
                """,
                mapped,
            )
        return [r["domain"] for r in rows]

    async def upsert_stream(self, name: str, max_age_s: int) -> None:
        """Insert or update a stream's max_age_s (operator-facing)."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO config.streams (name, max_age_s, updated_at)
                VALUES ($1, $2, now())
                ON CONFLICT (name) DO UPDATE SET
                    max_age_s = EXCLUDED.max_age_s,
                    updated_at = now()
                """,
                name,
                max_age_s,
            )

    async def update_stream_max_bytes(self, name: str, max_bytes: int) -> None:
        """Update a stream's max_bytes (supervisor-internal).

        This update only touches max_bytes, which does NOT trigger
        the column-filtered NOTIFY (only max_age_s changes fire NOTIFY).
        """
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE config.streams
                SET max_bytes = $2, updated_at = now()
                WHERE name = $1
                """,
                name,
                max_bytes,
            )

    # -------------------------------------------------------------------------
    # API key management
    # -------------------------------------------------------------------------

    async def set_api_key(self, alias: str, plaintext_value: str) -> None:
        """Store an API key, encrypting it with the master key."""
        encrypted = encrypt(plaintext_value.encode("utf-8"))
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO config.api_keys (alias, encrypted_value)
                VALUES ($1, $2)
                ON CONFLICT (alias) DO UPDATE SET
                    encrypted_value = EXCLUDED.encrypted_value,
                    rotated_at = now()
                """,
                alias,
                encrypted,
            )

    async def get_api_key(self, alias: str) -> str | None:
        """Retrieve and decrypt an API key by alias."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT encrypted_value FROM config.api_keys WHERE alias = $1
                """,
                alias,
            )
            if row is not None:
                # Update last_used_at
                await conn.execute(
                    """
                    UPDATE config.api_keys SET last_used_at = now() WHERE alias = $1
                    """,
                    alias,
                )
        if row is None:
            return None
        return decrypt(row["encrypted_value"]).decode("utf-8")

    async def delete_api_key(self, alias: str) -> bool:
        """Delete an API key. Returns True if key existed."""
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM config.api_keys WHERE alias = $1", alias
            )
        return result == "DELETE 1"

    async def set_adapter_last_error(self, name: str, error: str | None) -> None:
        """Set or clear the last_error field on an adapter row."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE config.adapters SET last_error = $1 WHERE name = $2",
                error, name,
            )

    # -------------------------------------------------------------------------
    # Change notifications
    # -------------------------------------------------------------------------

    async def listen_for_changes(
        self,
        callback: Callable[[str, str], Awaitable[None] | None],
    ) -> None:
        """Listen for config changes via Postgres NOTIFY.

        Runs forever, calling callback(table, key) each time a change is
        detected. The callback can be sync or async.

        On connection loss, automatically reconnects with exponential backoff.
        Cancellation (via task.cancel()) propagates cleanly.

        Args:
            callback: Function called with (table_name, row_key) on each change.
        """
        backoff = 1.0
        max_backoff = 30.0

        while True:
            conn = None
            try:
                conn = await self._pool.acquire()
                logger.info("Config listener connected to database")
                backoff = 1.0  # Reset backoff on successful connect

                def notification_handler(
                    conn: asyncpg.Connection,
                    pid: int,
                    channel: str,
                    payload: str,
                ) -> None:
                    # payload format: "table_name:key"
                    if ":" in payload:
                        table, key = payload.split(":", 1)
                    else:
                        table, key = payload, ""

                    result = callback(table, key)
                    if asyncio.iscoroutine(result):
                        asyncio.create_task(result)

                await conn.add_listener("config_changed", notification_handler)

                try:
                    # Keep connection alive with periodic keepalive
                    while True:
                        await asyncio.sleep(60)
                        await conn.execute("SELECT 1")
                finally:
                    await conn.remove_listener("config_changed", notification_handler)

            except asyncio.CancelledError:
                # Cancellation must propagate cleanly
                logger.info("Config listener cancelled")
                raise

            except (
                asyncpg.PostgresConnectionError,
                asyncpg.InterfaceError,
                ConnectionResetError,
                OSError,
            ) as e:
                logger.warning(
                    "Config listener connection lost, reconnecting in %.1fs: %s",
                    backoff,
                    e,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

            except Exception as e:
                # Unexpected error - log and retry with backoff
                logger.exception(
                    "Config listener unexpected error, reconnecting in %.1fs",
                    backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

            finally:
                if conn is not None:
                    try:
                        await self._pool.release(conn)
                    except Exception:
                        pass  # Connection may already be invalid
