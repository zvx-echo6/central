"""Database-backed configuration store.

Provides async access to the config schema tables with support for
Postgres LISTEN/NOTIFY for real-time config change notifications.
"""

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

import asyncpg

from central.config_models import AdapterConfig
from central.crypto import decrypt, encrypt


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

    # -------------------------------------------------------------------------
    # Adapter configuration
    # -------------------------------------------------------------------------

    async def get_adapter(self, name: str) -> AdapterConfig | None:
        """Get configuration for a specific adapter."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT name, enabled, cadence_s, settings, paused_at, updated_at
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
                SELECT name, enabled, cadence_s, settings, paused_at, updated_at
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

        Args:
            callback: Function called with (table_name, row_key) on each change.
        """
        conn = await self._pool.acquire()
        try:

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

            # Keep connection alive
            while True:
                await asyncio.sleep(60)
                # Periodic keepalive query
                await conn.execute("SELECT 1")

        finally:
            await conn.remove_listener("config_changed", notification_handler)
            await self._pool.release(conn)
