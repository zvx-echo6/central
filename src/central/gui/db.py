"""Database connection pool for GUI."""

import json
from typing import Any

import asyncpg

# Module-level pool instance
_pool: asyncpg.Pool | None = None


# TODO: Deduplicate with central.config_store._setup_json_codec
async def _setup_json_codec(conn: asyncpg.Connection) -> None:
    """Set up JSON codec for asyncpg connection."""
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def init_pool(dsn: str) -> asyncpg.Pool:
    """Initialize the connection pool."""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn,
            min_size=1,
            max_size=5,
            init=_setup_json_codec,
        )
    return _pool


def get_pool() -> asyncpg.Pool:
    """Get the connection pool. Must call init_pool first."""
    if _pool is None:
        raise RuntimeError("Database pool not initialized. Call init_pool first.")
    return _pool


async def close_pool() -> None:
    """Close the connection pool."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
