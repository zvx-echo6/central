"""Configuration source abstraction.

Provides a unified interface for loading adapter configuration from
the database-backed config store.
"""

import logging
from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

from central.config_models import AdapterConfig, EnrichmentConfig
from central.config_store import ConfigStore

logger = logging.getLogger(__name__)


@runtime_checkable
class ConfigSource(Protocol):
    """Protocol for configuration sources."""

    async def list_enabled_adapters(self) -> list[AdapterConfig]:
        """List all enabled adapters."""
        ...

    async def get_adapter(self, name: str) -> AdapterConfig | None:
        """Get configuration for a specific adapter."""
        ...

    async def get_enrichment_config(self) -> EnrichmentConfig:
        """Get the enrichment configuration."""
        ...

    async def watch_for_changes(
        self,
        callback: Callable[[str, str], Awaitable[None] | None],
    ) -> None:
        """Watch for configuration changes.

        Runs forever, calling callback(table, key) on changes.
        """
        ...

    async def close(self) -> None:
        """Clean up resources."""
        ...


class DbConfigSource:
    """Configuration source backed by the Postgres config store.

    Supports hot-reload via LISTEN/NOTIFY.
    """

    def __init__(self, config_store: ConfigStore) -> None:
        self._store = config_store

    @classmethod
    async def create(cls, dsn: str) -> "DbConfigSource":
        """Create a DbConfigSource with a new ConfigStore."""
        store = await ConfigStore.create(dsn)
        return cls(store)

    async def list_enabled_adapters(self) -> list[AdapterConfig]:
        """List all enabled adapters from database."""
        all_adapters = await self._store.list_adapters()
        return [a for a in all_adapters if a.enabled and not a.is_paused]

    async def get_adapter(self, name: str) -> AdapterConfig | None:
        """Get a specific adapter from database."""
        return await self._store.get_adapter(name)

    async def get_enrichment_config(self) -> EnrichmentConfig:
        """Get the enrichment configuration from database."""
        return await self._store.get_enrichment_config()

    async def watch_for_changes(
        self,
        callback: Callable[[str, str], Awaitable[None] | None],
    ) -> None:
        """Watch for changes via Postgres LISTEN/NOTIFY.

        Runs forever, calling callback(table, key) on each change.
        """
        await self._store.listen_for_changes(callback)

    async def close(self) -> None:
        """Close the underlying config store."""
        await self._store.close()
