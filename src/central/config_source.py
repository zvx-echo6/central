"""Configuration source abstraction.

Provides a unified interface for loading adapter configuration from
either TOML files or the database-backed config store.
"""

import logging
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import tomllib

from central.config import NWSAdapterConfig
from central.config_models import AdapterConfig
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

    async def watch_for_changes(
        self,
        callback: Callable[[str, str], Awaitable[None] | None],
    ) -> None:
        """Watch for configuration changes.

        For TOML source, this is a no-op (returns immediately).
        For DB source, this runs forever, calling callback(table, key) on changes.
        """
        ...

    async def close(self) -> None:
        """Clean up resources."""
        ...


class TomlConfigSource:
    """Configuration source backed by a TOML file.

    This is the legacy configuration path. Does not support hot-reload.
    """

    def __init__(self, toml_path: Path) -> None:
        self._toml_path = toml_path
        self._adapters: dict[str, AdapterConfig] = {}
        self._loaded = False

    def _load(self) -> None:
        """Load configuration from TOML file."""
        if self._loaded:
            return

        with self._toml_path.open("rb") as f:
            data = tomllib.load(f)

        adapters_raw = data.get("adapters", {})
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)

        for name, adapter_data in adapters_raw.items():
            # Convert TOML adapter config to unified AdapterConfig
            # TOML uses NWSAdapterConfig shape, we need to convert to AdapterConfig
            enabled = adapter_data.get("enabled", True)
            cadence_s = adapter_data.get("cadence_s", 60)

            # Extract settings (everything except enabled/cadence_s)
            settings = {
                k: v
                for k, v in adapter_data.items()
                if k not in ("enabled", "cadence_s")
            }

            self._adapters[name] = AdapterConfig(
                name=name,
                enabled=enabled,
                cadence_s=cadence_s,
                settings=settings,
                paused_at=None,
                updated_at=now,
            )

        self._loaded = True
        logger.info(
            "Loaded TOML config",
            extra={"path": str(self._toml_path), "adapters": list(self._adapters.keys())},
        )

    async def list_enabled_adapters(self) -> list[AdapterConfig]:
        """List all enabled adapters from TOML."""
        self._load()
        return [a for a in self._adapters.values() if a.enabled and not a.is_paused]

    async def get_adapter(self, name: str) -> AdapterConfig | None:
        """Get a specific adapter from TOML."""
        self._load()
        return self._adapters.get(name)

    async def watch_for_changes(
        self,
        callback: Callable[[str, str], Awaitable[None] | None],
    ) -> None:
        """TOML does not support hot-reload. Returns immediately."""
        logger.debug("TOML config source does not support hot-reload")
        return

    async def close(self) -> None:
        """No resources to clean up for TOML source."""
        pass


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


async def create_config_source(
    source_type: str,
    dsn: str | None = None,
    toml_path: Path | None = None,
) -> ConfigSource:
    """Factory function to create the appropriate config source.

    Args:
        source_type: "toml" or "db"
        dsn: PostgreSQL DSN (required for "db")
        toml_path: Path to TOML file (required for "toml")

    Returns:
        ConfigSource implementation
    """
    if source_type == "toml":
        if toml_path is None:
            raise ValueError("toml_path required for toml config source")
        return TomlConfigSource(toml_path)
    elif source_type == "db":
        if dsn is None:
            raise ValueError("dsn required for db config source")
        return await DbConfigSource.create(dsn)
    else:
        raise ValueError(f"Unknown config source type: {source_type}")
