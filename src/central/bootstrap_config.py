"""Bootstrap configuration from environment variables.

This module provides early-stage configuration loading from environment
variables or a .env file. Used before the database-backed config store
is available.
"""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Bootstrap settings loaded from environment or .env file."""

    model_config = SettingsConfigDict(
        env_prefix="CENTRAL_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    db_dsn: str = Field(description="PostgreSQL connection string")
    nats_url: str = Field(default="nats://localhost:4222", description="NATS server URL")
    master_key_path: Path = Field(
        default=Path("/etc/central/master.key"),
        description="Path to AES-256 master key file",
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        description="Logging level",
    )


@lru_cache
def get_settings(env_file: Path | None = None) -> Settings:
    """Load settings, optionally from a specific .env file.

    Results are cached. Call get_settings.cache_clear() to reload.
    """
    if env_file is not None:
        return Settings(_env_file=env_file)
    return Settings()
