"""Configuration loader for Central."""

import tomllib
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator


class NWSAdapterConfig(BaseModel):
    """NWS adapter configuration."""
    
    model_config = ConfigDict(extra="forbid")
    
    enabled: bool = True
    cadence_s: int = 60
    states: list[str] = []
    contact_email: str
    
    @field_validator("contact_email")
    @classmethod
    def validate_contact_email(cls, v: str) -> str:
        if "CHANGE_ME" in v or v.endswith("example.invalid"):
            raise ValueError(
                f"adapters.nws.contact_email is still a placeholder: {v!r}"
            )
        return v


class CloudEventsConfig(BaseModel):
    """CloudEvents envelope configuration."""
    
    model_config = ConfigDict(extra="forbid")
    
    type_prefix: str = "central"
    source: str = "central"
    schema_version: str = "1.0"


class NATSConfig(BaseModel):
    """NATS connection configuration."""
    
    model_config = ConfigDict(extra="forbid")
    
    url: str = "nats://localhost:4222"


class PostgresConfig(BaseModel):
    """PostgreSQL connection configuration."""
    
    model_config = ConfigDict(extra="forbid")
    
    dsn: str
    
    @field_validator("dsn")
    @classmethod
    def validate_dsn(cls, v: str) -> str:
        if "CHANGE_ME" in v:
            raise ValueError(f"postgres.dsn contains placeholder: {v!r}")
        return v


class Config(BaseModel):
    """Root configuration model."""
    
    model_config = ConfigDict(extra="forbid")
    
    adapters: dict[str, NWSAdapterConfig]
    cloudevents: CloudEventsConfig
    nats: NATSConfig
    postgres: PostgresConfig


def _check_placeholders(data: dict[str, Any], path: str = "") -> None:
    """Recursively check for CHANGE_ME placeholders in config data."""
    for key, value in data.items():
        current_path = f"{path}.{key}" if path else key
        if isinstance(value, str) and "CHANGE_ME" in value:
            raise ValueError(
                f"Configuration field {current_path!r} contains placeholder: {value!r}"
            )
        elif isinstance(value, dict):
            _check_placeholders(value, current_path)


def load_config(path: str = "/etc/central/central.toml") -> Config:
    """
    Load and validate configuration from TOML file.
    
    Raises ValueError if any field contains CHANGE_ME placeholder.
    """
    config_path = Path(path)
    
    with config_path.open("rb") as f:
        data = tomllib.load(f)
    
    # Check for any remaining placeholders before Pydantic validation
    _check_placeholders(data)
    
    # Parse adapters section
    adapters_raw = data.get("adapters", {})
    adapters = {}
    for name, adapter_data in adapters_raw.items():
        adapters[name] = NWSAdapterConfig(**adapter_data)
    
    return Config(
        adapters=adapters,
        cloudevents=CloudEventsConfig(**data.get("cloudevents", {})),
        nats=NATSConfig(**data.get("nats", {})),
        postgres=PostgresConfig(**data.get("postgres", {})),
    )
