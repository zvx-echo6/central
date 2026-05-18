"""Pydantic models for database-backed configuration."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, model_validator


class RegionConfig(BaseModel):
    """Geographic bounding box for adapter region filtering."""
    
    north: float = Field(ge=-90, le=90, description="Northern latitude bound")
    south: float = Field(ge=-90, le=90, description="Southern latitude bound")
    east: float = Field(ge=-180, le=180, description="Eastern longitude bound")
    west: float = Field(ge=-180, le=180, description="Western longitude bound")

    @model_validator(mode="after")
    def validate_bounds(self) -> "RegionConfig":
        if self.north <= self.south:
            raise ValueError(
                f"north ({self.north}) must be greater than south ({self.south})"
            )
        if self.east == self.west:
            raise ValueError("east and west cannot be equal (zero-width bbox)")
        if self.east < self.west:
            raise ValueError("antimeridian-crossing bboxes not supported")
        return self


class AdapterConfig(BaseModel):
    """Configuration for a single adapter."""

    name: str = Field(description="Unique adapter identifier")
    enabled: bool = Field(default=True, description="Whether adapter is active")
    cadence_s: int = Field(ge=10, description="Poll interval in seconds")
    settings: dict[str, Any] = Field(
        default_factory=dict, description="Adapter-specific settings"
    )
    paused_at: datetime | None = Field(
        default=None, description="When adapter was paused, if paused"
    )
    updated_at: datetime = Field(description="Last configuration update time")

    @property
    def is_paused(self) -> bool:
        """Check if adapter is currently paused."""
        return self.paused_at is not None


class StreamConfig(BaseModel):
    """Configuration for a JetStream stream."""
    
    name: str = Field(description="Stream name")
    max_age_s: int = Field(description="Maximum message age in seconds")
    max_bytes: int = Field(description="Maximum stream size in bytes")
    managed_max_bytes: bool = Field(
        default=True, description="Whether max_bytes is auto-managed by supervisor"
    )
    updated_at: datetime = Field(description="Last configuration update time")


class ApiKeyInfo(BaseModel):
    """Metadata about an API key (without the decrypted value)."""

    alias: str = Field(description="Key identifier/alias")
    created_at: datetime = Field(description="When key was created")
    rotated_at: datetime | None = Field(
        default=None, description="Last rotation time"
    )
    last_used_at: datetime | None = Field(
        default=None, description="Last usage time"
    )
