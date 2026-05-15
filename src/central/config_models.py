"""Pydantic models for database-backed configuration."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class AdapterConfig(BaseModel):
    """Configuration for a single adapter."""

    name: str = Field(description="Unique adapter identifier")
    enabled: bool = Field(default=True, description="Whether adapter is active")
    cadence_s: int = Field(description="Poll interval in seconds")
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
