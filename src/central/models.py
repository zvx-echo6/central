"""Data models for Central event processing."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class Geo(BaseModel):
    """Geographic context for an event."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    centroid: tuple[float, float] | None = None  # (lon, lat) GeoJSON order
    bbox: tuple[float, float, float, float] | None = None  # (minLon, minLat, maxLon, maxLat)
    regions: list[str] = []  # ["US-ID-Ada", "US-ID-Z033", ...]
    primary_region: str | None = None  # alphabetically first region, used for subject
    geometry: dict[str, Any] | None = None  # full GeoJSON geometry; preferred by the archive over bbox/centroid for the map geom column


class Event(BaseModel):
    """Canonical event representation for all adapters."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str  # unique, stable across republish
    adapter: str  # adapter identity, e.g. "nws"
    category: str  # e.g. "wx.alert.severe_thunderstorm_warning" or "fire.hotspot.viirs_snpp.high"
    time: datetime  # event-time UTC, not processing-time
    expires: datetime | None = None
    severity: int | None = None  # 0..4 or None for "Unknown"
    geo: Geo
    data: dict[str, Any]  # adapter-specific payload


