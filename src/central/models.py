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


class Event(BaseModel):
    """Canonical event representation for all adapters."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str  # unique, stable across republish
    source: str  # adapter identity, e.g. "central/adapters/nws"
    category: str  # e.g. "wx.alert.severe_thunderstorm_warning" or "fire.hotspot.viirs_snpp.high"
    time: datetime  # event-time UTC, not processing-time
    expires: datetime | None = None
    severity: int | None = None  # 0..4 or None for "Unknown"
    geo: Geo
    data: dict[str, Any]  # adapter-specific payload


def subject_for_event(ev: Event) -> str:
    """
    Compute the NATS subject for an event based on its category.

    Dispatch by category prefix:
    - fire.*: returns central.<category> directly
    - quake.*: returns central.<category> directly
    - wx.*: uses weather alert subject logic

    Weather alert subjects:
      central.wx.alert.us.<state_lower>.county.<county_lower>
    or
      central.wx.alert.us.<state_lower>.zone.<zone_lower>
    based on whether the primary_region encodes a county or a zone.

    Fire hotspot subjects:
      central.fire.hotspot.<satellite>.<confidence>

    Quake event subjects:
      central.quake.event.<magnitude_tier>
    """
    # Fire events: subject is just central.<category>
    if ev.category.startswith("fire."):
        return f"central.{ev.category}"

    # Quake events: subject is just central.<category>
    if ev.category.startswith("quake."):
        return f"central.{ev.category}"

    # Weather events: use geo-based subject logic
    prefix = "central.wx"

    if ev.geo.primary_region is None:
        return f"{prefix}.alert.us.unknown"

    region = ev.geo.primary_region

    # Parse US-<STATE>-<CODE> format
    # County codes are like "Ada", "Canyon" (names)
    # Zone codes start with "Z" like "Z033"
    parts = region.split("-")
    if len(parts) < 3 or parts[0] != "US":
        return f"{prefix}.alert.us.unknown"

    state = parts[1].lower()
    code = "-".join(parts[2:])  # Handle multi-part names like "Payette-Washington"

    if code.startswith("Z") and len(code) >= 2 and code[1:].isdigit():
        # Zone code like Z033
        return f"{prefix}.alert.us.{state}.zone.{code.lower()}"
    else:
        # County name
        return f"{prefix}.alert.us.{state}.county.{code.lower()}"
