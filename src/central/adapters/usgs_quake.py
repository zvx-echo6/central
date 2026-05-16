"""USGS Earthquake Hazards Program adapter."""

import logging
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
from shapely.geometry import Point, box as shapely_box
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception_type,
)

from central.adapter import SourceAdapter
from central.config_models import AdapterConfig, RegionConfig
from central.config_store import ConfigStore
from central.models import Event, Geo

logger = logging.getLogger(__name__)

# USGS GeoJSON feed base URL
USGS_FEED_BASE = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary"

# Valid feed options
VALID_FEEDS = {"all_hour", "all_day", "all_week", "all_month"}


def magnitude_tier(mag: float) -> str:
    """Classify magnitude into USGS-style tier."""
    if mag < 3.0:
        return "minor"
    if mag < 4.0:
        return "light"
    if mag < 5.0:
        return "moderate"
    if mag < 6.0:
        return "strong"
    if mag < 7.0:
        return "major"
    return "great"


def magnitude_to_severity(mag: float) -> int:
    """Map magnitude to severity level (0-5)."""
    if mag < 3.0:
        return 0
    if mag < 4.0:
        return 1
    if mag < 5.0:
        return 2
    if mag < 6.0:
        return 3
    if mag < 7.0:
        return 4
    return 5


class USGSQuakeAdapter(SourceAdapter):
    """USGS Earthquake Hazards Program adapter."""

    name = "usgs_quake"

    def __init__(
        self,
        config: AdapterConfig,
        config_store: ConfigStore,  # Unused, accepted for signature uniformity
        cursor_db_path: Path,
    ) -> None:
        self._cursor_db_path = cursor_db_path
        self._session: aiohttp.ClientSession | None = None
        self._db: sqlite3.Connection | None = None

        # Extract settings from config
        self._feed: str = config.settings.get("feed", "all_hour")
        if self._feed not in VALID_FEEDS:
            logger.warning(
                "Invalid feed setting, using all_hour",
                extra={"feed": self._feed, "valid": list(VALID_FEEDS)},
            )
            self._feed = "all_hour"

        # Parse region from settings
        region_dict = config.settings.get("region")
        if region_dict:
            self.region: RegionConfig | None = RegionConfig(**region_dict)
            self._region_box = shapely_box(
                self.region.west,
                self.region.south,
                self.region.east,
                self.region.north,
            )
        else:
            self.region = None
            self._region_box = None

    async def apply_config(self, new_config: AdapterConfig) -> None:
        """Apply new configuration from hot-reload."""
        # Update feed
        new_feed = new_config.settings.get("feed", "all_hour")
        if new_feed in VALID_FEEDS:
            self._feed = new_feed
        else:
            logger.warning(
                "Invalid feed in new config, keeping current",
                extra={"new_feed": new_feed, "current": self._feed},
            )

        # Update region
        region_dict = new_config.settings.get("region")
        if region_dict:
            self.region = RegionConfig(**region_dict)
            self._region_box = shapely_box(
                self.region.west,
                self.region.south,
                self.region.east,
                self.region.north,
            )
        else:
            self.region = None
            self._region_box = None

        logger.info(
            "USGS quake config applied",
            extra={
                "region": region_dict,
                "feed": self._feed,
            },
        )

    async def startup(self) -> None:
        """Initialize HTTP session and dedup tracker."""
        # Initialize HTTP session
        self._session = aiohttp.ClientSession(
            headers={"User-Agent": "Central/1.0 (earthquake monitoring)"},
            timeout=aiohttp.ClientTimeout(total=30),
        )

        # Initialize dedup tracker (shared sqlite DB)
        self._db = sqlite3.connect(str(self._cursor_db_path))
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS published_ids (
                adapter TEXT NOT NULL,
                event_id TEXT NOT NULL,
                first_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (adapter, event_id)
            )
        """)
        self._db.execute("""
            CREATE INDEX IF NOT EXISTS published_ids_last_seen
            ON published_ids (last_seen)
        """)
        self._db.commit()

        # Sweep old entries on startup (7 days for quakes)
        self.sweep_old_ids()

        logger.info(
            "USGS quake adapter started",
            extra={
                "region": {
                    "north": self.region.north,
                    "south": self.region.south,
                    "east": self.region.east,
                    "west": self.region.west,
                } if self.region else None,
                "feed": self._feed,
            },
        )

    async def shutdown(self) -> None:
        """Close HTTP session and database."""
        if self._session:
            await self._session.close()
            self._session = None
        if self._db:
            self._db.close()
            self._db = None
        logger.info("USGS quake adapter shut down")

    def is_published(self, event_id: str) -> bool:
        """Check if an event has already been published."""
        if not self._db:
            return False
        cur = self._db.execute(
            "SELECT 1 FROM published_ids WHERE adapter = ? AND event_id = ?",
            (self.name, event_id),
        )
        return cur.fetchone() is not None

    def mark_published(self, event_id: str) -> None:
        """Mark an event as published."""
        if not self._db:
            return
        self._db.execute(
            """
            INSERT INTO published_ids (adapter, event_id, first_seen, last_seen)
            VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (adapter, event_id) DO UPDATE SET
                last_seen = CURRENT_TIMESTAMP
            """,
            (self.name, event_id),
        )
        self._db.commit()

    def sweep_old_ids(self) -> int:
        """Remove published_ids older than 7 days. Returns count deleted."""
        if not self._db:
            return 0
        cur = self._db.execute(
            "DELETE FROM published_ids WHERE adapter = ? AND last_seen < datetime('now', '-7 days')",
            (self.name,),
        )
        self._db.commit()
        count = cur.rowcount
        if count > 0:
            logger.info("USGS quake swept old dedup entries", extra={"count": count})
        return count

    def _build_url(self) -> str:
        """Build USGS GeoJSON feed URL."""
        return f"{USGS_FEED_BASE}/{self._feed}.geojson"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=15),
        retry=retry_if_exception_type((aiohttp.ClientError,)),
        reraise=True,
    )
    async def _fetch_geojson(self) -> dict[str, Any]:
        """Fetch GeoJSON data from USGS."""
        if not self._session:
            raise RuntimeError("Session not initialized")

        url = self._build_url()
        async with self._session.get(url) as resp:
            resp.raise_for_status()
            return await resp.json()

    def _point_in_region(self, lon: float, lat: float) -> bool:
        """Check if point intersects region bbox using shapely."""
        if self._region_box is None:
            return True
        point = Point(lon, lat)
        return self._region_box.intersects(point)

    def _feature_to_event(self, feature: dict[str, Any]) -> Event | None:
        """Convert a GeoJSON feature to an Event."""
        props = feature.get("properties", {})
        geometry = feature.get("geometry", {})
        coords = geometry.get("coordinates", [])

        # Validate required fields
        event_id = feature.get("id")
        if not event_id:
            logger.warning("Feature missing id", extra={"properties": props})
            return None

        # Get magnitude - skip if null/missing (PM decision)
        mag = props.get("mag")
        if mag is None:
            logger.debug(
                "Skipping event with null magnitude",
                extra={"id": event_id, "place": props.get("place")},
            )
            return None

        try:
            mag = float(mag)
        except (TypeError, ValueError):
            logger.warning(
                "Invalid magnitude value",
                extra={"id": event_id, "mag": mag},
            )
            return None

        # Get coordinates [lon, lat, depth]
        if len(coords) < 2:
            logger.warning("Feature missing coordinates", extra={"id": event_id})
            return None

        lon, lat = coords[0], coords[1]
        depth = coords[2] if len(coords) > 2 else None

        # Region filter
        if not self._point_in_region(lon, lat):
            return None

        # Parse event time (milliseconds since epoch)
        time_ms = props.get("time")
        if time_ms is not None:
            try:
                event_time = datetime.fromtimestamp(time_ms / 1000, tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                event_time = datetime.now(timezone.utc)
        else:
            event_time = datetime.now(timezone.utc)

        # Build tier and severity
        tier = magnitude_tier(mag)
        severity = magnitude_to_severity(mag)

        # Build geo
        geo = Geo(
            centroid=(lon, lat),
            bbox=(lon, lat, lon, lat),
            regions=[],
            primary_region=None,
        )

        # Build data payload
        data = {
            "magnitude": mag,
            "place": props.get("place"),
            "time_ms": time_ms,
            "updated_ms": props.get("updated"),
            "tz": props.get("tz"),
            "url": props.get("url"),
            "detail": props.get("detail"),
            "felt": props.get("felt"),
            "cdi": props.get("cdi"),
            "mmi": props.get("mmi"),
            "alert": props.get("alert"),
            "status": props.get("status"),
            "tsunami": props.get("tsunami"),
            "sig": props.get("sig"),
            "net": props.get("net"),
            "code": props.get("code"),
            "ids": props.get("ids"),
            "sources": props.get("sources"),
            "types": props.get("types"),
            "nst": props.get("nst"),
            "dmin": props.get("dmin"),
            "rms": props.get("rms"),
            "gap": props.get("gap"),
            "magType": props.get("magType"),
            "type": props.get("type"),
            "title": props.get("title"),
            "longitude": lon,
            "latitude": lat,
            "depth": depth,
        }

        return Event(
            id=event_id,
            source="central/adapters/usgs_quake",
            category=f"quake.event.{tier}",
            time=event_time,
            expires=None,
            severity=severity,
            geo=geo,
            data=data,
        )

    async def poll(self) -> AsyncIterator[Event]:
        """Poll USGS for earthquake data."""
        if not self.region:
            logger.warning("USGS quake region not configured, skipping poll")
            return

        # Sweep old dedup entries periodically
        self.sweep_old_ids()

        try:
            data = await self._fetch_geojson()
        except Exception as e:
            logger.error("Failed to fetch USGS data", extra={"error": str(e)})
            raise

        features = data.get("features", [])
        metadata = data.get("metadata", {})

        logger.info(
            "USGS quake poll completed",
            extra={
                "feature_count": len(features),
                "title": metadata.get("title"),
                "generated": metadata.get("generated"),
            },
        )

        new_count = 0
        for feature in features:
            event = self._feature_to_event(feature)
            if event is None:
                continue

            if self.is_published(event.id):
                continue

            yield event
            self.mark_published(event.id)
            new_count += 1

        logger.info("USGS quake yielded events", extra={"count": new_count})
