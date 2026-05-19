"""FIRMS (Fire Information for Resource Management System) adapter."""

import csv
import logging
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Literal

import aiohttp
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception_type,
)

from central.adapter import SourceAdapter
from pydantic import BaseModel

from central.config_models import AdapterConfig, RegionConfig
from central.config_store import ConfigStore
from central.models import Event, Geo

logger = logging.getLogger(__name__)

# FIRMS API base URL
FIRMS_API_BASE = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"

# Satellite name mapping
SATELLITE_SHORT = {
    "VIIRS_SNPP_NRT": "viirs_snpp",
    "VIIRS_NOAA20_NRT": "viirs_noaa20",
    "VIIRS_NOAA21_NRT": "viirs_noaa21",
}

# Confidence mapping
CONFIDENCE_MAP = {
    "l": "low",
    "n": "nominal",
    "h": "high",
}

# Severity mapping (confidence -> severity level)
SEVERITY_MAP = {
    "high": 3,
    "nominal": 2,
    "low": 1,
}


class FIRMSSettings(BaseModel):
    """Settings schema for FIRMS adapter."""
    api_key_alias: str = "firms"
    satellites: list[Literal["VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT", "VIIRS_NOAA21_NRT"]] = ["VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT"]
    region: RegionConfig | None = None


class FIRMSAdapter(SourceAdapter):
    """NASA FIRMS fire hotspot adapter."""

    name = "firms"
    display_name = "NASA FIRMS Fire Hotspots"
    description = "Near-real-time satellite-detected fire hotspots from NASA FIRMS."
    settings_schema = FIRMSSettings
    requires_api_key = "firms"
    api_key_field = "api_key_alias"
    wizard_order = 2
    default_cadence_s = 300

    def __init__(
        self,
        config: AdapterConfig,
        config_store: ConfigStore,
        cursor_db_path: Path,
    ) -> None:
        self._config_store = config_store
        self._cursor_db_path = cursor_db_path
        self._session: aiohttp.ClientSession | None = None
        self._db: sqlite3.Connection | None = None
        self._api_key: str | None = None

        # Extract settings from config
        self._api_key_alias: str = config.settings.get("api_key_alias", "firms")
        self._satellites: list[str] = config.settings.get(
            "satellites", ["VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT"]
        )

        # Parse region from settings
        region_dict = config.settings.get("region")
        if region_dict:
            self.region: RegionConfig | None = RegionConfig(**region_dict)
        else:
            self.region = None

    async def apply_config(self, new_config: AdapterConfig) -> None:
        """Apply new configuration from hot-reload."""
        old_alias = self._api_key_alias

        # Update settings
        self._api_key_alias = new_config.settings.get("api_key_alias", "firms")
        self._satellites = new_config.settings.get(
            "satellites", ["VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT"]
        )

        # Update region
        region_dict = new_config.settings.get("region")
        if region_dict:
            self.region = RegionConfig(**region_dict)
        else:
            self.region = None

        # If API key alias changed, re-fetch the key
        if self._api_key_alias != old_alias:
            self._api_key = await self._config_store.get_api_key(self._api_key_alias)
            if self._api_key:
                logger.info("FIRMS API key reloaded", extra={"alias": self._api_key_alias})
            else:
                logger.warning(
                    "FIRMS API key not found after alias change",
                    extra={"alias": self._api_key_alias},
                )

        logger.info(
            "FIRMS config applied",
            extra={
                "region": region_dict,
                "satellites": self._satellites,
                "api_key_alias": self._api_key_alias,
            },
        )

    def subject_for(self, event: Event) -> str:
        """Compute NATS subject for a fire hotspot event.

        Subject format: central.fire.hotspot.<satellite>.<confidence>
        The category already contains this structure.
        """
        return f"central.{event.category}"


    async def startup(self) -> None:
        """Initialize HTTP session, dedup tracker, and fetch API key."""
        # Fetch API key
        self._api_key = await self._config_store.get_api_key(self._api_key_alias)
        if not self._api_key:
            logger.error(
                "FIRMS API key not found - polling will be skipped until key is set",
                extra={"alias": self._api_key_alias},
            )

        # Initialize HTTP session
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
        )

        # Initialize dedup tracker (shared sqlite DB with NWS)
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

        # Sweep old entries on startup (48h for FIRMS)
        self.sweep_old_ids()

        logger.info(
            "FIRMS adapter started",
            extra={
                "region": {
                    "north": self.region.north,
                    "south": self.region.south,
                    "east": self.region.east,
                    "west": self.region.west,
                } if self.region else None,
                "satellites": self._satellites,
                "api_key_present": self._api_key is not None,
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
        logger.info("FIRMS adapter shut down")

    def is_published(self, stable_id: str) -> bool:
        """Check if an event has already been published."""
        if not self._db:
            return False
        cur = self._db.execute(
            "SELECT 1 FROM published_ids WHERE adapter = ? AND event_id = ?",
            (self.name, stable_id),
        )
        return cur.fetchone() is not None

    def mark_published(self, stable_id: str) -> None:
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
            (self.name, stable_id),
        )
        self._db.commit()

    def sweep_old_ids(self) -> int:
        """Remove published_ids older than 48 hours. Returns count deleted."""
        if not self._db:
            return 0
        cur = self._db.execute(
            "DELETE FROM published_ids WHERE adapter = ? AND last_seen < datetime('now', '-48 hours')",
            (self.name,),
        )
        self._db.commit()
        count = cur.rowcount
        if count > 0:
            logger.info("FIRMS swept old dedup entries", extra={"count": count})
        return count

    def _build_stable_id(
        self, satellite: str, acq_date: str, acq_time: str, lat: float, lon: float
    ) -> str:
        """Build stable ID for deduplication."""
        # Round lat/lon to 0.001 degrees to handle floating-point comparison
        lat_rounded = round(lat, 3)
        lon_rounded = round(lon, 3)
        return f"{satellite}:{acq_date}:{acq_time}:{lat_rounded}:{lon_rounded}"

    def _build_url(self, satellite: str) -> str | None:
        """Build FIRMS API URL for a satellite."""
        if not self._api_key or not self.region:
            return None

        # Area format: west,south,east,north
        area = f"{self.region.west},{self.region.south},{self.region.east},{self.region.north}"
        return f"{FIRMS_API_BASE}/{self._api_key}/{satellite}/{area}/1"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=2, max=30),
        retry=retry_if_exception_type((aiohttp.ClientError,)),
        reraise=True,
    )
    async def _fetch_csv(self, url: str) -> str:
        """Fetch CSV data from FIRMS API."""
        if not self._session:
            raise RuntimeError("Session not initialized")

        async with self._session.get(url) as resp:
            # Check for error responses
            content_type = resp.headers.get("Content-Type", "")
            if "text/html" in content_type:
                text = await resp.text()
                logger.error(
                    "FIRMS returned HTML (likely auth error)",
                    extra={"status": resp.status, "preview": text[:200]},
                )
                raise ValueError("FIRMS returned HTML instead of CSV")

            resp.raise_for_status()
            return await resp.text()

    def _parse_csv(self, csv_text: str, satellite: str) -> list[dict[str, Any]]:
        """Parse FIRMS CSV response into list of dicts."""
        rows = []
        reader = csv.DictReader(StringIO(csv_text))

        for row in reader:
            try:
                # Parse required fields
                lat = float(row["latitude"])
                lon = float(row["longitude"])
                acq_date = row["acq_date"]
                acq_time = row["acq_time"]
                confidence_raw = row.get("confidence", "n").lower()
                confidence = CONFIDENCE_MAP.get(confidence_raw, "nominal")

                rows.append({
                    "latitude": lat,
                    "longitude": lon,
                    "bright_ti4": float(row.get("bright_ti4", 0)) if row.get("bright_ti4") else None,
                    "bright_ti5": float(row.get("bright_ti5", 0)) if row.get("bright_ti5") else None,
                    "scan": float(row.get("scan", 0)) if row.get("scan") else None,
                    "track": float(row.get("track", 0)) if row.get("track") else None,
                    "acq_date": acq_date,
                    "acq_time": acq_time,
                    "satellite": row.get("satellite", satellite),
                    "instrument": row.get("instrument", "VIIRS"),
                    "confidence": confidence,
                    "confidence_raw": confidence_raw,
                    "version": row.get("version", ""),
                    "frp": float(row.get("frp", 0)) if row.get("frp") else None,
                    "daynight": row.get("daynight", ""),
                })
            except (KeyError, ValueError) as e:
                logger.warning(
                    "Failed to parse FIRMS row",
                    extra={"error": str(e), "row": dict(row)},
                )
                continue

        return rows

    def _row_to_event(self, row: dict[str, Any], satellite: str) -> Event:
        """Convert a parsed CSV row to an Event."""
        satellite_short = SATELLITE_SHORT.get(satellite, satellite.lower().replace("_nrt", ""))
        confidence = row["confidence"]
        severity = SEVERITY_MAP.get(confidence, 1)

        # Parse acquisition time
        acq_date = row["acq_date"]
        acq_time = row["acq_time"]
        # acq_time is HHMM format
        try:
            time = datetime.strptime(
                f"{acq_date} {acq_time}", "%Y-%m-%d %H%M"
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            time = datetime.now(timezone.utc)

        lat = row["latitude"]
        lon = row["longitude"]

        # Build stable ID
        stable_id = self._build_stable_id(satellite, acq_date, acq_time, lat, lon)

        geo = Geo(
            centroid=(lon, lat),  # GeoJSON order: lon, lat
            bbox=(lon, lat, lon, lat),  # Point bbox
            regions=[],
            primary_region=None,
        )

        return Event(
            id=stable_id,
            adapter="firms",
            category=f"fire.hotspot.{satellite_short}.{confidence}",
            time=time,
            expires=None,
            severity=severity,
            geo=geo,
            data=row,
        )

    async def poll(self) -> AsyncIterator[Event]:
        """Poll FIRMS API for fire hotspots."""
        # Check API key
        if not self._api_key:
            # Try to fetch again in case it was added
            self._api_key = await self._config_store.get_api_key(self._api_key_alias)
            if not self._api_key:
                logger.warning(
                    "FIRMS API key still not available, skipping poll",
                    extra={"alias": self._api_key_alias},
                )
                return

        if not self.region:
            logger.warning("FIRMS region not configured, skipping poll")
            return

        # Sweep old dedup entries periodically
        self.sweep_old_ids()

        total_features = 0
        total_new = 0

        for satellite in self._satellites:
            url = self._build_url(satellite)
            if not url:
                continue

            try:
                csv_text = await self._fetch_csv(url)
                rows = self._parse_csv(csv_text, satellite)
                feature_count = len(rows)
                total_features += feature_count

                new_count = 0
                for row in rows:
                    stable_id = self._build_stable_id(
                        satellite,
                        row["acq_date"],
                        row["acq_time"],
                        row["latitude"],
                        row["longitude"],
                    )

                    if self.is_published(stable_id):
                        continue

                    event = self._row_to_event(row, satellite)
                    yield event
                    self.mark_published(stable_id)
                    new_count += 1

                total_new += new_count
                logger.info(
                    "FIRMS satellite poll completed",
                    extra={
                        "satellite": satellite,
                        "feature_count": feature_count,
                        "new_count": new_count,
                    },
                )

            except Exception as e:
                logger.error(
                    "FIRMS poll failed for satellite",
                    extra={"satellite": satellite, "error": str(e)},
                )
                continue

        logger.info(
            "FIRMS poll completed",
            extra={
                "total_features": total_features,
                "total_new": total_new,
                "satellites": self._satellites,
            },
        )

