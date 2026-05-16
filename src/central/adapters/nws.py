"""NWS (National Weather Service) alert adapter."""

import asyncio
import logging
import re
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception_type,
)

from central import __version__
from central.adapter import SourceAdapter
from central.config_models import AdapterConfig, RegionConfig
from central.config_store import ConfigStore
from central.models import Event, Geo
from shapely.geometry import box as shapely_box, shape as shapely_shape

logger = logging.getLogger(__name__)

# FIPS state codes to postal abbreviations
FIPS_TO_STATE: dict[str, str] = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY", "60": "AS", "66": "GU", "69": "MP", "72": "PR",
    "78": "VI",
}

SEVERITY_MAP: dict[str, int | None] = {
    "Extreme": 4,
    "Severe": 3,
    "Moderate": 2,
    "Minor": 1,
    "Unknown": None,
}

NWS_API_URL = "https://api.weather.gov/alerts/active"


def _snake_case(s: str) -> str:
    """Convert a string to snake_case."""
    s = re.sub(r"[^a-zA-Z0-9\s]", "", s)
    s = re.sub(r"\s+", "_", s.strip())
    return s.lower()


def _parse_datetime(s: str | None) -> datetime | None:
    """Parse an ISO datetime string to UTC datetime."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def _compute_centroid(geometry: dict[str, Any] | None) -> tuple[float, float] | None:
    """Compute centroid from GeoJSON geometry using arithmetic mean of vertices."""
    if not geometry:
        return None

    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")

    if not coords:
        return None

    all_points: list[tuple[float, float]] = []

    if geom_type == "Point":
        return (coords[0], coords[1])
    elif geom_type == "Polygon":
        for ring in coords:
            for point in ring:
                all_points.append((point[0], point[1]))
    elif geom_type == "MultiPolygon":
        for polygon in coords:
            for ring in polygon:
                for point in ring:
                    all_points.append((point[0], point[1]))
    else:
        return None

    if not all_points:
        return None

    avg_lon = sum(p[0] for p in all_points) / len(all_points)
    avg_lat = sum(p[1] for p in all_points) / len(all_points)
    return (avg_lon, avg_lat)


def _compute_bbox(
    geometry: dict[str, Any] | None
) -> tuple[float, float, float, float] | None:
    """Compute bounding box from GeoJSON geometry."""
    if not geometry:
        return None

    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")

    if not coords:
        return None

    all_points: list[tuple[float, float]] = []

    if geom_type == "Point":
        return (coords[0], coords[1], coords[0], coords[1])
    elif geom_type == "Polygon":
        for ring in coords:
            for point in ring:
                all_points.append((point[0], point[1]))
    elif geom_type == "MultiPolygon":
        for polygon in coords:
            for ring in polygon:
                for point in ring:
                    all_points.append((point[0], point[1]))
    else:
        return None

    if not all_points:
        return None

    min_lon = min(p[0] for p in all_points)
    max_lon = max(p[0] for p in all_points)
    min_lat = min(p[1] for p in all_points)
    max_lat = max(p[1] for p in all_points)
    return (min_lon, min_lat, max_lon, max_lat)


def _extract_states_from_codes(
    same_codes: list[str], ugc_codes: list[str]
) -> set[str]:
    """Extract state abbreviations from SAME and UGC codes."""
    states: set[str] = set()

    for code in same_codes:
        if len(code) >= 2:
            fips_state = code[:2]
            if fips_state in FIPS_TO_STATE:
                states.add(FIPS_TO_STATE[fips_state])

    for code in ugc_codes:
        if len(code) >= 2 and code[:2].isalpha():
            states.add(code[:2].upper())

    return states


def _build_regions(same_codes: list[str], ugc_codes: list[str]) -> list[str]:
    """Build sorted list of region strings from geocodes."""
    regions: set[str] = set()

    for code in same_codes:
        if len(code) >= 2:
            fips_state = code[:2]
            if fips_state in FIPS_TO_STATE:
                state = FIPS_TO_STATE[fips_state]
                regions.add(f"US-{state}-FIPS{code}")

    for code in ugc_codes:
        if len(code) >= 3 and code[:2].isalpha():
            state = code[:2].upper()
            rest = code[2:]
            if rest.startswith("C"):
                regions.add(f"US-{state}-C{rest[1:]}")
            elif rest.startswith("Z"):
                regions.add(f"US-{state}-Z{rest[1:]}")
            else:
                regions.add(f"US-{state}-{rest}")

    return sorted(regions)


class NWSAdapter(SourceAdapter):
    """National Weather Service alerts adapter."""

    name = "nws"

    def __init__(
        self,
        config: AdapterConfig,
        config_store: ConfigStore,
        cursor_db_path: Path,
    ) -> None:
        self.cursor_db_path = cursor_db_path
        self._session: aiohttp.ClientSession | None = None
        self._db: sqlite3.Connection | None = None

        # Extract settings from unified config
        self.contact_email: str = config.settings.get("contact_email", "")

        # Parse region from settings
        region_dict = config.settings.get("region")
        if region_dict:
            self.region: RegionConfig | None = RegionConfig(**region_dict)
        else:
            self.region = None

    async def apply_config(self, new_config: AdapterConfig) -> None:
        """Apply new configuration from hot-reload."""
        # Update contact email
        self.contact_email = new_config.settings.get("contact_email", "")

        # Update region
        region_dict = new_config.settings.get("region")
        if region_dict:
            self.region = RegionConfig(**region_dict)
        else:
            self.region = None

        logger.info(
            "NWS config applied",
            extra={
                "region": region_dict,
                "contact_email": self.contact_email,
            },
        )

    def _geometry_intersects_region(self, geometry: dict[str, Any] | None) -> bool:
        """Check if feature geometry intersects configured region bbox.
        
        Uses Shapely for proper polygon intersection rather than centroid-only
        filtering, avoiding false negatives on large alert polygons.
        """
        if self.region is None:
            # No region configured = accept all
            return True
        if geometry is None:
            return False
        
        try:
            # Build region box (west, south, east, north)
            region_box = shapely_box(
                self.region.west,
                self.region.south,
                self.region.east,
                self.region.north,
            )
            
            # Parse GeoJSON geometry to shapely shape
            feature_shape = shapely_shape(geometry)
            
            return region_box.intersects(feature_shape)
        except Exception:
            # If geometry parsing fails, fall back to rejecting
            return False

    async def startup(self) -> None:
        """Initialize HTTP session and cursor database."""
        user_agent = f"Central/{__version__} ({self.contact_email})"
        self._session = aiohttp.ClientSession(
            headers={"User-Agent": user_agent},
            timeout=aiohttp.ClientTimeout(total=30),
        )

        self._db = sqlite3.connect(str(self.cursor_db_path))
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS adapter_cursors (
                adapter TEXT PRIMARY KEY,
                cursor_data TEXT NOT NULL,
                updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
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

        logger.info(
            "NWS adapter started",
            extra={
                "region": {
                    "north": self.region.north,
                    "south": self.region.south,
                    "east": self.region.east,
                    "west": self.region.west,
                } if self.region else None,
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
        logger.info("NWS adapter shut down")

    def _get_cursor(self) -> str | None:
        """Get the stored If-Modified-Since cursor."""
        if not self._db:
            return None
        cur = self._db.execute(
            "SELECT cursor_data FROM adapter_cursors WHERE adapter = ?",
            (self.name,)
        )
        row = cur.fetchone()
        return row[0] if row else None

    def _set_cursor(self, last_modified: str) -> None:
        """Store the Last-Modified header for next request."""
        if not self._db:
            return
        self._db.execute(
            """
            INSERT INTO adapter_cursors (adapter, cursor_data, updated)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (adapter) DO UPDATE SET
                cursor_data = excluded.cursor_data,
                updated = CURRENT_TIMESTAMP
            """,
            (self.name, last_modified)
        )
        self._db.commit()

    def is_published(self, event_id: str) -> bool:
        """Check if an event has already been published."""
        if not self._db:
            return False
        cur = self._db.execute(
            "SELECT 1 FROM published_ids WHERE adapter = ? AND event_id = ?",
            (self.name, event_id)
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
            (self.name, event_id)
        )
        self._db.commit()

    def bump_last_seen(self, event_id: str) -> None:
        """Bump the last_seen timestamp for an event."""
        if not self._db:
            return
        self._db.execute(
            "UPDATE published_ids SET last_seen = CURRENT_TIMESTAMP WHERE adapter = ? AND event_id = ?",
            (self.name, event_id)
        )
        self._db.commit()

    def sweep_old_ids(self) -> int:
        """Remove published_ids older than 8 days. Returns count deleted."""
        if not self._db:
            return 0
        cur = self._db.execute(
            "DELETE FROM published_ids WHERE last_seen < datetime('now', '-8 days')"
        )
        self._db.commit()
        return cur.rowcount

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=1, max=60),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        reraise=True,
    )
    async def _fetch_alerts(self) -> tuple[int, dict[str, Any] | None, str | None]:
        """Fetch alerts from NWS API with conditional request."""
        if not self._session:
            raise RuntimeError("Session not initialized")

        headers: dict[str, str] = {}
        cursor = self._get_cursor()
        if cursor:
            headers["If-Modified-Since"] = cursor

        async with self._session.get(NWS_API_URL, headers=headers) as resp:
            if resp.status in (429, 403):
                retry_after = resp.headers.get("Retry-After", "60")
                try:
                    wait_time = int(retry_after)
                except ValueError:
                    wait_time = 60
                logger.warning(
                    "Rate limited by NWS",
                    extra={"status": resp.status, "retry_after": wait_time}
                )
                await asyncio.sleep(wait_time)
                raise aiohttp.ClientError(f"Rate limited: {resp.status}")

            if resp.status == 304:
                return (304, None, None)

            resp.raise_for_status()

            data = await resp.json()
            last_modified = resp.headers.get("Last-Modified")

            return (resp.status, data, last_modified)

    def _normalize_feature(self, feature: dict[str, Any]) -> Event | None:
        """Normalize a GeoJSON feature to an Event."""
        props = feature.get("properties", {})
        geocode = props.get("geocode", {})

        same_codes = geocode.get("SAME", [])
        ugc_codes = geocode.get("UGC", [])

        # Compute geometry data first
        geometry = feature.get("geometry")
        centroid = _compute_centroid(geometry)
        bbox = _compute_bbox(geometry)

        # Filter by region bbox (client-side filtering)
        if not self._geometry_intersects_region(geometry):
            return None

        event_id = feature.get("id")
        if not event_id:
            logger.warning("Feature missing id", extra={"properties": props})
            return None

        event_type = props.get("event", "Unknown")
        category = f"wx.alert.{_snake_case(event_type)}"

        time = _parse_datetime(props.get("sent"))
        if not time:
            logger.warning("Feature missing sent time", extra={"id": event_id})
            return None

        expires = _parse_datetime(props.get("expires"))

        severity_str = props.get("severity", "Unknown")
        severity = SEVERITY_MAP.get(severity_str)

        regions = _build_regions(same_codes, ugc_codes)
        primary_region = regions[0] if regions else None

        geo = Geo(
            centroid=centroid,
            bbox=bbox,
            regions=regions,
            primary_region=primary_region,
        )

        return Event(
            id=event_id,
            source="central/adapters/nws",
            category=category,
            time=time,
            expires=expires,
            severity=severity,
            geo=geo,
            data=props,
        )

    async def poll(self) -> AsyncIterator[Event]:
        """Poll NWS API for active alerts."""
        try:
            status, data, last_modified = await self._fetch_alerts()
        except Exception as e:
            logger.error("Failed to fetch NWS alerts", extra={"error": str(e)})
            raise

        if status == 304:
            logger.info("NWS returned 304 Not Modified")
            return

        if last_modified:
            self._set_cursor(last_modified)

        features = data.get("features", []) if data else []
        logger.info(
            "NWS poll completed",
            extra={"status": status, "feature_count": len(features)}
        )

        yielded = 0
        for feature in features:
            try:
                event = self._normalize_feature(feature)
                if event:
                    yield event
                    yielded += 1
            except Exception as e:
                logger.warning(
                    "Failed to normalize feature",
                    extra={"error": str(e), "feature_id": feature.get("id")}
                )

        logger.info("NWS yielded events", extra={"count": yielded})
