"""InciWeb adapter for wildfire narrative updates."""

import html
import logging
import re
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import aiohttp
from pydantic import BaseModel
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from central.adapter import SourceAdapter
from central.config_models import AdapterConfig, RegionConfig
from central.config_store import ConfigStore
from central.models import Event, Geo

logger = logging.getLogger(__name__)

# InciWeb RSS feed URL
INCIWEB_RSS_URL = "https://inciweb.wildfire.gov/incidents/rss.xml"

# State name to 2-letter code mapping
STATE_NAME_TO_CODE = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
    "puerto rico": "PR", "guam": "GU", "virgin islands": "VI",
    "american samoa": "AS", "northern mariana islands": "MP",
}


def parse_coordinates_from_description(description: str) -> tuple[float, float] | None:
    """
    Parse latitude/longitude from InciWeb description text.

    Format: "Latitude: 47° 3 17  Longitude: 91° 38 6"
    InciWeb uses unsigned values for US coordinates (west longitude implied).
    Returns (lon, lat) tuple or None if not found.
    """
    # Pattern for degree/minute/second format
    lat_pattern = r"Latitude:\s*(-?\d+)°\s*(\d+)\s*(\d+(?:\.\d+)?)"
    lon_pattern = r"Longitude:\s*(-?\d+)°\s*(\d+)\s*(\d+(?:\.\d+)?)"

    lat_match = re.search(lat_pattern, description)
    lon_match = re.search(lon_pattern, description)

    if not lat_match or not lon_match:
        return None

    try:
        lat_deg = int(lat_match.group(1))
        lat_min = int(lat_match.group(2))
        lat_sec = float(lat_match.group(3))

        lon_deg = int(lon_match.group(1))
        lon_min = int(lon_match.group(2))
        lon_sec = float(lon_match.group(3))

        # Convert to decimal degrees
        # Latitude: positive in northern hemisphere
        if lat_deg >= 0:
            lat = lat_deg + lat_min / 60 + lat_sec / 3600
        else:
            lat = lat_deg - lat_min / 60 - lat_sec / 3600

        # Longitude: InciWeb gives unsigned values for US west longitudes
        # Make negative for western hemisphere (US coordinates)
        lon = lon_deg + lon_min / 60 + lon_sec / 3600
        if lon > 0:
            lon = -lon  # US longitudes are west (negative)

        return (lon, lat)
    except (ValueError, TypeError):
        return None


def parse_state_from_description(description: str) -> str | None:
    """
    Parse state name from InciWeb description text.

    Format: "State: Minnesota" or "State: New Mexico"
    Returns 2-letter state code or None if not found.

    Design note: State is parsed from the description rather than the title
    because InciWeb titles use unit code prefixes (e.g., "MNMNS Stewart Trail",
    "CACNP Santa Rosa Island Fire") which are not reliable state indicators.
    The description has a structured "State: <name>" field that reliably
    identifies the state for all incidents.
    """
    pattern = r"State:\s*([A-Za-z\s]+?)(?:\n|---|$)"
    match = re.search(pattern, description)

    if not match:
        return None

    state_name = match.group(1).strip().lower()
    return STATE_NAME_TO_CODE.get(state_name)


def strip_html(html_text: str) -> str:
    """
    Strip HTML tags and decode entities to plain text.
    """
    # Decode HTML entities (handles &amp; &lt; &gt; etc.)
    text = html.unescape(html_text)

    # Handle &nbsp; specifically (not a standard Python html entity)
    text = text.replace("&nbsp;", " ")
    text = text.replace("\xa0", " ")  # Non-breaking space character

    # Remove HTML tags
    text = re.sub(r"<[^>]+>", "", text)

    # Normalize whitespace
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def point_in_bbox(
    lon: float,
    lat: float,
    west: float,
    south: float,
    east: float,
    north: float,
) -> bool:
    """Check if a point is within a bounding box."""
    return west <= lon <= east and south <= lat <= north


class InciWebSettings(BaseModel):
    """Settings schema for InciWeb adapter."""

    region: RegionConfig | None = None


class InciWebAdapter(SourceAdapter):
    """NIFC InciWeb wildfire narrative adapter."""

    name = "inciweb"
    display_name = "NIFC InciWeb — Wildfire Narrative"
    description = (
        "Narrative wildfire updates from InciWeb. Editorial; lower precision "
        "than WFIGS. Use as supplementary context."
    )
    settings_schema = InciWebSettings
    requires_api_key = None
    api_key_field = None
    wizard_order = None  # Ships disabled
    default_cadence_s = 600

    # Coords parsed from the narrative, mirrored from Geo.centroid into event.data.
    enrichment_locations = [("latitude", "longitude")]

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

        # Conditional fetch state
        self._last_modified: str | None = None
        self._etag: str | None = None

        # Parse region from settings
        region_dict = config.settings.get("region")
        if region_dict:
            self.region: RegionConfig | None = RegionConfig(**region_dict)
        else:
            self.region = None

    async def startup(self) -> None:
        """Initialize HTTP session and SQLite connection."""
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
        )
        self._db = sqlite3.connect(self._cursor_db_path)

        # Create table for dedup tracking
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
            "InciWeb adapter started",
            extra={"region": self.region.model_dump() if self.region else None},
        )

    async def shutdown(self) -> None:
        """Close HTTP session and SQLite connection."""
        if self._session:
            await self._session.close()
            self._session = None
        if self._db:
            self._db.close()
            self._db = None
        logger.info("InciWeb adapter shut down")

    async def apply_config(self, new_config: AdapterConfig) -> None:
        """Apply new configuration from hot-reload."""
        region_dict = new_config.settings.get("region")
        if region_dict:
            self.region = RegionConfig(**region_dict)
        else:
            self.region = None
        logger.info(
            "InciWeb config updated",
            extra={"region": self.region.model_dump() if self.region else None},
        )

    def subject_for(self, event: Event) -> str:
        """Compute NATS subject for an event."""
        state = event.geo.primary_region
        if state and state.startswith("US-") and len(state) == 5:
            state_code = state[3:].lower()
            return f"central.fire.narrative.inciweb.{state_code}"
        return "central.fire.narrative.inciweb.unknown"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=30),
        retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError)),
    )
    async def _fetch_rss(self) -> list[dict[str, Any]]:
        """Fetch and parse RSS feed from InciWeb."""
        if not self._session:
            raise RuntimeError("Session not initialized")

        # Build request headers with conditional fetch support
        headers = {"User-Agent": "Central/0.4"}
        if self._last_modified:
            headers["If-Modified-Since"] = self._last_modified
        if self._etag:
            headers["If-None-Match"] = self._etag

        async with self._session.get(INCIWEB_RSS_URL, headers=headers) as resp:
            # Handle 304 Not Modified
            if resp.status == 304:
                logger.info("InciWeb not modified")
                return []

            resp.raise_for_status()

            # Capture conditional fetch headers for next request
            self._last_modified = resp.headers.get("Last-Modified")
            self._etag = resp.headers.get("ETag")

            content = await resp.text()

        # Parse RSS XML
        items = []
        try:
            root = ET.fromstring(content)
            channel = root.find("channel")
            if channel is None:
                return []

            for item_elem in channel.findall("item"):
                item: dict[str, Any] = {}

                title = item_elem.find("title")
                item["title"] = title.text if title is not None and title.text else ""

                link = item_elem.find("link")
                item["link"] = link.text if link is not None and link.text else ""

                description = item_elem.find("description")
                item["description"] = description.text if description is not None and description.text else ""

                pub_date = item_elem.find("pubDate")
                item["pubDate"] = pub_date.text if pub_date is not None and pub_date.text else ""

                guid = item_elem.find("guid")
                item["guid"] = guid.text if guid is not None and guid.text else ""

                # Check for dc:creator
                creator = item_elem.find("{http://purl.org/dc/elements/1.1/}creator")
                item["creator"] = creator.text if creator is not None and creator.text else ""

                items.append(item)

        except ET.ParseError as e:
            logger.error("InciWeb RSS parse error", extra={"error": str(e)})
            raise

        logger.info(
            "InciWeb fetch completed",
            extra={"item_count": len(items)},
        )
        return items

    async def poll(self) -> AsyncIterator[Event]:
        """Poll InciWeb for narrative updates."""
        if not self._db:
            raise RuntimeError("Database not initialized")

        # Fetch RSS feed
        try:
            items = await self._fetch_rss()
        except Exception as e:
            logger.error("InciWeb fetch failed", extra={"error": str(e)})
            raise

        events_yielded = 0

        for item in items:
            guid = item.get("guid", "")
            if not guid:
                continue

            # Dedup: skip if already published
            if self.is_published(guid):
                self.bump_last_seen(guid)
                continue

            description_html = item.get("description", "")

            # Parse coordinates from description
            centroid = parse_coordinates_from_description(description_html)

            # Post-filter: skip if point outside region bbox
            if self.region and centroid:
                lon, lat = centroid
                if not point_in_bbox(
                    lon, lat,
                    self.region.west, self.region.south,
                    self.region.east, self.region.north,
                ):
                    continue

            # Parse state from description
            state_code = parse_state_from_description(description_html)

            # Build regions
            if state_code:
                regions = [f"US-{state_code}"]
                primary_region = f"US-{state_code}"
            else:
                regions = []
                primary_region = None

            # Parse pubDate (RFC 822 format)
            pub_date_str = item.get("pubDate", "")
            try:
                event_time = parsedate_to_datetime(pub_date_str)
                # Ensure UTC
                if event_time.tzinfo is None:
                    event_time = event_time.replace(tzinfo=timezone.utc)
                else:
                    event_time = event_time.astimezone(timezone.utc)
            except (ValueError, TypeError):
                event_time = datetime.now(timezone.utc)

            # Build geo
            geo = Geo(
                centroid=centroid,
                bbox=(centroid[0], centroid[1], centroid[0], centroid[1]) if centroid else None,
                regions=regions,
                primary_region=primary_region,
            )

            # Strip HTML from description
            description_plain = strip_html(description_html)

            # Build event
            event = Event(
                id=guid,
                adapter=self.name,
                category="fire.narrative.inciweb",
                time=event_time,
                severity=0,  # Narrative; not authoritative
                geo=geo,
                data={
                    "title": item.get("title", ""),
                    "description": description_plain,
                    "description_html": description_html,
                    "url": item.get("link", ""),
                    "guid": guid,
                    "raw": item,
                    # Mirror centroid (lon, lat) for the flat enrichment path.
                    "latitude": centroid[1] if centroid else None,
                    "longitude": centroid[0] if centroid else None,
                },
            )

            yield event
            self.mark_published(guid)
            events_yielded += 1

        # Periodic cleanup of old entries
        self.sweep_old_ids()

        logger.info(
            "InciWeb poll completed",
            extra={"events_yielded": events_yielded},
        )
