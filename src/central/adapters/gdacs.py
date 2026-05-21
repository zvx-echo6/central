"""GDACS (Global Disaster Alert and Coordination System) adapter."""

import logging
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
from central.config_models import AdapterConfig
from central.config_store import ConfigStore
from central.models import Event, Geo

logger = logging.getLogger(__name__)

GDACS_RSS_URL = "https://www.gdacs.org/xml/rss.xml"

NS = {
    "gdacs": "http://www.gdacs.org",
    "geo": "http://www.w3.org/2003/01/geo/wgs84_pos#",
    "georss": "http://www.georss.org/georss",
    "dc": "http://purl.org/dc/elements/1.1/",
}

_ALERTLEVEL_TO_SEVERITY = {"Green": 1, "Orange": 2, "Red": 3}

DEFAULT_EVENT_TYPES = ["WF", "DR", "FL", "VO", "TC"]


def severity_from_alertlevel(level: str | None) -> int:
    """Green=1, Orange=2, Red=3, default 0."""
    if not level:
        return 0
    return _ALERTLEVEL_TO_SEVERITY.get(level.strip().capitalize(), 0)


def subject_for_country(country: str | None) -> str:
    """Lowercase, hyphenate spaces. 'unknown' for missing/empty. Takes first if comma-separated."""
    if not country:
        return "unknown"
    first = country.split(",")[0].strip()
    if not first:
        return "unknown"
    return first.lower().replace(" ", "-")


def parse_rfc822_utc(raw: str | None) -> datetime | None:
    """Parse an RFC 822 datetime string to UTC datetime."""
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_gdacs_bbox(raw: str | None) -> tuple[float, float, float, float] | None:
    """Parse GDACS bbox 'lonmin lonmax latmin latmax' to Geo.bbox (minLon, minLat, maxLon, maxLat)."""
    if not raw:
        return None
    try:
        parts = [float(p) for p in raw.split()]
    except ValueError:
        return None
    if len(parts) != 4:
        return None
    lon_min, lon_max, lat_min, lat_max = parts
    return (lon_min, lat_min, lon_max, lat_max)


def init_gdacs_observed_table(db: sqlite3.Connection) -> None:
    db.execute("""
        CREATE TABLE IF NOT EXISTS gdacs_observed (
            guid TEXT PRIMARY KEY,
            country TEXT,
            eventtype TEXT,
            last_observed_at TEXT NOT NULL
        )
    """)
    db.commit()


def get_observed(db: sqlite3.Connection) -> dict[str, tuple[str | None, str | None]]:
    cur = db.execute("SELECT guid, country, eventtype FROM gdacs_observed")
    return {row[0]: (row[1], row[2]) for row in cur.fetchall()}


def mark_observed(db: sqlite3.Connection, guid: str, country: str | None, eventtype: str | None) -> None:
    db.execute(
        """
        INSERT INTO gdacs_observed (guid, country, eventtype, last_observed_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (guid) DO UPDATE SET last_observed_at = CURRENT_TIMESTAMP
        """,
        (guid, country, eventtype),
    )
    db.commit()


def mark_retired(db: sqlite3.Connection, guids: set[str]) -> None:
    for guid in guids:
        db.execute("DELETE FROM gdacs_observed WHERE guid = ?", (guid,))
    db.commit()


class GDACSSettings(BaseModel):
    """Settings schema for GDACS adapter.

    event_types is the explicit allowlist of GDACS eventtype codes to publish.
    EQ is intentionally absent from the default because USGS is the canonical
    earthquake source for Central and quakes are already published on
    central.quake.>. Operators can re-include "EQ" here if USGS is
    temporarily unavailable or if the GDACS alertlevel triage signal is
    operationally needed.

    Future-unknown eventtypes (anything GDACS may add later) are dropped by
    default — opt in by adding the code to this list.
    """

    event_types: list[str] = DEFAULT_EVENT_TYPES.copy()


class GDACSAdapter(SourceAdapter):
    """Global Disaster Alert and Coordination System adapter."""

    name = "gdacs"
    display_name = "GDACS — Global Disaster Alerts"
    description = (
        "Global Disaster Alert and Coordination System events: wildfires, drought, "
        "flood, volcano, and tropical cyclones with humanitarian-coordination triage signals."
    )
    settings_schema = GDACSSettings
    requires_api_key = None
    api_key_field = None
    wizard_order = None
    default_cadence_s = 600

    # Event lat/lon mirrored from Geo.centroid into event.data (see poll()).
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
        self.event_types: list[str] = list(
            config.settings.get("event_types", DEFAULT_EVENT_TYPES)
        )

    async def startup(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
        )
        self._db = sqlite3.connect(self._cursor_db_path)
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
        init_gdacs_observed_table(self._db)
        self._db.commit()
        logger.info("GDACS adapter started", extra={"event_types": self.event_types})

    async def shutdown(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
        if self._db:
            self._db.close()
            self._db = None
        logger.info("GDACS adapter shut down")

    async def apply_config(self, new_config: AdapterConfig) -> None:
        self.event_types = list(
            new_config.settings.get("event_types", DEFAULT_EVENT_TYPES)
        )
        logger.info("GDACS config updated", extra={"event_types": self.event_types})

    def is_published(self, event_id: str) -> bool:
        if not self._db:
            return False
        cur = self._db.execute(
            "SELECT 1 FROM published_ids WHERE adapter = ? AND event_id = ?",
            (self.name, event_id),
        )
        return cur.fetchone() is not None

    def mark_published(self, event_id: str) -> None:
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
        if not self._db:
            return 0
        cur = self._db.execute(
            "DELETE FROM published_ids WHERE adapter = ? AND last_seen < datetime('now', '-30 days')",
            (self.name,),
        )
        self._db.commit()
        count = cur.rowcount
        if count > 0:
            logger.info("GDACS swept old dedup entries", extra={"count": count})
        return count

    def subject_for(self, event: Event) -> str:
        parts = event.category.split(".")
        country_subj = subject_for_country(event.data.get("country"))
        if len(parts) >= 3 and parts[-1] == "removed":
            eventtype = parts[1]
            return f"central.disaster.{eventtype}.removed.{country_subj}"
        eventtype = (event.data.get("eventtype") or "").lower() or "unknown"
        return f"central.disaster.{eventtype}.{country_subj}"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=30),
        retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError)),
    )
    async def _fetch(self) -> str:
        if not self._session:
            raise RuntimeError("Session not initialized")
        async with self._session.get(
            GDACS_RSS_URL, headers={"User-Agent": "Central/0.4"}
        ) as resp:
            resp.raise_for_status()
            text = await resp.text()
        return text

    async def poll(self) -> AsyncIterator[Event]:
        if not self._db:
            raise RuntimeError("Database not initialized")

        try:
            content = await self._fetch()
        except Exception as e:
            logger.error("GDACS fetch failed", extra={"error": str(e)})
            raise

        try:
            root = ET.fromstring(content)
        except ET.ParseError as e:
            logger.error("GDACS RSS parse error", extra={"error": str(e)})
            raise

        channel = root.find("channel")
        if channel is None:
            logger.info("GDACS fetch completed", extra={"item_count": 0})
            return

        items = channel.findall("item")
        logger.info("GDACS fetch completed", extra={"item_count": len(items)})

        observed_before = get_observed(self._db)
        current_guids: set[str] = set()
        events_yielded = 0

        for item_elem in items:
            guid_elem = item_elem.find("guid")
            if guid_elem is None or not guid_elem.text:
                continue
            guid = guid_elem.text.strip()
            if not guid:
                continue

            eventtype_elem = item_elem.find("gdacs:eventtype", NS)
            eventtype = (
                eventtype_elem.text.strip()
                if eventtype_elem is not None and eventtype_elem.text
                else ""
            )

            if eventtype not in self.event_types:
                continue

            iscurrent_elem = item_elem.find("gdacs:iscurrent", NS)
            iscurrent = True
            if iscurrent_elem is not None and iscurrent_elem.text:
                iscurrent = iscurrent_elem.text.strip().lower() == "true"

            country_elem = item_elem.find("gdacs:country", NS)
            country = (
                country_elem.text.strip()
                if country_elem is not None and country_elem.text
                else None
            )

            iso3_elem = item_elem.find("gdacs:iso3", NS)
            iso3 = (
                iso3_elem.text.strip()
                if iso3_elem is not None and iso3_elem.text
                else None
            )

            alertlevel_elem = item_elem.find("gdacs:alertlevel", NS)
            alertlevel = (
                alertlevel_elem.text.strip()
                if alertlevel_elem is not None and alertlevel_elem.text
                else None
            )

            fromdate_elem = item_elem.find("gdacs:fromdate", NS)
            event_time = parse_rfc822_utc(
                fromdate_elem.text if fromdate_elem is not None else None
            )
            if event_time is None:
                pub_date_elem = item_elem.find("pubDate")
                event_time = (
                    parse_rfc822_utc(pub_date_elem.text if pub_date_elem is not None else None)
                    or datetime.now(timezone.utc)
                )

            bbox_elem = item_elem.find("gdacs:bbox", NS)
            bbox = parse_gdacs_bbox(
                bbox_elem.text if bbox_elem is not None else None
            )

            lat_elem = item_elem.find("geo:Point/geo:lat", NS)
            lon_elem = item_elem.find("geo:Point/geo:long", NS)
            centroid: tuple[float, float] | None = None
            if lat_elem is not None and lon_elem is not None and lat_elem.text and lon_elem.text:
                try:
                    centroid = (float(lon_elem.text), float(lat_elem.text))
                except ValueError:
                    centroid = None

            region = iso3 or country
            regions = [region] if region else []
            primary_region = region

            title_elem = item_elem.find("title")
            description_elem = item_elem.find("description")
            link_elem = item_elem.find("link")
            eventid_elem = item_elem.find("gdacs:eventid", NS)
            alertscore_elem = item_elem.find("gdacs:alertscore", NS)
            datemodified_elem = item_elem.find("gdacs:datemodified", NS)

            geo = Geo(
                centroid=centroid,
                bbox=bbox,
                regions=regions,
                primary_region=primary_region,
            )

            data: dict[str, Any] = {
                "guid": guid,
                "eventtype": eventtype,
                "eventid": eventid_elem.text.strip() if eventid_elem is not None and eventid_elem.text else None,
                "country": country,
                "iso3": iso3,
                "alertlevel": alertlevel,
                "alertscore": alertscore_elem.text.strip() if alertscore_elem is not None and alertscore_elem.text else None,
                "title": title_elem.text if title_elem is not None and title_elem.text else "",
                "description": description_elem.text if description_elem is not None and description_elem.text else "",
                "url": link_elem.text if link_elem is not None and link_elem.text else "",
                "datemodified": datemodified_elem.text if datemodified_elem is not None and datemodified_elem.text else None,
                "iscurrent": iscurrent,
            }

            # Mirror centroid (lon, lat) into top-level data keys for the flat
            # enrichment path (see enrichment_locations).
            if centroid is not None:
                data["latitude"] = centroid[1]
                data["longitude"] = centroid[0]

            if not iscurrent:
                # Explicit tombstone from upstream. Only emit if we previously observed it.
                if guid in observed_before:
                    tombstone = Event(
                        id=f"{guid}:removed",
                        adapter=self.name,
                        category=f"disaster.{eventtype.lower()}.removed",
                        time=datetime.now(timezone.utc),
                        severity=0,
                        geo=geo,
                        data={**data, "reason": "iscurrent_false"},
                    )
                    if not self.is_published(tombstone.id):
                        yield tombstone
                        self.mark_published(tombstone.id)
                        events_yielded += 1
                continue

            current_guids.add(guid)

            if self.is_published(guid):
                mark_observed(self._db, guid, country, eventtype)
                continue

            event = Event(
                id=guid,
                adapter=self.name,
                category=f"disaster.{eventtype.lower()}",
                time=event_time,
                severity=severity_from_alertlevel(alertlevel),
                geo=geo,
                data=data,
            )

            yield event
            self.mark_published(guid)
            mark_observed(self._db, guid, country, eventtype)
            events_yielded += 1

        # Fall-off: events present in observed_before but absent from this poll
        fallen_off = set(observed_before.keys()) - current_guids
        for guid in fallen_off:
            prior_country, prior_eventtype = observed_before[guid]
            if prior_eventtype and prior_eventtype not in self.event_types:
                # Was published before settings narrowed; clean up silently.
                mark_retired(self._db, {guid})
                continue
            tombstone_id = f"{guid}:removed"
            if self.is_published(tombstone_id):
                mark_retired(self._db, {guid})
                continue
            now = datetime.now(timezone.utc)
            region = prior_country
            geo = Geo(
                regions=[region] if region else [],
                primary_region=region,
            )
            tombstone = Event(
                id=tombstone_id,
                adapter=self.name,
                category=f"disaster.{(prior_eventtype or '').lower()}.removed",
                time=now,
                severity=0,
                geo=geo,
                data={
                    "guid": guid,
                    "country": prior_country,
                    "eventtype": prior_eventtype,
                    "reason": "missing_from_feed",
                },
            )
            yield tombstone
            self.mark_published(tombstone_id)
            mark_retired(self._db, {guid})
            events_yielded += 1

        self.sweep_old_ids()
        logger.info(
            "GDACS poll completed",
            extra={
                "events_yielded": events_yielded,
                "current_observed": len(current_guids),
                "fallen_off": len(fallen_off),
            },
        )
