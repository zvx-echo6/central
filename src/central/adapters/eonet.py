"""NASA EONET (Earth Observatory Natural Event Tracker) adapter."""

import json
import logging
import re
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
from pydantic import BaseModel, Field
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

EONET_EVENTS_URL = "https://eonet.gsfc.nasa.gov/api/v3/events"

# Single source of truth for default category list. Mirrors the upstream
# /api/v3/categories registry at the time of integration. Do NOT duplicate
# this list in tests, fixtures, or migrations — derive from EONETSettings'
# default instead. Refresh by curling /api/v3/categories if upstream adds
# new IDs.
_DEFAULT_CATEGORIES: list[str] = [
    "drought",
    "dustHaze",
    "earthquakes",
    "floods",
    "landslides",
    "manmade",
    "seaLakeIce",
    "severeStorms",
    "snow",
    "tempExtremes",
    "volcanoes",
    "waterColor",
    "wildfires",
]


def _subject_category(category_id: str | None) -> str:
    """Convert upstream EONET camelCase category id to lower_snake_case subject component.

    Examples: seaLakeIce -> sea_lake_ice, dustHaze -> dust_haze,
    severeStorms -> severe_storms. Single-word ids pass through lowercased.
    Empty/None -> 'unknown'. This is the ONLY place this mapping lives.
    """
    if not category_id:
        return "unknown"
    return re.sub(r"(?<!^)(?=[A-Z])", "_", category_id).lower()


def _parse_iso_utc(raw: str | None) -> datetime | None:
    """Parse an ISO 8601 timestamp ('...Z' or with offset) to UTC datetime."""
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _dedup_key(event_id: str, latest_geometry_date_iso: str) -> str:
    """Composite dedup key: same id + same timeline -> suppress; timeline advance -> re-publish."""
    return f"eonet:{event_id}:{latest_geometry_date_iso}"


def init_eonet_observed_table(db: sqlite3.Connection) -> None:
    db.execute("""
        CREATE TABLE IF NOT EXISTS eonet_observed (
            event_id TEXT PRIMARY KEY,
            category_id TEXT,
            last_observed_at TEXT NOT NULL
        )
    """)
    db.commit()


def get_observed(db: sqlite3.Connection) -> dict[str, str | None]:
    cur = db.execute("SELECT event_id, category_id FROM eonet_observed")
    return {row[0]: row[1] for row in cur.fetchall()}


def mark_observed(db: sqlite3.Connection, event_id: str, category_id: str | None) -> None:
    db.execute(
        """
        INSERT INTO eonet_observed (event_id, category_id, last_observed_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (event_id) DO UPDATE SET
            category_id = excluded.category_id,
            last_observed_at = CURRENT_TIMESTAMP
        """,
        (event_id, category_id),
    )
    db.commit()


def mark_retired(db: sqlite3.Connection, event_ids: set[str]) -> None:
    for event_id in event_ids:
        db.execute("DELETE FROM eonet_observed WHERE event_id = ?", (event_id,))
    db.commit()


class EONETSettings(BaseModel):
    """Settings schema for NASA EONET adapter.

    category_allowlist defaults to ALL upstream categories. PM call: keep the
    knob symmetric with GDACS event_types — operator opts OUT per-category if
    duplicate events on gdacs.* and eonet.* subjects become a problem in
    practice. Empirical note: in a 200-event upstream sample, ~77.5% of events
    for wildfires/floods/severeStorms/volcanoes were GDACS-sourced. Disable
    those categories here if downstream subscribers already consume the
    GDACS adapter.
    """

    category_allowlist: list[str] = Field(default=list(_DEFAULT_CATEGORIES))
    region: RegionConfig | None = None


class EONETAdapter(SourceAdapter):
    """NASA EONET v3 natural-event tracker adapter."""

    name = "eonet"
    display_name = "NASA EONET — Earth Observatory"
    description = (
        "Natural event tracker from NASA EONET v3. Note: heavy GDACS overlap "
        "for wildfires/floods/severeStorms/volcanoes — disable per-category "
        "via the allowlist below if duplicate events on gdacs.* and eonet.* "
        "subjects are not wanted."
    )
    settings_schema = EONETSettings
    requires_api_key = None
    api_key_field = None
    wizard_order = None
    default_cadence_s = 1800
    dedup_sweep_days = 30

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
        self.category_allowlist: list[str] = list(
            config.settings.get("category_allowlist", _DEFAULT_CATEGORIES)
        )
        region_dict = config.settings.get("region")
        self.region: RegionConfig | None = (
            RegionConfig(**region_dict) if region_dict else None
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
        init_eonet_observed_table(self._db)
        self._db.commit()
        logger.info(
            "EONET adapter started",
            extra={
                "categories": self.category_allowlist,
                "region": self.region.model_dump() if self.region else None,
            },
        )

    async def shutdown(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
        if self._db:
            self._db.close()
            self._db = None
        logger.info("EONET adapter shut down")

    async def apply_config(self, new_config: AdapterConfig) -> None:
        self.category_allowlist = list(
            new_config.settings.get("category_allowlist", _DEFAULT_CATEGORIES)
        )
        region_dict = new_config.settings.get("region")
        self.region = RegionConfig(**region_dict) if region_dict else None
        logger.info(
            "EONET config updated",
            extra={
                "categories": self.category_allowlist,
                "region": self.region.model_dump() if self.region else None,
            },
        )

    def subject_for(self, event: Event) -> str:
        # event.category is "disaster.eonet.<subject_category>[.removed]"
        parts = event.category.split(".")
        subj_cat = parts[2] if len(parts) >= 3 else "unknown"
        if len(parts) >= 4 and parts[-1] == "removed":
            return f"central.disaster.eonet.{subj_cat}.removed.global"
        return f"central.disaster.eonet.{subj_cat}.global"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=30),
        retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError)),
    )
    async def _fetch(self) -> str:
        if not self._session:
            raise RuntimeError("Session not initialized")
        async with self._session.get(
            EONET_EVENTS_URL, headers={"User-Agent": "Central/0.4"}
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
            logger.error("EONET fetch failed", extra={"error": str(e)})
            raise

        try:
            payload = json.loads(content)
        except json.JSONDecodeError as e:
            logger.error("EONET JSON parse error", extra={"error": str(e)})
            raise

        items = payload.get("events", [])
        logger.info("EONET fetch completed", extra={"item_count": len(items)})

        observed_before = get_observed(self._db)
        current_ids: set[str] = set()
        events_yielded = 0

        for item in items:
            event_id = item.get("id")
            if not event_id:
                continue

            categories = item.get("categories") or []
            category_id: str | None = None
            if categories and isinstance(categories[0], dict):
                category_id = categories[0].get("id")

            if not category_id or category_id not in self.category_allowlist:
                continue

            subject_cat = _subject_category(category_id)

            geometry = item.get("geometry") or []
            if geometry:
                latest = max(geometry, key=lambda g: g.get("date") or "")
            else:
                latest = None

            latest_date_iso = (latest or {}).get("date") or ""
            event_time = _parse_iso_utc(latest_date_iso) or datetime.now(timezone.utc)

            coords = (latest or {}).get("coordinates")
            centroid: tuple[float, float] | None = None
            if (
                isinstance(coords, list)
                and len(coords) == 2
                and all(isinstance(c, (int, float)) for c in coords)
            ):
                centroid = (float(coords[0]), float(coords[1]))  # GeoJSON (lon, lat)

            if self.region is not None:
                if centroid is None:
                    continue
                lon, lat = centroid
                if not (
                    self.region.west <= lon <= self.region.east
                    and self.region.south <= lat <= self.region.north
                ):
                    continue

            current_ids.add(event_id)

            geo = Geo(
                centroid=centroid,
                bbox=None,
                regions=[],
                primary_region=None,
            )

            magnitude_value = (latest or {}).get("magnitudeValue")
            magnitude_unit = (latest or {}).get("magnitudeUnit")

            sources = item.get("sources") or []
            data: dict[str, Any] = {
                "event_id": event_id,
                "category_id": category_id,
                "title": item.get("title") or "",
                "description": item.get("description") or "",
                "url": item.get("link") or "",
                "closed": item.get("closed"),
                "sources": [
                    {"id": s.get("id"), "url": s.get("url")} for s in sources
                ],
                "magnitudeValue": magnitude_value,
                "magnitudeUnit": magnitude_unit,
                "latest_geometry_date": latest_date_iso or None,
            }

            # Mirror centroid (lon, lat) into top-level data keys for the flat
            # enrichment path (see enrichment_locations).
            if centroid is not None:
                data["latitude"] = centroid[1]
                data["longitude"] = centroid[0]

            dedup_key = _dedup_key(event_id, latest_date_iso)

            if self.is_published(dedup_key):
                mark_observed(self._db, event_id, category_id)
                continue

            event = Event(
                id=event_id,
                adapter=self.name,
                category=f"disaster.eonet.{subject_cat}",
                time=event_time,
                severity=0,
                geo=geo,
                data=data,
            )

            yield event
            self.mark_published(dedup_key)
            mark_observed(self._db, event_id, category_id)
            events_yielded += 1

        # Fall-off: events present in observed_before but absent from this poll
        fallen_off = set(observed_before.keys()) - current_ids
        for event_id in fallen_off:
            prior_category_id = observed_before[event_id]
            if prior_category_id and prior_category_id not in self.category_allowlist:
                # Was published before settings narrowed; clean up silently.
                mark_retired(self._db, {event_id})
                continue
            subject_cat = _subject_category(prior_category_id)
            tombstone_id = f"{event_id}:removed"
            tombstone_dedup_key = _dedup_key(tombstone_id, "")
            if self.is_published(tombstone_dedup_key):
                mark_retired(self._db, {event_id})
                continue
            tombstone = Event(
                id=tombstone_id,
                adapter=self.name,
                category=f"disaster.eonet.{subject_cat}.removed",
                time=datetime.now(timezone.utc),
                severity=0,
                geo=Geo(),
                data={
                    "event_id": event_id,
                    "category_id": prior_category_id,
                    "reason": "missing_from_feed",
                },
            )
            yield tombstone
            self.mark_published(tombstone_dedup_key)
            mark_retired(self._db, {event_id})
            events_yielded += 1

        self.sweep_old_ids()
        logger.info(
            "EONET poll completed",
            extra={
                "events_yielded": events_yielded,
                "current_observed": len(current_ids),
                "fallen_off": len(fallen_off),
            },
        )
