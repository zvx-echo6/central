"""USGS NWIS (National Water Information System) adapter — OGC API v0."""

import json
import logging
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

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

NWIS_LATEST_CONTINUOUS_URL = (
    "https://api.waterdata.usgs.gov/ogcapi/v0/collections/latest-continuous/items"
)
NWIS_MONITORING_LOCATIONS_URL = (
    "https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items"
)
# Per-render cap for the settings-driven preview (PR G.5). Keep small so the
# /adapters/<name> edit page renders quickly.
_PREVIEW_LIMIT = 50

# Single source of truth for the parameter-code default. Operators tune via
# NWISSettings.parameter_codes; do NOT duplicate this list elsewhere
# (tests, fixtures, migration JSON all derive from NWISSettings defaults).
# Codes are USGS pcodes — see /api/v3/parameter-codes for the registry.
#   00060 = Discharge, cubic feet per second
#   00065 = Gage height, feet
#   00010 = Temperature, water, degrees Celsius
_DEFAULT_PARAMETER_CODES: list[str] = ["00060", "00065", "00010"]

# Per-request page size cap. Upstream maxes around 10000; we use a
# moderate value to balance pagination overhead vs latency.
_PAGE_LIMIT = 1000


def _subject_tokens_for_id(monitoring_location_id: str) -> tuple[str, str]:
    """Decompose an agency-prefixed monitoring_location_id into (agency, bare_site_no).

    Examples:
        USGS-05420500   -> ("usgs", "05420500")
        MO005-400105... -> ("mo005", "400105...")
        no-dash-id      -> ("unknown", "no-dash-id"-lowercased; effectively the whole id)

    This is the ONLY place this decomposition lives — subject_for() and
    Event.category construction both call through here.
    """
    if "-" not in monitoring_location_id:
        return ("unknown", monitoring_location_id.lower())
    agency, bare = monitoring_location_id.split("-", 1)
    return (agency.lower(), bare)


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


def _dedup_key(monitoring_location_id: str, parameter_code: str, time_iso: str) -> str:
    """Composite dedup: same site+param+measurement-time -> suppress; new time -> re-publish."""
    return f"nwis:{monitoring_location_id}:{parameter_code}:{time_iso}"


def _next_link(page: dict) -> str | None:
    """Extract OGC API pagination 'next' link href, or None if absent."""
    for link in page.get("links") or []:
        if link.get("rel") == "next" and link.get("href"):
            return link["href"]
    return None


class NWISSettings(BaseModel):
    """Settings schema for USGS NWIS adapter.

    bbox via RegionConfig is REQUIRED in practice — without a region the
    upstream endpoint returns CONUS-wide records (tens of thousands per poll).
    Adapter logs WARN at startup if region is None; it does not refuse to
    start (operator may be testing).
    """

    parameter_codes: list[str] = Field(default=list(_DEFAULT_PARAMETER_CODES))
    region: RegionConfig | None = None


class NWISAdapter(SourceAdapter):
    """USGS NWIS adapter via the OGC API v0 `latest-continuous` collection."""

    name = "nwis"
    display_name = "USGS NWIS — Water Data (OGC)"
    description = (
        "USGS National Water Information System via the OGC API "
        "(latest-continuous collection). Polls the configured parameter codes "
        "within the configured bbox. Default params: discharge (00060), "
        "gage height (00065), water temperature (00010). Operator opts in to "
        "more via parameter_codes. bbox is REQUIRED — without one the endpoint "
        "returns the entire US (tens of thousands of records per poll)."
    )
    settings_schema = NWISSettings
    requires_api_key = None
    api_key_field = None
    wizard_order = None
    default_cadence_s = 900

    # Site lat/lon mirrored from Geo.centroid into event.data (see _build_event).
    enrichment_locations = [("latitude", "longitude")]

    # Continuous high-volume water-gauge feed -> the /telemetry tab, not /events.
    data_class = "telemetry"

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
        self.parameter_codes: list[str] = list(
            config.settings.get("parameter_codes", _DEFAULT_PARAMETER_CODES)
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
        self._db.commit()
        if self.region is None:
            logger.warning(
                "NWIS started without region bbox — upstream will return CONUS-wide records on every poll. "
                "Set region via the GUI before relying on this adapter."
            )
        logger.info(
            "NWIS adapter started",
            extra={
                "parameter_codes": self.parameter_codes,
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
        logger.info("NWIS adapter shut down")

    async def apply_config(self, new_config: AdapterConfig) -> None:
        self.parameter_codes = list(
            new_config.settings.get("parameter_codes", _DEFAULT_PARAMETER_CODES)
        )
        region_dict = new_config.settings.get("region")
        self.region = RegionConfig(**region_dict) if region_dict else None
        logger.info(
            "NWIS config updated",
            extra={
                "parameter_codes": self.parameter_codes,
                "region": self.region.model_dump() if self.region else None,
            },
        )

    def is_published(self, dedup_key: str) -> bool:
        if not self._db:
            return False
        cur = self._db.execute(
            "SELECT 1 FROM published_ids WHERE adapter = ? AND event_id = ?",
            (self.name, dedup_key),
        )
        return cur.fetchone() is not None

    def mark_published(self, dedup_key: str) -> None:
        if not self._db:
            return
        self._db.execute(
            """
            INSERT INTO published_ids (adapter, event_id, first_seen, last_seen)
            VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (adapter, event_id) DO UPDATE SET
                last_seen = CURRENT_TIMESTAMP
            """,
            (self.name, dedup_key),
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
            logger.info("NWIS swept old dedup entries", extra={"count": count})
        return count

    def subject_for(self, event: Event) -> str:
        # event.category is "hydro.<parameter_code>.<agency>.<bare_site_no>"
        parts = event.category.split(".")
        if len(parts) >= 4:
            return f"central.hydro.{parts[1]}.{parts[2]}.{parts[3]}"
        return "central.hydro.unknown.unknown.unknown"

    def _initial_url(self, parameter_code: str) -> str:
        params: dict[str, str] = {
            "parameter_code": parameter_code,
            "limit": str(_PAGE_LIMIT),
        }
        if self.region is not None:
            params["bbox"] = (
                f"{self.region.west},{self.region.south},"
                f"{self.region.east},{self.region.north}"
            )
        return f"{NWIS_LATEST_CONTINUOUS_URL}?{urlencode(params)}"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=30),
        retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError)),
    )
    async def _fetch(self, url: str) -> str:
        if not self._session:
            raise RuntimeError("Session not initialized")
        async with self._session.get(
            url, headers={"User-Agent": "Central/0.4"}
        ) as resp:
            resp.raise_for_status()
            return await resp.text()

    async def poll(self) -> AsyncIterator[Event]:
        if not self._db:
            raise RuntimeError("Database not initialized")

        events_yielded = 0
        for parameter_code in self.parameter_codes:
            url: str | None = self._initial_url(parameter_code)
            pages_fetched = 0
            features_seen = 0
            while url:
                try:
                    content = await self._fetch(url)
                except Exception as e:
                    logger.error(
                        "NWIS fetch failed",
                        extra={"error": str(e), "parameter_code": parameter_code},
                    )
                    raise
                try:
                    page = json.loads(content)
                except json.JSONDecodeError as e:
                    logger.error(
                        "NWIS JSON parse error",
                        extra={"error": str(e), "parameter_code": parameter_code},
                    )
                    raise
                pages_fetched += 1
                features = page.get("features") or []
                features_seen += len(features)

                for feature in features:
                    event = self._build_event(feature, parameter_code)
                    if event is None:
                        continue
                    dedup_key = _dedup_key(
                        event.data["monitoring_location_id"],
                        parameter_code,
                        event.data["time"],
                    )
                    if self.is_published(dedup_key):
                        continue
                    yield event
                    self.mark_published(dedup_key)
                    events_yielded += 1

                url = _next_link(page)

            logger.info(
                "NWIS parameter poll completed",
                extra={
                    "parameter_code": parameter_code,
                    "pages_fetched": pages_fetched,
                    "features_seen": features_seen,
                },
            )

        self.sweep_old_ids()
        logger.info(
            "NWIS poll completed",
            extra={"events_yielded": events_yielded},
        )

    def _build_event(self, feature: dict, parameter_code: str) -> Event | None:
        props = feature.get("properties") or {}
        monitoring_location_id = props.get("monitoring_location_id")
        if not monitoring_location_id:
            return None

        time_iso = props.get("time")
        event_time = _parse_iso_utc(time_iso)
        if event_time is None or not time_iso:
            return None

        value_raw = props.get("value")
        try:
            value = float(value_raw) if value_raw is not None else None
        except (TypeError, ValueError):
            value = None
        if value is None:
            return None

        geom = feature.get("geometry") or {}
        coords = geom.get("coordinates")
        centroid: tuple[float, float] | None = None
        if (
            isinstance(coords, list)
            and len(coords) == 2
            and all(isinstance(c, (int, float)) for c in coords)
        ):
            centroid = (float(coords[0]), float(coords[1]))  # GeoJSON (lon, lat)

        agency, bare_site_no = _subject_tokens_for_id(monitoring_location_id)

        data: dict[str, Any] = {
            "monitoring_location_id": monitoring_location_id,
            "parameter_code": parameter_code,
            "time": time_iso,
            "value": value,
            "unit_of_measure": props.get("unit_of_measure"),
            "statistic_id": props.get("statistic_id"),
            "approval_status": props.get("approval_status"),
            "qualifier": props.get("qualifier"),
            "time_series_id": props.get("time_series_id"),
            "last_modified": props.get("last_modified"),
        }

        # Mirror centroid (lon, lat) into top-level data keys so the flat
        # enrichment path can reach them (see enrichment_locations).
        if centroid is not None:
            data["latitude"] = centroid[1]
            data["longitude"] = centroid[0]

        return Event(
            id=f"{monitoring_location_id}:{parameter_code}:{time_iso}",
            adapter=self.name,
            category=f"hydro.{parameter_code}.{agency}.{bare_site_no}",
            time=event_time,
            severity=0,
            geo=Geo(centroid=centroid),
            data=data,
        )

    async def _fetch_preview_text(self, url: str) -> str:
        """One-shot GET for the preview render.

        Uses a fresh aiohttp session — preview must work even when the adapter
        isn't started (the GUI process never calls startup()). Factored out so
        tests can mock the HTTP call without touching aiohttp internals.
        """
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15),
        ) as session:
            async with session.get(
                url, headers={"User-Agent": "Central/0.4"}
            ) as resp:
                resp.raise_for_status()
                return await resp.text()

    async def preview_for_settings(self, settings: NWISSettings) -> list[dict] | None:
        """Surface monitoring-locations inside the configured bbox.

        Returns up to _PREVIEW_LIMIT rows from the monitoring-locations
        collection. Returns None if region is unset (no useful preview).
        Raises on HTTP / JSON / shape failure — framework catches at the route.
        """
        if settings.region is None:
            return None

        params = {
            "bbox": (
                f"{settings.region.west},{settings.region.south},"
                f"{settings.region.east},{settings.region.north}"
            ),
            "limit": str(_PREVIEW_LIMIT),
        }
        url = f"{NWIS_MONITORING_LOCATIONS_URL}?{urlencode(params)}"

        text = await self._fetch_preview_text(url)
        page = json.loads(text)
        features = page.get("features") or []

        rows: list[dict] = []
        for feat in features:
            props = feat.get("properties") or {}
            rows.append(
                {
                    "site_id": feat.get("id"),
                    "name": props.get("monitoring_location_name"),
                    "site_type": props.get("site_type_code"),
                    "state": props.get("state_name"),
                }
            )
        return rows
