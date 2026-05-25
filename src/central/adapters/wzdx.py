"""WZDx adapter — FHWA Work Zone Data Exchange registry → work_zone events.

First adapter to use the v0.9.0 category/subject split: category="work_zone.wzdx"
(so the GUI's split_part(category,'.',1) surfaces event_type "work_zone") while the
NATS subject is "central.traffic.work_zone.{state}" on CENTRAL_TRAFFIC. Subject
state comes from the registry row (reliable, pre-enrichment); the geocoder state
is a fallback. Discovery is stateless per poll; dedup uses the shared cursors.db.
"""

import asyncio
import logging
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
from pydantic import BaseModel
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from central.adapter import SourceAdapter
from central.adapters.inciweb import STATE_NAME_TO_CODE
from central.config_models import AdapterConfig
from central.config_store import ConfigStore
from central.models import Event, Geo

logger = logging.getLogger(__name__)

# FHWA Work Zone Data Exchange Feed Registry (Socrata, public-unauth).
WZDX_REGISTRY_URL = "https://datahub.transportation.gov/resource/69qe-yiui.json?$limit=200"

# vehicle_impact -> severity. Locked: unknown/missing = 1 (real active zones).
_VEHICLE_IMPACT_SEVERITY = {"all-lanes-closed": 3, "some-lanes-closed": 2, "all-lanes-open": 1}
_DEFAULT_SEVERITY = 1

# Bounded per-poll fan-out (~21 feeds pass the filter; Iowa alone is ~1.4 MB).
_FEED_CONCURRENCY = 6
_FEED_TIMEOUT_S = 60

_DEDUP_DDL = (
    "CREATE TABLE IF NOT EXISTS published_ids ("
    "adapter TEXT NOT NULL, event_id TEXT NOT NULL, "
    "first_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "PRIMARY KEY (adapter, event_id))"
)


def _eligible(row: dict[str, Any]) -> bool:
    """Registry-row filter (v0.9.0 locked): geojson + active + no api key + v4.x."""
    return (
        row.get("format") == "geojson"
        and row.get("active") is True
        and row.get("needapikey") is not True
        and str(row.get("version") or "").startswith("4")
    )


def _state_code(state_name: str | None) -> str | None:
    """Full state name (registry/geocoder) -> 2-letter UPPER code, or None."""
    if not state_name:
        return None
    return STATE_NAME_TO_CODE.get(state_name.strip().lower())


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _flatten_geometry(
    geometry: dict[str, Any] | None,
) -> tuple[float | None, float | None]:
    """First (lat, lon) from a WZDx geometry (coords are [lon, lat]).

    LineString/MultiPoint -> coordinates[0]; Point -> coordinates. Anything else
    (Polygon, empty, missing) -> (None, None) so the event still publishes.
    """
    if not geometry:
        return (None, None)
    coords = geometry.get("coordinates")
    gtype = geometry.get("type")
    try:
        if gtype == "Point":
            lon, lat = coords[0], coords[1]
        elif gtype in ("LineString", "MultiPoint"):
            lon, lat = coords[0][0], coords[0][1]
        else:
            return (None, None)
        return (float(lat), float(lon))
    except (TypeError, IndexError, ValueError):
        return (None, None)


class WZDxSettings(BaseModel):
    """states: allowlist of 2-letter codes to poll; None = every eligible feed."""

    states: list[str] | None = None


class WZDxAdapter(SourceAdapter):
    """FHWA Work Zone Data Exchange registry-driven adapter."""

    name = "wzdx"
    display_name = "WZDx — Work Zone Data Exchange"
    description = (
        "Federal FHWA Work Zone Data Exchange. Discovers active state-DOT GeoJSON "
        "feeds from the WZDx Feed Registry and emits work_zone events."
    )
    settings_schema = WZDxSettings
    requires_api_key = None
    api_key_field = None
    wizard_order = None  # Ships disabled
    default_cadence_s = 600
    data_class = "event"
    # Canonical point-adapter paths (FIRMS/inciweb convention, enforced by
    # tests/test_enrichment_locations_coverage); the supervisor reverse-geocodes
    # them into data["_enriched"]["geocoder"] (city/county/state).
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
        self._states: set[str] | None = self._read_states(config)

    @staticmethod
    def _read_states(config: AdapterConfig) -> set[str] | None:
        raw = config.settings.get("states")
        if not raw:
            return None
        return {s.strip().upper() for s in raw if s and s.strip()} or None

    async def startup(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_FEED_TIMEOUT_S),
            headers={"User-Agent": "Central/0.9 (+wzdx)"},
        )
        self._db = sqlite3.connect(self._cursor_db_path)
        self._db.execute(_DEDUP_DDL)
        self._db.execute("CREATE INDEX IF NOT EXISTS published_ids_last_seen ON published_ids (last_seen)")
        self._db.commit()
        logger.info("WZDx adapter started", extra={"states": sorted(self._states) if self._states else None})

    async def shutdown(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
        if self._db:
            self._db.close()
            self._db = None

    async def apply_config(self, new_config: AdapterConfig) -> None:
        self._states = self._read_states(new_config)
        logger.info("WZDx config updated", extra={"states": sorted(self._states) if self._states else None})

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=30),
        retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError)),
    )
    async def _fetch_registry(self) -> list[dict[str, Any]]:
        assert self._session is not None
        async with self._session.get(WZDX_REGISTRY_URL) as resp:
            resp.raise_for_status()
            rows = await resp.json(content_type=None)
        return rows if isinstance(rows, list) else []

    async def _fetch_feed(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        """Fetch + parse one publisher feed; [] on any failure (never raises)."""
        assert self._session is not None
        url = (row.get("url") or {}).get("url")
        if not url:
            return []
        try:
            async with self._session.get(url) as resp:
                resp.raise_for_status()
                doc = await resp.json(content_type=None)
        except (aiohttp.ClientError, TimeoutError, asyncio.TimeoutError, ValueError) as exc:
            logger.warning("WZDx feed failed", extra={"feed": row.get("feedname"), "error": str(exc)})
            return []
        if not isinstance(doc, dict) or not isinstance(doc.get("features"), list):
            return []
        return doc["features"]

    def _discover(self, registry_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Eligible rows, optionally narrowed to the operator's state allowlist."""
        feeds: list[dict[str, Any]] = []
        for row in registry_rows:
            if not _eligible(row):
                continue
            if self._states is not None:
                code = _state_code(row.get("state"))
                if code is None or code not in self._states:
                    continue
            feeds.append(row)
        return feeds

    def _build_event(self, feature: dict[str, Any], row: dict[str, Any]) -> Event | None:
        """Map one WZDx RoadEventFeature to a Central Event (None to skip)."""
        props = feature.get("properties") or {}
        core = props.get("core_details") or {}
        if core.get("event_type") != "work-zone":
            return None  # only work-zone this PR; detour/restriction map later
        feature_id = feature.get("id")
        if feature_id is None:
            return None
        data_source_id = core.get("data_source_id") or row.get("feedname") or "wzdx"
        lat, lon = _flatten_geometry(feature.get("geometry"))
        code = _state_code(row.get("state"))
        return Event(
            id=f"{data_source_id}:{feature_id}",
            adapter=self.name,
            category="work_zone.wzdx",
            time=(_parse_dt(core.get("update_date")) or _parse_dt(props.get("start_date")) or datetime.now(timezone.utc)),
            expires=_parse_dt(props.get("end_date")),
            severity=_VEHICLE_IMPACT_SEVERITY.get(props.get("vehicle_impact"), _DEFAULT_SEVERITY),
            geo=Geo(
                centroid=(lon, lat) if lat is not None and lon is not None else None,
                regions=[f"US-{code}"] if code else [],
                primary_region=f"US-{code}" if code else None,
            ),
            data={
                "road_names": core.get("road_names") or [],
                "direction": core.get("direction"),
                "description": core.get("description"),
                "vehicle_impact": props.get("vehicle_impact"),
                "event_status": props.get("event_status"),
                "start_date": props.get("start_date"),
                "end_date": props.get("end_date"),
                "data_source_id": data_source_id,
                "feed_name": row.get("feedname"),
                "feed_state": row.get("state"),
                "feed_state_code": code,  # subject routing, fixed at poll time
                "latitude": lat,  # enrichment_locations pair (canonical paths)
                "longitude": lon,
            },
        )

    async def poll(self) -> AsyncIterator[Event]:
        """Discover eligible feeds, fetch concurrently, yield work_zone events."""
        if not self._session:
            raise RuntimeError("Session not initialized")
        try:
            registry_rows = await self._fetch_registry()
        except (aiohttp.ClientError, TimeoutError) as exc:
            logger.warning("WZDx registry fetch failed; skipping cycle", extra={"error": str(exc)})
            return

        feeds = self._discover(registry_rows)
        logger.info("WZDx discovered feeds", extra={"eligible": len(feeds)})
        sem = asyncio.Semaphore(_FEED_CONCURRENCY)

        async def _guarded(row: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
            async with sem:
                return row, await self._fetch_feed(row)

        results = await asyncio.gather(*[_guarded(r) for r in feeds])
        yielded = 0
        for row, features in results:
            for feature in features:
                try:
                    event = self._build_event(feature, row)
                except Exception:  # one bad feature never sinks a poll
                    logger.exception("WZDx feature parse failed", extra={"feed": row.get("feedname")})
                    continue
                if event is None:
                    continue
                yield event
                yielded += 1

        self.sweep_old_ids()
        logger.info("WZDx poll completed", extra={"events_yielded": yielded})

    def subject_for(self, event: Event) -> str:
        """central.traffic.work_zone.{state}; registry code first, geocoder fallback."""
        code = event.data.get("feed_state_code")
        if not code:
            enr = (event.data.get("_enriched") or {}).get("geocoder") or {}
            code = _state_code(enr.get("state"))
        return f"central.traffic.work_zone.{code.lower() if code else 'unknown'}"
