"""TomTom Traffic Incidents adapter — commercial real-time incidents (event).

Polls the TomTom Orbis incidentDetails endpoint for configured bounding boxes
(each must be <= 10,000 km^2 per the API limit), emitting one event per incident
to CENTRAL_TRAFFIC (subject central.traffic.incident.{state}). Discrete events
with start/end times -> data_class="event". The incident geometry (Point or
LineString) is already GeoJSON lon/lat, shipped via geo.geometry (the v0.9.3
framework) so the affected road renders as a polyline on the map.

Dedup is inherited from SourceAdapter; ids use the upstream-stable TomTom id.
"""

import asyncio
import logging
import math
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
from pydantic import BaseModel, field_validator, model_validator
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

_INCIDENTS_URL = "https://api.tomtom.com/maps/orbis/traffic/incidentDetails"
_FIELDS = ("{incidents{type,geometry{type,coordinates},properties{id,iconCategory,"
           "magnitudeOfDelay,events{description,code},startTime,endTime,from,to,"
           "length,delay,roadNumbers,timeValidity}}}")
# TomTom magnitudeOfDelay (0 unknown, 1 minor, 2 moderate, 3 major, 4 undefined/
# closure) -> severity; never None (v0.8.0 "real signal or 1" rule).
_MAGNITUDE_SEVERITY = {0: 1, 1: 1, 2: 2, 3: 3, 4: 4}
_FETCH_CONCURRENCY = 4
_FETCH_TIMEOUT_S = 30

_DEDUP_DDL = (
    "CREATE TABLE IF NOT EXISTS published_ids ("
    "adapter TEXT NOT NULL, event_id TEXT NOT NULL, "
    "first_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "PRIMARY KEY (adapter, event_id))"
)

_MAX_BBOX_AREA_KM2 = 10_000.0   # TomTom incidentDetails hard cap per bbox
_MIN_BBOX_CADENCE_S = 60        # per-bbox poll-interval floor
_EARTH_RADIUS_KM = 6371.0
_SECONDS_PER_MONTH = 30 * 24 * 3600  # 2_592_000, for quota estimation
# TomTom Orbis free tier: 2,500 incidentDetails calls/month. A paid-tier
# operator can raise this ceiling (v0.9.9 follow-up hook).
TOMTOM_FREE_TIER_CALLS_PER_MONTH = 2500


def _bbox_area_km2(min_lon: float, min_lat: float, max_lon: float, max_lat: float) -> float:
    """Spherical area of a lon/lat bbox in km^2."""
    lat1, lat2 = math.radians(min_lat), math.radians(max_lat)
    dlon = math.radians(max_lon - min_lon)
    return _EARTH_RADIUS_KM ** 2 * abs(math.sin(lat2) - math.sin(lat1)) * abs(dlon)


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _first_vertex(geom: dict[str, Any] | None) -> tuple[float | None, float | None]:
    """First (lat, lon) from a Point or LineString (coords are [lon, lat])."""
    coords = (geom or {}).get("coordinates")
    gtype = (geom or {}).get("type")
    try:
        if gtype == "Point":
            return (float(coords[1]), float(coords[0]))
        if gtype == "LineString":
            return (float(coords[0][1]), float(coords[0][0]))
    except (TypeError, IndexError, ValueError):
        pass
    return (None, None)


class BBox(BaseModel):
    name: str
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float
    state_code: str
    cadence_s: int | None = None  # per-bbox poll interval; None -> adapter default_cadence_s

    @field_validator("cadence_s")
    @classmethod
    def _cadence_floor(cls, v: int | None) -> int | None:
        if v is not None and v < _MIN_BBOX_CADENCE_S:
            raise ValueError(f"cadence_s must be >= {_MIN_BBOX_CADENCE_S} seconds")
        return v

    @model_validator(mode="after")
    def _validate_box(self) -> "BBox":
        if not (-180.0 <= self.min_lon < self.max_lon <= 180.0):
            raise ValueError("require -180 <= min_lon < max_lon <= 180")
        if not (-90.0 <= self.min_lat < self.max_lat <= 90.0):
            raise ValueError("require -90 <= min_lat < max_lat <= 90")
        area = _bbox_area_km2(self.min_lon, self.min_lat, self.max_lon, self.max_lat)
        if area > _MAX_BBOX_AREA_KM2:
            raise ValueError(
                f"bbox area {area:.0f} km^2 exceeds TomTom limit of {_MAX_BBOX_AREA_KM2:.0f} km^2"
            )
        return self


class TomTomIncidentsSettings(BaseModel):
    """bboxes: metro boxes to poll (each <= 10,000 km^2). api_key_alias: config key."""

    bboxes: list[BBox] = []
    api_key_alias: str = "tomtom"

    @model_validator(mode="after")
    def _unique_names(self) -> "TomTomIncidentsSettings":
        names = [b.name for b in self.bboxes]
        dupes = sorted({n for n in names if names.count(n) > 1})
        if dupes:
            raise ValueError(f"duplicate bbox names: {', '.join(dupes)}")
        return self


class TomTomIncidentsAdapter(SourceAdapter):
    """TomTom Orbis incidentDetails adapter (per-bbox real-time incidents)."""

    name = "tomtom_incidents"
    display_name = "TomTom Traffic Incidents"
    description = (
        "Real-time traffic incidents (closures, jams, hazards, road work) from "
        "TomTom Orbis incidentDetails for configured metro bboxes (each <= 10,000 km^2)."
    )
    settings_schema = TomTomIncidentsSettings
    requires_api_key = "tomtom"
    api_key_field = "api_key_alias"
    wizard_order = None  # Ships disabled
    default_cadence_s = 1800
    data_class = "event"
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
        self._bboxes: list[BBox] = self._read_bboxes(config)
        self._api_key_alias: str = config.settings.get("api_key_alias", "tomtom")
        self._api_key: str | None = None
        self._last_polled: dict[str, datetime] = {}  # bbox name -> last successful fetch (in-memory)

    @staticmethod
    def _read_bboxes(config: AdapterConfig) -> list[BBox]:
        return [BBox(**b) for b in (config.settings.get("bboxes") or [])]

    def _redact(self, text: str) -> str:
        return text.replace(self._api_key, "<KEY>") if self._api_key else text

    def _bbox_due(self, bbox: "BBox", now: datetime) -> bool:
        """True if this bbox is due to poll (never polled this process, or its
        per-bbox cadence_s -- falling back to default_cadence_s -- has elapsed)."""
        last = self._last_polled.get(bbox.name)
        if last is None:
            return True
        return (now - last).total_seconds() >= (bbox.cadence_s or self.default_cadence_s)

    async def startup(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_FETCH_TIMEOUT_S),
            headers={"User-Agent": "Central/0.9 (+tomtom_incidents)"},
        )
        self._db = sqlite3.connect(self._cursor_db_path)
        self._db.execute(_DEDUP_DDL)
        self._db.execute("CREATE INDEX IF NOT EXISTS published_ids_last_seen ON published_ids (last_seen)")
        self._db.commit()
        self._api_key = await self._config_store.get_api_key(self._api_key_alias)
        logger.info("tomtom_incidents adapter started",
                    extra={"bboxes": len(self._bboxes), "api_key_present": bool(self._api_key)})

    async def shutdown(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
        if self._db:
            self._db.close()
            self._db = None

    async def apply_config(self, new_config: AdapterConfig) -> None:
        self._bboxes = self._read_bboxes(new_config)
        self._api_key_alias = new_config.settings.get("api_key_alias", "tomtom")
        self._api_key = await self._config_store.get_api_key(self._api_key_alias)
        logger.info("tomtom_incidents config updated",
                    extra={"bboxes": len(self._bboxes), "api_key_present": bool(self._api_key)})

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=30),
        retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError)),
    )
    async def _fetch_bbox(self, bbox: BBox) -> list[dict[str, Any]]:
        assert self._session is not None
        params = {
            "bbox": f"{bbox.min_lon},{bbox.min_lat},{bbox.max_lon},{bbox.max_lat}",
            "fields": _FIELDS,
            "key": self._api_key,
            "apiVersion": "1",
        }
        async with self._session.get(_INCIDENTS_URL, params=params) as resp:
            resp.raise_for_status()
            doc = await resp.json(content_type=None)
        return doc.get("incidents") or []

    def _build_event(self, inc: dict[str, Any], bbox: BBox) -> Event | None:
        props = inc.get("properties") or {}
        tid = props.get("id")
        if not tid:
            return None
        geom = inc.get("geometry") or {}
        lat, lon = _first_vertex(geom)
        events = props.get("events") or []
        first = events[0] if events else {}
        return Event(
            id=f"{bbox.state_code}:tomtom:{tid}",
            adapter=self.name,
            category="incident.tomtom_incidents",
            time=(_parse_iso(props.get("startTime")) or datetime.now(timezone.utc)),
            expires=_parse_iso(props.get("endTime")),
            severity=_MAGNITUDE_SEVERITY.get(props.get("magnitudeOfDelay"), 1),
            geo=Geo(
                centroid=(lon, lat) if lat is not None and lon is not None else None,
                geometry=geom if geom.get("coordinates") else None,
                regions=[f"US-{bbox.state_code}"],
                primary_region=f"US-{bbox.state_code}",
            ),
            data={
                "description": first.get("description"),
                "event_code": first.get("code"),
                "from": props.get("from"),
                "to": props.get("to"),
                "magnitude_of_delay": props.get("magnitudeOfDelay"),
                "icon_category": props.get("iconCategory"),
                "length": props.get("length"),
                "delay": props.get("delay"),
                "road_numbers": props.get("roadNumbers") or [],
                "start_time": props.get("startTime"),
                "end_time": props.get("endTime"),
                "time_validity": props.get("timeValidity"),
                "state_code": bbox.state_code,
                "bbox_name": bbox.name,
                "latitude": lat,
                "longitude": lon,
            },
        )

    async def poll(self) -> AsyncIterator[Event]:
        if not self._session:
            raise RuntimeError("Session not initialized")
        if not self._api_key:
            logger.warning("tomtom_incidents: no API key for alias; skipping poll",
                           extra={"alias": self._api_key_alias})
            return
        sem = asyncio.Semaphore(_FETCH_CONCURRENCY)
        now = datetime.now(timezone.utc)
        due = [b for b in self._bboxes if self._bbox_due(b, now)]

        async def _one(bbox: BBox) -> list[Event]:
            async with sem:
                try:
                    incidents = await self._fetch_bbox(bbox)
                except (aiohttp.ClientError, TimeoutError) as exc:
                    logger.warning("tomtom_incidents bbox fetch failed",
                                   extra={"bbox": bbox.name, "error": self._redact(str(exc))})
                    return []
                self._last_polled[bbox.name] = now  # only after a successful fetch
                out: list[Event] = []
                for inc in incidents:
                    try:
                        ev = self._build_event(inc, bbox)
                    except Exception:
                        logger.exception("tomtom_incidents parse failed", extra={"bbox": bbox.name})
                        continue
                    if ev is not None:
                        out.append(ev)
                return out

        results = await asyncio.gather(*[_one(b) for b in due])
        yielded = 0
        for evs in results:
            for ev in evs:
                yield ev
                yielded += 1

        self.sweep_old_ids()
        logger.info("tomtom_incidents poll completed",
                    extra={"events_yielded": yielded, "bboxes": len(self._bboxes)})

    def subject_for(self, event: Event) -> str:
        code = (event.data.get("state_code") or "").lower() or "unknown"
        return f"central.traffic.incident.{code}"

    @classmethod
    def quota_estimate(cls, settings: BaseModel, cadence_s: int) -> dict | None:
        bboxes = getattr(settings, "bboxes", None) or []
        if not bboxes:
            return None
        calls = sum(
            _SECONDS_PER_MONTH / max(cadence_s, b.cadence_s or cls.default_cadence_s)
            for b in bboxes
        )
        calls_per_month = round(calls)
        cap = TOMTOM_FREE_TIER_CALLS_PER_MONTH
        percent = (calls_per_month / cap * 100) if cap else 0.0
        return {
            "calls_per_month": calls_per_month,
            "cap": cap,
            "seconds_per_month": _SECONDS_PER_MONTH,
            "default_cadence_s": cls.default_cadence_s,
            "percent": percent,
            "warn": percent >= 80.0,
            "blocked": percent >= 100.0,
            "detail": (
                f"{calls_per_month:,} est. calls/month across {len(bboxes)} "
                f"bbox(es) vs {cap:,}/month free tier ({percent:.0f}%)"
            ),
        }
