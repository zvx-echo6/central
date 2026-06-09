"""avalanche.org public map-layer adapter — backcountry avalanche advisories.

Polls ``https://api.avalanche.org/v2/public/products/map-layer/{center_id}`` per
configured center (defaults: SNFAC Sawtooth, PAC Payette). Each response is a
GeoJSON FeatureCollection where every feature is a forecast zone Polygon with
``properties.danger_level`` (1 Low → 5 Extreme; -1 = "no rating" during the
off-season).

Severity gate (per v0.10.10 meshai spec): only ``danger_level >= 3`` events
publish. ``danger_level < 3``, ``danger_level == -1``, and ``off_season=true``
are all omitted at adapter level -- no Event yielded, no publish, no record in
``published_ids``. During Idaho summer this means SNFAC + PAC both yield zero
events; that's correct off-season behavior.

danger_level → Event.severity (= centralseverity) follows Central's
4-most-severe convention (consistent with nws.SEVERITY_MAP):

    5 (Extreme)      → 4
    4 (High)         → 3
    3 (Considerable) → 2

(meshai's original spec had this inverted; corrected after a clarifying check.)

Subject convention: ``central.avy.advisory.us.{state_lower}`` -- one subject
per state, all zones for that state collapse to it (JetStream dedup is per
(msg_id, stream) and msg_id is now category-discriminated post-v0.10.8 so
multiple zones in the same state coexist cleanly).

Geometry passes through as the upstream Polygon; centroid computed via
shapely. Latitude/longitude in ``data.data`` mirror the centroid so the
supervisor's enrichment pipeline can geocode if configured.
"""

from __future__ import annotations

import asyncio
import logging
import re
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
from pydantic import BaseModel
from shapely.geometry import shape as shapely_shape
from tenacity import (
    after_nothing,
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

_BASE_URL = "https://api.avalanche.org/v2/public/products/map-layer"
_FETCH_TIMEOUT_S = 30
_TRAVEL_ADVICE_MAX_CHARS = 200

# avalanche.org danger_level → Central event.severity (= centralseverity).
# Higher = more severe, matching nws.SEVERITY_MAP convention (Central-wide).
_DANGER_TO_SEVERITY: dict[int, int] = {3: 2, 4: 3, 5: 4}

_DEDUP_DDL = (
    "CREATE TABLE IF NOT EXISTS published_ids ("
    "adapter TEXT NOT NULL, event_id TEXT NOT NULL, "
    "first_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "PRIMARY KEY (adapter, event_id))"
)


def _slug(text: str) -> str:
    """Lowercase + collapse runs of non-alphanum to single underscores.

    'Sawtooth & Western Smoky Mtns' → 'sawtooth_western_smoky_mtns'.
    """
    return re.sub(r"[^a-zA-Z0-9]+", "_", text or "").strip("_").lower()


def _parse_iso(value: Any) -> datetime | None:
    """Parse an upstream ISO datetime (possibly timezone-naive) into UTC."""
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        # avalanche.org returns naive strings tagged with a separate timezone
        # field; treat naive as UTC for our internal timeline -- the precise
        # local tz lives in data.data and consumers convert if needed.
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _centroid(geometry: dict[str, Any] | None) -> tuple[float, float] | None:
    """Return (lon, lat) of a GeoJSON Polygon/MultiPolygon centroid; None on bad input."""
    if not geometry:
        return None
    try:
        c = shapely_shape(geometry).centroid
        return (float(c.x), float(c.y))
    except Exception:
        return None


class AvalancheOrgSettings(BaseModel):
    """``center_ids`` lists the avalanche center IDs to poll. Defaults are
    Sawtooth (SNFAC) + Payette (PAC) for Idaho-region coverage; operator can
    extend to any avalanche.org-recognised center (NWAC, CAIC, etc.)."""

    center_ids: list[str] = ["SNFAC", "PAC"]


_EXP_WAIT = wait_exponential_jitter(initial=1, max=30)


class AvalancheOrgAdapter(SourceAdapter):
    """avalanche.org backcountry advisory map-layer poller."""

    name = "avalanche_org"
    display_name = "avalanche.org Forecast Centers"
    description = (
        "Backcountry avalanche advisories from avalanche.org's per-center map "
        "layers. Severity-gated: only Considerable (3) and above publish; "
        "off-season + 'no rating' zones are omitted."
    )
    settings_schema = AvalancheOrgSettings
    requires_api_key = None
    wizard_order = None
    default_cadence_s = 1800  # 30 min — matches avalanche.org daily-update cadence
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
        self._center_ids: list[str] = list(
            config.settings.get("center_ids") or ["SNFAC", "PAC"]
        )

    async def startup(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_FETCH_TIMEOUT_S),
            headers={"User-Agent": "Central/0.10 (+avalanche_org)"},
        )
        self._db = sqlite3.connect(self._cursor_db_path)
        self._db.execute(_DEDUP_DDL)
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS published_ids_last_seen "
            "ON published_ids (last_seen)"
        )
        self._db.commit()
        logger.info(
            "avalanche_org adapter started",
            extra={"center_ids": self._center_ids},
        )

    async def shutdown(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
        if self._db:
            self._db.close()
            self._db = None

    async def apply_config(self, new_config: AdapterConfig) -> None:
        self._center_ids = list(
            new_config.settings.get("center_ids") or ["SNFAC", "PAC"]
        )
        logger.info(
            "avalanche_org config updated",
            extra={"center_ids": self._center_ids},
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=_EXP_WAIT,
        retry=retry_if_exception_type(
            (aiohttp.ClientConnectionError, asyncio.TimeoutError, TimeoutError)
        ),
        reraise=True,
        before_sleep=None,
        after=after_nothing,
    )
    async def _fetch(self, center_id: str) -> dict[str, Any] | None:
        """GET one center's map-layer. Returns parsed JSON, or None on permanent error."""
        if self._session is None:
            raise RuntimeError("avalanche_org session not started")
        async with self._session.get(f"{_BASE_URL}/{center_id}") as resp:
            if resp.status == 200:
                return await resp.json(content_type=None)
            body_preview = (await resp.text())[:200]
            logger.warning(
                "avalanche_org upstream non-200; skipping center this poll",
                extra={"center_id": center_id, "status": resp.status,
                       "body": body_preview},
            )
            return None

    def _build_event_record(
        self, feature: dict[str, Any], center_id: str
    ) -> Event | None:
        """Build an Event from one map-layer feature; returns None if gated out."""
        props = feature.get("properties") or {}
        if props.get("off_season"):
            return None
        danger_level = props.get("danger_level")
        severity = _DANGER_TO_SEVERITY.get(danger_level if isinstance(danger_level, int) else -999)
        if severity is None:
            # danger_level not in {3,4,5}: omit (covers -1 / 0 / 1 / 2 / unset).
            return None

        zone_name = props.get("name") or "unknown"
        state = (props.get("state") or "").strip()
        if not state:
            return None
        geometry = feature.get("geometry")
        centroid = _centroid(geometry)
        if centroid is None:
            return None
        lon, lat = centroid

        valid_dt = _parse_iso(props.get("start_date")) or datetime.now(timezone.utc)
        advice = (props.get("travel_advice") or "")[:_TRAVEL_ADVICE_MAX_CHARS]

        return Event(
            id=f"{center_id}_{_slug(zone_name)}",
            adapter=self.name,
            category=f"avy.advisory.{center_id.lower()}",
            time=valid_dt,
            expires=_parse_iso(props.get("end_date")),
            severity=severity,
            geo=Geo(
                centroid=(lon, lat),
                geometry=geometry,
                regions=[f"US-{state}"],
                primary_region=f"US-{state}",
            ),
            data={
                "center_id": center_id,
                "zone_name": zone_name,
                "danger_level": danger_level,
                "danger_name": props.get("danger"),
                "travel_advice": advice,
                "state": state,
                "valid_date": props.get("start_date"),
                "end_date": props.get("end_date"),
                "off_season": False,
                "latitude": lat,
                "longitude": lon,
            },
        )

    async def poll(self) -> AsyncIterator[Event]:
        if not self._session:
            raise RuntimeError("Session not initialized")

        results = await asyncio.gather(
            *[self._fetch(c) for c in self._center_ids], return_exceptions=True,
        )

        yielded = 0
        omitted = 0
        for center_id, result in zip(self._center_ids, results):
            if isinstance(result, BaseException) or result is None:
                if isinstance(result, BaseException):
                    logger.warning(
                        "avalanche_org fetch failed",
                        extra={"center_id": center_id, "error": str(result)},
                    )
                continue
            features = result.get("features") or []
            for feat in features:
                try:
                    ev = self._build_event_record(feat, center_id)
                except Exception:
                    logger.exception(
                        "avalanche_org feature parse failed",
                        extra={"center_id": center_id},
                    )
                    omitted += 1
                    continue
                if ev is None:
                    omitted += 1
                    continue
                yield ev
                yielded += 1

        self.sweep_old_ids()
        logger.info(
            "avalanche_org poll completed",
            extra={"centers": self._center_ids,
                   "events_yielded": yielded, "events_omitted": omitted},
        )

    def subject_for(self, event: Event) -> str:
        state = (event.data.get("state") or "").lower() or "unknown"
        return f"central.avy.advisory.us.{state}"
