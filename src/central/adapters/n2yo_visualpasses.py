"""n2yo_visualpasses adapter -- server-side visible-pass alerts (v0.12.1).

Complements satpass_predict (v0.11.1, SGP4-from-TLE): n2yo's API adds sun
illumination + visual magnitude, which local SGP4 propagation alone cannot
compute. Subject collision with satpass_predict on
``central.sat.pass.us.<state>.<observer_slug>`` is intentional; consumers
disambiguate via ``data.category`` (``pass.n2yo_visualpasses`` vs
``pass.satpass_predict``). Category-discriminated Nats-Msg-Id (v0.10.8)
keeps the JetStream dedup windows distinct.

The trailing ``/&apiKey=`` in the URL is n2yo's quirky convention, not a
typo. UTC fields in the response are Unix timestamps; ``mag`` is visual
magnitude (LOWER = BRIGHTER).
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
from pydantic import BaseModel

from central.adapter import SourceAdapter
from central.config_models import AdapterConfig
from central.config_store import ConfigStore
from central.models import Event, Geo

logger = logging.getLogger(__name__)

_FETCH_TIMEOUT_S = 30
_FETCH_CONCURRENCY = 4

_VISUALPASSES_URL = (
    "https://api.n2yo.com/rest/v1/satellite/visualpasses/"
    "{norad_id}/{lat}/{lng}/{alt}/{days}/{min_vis_s}/&apiKey={key}"
)

_DEDUP_DDL = (
    "CREATE TABLE IF NOT EXISTS published_ids ("
    "adapter TEXT NOT NULL, event_id TEXT NOT NULL, "
    "first_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "PRIMARY KEY (adapter, event_id))"
)


def _severity_from_magnitude(mag: float) -> int:
    """Visual-magnitude buckets. Lower = brighter.
    <=-3 -> 4 (very bright); -3..-1 -> 3 (naked-eye); -1..2 -> 2 (binoculars);
    >2 -> 1 (telescope-grade; rarely fires for sunlit passes)."""
    if mag <= -3.0:
        return 4
    if mag <= -1.0:
        return 3
    if mag <= 2.0:
        return 2
    return 1


class Observer(BaseModel):
    """Fixed observer location for n2yo's pre-computed pass queries."""

    name: str
    slug: str
    state: str
    lat: float
    lon: float
    elev_m: float = 0.0


class N2yoVisualpassesSettings(BaseModel):
    """Default 6 observers x 6 sats x 24 polls/day = 864 transactions/day,
    under n2yo's free 1000/day cap. Operator can extend either list if
    they upgrade quota. api_key_alias defaults to "n2yo"."""

    observers: list[Observer] = [
        Observer(name="Filer", slug="filer", state="ID",
                 lat=42.57, lon=-114.60, elev_m=1200.0),
        Observer(name="Boise", slug="boise", state="ID",
                 lat=43.62, lon=-116.20, elev_m=825.0),
        Observer(name="Idaho Falls", slug="idaho-falls", state="ID",
                 lat=43.49, lon=-112.04, elev_m=1438.0),
        Observer(name="Ogden", slug="ogden", state="UT",
                 lat=41.22, lon=-111.97, elev_m=1330.0),
        Observer(name="Salt Lake City", slug="salt-lake-city", state="UT",
                 lat=40.76, lon=-111.89, elev_m=1290.0),
        Observer(name="Provo", slug="provo", state="UT",
                 lat=40.23, lon=-111.66, elev_m=1387.0),
    ]
    norad_ids: list[int] = [25544, 25338, 28654, 33591, 27607, 43017]
    days_ahead: int = 2
    min_visibility_seconds: int = 300
    api_key_alias: str = "n2yo"


class N2yoVisualpassesAdapter(SourceAdapter):
    """Server-side visible-pass alerts via n2yo's visualpasses endpoint."""

    name = "n2yo_visualpasses"
    display_name = "n2yo Visible Passes"
    description = (
        "Pre-computed visible-pass alerts from n2yo.com -- sun illumination "
        "and visual magnitude are server-side data that complement "
        "satpass_predict's local SGP4 propagation. Requires a free n2yo API "
        "key (configured via /api-keys). One Event per (observer, satellite, "
        "AOS) tuple within a 2-day horizon, severity bucketed by visual "
        "magnitude."
    )
    settings_schema = N2yoVisualpassesSettings
    requires_api_key = "n2yo"
    api_key_field = "api_key_alias"
    wizard_order = None  # Ships disabled; operator enables after adding key
    default_cadence_s = 3600  # 1h
    data_class = "event"
    enrichment_locations = []

    def __init__(
        self,
        config: AdapterConfig,
        config_store: ConfigStore,
        cursor_db_path: Path,
    ) -> None:
        self._config_store = config_store
        self._cursor_db_path = cursor_db_path
        self._db: sqlite3.Connection | None = None
        self._session: aiohttp.ClientSession | None = None
        self._api_key: str | None = None
        self._apply_settings(config.settings or {})

    def _apply_settings(self, settings: dict[str, Any]) -> None:
        raw_obs = settings.get("observers") or []
        self._observers: list[Observer] = [
            o if isinstance(o, Observer) else Observer(**o) for o in raw_obs
        ]
        self._norad_ids: list[int] = [int(n) for n in (settings.get("norad_ids") or [])]
        self._days_ahead: int = int(settings.get("days_ahead") or 2)
        self._min_vis_s: int = int(settings.get("min_visibility_seconds") or 300)
        self._api_key_alias: str = settings.get("api_key_alias") or "n2yo"

    def _redact(self, text: str) -> str:
        """Strip the live key from log strings before they hit journald."""
        return text.replace(self._api_key, "<KEY>") if self._api_key else text

    async def startup(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_FETCH_TIMEOUT_S),
            headers={"User-Agent": "Central/0.12 (+n2yo_visualpasses)"},
        )
        self._db = sqlite3.connect(self._cursor_db_path)
        self._db.execute(_DEDUP_DDL)
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS published_ids_last_seen ON published_ids (last_seen)"
        )
        self._db.commit()
        self._api_key = await self._config_store.get_api_key(self._api_key_alias)
        logger.info(
            "n2yo_visualpasses adapter started",
            extra={
                "observers": [o.slug for o in self._observers],
                "norad_ids": self._norad_ids,
                "days_ahead": self._days_ahead,
                "min_visibility_seconds": self._min_vis_s,
                "api_key_present": bool(self._api_key),
            },
        )

    async def shutdown(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
        if self._db:
            self._db.close()
            self._db = None

    async def apply_config(self, new_config: AdapterConfig) -> None:
        self._apply_settings(new_config.settings or {})
        self._api_key = await self._config_store.get_api_key(self._api_key_alias)
        logger.info(
            "n2yo_visualpasses config updated",
            extra={
                "observers": [o.slug for o in self._observers],
                "norad_ids": self._norad_ids,
                "api_key_present": bool(self._api_key),
            },
        )

    async def _fetch_passes(self, observer: Observer, norad_id: int) -> dict[str, Any] | None:
        """One n2yo API call. Returns parsed JSON or None on failure (live key
        scrubbed from log; caller skips this pair, one failure must not kill the poll)."""
        assert self._session is not None
        url = _VISUALPASSES_URL.format(
            norad_id=norad_id,
            lat=observer.lat, lng=observer.lon, alt=observer.elev_m,
            days=self._days_ahead, min_vis_s=self._min_vis_s,
            key=self._api_key,
        )
        try:
            async with self._session.get(url) as resp:
                if resp.status != 200:
                    logger.warning(
                        "n2yo_visualpasses HTTP non-200",
                        extra={"observer": observer.slug, "norad_id": norad_id,
                               "status": resp.status},
                    )
                    return None
                return await resp.json()
        except (aiohttp.ClientError, TimeoutError) as exc:
            logger.warning(
                "n2yo_visualpasses fetch failed",
                extra={"observer": observer.slug, "norad_id": norad_id,
                       "error": self._redact(str(exc))},
            )
            return None

    def _pass_to_event(
        self, p: dict[str, Any], info: dict[str, Any], observer: Observer,
    ) -> Event:
        # All UTC fields from n2yo are Unix timestamps.
        aos = datetime.fromtimestamp(p["startUTC"], tz=timezone.utc)
        peak = datetime.fromtimestamp(p["maxUTC"], tz=timezone.utc)
        los = datetime.fromtimestamp(p["endUTC"], tz=timezone.utc)
        mag = float(p["mag"])
        return Event(
            id=f"{observer.slug}:{info['satid']}:{aos.isoformat()}",
            adapter=self.name,
            category="pass.n2yo_visualpasses",
            time=peak,
            severity=_severity_from_magnitude(mag),
            geo=Geo(
                centroid=(observer.lon, observer.lat),
                regions=[f"US-{observer.state}"],
                primary_region=f"US-{observer.state}",
            ),
            data={
                "observer_name": observer.name,
                "observer_slug": observer.slug,
                "observer_state": observer.state,
                "norad_id": int(info["satid"]),
                "satellite_name": info["satname"],
                "aos_time": aos.isoformat(),
                "peak_time": peak.isoformat(),
                "los_time": los.isoformat(),
                "max_elevation_deg": round(float(p["maxEl"]), 2),
                "magnitude": round(mag, 2),
                "azimuth_at_aos": round(float(p["startAz"]), 1),
                "azimuth_at_aos_compass": p.get("startAzCompass"),
                "azimuth_at_peak": round(float(p["maxAz"]), 1),
                "azimuth_at_peak_compass": p.get("maxAzCompass"),
                "azimuth_at_los": round(float(p["endAz"]), 1),
                "azimuth_at_los_compass": p.get("endAzCompass"),
                "duration_s": int(p.get("duration") or 0),
            },
        )

    async def poll(self) -> AsyncIterator[Event]:
        if not self._session:
            raise RuntimeError("Session not initialized")
        if not self._api_key:
            logger.info(
                "n2yo_visualpasses: no API key for alias; skipping poll",
                extra={"alias": self._api_key_alias},
            )
            return
        if not self._observers or not self._norad_ids:
            logger.info(
                "n2yo_visualpasses: empty observers or norad_ids; nothing to poll",
                extra={"observers": len(self._observers),
                       "norad_ids": len(self._norad_ids)},
            )
            return

        sem = asyncio.Semaphore(_FETCH_CONCURRENCY)

        async def _one(obs: Observer, nid: int) -> tuple[
            Observer, dict[str, Any] | None,
        ]:
            async with sem:
                return obs, await self._fetch_passes(obs, nid)

        tasks = [
            _one(obs, nid) for obs in self._observers for nid in self._norad_ids
        ]
        results = await asyncio.gather(*tasks)

        yielded = 0
        transactions_total = 0
        passes_total = 0
        failures = 0
        for obs, payload in results:
            if payload is None:
                failures += 1
                continue
            info = payload.get("info") or {}
            passes = payload.get("passes") or []
            transactions_total += int(info.get("transactionscount") or 0)
            passes_total += len(passes)
            for p in passes:
                try:
                    yield self._pass_to_event(p, info, obs)
                    yielded += 1
                except Exception:
                    logger.exception(
                        "n2yo_visualpasses event-build failed",
                        extra={"observer": obs.slug,
                               "norad_id": info.get("satid")},
                    )

        self.sweep_old_ids()
        logger.info(
            "n2yo_visualpasses poll completed",
            extra={
                "observers": [o.slug for o in self._observers],
                "norad_ids": self._norad_ids,
                "transactions_used_this_call": transactions_total,
                "passes_returned": passes_total,
                "events_yielded": yielded,
                "fetch_failures": failures,
            },
        )

    def subject_for(self, event: Event) -> str:
        state = (event.data.get("observer_state") or "").lower() or "unknown"
        slug = event.data.get("observer_slug") or "unknown"
        return f"central.sat.pass.us.{state}.{slug}"
