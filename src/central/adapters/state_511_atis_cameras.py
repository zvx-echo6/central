"""State 511 (Castle Rock ATIS) cameras adapter — telemetry.

Polls the full state camera directory via POST /List/GetData/Cameras (paginated,
100/page), emitting one telemetry Event per camera to CENTRAL_TRAFFIC_CAMERAS
(subject central.traffic_cameras.{state}.{camera_id}). data_class="telemetry" ->
the /telemetry tab. The detail drawer renders <img> straight from the upstream
image URL -- no blob storage or proxy in Central. Idaho-only; templatized per
state via settings, same shape as state_511_atis.

Dedup is per-UTC-day ({state}:cam:{id}:{YYYY-MM-DD}): one event per camera per day
-- the table always shows today's cameras, with no dedup-window/retention
coordination and no per-poll flooding. Inherits the v0.9.1 dedup mixin.
"""

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

_PAGE_LENGTH = 100  # Castle Rock caps the DataTables page at 100 rows
_MAX_PAGES = 20  # defensive ceiling (~2,000 cameras)
_XHR = {"X-Requested-With": "XMLHttpRequest"}
_FETCH_CONCURRENCY = 4
_FETCH_TIMEOUT_S = 30
_WKT_POINT = re.compile(r"POINT \(([-0-9.]+) ([-0-9.]+)\)")

_DEDUP_DDL = (
    "CREATE TABLE IF NOT EXISTS published_ids ("
    "adapter TEXT NOT NULL, event_id TEXT NOT NULL, "
    "first_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "PRIMARY KEY (adapter, event_id))"
)


def _parse_wkt(wkt: str | None) -> tuple[float | None, float | None]:
    """'POINT (lon lat)' -> (lat, lon); (None, None) on failure."""
    if not wkt:
        return (None, None)
    m = _WKT_POINT.match(wkt)
    if not m:
        return (None, None)
    try:
        return (float(m.group(2)), float(m.group(1)))
    except ValueError:
        return (None, None)


class StateConfig(BaseModel):
    code: str
    base_url: str


class State511CamerasSettings(BaseModel):
    """states: Castle Rock 511 deployments to poll for cameras."""

    states: list[StateConfig] = []


class State511ATISCamerasAdapter(SourceAdapter):
    """Castle Rock ATIS 511 camera directory adapter (telemetry)."""

    name = "state_511_atis_cameras"
    display_name = "State 511 Cameras (Castle Rock ATIS)"
    description = (
        "State DOT 511 traffic cameras from the Castle Rock ATIS platform. One "
        "telemetry event per camera (per UTC day); the detail drawer shows the live "
        "image direct from the source. Verified for Idaho."
    )
    settings_schema = State511CamerasSettings
    requires_api_key = None
    api_key_field = None
    wizard_order = None  # Ships disabled
    default_cadence_s = 600
    data_class = "telemetry"
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
        self._states: list[StateConfig] = self._read_states(config)

    @staticmethod
    def _read_states(config: AdapterConfig) -> list[StateConfig]:
        return [StateConfig(**s) for s in (config.settings.get("states") or [])]

    async def startup(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_FETCH_TIMEOUT_S),
            headers={"User-Agent": "Central/0.9 (+state_511_atis_cameras)"},
        )
        self._db = sqlite3.connect(self._cursor_db_path)
        self._db.execute(_DEDUP_DDL)
        self._db.execute("CREATE INDEX IF NOT EXISTS published_ids_last_seen ON published_ids (last_seen)")
        self._db.commit()
        logger.info("state_511_atis_cameras adapter started",
                    extra={"states": [s.code for s in self._states]})

    async def shutdown(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
        if self._db:
            self._db.close()
            self._db = None

    async def apply_config(self, new_config: AdapterConfig) -> None:
        self._states = self._read_states(new_config)
        logger.info("state_511_atis_cameras config updated",
                    extra={"states": [s.code for s in self._states]})

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=30),
        retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError)),
    )
    async def _fetch_page(self, base_url: str, start: int) -> dict[str, Any]:
        assert self._session is not None
        body = {
            "draw": "1", "start": str(start), "length": str(_PAGE_LENGTH),
            "columns[0][data]": "0", "order[0][column]": "0",
            "order[0][dir]": "asc", "search[value]": "",
        }
        async with self._session.post(
            f"{base_url}/List/GetData/Cameras", data=body, headers=_XHR
        ) as resp:
            resp.raise_for_status()
            return await resp.json(content_type=None)

    async def _fetch_all(self, base_url: str) -> list[dict[str, Any]]:
        """Paginate the camera directory (100/page) -> all rows. [] on failure."""
        try:
            first = await self._fetch_page(base_url, 0)
        except (aiohttp.ClientError, TimeoutError, asyncio.TimeoutError, ValueError) as exc:
            logger.warning("state_511_atis_cameras page fetch failed",
                           extra={"base_url": base_url, "start": 0, "error": str(exc)})
            return []
        total = first.get("recordsTotal") or 0
        rows = list(first.get("data") or [])
        start = _PAGE_LENGTH
        pages = 1
        while start < total and pages < _MAX_PAGES:
            try:
                page = await self._fetch_page(base_url, start)
            except (aiohttp.ClientError, TimeoutError, asyncio.TimeoutError, ValueError) as exc:
                logger.warning("state_511_atis_cameras page fetch failed",
                               extra={"base_url": base_url, "start": start, "error": str(exc)})
                break
            rows.extend(page.get("data") or [])
            start += _PAGE_LENGTH
            pages += 1
        return rows

    def _build_event(self, cam: dict[str, Any], state: StateConfig) -> Event | None:
        cam_id = cam.get("id")
        if cam_id is None:
            return None
        lat, lon = _parse_wkt((cam.get("latLng") or {}).get("geography", {}).get("wellKnownText"))
        images = cam.get("images") or []
        image_url = None
        if images and images[0].get("imageUrl"):
            image_url = state.base_url + images[0]["imageUrl"]
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return Event(
            id=f"{state.code}:cam:{cam_id}:{day}",
            adapter=self.name,
            category="camera.state_511_atis_cameras",
            time=datetime.now(timezone.utc),
            severity=1,  # telemetry; cameras have no severity signal
            geo=Geo(
                centroid=(lon, lat) if lat is not None and lon is not None else None,
                regions=[f"US-{state.code}"],
                primary_region=f"US-{state.code}",
            ),
            data={
                "camera_id": cam_id,
                "roadway_name": cam.get("roadway"),
                "location_description": cam.get("location"),
                "direction": cam.get("direction"),
                "source": cam.get("source"),
                "image_url": image_url,
                "image_count": len(images),
                "record_updated": cam.get("lastUpdated"),  # camera config edit time (not capture time)
                "state_code": state.code,
                "latitude": lat,
                "longitude": lon,
            },
        )

    async def poll(self) -> AsyncIterator[Event]:
        if not self._session:
            raise RuntimeError("Session not initialized")
        sem = asyncio.Semaphore(_FETCH_CONCURRENCY)

        async def _one(state: StateConfig) -> list[Event]:
            async with sem:
                cams = await self._fetch_all(state.base_url)
                out: list[Event] = []
                for cam in cams:
                    try:
                        ev = self._build_event(cam, state)
                    except Exception:
                        logger.exception("state_511_atis_cameras parse failed", extra={"state": state.code})
                        continue
                    if ev is not None:
                        out.append(ev)
                return out

        results = await asyncio.gather(*[_one(s) for s in self._states])
        yielded = 0
        for evs in results:
            for ev in evs:
                yield ev
                yielded += 1

        self.sweep_old_ids()
        logger.info("state_511_atis_cameras poll completed", extra={"events_yielded": yielded})

    def subject_for(self, event: Event) -> str:
        d = event.data
        code = (d.get("state_code") or "").lower() or "unknown"
        return f"central.traffic_cameras.{code}.{d.get('camera_id')}"
