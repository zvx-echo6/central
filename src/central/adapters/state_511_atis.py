"""State 511 (Castle Rock ATIS) adapter — Idaho first.

Castle Rock's ATIS platform exposes two endpoints per layer that must be joined:
  - GET  /map/mapIcons/<Layer>      -> thin markers: {itemId, location:[lat,lon], ...}
  - POST /List/GetData/<Layer>      -> rich DataTables rows keyed by id==itemId
The marker feed has coordinates but no text; the List feed has road name /
description / county / severity but no coordinates. We join on id.

Layers map to traffic event_types (wzdx precedent — category drives the GUI
event_type via split_part, subject is central.traffic.<event_type>.<state>):
  Incidents -> incident, Closures -> closure, Construction -> work_zone.
Cameras are telemetry (data_class) and ship as a separate adapter later.

Templatized per state via settings {"states":[{"code","base_url"}]}; only Idaho
is verified (Oregon/Wyoming are not Castle Rock). Add states as settings rows
once each host's URL shape is confirmed. Dedup is inherited from SourceAdapter.
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
from central.config_models import AdapterConfig
from central.config_store import ConfigStore
from central.models import Event, Geo

logger = logging.getLogger(__name__)

# Castle Rock layer -> Central event_type (category = "<event_type>.state_511_atis").
LAYER_EVENT_TYPE: dict[str, str] = {
    "Incidents": "incident",
    "Closures": "closure",
    "Construction": "work_zone",
}

# DataTables server-side body. POST is required (GET returns an empty data array);
# length covers Idaho's largest layer today (~114) with headroom — warn if exceeded.
_LIST_PAGE_LENGTH = 1000
_LIST_BODY = {
    "draw": "1", "start": "0", "length": str(_LIST_PAGE_LENGTH),
    "columns[0][data]": "0", "order[0][column]": "0",
    "order[0][dir]": "asc", "search[value]": "",
}
_XHR = {"X-Requested-With": "XMLHttpRequest"}

_FETCH_CONCURRENCY = 4
_FETCH_TIMEOUT_S = 30

_DEDUP_DDL = (
    "CREATE TABLE IF NOT EXISTS published_ids ("
    "adapter TEXT NOT NULL, event_id TEXT NOT NULL, "
    "first_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "PRIMARY KEY (adapter, event_id))"
)


def _parse_us_dt(value: str | None) -> datetime | None:
    """Parse Castle Rock's US-format local timestamp (e.g. "5/25/26, 2:32 PM").

    No timezone is supplied; treated as naive -> UTC (approximate — the freshness
    signal is last_updated ordering, not absolute TZ). Returns None on failure.
    """
    if not value:
        return None
    try:
        return datetime.strptime(value.strip(), "%m/%d/%y, %I:%M %p").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


class StateConfig(BaseModel):
    """One Castle Rock 511 deployment to poll."""

    code: str       # 2-letter state code, e.g. "ID"
    base_url: str   # e.g. "https://511.idaho.gov"


class State511ATISSettings(BaseModel):
    """states: verified Castle Rock deployments. Empty = nothing to poll."""

    states: list[StateConfig] = []


class State511ATISAdapter(SourceAdapter):
    """Castle Rock ATIS 511 adapter (incidents / closures / construction)."""

    name = "state_511_atis"
    display_name = "State 511 (Castle Rock ATIS)"
    description = (
        "State DOT 511 incidents, closures, and road work from the Castle Rock "
        "ATIS platform. Joins the map-marker and detail-list endpoints per layer. "
        "Verified for Idaho; add states as settings rows once each is confirmed."
    )
    settings_schema = State511ATISSettings
    requires_api_key = None
    api_key_field = None
    wizard_order = None  # Ships disabled
    default_cadence_s = 300
    data_class = "event"
    # Coords come from the marker join; geocoder fills city (county/state are upstream).
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
        raw = config.settings.get("states") or []
        return [StateConfig(**s) for s in raw]

    async def startup(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_FETCH_TIMEOUT_S),
            headers={"User-Agent": "Central/0.9 (+state_511_atis)"},
        )
        self._db = sqlite3.connect(self._cursor_db_path)
        self._db.execute(_DEDUP_DDL)
        self._db.execute("CREATE INDEX IF NOT EXISTS published_ids_last_seen ON published_ids (last_seen)")
        self._db.commit()
        logger.info("state_511_atis adapter started",
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
        logger.info("state_511_atis config updated",
                    extra={"states": [s.code for s in self._states]})

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=30),
        retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError)),
    )
    async def _fetch_markers(self, base_url: str, layer: str) -> dict[str, tuple[float, float]]:
        """GET /map/mapIcons/<Layer> -> {itemId: (lat, lon)}."""
        assert self._session is not None
        async with self._session.get(f"{base_url}/map/mapIcons/{layer}") as resp:
            resp.raise_for_status()
            doc = await resp.json(content_type=None)
        out: dict[str, tuple[float, float]] = {}
        for m in (doc.get("item2") or []):
            loc = m.get("location")
            if isinstance(loc, list) and len(loc) == 2 and m.get("itemId") is not None:
                out[str(m["itemId"])] = (float(loc[0]), float(loc[1]))
        return out

    async def _fetch_details(self, base_url: str, layer: str) -> list[dict[str, Any]]:
        """POST /List/GetData/<Layer> (DataTables) -> rich rows. [] on failure."""
        assert self._session is not None
        try:
            async with self._session.post(
                f"{base_url}/List/GetData/{layer}", data=_LIST_BODY, headers=_XHR
            ) as resp:
                resp.raise_for_status()
                doc = await resp.json(content_type=None)
        except (aiohttp.ClientError, TimeoutError, asyncio.TimeoutError, ValueError) as exc:
            logger.warning("state_511_atis detail fetch failed",
                           extra={"layer": layer, "base_url": base_url, "error": str(exc)})
            return []
        total = doc.get("recordsTotal") or 0
        rows = doc.get("data") or []
        if total > _LIST_PAGE_LENGTH:
            logger.warning("state_511_atis layer exceeds page length; add pagination",
                           extra={"layer": layer, "recordsTotal": total, "length": _LIST_PAGE_LENGTH})
        return rows

    def _build_event(
        self, detail: dict[str, Any], coords: tuple[float, float] | None,
        state_code: str, layer: str,
    ) -> Event | None:
        record_id = detail.get("id")
        if record_id is None:
            return None
        event_type = LAYER_EVENT_TYPE[layer]
        lat, lon = (coords if coords else (None, None))
        return Event(
            id=f"{state_code}:{layer}:{record_id}",
            adapter=self.name,
            category=f"{event_type}.state_511_atis",
            time=(_parse_us_dt(detail.get("lastUpdated"))
                  or _parse_us_dt(detail.get("startDate"))
                  or datetime.now(timezone.utc)),
            expires=_parse_us_dt(detail.get("endDate")),
            severity=(3 if detail.get("isFullClosure") else 1),
            geo=Geo(
                centroid=(lon, lat) if lat is not None and lon is not None else None,
                regions=[f"US-{state_code}"],
                primary_region=f"US-{state_code}",
            ),
            data={
                "roadway_name": detail.get("roadwayName"),
                "description": (detail.get("description") or "").strip() or None,
                "event_sub_type": detail.get("eventSubType"),
                "direction": detail.get("direction"),
                "location_description": detail.get("locationDescription"),
                "county": detail.get("county"),
                "state": detail.get("state"),
                "start_date": detail.get("startDate"),
                "last_updated": detail.get("lastUpdated"),
                "is_full_closure": detail.get("isFullClosure"),
                "layer": layer,
                "state_code": state_code,
                "latitude": lat,   # enrichment_locations pair (canonical)
                "longitude": lon,
            },
        )

    async def poll(self) -> AsyncIterator[Event]:
        if not self._session:
            raise RuntimeError("Session not initialized")
        sem = asyncio.Semaphore(_FETCH_CONCURRENCY)

        async def _layer(state: StateConfig, layer: str):
            async with sem:
                try:
                    markers = await self._fetch_markers(state.base_url, layer)
                except (aiohttp.ClientError, TimeoutError) as exc:
                    logger.warning("state_511_atis marker fetch failed",
                                   extra={"layer": layer, "state": state.code, "error": str(exc)})
                    markers = {}
                details = await self._fetch_details(state.base_url, layer)
                return state.code, layer, markers, details

        tasks = [_layer(s, layer) for s in self._states for layer in LAYER_EVENT_TYPE]
        yielded = 0
        for state_code, layer, markers, details in await asyncio.gather(*tasks):
            for detail in details:
                try:
                    coords = markers.get(str(detail.get("id")))
                    event = self._build_event(detail, coords, state_code, layer)
                except Exception:  # one bad record never sinks the poll
                    logger.exception("state_511_atis record parse failed",
                                     extra={"layer": layer, "state": state_code})
                    continue
                if event is None:
                    continue
                yield event
                yielded += 1

        self.sweep_old_ids()
        logger.info("state_511_atis poll completed", extra={"events_yielded": yielded})

    def subject_for(self, event: Event) -> str:
        d = event.data
        event_type = LAYER_EVENT_TYPE.get(d.get("layer"), "incident")
        code = (d.get("state_code") or "").lower() or "unknown"
        return f"central.traffic.{event_type}.{code}"
