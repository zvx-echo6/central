"""TomTom Orbis vector flow-tile polling adapter (telemetry).

Polls a configured set of slippy tiles (Idaho metros at z=10), fetches each
Orbis vector flow tile, decodes per-segment relative_speed + geometry via
``tomtom_flow_parse``, and emits one telemetry Event per road segment per poll
to CENTRAL_TRAFFIC_FLOW (subject ``central.traffic_flow.{z}.{x}.{y}``). The
v0.9.4 on-demand passthrough route reuses the same decode helper.

Dedup is inherited from SourceAdapter; ids are minute-bucketed so an adapter
poll and a passthrough fetch of the same tile in the same minute don't double
store (TomTom advertises a 60s tile TTL).
"""

import asyncio
import logging
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path

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
from central.models import Event
from central.tomtom_flow_parse import decode_flow_tile

logger = logging.getLogger(__name__)

_ORBIS_FLOW_URL = "https://api.tomtom.com/maps/orbis/traffic/tile/flow/{z}/{x}/{y}.pbf?key={key}&apiVersion=1"
_FETCH_CONCURRENCY = 4
_FETCH_TIMEOUT_S = 30

_DEDUP_DDL = (
    "CREATE TABLE IF NOT EXISTS published_ids ("
    "adapter TEXT NOT NULL, event_id TEXT NOT NULL, "
    "first_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "PRIMARY KEY (adapter, event_id))"
)


class TileCoord(BaseModel):
    z: int
    x: int
    y: int


class TomTomFlowSettings(BaseModel):
    """tiles: slippy coordinates to poll. api_key_alias: config.api_keys entry."""

    tiles: list[TileCoord] = []
    api_key_alias: str = "tomtom"


class TomTomFlowAdapter(SourceAdapter):
    """TomTom Orbis vector flow-tile telemetry adapter."""

    name = "tomtom_flow"
    display_name = "TomTom Traffic Flow"
    description = (
        "Per-road-segment traffic speed (relative_speed) from TomTom Orbis vector "
        "flow tiles, for a configured tile coverage set. Telemetry; renders as "
        "colored polylines on the map."
    )
    settings_schema = TomTomFlowSettings
    requires_api_key = "tomtom"
    api_key_field = "api_key_alias"
    wizard_order = None  # Ships disabled
    default_cadence_s = 300
    data_class = "telemetry"
    enrichment_locations = []  # segments carry their own geometry; no point geocode

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
        self._tiles: list[TileCoord] = self._read_tiles(config)
        self._api_key_alias: str = config.settings.get("api_key_alias", "tomtom")
        self._api_key: str | None = None

    @staticmethod
    def _read_tiles(config: AdapterConfig) -> list[TileCoord]:
        return [TileCoord(**t) for t in (config.settings.get("tiles") or [])]

    def _redact(self, text: str) -> str:
        return text.replace(self._api_key, "<KEY>") if self._api_key else text

    async def startup(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_FETCH_TIMEOUT_S),
            headers={"User-Agent": "Central/0.9 (+tomtom_flow)"},
        )
        self._db = sqlite3.connect(self._cursor_db_path)
        self._db.execute(_DEDUP_DDL)
        self._db.execute("CREATE INDEX IF NOT EXISTS published_ids_last_seen ON published_ids (last_seen)")
        self._db.commit()
        self._api_key = await self._config_store.get_api_key(self._api_key_alias)
        logger.info("tomtom_flow adapter started",
                    extra={"tiles": len(self._tiles), "api_key_present": bool(self._api_key)})

    async def shutdown(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
        if self._db:
            self._db.close()
            self._db = None

    async def apply_config(self, new_config: AdapterConfig) -> None:
        self._tiles = self._read_tiles(new_config)
        self._api_key_alias = new_config.settings.get("api_key_alias", "tomtom")
        self._api_key = await self._config_store.get_api_key(self._api_key_alias)
        logger.info("tomtom_flow config updated",
                    extra={"tiles": len(self._tiles), "api_key_present": bool(self._api_key)})

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=30),
        retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError)),
    )
    async def _fetch_tile(self, z: int, x: int, y: int) -> bytes:
        assert self._session is not None
        url = _ORBIS_FLOW_URL.format(z=z, x=x, y=y, key=self._api_key)
        async with self._session.get(url) as resp:
            resp.raise_for_status()
            return await resp.read()

    async def poll(self) -> AsyncIterator[Event]:
        if not self._session:
            raise RuntimeError("Session not initialized")
        if not self._api_key:
            logger.warning("tomtom_flow: no API key for alias; skipping poll",
                           extra={"alias": self._api_key_alias})
            return
        now = datetime.now(timezone.utc)
        sem = asyncio.Semaphore(_FETCH_CONCURRENCY)

        async def _one(tile: TileCoord) -> list[Event]:
            async with sem:
                try:
                    pbf = await self._fetch_tile(tile.z, tile.x, tile.y)
                except (aiohttp.ClientError, TimeoutError) as exc:
                    logger.warning("tomtom_flow tile fetch failed",
                                   extra={"tile": [tile.z, tile.x, tile.y], "error": self._redact(str(exc))})
                    return []
                try:
                    return decode_flow_tile(pbf, tile.z, tile.x, tile.y, now)
                except Exception:
                    logger.exception("tomtom_flow decode failed", extra={"tile": [tile.z, tile.x, tile.y]})
                    return []

        results = await asyncio.gather(*[_one(t) for t in self._tiles])
        yielded = 0
        for evs in results:
            for ev in evs:
                yield ev
                yielded += 1

        self.sweep_old_ids()
        logger.info("tomtom_flow poll completed", extra={"events_yielded": yielded, "tiles": len(self._tiles)})

    def subject_for(self, event: Event) -> str:
        d = event.data
        return f"central.traffic_flow.{d.get('tile_z')}.{d.get('tile_x')}.{d.get('tile_y')}"
