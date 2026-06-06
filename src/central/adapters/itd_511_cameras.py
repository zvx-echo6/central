"""ITD 511 cameras adapter — telemetry.

Polls /api/v2/get/cameras (664 cameras in the v0.10.0 step-0 snapshot) and
emits one telemetry Event per camera per UTC day. ITD aggregates border-region
mirrors from neighbouring DOTs (UDOT, ODOT, WYDOT, WSDOT, NDot, MTD, DriveBC,
Lemhi County) plus its own ITDNET/ACHD/RWIS/Idaho511 feeds; ~1.2% of cameras
sit outside Idaho but ride the same feed.

Per the v0.10.0 finding-4 architectural call: region tagged uniformly US-ID;
``data.source_jurisdiction`` carries the raw upstream ``Source`` field so
border-region cameras remain distinguishable for downstream consumers (lat/lon
bounds-checking deferred to a later release).

Subject convention (v0.9.20 forward, locked v0.10.0 architectural call A):
  central.traffic_cameras.us.id.<camera_id>

Dedup id: ``idaho_511:cam:<Camera_Id>:<YYYY-MM-DD>`` — one telemetry event per
camera per UTC day. Image URLs ship straight through; the partial renders
<img src=...> from Views[0].Url (jpeg/gif/png all confirmed publicly reachable).

Retry predicate matches itd_511's (architectural call B): no retry on 4xx
except 429 with Retry-After. Shares the ``_Transient`` sentinel and
``_MAX_RETRY_AFTER_S`` cap from the sibling module.
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
    after_nothing,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
)

from central.adapter import SourceAdapter
from central.adapters.itd_511 import _MAX_RETRY_AFTER_S, _Transient, _wait_strategy
from central.config_models import AdapterConfig
from central.config_store import ConfigStore
from central.models import Event, Geo

logger = logging.getLogger(__name__)

_BASE_URL = "https://511.idaho.gov/api/v2/get"
_FETCH_TIMEOUT_S = 30

_DEDUP_DDL = (
    "CREATE TABLE IF NOT EXISTS published_ids ("
    "adapter TEXT NOT NULL, event_id TEXT NOT NULL, "
    "first_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "PRIMARY KEY (adapter, event_id))"
)

# Source values that are native ITD-internal feeds (not cross-DOT border-region
# mirrors). Drives the row-partial "(cross-DOT mirror)" annotation via the
# data.is_native_source boolean computed in _build_event — per
# feedback_no_hardcoding, the partial must not carry the allow-list itself.
NATIVE_SOURCES = frozenset({"ITDNET", "Idaho511", "ACHD", "RWIS"})


class Itd511CamerasSettings(BaseModel):
    """api_key_alias: config.api_keys entry (default 'idaho_511', shared with
    the itd_511 event adapter)."""

    api_key_alias: str = "idaho_511"


class Itd511CamerasAdapter(SourceAdapter):
    """Idaho 511 official cameras directory adapter (telemetry)."""

    name = "itd_511_cameras"
    display_name = "Idaho 511 Cameras (Official API)"
    description = (
        "Idaho Transportation Department's traffic camera directory (664 cameras "
        "statewide plus border-region cross-DOT mirrors). One telemetry event "
        "per camera per UTC day; the detail drawer streams the live image "
        "direct from the source."
    )
    settings_schema = Itd511CamerasSettings
    requires_api_key = "idaho_511"
    api_key_field = "api_key_alias"
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
        self._api_key_alias: str = config.settings.get("api_key_alias", "idaho_511")
        self._api_key: str | None = None

    def _redact(self, text: str) -> str:
        return text.replace(self._api_key, "<KEY>") if self._api_key else text

    async def startup(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_FETCH_TIMEOUT_S),
            headers={"User-Agent": "Central/0.10 (+itd_511_cameras)"},
        )
        self._db = sqlite3.connect(self._cursor_db_path)
        self._db.execute(_DEDUP_DDL)
        self._db.execute("CREATE INDEX IF NOT EXISTS published_ids_last_seen ON published_ids (last_seen)")
        self._db.commit()
        self._api_key = await self._config_store.get_api_key(self._api_key_alias)
        logger.info("itd_511_cameras adapter started",
                    extra={"alias": self._api_key_alias, "api_key_present": bool(self._api_key)})

    async def shutdown(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
        if self._db:
            self._db.close()
            self._db = None

    async def apply_config(self, new_config: AdapterConfig) -> None:
        self._api_key_alias = new_config.settings.get("api_key_alias", "idaho_511")
        self._api_key = await self._config_store.get_api_key(self._api_key_alias)
        logger.info("itd_511_cameras config updated",
                    extra={"alias": self._api_key_alias, "api_key_present": bool(self._api_key)})

    @retry(
        stop=stop_after_attempt(3),
        wait=_wait_strategy,
        retry=retry_if_exception_type((_Transient, aiohttp.ClientConnectionError,
                                       asyncio.TimeoutError, TimeoutError)),
        reraise=True,         # propagate _Transient/ClientError verbatim, not RetryError
        before_sleep=None,    # explicit per BUG D5 audit: tenacity must not log between attempts
        after=after_nothing,  # explicit per BUG D5 audit: tenacity must not log after attempts
    )
    async def _fetch_cameras(self) -> list[dict[str, Any]] | None:
        """GET /api/v2/get/cameras?key=<KEY>. Same retry policy as itd_511
        (the _wait_strategy + _Transient(wait_s=...) shape avoids the BUG C
        double-wait on Retry-After)."""
        if self._session is None:
            raise RuntimeError("itd_511_cameras session not started")
        async with self._session.get(
            f"{_BASE_URL}/cameras", params={"key": self._api_key},
        ) as resp:
            if resp.status == 200:
                return await resp.json(content_type=None)
            body_preview = self._redact((await resp.text())[:200])
            if resp.status == 429:
                ra = resp.headers.get("Retry-After", "")
                wait_s = min(int(ra) if ra.isdigit() else 5, _MAX_RETRY_AFTER_S)
                logger.warning("itd_511_cameras rate-limited",
                               extra={"retry_after": wait_s, "body": body_preview})
                raise _Transient(f"429 retry-after={wait_s}", wait_s=wait_s)
            if 500 <= resp.status < 600:
                logger.warning("itd_511_cameras upstream 5xx",
                               extra={"status": resp.status, "body": body_preview})
                raise _Transient(f"{resp.status} server error")
            logger.warning("itd_511_cameras permanent error; skipping poll",
                           extra={"status": resp.status, "body": body_preview})
            return None

    def _build_event(self, cam: dict[str, Any]) -> Event | None:
        cam_id = cam.get("Id")
        if cam_id is None:
            return None
        views = cam.get("Views") or []
        image_url = views[0].get("Url") if views else None
        lat, lon = cam.get("Latitude"), cam.get("Longitude")
        now = datetime.now(timezone.utc)
        day = now.strftime("%Y-%m-%d")
        source = cam.get("Source")
        return Event(
            id=f"idaho_511:cam:{cam_id}:{day}",
            adapter=self.name,
            category="camera.itd_511_cameras",
            time=now,
            severity=1,  # telemetry; cameras carry no severity signal
            geo=Geo(
                centroid=((lon, lat) if lat is not None and lon is not None else None),
                regions=["US-ID"],
                primary_region="US-ID",
            ),
            data={
                "camera_id": cam_id,
                "roadway": cam.get("Roadway"),
                "direction": cam.get("Direction"),
                "location": cam.get("Location"),
                "source": source,
                "source_jurisdiction": source,  # alias per v0.10.0 finding 4
                # Drives the row-partial "(cross-DOT mirror)" annotation
                # without hardcoding the allow-list at the template layer
                # (BUG D2 / [[feedback_no_hardcoding]]).
                "is_native_source": source in NATIVE_SOURCES,
                "source_id_upstream": cam.get("SourceId"),
                "image_url": image_url,
                "additional_views": [v.get("Url") for v in views[1:]],
                "view_count": len(views),
                "view_descriptions": [v.get("Description") for v in views],
                "sort_order": cam.get("SortOrder"),
                "latitude": lat,
                "longitude": lon,
            },
        )

    async def poll(self) -> AsyncIterator[Event]:
        if not self._session:
            raise RuntimeError("Session not initialized")
        if not self._api_key:
            logger.warning("itd_511_cameras: no API key for alias; skipping poll",
                           extra={"alias": self._api_key_alias})
            return
        try:
            cams = await self._fetch_cameras()
        except (_Transient, aiohttp.ClientError, TimeoutError) as exc:
            # BUG E fix: _Transient must be caught alongside ClientError —
            # else tenacity's reraise of a persistent 5xx/429 (after exhausted
            # retries) crashes the whole poll cycle.
            logger.warning("itd_511_cameras fetch failed",
                           extra={"error": self._redact(str(exc))})
            return
        yielded = 0
        for cam in (cams or []):
            try:
                ev = self._build_event(cam)
            except Exception:
                logger.exception("itd_511_cameras parse failed",
                                 extra={"id": cam.get("Id")})
                continue
            if ev is not None:
                yield ev
                yielded += 1
        self.sweep_old_ids()
        logger.info("itd_511_cameras poll completed",
                    extra={"cameras_yielded": yielded, "cameras_raw": len(cams or [])})

    def subject_for(self, event: Event) -> str:
        return f"central.traffic_cameras.us.id.{event.data.get('camera_id')}"
