"""ITD 511 traffic adapter — Idaho DOT official REST API (events + advisories).

Polls /api/v2/get/event (60s) and /api/v2/get/alerts (300s = every 5th poll)
from https://511.idaho.gov per v0.10.0. Sibling adapter ``itd_511_cameras``
(data_class="telemetry") handles /get/cameras. Both ship to existing streams
(CENTRAL_TRAFFIC / CENTRAL_TRAFFIC_CAMERAS) — no archive restart needed.

EncodedPolyline (Google polyline format) decodes to a LineString via the
``polyline`` PyPI dep; falls back to (Latitude, LatitudeSecondary) bookend
LineString or a single Point when geometry is sparse. RecurrenceSchedules,
Restrictions, and DetourPolyline pass through unmodified for downstream
consumers (truckers, route planners).

ITD Severity is a string ("Major"/"Minor"/"None") not 0-3 as the v0.10.0 spec
initially assumed; mapping is None->1, Minor->2, Major->3 with IsFullClosure
=true forcing 3 regardless (Severity and full-closure are orthogonal upstream
signals — 15 of 152 "None"-severity events are full closures in the live data).

Subject convention (v0.9.20 forward, locked v0.10.0 architectural call A):
  central.traffic.<event_type>.us.id

where event_type ∈ {incident, closure, work_zone, special_event, advisory}.

Dedup id: ``idaho_511:event:<SourceId>`` (SourceId is upstream-allocated, more
stable across ITD-side restamps than the internal ID; falls back to ID if
SourceId is null, never happened in the v0.10.0 step-0 snapshot).

Retry predicate tightened from the Castle Rock/TomTom precedent (architectural
call B): 5xx + connection/timeout retry; 4xx other than 429 skip the poll with
a warn (don't burn quota on permanent errors like a bad key); 429 honors
Retry-After once then retries.

API key (alias 'idaho_511') travels in the ?key=<KEY> query string — aiohttp's
default error messages leak the URL, so every error log path runs through
``self._redact()``.
"""

import asyncio
import logging
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
import polyline as polyline_lib
from pydantic import BaseModel
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
from central.enrichment import mile_marker
from central.models import Event, Geo

logger = logging.getLogger(__name__)

_BASE_URL = "https://511.idaho.gov/api/v2/get"
_FETCH_TIMEOUT_S = 30
_MAX_RETRY_AFTER_S = 60  # cap on Retry-After honor; longer => skip the cycle
_ADVISORY_EVERY_N_POLLS = 5  # 60s * 5 = 300s effective cadence

_DEDUP_DDL = (
    "CREATE TABLE IF NOT EXISTS published_ids ("
    "adapter TEXT NOT NULL, event_id TEXT NOT NULL, "
    "first_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "PRIMARY KEY (adapter, event_id))"
)

# ITD EventType -> Central event_type prefix (category = "<prefix>.itd_511",
# subject = "central.traffic.<prefix>.us.id"). Advisories use "advisory".
EVENT_TYPE_MAP = {
    "roadwork": "work_zone",
    "closures": "closure",
    "accidentsAndIncidents": "incident",
    "specialEvents": "special_event",
}

# ITD Severity (string) -> Central 1-4. IsFullClosure=true forces 3 regardless
# (orthogonal upstream signals; 15/152 "None"-severity events are full closures).
_SEVERITY_MAP = {"None": 1, "Minor": 2, "Major": 3}


class _Transient(Exception):
    """Internal: retryable. 5xx or 429 (with optional Retry-After honor).

    ``wait_s`` carries an explicit wait override the retry strategy reads,
    so a documented Retry-After value drives the next attempt's delay
    directly. Fixes the BUG C double-wait: the previous shape did an inline
    ``await asyncio.sleep(wait_s)`` AND then let tenacity's exponential
    jitter wait again on top, blocking ~120s+ per cycle for a single 429
    with Retry-After=60.
    """

    def __init__(self, msg: str, *, wait_s: int | None = None) -> None:
        super().__init__(msg)
        self.wait_s = wait_s


_EXP_WAIT = wait_exponential_jitter(initial=1, max=30)


def _wait_strategy(retry_state: Any) -> float:
    """Tenacity wait callable. Honors ``_Transient.wait_s`` when set (429
    Retry-After path); else falls through to exponential jitter. Shared
    across itd_511 and itd_511_cameras via direct import."""
    if retry_state.outcome is not None:
        exc = retry_state.outcome.exception()
        if isinstance(exc, _Transient) and exc.wait_s is not None:
            return float(exc.wait_s)
    return float(_EXP_WAIT(retry_state))


def _parse_epoch(value: Any) -> datetime | None:
    """Unix epoch seconds -> UTC datetime; None on missing/non-numeric/out-of-range."""
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except (ValueError, OSError, TypeError):
        return None


def _strip_or_none(value: Any) -> Any:
    """Strip a string (handles EventSubType trailing-space artifact); '' -> None.
    Non-strings pass through unchanged so the call site can stay terse."""
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    return stripped or None


def _itd_severity(itd_sev: str | None, is_full_closure: bool) -> int:
    """ITD Severity + IsFullClosure -> Central 1-4 (None->1, Minor->2, Major->3,
    full-closure overrides to 3 regardless)."""
    if is_full_closure:
        return 3
    return _SEVERITY_MAP.get(itd_sev or "", 1)


def _decode_polyline(encoded: str | None) -> list[tuple[float, float]]:
    """Decode a Google polyline -> list of (lat, lon). [] on null/empty/invalid."""
    if not encoded:
        return []
    try:
        return polyline_lib.decode(encoded)
    except Exception:  # the lib raises a grab-bag of unicode/index errors on bad input
        return []


def _build_geometry(
    lat: float | None, lon: float | None,
    lat2: float | None, lon2: float | None,
    encoded: str | None,
) -> tuple[dict[str, Any] | None, tuple[float, float] | None]:
    """Pick the best geometry available -> (GeoJSON geometry, centroid lon/lat).

    Priority: decoded EncodedPolyline LineString -> bookend LineString
    (primary + secondary lat/lon) -> single Point. Centroid is the first vertex.
    """
    decoded = _decode_polyline(encoded)
    if len(decoded) >= 2:
        coords = [(lon_, lat_) for lat_, lon_ in decoded]
        return {"type": "LineString", "coordinates": coords}, coords[0]
    if lat is not None and lon is not None:
        if lat2 is not None and lon2 is not None:
            return ({"type": "LineString", "coordinates": [(lon, lat), (lon2, lat2)]},
                    (lon, lat))
        return {"type": "Point", "coordinates": (lon, lat)}, (lon, lat)
    return None, None


class Itd511Settings(BaseModel):
    """api_key_alias: config.api_keys entry (default 'idaho_511', shared with
    itd_511_cameras). ITD is Idaho-only — no bbox/state list; region tagged
    uniformly US-ID."""

    api_key_alias: str = "idaho_511"


class Itd511Adapter(SourceAdapter):
    """Idaho 511 official API — events + advisories."""

    name = "itd_511"
    display_name = "Idaho 511 (Official API)"
    description = (
        "Idaho Transportation Department's official 511 REST API. Polls roadwork, "
        "closures, incidents, special events, and advisories statewide. Geometry "
        "via decoded EncodedPolyline; ships to CENTRAL_TRAFFIC."
    )
    settings_schema = Itd511Settings
    requires_api_key = "idaho_511"
    api_key_field = "api_key_alias"
    wizard_order = None  # Ships disabled
    default_cadence_s = 60
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
        self._api_key_alias: str = config.settings.get("api_key_alias", "idaho_511")
        self._api_key: str | None = None
        self._adv_counter = 0  # advisories polled on counter == 0

    def _redact(self, text: str) -> str:
        return text.replace(self._api_key, "<KEY>") if self._api_key else text

    async def startup(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_FETCH_TIMEOUT_S),
            headers={"User-Agent": "Central/0.10 (+itd_511)"},
        )
        self._db = sqlite3.connect(self._cursor_db_path)
        self._db.execute(_DEDUP_DDL)
        self._db.execute("CREATE INDEX IF NOT EXISTS published_ids_last_seen ON published_ids (last_seen)")
        self._db.commit()
        self._api_key = await self._config_store.get_api_key(self._api_key_alias)
        logger.info("itd_511 adapter started",
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
        logger.info("itd_511 config updated",
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
    async def _fetch(self, endpoint: str) -> list[dict[str, Any]] | None:
        """GET /api/v2/get/<endpoint>?key=<KEY>. Returns the list, or None on a
        permanent 4xx (skip cycle, don't retry). Raises _Transient on 5xx/429
        for tenacity to retry; the 429 path attaches the Retry-After value via
        ``_Transient(wait_s=...)`` so ``_wait_strategy`` honors it directly
        (no in-handler sleep — fixes BUG C double-wait). Key is redacted in
        every error log."""
        if self._session is None:
            raise RuntimeError("itd_511 session not started")
        async with self._session.get(
            f"{_BASE_URL}/{endpoint}", params={"key": self._api_key},
        ) as resp:
            if resp.status == 200:
                return await resp.json(content_type=None)
            body_preview = self._redact((await resp.text())[:200])
            if resp.status == 429:
                ra = resp.headers.get("Retry-After", "")
                wait_s = min(int(ra) if ra.isdigit() else 5, _MAX_RETRY_AFTER_S)
                logger.warning("itd_511 rate-limited",
                               extra={"endpoint": endpoint, "retry_after": wait_s, "body": body_preview})
                raise _Transient(f"429 retry-after={wait_s}", wait_s=wait_s)
            if 500 <= resp.status < 600:
                logger.warning("itd_511 upstream 5xx",
                               extra={"endpoint": endpoint, "status": resp.status, "body": body_preview})
                raise _Transient(f"{resp.status} server error")
            logger.warning("itd_511 permanent error; skipping endpoint",
                           extra={"endpoint": endpoint, "status": resp.status, "body": body_preview})
            return None

    def _build_event_record(self, rec: dict[str, Any]) -> Event | None:
        """Build an Event from one /get/event row. Returns None without a stable id."""
        source_id = rec.get("SourceId") or rec.get("ID")
        if source_id is None:
            return None
        et = EVENT_TYPE_MAP.get(rec.get("EventType") or "", "incident")
        lat, lon = rec.get("Latitude"), rec.get("Longitude")
        geom, centroid = _build_geometry(
            lat, lon, rec.get("LatitudeSecondary"), rec.get("LongitudeSecondary"),
            rec.get("EncodedPolyline"),
        )
        comment = _strip_or_none(rec.get("Comment"))
        data: dict[str, Any] = {
            "event_type_short": et,
            "event_sub_type": _strip_or_none(rec.get("EventSubType")),
            "roadway_name": _strip_or_none(rec.get("RoadwayName")),
            "direction": _strip_or_none(rec.get("DirectionOfTravel")),
            "description": _strip_or_none(rec.get("Description")),
            "lanes_affected": _strip_or_none(rec.get("LanesAffected")),
            "is_full_closure": bool(rec.get("IsFullClosure")),
            "itd_severity": rec.get("Severity"),
            "comment": comment,
            "cause": _strip_or_none(rec.get("Cause")),
            "organization": rec.get("Organization"),
            "recurrence_text": _strip_or_none(rec.get("Recurrence")),
            "recurrence_schedules": rec.get("RecurrenceSchedules") or [],
            "restrictions": rec.get("Restrictions") or {},
            "detour_polyline": rec.get("DetourPolyline") or None,
            "detour_instructions": _strip_or_none(rec.get("DetourInstructions")),
            "encoded_polyline": rec.get("EncodedPolyline"),
            "id_internal": rec.get("ID"),
            "source_id": rec.get("SourceId"),
            "reported_epoch": rec.get("Reported"),
            "last_updated_epoch": rec.get("LastUpdated"),
            "start_epoch": rec.get("StartDate"),
            "planned_end_epoch": rec.get("PlannedEndDate"),
            "latitude": lat,
            "longitude": lon,
        }
        mm = mile_marker.extract(comment)
        if mm is not None:
            data.setdefault("_enriched", {})["mile_marker"] = mm
        return Event(
            id=f"idaho_511:event:{source_id}",
            adapter=self.name,
            category=f"{et}.itd_511",
            time=(_parse_epoch(rec.get("LastUpdated"))
                  or _parse_epoch(rec.get("Reported"))
                  or _parse_epoch(rec.get("StartDate"))
                  or datetime.now(timezone.utc)),
            expires=_parse_epoch(rec.get("PlannedEndDate")),
            severity=_itd_severity(rec.get("Severity"), bool(rec.get("IsFullClosure"))),
            geo=Geo(
                centroid=centroid, geometry=geom,
                regions=["US-ID"], primary_region="US-ID",
            ),
            data=data,
        )

    def _build_advisory_record(self, rec: dict[str, Any]) -> Event | None:
        """Build an Event from one /get/alerts row. ITD's currently-empty
        response means we can't field-map exactly; we structural-pass-through
        the whole record under ``data.advisory`` and probe a few likely id /
        timestamp / coord fields best-effort. Per-record try/except in poll()
        keeps a surprise shape from sinking the cycle."""
        source_id = (rec.get("SourceId") or rec.get("ID")
                     or rec.get("AlertId") or rec.get("Id"))
        if source_id is None:
            return None
        lat, lon = rec.get("Latitude"), rec.get("Longitude")
        geom, centroid = _build_geometry(
            lat, lon, rec.get("LatitudeSecondary"), rec.get("LongitudeSecondary"),
            rec.get("EncodedPolyline"),
        )
        return Event(
            id=f"idaho_511:advisory:{source_id}",
            adapter=self.name,
            category="advisory.itd_511",
            time=(_parse_epoch(rec.get("LastUpdated"))
                  or _parse_epoch(rec.get("Reported"))
                  or _parse_epoch(rec.get("StartDate"))
                  or datetime.now(timezone.utc)),
            expires=_parse_epoch(rec.get("PlannedEndDate") or rec.get("EndDate")),
            severity=_itd_severity(rec.get("Severity"), False),
            geo=Geo(
                centroid=centroid, geometry=geom,
                regions=["US-ID"], primary_region="US-ID",
            ),
            data={
                "event_type_short": "advisory",
                "description": _strip_or_none(rec.get("Description") or rec.get("Message")),
                "roadway_name": _strip_or_none(rec.get("RoadwayName")),
                "advisory": rec,  # full structural pass-through
                "source_id": rec.get("SourceId"),
                "latitude": lat,
                "longitude": lon,
            },
        )

    async def poll(self) -> AsyncIterator[Event]:
        if not self._session:
            raise RuntimeError("Session not initialized")
        if not self._api_key:
            logger.warning("itd_511: no API key for alias; skipping poll",
                           extra={"alias": self._api_key_alias})
            return

        do_advisories = self._adv_counter == 0
        self._adv_counter = (self._adv_counter + 1) % _ADVISORY_EVERY_N_POLLS

        endpoints = ["event"] + (["alerts"] if do_advisories else [])
        results = await asyncio.gather(
            *[self._fetch(ep) for ep in endpoints], return_exceptions=True,
        )

        events_raw: list[dict[str, Any]] | None = None
        adv_raw: list[dict[str, Any]] | None = None
        for ep, result in zip(endpoints, results):
            if isinstance(result, BaseException):
                logger.warning("itd_511 fetch failed",
                               extra={"endpoint": ep, "error": self._redact(str(result))})
                continue
            if ep == "event":
                events_raw = result
            else:
                adv_raw = result

        yielded = 0
        for rec in (events_raw or []):
            try:
                ev = self._build_event_record(rec)
            except Exception:  # one bad record never sinks the poll
                logger.exception("itd_511 event parse failed",
                                 extra={"id": rec.get("ID"), "source_id": rec.get("SourceId")})
                continue
            if ev is not None:
                yield ev
                yielded += 1
        for rec in (adv_raw or []):
            try:
                ev = self._build_advisory_record(rec)
            except Exception:  # advisory shape unverified — be defensive
                logger.exception("itd_511 advisory parse failed",
                                 extra={"id": rec.get("Id") or rec.get("ID")})
                continue
            if ev is not None:
                yield ev
                yielded += 1

        self.sweep_old_ids()
        logger.info("itd_511 poll completed",
                    extra={"events_yielded": yielded,
                           "events_raw": len(events_raw or []),
                           "advisories_raw": len(adv_raw or []) if do_advisories else None})

    def subject_for(self, event: Event) -> str:
        et = event.category.split(".", 1)[0]
        return f"central.traffic.{et}.us.id"
