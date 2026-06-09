"""CelesTrak TLE (Two-Line Element) adapter — orbital state for satellites.

Polls ``https://celestrak.org/NORAD/elements/gp.php`` for each configured
group (defaults: ``stations``, ``weather``, ``amateur``) plus any operator-
specified ``extra_norad_ids`` not already in a group. Each fetched group is
plain text in the 3-line TLE format (name, line1, line2). One Event per
satellite per refresh cycle.

Subjects are global (no per-state coord — TLEs are universal orbital state):

    central.sat.tle.<norad_id>

Consumers compute satellite passes locally with their own observer
geolocation (satellite.js / sgp4 client-side). The adapter publishes the raw
TLE strings on the wire so the pass-prediction math stays where the location
context lives -- not in Central.

Dedup key is ``{norad_id}:{epoch_iso}``: re-fetching the same TLE yields the
same key and is swallowed by the dedup mixin. CelesTrak refreshes TLEs ~8h;
the 4h default cadence gives us one update per refresh cycle without
thrashing them.

``_enriched.orbit`` is parsed directly from Line 2 columns -- no skyfield or
sgp4 dependency. The implicit leading ``0.`` on the eccentricity field
(NORAD's standard packed form) is reconstructed at parse time.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiohttp
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
from central.models import Event, Geo

logger = logging.getLogger(__name__)

_BASE_URL = "https://celestrak.org/NORAD/elements/gp.php"
_FETCH_TIMEOUT_S = 30

_DEDUP_DDL = (
    "CREATE TABLE IF NOT EXISTS published_ids ("
    "adapter TEXT NOT NULL, event_id TEXT NOT NULL, "
    "first_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "PRIMARY KEY (adapter, event_id))"
)

# TLE Line 1 / Line 2 are fixed 69-char records. Column slice offsets used
# below are 0-indexed inclusive-start, exclusive-end (Python convention).
# Per the v0.11.0 spec these are: epoch [18:32] (14 chars YYDDD.DDDDDDDD --
# the spec said cols 19-32 which was off-by-one against the actual layout),
# inclination [8:16], eccentricity [26:33] (with implicit leading "0."),
# mean motion [52:63]. Verified against live ISS TLE 25544 at probe time.


def _group_source_url(group: str) -> str:
    return f"{_BASE_URL}?GROUP={group}&FORMAT=TLE"


def _catnr_source_url(norad_id: int) -> str:
    return f"{_BASE_URL}?CATNR={norad_id}&FORMAT=TLE"


def _parse_tle_groups(text: str) -> list[tuple[str, str, str]]:
    """Split a CelesTrak TLE response into ``(name, line1, line2)`` triples.

    Tolerant to trailing whitespace, blank lines between records, and CRLF
    line endings (CelesTrak emits LF; defensive normalisation costs nothing).
    Records whose lines don't start with the expected ``1 ``/``2 `` markers
    are skipped silently -- malformed segments don't sink the batch.
    """
    out: list[tuple[str, str, str]] = []
    # Normalise line endings; filter out blank lines.
    lines = [
        ln.rstrip("\r").rstrip()
        for ln in text.replace("\r\n", "\n").split("\n")
        if ln.strip()
    ]
    i = 0
    while i + 2 < len(lines):
        name = lines[i].rstrip()
        l1 = lines[i + 1]
        l2 = lines[i + 2]
        if l1.startswith("1 ") and l2.startswith("2 "):
            out.append((name, l1, l2))
            i += 3
        else:
            # Mis-aligned: skip one line and try to re-sync.
            i += 1
    return out


def _decode_epoch(line1: str) -> datetime | None:
    """Decode Line 1 cols [18:32] (YYDDD.DDDDDDDD) into a UTC datetime.

    NORAD's Y2K rule: ``YY`` 00–56 → 2000–2056, ``YY`` 57–99 → 1957–1999.
    Day-of-year is 1-indexed (``001`` = Jan 1).
    """
    if not line1 or len(line1) < 32:
        return None
    field = line1[18:32]
    try:
        yy = int(field[0:2])
        doy_frac = float(field[2:])
    except (ValueError, IndexError):
        return None
    year = 2000 + yy if yy < 57 else 1900 + yy
    try:
        return datetime(year, 1, 1, tzinfo=timezone.utc) + timedelta(days=doy_frac - 1.0)
    except (OverflowError, ValueError):
        return None


def _decode_orbit(line2: str) -> dict[str, float] | None:
    """Parse the three orbit elements out of Line 2 column slots.

    Returns ``{inclination_deg, mean_motion_rev_per_day, eccentricity}`` or
    ``None`` on any parse failure (the ``_enriched.orbit`` bundle is then
    omitted from the Event, per spec).
    """
    if not line2 or len(line2) < 63:
        return None
    try:
        inclination = float(line2[8:16])
        # Eccentricity is stored as 7 digits with an implicit leading "0.".
        ecc_raw = line2[26:33].strip()
        if not ecc_raw.isdigit():
            return None
        eccentricity = float("0." + ecc_raw)
        mean_motion = float(line2[52:63])
    except (ValueError, IndexError):
        return None
    return {
        "inclination_deg": inclination,
        "mean_motion_rev_per_day": mean_motion,
        "eccentricity": eccentricity,
    }


def _norad_id_from_line1(line1: str) -> int | None:
    """Extract the catalog number from Line 1 cols [2:7]."""
    try:
        return int(line1[2:7].strip())
    except (ValueError, IndexError):
        return None


class CelestrakTleSettings(BaseModel):
    """``groups``: CelesTrak group IDs to fetch in full. ``extra_norad_ids``:
    operator-specified satellites outside the group rosters."""

    groups: list[str] = ["stations", "weather", "amateur"]
    extra_norad_ids: list[int] = []


_EXP_WAIT = wait_exponential_jitter(initial=1, max=30)


class CelestrakTleAdapter(SourceAdapter):
    """CelesTrak TLE (orbital element) publisher."""

    name = "celestrak_tle"
    display_name = "CelesTrak satellite TLEs"
    description = (
        "Orbital state (TLE / two-line elements) for satellites listed by "
        "CelesTrak. Polls the configured group rosters plus any extra NORAD "
        "IDs the operator pins; consumers compute passes locally."
    )
    settings_schema = CelestrakTleSettings
    requires_api_key = None
    wizard_order = None
    default_cadence_s = 14400  # 4h; CelesTrak refreshes ~8h
    data_class = "telemetry"
    enrichment_locations = []

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
        self._groups: list[str] = list(
            config.settings.get("groups") or ["stations", "weather", "amateur"]
        )
        self._extra_ids: set[int] = {
            int(x) for x in (config.settings.get("extra_norad_ids") or [])
        }

    async def startup(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_FETCH_TIMEOUT_S),
            headers={"User-Agent": "Central/0.11 (+celestrak_tle)"},
        )
        self._db = sqlite3.connect(self._cursor_db_path)
        self._db.execute(_DEDUP_DDL)
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS published_ids_last_seen "
            "ON published_ids (last_seen)"
        )
        self._db.commit()
        logger.info(
            "celestrak_tle adapter started",
            extra={"groups": self._groups, "extra_norad_ids": sorted(self._extra_ids)},
        )

    async def shutdown(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
        if self._db:
            self._db.close()
            self._db = None

    async def apply_config(self, new_config: AdapterConfig) -> None:
        self._groups = list(
            new_config.settings.get("groups") or ["stations", "weather", "amateur"]
        )
        self._extra_ids = {
            int(x) for x in (new_config.settings.get("extra_norad_ids") or [])
        }
        logger.info(
            "celestrak_tle config updated",
            extra={"groups": self._groups, "extra_norad_ids": sorted(self._extra_ids)},
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
    async def _fetch_url(self, url: str) -> str | None:
        """GET a CelesTrak URL; return body text on 200 else None."""
        if self._session is None:
            raise RuntimeError("celestrak_tle session not started")
        async with self._session.get(url) as resp:
            if resp.status == 200:
                body = await resp.text()
                if body.startswith("Invalid query"):
                    logger.warning(
                        "celestrak_tle upstream rejected request",
                        extra={"url": url, "body": body[:200]},
                    )
                    return None
                return body
            logger.warning(
                "celestrak_tle upstream non-200; skipping this fetch",
                extra={"url": url, "status": resp.status},
            )
            return None

    def _build_event_record(
        self,
        name: str,
        line1: str,
        line2: str,
        source_url: str,
    ) -> Event | None:
        """Build an Event from one (name, line1, line2) triple. None on parse fail."""
        norad_id = _norad_id_from_line1(line1)
        if norad_id is None:
            return None
        epoch = _decode_epoch(line1)
        if epoch is None:
            return None
        epoch_iso = epoch.isoformat()
        intl_designator = line1[9:17].strip()
        classification = line1[7:8].strip() or "U"
        orbit = _decode_orbit(line2)

        data: dict[str, Any] = {
            "norad_id": norad_id,
            "satellite_name": name.strip(),
            "tle_line1": line1,
            "tle_line2": line2,
            "epoch": epoch_iso,
            "classification": classification,
            "intl_designator": intl_designator,
            "source_url": source_url,
        }
        if orbit is not None:
            data["_enriched"] = {"orbit": orbit}

        return Event(
            id=f"{norad_id}:{epoch_iso}",
            adapter=self.name,
            category="tle.celestrak_tle",
            time=epoch,
            severity=1,
            geo=Geo(),
            data=data,
        )

    async def poll(self) -> AsyncIterator[Event]:
        if not self._session:
            raise RuntimeError("Session not initialized")

        # Phase 1: fetch all groups concurrently.
        group_results = await asyncio.gather(
            *[self._fetch_url(_group_source_url(g)) for g in self._groups],
            return_exceptions=True,
        )

        # records: norad_id -> (name, line1, line2, source_url)
        records: dict[int, tuple[str, str, str, str]] = {}
        for group, result in zip(self._groups, group_results):
            if isinstance(result, BaseException) or not result:
                if isinstance(result, BaseException):
                    logger.warning(
                        "celestrak_tle group fetch failed",
                        extra={"group": group, "error": str(result)},
                    )
                continue
            source_url = _group_source_url(group)
            for name, l1, l2 in _parse_tle_groups(result):
                nid = _norad_id_from_line1(l1)
                if nid is None:
                    continue
                # Duplicates across groups collapse: first group wins.
                if nid not in records:
                    records[nid] = (name, l1, l2, source_url)

        # Phase 2: fetch extras not already collected by any group.
        needed_extras = [nid for nid in self._extra_ids if nid not in records]
        if needed_extras:
            extra_results = await asyncio.gather(
                *[self._fetch_url(_catnr_source_url(nid)) for nid in needed_extras],
                return_exceptions=True,
            )
            for nid, result in zip(needed_extras, extra_results):
                if isinstance(result, BaseException) or not result:
                    continue
                parsed = _parse_tle_groups(result)
                if parsed:
                    name, l1, l2 = parsed[0]
                    records[nid] = (name, l1, l2, _catnr_source_url(nid))

        # Phase 3: yield Events.
        yielded = 0
        for nid, (name, l1, l2, source_url) in records.items():
            try:
                ev = self._build_event_record(name, l1, l2, source_url)
            except Exception:
                logger.exception(
                    "celestrak_tle event build failed", extra={"norad_id": nid}
                )
                continue
            if ev is not None:
                yield ev
                yielded += 1

        self.sweep_old_ids()
        logger.info(
            "celestrak_tle poll completed",
            extra={
                "groups": self._groups,
                "extras": sorted(self._extra_ids),
                "records_collected": len(records),
                "events_yielded": yielded,
            },
        )

    def subject_for(self, event: Event) -> str:
        nid = event.data.get("norad_id")
        if nid is None:
            return "central.sat.tle.unknown"
        return f"central.sat.tle.{nid}"
