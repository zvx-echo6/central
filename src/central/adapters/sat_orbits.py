"""Forward-orbit-track publisher (v0.13.0).

Line counterpart to sat_positions: one LineString (or antimeridian-split
MultiLineString) per tracked satellite per poll, projecting the next 90
minutes of sub-satellite track. ``data_class = "telemetry"`` so these
events surface on /telemetry, not /events -- they're continuous-state
data, not alerts. Geo carries both centroid (current sub-sat point, for
the "where it is" dot) and geometry (the forward track, for the
"where it's going" line).

Math reuses the sat_common SGP4/ECEF/lat-lon helpers; the antimeridian
splitter (also in sat_common) handles polar-orbit dateline crossings so
Leaflet doesn't draw the "wrong-way wrap" Matt saw with the v0.11.2
satpass_predict ground-track render.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel
from sgp4.api import Satrec, jday

from central.adapter import SourceAdapter
from central.adapters.sat_common import (
    eci_to_ecef,
    gmst_rad,
    split_antimeridian,
    subsatellite_point,
)
from central.config_models import AdapterConfig
from central.config_store import ConfigStore
from central.models import Event, Geo

logger = logging.getLogger(__name__)

_DEDUP_DDL = (
    "CREATE TABLE IF NOT EXISTS published_ids ("
    "adapter TEXT NOT NULL, event_id TEXT NOT NULL, "
    "first_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "PRIMARY KEY (adapter, event_id))"
)

_LATEST_TLES_SQL = """
SELECT DISTINCT ON (payload->'data'->'data'->>'norad_id')
    (payload->'data'->'data'->>'norad_id')::int AS norad_id,
    payload->'data'->'data'->>'satellite_name' AS satellite_name,
    payload->'data'->'data'->>'tle_line1' AS tle_line1,
    payload->'data'->'data'->>'tle_line2' AS tle_line2,
    payload->'data'->'data'->>'epoch' AS tle_epoch
FROM events
WHERE adapter = 'celestrak_tle'
  AND time > now() - $1::interval
ORDER BY payload->'data'->'data'->>'norad_id', time DESC
"""


def _propagate_track(
    sat: Satrec,
    start: datetime,
    forward_minutes: int,
    sample_seconds: int,
) -> list[tuple[float, float, float]]:
    """Sub-sat track from start, forward_minutes ahead, every sample_seconds.

    Returns list of (lon, lat, alt) tuples. Vertices that fail SGP4
    propagation are silently skipped (decayed orbit / numerical edge case);
    the caller can detect a degenerate result via len() check.
    """
    track: list[tuple[float, float, float]] = []
    total_steps = (forward_minutes * 60) // sample_seconds
    for k in range(total_steps + 1):
        t = start + timedelta(seconds=k * sample_seconds)
        jd, fr = jday(
            t.year, t.month, t.day, t.hour, t.minute,
            t.second + t.microsecond / 1e6,
        )
        err, pos_eci, _ = sat.sgp4(jd, fr)
        if err:
            continue
        pos_ecef = eci_to_ecef(pos_eci, gmst_rad(jd, fr))
        lon, lat, alt = subsatellite_point(pos_ecef)
        track.append((lon, lat, alt))
    return track


class SatOrbitsSettings(BaseModel):
    """track_only_norad_ids empty = track every NORAD ID with a fresh TLE
    (derive-from-celestrak_tle default). forward_minutes 90 covers ~1 LEO
    orbit at 7.7 km/s. sample_seconds 60 yields ~90 vertices per event;
    lower = smoother but heavier JSON. max_tle_age_days bounds TLE
    freshness for SGP4 accuracy."""
    track_only_norad_ids: list[int] = []
    forward_minutes: int = 90
    sample_seconds: int = 60
    max_tle_age_days: int = 14


class SatOrbitsAdapter(SourceAdapter):
    """Forward-orbit-track telemetry: one LineString per satellite per poll."""

    name = "sat_orbits"
    display_name = "Satellite Orbit Tracks"
    description = (
        "Forward-projects each tracked satellite's sub-satellite track for "
        "the next 90 minutes (~1 LEO orbit), publishing one LineString "
        "telemetry event per satellite per poll. Companion to sat_positions "
        "(current point per sat) and satpass_predict (observer-anchored "
        "passes). Antimeridian-aware: polar orbits split into MultiLineString "
        "to render cleanly across the dateline in Leaflet."
    )
    settings_schema = SatOrbitsSettings
    requires_api_key = None
    wizard_order = None
    default_cadence_s = 300  # 5 min
    data_class = "telemetry"
    enrichment_locations = []
    # Global-by-design: forward-orbit LineStrings (mostly polar) span the globe,
    # not a region. A geographic monitoring area would drop nearly all of them.
    # Skip the publish-time/archive bbox filter (v0.14.2). Keep in sync with
    # archive._BYPASS_BBOX_ADAPTERS.
    bypass_bbox_filter = True
    # Dedup IDs are "<norad_id>:<propagation_iso>" -- unique per second by design,
    # so a 14-day window accumulates rows unnecessarily. TLEs are stable for days,
    # so 2 days is sufficient to catch re-emitted orbits without bloating the table.
    dedup_sweep_days = 2

    def __init__(
        self,
        config: AdapterConfig,
        config_store: ConfigStore,
        cursor_db_path: Path,
    ) -> None:
        self._config_store = config_store
        self._cursor_db_path = cursor_db_path
        self._db: sqlite3.Connection | None = None
        self._apply_settings(config.settings or {})

    def _apply_settings(self, settings: dict[str, Any]) -> None:
        raw_ids = settings.get("track_only_norad_ids") or []
        self._track_only: set[int] = {int(n) for n in raw_ids}
        self._forward_minutes: int = int(settings.get("forward_minutes") or 90)
        self._sample_seconds: int = int(settings.get("sample_seconds") or 60)
        self._max_tle_age_days: int = int(settings.get("max_tle_age_days") or 14)

    async def startup(self) -> None:
        self._db = sqlite3.connect(self._cursor_db_path)
        # WAL + NORMAL sync: eliminates per-commit fsync overhead on cursors.db.
        # WAL mode is file-level persistent; applies to all adapters on this DB.
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.execute(_DEDUP_DDL)
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS published_ids_last_seen ON published_ids (last_seen)"
        )
        self._db.commit()
        logger.info(
            "sat_orbits adapter started",
            extra={
                "track_only_count": len(self._track_only),
                "forward_minutes": self._forward_minutes,
                "sample_seconds": self._sample_seconds,
                "max_tle_age_days": self._max_tle_age_days,
            },
        )

    async def shutdown(self) -> None:
        if self._db:
            self._db.close()
            self._db = None

    async def apply_config(self, new_config: AdapterConfig) -> None:
        self._apply_settings(new_config.settings or {})
        logger.info(
            "sat_orbits config updated",
            extra={
                "track_only_count": len(self._track_only),
                "forward_minutes": self._forward_minutes,
                "sample_seconds": self._sample_seconds,
            },
        )

    async def _fetch_latest_tles(self) -> list[dict[str, Any]]:
        pool = self._config_store.get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                _LATEST_TLES_SQL, timedelta(days=self._max_tle_age_days),
            )
        return [dict(r) for r in rows]

    def _build_event(
        self,
        row: dict[str, Any],
        propagation_start: datetime,
        track: list[tuple[float, float, float]],
    ) -> Event | None:
        if len(track) < 2:
            return None
        # First vertex is "current" -- the propagation_start sample.
        current_lon, current_lat, current_alt = track[0]
        geometry = split_antimeridian([(p[0], p[1]) for p in track])
        # Truncate to whole seconds so the dedup id collapses two ticks that
        # land in the same second. Matches the sat_positions convention.
        prop_iso = propagation_start.replace(microsecond=0).isoformat()
        return Event(
            id=f"{row['norad_id']}:{prop_iso}",
            adapter=self.name,
            category="orbit.sat_orbits",
            time=propagation_start,
            severity=1,
            geo=Geo(
                centroid=(current_lon, current_lat),
                geometry=geometry,
            ),
            data={
                "norad_id": row["norad_id"],
                "satellite_name": row["satellite_name"],
                "propagation_start_iso": prop_iso,
                "forward_minutes": self._forward_minutes,
                "sample_seconds": self._sample_seconds,
                "vertex_count": len(track),
                "current_lon_deg": round(current_lon, 4),
                "current_lat_deg": round(current_lat, 4),
                "current_alt_km": round(current_alt, 1),
                "tle_epoch": row["tle_epoch"],
            },
        )

    async def poll(self) -> AsyncIterator[Event]:
        rows = await self._fetch_latest_tles()
        if not rows:
            logger.info(
                "sat_orbits: no TLEs available; nothing to publish "
                "(is celestrak_tle enabled and has it polled at least once?)"
            )
            return

        propagation_start = datetime.now(timezone.utc)
        yielded = 0
        skipped_parse = 0
        skipped_short = 0
        for row in rows:
            if self._track_only and row["norad_id"] not in self._track_only:
                continue
            try:
                sat = Satrec.twoline2rv(row["tle_line1"], row["tle_line2"])
            except Exception:
                skipped_parse += 1
                logger.warning(
                    "sat_orbits: TLE parse failed",
                    extra={"norad_id": row["norad_id"]},
                )
                continue
            track = _propagate_track(
                sat, propagation_start,
                self._forward_minutes, self._sample_seconds,
            )
            ev = self._build_event(row, propagation_start, track)
            if ev is None:
                skipped_short += 1
                logger.warning(
                    "sat_orbits: track too short after propagation",
                    extra={"norad_id": row["norad_id"], "vertices": len(track)},
                )
                continue
            yield ev
            yielded += 1

        self.sweep_old_ids()
        logger.info(
            "sat_orbits poll completed",
            extra={
                "tles_considered": len(rows),
                "track_only_count": len(self._track_only),
                "events_yielded": yielded,
                "skipped_parse": skipped_parse,
                "skipped_short": skipped_short,
            },
        )

    def subject_for(self, event: Event) -> str:
        return f"central.sat.orbit.{event.data['norad_id']}"
