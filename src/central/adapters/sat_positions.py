"""Global satellite positions -- live sub-satellite point publisher (v0.12.0).

Complement to satpass_predict (which is observer-anchored: "when does sat X
pass over observer Y?"). This adapter is global: "where is sat X right now?"
Publishes one telemetry Event per tracked satellite per poll (default 60s)
on subject ``central.sat.position.<norad_id>`` so any consumer (meshAI, GUI
map, ...) can render a live world map of every tracked satellite without
caring about observers.

TLE source is the same celestrak_tle events table that satpass_predict
reads -- enable that adapter first. Empty TLE table = zero events yielded,
no exception.

Math: SGP4 propagation gives ECI position + velocity; ECEF rotation gives
the sub-satellite point. Velocity magnitude is the orbital speed. Heading
is the great-circle bearing of motion derived by finite-difference between
the current position and a position 1 second later (avoids rotating the
velocity vector through GMST + the earth-rotation cross term).

Severity 1 (informational telemetry). ``data_class = "telemetry"`` so these
events surface on /telemetry, not /events -- 60s ticks across ~190 sats
would drown discrete-event signal otherwise.

Dedup id ``<norad_id>:<position_iso>`` where position_iso is the
propagation timestamp truncated to whole seconds. Two ticks landing in the
same second collapse (defensive at 60s cadence; matters if cadence is ever
tightened past once-per-second).
"""

from __future__ import annotations

import logging
import math
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel
from sgp4.api import Satrec, jday

from central.adapter import SourceAdapter
from central.adapters.sat_common import eci_to_ecef, gmst_rad, subsatellite_point
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

# Parameterized on max_tle_age_days so operator-tightened windows (e.g. 3 days
# for high-drag LEO emphasis) apply without a string-interpolated interval.
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


def _great_circle_bearing(
    lon1_deg: float, lat1_deg: float, lon2_deg: float, lat2_deg: float,
) -> float:
    """Initial bearing from point 1 to point 2 in [0, 360) degrees.

    0 = north, 90 = east, 180 = south, 270 = west. Used for instantaneous
    heading via finite-difference between two SGP4 samples 1 second apart.
    """
    lat1 = math.radians(lat1_deg)
    lat2 = math.radians(lat2_deg)
    dlon = math.radians(lon2_deg - lon1_deg)
    y = math.sin(dlon) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    return math.degrees(math.atan2(y, x)) % 360.0


def _propagate_position(
    sat: Satrec, t: datetime,
) -> tuple[tuple[float, float, float], tuple[float, float, float], float, float, float] | None:
    """One SGP4 step. Returns ``(pos_ecef, vel_eci, lon, lat, alt)`` or None on err."""
    jd, fr = jday(t.year, t.month, t.day, t.hour, t.minute, t.second + t.microsecond / 1e6)
    err, pos_eci, vel_eci = sat.sgp4(jd, fr)
    if err:
        return None
    pos_ecef = eci_to_ecef(pos_eci, gmst_rad(jd, fr))
    lon, lat, alt = subsatellite_point(pos_ecef)
    return pos_ecef, vel_eci, lon, lat, alt


class SatPositionsSettings(BaseModel):
    """track_only_norad_ids empty = track every NORAD ID with a fresh TLE
    (derive-from-celestrak_tle, the default). Non-empty list pins to those
    NORAD IDs only -- useful for "I only care about the ISS and Starlink-N".
    max_tle_age_days bounds how stale a TLE can be before the propagation
    is considered too drifty to publish; LEO drag means TLEs go stale in
    days, GEO satellites are good for months. 14 is a safe default."""
    track_only_norad_ids: list[int] = []
    max_tle_age_days: int = 14


class SatPositionsAdapter(SourceAdapter):
    """Live global satellite-position telemetry."""

    name = "sat_positions"
    display_name = "Live Satellite Positions"
    description = (
        "Publishes the current sub-satellite point (lon, lat, alt) for every "
        "tracked NORAD ID by propagating the latest TLE via SGP4. One "
        "telemetry event per satellite per poll (default 60s) so consumers "
        "can render a live world map of where the satellites are right now. "
        "Source TLEs come from the celestrak_tle adapter -- enable that first."
    )
    settings_schema = SatPositionsSettings
    requires_api_key = None
    wizard_order = None
    default_cadence_s = 60
    data_class = "telemetry"
    enrichment_locations = []
    # Global-by-design: one event per satellite per poll with the sub-satellite
    # point as centroid -- a worldwide firehose. A geographic monitoring area
    # (e.g. Idaho) would drop ~99.7% of it, defeating "where is the ISS" global
    # queries. Skip the publish-time/archive bbox filter (v0.14.2). Keep in sync
    # with archive._BYPASS_BBOX_ADAPTERS.
    bypass_bbox_filter = True
    # Dedup IDs are "<norad_id>:<position_iso>" -- unique per second by design,
    # so a 14-day window accumulates ~3.5M rows unnecessarily. 1 day is enough.
    dedup_sweep_days = 1

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
            "sat_positions adapter started",
            extra={
                "track_only_count": len(self._track_only),
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
            "sat_positions config updated",
            extra={
                "track_only_count": len(self._track_only),
                "max_tle_age_days": self._max_tle_age_days,
            },
        )

    async def _fetch_latest_tles(self) -> list[dict[str, Any]]:
        """Latest TLE row per norad_id within the configured age window.
        Empty list if no TLEs available; never raises."""
        pool = self._config_store.get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                _LATEST_TLES_SQL, timedelta(days=self._max_tle_age_days),
            )
        return [dict(r) for r in rows]

    def _build_event(
        self,
        row: dict[str, Any],
        ref_time: datetime,
        lon_deg: float,
        lat_deg: float,
        alt_km: float,
        velocity_kmps: float,
        heading_deg: float,
    ) -> Event:
        # Truncate to whole seconds so the dedup id collapses two ticks that
        # land in the same second. Defensive at 60s cadence; relevant if the
        # operator ever drops cadence below 1s (won't happen, but the bug
        # would be silent dedup-window collisions instead of a clear error).
        position_iso = ref_time.replace(microsecond=0).isoformat()
        return Event(
            id=f"{row['norad_id']}:{position_iso}",
            adapter=self.name,
            category="position.sat_positions",
            time=ref_time,
            severity=1,
            geo=Geo(centroid=(lon_deg, lat_deg)),
            data={
                "norad_id": row["norad_id"],
                "satellite_name": row["satellite_name"],
                "lon_deg": round(lon_deg, 4),
                "lat_deg": round(lat_deg, 4),
                "alt_km": round(alt_km, 1),
                "velocity_kmps": round(velocity_kmps, 3),
                "heading_deg": round(heading_deg, 1),
                "tle_epoch": row["tle_epoch"],
            },
        )

    async def poll(self) -> AsyncIterator[Event]:
        rows = await self._fetch_latest_tles()
        if not rows:
            logger.info(
                "sat_positions: no TLEs available; nothing to publish "
                "(is celestrak_tle enabled and has it polled at least once?)"
            )
            return

        ref_time = datetime.now(timezone.utc)
        ref_plus_1s = ref_time + timedelta(seconds=1)
        yielded = 0
        for row in rows:
            if self._track_only and row["norad_id"] not in self._track_only:
                continue
            try:
                sat = Satrec.twoline2rv(row["tle_line1"], row["tle_line2"])
            except Exception:
                logger.warning(
                    "sat_positions: TLE parse failed",
                    extra={"norad_id": row["norad_id"]},
                )
                continue
            sample = _propagate_position(sat, ref_time)
            if sample is None:
                logger.warning(
                    "sat_positions: SGP4 propagation failed",
                    extra={"norad_id": row["norad_id"]},
                )
                continue
            _, vel_eci, lon_deg, lat_deg, alt_km = sample
            # Velocity magnitude in ECI -- close enough to ECEF for "the sat
            # is moving at X km/s" consumer text (earth rotation is ~0.46
            # km/s at equator vs ~7.7 km/s LEO orbital speed, sub-6% delta).
            velocity_kmps = math.sqrt(
                vel_eci[0] ** 2 + vel_eci[1] ** 2 + vel_eci[2] ** 2,
            )
            sample_next = _propagate_position(sat, ref_plus_1s)
            if sample_next is None:
                heading_deg = 0.0
            else:
                _, _, lon_next, lat_next, _ = sample_next
                heading_deg = _great_circle_bearing(
                    lon_deg, lat_deg, lon_next, lat_next,
                )
            yield self._build_event(
                row, ref_time, lon_deg, lat_deg, alt_km,
                velocity_kmps, heading_deg,
            )
            yielded += 1

        self.sweep_old_ids()
        logger.info(
            "sat_positions poll completed",
            extra={
                "tles_considered": len(rows),
                "track_only_count": len(self._track_only),
                "events_yielded": yielded,
            },
        )

    def subject_for(self, event: Event) -> str:
        return f"central.sat.position.{event.data['norad_id']}"
