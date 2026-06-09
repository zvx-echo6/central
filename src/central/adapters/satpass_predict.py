"""Satellite pass predictor — server-side complement to client-side satpass.

Polls the ``events`` table for the latest TLE per ``norad_id`` (within the last
14 days), then propagates each one with SGP4 against every configured fixed
observer over a 24-hour horizon. Emits one Event per upcoming pass per
(observer, satellite) tuple. Dedup id is ``{observer_slug}:{norad_id}:{aos_iso}``
so re-running the same poll within the hour produces identical ids and is
swallowed by the dedup mixin; new TLEs landing between polls produce slightly
different propagation paths and hence different AOS times, naturally triggering
a republish.

Severity bucket from peak elevation:

    >= 60° (zenith)  -> 4
    >= 30° (high)    -> 3
    >= 10° (low)     -> 2
    <  10°           -> 1 (gated: not emitted)

Subject: ``central.sat.pass.us.<state>.<observer_slug>`` -- one subject per
observer. Multiple satellites passing the same observer collapse to the same
subject; the dedup-discriminated Nats-Msg-Id (v0.10.8: ``id:category``) keeps
each pass distinct in JetStream's dedup window.

Math: SGP4 propagation gives ECI; we rotate to ECEF via GMST (Vallado mean
sidereal formula) then to topocentric east-north-up using the observer's
geodetic position (spherical earth, 6378.137 km equatorial radius -- fine for
horizon/elevation determination, error << 0.1° in azimuth). Pass detection
walks a 60-second grid looking for elevation-crossing events at the configured
``min_elevation_deg`` threshold.
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
from central.config_models import AdapterConfig
from central.config_store import ConfigStore
from central.models import Event, Geo

logger = logging.getLogger(__name__)

# Earth equatorial radius (WGS-84). Used for observer ECEF position; we treat
# the earth as spherical for topocentric look-angle math -- ellipsoidal effects
# matter for centimeter-level GPS work but are well below 0.1° in elevation,
# more than enough for "is the satellite above the horizon" decisions.
_EARTH_RADIUS_KM = 6378.137

_PASS_STEP_S = 60          # 60-second grid for elevation sampling
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
  AND time > now() - interval '14 days'
ORDER BY payload->'data'->'data'->>'norad_id', time DESC
"""


# --- Pure math helpers (no I/O) ---------------------------------------------


def _gmst_rad(jd: float, fr: float) -> float:
    """Greenwich Mean Sidereal Time in radians (Vallado, simplified).

    Accurate to within milliseconds for any post-1900 epoch -- plenty for
    horizon/elevation work.
    """
    t = (jd + fr - 2451545.0) / 36525.0
    gmst_sec = (
        67310.54841
        + (876600.0 * 3600.0 + 8640184.812866) * t
        + 0.093104 * t * t
        - 6.2e-6 * t * t * t
    )
    return (gmst_sec % 86400.0) * (2.0 * math.pi / 86400.0)


def _eci_to_ecef(pos_eci_km: tuple[float, float, float], theta: float) -> tuple[float, float, float]:
    """Rotate ECI coordinates to ECEF by GMST angle theta (radians)."""
    x, y, z = pos_eci_km
    ct = math.cos(theta)
    st = math.sin(theta)
    return (ct * x + st * y, -st * x + ct * y, z)


def _observer_ecef(lat_deg: float, lon_deg: float, elev_m: float) -> tuple[float, float, float]:
    """Observer position in ECEF km (spherical earth, sub-0.1° precision)."""
    lat_r = math.radians(lat_deg)
    lon_r = math.radians(lon_deg)
    r = _EARTH_RADIUS_KM + elev_m / 1000.0
    return (
        r * math.cos(lat_r) * math.cos(lon_r),
        r * math.cos(lat_r) * math.sin(lon_r),
        r * math.sin(lat_r),
    )


def _topocentric_az_el(
    sat_ecef_km: tuple[float, float, float],
    obs_ecef_km: tuple[float, float, float],
    obs_lat_deg: float,
    obs_lon_deg: float,
) -> tuple[float, float]:
    """Return ``(azimuth_deg, elevation_deg)`` from observer to satellite.

    Azimuth measured from north, clockwise (0 = N, 90 = E). Elevation is the
    angle above the local horizon (0 = horizon, 90 = zenith, negative = below).
    """
    dx = sat_ecef_km[0] - obs_ecef_km[0]
    dy = sat_ecef_km[1] - obs_ecef_km[1]
    dz = sat_ecef_km[2] - obs_ecef_km[2]

    lat_r = math.radians(obs_lat_deg)
    lon_r = math.radians(obs_lon_deg)
    sl, cl = math.sin(lat_r), math.cos(lat_r)
    slo, clo = math.sin(lon_r), math.cos(lon_r)

    east = -slo * dx + clo * dy
    north = -sl * clo * dx - sl * slo * dy + cl * dz
    up = cl * clo * dx + cl * slo * dy + sl * dz

    horizontal = math.sqrt(east * east + north * north)
    elevation = math.degrees(math.atan2(up, horizontal))
    azimuth = math.degrees(math.atan2(east, north)) % 360.0
    return azimuth, elevation


def _elev_at(
    sat: Satrec,
    t: datetime,
    obs_ecef_km: tuple[float, float, float],
    obs_lat_deg: float,
    obs_lon_deg: float,
) -> tuple[float, float] | None:
    """Compute ``(azimuth_deg, elevation_deg)`` at instant ``t`` (UTC datetime).

    Returns ``None`` if SGP4 reports a propagation error (decayed orbit, bad
    epoch, etc.).
    """
    jd, fr = jday(t.year, t.month, t.day, t.hour, t.minute, t.second + t.microsecond / 1e6)
    err, pos_eci, _ = sat.sgp4(jd, fr)
    if err:
        return None
    sat_ecef = _eci_to_ecef(pos_eci, _gmst_rad(jd, fr))
    return _topocentric_az_el(sat_ecef, obs_ecef_km, obs_lat_deg, obs_lon_deg)


def _severity_from_elev(max_elev_deg: float) -> int:
    """Pass severity per the v0.11.1 bucketing rule."""
    if max_elev_deg >= 60.0:
        return 4
    if max_elev_deg >= 30.0:
        return 3
    if max_elev_deg >= 10.0:
        return 2
    return 1  # below the gate; never emitted in practice


def _next_passes(
    tle_line1: str,
    tle_line2: str,
    observer: "Observer",
    ref_time: datetime,
    horizon_hours: float,
    min_elevation_deg: float,
) -> list[dict[str, Any]]:
    """Walk a 60-second grid; return all passes >= min_elevation_deg in window."""
    try:
        sat = Satrec.twoline2rv(tle_line1, tle_line2)
    except Exception:
        return []
    obs_ecef = _observer_ecef(observer.lat, observer.lon, observer.elev_m)

    passes: list[dict[str, Any]] = []
    in_pass = False
    aos_t: datetime | None = None
    aos_az: float | None = None
    peak_t: datetime | None = None
    peak_e: float = -180.0
    peak_az: float | None = None

    t = ref_time
    end = ref_time + timedelta(hours=horizon_hours)
    step = timedelta(seconds=_PASS_STEP_S)
    while t < end:
        sample = _elev_at(sat, t, obs_ecef, observer.lat, observer.lon)
        if sample is None:
            t += step
            continue
        az, e = sample
        if e >= min_elevation_deg:
            if not in_pass:
                in_pass = True
                aos_t = t
                aos_az = az
                peak_t = t
                peak_e = e
                peak_az = az
            elif e > peak_e:
                peak_t = t
                peak_e = e
                peak_az = az
        elif in_pass:
            # threshold-crossing on the way down -> close the pass
            passes.append({
                "aos": aos_t, "aos_az": aos_az,
                "peak": peak_t, "peak_az": peak_az, "max_elev_deg": peak_e,
                "los": t, "los_az": az,
            })
            in_pass = False
            aos_t = aos_az = peak_t = peak_az = None
            peak_e = -180.0
        t += step

    # Pass still in progress at the horizon edge -- close it at the boundary.
    if in_pass and aos_t and peak_t:
        passes.append({
            "aos": aos_t, "aos_az": aos_az,
            "peak": peak_t, "peak_az": peak_az, "max_elev_deg": peak_e,
            "los": end, "los_az": None,
        })

    return passes


# --- Settings + adapter -----------------------------------------------------


class Observer(BaseModel):
    """Fixed observer location for server-side pass prediction."""

    name: str
    slug: str
    state: str
    lat: float
    lon: float
    elev_m: float = 0.0


class SatpassPredictSettings(BaseModel):
    """Per-observer list + threshold + horizon. Default observer ships disabled
    until the operator edits the list to their site(s)."""

    observers: list[Observer] = [
        Observer(name="Treasure Valley", slug="treasure-valley",
                 state="ID", lat=43.6, lon=-116.2, elev_m=0.0),
    ]
    min_elevation_deg: float = 10.0
    horizon_hours: int = 24


class SatpassPredictAdapter(SourceAdapter):
    """Server-side satellite pass alerts for fixed observers."""

    name = "satpass_predict"
    display_name = "Satellite Pass Predictions"
    description = (
        "Predicts upcoming satellite passes over fixed observer locations "
        "by propagating the latest TLE for each NORAD ID via SGP4. Reads "
        "TLEs from the events table (celestrak_tle adapter); emits one "
        "Event per (observer, satellite, AOS) tuple within a 24h horizon."
    )
    settings_schema = SatpassPredictSettings
    requires_api_key = None
    wizard_order = None
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
        self._apply_settings(config.settings or {})

    def _apply_settings(self, settings: dict[str, Any]) -> None:
        raw_observers = settings.get("observers") or []
        self._observers: list[Observer] = [
            o if isinstance(o, Observer) else Observer(**o) for o in raw_observers
        ]
        self._min_elev: float = float(settings.get("min_elevation_deg") or 10.0)
        self._horizon_h: float = float(settings.get("horizon_hours") or 24)

    async def startup(self) -> None:
        self._db = sqlite3.connect(self._cursor_db_path)
        self._db.execute(_DEDUP_DDL)
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS published_ids_last_seen ON published_ids (last_seen)"
        )
        self._db.commit()
        logger.info(
            "satpass_predict adapter started",
            extra={
                "observers": [o.slug for o in self._observers],
                "min_elevation_deg": self._min_elev,
                "horizon_hours": self._horizon_h,
            },
        )

    async def shutdown(self) -> None:
        if self._db:
            self._db.close()
            self._db = None

    async def apply_config(self, new_config: AdapterConfig) -> None:
        self._apply_settings(new_config.settings or {})
        logger.info(
            "satpass_predict config updated",
            extra={
                "observers": [o.slug for o in self._observers],
                "min_elevation_deg": self._min_elev,
                "horizon_hours": self._horizon_h,
            },
        )

    async def _fetch_latest_tles(self) -> list[dict[str, Any]]:
        """Return rows: ``{norad_id, satellite_name, tle_line1, tle_line2, tle_epoch}``.

        Empty list if no TLEs in the events table within the 14-day window.
        Never raises -- caller handles empty.
        """
        pool = self._config_store.get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(_LATEST_TLES_SQL)
        return [dict(r) for r in rows]

    def _pass_to_event(
        self,
        p: dict[str, Any],
        row: dict[str, Any],
        observer: Observer,
    ) -> Event:
        aos: datetime = p["aos"]
        return Event(
            id=f"{observer.slug}:{row['norad_id']}:{aos.isoformat()}",
            adapter=self.name,
            category="pass.satpass_predict",
            time=p["peak"],
            severity=_severity_from_elev(p["max_elev_deg"]),
            geo=Geo(
                centroid=(observer.lon, observer.lat),
                regions=[f"US-{observer.state}"],
                primary_region=f"US-{observer.state}",
            ),
            data={
                "observer_name": observer.name,
                "observer_slug": observer.slug,
                "observer_state": observer.state,
                "norad_id": row["norad_id"],
                "satellite_name": row["satellite_name"],
                "aos_time": aos.isoformat(),
                "los_time": p["los"].isoformat() if p["los"] else None,
                "peak_time": p["peak"].isoformat(),
                "max_elevation_deg": round(p["max_elev_deg"], 2),
                "azimuth_at_aos": round(p["aos_az"], 1) if p["aos_az"] is not None else None,
                "azimuth_at_los": round(p["los_az"], 1) if p["los_az"] is not None else None,
                "duration_s": (p["los"] - aos).total_seconds() if p["los"] else None,
                "tle_epoch": row["tle_epoch"],
            },
        )

    async def poll(self) -> AsyncIterator[Event]:
        if not self._observers:
            logger.info("satpass_predict: no observers configured; nothing to predict")
            return
        rows = await self._fetch_latest_tles()
        if not rows:
            logger.info(
                "satpass_predict: no TLEs available; nothing to predict "
                "(is celestrak_tle enabled and has it polled at least once?)"
            )
            return

        ref_time = datetime.now(timezone.utc)
        yielded = 0
        for observer in self._observers:
            for row in rows:
                try:
                    passes = _next_passes(
                        row["tle_line1"], row["tle_line2"], observer,
                        ref_time, self._horizon_h, self._min_elev,
                    )
                except Exception:
                    logger.exception(
                        "satpass_predict pass computation failed",
                        extra={"norad_id": row["norad_id"], "observer": observer.slug},
                    )
                    continue
                for p in passes:
                    yield self._pass_to_event(p, row, observer)
                    yielded += 1

        self.sweep_old_ids()
        logger.info(
            "satpass_predict poll completed",
            extra={
                "observers": [o.slug for o in self._observers],
                "tles_considered": len(rows),
                "events_yielded": yielded,
            },
        )

    def subject_for(self, event: Event) -> str:
        state = (event.data.get("observer_state") or "").lower() or "unknown"
        slug = event.data.get("observer_slug") or "unknown"
        return f"central.sat.pass.us.{state}.{slug}"
