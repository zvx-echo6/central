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
from central.adapters.sat_common import (
    EARTH_RADIUS_KM,
    eci_to_ecef,
    gmst_rad,
    subsatellite_point,
)
from central.config_models import AdapterConfig
from central.config_store import ConfigStore
from central.models import Event, Geo

logger = logging.getLogger(__name__)

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


def _observer_ecef(lat_deg: float, lon_deg: float, elev_m: float) -> tuple[float, float, float]:
    """Observer position in ECEF km (spherical earth, sub-0.1° precision)."""
    lat_r = math.radians(lat_deg)
    lon_r = math.radians(lon_deg)
    r = EARTH_RADIUS_KM + elev_m / 1000.0
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


def _sample_at(
    sat: Satrec,
    t: datetime,
    obs_ecef_km: tuple[float, float, float],
    obs_lat_deg: float,
    obs_lon_deg: float,
) -> tuple[float, float, tuple[float, float, float]] | None:
    """Sample SGP4 at instant ``t`` and return ``(azimuth_deg, elevation_deg, sat_ecef_km)``.

    Single sgp4 call per sample -- the satellite ECEF position is returned so
    the caller can also compute the sub-satellite point without a second
    propagation. Returns ``None`` if SGP4 reports a propagation error.
    """
    jd, fr = jday(t.year, t.month, t.day, t.hour, t.minute, t.second + t.microsecond / 1e6)
    err, pos_eci, _ = sat.sgp4(jd, fr)
    if err:
        return None
    sat_ecef = eci_to_ecef(pos_eci, gmst_rad(jd, fr))
    az, el = _topocentric_az_el(sat_ecef, obs_ecef_km, obs_lat_deg, obs_lon_deg)
    return az, el, sat_ecef


def _elev_at(
    sat: Satrec,
    t: datetime,
    obs_ecef_km: tuple[float, float, float],
    obs_lat_deg: float,
    obs_lon_deg: float,
) -> tuple[float, float] | None:
    """Back-compat wrapper retained for v0.11.1 tests that expected ``(az, el)``.

    Prefer ``_sample_at`` in new code so the ECEF position is available for
    sub-satellite point computation without a second propagation.
    """
    sample = _sample_at(sat, t, obs_ecef_km, obs_lat_deg, obs_lon_deg)
    return None if sample is None else (sample[0], sample[1])


def _visibility_footprint(
    lon_deg: float, lat_deg: float, alt_km: float, n_vertices: int = 32,
) -> dict[str, Any] | None:
    """Geodesic circle visible from a satellite at altitude ``alt_km``.

    Radius is the horizon distance on a spherical earth:
    ``r = R * acos(R / (R + alt))``. ISS at 408km -> ~2253km; GEO at 35786km
    -> ~9055km. Returns a GeoJSON Polygon (single closed ring of
    ``n_vertices + 1`` ``[lon, lat]`` pairs); ``None`` if ``alt_km <= 0``
    (decayed orbit / parse error).

    Antimeridian behaviour: the longitude of each vertex is normalised to
    [-180, 180] independently. For sub-satellite points well clear of the
    dateline (which includes all Idaho-overhead passes for ISS-class
    altitudes) the resulting polygon is well-formed in Leaflet. Polar-orbit
    crossings near ±180° will produce a polygon that visually wraps the
    "wrong way" around the globe -- documented limitation, not handled.
    """
    if alt_km <= 0:
        return None
    r_earth = EARTH_RADIUS_KM
    radius_km = r_earth * math.acos(r_earth / (r_earth + alt_km))
    angular = radius_km / r_earth
    lat1 = math.radians(lat_deg)
    lon1 = math.radians(lon_deg)
    sin_lat1, cos_lat1 = math.sin(lat1), math.cos(lat1)
    sin_d, cos_d = math.sin(angular), math.cos(angular)

    ring: list[list[float]] = []
    for i in range(n_vertices):
        bearing = 2.0 * math.pi * i / n_vertices
        lat2 = math.asin(sin_lat1 * cos_d + cos_lat1 * sin_d * math.cos(bearing))
        lon2 = lon1 + math.atan2(
            math.sin(bearing) * sin_d * cos_lat1,
            cos_d - sin_lat1 * math.sin(lat2),
        )
        lon2_deg = math.degrees(lon2)
        # Wrap each vertex into [-180, 180].
        lon2_deg = ((lon2_deg + 180.0) % 360.0) - 180.0
        ring.append([lon2_deg, math.degrees(lat2)])
    ring.append(ring[0])  # close
    return {"type": "Polygon", "coordinates": [ring]}


def _build_pass_geometry(p: dict[str, Any]) -> dict[str, Any] | None:
    """Assemble the GeoJSON GeometryCollection for one pass (v0.11.2).

    Combines the ground-track LineString (AOS -> LOS) and the visibility-
    footprint Polygon at peak time. Returns ``None`` if neither is buildable
    (degenerate pass, propagation error, etc.) so the caller can omit
    ``geo.geometry`` entirely rather than write a ``{"type": ..., ...}``
    placeholder.
    """
    geometries: list[dict[str, Any]] = []
    track = p.get("ground_track") or []
    if len(track) >= 2:
        geometries.append({
            "type": "LineString",
            "coordinates": [[lon, lat] for lon, lat in track],
        })
    peak_subsat = p.get("peak_subsat")
    if peak_subsat:
        lon, lat, alt = peak_subsat
        footprint = _visibility_footprint(lon, lat, alt)
        if footprint:
            geometries.append(footprint)
    if not geometries:
        return None
    return {"type": "GeometryCollection", "geometries": geometries}


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
    """Walk a 60-second grid; return all passes >= min_elevation_deg in window.

    Each returned dict now (v0.11.2) also carries:
      ``peak_subsat``: ``(lon_deg, lat_deg, alt_km)`` at the moment of peak
        elevation, for visibility-footprint construction.
      ``ground_track``: list of ``(lon_deg, lat_deg)`` sub-satellite points
        sampled at the same grid from AOS through LOS, for the
        ground-track polyline.
    """
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
    peak_subsat: tuple[float, float, float] | None = None
    track: list[tuple[float, float]] = []

    t = ref_time
    end = ref_time + timedelta(hours=horizon_hours)
    step = timedelta(seconds=_PASS_STEP_S)
    while t < end:
        sample = _sample_at(sat, t, obs_ecef, observer.lat, observer.lon)
        if sample is None:
            t += step
            continue
        az, e, sat_ecef = sample
        subsat = subsatellite_point(sat_ecef)  # (lon, lat, alt)
        if e >= min_elevation_deg:
            if not in_pass:
                in_pass = True
                aos_t = t
                aos_az = az
                peak_t = t
                peak_e = e
                peak_az = az
                peak_subsat = subsat
                track = [(subsat[0], subsat[1])]
            else:
                track.append((subsat[0], subsat[1]))
                if e > peak_e:
                    peak_t = t
                    peak_e = e
                    peak_az = az
                    peak_subsat = subsat
        elif in_pass:
            # threshold-crossing on the way down -> close the pass; include
            # the descending boundary point in the track so the polyline
            # ends at LOS rather than at the last sample above min_elev.
            track.append((subsat[0], subsat[1]))
            passes.append({
                "aos": aos_t, "aos_az": aos_az,
                "peak": peak_t, "peak_az": peak_az, "max_elev_deg": peak_e,
                "los": t, "los_az": az,
                "peak_subsat": peak_subsat,
                "ground_track": list(track),
            })
            in_pass = False
            aos_t = aos_az = peak_t = peak_az = None
            peak_e = -180.0
            peak_subsat = None
            track = []
        t += step

    # Pass still in progress at the horizon edge -- close it at the boundary
    # (los_az=None signals the pass extended beyond the horizon).
    if in_pass and aos_t and peak_t:
        passes.append({
            "aos": aos_t, "aos_az": aos_az,
            "peak": peak_t, "peak_az": peak_az, "max_elev_deg": peak_e,
            "los": end, "los_az": None,
            "peak_subsat": peak_subsat,
            "ground_track": list(track),
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
        # v0.11.2: enrich geo.geometry with a GeometryCollection so the
        # events-list map (Leaflet L.geoJSON handles GeometryCollection
        # natively) renders BOTH the ground-track LineString from AOS->LOS
        # AND the visibility-footprint Polygon at peak time. centroid stays
        # at the observer point -- the alert is logically AT the observer,
        # the added geometry is supplementary spatial context.
        geometry = _build_pass_geometry(p)
        return Event(
            id=f"{observer.slug}:{row['norad_id']}:{aos.isoformat()}",
            adapter=self.name,
            category="pass.satpass_predict",
            time=p["peak"],
            severity=_severity_from_elev(p["max_elev_deg"]),
            geo=Geo(
                centroid=(observer.lon, observer.lat),
                geometry=geometry,
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
