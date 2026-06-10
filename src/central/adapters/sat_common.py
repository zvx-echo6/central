"""Shared SGP4 / ECEF helpers for satellite adapters.

Extracted from satpass_predict.py in v0.12.0 when sat_positions needed the
same primitives. Moving them to a sibling module (rather than having
sat_positions cross-import from satpass_predict) keeps both adapters
peers of a common helper, matching the wfigs_common / swpc_common
precedent.

All helpers here are pure math: no I/O, no global state. Tests can pin
them to reference TLEs at known reference times.
"""

from __future__ import annotations

import math
from typing import Any

EARTH_RADIUS_KM = 6378.137


def gmst_rad(jd: float, fr: float) -> float:
    """Greenwich Mean Sidereal Time in radians (Vallado, simplified).

    Accurate to within milliseconds for any post-1900 epoch -- plenty for
    horizon/elevation and sub-satellite point work.
    """
    t = (jd + fr - 2451545.0) / 36525.0
    gmst_sec = (
        67310.54841
        + (876600.0 * 3600.0 + 8640184.812866) * t
        + 0.093104 * t * t
        - 6.2e-6 * t * t * t
    )
    return (gmst_sec % 86400.0) * (2.0 * math.pi / 86400.0)


def eci_to_ecef(
    pos_eci_km: tuple[float, float, float], theta: float,
) -> tuple[float, float, float]:
    """Rotate ECI coordinates to ECEF by GMST angle theta (radians)."""
    x, y, z = pos_eci_km
    ct = math.cos(theta)
    st = math.sin(theta)
    return (ct * x + st * y, -st * x + ct * y, z)


def subsatellite_point(
    pos_ecef_km: tuple[float, float, float],
) -> tuple[float, float, float]:
    """ECEF (km) -> ``(lon_deg, lat_deg, alt_km)``.

    Sub-satellite point is the ground location directly beneath the satellite
    on a spherical earth. Longitude normalised to [-180, 180]. Altitude is
    geocentric height above the equatorial radius (not WGS-84 height above
    ellipsoid -- close enough for footprint-radius and tracking-map work).
    """
    x, y, z = pos_ecef_km
    horizontal = math.sqrt(x * x + y * y)
    lat = math.degrees(math.atan2(z, horizontal))
    lon = math.degrees(math.atan2(y, x))
    if lon > 180.0:
        lon -= 360.0
    elif lon < -180.0:
        lon += 360.0
    alt = math.sqrt(x * x + y * y + z * z) - EARTH_RADIUS_KM
    return lon, lat, alt


def split_antimeridian(
    coords: list[tuple[float, float]],
) -> dict[str, Any] | None:
    """Split a (lon, lat) polyline at antimeridian (+/-180) crossings.

    Returns None if fewer than 2 vertices. Returns a GeoJSON LineString dict
    if no crossings (the common case). Returns a MultiLineString dict when
    one or more crossings exist; each crossing closes the current segment at
    sign(prev_lon)*180 with a linearly-interpolated latitude, then starts
    the next segment at sign(cur_lon)*180 with the same lat. Linear lon/lat
    interpolation has sub-0.1 degree error at LEO orbital speeds, well below
    Leaflet rendering precision.

    Crossing detection: ``abs(cur_lon - prev_lon) > 180``. The "short way"
    around the globe between two points is always <=180 degrees of longitude,
    so a larger jump only happens when the segment wraps across the dateline.
    """
    if len(coords) < 2:
        return None

    segments: list[list[list[float]]] = []
    current: list[list[float]] = [[float(coords[0][0]), float(coords[0][1])]]

    for i in range(1, len(coords)):
        prev_lon, prev_lat = float(coords[i - 1][0]), float(coords[i - 1][1])
        cur_lon, cur_lat = float(coords[i][0]), float(coords[i][1])
        if abs(cur_lon - prev_lon) > 180.0:
            close_lon = 180.0 if prev_lon >= 0 else -180.0
            start_lon = 180.0 if cur_lon >= 0 else -180.0
            # Fraction of the segment that lies on the "prev" side of the
            # antimeridian. Guard the denominator: both endpoints exactly on
            # the dateline (degenerate) -> just split at midpoint of lats.
            denom = (180.0 - abs(prev_lon)) + (180.0 - abs(cur_lon))
            if denom <= 0:
                interp_lat = (prev_lat + cur_lat) / 2.0
            else:
                frac = (180.0 - abs(prev_lon)) / denom
                interp_lat = prev_lat + frac * (cur_lat - prev_lat)
            current.append([close_lon, interp_lat])
            segments.append(current)
            current = [[start_lon, interp_lat], [cur_lon, cur_lat]]
        else:
            current.append([cur_lon, cur_lat])
    segments.append(current)

    if len(segments) == 1:
        return {"type": "LineString", "coordinates": segments[0]}
    return {"type": "MultiLineString", "coordinates": segments}
