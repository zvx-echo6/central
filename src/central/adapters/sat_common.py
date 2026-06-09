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
