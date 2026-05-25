"""Decode TomTom Orbis vector flow tiles into per-segment telemetry Events.

Shared by the ``tomtom_flow`` polling adapter and (v0.9.4) the on-demand
passthrough route. A vector flow tile's ``"Traffic flow"`` layer carries one
feature per road segment with ``relative_speed`` (0-1 current/free-flow ratio)
and ``road_category``, in tile-local MVT coordinates (extent 4096, y-up). We
georeference each vertex to lon/lat via the slippy-tile + Web-Mercator inverse
and emit one telemetry Event per segment, carrying the LineString geometry (so
the map draws a colored polyline) and a severity derived from relative_speed.
"""

import math
from datetime import datetime
from typing import Any

import mapbox_vector_tile

from central.models import Event, Geo

FLOW_LAYER = "Traffic flow"
ADAPTER_NAME = "tomtom_flow"


def severity_from_relative_speed(rs: float | None) -> int:
    """relative_speed (0-1, current/free-flow) -> severity; lower speed = worse."""
    if rs is None:
        return 1
    if rs >= 0.75:
        return 1
    if rs >= 0.5:
        return 2
    if rs >= 0.25:
        return 3
    return 4


def _merc_y_to_lat(t: float) -> float:
    """Normalized web-mercator row (0=north world edge .. 1=south) -> latitude deg."""
    return math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * t))))


def _local_to_lonlat(lx: float, ly: float, z: int, x: int, y: int, extent: int) -> list[float]:
    """MVT tile-local (lx, ly) [y-up, 0..extent] -> [lon, lat] degrees."""
    n = 2 ** z
    lon_left = x / n * 360.0 - 180.0
    lon_right = (x + 1) / n * 360.0 - 180.0
    fx = lx / extent
    fy = ly / extent  # mapbox_vector_tile default orientation is y-up (0 = tile bottom)
    lon = lon_left + fx * (lon_right - lon_left)
    lat = _merc_y_to_lat((y + (1 - fy)) / n)  # fy=1 (top)->y/n ; fy=0 (bottom)->(y+1)/n
    return [round(lon, 6), round(lat, 6)]


def _transform_coords(coords: Any, z: int, x: int, y: int, extent: int) -> Any:
    """Recursively georeference nested MVT coordinate arrays to lon/lat."""
    if coords and isinstance(coords[0], (int, float)):
        return _local_to_lonlat(coords[0], coords[1], z, x, y, extent)
    return [_transform_coords(c, z, x, y, extent) for c in coords]


def _midpoint(coordinates: Any) -> tuple[float, float] | None:
    """Mean (lon, lat) over all vertices — the clustering centroid."""
    pts: list[list[float]] = []

    def walk(c: Any) -> None:
        if c and isinstance(c[0], (int, float)):
            pts.append(c)
        else:
            for sub in c:
                walk(sub)

    walk(coordinates or [])
    if not pts:
        return None
    return (sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts))


def decode_flow_tile(pbf: bytes, z: int, x: int, y: int, fetched_at: datetime) -> list[Event]:
    """Decode one Orbis vector flow tile into per-segment telemetry Events."""
    decoded = mapbox_vector_tile.decode(pbf)
    layer = decoded.get(FLOW_LAYER)
    if not layer:
        return []
    extent = layer.get("extent", 4096)
    minute = fetched_at.strftime("%Y-%m-%dT%H:%M")
    events: list[Event] = []
    for idx, feat in enumerate(layer.get("features", [])):
        props = feat.get("properties") or {}
        geom = feat.get("geometry") or {}
        coords = _transform_coords(geom.get("coordinates") or [], z, x, y, extent)
        gj = {"type": geom.get("type"), "coordinates": coords}
        rs = props.get("relative_speed")
        events.append(
            Event(
                id=f"{z}/{x}/{y}:{idx}:{minute}",
                adapter=ADAPTER_NAME,
                category="flow.tomtom_flow",
                time=fetched_at,
                severity=severity_from_relative_speed(rs),
                geo=Geo(centroid=_midpoint(coords), geometry=gj, regions=[], primary_region=None),
                data={
                    "relative_speed": rs,
                    "road_category": props.get("road_category"),
                    "tile_z": z,
                    "tile_x": x,
                    "tile_y": y,
                    "segment_index": idx,
                    "tier": "orbis",
                    "fetched_at": fetched_at.isoformat(),
                },
            )
        )
    return events
