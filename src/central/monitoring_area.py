"""System monitoring-area bbox: shared geometry/classification helpers.

Originally lived inside ``central.archive`` (v0.9.12); lifted here in v0.10.2 so
the supervisor can enforce the same bbox at publish time and NATS subscribers
(meshai, Navi) stop seeing out-of-area events. Archive keeps using it as the
defense-in-depth layer between NATS and Postgres.

Behavior MUST stay byte-identical to archive's v0.9.12-v0.10.1 implementation:
``intersects()`` semantics (border-straddlers kept), fail-open on parse failure,
all-NULL column row -> ``None`` area (filter no-ops).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

import asyncpg
from shapely.geometry import box as _shapely_box
from shapely.geometry import shape

logger = logging.getLogger(__name__)

# How often archive / supervisor re-read the monitoring-area bbox from
# config.system so GUI edits propagate without a service restart.
MONITORING_AREA_REFRESH_S = 60


@dataclass(frozen=True)
class MonitoringArea:
    """System-level bounding box that events must intersect to be archived/published."""

    north: float
    south: float
    east: float
    west: float

    def as_box(self):
        # shapely box(minx, miny, maxx, maxy) -> (west, south, east, north)
        return _shapely_box(self.west, self.south, self.east, self.north)


def build_geom_json(geo_data: dict[str, Any] | None) -> str | None:
    """Build a GeoJSON string from an event's ``Geo`` dict.

    Priority matches archive's v0.9.8+ rule: a full ``geometry`` dict wins over
    ``bbox`` (rendered as a 4-corner Polygon) over ``centroid`` (rendered as a
    Point). Returns ``None`` if no usable shape is present.
    """
    if not geo_data:
        return None

    geometry = geo_data.get("geometry")
    if geometry:
        return json.dumps(geometry)

    bbox = geo_data.get("bbox")
    centroid = geo_data.get("centroid")

    if bbox and len(bbox) == 4:
        min_lon, min_lat, max_lon, max_lat = bbox
        return json.dumps({
            "type": "Polygon",
            "coordinates": [[
                [min_lon, min_lat],
                [max_lon, min_lat],
                [max_lon, max_lat],
                [min_lon, max_lat],
                [min_lon, min_lat],
            ]]
        })
    if centroid and len(centroid) == 2:
        return json.dumps({
            "type": "Point",
            "coordinates": centroid,
        })

    return None


def classify_geom(geom_json: str | None, area: MonitoringArea | None) -> str:
    """Classify a GeoJSON string against the monitoring area.

    Returns one of:
      'null-geom'     -- no geometry; always kept (SWPC trio, .removed tombstones)
      'no-area'       -- no monitoring area configured; keep everything
      'in-bounds'     -- geometry intersects the area; keep
      'out-of-bounds' -- geometry lies entirely outside the area; drop
      'invalid-geom'  -- geometry could not be evaluated; kept (fail-open) + warn

    Uses ``intersects()`` so border-straddlers and points-on-edge are kept
    (matches PostGIS ``ST_Intersects``). The filter must never drop an event
    because of a parse failure -- when in doubt, keep it.
    """
    if geom_json is None:
        return "null-geom"
    if area is None:
        return "no-area"
    try:
        geom = shape(json.loads(geom_json))
        return "in-bounds" if geom.intersects(area.as_box()) else "out-of-bounds"
    except Exception:
        return "invalid-geom"


async def load_monitoring_area(conn: asyncpg.Connection) -> MonitoringArea | None:
    """Read the system monitoring area from ``config.system``.

    Returns ``None`` when:
      - no row exists (``id = true`` not set), or
      - any of monitor_{north,south,east,west} is NULL.

    Callers are expected to handle their own pool acquisition AND any exception
    (archive's pattern: keep the last-known value on read failure and warn).
    """
    row = await conn.fetchrow(
        "SELECT monitor_north, monitor_south, monitor_east, monitor_west "
        "FROM config.system WHERE id = true"
    )
    cols = ("monitor_north", "monitor_south", "monitor_east", "monitor_west")
    if row and all(row[c] is not None for c in cols):
        return MonitoringArea(
            north=row["monitor_north"],
            south=row["monitor_south"],
            east=row["monitor_east"],
            west=row["monitor_west"],
        )
    return None
