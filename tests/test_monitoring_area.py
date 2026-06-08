"""Unit tests for the shared monitoring-area module (v0.10.2).

Covers the four exports:
  - ``MonitoringArea.as_box`` (shapely box construction)
  - ``build_geom_json`` (all five Geo shapes the archive sees)
  - ``classify_geom`` (the five verdicts)
  - ``load_monitoring_area`` (DB read; None on missing-row / NULL-column)

The classify_geom + as_box assertions are the v0.9.12 archive bbox tests
lifted out of ``test_archive_bbox_filter.py`` and expanded with edge cases.
"""

import json

import pytest
from unittest.mock import AsyncMock, MagicMock
from shapely.geometry import Polygon, box as shapely_box

from central.monitoring_area import (
    MonitoringArea,
    build_geom_json,
    classify_geom,
    load_monitoring_area,
)

IDAHO = MonitoringArea(north=44.5, south=41.8, east=-111.0, west=-117.5)


def _pt(lon, lat):
    return json.dumps({"type": "Point", "coordinates": [lon, lat]})


class TestMonitoringAreaAsBox:
    def test_as_box_returns_shapely_polygon(self):
        b = IDAHO.as_box()
        assert isinstance(b, Polygon)

    def test_as_box_corners_match_west_south_east_north(self):
        # shapely box(minx, miny, maxx, maxy) -> envelope (west, south, east, north)
        expected = shapely_box(-117.5, 41.8, -111.0, 44.5)
        assert IDAHO.as_box().equals(expected)


class TestBuildGeomJson:
    def test_none_input_returns_none(self):
        assert build_geom_json(None) is None

    def test_empty_dict_returns_none(self):
        assert build_geom_json({}) is None

    def test_full_geometry_wins_over_bbox(self):
        # If a real geometry is present it MUST be used verbatim (this is the
        # v0.9.8 wfigs/tomtom invariant -- the map needs the real shape).
        geom = {"type": "LineString", "coordinates": [[-114, 43], [-115, 44]]}
        out = build_geom_json({
            "geometry": geom,
            "bbox": [-115, 43, -114, 44],
            "centroid": [-114.5, 43.5],
        })
        assert json.loads(out) == geom

    def test_bbox_rendered_as_closed_polygon(self):
        out = build_geom_json({"bbox": [-117, 42, -111, 44]})
        parsed = json.loads(out)
        assert parsed["type"] == "Polygon"
        coords = parsed["coordinates"][0]
        # 5 points: 4 corners + closing duplicate
        assert len(coords) == 5
        assert coords[0] == coords[-1]
        assert coords[0] == [-117, 42]

    def test_centroid_rendered_as_point(self):
        out = build_geom_json({"centroid": [-114.5, 43.5]})
        assert json.loads(out) == {"type": "Point", "coordinates": [-114.5, 43.5]}

    def test_partial_bbox_falls_through(self):
        # An invalid 3-element bbox should not produce a 3-corner polygon;
        # caller is expected to fall through to centroid or return None.
        assert build_geom_json({"bbox": [-117, 42, -111]}) is None

    def test_centroid_wins_when_bbox_invalid(self):
        out = build_geom_json({"bbox": [-117, 42, -111], "centroid": [-114, 43]})
        assert json.loads(out) == {"type": "Point", "coordinates": [-114, 43]}


class TestClassifyGeom:
    def test_null_geom_always_kept(self):
        assert classify_geom(None, IDAHO) == "null-geom"

    def test_null_geom_kept_even_without_area(self):
        assert classify_geom(None, None) == "null-geom"

    def test_no_area_keeps_everything(self):
        assert classify_geom(_pt(-114.0, 43.5), None) == "no-area"

    def test_in_bounds_kept(self):
        assert classify_geom(_pt(-114.0, 43.5), IDAHO) == "in-bounds"

    def test_out_of_bounds_dropped(self):
        assert classify_geom(_pt(-74.0, 40.7), IDAHO) == "out-of-bounds"

    def test_border_straddling_polygon_kept(self):
        # Spans the western edge (west=-117.5): partly out, partly in -> kept.
        poly = json.dumps({
            "type": "Polygon",
            "coordinates": [[[-119, 42], [-116, 42], [-116, 43], [-119, 43], [-119, 42]]],
        })
        assert classify_geom(poly, IDAHO) == "in-bounds"

    def test_point_exactly_on_border_kept(self):
        assert classify_geom(_pt(-117.5, 43.0), IDAHO) == "in-bounds"

    def test_unparseable_geom_keeps_failopen(self):
        assert classify_geom("{not valid json", IDAHO) == "invalid-geom"

    def test_unknown_geom_type_keeps_failopen(self):
        assert classify_geom(json.dumps({"type": "Nonsense"}), IDAHO) == "invalid-geom"


@pytest.mark.asyncio
class TestLoadMonitoringArea:
    async def test_returns_area_when_all_columns_set(self):
        conn = MagicMock()
        conn.fetchrow = AsyncMock(return_value={
            "monitor_north": 44.5, "monitor_south": 41.8,
            "monitor_east": -111.0, "monitor_west": -117.5,
        })
        area = await load_monitoring_area(conn)
        assert area == IDAHO

    async def test_returns_none_when_no_row(self):
        conn = MagicMock()
        conn.fetchrow = AsyncMock(return_value=None)
        assert await load_monitoring_area(conn) is None

    async def test_returns_none_when_any_column_null(self):
        conn = MagicMock()
        conn.fetchrow = AsyncMock(return_value={
            "monitor_north": 44.5, "monitor_south": None,
            "monitor_east": -111.0, "monitor_west": -117.5,
        })
        assert await load_monitoring_area(conn) is None


class TestDefaultMonitoringAreaCoversIdaho:
    """v0.10.9 regression guard for migration 034 + the routes._DEFAULT_MONITOR fallback.

    v0.10.2 shipped with the default monitor_north = 44.5, which only covered the
    southern third of Idaho -- the supervisor's publish-time bbox filter was
    silently dropping ~56 itd_511 events per 60-second poll (the entire northern
    panhandle: Coeur d'Alene, Lewiston, Sandpoint, Moscow, McCall). NWS Idaho
    UGC-zone alerts in the panhandle were similarly dropped (v0.10.7 followup
    ticket (b)). Idaho actually extends to 49.0N. Migration 034 widens the
    default to cover the full state.

    If anything reverts the default to a value that excludes any Idaho city,
    this test fails loudly.
    """

    # (city, lat, lon) -- spread across Idaho to catch any narrowing on any side.
    IDAHO_SENTINEL_CITIES = [
        ("Coeur d'Alene (panhandle, north)",   47.6777, -116.7805),
        ("Sandpoint (panhandle, far north)",   48.2766, -116.5534),
        ("Lewiston (north-central)",           46.4165, -117.0177),
        ("Moscow (panhandle, west)",           46.7324, -116.9999),
        ("McCall (central-north)",             44.9111, -116.0998),
        ("Boise (south-west)",                 43.6150, -116.2023),
        ("Idaho Falls (south-east)",           43.4917, -112.0339),
        ("Twin Falls (south-central)",         42.5629, -114.4609),
        ("Pocatello (south-east)",             42.8713, -112.4455),
    ]

    def test_routes_default_monitor_covers_full_idaho_extent(self):
        """routes._DEFAULT_MONITOR must contain every Idaho sentinel city."""
        from central.gui.routes import _DEFAULT_MONITOR
        default = MonitoringArea(
            north=_DEFAULT_MONITOR["north"],
            south=_DEFAULT_MONITOR["south"],
            east=_DEFAULT_MONITOR["east"],
            west=_DEFAULT_MONITOR["west"],
        )
        for label, lat, lon in self.IDAHO_SENTINEL_CITIES:
            assert classify_geom(_pt(lon, lat), default) == "in-bounds", (
                f"_DEFAULT_MONITOR rejected {label} at ({lat}, {lon}) -- "
                f"check that monitor_north is at least 49.0"
            )

    def test_routes_default_monitor_bounds_match_idaho_extent(self):
        """Belt-and-suspenders: assert the exact corner values rather than
        just inferring from the city checks. v0.10.9 set the default to
        (49.0, 41.8, -111.0, -117.5) -- the bounding box of Idaho."""
        from central.gui.routes import _DEFAULT_MONITOR
        assert _DEFAULT_MONITOR["north"] >= 49.0
        assert _DEFAULT_MONITOR["south"] <= 41.8
        assert _DEFAULT_MONITOR["east"] >= -111.0
        assert _DEFAULT_MONITOR["west"] <= -117.5

    def test_v0_10_2_narrow_default_would_reject_panhandle(self):
        """Anti-regression: the OLD v0.10.2 north=44.5 default would have
        rejected the panhandle. This isn't testing current behavior -- it's
        documenting what we fixed, and ensuring nothing reintroduces a
        narrow north bound silently."""
        narrow = MonitoringArea(north=44.5, south=41.8, east=-111.0, west=-117.5)
        panhandle_cities = [c for c in self.IDAHO_SENTINEL_CITIES if c[1] > 44.5]
        assert len(panhandle_cities) >= 4, "expected at least 4 north-of-44.5 sentinels"
        for label, lat, lon in panhandle_cities:
            assert classify_geom(_pt(lon, lat), narrow) == "out-of-bounds", label
