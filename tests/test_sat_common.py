"""Tests for the shared satellite-math helpers extracted in v0.12.0.

These pin the public API surface (no leading underscores) and the numerical
behavior at known reference points. They duplicate some property tests from
test_satpass_predict.py by design -- those test the helpers via internal
re-exports (aliased imports), while these test the module's published
interface directly. If the public names ever drift or get renamed, these
fail first.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone

import pytest
from sgp4.api import Satrec, jday

from central.adapters.sat_common import (
    EARTH_RADIUS_KM,
    eci_to_ecef,
    gmst_rad,
    split_antimeridian,
    subsatellite_point,
)


# Live TLE from the v0.11.0 stations fixture, ISS (NORAD 25544).
_ISS_L1 = "1 25544U 98067A   26159.80410962  .00007129  00000+0  13425-3 0  9999"
_ISS_L2 = "2 25544  51.6336 341.5878 0006923 148.5365 211.6039 15.49672912570453"
_REF = datetime(2026, 6, 9, 7, 0, 0, tzinfo=timezone.utc)


class TestEarthRadius:
    def test_value_matches_wgs84_equatorial(self):
        assert EARTH_RADIUS_KM == pytest.approx(6378.137, abs=1e-6)


class TestGmstRad:
    def test_returns_value_in_canonical_range(self):
        val = gmst_rad(2460835.0, 0.5)  # arbitrary post-2000 JD
        assert 0.0 <= val < 2.0 * math.pi

    def test_monotonic_within_a_day(self):
        """GMST advances ~2π per sidereal day. Two samples 6h apart must
        differ by roughly π/2 (modulo wraparound)."""
        v0 = gmst_rad(2460835.0, 0.0)
        v1 = gmst_rad(2460835.0, 0.25)
        delta = (v1 - v0) % (2.0 * math.pi)
        # 6h sidereal angle is slightly more than π/2 (sidereal day < solar day).
        assert math.pi / 2.0 < delta < math.pi / 2.0 + 0.02


class TestEciToEcef:
    def test_zero_rotation_is_identity(self):
        result = eci_to_ecef((100.0, 200.0, 300.0), 0.0)
        assert result == pytest.approx((100.0, 200.0, 300.0))

    def test_rotation_preserves_magnitude(self):
        """Rotation about the z-axis preserves the vector norm."""
        pos = (3000.0, 4000.0, 5000.0)
        rot = eci_to_ecef(pos, math.pi / 3.0)
        mag_in = math.sqrt(sum(c * c for c in pos))
        mag_out = math.sqrt(sum(c * c for c in rot))
        assert mag_out == pytest.approx(mag_in, rel=1e-12)

    def test_z_component_unaffected(self):
        """Earth-rotation axis is z; z component never changes under GMST rotation."""
        _, _, z = eci_to_ecef((1.0, 2.0, 42.0), 1.3)
        assert z == 42.0


class TestSubsatellitePoint:
    def test_north_pole_returns_pole(self):
        lon, lat, alt = subsatellite_point((0.0, 0.0, 7000.0))
        assert lat == pytest.approx(90.0)
        assert alt == pytest.approx(7000.0 - EARTH_RADIUS_KM)

    def test_equator_lon_zero(self):
        lon, lat, alt = subsatellite_point((EARTH_RADIUS_KM + 400.0, 0.0, 0.0))
        assert lon == pytest.approx(0.0)
        assert lat == pytest.approx(0.0)
        assert alt == pytest.approx(400.0, abs=1e-6)

    def test_equator_lon_90_east(self):
        lon, lat, alt = subsatellite_point((0.0, EARTH_RADIUS_KM + 400.0, 0.0))
        assert lon == pytest.approx(90.0)
        assert lat == pytest.approx(0.0)

    def test_lon_normalised_into_180_range(self):
        """A satellite over the antimeridian (-y axis) reads as -90°, never +270°."""
        lon, _, _ = subsatellite_point((0.0, -(EARTH_RADIUS_KM + 400.0), 0.0))
        assert -180.0 <= lon <= 180.0
        assert lon == pytest.approx(-90.0)


class TestSplitAntimeridian:
    """v0.13.0 splitter. Returns None for <2 vertices; LineString for tracks
    with no crossing; MultiLineString when crossings exist. Each crossing
    closes the current segment at sign(prev)*180 with linearly-interpolated
    lat and starts the next at sign(cur)*180 with the same lat."""

    def test_none_for_empty(self):
        assert split_antimeridian([]) is None

    def test_none_for_single_vertex(self):
        assert split_antimeridian([(0.0, 0.0)]) is None

    def test_linestring_for_no_crossing(self):
        result = split_antimeridian([(0.0, 0.0), (10.0, 5.0), (20.0, 0.0)])
        assert result["type"] == "LineString"
        assert len(result["coordinates"]) == 3

    def test_multilinestring_for_eastward_crossing(self):
        """+170 -> +179 -> -179 -> -170 crosses +180 once."""
        result = split_antimeridian([
            (170.0, 0.0), (179.0, 0.0), (-179.0, 0.0), (-170.0, 0.0),
        ])
        assert result["type"] == "MultiLineString"
        assert len(result["coordinates"]) == 2
        # First segment closes at +180
        assert result["coordinates"][0][-1] == [180.0, 0.0]
        # Second segment starts at -180
        assert result["coordinates"][1][0] == [-180.0, 0.0]

    def test_multilinestring_for_westward_crossing(self):
        """-170 -> -179 -> +179 -> +170 crosses -180 once."""
        result = split_antimeridian([
            (-170.0, 0.0), (-179.0, 0.0), (179.0, 0.0), (170.0, 0.0),
        ])
        assert result["type"] == "MultiLineString"
        assert len(result["coordinates"]) == 2
        # First segment closes at -180; second starts at +180.
        assert result["coordinates"][0][-1] == [-180.0, 0.0]
        assert result["coordinates"][1][0] == [180.0, 0.0]

    def test_two_crossings_produce_three_segments(self):
        """Polar-orbit-like sequence crossing the dateline twice in 6 vertices."""
        result = split_antimeridian([
            (170.0, 50.0), (179.0, 60.0), (-179.0, 70.0),
            (-179.0, -70.0), (179.0, -60.0), (170.0, -50.0),
        ])
        assert result["type"] == "MultiLineString"
        assert len(result["coordinates"]) == 3

    def test_interpolated_lat_at_crossing(self):
        """Lat interpolates linearly between pre- and post-crossing vertices.
        +179 lat=0 -> -179 lat=10 should put the +/-180 vertex at lat=5."""
        result = split_antimeridian([(179.0, 0.0), (-179.0, 10.0)])
        assert result["type"] == "MultiLineString"
        # Crossing point lat is 5.0
        assert result["coordinates"][0][-1] == [180.0, 5.0]
        assert result["coordinates"][1][0] == [-180.0, 5.0]


class TestIssRoundTripViaSgp4:
    """End-to-end: TLE -> SGP4 ECI -> ECEF -> sub-sat point. Pins the math
    against a known orbital configuration. Drift from this would mean the
    helpers regressed in a way that affects production output."""

    def test_iss_sub_sat_point_at_pinned_ref_time(self):
        sat = Satrec.twoline2rv(_ISS_L1, _ISS_L2)
        jd, fr = jday(_REF.year, _REF.month, _REF.day,
                      _REF.hour, _REF.minute, _REF.second)
        err, pos_eci, _ = sat.sgp4(jd, fr)
        assert err == 0
        pos_ecef = eci_to_ecef(pos_eci, gmst_rad(jd, fr))
        lon, lat, alt = subsatellite_point(pos_ecef)
        # ISS inclination 51.6° -- lat must lie within bounds
        assert -52.0 <= lat <= 52.0
        # lon in valid range
        assert -180.0 <= lon <= 180.0
        # ISS altitude ~400-420 km
        assert 380.0 <= alt <= 460.0
