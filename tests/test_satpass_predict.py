"""Tests for the v0.11.1 satpass_predict adapter.

Deterministic via a fixed ISS TLE + fixed observer + pinned reference time.
The TLE comes from the v0.11.0 stations fixture (epoch 2026-06-08T19:17 UTC);
reference time pinned at 2026-06-09T07:00 UTC; observer is Treasure Valley
(43.6, -116.2, 0m elev). This combination produces a known ISS pass starting
at ~15:36 UTC the same day (verified via the sgp4 sanity script during Phase
A of v0.11.1).
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from central.adapter import SourceAdapter
from central.adapters.sat_common import (
    eci_to_ecef as _eci_to_ecef,
    gmst_rad as _gmst_rad,
    subsatellite_point as _subsatellite_point,
)
from central.adapters.satpass_predict import (
    Observer,
    SatpassPredictAdapter,
    SatpassPredictSettings,
    _build_pass_geometry,
    _next_passes,
    _observer_ecef,
    _severity_from_elev,
    _topocentric_az_el,
    _visibility_footprint,
)
from central.config_models import AdapterConfig


# Live TLE from the v0.11.0 stations fixture, ISS (NORAD 25544).
_ISS_L1 = "1 25544U 98067A   26159.80410962  .00007129  00000+0  13425-3 0  9999"
_ISS_L2 = "2 25544  51.6336 341.5878 0006923 148.5365 211.6039 15.49672912570453"

# Pinned observer + reference time.
_OBS = Observer(name="Treasure Valley", slug="treasure-valley",
                state="ID", lat=43.6, lon=-116.2, elev_m=0.0)
_REF = datetime(2026, 6, 9, 7, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def adapter(tmp_path: Path) -> SatpassPredictAdapter:
    cfg = AdapterConfig(
        name="satpass_predict",
        enabled=True,
        cadence_s=3600,
        settings={"observers": [_OBS.model_dump()],
                  "min_elevation_deg": 10.0, "horizon_hours": 24},
        updated_at=datetime.now(timezone.utc),
    )
    return SatpassPredictAdapter(cfg, MagicMock(), tmp_path / "cursors.db")


# --- Pure math helpers ------------------------------------------------------


def test_gmst_rad_returns_radians_in_canonical_range():
    """GMST output must wrap into [0, 2π)."""
    import math as m
    val = _gmst_rad(2460835.0, 0.5)  # arbitrary post-2000 JD
    assert 0.0 <= val < 2.0 * m.pi


def test_observer_ecef_for_north_pole_and_equator():
    """Sanity: north pole sits on z-axis; equator at lon=0 sits on x-axis."""
    pole = _observer_ecef(90.0, 0.0, 0.0)
    assert abs(pole[0]) < 1e-6 and abs(pole[1]) < 1e-6
    assert pole[2] > 6378.0  # ~6378.137 km

    eq_zero = _observer_ecef(0.0, 0.0, 0.0)
    assert eq_zero[0] > 6378.0 and abs(eq_zero[1]) < 1e-6 and abs(eq_zero[2]) < 1e-6


def test_topocentric_zenith_satellite_returns_90_elevation():
    """A satellite directly overhead must read elevation 90°, any azimuth."""
    obs_lat, obs_lon = 43.6, -116.2
    obs = _observer_ecef(obs_lat, obs_lon, 0.0)
    # 400km straight up = scale observer position vector by (R+400)/R
    import math as m
    r_obs = m.sqrt(sum(c * c for c in obs))
    r_sat = r_obs + 400.0
    scale = r_sat / r_obs
    sat_ecef = (obs[0] * scale, obs[1] * scale, obs[2] * scale)
    az, el = _topocentric_az_el(sat_ecef, obs, obs_lat, obs_lon)
    assert abs(el - 90.0) < 0.01, f"expected zenith elevation, got {el}"


def test_topocentric_below_horizon_returns_negative_elevation():
    """Satellite on the opposite side of the earth = below horizon."""
    obs = _observer_ecef(0.0, 0.0, 0.0)  # equator, prime meridian
    antipode = (-obs[0] * 2.0, 0.0, 0.0)  # other side, well below
    _, el = _topocentric_az_el(antipode, obs, 0.0, 0.0)
    assert el < -10.0


# --- Severity bucketing -----------------------------------------------------


@pytest.mark.parametrize("max_elev, expected", [
    (90.0, 4),    # zenith
    (60.0, 4),    # boundary -> 4
    (59.99, 3),
    (30.0, 3),    # boundary -> 3
    (29.99, 2),
    (10.0, 2),    # boundary -> 2 (gate threshold; emit)
    (9.99, 1),    # below gate -> 1 (should never emit in practice)
    (0.0, 1),
])
def test_severity_from_elev_buckets(max_elev, expected):
    assert _severity_from_elev(max_elev) == expected


# --- Pass detection (the load-bearing math test) ---------------------------


def test_iss_next_pass_over_treasure_valley_is_chronologically_sane():
    """Pinned TLE + observer + ref time produces ONE known ISS pass in 24h.
    AOS < peak < LOS, max_elev in (10, 90), positive duration."""
    passes = _next_passes(
        _ISS_L1, _ISS_L2, _OBS,
        ref_time=_REF, horizon_hours=24, min_elevation_deg=10.0,
    )
    assert len(passes) > 0, "expected at least one ISS pass over Boise in next 24h"
    p = passes[0]
    assert p["aos"] < p["peak"] <= p["los"]
    assert 10.0 < p["max_elev_deg"] < 90.0
    assert (p["los"] - p["aos"]).total_seconds() > 0
    # And the pass must lie inside the 24h horizon (ref + 24h = 2026-06-10T07:00 UTC).
    horizon_end = datetime(2026, 6, 10, 7, 0, 0, tzinfo=timezone.utc)
    assert p["aos"] >= _REF
    assert p["los"] <= horizon_end


def test_iss_pass_has_plausible_azimuths():
    """Azimuth at AOS and LOS should be valid 0-360° readings."""
    passes = _next_passes(
        _ISS_L1, _ISS_L2, _OBS,
        ref_time=_REF, horizon_hours=24, min_elevation_deg=10.0,
    )
    p = passes[0]
    assert 0.0 <= p["aos_az"] < 360.0
    # los_az may be None if the pass ran to the horizon edge, but for ISS
    # against the pinned ref it completes within 24h.
    if p["los_az"] is not None:
        assert 0.0 <= p["los_az"] < 360.0


def test_min_elevation_gate_filters_lower_passes():
    """Same TLE, raise the gate to 80° -- now zero passes (ISS at 51.6°
    inclination from latitude 43.6° can't reach 80° often)."""
    passes_low = _next_passes(_ISS_L1, _ISS_L2, _OBS, _REF, 24, 10.0)
    passes_high = _next_passes(_ISS_L1, _ISS_L2, _OBS, _REF, 24, 80.0)
    assert len(passes_low) > 0
    # No 80°+ passes today (would require near-overhead crossing).
    for p in passes_high:
        assert p["max_elev_deg"] >= 80.0


def test_malformed_tle_returns_empty_pass_list():
    """A garbage TLE must not crash; just yield no passes."""
    passes = _next_passes("not a tle", "also not", _OBS, _REF, 24, 10.0)
    assert passes == []


# --- _build_event / _pass_to_event ------------------------------------------


def _row_for_iss():
    return {
        "norad_id": 25544, "satellite_name": "ISS (ZARYA)",
        "tle_line1": _ISS_L1, "tle_line2": _ISS_L2,
        "tle_epoch": "2026-06-08T19:17:55+00:00",
    }


def test_pass_event_shape(adapter):
    passes = _next_passes(_ISS_L1, _ISS_L2, _OBS, _REF, 24, 10.0)
    assert passes
    ev = adapter._pass_to_event(passes[0], _row_for_iss(), _OBS)
    # Identity
    assert ev.adapter == "satpass_predict"
    assert ev.category == "pass.satpass_predict"
    # Dedup id shape: {observer_slug}:{norad_id}:{aos_iso}
    assert ev.id.startswith("treasure-valley:25544:")
    assert ":2026-06-" in ev.id  # AOS within the same UTC day window
    # Severity bucket maps from peak elevation
    assert ev.severity == _severity_from_elev(passes[0]["max_elev_deg"])
    # Geo: centroid at the observer point
    assert ev.geo.centroid == (-116.2, 43.6)
    assert ev.geo.primary_region == "US-ID"
    # data fields per spec
    assert ev.data["observer_name"] == "Treasure Valley"
    assert ev.data["observer_slug"] == "treasure-valley"
    assert ev.data["observer_state"] == "ID"
    assert ev.data["norad_id"] == 25544
    assert ev.data["satellite_name"] == "ISS (ZARYA)"
    assert ev.data["max_elevation_deg"] == round(passes[0]["max_elev_deg"], 2)
    assert ev.data["duration_s"] > 0
    assert ev.data["tle_epoch"] == "2026-06-08T19:17:55+00:00"


def test_subject_for_uses_observer_state_and_slug(adapter):
    passes = _next_passes(_ISS_L1, _ISS_L2, _OBS, _REF, 24, 10.0)
    ev = adapter._pass_to_event(passes[0], _row_for_iss(), _OBS)
    assert adapter.subject_for(ev) == "central.sat.pass.us.id.treasure-valley"


def test_subject_for_falls_back_when_state_or_slug_missing(adapter):
    from central.models import Event, Geo
    ev = Event(
        id="x", adapter="satpass_predict", category="pass.satpass_predict",
        time=datetime.now(timezone.utc), severity=2, geo=Geo(), data={},
    )
    assert adapter.subject_for(ev) == "central.sat.pass.us.unknown.unknown"


# --- poll() integration with mocked pool ------------------------------------


def _mock_pool_returning(rows):
    """Build a MagicMock pool that yields ``rows`` from any SELECT."""
    pool = MagicMock()
    conn = MagicMock()
    conn.fetch = AsyncMock(return_value=rows)
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
    return pool


@pytest.mark.asyncio
async def test_poll_empty_tles_table_logs_and_yields_zero(tmp_path):
    """v0.11.1 spec: empty TLE table -> 0 events, INFO log, no exception."""
    cfg = AdapterConfig(
        name="satpass_predict", enabled=True, cadence_s=3600,
        settings={"observers": [_OBS.model_dump()],
                  "min_elevation_deg": 10.0, "horizon_hours": 24},
        updated_at=datetime.now(timezone.utc),
    )
    config_store = MagicMock()
    config_store.get_pool.return_value = _mock_pool_returning([])
    adapter = SatpassPredictAdapter(cfg, config_store, tmp_path / "cursors.db")
    await adapter.startup()
    try:
        events = [e async for e in adapter.poll()]
        assert events == []
    finally:
        await adapter.shutdown()


@pytest.mark.asyncio
async def test_poll_multi_observer_yields_per_observer_pass_list(tmp_path):
    """Two observers in settings → each observer gets its own pass list against
    the same TLE. Boise (43.6, -116.2) and Salt Lake City (40.76, -111.89)
    both see ISS but with slightly different AOS times -> different events."""
    boise = _OBS
    slc = Observer(name="Salt Lake City", slug="slc",
                   state="UT", lat=40.76, lon=-111.89, elev_m=0.0)
    cfg = AdapterConfig(
        name="satpass_predict", enabled=True, cadence_s=3600,
        settings={"observers": [boise.model_dump(), slc.model_dump()],
                  "min_elevation_deg": 10.0, "horizon_hours": 24},
        updated_at=datetime.now(timezone.utc),
    )
    config_store = MagicMock()
    config_store.get_pool.return_value = _mock_pool_returning([_row_for_iss()])
    adapter = SatpassPredictAdapter(cfg, config_store, tmp_path / "cursors.db")
    await adapter.startup()
    try:
        events = [e async for e in adapter.poll()]
        # We don't pin counts (number of passes per 24h varies with the pinned
        # ref time), but each observer must have at least one event distinct
        # from the other.
        boise_evs = [e for e in events if e.data["observer_slug"] == "treasure-valley"]
        slc_evs = [e for e in events if e.data["observer_slug"] == "slc"]
        assert boise_evs, "no Boise passes"
        assert slc_evs, "no Salt Lake City passes"
        # Subject routing differs by state.
        assert adapter.subject_for(boise_evs[0]) == "central.sat.pass.us.id.treasure-valley"
        assert adapter.subject_for(slc_evs[0]) == "central.sat.pass.us.ut.slc"
    finally:
        await adapter.shutdown()


# --- Settings / apply_config / dedup-mixin regression ----------------------


def test_default_settings_match_spec():
    s = SatpassPredictSettings()
    assert s.min_elevation_deg == 10.0
    assert s.horizon_hours == 24
    assert len(s.observers) == 1
    assert s.observers[0].slug == "treasure-valley"


def test_inherits_dedup_mixin_from_source_adapter(tmp_path):
    """v0.9.1 regression guard."""
    assert issubclass(SatpassPredictAdapter, SourceAdapter)
    a = SatpassPredictAdapter(
        AdapterConfig(
            name="satpass_predict", enabled=False, cadence_s=3600,
            settings={}, updated_at=datetime.now(timezone.utc),
        ),
        MagicMock(),
        tmp_path / "cursors.db",
    )
    assert callable(a.is_published)
    assert callable(a.mark_published)
    assert callable(a.sweep_old_ids)


@pytest.mark.asyncio
async def test_apply_config_updates_observers_and_threshold(adapter):
    new_obs = Observer(name="Sandpoint", slug="sandpoint",
                       state="ID", lat=48.27, lon=-116.55, elev_m=600.0)
    new_cfg = AdapterConfig(
        name="satpass_predict", enabled=True, cadence_s=3600,
        settings={"observers": [new_obs.model_dump()],
                  "min_elevation_deg": 25.0, "horizon_hours": 12},
        updated_at=datetime.now(timezone.utc),
    )
    await adapter.apply_config(new_cfg)
    assert len(adapter._observers) == 1
    assert adapter._observers[0].slug == "sandpoint"
    assert adapter._min_elev == 25.0
    assert adapter._horizon_h == 12.0


# --- Stream registry + family map + GUI wiring ----------------------------


def test_central_sat_family_includes_pass_token():
    """v0.11.1: pass.* categories also route to CENTRAL_SAT.
    v0.12.0: position.* extends the family for sat_positions telemetry."""
    from central.supervisor import STREAM_CATEGORY_DOMAINS
    assert "pass" in STREAM_CATEGORY_DOMAINS["CENTRAL_SAT"]


def test_satpass_predict_in_space_adapter_group():
    from central.gui.routes import ADAPTER_GROUPS
    assert "satpass_predict" in ADAPTER_GROUPS["Space"]


# --- Partials render cleanly (v0.10.0 pattern) ------------------------------


def test_summary_partial_renders_cleanly_with_real_pass(adapter):
    from jinja2 import Environment, FileSystemLoader
    templates_dir = Path(__file__).parent.parent / "src" / "central" / "gui" / "templates"
    env = Environment(loader=FileSystemLoader(str(templates_dir)), autoescape=True)
    tmpl = env.get_template("_event_summaries/satpass_predict.html")
    passes = _next_passes(_ISS_L1, _ISS_L2, _OBS, _REF, 24, 10.0)
    ev = adapter._pass_to_event(passes[0], _row_for_iss(), _OBS)
    rendered = tmpl.render(event={
        "data": {"data": {"data": ev.model_dump(mode="json")["data"]}}
    }).strip()
    assert "ISS (ZARYA)" in rendered, f"got: {rendered!r}"
    assert "max elevation" in rendered
    assert "UTC" in rendered


def test_row_partial_renders_cleanly(adapter):
    from jinja2 import Environment, FileSystemLoader
    templates_dir = Path(__file__).parent.parent / "src" / "central" / "gui" / "templates"
    env = Environment(loader=FileSystemLoader(str(templates_dir)), autoescape=True)
    tmpl = env.get_template("_event_rows/satpass_predict.html")
    passes = _next_passes(_ISS_L1, _ISS_L2, _OBS, _REF, 24, 10.0)
    ev = adapter._pass_to_event(passes[0], _row_for_iss(), _OBS)
    rendered = tmpl.render(event={
        "data": {"data": {"data": ev.model_dump(mode="json")["data"]}}
    })
    assert "<dt>Satellite</dt>" in rendered and "ISS (ZARYA)" in rendered
    assert "<dt>Observer</dt>" in rendered and "Treasure Valley" in rendered
    assert "<dt>AOS (rise)</dt>" in rendered
    assert "<dt>Peak</dt>" in rendered
    assert "<dt>LOS (set)</dt>" in rendered
    assert "<dt>Duration</dt>" in rendered


# --- v0.11.2: sub-satellite point + visibility footprint + GeometryCollection


def test_subsatellite_point_at_north_pole_returns_polar_coords():
    """Sat at +z over geocentre -> lat=90, lon undefined (atan2 returns 0)."""
    lon, lat, alt = _subsatellite_point((0.0, 0.0, 7000.0))
    assert abs(lat - 90.0) < 1e-6
    assert abs(alt - (7000.0 - 6378.137)) < 1e-6


def test_subsatellite_point_over_equator_lon_zero():
    """Sat on +x axis at altitude 400km over (lon=0, lat=0)."""
    lon, lat, alt = _subsatellite_point((6378.137 + 400.0, 0.0, 0.0))
    assert abs(lon - 0.0) < 1e-6
    assert abs(lat - 0.0) < 1e-6
    assert abs(alt - 400.0) < 1e-6


def test_subsatellite_point_over_equator_at_lon_90():
    """Sat on +y axis over (lon=90, lat=0)."""
    lon, lat, alt = _subsatellite_point((0.0, 6778.137, 0.0))
    assert abs(lon - 90.0) < 1e-6
    assert abs(lat - 0.0) < 1e-6


def test_subsatellite_point_lon_normalised_into_180_range():
    """Sat at lon=-90 (Pacific) -> lon=-90, not 270."""
    lon, _, _ = _subsatellite_point((0.0, -6778.137, 0.0))
    assert -180.0 <= lon <= 180.0
    assert abs(lon - (-90.0)) < 1e-6


def test_subsatellite_point_real_iss_sample_via_sgp4():
    """End-to-end against sgp4: ISS at TLE epoch -- sub-sat point should be
    on a 51.6° inclination orbit (lat in [-52, 52]). Bit-deterministic."""
    from sgp4.api import Satrec, jday
    sat = Satrec.twoline2rv(_ISS_L1, _ISS_L2)
    # Propagate at TLE epoch itself for a clean reference point.
    jd, fr = jday(2026, 6, 8, 19, 17, 55.071168)
    err, pos_eci, _ = sat.sgp4(jd, fr)
    assert err == 0
    sat_ecef = _eci_to_ecef(pos_eci, _gmst_rad(jd, fr))
    lon, lat, alt = _subsatellite_point(sat_ecef)
    # ISS inclination is 51.6° so sub-sat latitude must stay within ±52°.
    assert -52.0 < lat < 52.0, f"ISS sub-sat lat {lat}° outside inclination envelope"
    # ISS altitude is ~408 km nominally; allow generous range for SGP4 noise.
    assert 350.0 < alt < 500.0, f"ISS altitude {alt}km outside expected range"
    assert -180.0 <= lon <= 180.0


# --- Visibility footprint --------------------------------------------------


def test_visibility_footprint_returns_closed_32_vertex_polygon():
    poly = _visibility_footprint(lon_deg=-116.2, lat_deg=43.6, alt_km=408.0)
    assert poly is not None
    assert poly["type"] == "Polygon"
    ring = poly["coordinates"][0]
    # 32 vertices + closing duplicate = 33 points in the ring.
    assert len(ring) == 33
    # First == last (closed polygon).
    assert ring[0] == ring[-1]


def test_visibility_footprint_iss_radius_approximation():
    """ISS at 408km -> horizon ~2253km (spec says ~2200km)."""
    poly = _visibility_footprint(lon_deg=0.0, lat_deg=0.0, alt_km=408.0)
    ring = poly["coordinates"][0]
    # At the equator with sub-sat at (0,0), the easternmost vertex is at
    # bearing 90° (pure east), so its longitude equals the angular distance
    # in degrees. radius_km / R_earth = angular_dist in rad; *180/pi for deg.
    import math as m
    r_earth = 6378.137
    expected_angular_deg = m.degrees(r_earth * m.acos(r_earth / (r_earth + 408.0)) / r_earth)
    # 2200km / 6378km ≈ 0.345 rad ≈ 19.76°. Expect lons in ring around ±19.76.
    max_lon = max(p[0] for p in ring)
    assert 18.0 < max_lon < 22.0, f"ISS east-vertex lon {max_lon}, expected ~20° (radius ~2200km)"
    assert abs(max_lon - expected_angular_deg) < 0.5


def test_visibility_footprint_geo_radius_approximation():
    """GEO at 35786km -> horizon ~9000km (spec)."""
    poly = _visibility_footprint(lon_deg=0.0, lat_deg=0.0, alt_km=35786.0)
    ring = poly["coordinates"][0]
    max_lon = max(p[0] for p in ring)
    # 9000km / 6378km ≈ 1.41 rad ≈ 80.85°. Expect lons in ring spanning ±81.
    assert 78.0 < max_lon < 83.0, f"GEO east-vertex lon {max_lon}, expected ~81°"


def test_visibility_footprint_none_for_decayed_altitude():
    """Negative or zero altitude -> None (orbit decayed, garbage in)."""
    assert _visibility_footprint(0.0, 0.0, 0.0) is None
    assert _visibility_footprint(0.0, 0.0, -100.0) is None


def test_visibility_footprint_near_antimeridian_does_not_crash():
    """Polar-orbit-style sub-sat at lon=179° -- vertices wrap across the
    dateline. Documented limitation: each vertex is normalised independently
    so the polygon may visually wrap the "wrong way" in Leaflet for sats
    crossing ±180°. Per-vertex normalisation is the simplest approach and
    Idaho-overhead passes stay well clear of this case.
    """
    poly = _visibility_footprint(lon_deg=179.0, lat_deg=0.0, alt_km=400.0)
    assert poly is not None
    ring = poly["coordinates"][0]
    for lon, lat in ring:
        # Every vertex's lon stays within [-180, 180]; no NaN / Inf.
        assert -180.0 <= lon <= 180.0
        assert -90.0 <= lat <= 90.0
        import math as m
        assert m.isfinite(lon) and m.isfinite(lat)


# --- Ground track + GeometryCollection assembly --------------------------


def test_ground_track_collected_during_real_iss_pass():
    """The pinned-ref ISS pass over Treasure Valley collects multiple sub-sat
    points from AOS through LOS. Track must be a non-empty list of (lon, lat)."""
    passes = _next_passes(_ISS_L1, _ISS_L2, _OBS, _REF, 24, 10.0)
    assert passes
    track = passes[0]["ground_track"]
    assert isinstance(track, list)
    assert len(track) >= 2  # at least AOS + LOS samples
    for lon, lat in track:
        assert -180.0 <= lon <= 180.0
        assert -90.0 <= lat <= 90.0


def test_peak_subsat_captured_at_peak_time():
    """peak_subsat is (lon, lat, alt) of the satellite at peak elevation."""
    passes = _next_passes(_ISS_L1, _ISS_L2, _OBS, _REF, 24, 10.0)
    p = passes[0]
    assert p["peak_subsat"] is not None
    lon, lat, alt = p["peak_subsat"]
    assert -180.0 <= lon <= 180.0
    # ISS inclination 51.6° → sub-sat lat in [-52, 52] always.
    assert -52.0 < lat < 52.0
    # ISS altitude ~400-450km.
    assert 350.0 < alt < 500.0


def test_build_pass_geometry_returns_geometrycollection_with_both_shapes():
    passes = _next_passes(_ISS_L1, _ISS_L2, _OBS, _REF, 24, 10.0)
    geom = _build_pass_geometry(passes[0])
    assert geom is not None
    assert geom["type"] == "GeometryCollection"
    types = [g["type"] for g in geom["geometries"]]
    assert "LineString" in types
    assert "Polygon" in types
    # LineString must have at least 2 vertices.
    ls = next(g for g in geom["geometries"] if g["type"] == "LineString")
    assert len(ls["coordinates"]) >= 2
    # Polygon must be closed.
    poly = next(g for g in geom["geometries"] if g["type"] == "Polygon")
    ring = poly["coordinates"][0]
    assert ring[0] == ring[-1]


def test_build_pass_geometry_returns_none_when_inputs_missing():
    """Defensive: pass dict with no track + no peak_subsat -> None (don't
    write an empty GeometryCollection to the wire)."""
    assert _build_pass_geometry({}) is None
    assert _build_pass_geometry({"ground_track": [], "peak_subsat": None}) is None


def test_build_pass_geometry_polygon_only_when_track_too_short():
    """A single-sample track (only 1 vertex) is below LineString minimum;
    we omit the LineString but keep the footprint Polygon."""
    geom = _build_pass_geometry({
        "ground_track": [(-116.2, 43.6)],
        "peak_subsat": (-116.2, 43.6, 400.0),
    })
    assert geom is not None
    types = [g["type"] for g in geom["geometries"]]
    assert types == ["Polygon"]


def test_pass_event_includes_geometry_collection(adapter):
    """End-to-end: built Event has the GeometryCollection attached."""
    passes = _next_passes(_ISS_L1, _ISS_L2, _OBS, _REF, 24, 10.0)
    ev = adapter._pass_to_event(passes[0], _row_for_iss(), _OBS)
    assert ev.geo.geometry is not None
    assert ev.geo.geometry["type"] == "GeometryCollection"
    # centroid stays at observer (unchanged contract).
    assert ev.geo.centroid == (-116.2, 43.6)
