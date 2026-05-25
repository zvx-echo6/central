"""Bug 2 (v0.7.0): out-of-range map bbox must not reach the SQL envelope.

Leaflet returns longitudes outside [-180, 180] when the map is panned past the
dateline at low zoom (e.g. east=411.3281, west=-608.2031). The map JS now
normalizes/omits those, but _parse_events_params also defends in depth: a
degenerate or out-of-range region is treated as "no bbox" rather than erroring
or building a bogus ST_MakeEnvelope that silently matches the wrong rows.
"""
from central.gui.routes import _parse_events_params


def _params(**kw):
    base = {"limit": "50"}
    base.update(kw)
    return base


def test_out_of_range_bbox_is_dropped_not_errored():
    """The exact pan-past-dateline artifact from the bug report -> no bbox."""
    parsed, error = _parse_events_params(_params(
        region_north="42.0", region_south="31.0",
        region_east="411.3281", region_west="-608.2031",
    ))
    assert error is None
    assert parsed is not None
    assert parsed["bbox"] is None


def test_valid_bbox_is_preserved():
    # v0.7.2: a bbox is only honored when the map-filter toggle is on.
    parsed, error = _parse_events_params(_params(
        region_north="42.0", region_south="31.0",
        region_east="-102.0", region_west="-124.5", map_filter="1",
    ))
    assert error is None
    assert parsed["bbox"] == {
        "north": 42.0, "south": 31.0, "east": -102.0, "west": -124.5,
    }


def test_inverted_bbox_is_dropped():
    """west >= east (a wrapped dateline-straddle) is degenerate -> no bbox."""
    parsed, error = _parse_events_params(_params(
        region_north="42.0", region_south="31.0",
        region_east="-124.5", region_west="-102.0",
    ))
    assert error is None
    assert parsed["bbox"] is None
