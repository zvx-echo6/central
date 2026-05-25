"""Tests for the v0.7.1 /events filtering (search, multi-select, time presets,
active pills, URL round-trip). Covers the pure helpers and the query builder
(via a captured-SQL mock), with no live DB.
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from central.gui import routes

NOW = datetime(2026, 5, 24, 12, 0, tzinfo=timezone.utc)


# --- pure helpers -----------------------------------------------------------

def test_csv_param_splits_and_trims():
    assert routes._csv_param({"adapter": "nws, firms ,usgs_quake"}, "adapter") == \
        ["nws", "firms", "usgs_quake"]
    assert routes._csv_param({}, "adapter") == []
    assert routes._csv_param({"adapter": ""}, "adapter") == []


def test_ilike_escape_escapes_wildcards():
    assert routes._ilike_escape("50%_off\\x") == "50\\%\\_off\\\\x"


@pytest.mark.parametrize("token,delta", [
    ("last_15m", timedelta(minutes=15)),
    ("last_1h", timedelta(hours=1)),
    ("last_6h", timedelta(hours=6)),
    ("last_24h", timedelta(hours=24)),
    ("last_7d", timedelta(days=7)),
])
def test_resolve_time_presets(token, delta):
    since, until, active, err = routes._resolve_time(token, NOW)
    assert err is None and active is False and until is None
    assert since == NOW - delta


def test_resolve_time_active_all_custom():
    assert routes._resolve_time("active", NOW) == (None, None, True, None)
    assert routes._resolve_time("all", NOW) == (None, None, False, None)
    assert routes._resolve_time(None, NOW) == (None, None, False, None)
    since, until, active, err = routes._resolve_time("custom:2026-05-01T00:00,2026-05-02T00:00", NOW)
    assert err is None and active is False
    # datetime-local values carry no tz (naive), matching the legacy since/until path.
    assert since == datetime(2026, 5, 1)
    assert until == datetime(2026, 5, 2)


def test_resolve_time_custom_inverted_is_error():
    _, _, _, err = routes._resolve_time("custom:2026-05-02T00:00,2026-05-01T00:00", NOW)
    assert err and "before" in err


def test_resolve_time_unknown_token_is_error():
    _, _, _, err = routes._resolve_time("yesterday", NOW)
    assert err and "Unknown" in err


# --- _parse_events_params ---------------------------------------------------

def test_parse_multi_value_csv():
    parsed, err = routes._parse_events_params({
        "adapter": "nws,firms", "category": "quake.event.light,wx.alert.tornado_warning",
        "event_type": "fire,quake", "severity": "critical,high", "q": "wildfire", "time": "all",
    })
    assert err is None
    assert parsed["adapters"] == ["nws", "firms"]
    assert parsed["categories"] == ["quake.event.light", "wx.alert.tornado_warning"]
    assert parsed["event_types"] == ["fire", "quake"]
    assert parsed["severities"] == ["critical", "high"]
    assert parsed["q"] == "wildfire"


def test_parse_drops_unknown_severity_labels():
    parsed, err = routes._parse_events_params({"severity": "critical,bogus,low"})
    assert err is None
    assert parsed["severities"] == ["critical", "low"]


def test_parse_gui_defaults_to_last_24h():
    parsed, _ = routes._parse_events_params({}, default_time="last_24h")
    assert parsed["time_token"] == "last_24h"
    assert parsed["since"] is not None


def test_parse_json_api_has_no_default_time():
    parsed, _ = routes._parse_events_params({})  # no default_time (events.json)
    assert parsed["time_token"] is None
    assert parsed["since"] is None and parsed["until"] is None


def test_parse_legacy_since_until_still_honored():
    parsed, err = routes._parse_events_params(
        {"since": "2026-05-17T00:00:00", "until": "2026-05-17T12:00:00"},
        default_time="last_24h",
    )
    assert err is None
    assert parsed["since"] == datetime(2026, 5, 17, 0, 0)
    assert parsed["time_token"] is None  # legacy path, not a preset


# --- query builder (captured SQL) -------------------------------------------

async def _capture(parsed):
    captured = {}

    async def fake_fetch(query, *args):
        captured["query"] = query
        captured["params"] = list(args)
        return []

    conn = MagicMock()
    conn.fetch = fake_fetch
    pool = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
    with patch("central.gui.routes.get_pool", return_value=pool):
        await routes._fetch_events(parsed)
    return captured


@pytest.mark.asyncio
async def test_multi_adapter_uses_any():
    parsed, _ = routes._parse_events_params({"adapter": "nws,firms,usgs_quake", "time": "all"})
    cap = await _capture(parsed)
    assert "adapter = ANY($" in cap["query"]
    assert ["nws", "firms", "usgs_quake"] in cap["params"]


@pytest.mark.asyncio
async def test_event_type_uses_split_part():
    parsed, _ = routes._parse_events_params({"event_type": "fire,quake", "time": "all"})
    cap = await _capture(parsed)
    assert "split_part(category, '.', 1) = ANY($" in cap["query"]
    assert ["fire", "quake"] in cap["params"]


@pytest.mark.asyncio
async def test_severity_unknown_includes_null():
    parsed, _ = routes._parse_events_params({"severity": "critical,unknown", "time": "all"})
    cap = await _capture(parsed)
    assert "severity = ANY($" in cap["query"] and "OR severity IS NULL" in cap["query"]
    assert [0, 4] in cap["params"]  # unknown->0, critical->4 (sorted)


@pytest.mark.asyncio
async def test_severity_without_unknown_has_no_null_clause():
    parsed, _ = routes._parse_events_params({"severity": "high", "time": "all"})
    cap = await _capture(parsed)
    assert "OR severity IS NULL" not in cap["query"]
    assert [3] in cap["params"]


@pytest.mark.asyncio
async def test_active_now_condition():
    parsed, _ = routes._parse_events_params({"time": "active"})
    cap = await _capture(parsed)
    assert "time <= now() AND (expires IS NULL OR expires > now())" in cap["query"]


@pytest.mark.asyncio
async def test_search_is_parameterized_and_escaped():
    # A term loaded with SQL/LIKE metacharacters must be safely parameterized.
    parsed, _ = routes._parse_events_params({"q": "50%_x'; DROP TABLE events;--", "time": "all"})
    cap = await _capture(parsed)
    assert "(payload->'data'->'data')::text ILIKE $" in cap["query"]
    assert "ESCAPE '\\'" in cap["query"]
    # The dangerous term never appears inline in the SQL; only as a bound param.
    assert "DROP TABLE" not in cap["query"]
    assert cap["params"][0] == "%50\\%\\_x'; DROP TABLE events;--%"


# --- active pills + URL round-trip ------------------------------------------

def test_build_active_pills():
    parsed = {
        "q": "fire", "adapters": ["nws", "firms"], "categories": ["a", "b"],
        "event_types": ["fire"], "severities": ["high", "critical"], "time_token": "last_24h",
    }
    pills = {p["key"]: p["label"] for p in routes._build_active_pills(parsed, adapter_total=12)}
    assert pills["q"] == 'Search: "fire"'
    assert pills["adapter"] == "Adapters: 2 of 12"
    assert pills["category"] == "Categories: 2"
    assert pills["event_type"] == "Event types: fire"
    assert pills["severity"] == "Severity: critical, high"  # ranked order
    assert pills["time"] == "Last 24 hours"


def test_no_time_pill_for_all():
    pills = routes._build_active_pills({"time_token": "all"}, adapter_total=12)
    assert all(p["key"] != "time" for p in pills)


def test_url_round_trip():
    q = {"q": "fire", "adapter": "nws,firms", "category": "quake.event.light",
         "event_type": "fire", "severity": "critical,high", "time": "last_1h", "limit": "50"}
    parsed, err = routes._parse_events_params(q)
    assert err is None
    # Re-serialize the parsed multi-values as CSV and re-parse: identical state.
    requery = {
        "q": parsed["q"], "adapter": ",".join(parsed["adapters"]),
        "category": ",".join(parsed["categories"]), "event_type": ",".join(parsed["event_types"]),
        "severity": ",".join(parsed["severities"]), "time": parsed["time_token"], "limit": "50",
    }
    reparsed, _ = routes._parse_events_params(requery)
    for k in ("q", "adapters", "categories", "event_types", "severities", "time_token"):
        assert reparsed[k] == parsed[k]


# --- map-filter toggle + severity column (v0.7.2) ---------------------------

_REGION = {
    "region_north": "42", "region_south": "31", "region_east": "-102", "region_west": "-124.5",
    "time": "all",
}


def test_map_filter_off_ignores_bbox():
    """Default (no map_filter): region params are present but the bbox must be
    dropped, so the table is not constrained by the map view."""
    parsed, err = routes._parse_events_params(dict(_REGION))
    assert err is None
    assert parsed["map_filter"] is False
    assert parsed["bbox"] is None


def test_map_filter_on_applies_bbox():
    """map_filter=1: the region bbox is honored."""
    parsed, err = routes._parse_events_params(dict(_REGION, map_filter="1"))
    assert err is None
    assert parsed["map_filter"] is True
    assert parsed["bbox"] == {"north": 42.0, "south": 31.0, "east": -102.0, "west": -124.5}


@pytest.mark.asyncio
async def test_bbox_reaches_query_only_when_map_filter_on():
    on, _ = routes._parse_events_params(dict(_REGION, map_filter="1"))
    off, _ = routes._parse_events_params(dict(_REGION))
    cap_on = await _capture(on)
    cap_off = await _capture(off)
    assert "ST_Intersects" in cap_on["query"]
    assert "ST_Intersects" not in cap_off["query"]


@pytest.mark.asyncio
async def test_select_includes_severity_column():
    """v0.7.2 marker opacity needs severity in the row -> SELECT must fetch it."""
    parsed, _ = routes._parse_events_params({"time": "all"})
    cap = await _capture(parsed)
    assert "severity" in cap["query"]
