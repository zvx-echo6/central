"""Tests for the itd_511 adapter (v0.10.0).

Fixtures are real captures from https://511.idaho.gov/api/v2/get/event,alerts
trimmed to one record per EventType plus an empty advisories baseline:
  tests/fixtures/itd_511_event_sample.json
  tests/fixtures/itd_511_alerts_sample.json

No conftest entry: dedup uses the supervisor-injected cursors.db (inherited
mixin); polling is stateless.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from central.adapter import SourceAdapter
from central.adapters.itd_511 import (
    EVENT_TYPE_MAP,
    Itd511Adapter,
    _build_geometry,
    _decode_polyline,
    _itd_severity,
    _parse_epoch,
    _strip_or_none,
    _Transient,
    _wait_strategy,
)
from central.config_models import AdapterConfig
from central.models import Event, Geo

FIX = Path(__file__).parent / "fixtures"
EVENT = json.loads((FIX / "itd_511_event_sample.json").read_text())
ALERTS = json.loads((FIX / "itd_511_alerts_sample.json").read_text())
BY_TYPE = {r["EventType"]: r for r in EVENT}


def _cfg():
    return AdapterConfig(
        name="itd_511", enabled=True, cadence_s=60,
        settings={"api_key_alias": "idaho_511"},
        updated_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def adapter(tmp_path):
    cs = MagicMock()
    cs.get_api_key = AsyncMock(return_value="testkey-32chars-deadbeefdeadbeef")
    return Itd511Adapter(_cfg(), cs, tmp_path / "cursors.db")


def test_event_type_map_is_complete():
    assert EVENT_TYPE_MAP == {
        "roadwork": "work_zone", "closures": "closure",
        "accidentsAndIncidents": "incident", "specialEvents": "special_event",
    }


def test_parse_epoch():
    assert _parse_epoch(1675113840) == datetime(2023, 1, 30, 21, 24, tzinfo=timezone.utc)
    assert _parse_epoch(None) is None
    assert _parse_epoch("not-an-int") is None
    assert _parse_epoch("1675113840") == datetime(2023, 1, 30, 21, 24, tzinfo=timezone.utc)


@pytest.mark.parametrize("sev,fc,expected", [
    ("None", False, 1), ("Minor", False, 2), ("Major", False, 3),
    ("None", True, 3), ("Minor", True, 3), ("Major", True, 3),  # full-closure forces 3
    (None, False, 1), ("Bogus", False, 1),
])
def test_severity_mapping(sev, fc, expected):
    assert _itd_severity(sev, fc) == expected


def test_strip_or_none_handles_eventsubtype_trailing_space():
    assert _strip_or_none("pavementMarkingOperations ") == "pavementMarkingOperations"
    assert _strip_or_none("") is None
    assert _strip_or_none("  ") is None
    assert _strip_or_none(None) is None
    assert _strip_or_none(42) == 42  # non-string passthrough


def test_decode_polyline_roundtrip():
    import polyline as polyline_lib
    enc = polyline_lib.encode([(43.6, -116.5), (43.7, -116.4)])
    assert _decode_polyline(enc) == [(43.6, -116.5), (43.7, -116.4)]
    assert _decode_polyline(None) == []
    assert _decode_polyline("") == []
    # malformed string => library raises => caught => []
    assert _decode_polyline("\x00\x00\x00not-a-polyline") == []


def test_build_geometry_polyline_wins():
    import polyline as polyline_lib
    enc = polyline_lib.encode([(43.6, -116.5), (43.7, -116.4)])
    geom, centroid = _build_geometry(40.0, -100.0, None, None, enc)
    assert geom["type"] == "LineString"
    assert len(geom["coordinates"]) == 2
    assert centroid == geom["coordinates"][0]  # first vertex (lon, lat) order


def test_build_geometry_bookend_linestring():
    geom, centroid = _build_geometry(43.6, -116.5, 43.7, -116.4, None)
    assert geom == {"type": "LineString",
                    "coordinates": [(-116.5, 43.6), (-116.4, 43.7)]}
    assert centroid == (-116.5, 43.6)


def test_build_geometry_point_only():
    geom, centroid = _build_geometry(43.6, -116.5, None, None, None)
    assert geom == {"type": "Point", "coordinates": (-116.5, 43.6)}
    assert centroid == (-116.5, 43.6)


def test_build_geometry_missing_all():
    assert _build_geometry(None, None, None, None, None) == (None, None)


@pytest.mark.parametrize("etype,short", [
    ("roadwork", "work_zone"), ("closures", "closure"),
    ("accidentsAndIncidents", "incident"), ("specialEvents", "special_event"),
])
def test_build_event_category_and_dedup_id(adapter, etype, short):
    rec = BY_TYPE[etype]
    e = adapter._build_event_record(rec)
    assert e.category == f"{short}.itd_511"
    assert e.id == f"idaho_511:event:{rec['SourceId']}"
    assert e.adapter == "itd_511"
    assert e.geo.primary_region == "US-ID"
    assert e.geo.regions == ["US-ID"]


def test_build_event_closure_has_linestring_geometry(adapter):
    e = adapter._build_event_record(BY_TYPE["closures"])
    assert e.geo.geometry is not None
    assert e.geo.geometry["type"] == "LineString"
    assert len(e.geo.geometry["coordinates"]) >= 2


def test_build_event_full_closure_forces_severity_3(adapter):
    e = adapter._build_event_record(BY_TYPE["closures"])
    assert e.data["is_full_closure"] is True
    assert e.severity == 3


def test_build_event_unknown_event_type_falls_back_to_incident(adapter):
    rec = {**BY_TYPE["roadwork"], "EventType": "WhoKnows", "SourceId": "X1"}
    e = adapter._build_event_record(rec)
    assert e.category == "incident.itd_511"


def test_build_event_dedup_id_falls_back_to_id_when_sourceid_missing(adapter):
    rec = {**BY_TYPE["roadwork"], "SourceId": None, "ID": 99999}
    e = adapter._build_event_record(rec)
    assert e.id == "idaho_511:event:99999"


def test_build_event_returns_none_without_any_id(adapter):
    rec = {**BY_TYPE["roadwork"], "SourceId": None, "ID": None}
    assert adapter._build_event_record(rec) is None


def test_build_event_strips_trailing_space_on_event_sub_type(adapter):
    rec = {**BY_TYPE["roadwork"], "EventSubType": "pavementMarkingOperations "}
    e = adapter._build_event_record(rec)
    assert e.data["event_sub_type"] == "pavementMarkingOperations"


def test_build_event_captures_cause_and_organization(adapter):
    e = adapter._build_event_record(BY_TYPE["roadwork"])
    assert e.data["cause"] == BY_TYPE["roadwork"]["Cause"]
    assert e.data["organization"] == BY_TYPE["roadwork"]["Organization"]


def test_build_event_passes_through_recurrence_and_restrictions(adapter):
    e = adapter._build_event_record(BY_TYPE["closures"])
    assert e.data["recurrence_schedules"] == BY_TYPE["closures"]["RecurrenceSchedules"]
    assert e.data["restrictions"] == BY_TYPE["closures"]["Restrictions"]


@pytest.mark.parametrize("short,expected_subject", [
    ("work_zone", "central.traffic.work_zone.us.id"),
    ("closure", "central.traffic.closure.us.id"),
    ("incident", "central.traffic.incident.us.id"),
    ("special_event", "central.traffic.special_event.us.id"),
    ("advisory", "central.traffic.advisory.us.id"),
])
def test_subject_for(adapter, short, expected_subject):
    e = Event(id="x", adapter="itd_511", category=f"{short}.itd_511",
              time=datetime.now(timezone.utc), severity=1, geo=Geo(), data={})
    assert adapter.subject_for(e) == expected_subject


def test_advisory_structural_passthrough(adapter):
    # Synthesize an advisory (ITD currently returns []); per-record try/except
    # in poll() means downstream surprises won't sink the cycle.
    rec = {"SourceId": "ADV-1", "Description": "Snow event in central Idaho",
           "Latitude": 44.0, "Longitude": -114.5, "Reported": 1780500000}
    e = adapter._build_advisory_record(rec)
    assert e is not None
    assert e.category == "advisory.itd_511"
    assert e.id == "idaho_511:advisory:ADV-1"
    assert e.data["advisory"] == rec  # full pass-through, schema-free
    assert e.data["latitude"] == 44.0
    assert e.data["event_type_short"] == "advisory"


def test_advisory_returns_none_without_any_id(adapter):
    assert adapter._build_advisory_record({"Description": "no id"}) is None


@pytest.mark.asyncio
async def test_poll_yields_events_from_both_endpoints(adapter):
    await adapter.startup()
    adapter._fetch = AsyncMock(side_effect=lambda ep: {"event": EVENT, "alerts": ALERTS}[ep])
    events = [e async for e in adapter.poll()]
    await adapter.shutdown()
    # alerts fixture is [] so events == EVENT count
    assert len(events) == len(EVENT)
    assert all(e.adapter == "itd_511" for e in events)
    assert {e.category for e in events} == {
        "work_zone.itd_511", "closure.itd_511",
        "incident.itd_511", "special_event.itd_511",
    }


@pytest.mark.asyncio
async def test_poll_advisory_cadence_throttles_alerts_endpoint(adapter):
    """Advisories poll on the 0th, 5th, 10th... event-poll (5x throttle)."""
    await adapter.startup()
    calls: list[str] = []

    async def fake_fetch(ep):
        calls.append(ep)
        return EVENT if ep == "event" else ALERTS

    adapter._fetch = fake_fetch
    for _ in range(6):
        [_ async for _ in adapter.poll()]
    await adapter.shutdown()
    # 6 polls: events every time, alerts on poll 0 and poll 5
    assert calls.count("event") == 6
    assert calls.count("alerts") == 2


@pytest.mark.asyncio
async def test_poll_skips_cleanly_without_api_key(tmp_path):
    cs = MagicMock()
    cs.get_api_key = AsyncMock(return_value=None)
    adapter = Itd511Adapter(_cfg(), cs, tmp_path / "cursors.db")
    await adapter.startup()
    events = [e async for e in adapter.poll()]
    await adapter.shutdown()
    assert events == []  # no fetch, clean skip per tomtom_flow precedent


@pytest.mark.asyncio
async def test_key_never_leaks_in_error_path(adapter, caplog):
    """The key travels in ?key=, so aiohttp's default error messages include
    the full URL; every error-log path must run through self._redact().
    Regression guard via caplog inspection. NOTE: ``caplog.text`` only contains
    the message field — structured ``extra={}`` kwargs land as attributes on
    the LogRecord, so we inspect both surfaces."""
    await adapter.startup()
    key_value = adapter._api_key  # the testkey set up in the adapter fixture
    assert key_value and len(key_value) > 16

    async def boom(endpoint):
        raise aiohttp.ClientConnectionError(
            f"Cannot connect to host 511.idaho.gov ssl:default [key={key_value}]"
        )

    adapter._fetch = boom
    with caplog.at_level(logging.WARNING, logger="central.adapters.itd_511"):
        events = [e async for e in adapter.poll()]
    await adapter.shutdown()
    assert events == []
    surfaces = [r.getMessage() for r in caplog.records]
    surfaces.extend(str(getattr(r, "error", "")) for r in caplog.records)
    joined = " ".join(surfaces)
    assert key_value not in joined, f"key leaked to log: {joined!r}"
    assert "<KEY>" in joined  # redaction marker proves _redact() actually fired


def test_inherits_dedup_mixin_from_source_adapter():
    for m in ("is_published", "mark_published", "sweep_old_ids"):
        assert m not in Itd511Adapter.__dict__, f"redefines {m}"
        assert getattr(Itd511Adapter, m) is getattr(SourceAdapter, m)


def test_summary_partial_renders_per_event_type():
    from central.gui.routes import _derive_subject
    cases = [
        ({"event_type_short": "work_zone", "roadway_name": "I-84"},
         "Road work on I-84"),
        ({"event_type_short": "closure", "roadway_name": "N McDermott Rd",
          "direction": "Both", "is_full_closure": True},
         "Closure on N McDermott Rd Both (full closure)"),
        ({"event_type_short": "incident", "roadway_name": "I-84",
          "direction": "East"},
         "Incident on I-84 East"),
        ({"event_type_short": "advisory"}, "Advisory"),
        # Drops "Unknown" direction per wzdx lesson
        ({"event_type_short": "work_zone", "roadway_name": "I-84",
          "direction": "Unknown"},
         "Road work on I-84"),
    ]
    for inner, expected in cases:
        row = {"adapter": "itd_511", "data": {"data": {"data": inner}}}
        assert _derive_subject(row) == expected, f"mismatch for {inner!r}"


def test_class_attributes_match_spec():
    assert Itd511Adapter.name == "itd_511"
    assert Itd511Adapter.data_class == "event"
    assert Itd511Adapter.requires_api_key == "idaho_511"
    assert Itd511Adapter.api_key_field == "api_key_alias"
    assert Itd511Adapter.default_cadence_s == 60
    assert Itd511Adapter.wizard_order is None
    assert Itd511Adapter.enrichment_locations == [("latitude", "longitude")]


# --- BUG C: 429 Retry-After must drive the wait directly; no double-sleep --

def test_transient_carries_wait_s():
    t = _Transient("429 retry-after=42", wait_s=42)
    assert t.wait_s == 42
    assert str(t) == "429 retry-after=42"
    assert _Transient("5xx").wait_s is None  # default omits


def test_wait_strategy_honors_transient_wait_s():
    """BUG C regression: a 429 Retry-After must drive the wait directly via
    _Transient.wait_s; tenacity must NOT also wait its exponential jitter on
    top (the previous shape did both, blocking ~120s+ per cycle)."""
    retry_state = MagicMock()
    outcome = MagicMock()
    outcome.exception.return_value = _Transient("429", wait_s=42)
    retry_state.outcome = outcome
    assert _wait_strategy(retry_state) == 42.0
    outcome.exception.return_value = _Transient("429", wait_s=60)
    assert _wait_strategy(retry_state) == 60.0


def test_wait_strategy_falls_back_for_transient_without_wait_s():
    """5xx _Transient (no Retry-After) falls through to exponential jitter."""
    retry_state = MagicMock()
    outcome = MagicMock()
    outcome.exception.return_value = _Transient("503 server error")  # wait_s None
    retry_state.outcome = outcome
    retry_state.attempt_number = 1
    retry_state.idle_for = 0
    wait = _wait_strategy(retry_state)
    assert isinstance(wait, float) and wait >= 0


def test_wait_strategy_falls_back_for_non_transient():
    """Network errors (no wait_s) get exponential jitter."""
    retry_state = MagicMock()
    outcome = MagicMock()
    outcome.exception.return_value = aiohttp.ClientConnectionError("net error")
    retry_state.outcome = outcome
    retry_state.attempt_number = 1
    retry_state.idle_for = 0
    wait = _wait_strategy(retry_state)
    assert isinstance(wait, float) and wait >= 0


# --- BUG D3: assert→if-raise (asserts strip under python -O) -----------------

@pytest.mark.asyncio
async def test_fetch_session_unset_raises_runtime_not_assert(adapter):
    """D3 regression: asserts strip under ``python -O``, so the session-not-
    started precondition must be enforced with an explicit if-raise."""
    assert adapter._session is None  # precondition: not yet started
    with pytest.raises(RuntimeError, match="session not started"):
        await adapter._fetch("event")


# --- BUG D5: tenacity has no default logging hooks (audit guard) -------------

def test_tenacity_decorator_has_explicit_no_log_hooks():
    """D5 audit: tenacity's defaults (before_sleep=None, after=after_nothing)
    have no logging — so the URL-with-key can't leak via the retry path. We
    pin them explicitly on @retry; if a future tenacity upgrade changes the
    defaults, this test fails loudly. Also confirms reraise=True so we get
    _Transient/ClientError verbatim instead of RetryError."""
    from tenacity import after_nothing, before_nothing
    retrying = Itd511Adapter._fetch.retry
    assert retrying.before_sleep is None
    assert retrying.after is after_nothing
    assert retrying.before is before_nothing
    assert retrying.reraise is True
