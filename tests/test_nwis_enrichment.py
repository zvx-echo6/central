"""Tests for v0.8.0 NWIS site + stats enrichment.

Covers the pure parse/classify/band logic, the SiteStatsCache (hit/miss/expire),
the adapter's _enrich_event orchestration with mocked USGS responses (incl.
graceful nulls when USGS is down and cache-hit avoiding a refetch), and the
L-c summary template rendering per WaterWatch band. No live network, no live DB.
"""

from datetime import datetime, timezone
from pathlib import Path

import jinja2
import pytest

from central.adapters import nwis_enrich as ne
from central.adapters.nwis import NWISAdapter
from central.models import Event, Geo

# ── canned USGS fixtures ────────────────────────────────────────────────────

SITE_JSON = """
{"type":"Feature",
 "geometry":{"type":"Point","coordinates":[-93.1926861111111,40.8004444444444]},
 "properties":{"monitoring_location_name":"South Fork Chariton River near Promise City, IA",
               "state_name":"Iowa","county_name":"Wayne County",
               "country_name":"United States of America"}}
"""

# header / RDB-format row / one data row for month 1 day 1 (p90 intentionally blank).
STATS_RDB = (
    "# canned\n"
    "agency_cd\tsite_no\tparameter_cd\tts_id\tloc_web_ds\tmonth_nu\tday_nu\tbegin_yr\tend_yr\tcount_nu\tmax_va_yr\tmax_va\tp10_va\tp25_va\tp50_va\tp75_va\tp90_va\n"
    "5s\t15s\t5s\t10n\t15s\t3n\t3n\t6n\t6n\t8n\t5n\t12s\t12s\t12s\t12s\t12s\t12s\n"
    "USGS\t06903700\t00060\t43334\t\t1\t1\t1968\t2026\t59\t2019\t315\t10\t25\t50\t75\t90\n"
)

DAY = {"p10": 10.0, "p25": 25.0, "p50": 50.0, "p75": 75.0, "p90": 90.0, "max": 200.0}


# ── parse_site_feature ──────────────────────────────────────────────────────

def test_parse_site_feature_full():
    import json
    b = ne.parse_site_feature(json.loads(SITE_JSON))
    assert b["name"] == "South Fork Chariton River near Promise City, IA"
    assert b["state"] == "Iowa" and b["county"] == "Wayne County"
    assert round(b["lat"], 3) == 40.800 and round(b["lon"], 3) == -93.193


def test_parse_site_feature_bad_shape_is_all_null():
    assert ne.parse_site_feature({}) == ne.site_null_bundle()
    assert ne.parse_site_feature(None) == ne.site_null_bundle()


# ── parse_stats_rdb ─────────────────────────────────────────────────────────

def test_parse_stats_rdb_keys_and_values():
    table = ne.parse_stats_rdb(STATS_RDB)
    assert "1-1" in table
    row = table["1-1"]
    assert row["p10"] == 10.0 and row["p75"] == 75.0 and row["max"] == 315.0
    assert row["count"] == 59 and row["begin_yr"] == 1968.0 and row["end_yr"] == 2026.0


def test_parse_stats_rdb_garbage_is_empty():
    assert ne.parse_stats_rdb("") == {}
    assert ne.parse_stats_rdb("not\trdb\n") == {}


# ── classify: WaterWatch band edges ─────────────────────────────────────────

@pytest.mark.parametrize(
    "value,label,severity",
    [
        (5.0, "much below normal", 3),     # < P10  (P0-P9 region)
        (9.0, "much below normal", 3),     # P9
        (10.0, "below normal", 2),         # == P10 boundary
        (24.0, "below normal", 2),
        (25.0, "normal", 1),               # == P25
        (50.0, "normal", 1),
        (75.0, "normal", 1),               # == P75 (not > P75)
        (80.0, "above normal", 2),         # P75..P90
        (90.0, "above normal", 2),         # == P90 (not > P90)
        (120.0, "much above normal", 3),   # > P90, <= max
        (250.0, "record high", 4),         # > historical max
    ],
)
def test_classify_bands(value, label, severity):
    lbl, pct, sev = ne.classify(value, DAY)
    assert lbl == label
    assert sev == severity
    assert ne.SEVERITY_BY_BAND.get(label) == severity


def test_classify_no_thresholds_is_none():
    assert ne.classify(42.0, {}) == (None, None, None)


def test_classify_none_value():
    assert ne.classify(None, DAY) == (None, None, None)


def test_percentile_interpolates_and_bounds():
    assert ne.percentile_of(50.0, DAY) == 50      # on the P50 point
    assert ne.percentile_of(0.0, DAY) == 0        # lower bound
    assert ne.percentile_of(200.0, DAY) == 100    # at max
    mid = ne.percentile_of(17.5, DAY)             # between P10(10) and P25(25)
    assert 10 < mid < 25


# ── build_stats_bundle ──────────────────────────────────────────────────────

def test_build_stats_bundle_classifies_matching_day():
    table = ne.parse_stats_rdb(STATS_RDB)
    b = ne.build_stats_bundle(120.0, table, 1, 1)
    assert b["value"] == 120.0
    assert b["class_label"] == "much above normal"
    assert b["severity_band"] == 3
    assert b["p50"] == 50.0 and b["record_max"] == 315.0
    assert b["period"] == "1968–2026" and b["count"] == 59


def test_build_stats_bundle_no_matching_day_echoes_value_only():
    table = ne.parse_stats_rdb(STATS_RDB)
    b = ne.build_stats_bundle(120.0, table, 7, 4)  # no 7-4 row
    assert b["value"] == 120.0
    assert b["class_label"] is None and b["severity_band"] is None


# ── SiteStatsCache: miss / hit / expire ─────────────────────────────────────

@pytest.mark.asyncio
async def test_cache_miss_then_hit(tmp_path):
    cache = ne.SiteStatsCache(tmp_path / "nwis_cache.db")
    assert await cache.get("site", "X", 100) is None
    await cache.set("site", "X", {"name": "Gauge"})
    assert (await cache.get("site", "X", 100))["name"] == "Gauge"


@pytest.mark.asyncio
async def test_cache_expired(tmp_path):
    cache = ne.SiteStatsCache(tmp_path / "nwis_cache.db")
    await cache.set("stats", "X:00060", {"p10": 1})
    assert await cache.get("stats", "X:00060", -1) is None  # ttl already elapsed


# ── adapter _enrich_event orchestration ─────────────────────────────────────

def _make_event():
    return Event(
        id="USGS-06903700:00060:2026-01-01T00:00:00+00:00",
        adapter="nwis",
        category="hydro.00060.usgs.06903700",
        time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        severity=0,
        geo=Geo(centroid=(-93.19, 40.80)),
        data={
            "monitoring_location_id": "USGS-06903700",
            "parameter_code": "00060",
            "value": 120.0,
        },
    )


def _adapter_with_fetch(fetch, cache=None):
    a = object.__new__(NWISAdapter)
    a._enrich_cache = cache
    a._fetch = fetch  # shadows the @retry method
    return a


@pytest.mark.asyncio
async def test_enrich_event_populates_bundles_and_severity(tmp_path):
    async def fetch(url):
        if "monitoring-locations" in url:
            return SITE_JSON
        if "/stat/" in url or "statReportType" in url:
            return STATS_RDB
        raise AssertionError(url)

    a = _adapter_with_fetch(fetch, cache=ne.SiteStatsCache(tmp_path / "c.db"))
    ev = _make_event()
    sev = await a._enrich_event(ev)
    enr = ev.data["_enriched"]
    assert enr["usgs_site"]["name"].startswith("South Fork Chariton")
    assert enr["usgs_site"]["state"] == "Iowa"
    assert enr["usgs_stats"]["class_label"] == "much above normal"
    assert enr["usgs_stats"]["severity_band"] == 3
    assert sev == 3


@pytest.mark.asyncio
async def test_enrich_event_graceful_nulls_when_usgs_down(tmp_path):
    async def fetch(url):
        raise TimeoutError("USGS down")

    a = _adapter_with_fetch(fetch, cache=ne.SiteStatsCache(tmp_path / "c.db"))
    ev = _make_event()
    sev = await a._enrich_event(ev)
    enr = ev.data["_enriched"]
    assert enr["usgs_site"] == ne.site_null_bundle()
    assert enr["usgs_stats"]["value"] == 120.0  # value still echoed
    assert enr["usgs_stats"]["class_label"] is None
    assert sev is None  # no stats -> unknown severity


@pytest.mark.asyncio
async def test_enrich_event_cache_hit_avoids_refetch(tmp_path):
    calls = {"n": 0}

    async def fetch(url):
        calls["n"] += 1
        return SITE_JSON if "monitoring-locations" in url else STATS_RDB

    cache = ne.SiteStatsCache(tmp_path / "c.db")
    a = _adapter_with_fetch(fetch, cache=cache)
    await a._enrich_event(_make_event())
    first = calls["n"]
    assert first == 2  # one site + one stats fetch
    await a._enrich_event(_make_event())  # same site+param -> both cached
    assert calls["n"] == first  # no additional USGS calls


# ── L-c summary template rendering per band ─────────────────────────────────

_TEMPLATES_DIR = Path(__file__).resolve().parents[1] / "src/central/gui/templates"


def _render_summary(stats=None, site=None, value=120.0, unit="ft3/s"):
    src = (_TEMPLATES_DIR / "_event_summaries/nwis.html").read_text()
    tmpl = jinja2.Environment(autoescape=True).from_string(src)
    inner = {"value": value, "unit_of_measure": unit}
    enriched = {}
    if site is not None:
        enriched["usgs_site"] = site
    if stats is not None:
        enriched["usgs_stats"] = stats
    if enriched:
        inner["_enriched"] = enriched
    event = type("E", (), {"data": {"data": {"data": inner}}})()
    return tmpl.render(event=event).strip()


def test_summary_full_band_and_percentile():
    out = _render_summary(
        site={"name": "South Fork Grand River near Gallatin, MO"},
        stats={"class_label": "below normal", "percentile": 18},
    )
    assert "South Fork Grand River near Gallatin, MO — 120.0 ft3/s" in out
    assert "(below normal, 18th percentile)" in out


def test_summary_site_no_stats():
    out = _render_summary(site={"name": "Some Creek near Town, IA"}, stats=None)
    assert out == "Some Creek near Town, IA — 120.0 ft3/s"


def test_summary_no_enrichment_falls_back():
    out = _render_summary(site=None, stats=None)
    assert out == "Water reading: 120.0 ft3/s"
