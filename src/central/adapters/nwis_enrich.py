"""USGS site + stats enrichment helpers for the NWIS adapter (v0.8.0).

NWIS-specific (Approach B — the adapter owns its USGS enrichment), producing the
``_enriched.usgs_site`` and ``_enriched.usgs_stats`` bundles. This module holds
the pure parse/classify functions plus a small sqlite cache; the adapter wires
them in (see nwis.py).

- Site metadata: OGC monitoring-locations item-by-id (JSON), same API family the
  adapter already speaks.
- Daily stats: the legacy waterservices RDB ``stat`` service — the OGC API has no
  statistics endpoint.

USGS percentiles are "percent of days at or below this value", so a HIGHER
percentile means HIGHER flow. WaterWatch bands map to a 0-4 severity (None is
reserved for "no stats available", which is distinct from a normal-flow gauge):

    value > historical daily max  -> record high          severity 4
    value > P90                   -> much above normal     severity 3
    P75 < value <= P90            -> above normal          severity 2
    P25 <= value <= P75           -> normal                severity 1
    P10 <= value <  P25           -> below normal          severity 2
    value < P10                   -> much below normal     severity 3
    (no usable thresholds)        -> None                  severity None
"""

import asyncio
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# TTLs: site metadata is near-static; the daily-percentile table drifts slowly
# (USGS recomputes period-of-record stats infrequently), so one fetch per
# site+parameter covers the whole year of day-of-year rows for a season.
USGS_SITE_TTL_S = 365 * 86400
USGS_STATS_TTL_S = 90 * 86400

SITE_FIELDS: tuple[str, ...] = ("name", "lat", "lon", "state", "county")
STATS_FIELDS: tuple[str, ...] = (
    "value", "percentile", "class_label", "severity_band",
    "p10", "p25", "p50", "p75", "p90", "record_max", "count", "period",
)

# WaterWatch band -> severity (0-4). None is NOT in here: it means "no stats".
SEVERITY_BY_BAND: dict[str, int] = {
    "record high": 4,
    "much above normal": 3,
    "above normal": 2,
    "normal": 1,
    "below normal": 2,
    "much below normal": 3,
}


def site_null_bundle() -> dict[str, Any]:
    return {f: None for f in SITE_FIELDS}


def stats_null_bundle() -> dict[str, Any]:
    return {f: None for f in STATS_FIELDS}


def parse_site_feature(feature: dict) -> dict[str, Any]:
    """OGC monitoring-locations Feature -> usgs_site bundle (all-null on bad shape)."""
    if not isinstance(feature, dict):
        return site_null_bundle()
    props = feature.get("properties") or {}
    geom = feature.get("geometry") or {}
    coords = geom.get("coordinates") if isinstance(geom, dict) else None
    lat = lon = None
    if (
        isinstance(coords, list)
        and len(coords) == 2
        and all(isinstance(c, (int, float)) for c in coords)
    ):
        lon, lat = float(coords[0]), float(coords[1])  # GeoJSON (lon, lat)
    return {
        "name": props.get("monitoring_location_name"),
        "lat": lat,
        "lon": lon,
        "state": props.get("state_name"),
        "county": props.get("county_name"),
    }


def _num(cols: list[str], idx: dict[str, int], key: str) -> float | None:
    i = idx.get(key)
    if i is None or i >= len(cols):
        return None
    raw = cols[i].strip()
    if raw == "":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def parse_stats_rdb(text: str) -> dict[str, dict[str, Any]]:
    """Parse the daily-statistics RDB into a per-day threshold table.

    Returns ``{"<month>-<day>": {p10, p25, p50, p75, p90, max, count,
    begin_yr, end_yr}}`` with blank/missing numeric cells as None. Keys are
    JSON-friendly strings so the table caches directly. ``{}`` on bad input.
    Column positions are read from the RDB header row (robust to USGS column
    reordering); the line after the header is the RDB format row and is skipped.
    """
    lines = [ln for ln in text.splitlines() if ln and not ln.startswith("#")]
    if len(lines) < 3:
        return {}
    header = lines[0].split("\t")
    idx = {name: i for i, name in enumerate(header)}
    if "month_nu" not in idx or "day_nu" not in idx:
        return {}
    table: dict[str, dict[str, Any]] = {}
    for ln in lines[2:]:  # lines[1] is the "5s 15s ..." format row
        cols = ln.split("\t")
        month = _num(cols, idx, "month_nu")
        day = _num(cols, idx, "day_nu")
        if month is None or day is None:
            continue
        count = _num(cols, idx, "count_nu")
        table[f"{int(month)}-{int(day)}"] = {
            "p10": _num(cols, idx, "p10_va"),
            "p25": _num(cols, idx, "p25_va"),
            "p50": _num(cols, idx, "p50_va"),
            "p75": _num(cols, idx, "p75_va"),
            "p90": _num(cols, idx, "p90_va"),
            "max": _num(cols, idx, "max_va"),
            "count": int(count) if count is not None else None,
            "begin_yr": _num(cols, idx, "begin_yr"),
            "end_yr": _num(cols, idx, "end_yr"),
        }
    return table


def percentile_of(value: float, day: dict[str, Any]) -> int | None:
    """Interpolate the value's approximate percentile from a day's thresholds.

    Piecewise-linear over the available (percentile, threshold) points, with an
    implicit (0th, 0.0) lower bound (flow/stage are non-negative) and a (100th,
    max) upper bound when the daily max is known. None when fewer than two
    usable points exist.
    """
    pts: list[tuple[float, float]] = [(0.0, 0.0)]
    for pct, key in ((10, "p10"), (25, "p25"), (50, "p50"), (75, "p75"), (90, "p90")):
        v = day.get(key)
        if v is not None:
            pts.append((float(pct), float(v)))
    mx = day.get("max")
    if mx is not None:
        pts.append((100.0, float(mx)))
    pts = sorted(set(pts), key=lambda t: t[1])
    if len(pts) < 2:
        return None
    if value <= pts[0][1]:
        return int(round(pts[0][0]))
    if value >= pts[-1][1]:
        return int(round(pts[-1][0]))
    for i in range(1, len(pts)):
        p0, v0 = pts[i - 1]
        p1, v1 = pts[i]
        if v0 <= value <= v1:
            if v1 == v0:
                return int(round(p1))
            return int(round(p0 + (p1 - p0) * (value - v0) / (v1 - v0)))
    return None


def classify(value: float | None, day: dict[str, Any]) -> tuple[str | None, int | None, int | None]:
    """Classify a value against a day's thresholds -> (class_label, percentile, severity).

    Best-effort when some thresholds are missing (e.g. P90 blank -> the top
    reachable band without a max is 'above normal'). Returns all-None when no
    threshold lets us place the value at all.
    """
    if value is None:
        return (None, None, None)
    p10, p25, p75, p90, mx = (
        day.get("p10"), day.get("p25"), day.get("p75"), day.get("p90"), day.get("max"),
    )
    label: str | None = None
    if mx is not None and value > mx:
        label = "record high"
    elif p90 is not None and value > p90:
        label = "much above normal"
    elif p75 is not None and value > p75:
        label = "above normal"
    elif p25 is not None and value >= p25:
        label = "normal"
    elif p10 is not None and value >= p10:
        label = "below normal"
    elif p10 is not None and value < p10:
        label = "much below normal"
    if label is None:
        return (None, percentile_of(value, day), None)
    return (label, percentile_of(value, day), SEVERITY_BY_BAND.get(label))


def build_stats_bundle(value: float | None, table: dict[str, dict[str, Any]], month: int, day: int) -> dict[str, Any]:
    """Assemble the usgs_stats bundle for one reading from a parsed day-table.

    The reading's ``value`` is always echoed (useful even with no thresholds);
    thresholds/classification fill in when the matching day-of-year row exists.
    """
    bundle = stats_null_bundle()
    bundle["value"] = value
    row = table.get(f"{month}-{day}") if table else None
    if not row:
        return bundle
    for k in ("p10", "p25", "p50", "p75", "p90"):
        bundle[k] = row.get(k)
    bundle["record_max"] = row.get("max")
    bundle["count"] = row.get("count")
    by, ey = row.get("begin_yr"), row.get("end_yr")
    bundle["period"] = f"{int(by)}–{int(ey)}" if by and ey else None
    label, pct, sev = classify(value, row)
    bundle["class_label"] = label
    bundle["percentile"] = pct
    bundle["severity_band"] = sev
    return bundle


class SiteStatsCache:
    """Thread-offloaded sqlite cache for NWIS site bundles + stats day-tables.

    Keyed by (kind, key): kind 'site' key=monitoring_location_id (TTL ~1yr),
    kind 'stats' key='<site_id>:<parameter_code>' (TTL ~90d, stores the whole
    parsed day-of-year table so one fetch serves every reading at that site).
    Mirrors the EnrichmentCache pattern (fresh connection per op, ttl on read).
    """

    _SCHEMA = """
    CREATE TABLE IF NOT EXISTS nwis_cache (
        kind TEXT NOT NULL,
        key TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        cached_at TEXT NOT NULL,
        PRIMARY KEY (kind, key)
    )
    """

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            conn.execute(self._SCHEMA)
            conn.commit()
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path, timeout=30)

    def _get_sync(self, kind: str, key: str, ttl_s: int) -> Any | None:
        conn = self._connect()
        try:
            cur = conn.execute(
                "SELECT payload_json, cached_at FROM nwis_cache WHERE kind = ? AND key = ?",
                (kind, key),
            )
            row = cur.fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        payload_json, cached_at_iso = row
        try:
            cached_at = datetime.fromisoformat(cached_at_iso)
        except ValueError:
            return None
        if cached_at.tzinfo is None:
            cached_at = cached_at.replace(tzinfo=timezone.utc)
        if (datetime.now(timezone.utc) - cached_at).total_seconds() > ttl_s:
            return None
        return json.loads(payload_json)

    def _set_sync(self, kind: str, key: str, payload: Any) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO nwis_cache (kind, key, payload_json, cached_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (kind, key) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    cached_at = excluded.cached_at
                """,
                (kind, key, json.dumps(payload), now_iso),
            )
            conn.commit()
        finally:
            conn.close()

    async def get(self, kind: str, key: str, ttl_s: int) -> Any | None:
        return await asyncio.to_thread(self._get_sync, kind, key, ttl_s)

    async def set(self, kind: str, key: str, payload: Any) -> None:
        await asyncio.to_thread(self._set_sync, kind, key, payload)
