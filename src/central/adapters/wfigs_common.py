"""Shared utilities for WFIGS (Wildland Fire Interagency Geospatial Services) adapters."""

import sqlite3
from datetime import datetime, timezone
from typing import Any

# WFIGS FeatureServer endpoints
WFIGS_INCIDENTS_URL = (
    "https://services3.arcgis.com/T4QMspbfLg3qTGWY/ArcGIS/rest/services/"
    "WFIGS_Incident_Locations_Current/FeatureServer/0/query"
)
WFIGS_PERIMETERS_URL = (
    "https://services3.arcgis.com/T4QMspbfLg3qTGWY/ArcGIS/rest/services/"
    "WFIGS_Interagency_Perimeters_Current/FeatureServer/0/query"
)

# Fall-off sweep window: 14 days (matches WFIGS's longest fall-off: large fires)
FALLOFF_WINDOW_DAYS = 14

# Incident type code mappings (WFIGS uses 2-letter codes)
INCIDENT_TYPE_MAP = {
    "WF": "wildfire",
    "RX": "prescribed_fire",
    "CX": "complex",
    "FA": "false_alarm",
}


def normalize_state(state: str | None) -> str | None:
    """Strip 'US-' prefix from POOState (ISO 3166-2 -> 2-letter)."""
    if not state:
        return None
    if state.startswith("US-") and len(state) == 5:
        return state[3:]
    if len(state) == 2:
        return state
    return state  # unknown shape, pass through


def normalize_incident_type(code: str | None) -> str:
    """Map IncidentTypeCategory code to a readable name."""
    if not code:
        return "unknown"
    upper = code.upper()
    if upper in INCIDENT_TYPE_MAP:
        return INCIDENT_TYPE_MAP[upper]
    return code.lower()


def severity_from_acres(acres: float | None) -> int:
    """Map DailyAcres to severity level 0-4."""
    if acres is None or acres == 0:
        return 0
    if acres < 10:
        return 1
    if acres < 100:
        return 2
    if acres < 1000:
        return 3
    return 4


def parse_wfigs_timestamp(epoch_ms: int | None) -> datetime | None:
    """Parse WFIGS epoch milliseconds to UTC datetime."""
    if epoch_ms is None:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)


def build_regions(state: str | None, county: str | None) -> tuple[list[str], str | None]:
    """
    Build geo.regions list and primary_region from POOState and POOCounty.

    Expects normalized 2-letter state codes (e.g., "MT" not "US-MT").
    Returns (regions, primary_region).
    """
    if not state:
        return [], None

    state_upper = state.upper()
    if county:
        # Normalize county: remove spaces, uppercase
        county_normalized = county.replace(" ", "_").upper()
        region = f"US-{state_upper}-{county_normalized}"
        return [region], region
    else:
        region = f"US-{state_upper}"
        return [region], region


def subject_suffix(state: str | None, county: str | None) -> str:
    """
    Build subject suffix from state and county.

    Expects normalized 2-letter state codes.
    Returns lowercase state.county (county with spaces→underscores).
    Falls back to "unknown" if state is not available.
    """
    if not state:
        return "unknown"

    state_lower = state.lower()
    if county:
        county_lower = county.lower().replace(" ", "_")
        return f"{state_lower}.{county_lower}"
    return state_lower


def init_observed_table(db: sqlite3.Connection) -> None:
    """Create the wfigs_observed table if it doesn't exist."""
    db.execute("""
        CREATE TABLE IF NOT EXISTS wfigs_observed (
            layer TEXT NOT NULL,
            irwin_id TEXT NOT NULL,
            last_observed_at TEXT NOT NULL,
            state TEXT,
            county TEXT,
            PRIMARY KEY (layer, irwin_id)
        )
    """)
    db.commit()


def get_observed_guids(db: sqlite3.Connection, layer: str) -> dict[str, tuple[str, str | None, str | None]]:
    """
    Get all observed IRWIN GUIDs for a layer.

    Returns dict mapping irwin_id -> (last_observed_at, state, county).
    """
    cursor = db.execute(
        "SELECT irwin_id, last_observed_at, state, county FROM wfigs_observed WHERE layer = ?",
        (layer,),
    )
    return {row[0]: (row[1], row[2], row[3]) for row in cursor.fetchall()}


def update_observed(
    db: sqlite3.Connection,
    layer: str,
    current_guids: dict[str, tuple[str | None, str | None]],
) -> None:
    """
    Update the observed table with current poll's GUIDs.

    current_guids: dict mapping irwin_id -> (state, county)
    """
    now_iso = datetime.now(timezone.utc).isoformat()

    # Use INSERT OR REPLACE to upsert
    for irwin_id, (state, county) in current_guids.items():
        db.execute(
            """
            INSERT OR REPLACE INTO wfigs_observed (layer, irwin_id, last_observed_at, state, county)
            VALUES (?, ?, ?, ?, ?)
            """,
            (layer, irwin_id, now_iso, state, county),
        )
    db.commit()


def delete_observed(db: sqlite3.Connection, layer: str, irwin_ids: set[str]) -> None:
    """Delete fallen-off GUIDs from the observed table."""
    for irwin_id in irwin_ids:
        db.execute(
            "DELETE FROM wfigs_observed WHERE layer = ? AND irwin_id = ?",
            (layer, irwin_id),
        )
    db.commit()


def cleanup_old_observed(db: sqlite3.Connection, layer: str, days: int = FALLOFF_WINDOW_DAYS) -> None:
    """Remove observed entries older than the sweep window."""
    cutoff = datetime.now(timezone.utc).isoformat()
    db.execute(
        f"""
        DELETE FROM wfigs_observed
        WHERE layer = ?
        AND datetime(last_observed_at) < datetime(?, '-{days} days')
        """,
        (layer, cutoff),
    )
    db.commit()


def point_in_bbox(
    lon: float,
    lat: float,
    west: float,
    south: float,
    east: float,
    north: float,
) -> bool:
    """Check if a point is within a bounding box."""
    return west <= lon <= east and south <= lat <= north


def polygon_intersects_bbox(
    geometry: dict[str, Any],
    west: float,
    south: float,
    east: float,
    north: float,
) -> bool:
    """
    Check if a GeoJSON geometry intersects a bounding box.

    Uses shapely for accurate polygon intersection.
    """
    try:
        from shapely.geometry import box, shape

        bbox_polygon = box(west, south, east, north)
        geom = shape(geometry)
        return bbox_polygon.intersects(geom)
    except Exception:
        # If shapely fails, fall back to centroid check
        if geometry.get("type") == "Point":
            coords = geometry.get("coordinates", [])
            if len(coords) >= 2:
                return point_in_bbox(coords[0], coords[1], west, south, east, north)
        return True  # Include if we can't determine


def extract_centroid(geometry: dict[str, Any]) -> tuple[float, float] | None:
    """Extract centroid from GeoJSON geometry."""
    if not geometry:
        return None

    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")

    if geom_type == "Point" and coords and len(coords) >= 2:
        return (coords[0], coords[1])

    # For polygons, use shapely to compute centroid
    try:
        from shapely.geometry import shape
        geom = shape(geometry)
        centroid = geom.centroid
        return (centroid.x, centroid.y)
    except Exception:
        return None
