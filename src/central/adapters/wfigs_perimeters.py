"""WFIGS Perimeters adapter for wildfire perimeter polygons."""

import logging
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
from pydantic import BaseModel
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from central.adapter import SourceAdapter
from central.adapters.wfigs_common import (
    WFIGS_PERIMETERS_URL,
    build_regions,
    cleanup_old_observed,
    delete_observed,
    extract_centroid,
    get_observed_guids,
    init_observed_table,
    normalize_incident_type,
    normalize_state,
    parse_wfigs_timestamp,
    polygon_intersects_bbox,
    severity_from_acres,
    subject_suffix,
    update_observed,
)
from central.config_models import AdapterConfig, RegionConfig
from central.config_store import ConfigStore
from central.models import Event, Geo

logger = logging.getLogger(__name__)

LAYER_NAME = "perimeters"


class WFIGSPerimetersSettings(BaseModel):
    """Settings schema for WFIGS Perimeters adapter."""

    region: RegionConfig | None = None


class WFIGSPerimetersAdapter(SourceAdapter):
    """NIFC WFIGS wildfire perimeters adapter."""

    name = "wfigs_perimeters"
    display_name = "NIFC WFIGS — Wildfire Perimeters"
    description = "Active wildfire perimeter polygons from NIFC WFIGS."
    settings_schema = WFIGSPerimetersSettings
    requires_api_key = None
    api_key_field = None
    wizard_order = None  # Not in setup wizard
    default_cadence_s = 300

    def __init__(
        self,
        config: AdapterConfig,
        config_store: ConfigStore,
        cursor_db_path: Path,
    ) -> None:
        self._config_store = config_store
        self._cursor_db_path = cursor_db_path
        self._session: aiohttp.ClientSession | None = None
        self._db: sqlite3.Connection | None = None
        self._last_poll_time: datetime | None = None

        # Parse region from settings
        region_dict = config.settings.get("region")
        if region_dict:
            self.region: RegionConfig | None = RegionConfig(**region_dict)
        else:
            self.region = None

    async def startup(self) -> None:
        """Initialize HTTP session and SQLite connection."""
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=120),  # Longer timeout for large polygons
        )
        self._db = sqlite3.connect(self._cursor_db_path)

        # Create tables for dedup and fall-off tracking
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS published_ids (
                adapter TEXT NOT NULL,
                event_id TEXT NOT NULL,
                first_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (adapter, event_id)
            )
        """)
        self._db.execute("""
            CREATE INDEX IF NOT EXISTS published_ids_last_seen
            ON published_ids (last_seen)
        """)
        init_observed_table(self._db)
        self._db.commit()

        logger.info(
            "WFIGS perimeters adapter started",
            extra={"region": self.region.model_dump() if self.region else None},
        )

    async def shutdown(self) -> None:
        """Close HTTP session and SQLite connection."""
        if self._session:
            await self._session.close()
            self._session = None
        if self._db:
            self._db.close()
            self._db = None
        logger.info("WFIGS perimeters adapter shut down")

    async def apply_config(self, new_config: AdapterConfig) -> None:
        """Apply new configuration from hot-reload."""
        region_dict = new_config.settings.get("region")
        if region_dict:
            self.region = RegionConfig(**region_dict)
        else:
            self.region = None
        logger.info(
            "WFIGS perimeters config updated",
            extra={"region": self.region.model_dump() if self.region else None},
        )

    def is_published(self, event_id: str) -> bool:
        """Check if an event has already been published."""
        if not self._db:
            return False
        cur = self._db.execute(
            "SELECT 1 FROM published_ids WHERE adapter = ? AND event_id = ?",
            (self.name, event_id),
        )
        return cur.fetchone() is not None

    def mark_published(self, event_id: str) -> None:
        """Mark an event as published."""
        if not self._db:
            return
        self._db.execute(
            """
            INSERT INTO published_ids (adapter, event_id, first_seen, last_seen)
            VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (adapter, event_id) DO UPDATE SET
                last_seen = CURRENT_TIMESTAMP
            """,
            (self.name, event_id),
        )
        self._db.commit()

    def bump_last_seen(self, event_id: str) -> None:
        """Bump the last_seen timestamp for an event."""
        if not self._db:
            return
        self._db.execute(
            "UPDATE published_ids SET last_seen = CURRENT_TIMESTAMP WHERE adapter = ? AND event_id = ?",
            (self.name, event_id),
        )
        self._db.commit()

    def sweep_old_ids(self) -> int:
        """Remove published_ids older than 14 days. Returns count deleted."""
        if not self._db:
            return 0
        cur = self._db.execute(
            "DELETE FROM published_ids WHERE adapter = ? AND last_seen < datetime('now', '-14 days')",
            (self.name,),
        )
        self._db.commit()
        count = cur.rowcount
        if count > 0:
            logger.info("WFIGS perimeters swept old dedup entries", extra={"count": count})
        return count

    def subject_for(self, event: Event) -> str:
        """Compute NATS subject for an event."""
        # Removal events have a different subject pattern
        if event.category.startswith("fire.perimeter.removed"):
            state = event.data.get("state", "").lower() or "unknown"
            return f"central.fire.perimeter.removed.{state}"

        # Regular perimeters: central.fire.perimeter.<state>.<county>
        # POOState is already normalized (2-letter code)
        state = event.data.get("POOState")
        county = event.data.get("POOCounty")
        suffix = subject_suffix(state, county)
        return f"central.fire.perimeter.{suffix}"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=30),
        retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError)),
    )
    async def _fetch_features(self) -> list[dict[str, Any]]:
        """Fetch features from WFIGS FeatureServer."""
        if not self._session:
            raise RuntimeError("Session not initialized")

        # Build query params
        params: dict[str, str] = {
            "outFields": "*",
            "returnGeometry": "true",
            "f": "geojson",
        }

        # Time filter: only fetch modified since last poll
        # Note: perimeters use attr_ModifiedOnDateTime_dt field
        if self._last_poll_time:
            iso_time = self._last_poll_time.strftime("%Y-%m-%d %H:%M:%S")
            params["where"] = f"attr_ModifiedOnDateTime_dt > timestamp '{iso_time}'"
        else:
            params["where"] = "1=1"

        # Bbox filter if region configured
        if self.region:
            bbox = f"{self.region.west},{self.region.south},{self.region.east},{self.region.north}"
            params["geometry"] = bbox
            params["geometryType"] = "esriGeometryEnvelope"
            params["spatialRel"] = "esriSpatialRelIntersects"
            params["inSR"] = "4326"

        async with self._session.get(WFIGS_PERIMETERS_URL, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()

        features = data.get("features", [])
        logger.info(
            "WFIGS perimeters fetch completed",
            extra={"feature_count": len(features)},
        )
        return features

    async def poll(self) -> AsyncIterator[Event]:
        """Poll WFIGS for perimeter updates."""
        if not self._db:
            raise RuntimeError("Database not initialized")

        # Fetch features from upstream
        try:
            features = await self._fetch_features()
        except Exception as e:
            logger.error("WFIGS perimeters fetch failed", extra={"error": str(e)})
            raise

        # Get previous poll's observed GUIDs for fall-off detection
        observed_before = get_observed_guids(self._db, LAYER_NAME)

        # Process features and track current GUIDs
        current_guids: dict[str, tuple[str | None, str | None]] = {}
        events_yielded = 0

        for feature in features:
            props = feature.get("properties", {})
            geometry = feature.get("geometry")

            # WFIGS Perimeters use prefixed field names (attr_*, poly_*)
            irwin_id = props.get("attr_IrwinID") or props.get("poly_IRWINID")
            if not irwin_id:
                continue

            # Post-filter: skip if geometry doesn't intersect region bbox
            if self.region and geometry:
                if not polygon_intersects_bbox(
                    geometry,
                    self.region.west, self.region.south,
                    self.region.east, self.region.north,
                ):
                    continue

            # Normalize at parse boundary
            state_raw = props.get("attr_POOState")
            state = normalize_state(state_raw)
            county = props.get("attr_POOCounty")
            incident_type_raw = props.get("attr_IncidentTypeCategory")
            incident_type = normalize_incident_type(incident_type_raw)

            # Track this GUID as observed (for fall-off detection)
            # Store normalized state for consistency
            current_guids[irwin_id] = (state, county)

            # Parse fields using prefixed names
            discovery_time = parse_wfigs_timestamp(props.get("attr_FireDiscoveryDateTime"))
            # Use poly_GISAcres or attr_IncidentSize for acreage
            daily_acres = props.get("attr_IncidentSize") or props.get("poly_GISAcres")

            # Build regions (expects normalized 2-letter state code)
            regions, primary_region = build_regions(state, county)

            # Extract centroid for geo
            centroid = extract_centroid(geometry)

            # Build bbox from geometry if available
            bbox = None
            if geometry:
                try:
                    from shapely.geometry import shape
                    geom = shape(geometry)
                    bounds = geom.bounds  # (minx, miny, maxx, maxy)
                    bbox = (bounds[0], bounds[1], bounds[2], bounds[3])
                except Exception:
                    if centroid:
                        bbox = (centroid[0], centroid[1], centroid[0], centroid[1])

            # Build geo
            geo = Geo(
                centroid=centroid,
                bbox=bbox,
                regions=regions,
                primary_region=primary_region,
            )

            # Build event with geometry in data
            # Use normalized field names in event data for consistency
            event = Event(
                id=irwin_id,
                adapter=self.name,
                category=f"fire.perimeter.{incident_type}",
                time=discovery_time or datetime.now(timezone.utc),
                severity=severity_from_acres(daily_acres),
                geo=geo,
                data={
                    "IrwinID": irwin_id,
                    "IncidentName": props.get("attr_IncidentName") or props.get("poly_IncidentName"),
                    "IncidentTypeCategory": incident_type,
                    "IncidentTypeCategory_raw": incident_type_raw,
                    "DailyAcres": props.get("attr_IncidentSize"),
                    "GISAcres": props.get("poly_GISAcres"),
                    "PercentContained": props.get("attr_PercentContained"),
                    "FireDiscoveryDateTime": props.get("attr_FireDiscoveryDateTime"),
                    "ModifiedOnDateTime": props.get("attr_ModifiedOnDateTime_dt"),
                    "POOState": state,
                    "POOState_raw": state_raw,
                    "POOCounty": county,
                    "geometry": geometry,  # Full GeoJSON polygon
                    "raw": props,
                },
            )

            yield event
            events_yielded += 1

        # Detect fall-offs: GUIDs in previous but not current
        fallen_off = set(observed_before.keys()) - set(current_guids.keys())

        for irwin_id in fallen_off:
            last_observed, state, county = observed_before[irwin_id]
            now = datetime.now(timezone.utc)

            removal_event = Event(
                id=f"{irwin_id}:removed:{now.isoformat()}",
                adapter=self.name,
                category="fire.perimeter.removed",
                time=now,
                severity=0,
                geo=Geo(),
                data={
                    "irwin_id": irwin_id,
                    "last_observed_at": last_observed,
                    "state": state,
                    "county": county,
                    "reason": "fallen_off_current_service",
                },
            )

            yield removal_event
            events_yielded += 1
            logger.info(
                "WFIGS perimeter fall-off detected",
                extra={"irwin_id": irwin_id, "state": state},
            )

        # Update observed table
        update_observed(self._db, LAYER_NAME, current_guids)
        delete_observed(self._db, LAYER_NAME, fallen_off)

        # Periodic cleanup of old entries
        cleanup_old_observed(self._db, LAYER_NAME)
        self.sweep_old_ids()

        # Update last poll time
        self._last_poll_time = datetime.now(timezone.utc)

        logger.info(
            "WFIGS perimeters poll completed",
            extra={
                "events_yielded": events_yielded,
                "current_observed": len(current_guids),
                "fallen_off": len(fallen_off),
            },
        )
