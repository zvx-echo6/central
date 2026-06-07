"""WFIGS Incidents adapter for wildfire incident locations."""

import logging
import sqlite3
import time
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
    build_regions,
    cleanup_old_observed,
    delete_observed,
    extract_centroid,
    get_observed_guids,
    init_observed_table,
    normalize_incident_type,
    normalize_state,
    parse_wfigs_timestamp,
    point_in_bbox,
    severity_from_acres,
    subject_suffix,
    update_observed,
)
from central.config_models import AdapterConfig, RegionConfig
from central.config_store import ConfigStore
from central.models import Event, Geo

logger = logging.getLogger(__name__)

LAYER_NAME = "incidents"

# v0.10.4: switched from the `_Current` view to the parent `WFIGS_Incident_Locations`
# endpoint. The Current view excludes IMT-managed BLM fires once they transition
# to Type 3 IC / ICS-209 reporting (e.g. Blue Ridge: 14k acres, modified upstream
# within the hour, but absent from _Current). The parent endpoint has the
# IMT-managed fires; we filter to active wildfires server-side and cap recency
# client-side. The wfigs_perimeters adapter stays on `_Current` (perimeters have
# a different lifecycle and Blue Ridge isn't in either perimeter layer).
WFIGS_INCIDENTS_URL = (
    "https://services3.arcgis.com/T4QMspbfLg3qTGWY/ArcGIS/rest/services/"
    "WFIGS_Incident_Locations/FeatureServer/0/query"
)

# Client-side recency cutoff: drop features whose ModifiedOnDateTime_dt is older
# than this many seconds. Server-side `ModifiedOnDateTime_dt > N` combined with
# any other predicate returns "Unable to perform query" on this layer, so the
# floor is enforced after the JSON parse. Computed fresh each poll -- this is
# NOT a persisted cursor (avoids the v0.10.2.1 silent-zero failure mode).
_RECENCY_CUTOFF_S = 30 * 86400

# Server-side cap. The endpoint also accepts orderByFields=ModifiedOnDateTime_dt
# DESC, so the 300 we get back are the 300 most-recently-touched WF records in
# the configured POOState (or globally if state is unset). Idaho currently has
# ~30 within the 30-day client-side window.
_RESULT_RECORD_COUNT = 300


class WFIGSIncidentsSettings(BaseModel):
    """Settings schema for WFIGS Incidents adapter."""

    region: RegionConfig | None = None
    # v0.10.4: ISO 3166-2 POOState code (e.g. "US-ID") for the server-side
    # POOState filter. None disables the predicate -- the adapter then sees
    # every state's WF records up to the resultRecordCount cap and relies on
    # the existing client-side `region` bbox to scope. Operators normally
    # set this to match the bbox state for a tighter upstream call.
    state: str | None = None


class WFIGSIncidentsAdapter(SourceAdapter):
    """NIFC WFIGS wildfire incidents adapter."""

    name = "wfigs_incidents"
    display_name = "NIFC WFIGS — Wildfire Incidents"
    description = "Active wildfire incident locations from NIFC WFIGS."
    settings_schema = WFIGSIncidentsSettings
    requires_api_key = None
    api_key_field = None
    wizard_order = None  # Not in setup wizard
    default_cadence_s = 300

    # Incident-point lat/lon mirrored from Geo.centroid into event.data.
    enrichment_locations = [("latitude", "longitude")]

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

        # Parse region from settings
        region_dict = config.settings.get("region")
        if region_dict:
            self.region: RegionConfig | None = RegionConfig(**region_dict)
        else:
            self.region = None
        # v0.10.4: POOState code (e.g. "US-ID") for the server-side filter.
        self.state: str | None = config.settings.get("state")

    async def startup(self) -> None:
        """Initialize HTTP session and SQLite connection."""
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
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
            "WFIGS incidents adapter started",
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
        logger.info("WFIGS incidents adapter shut down")

    async def apply_config(self, new_config: AdapterConfig) -> None:
        """Apply new configuration from hot-reload."""
        region_dict = new_config.settings.get("region")
        if region_dict:
            self.region = RegionConfig(**region_dict)
        else:
            self.region = None
        self.state = new_config.settings.get("state")
        logger.info(
            "WFIGS incidents config updated",
            extra={"region": self.region.model_dump() if self.region else None},
        )

    def subject_for(self, event: Event) -> str:
        """Compute NATS subject for an event."""
        # Removal events have a different subject pattern
        if event.category.startswith("fire.incident.removed"):
            state = event.data.get("state", "").lower() or "unknown"
            return f"central.fire.incident.removed.{state}"

        # Regular incidents: central.fire.incident.<state>.<county>
        # POOState is already normalized (2-letter code)
        state = event.data.get("POOState")
        county = event.data.get("POOCounty")
        suffix = subject_suffix(state, county)
        return f"central.fire.incident.{suffix}"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=30),
        retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError)),
    )
    async def _fetch_features(self) -> list[dict[str, Any]]:
        """Fetch features from WFIGS FeatureServer.

        v0.10.4: switched from the `_Current` view to the parent endpoint with
        a server-side active-wildfire filter (`IncidentTypeCategory='WF' AND
        FireOutDateTime IS NULL [AND POOState='<state>']`), capped at the 300
        most-recently-touched records via `orderByFields=ModifiedOnDateTime_dt
        DESC + resultRecordCount=300`. A client-side recency cutoff drops
        anything older than ``_RECENCY_CUTOFF_S``. The cutoff is recomputed
        fresh each poll -- this is NOT a persisted cursor (see v0.10.2.1).
        """
        if not self._session:
            raise RuntimeError("Session not initialized")

        # Server-side WHERE: active wildfires only, optional POOState scope.
        # The state code is plumbed from settings (no hardcoded codes); when
        # unset, the predicate is omitted and the call returns every state's
        # active WF records up to the result cap.
        where_parts = ["IncidentTypeCategory='WF'", "FireOutDateTime IS NULL"]
        if self.state:
            # POOState literal is a 2-segment ISO-3166-2 code ("US-ID"), no quotes
            # required inside it; escape any embedded single quotes defensively.
            safe_state = self.state.replace("'", "''")
            where_parts.append(f"POOState='{safe_state}'")
        params: dict[str, str] = {
            "outFields": "*",
            "returnGeometry": "true",
            "f": "geojson",
            "where": " AND ".join(where_parts),
            "orderByFields": "ModifiedOnDateTime_dt DESC",
            "resultRecordCount": str(_RESULT_RECORD_COUNT),
        }

        # Bbox filter if region configured (defense-in-depth alongside POOState).
        if self.region:
            bbox = f"{self.region.west},{self.region.south},{self.region.east},{self.region.north}"
            params["geometry"] = bbox
            params["geometryType"] = "esriGeometryEnvelope"
            params["spatialRel"] = "esriSpatialRelIntersects"
            params["inSR"] = "4326"

        async with self._session.get(WFIGS_INCIDENTS_URL, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()

        features = data.get("features", [])
        raw_count = len(features)

        # Client-side recency floor. Server-side `ModifiedOnDateTime_dt > N`
        # combined with any other predicate is rejected on this layer, so the
        # 30-day cutoff is enforced here after the JSON parse. Computed fresh
        # each call -- never persisted.
        cutoff_ms = (int(time.time()) - _RECENCY_CUTOFF_S) * 1000
        features = [
            f for f in features
            if (f.get("properties", {}).get("ModifiedOnDateTime_dt") or 0) > cutoff_ms
        ]
        logger.info(
            "WFIGS incidents fetch completed",
            extra={
                "feature_count_raw": raw_count,
                "feature_count_after_recency_filter": len(features),
                "recency_cutoff_s": _RECENCY_CUTOFF_S,
            },
        )
        return features

    async def poll(self) -> AsyncIterator[Event]:
        """Poll WFIGS for incident updates."""
        if not self._db:
            raise RuntimeError("Database not initialized")

        # Fetch features from upstream
        try:
            features = await self._fetch_features()
        except Exception as e:
            logger.error("WFIGS incidents fetch failed", extra={"error": str(e)})
            raise

        # Get previous poll's observed GUIDs for fall-off detection
        observed_before = get_observed_guids(self._db, LAYER_NAME)

        # Process features and track current GUIDs
        current_guids: dict[str, tuple[str | None, str | None]] = {}
        events_yielded = 0

        for feature in features:
            props = feature.get("properties", {})
            geometry = feature.get("geometry")

            irwin_id = props.get("IrwinID")
            if not irwin_id:
                continue

            # Extract location
            centroid = extract_centroid(geometry)

            # Post-filter: skip if outside region bbox
            if self.region and centroid:
                lon, lat = centroid
                if not point_in_bbox(
                    lon, lat,
                    self.region.west, self.region.south,
                    self.region.east, self.region.north,
                ):
                    continue

            # Normalize at parse boundary
            state_raw = props.get("POOState")
            state = normalize_state(state_raw)
            county = props.get("POOCounty")
            incident_type_raw = props.get("IncidentTypeCategory")
            incident_type = normalize_incident_type(incident_type_raw)

            # Track this GUID as observed (for fall-off detection)
            # Store normalized state for consistency
            current_guids[irwin_id] = (state, county)

            # Parse fields
            discovery_time = parse_wfigs_timestamp(props.get("FireDiscoveryDateTime"))
            daily_acres = props.get("DailyAcres")

            # Build regions (expects normalized 2-letter state code)
            regions, primary_region = build_regions(state, county)

            # Build geo
            if centroid:
                geo = Geo(
                    centroid=centroid,
                    bbox=(centroid[0], centroid[1], centroid[0], centroid[1]),
                    regions=regions,
                    primary_region=primary_region,
                )
            else:
                geo = Geo(regions=regions, primary_region=primary_region)

            # Build event with normalized values in data
            event = Event(
                id=irwin_id,
                adapter=self.name,
                category=f"fire.incident.{incident_type}",
                time=discovery_time or datetime.now(timezone.utc),
                severity=severity_from_acres(daily_acres),
                geo=geo,
                data={
                    "IrwinID": irwin_id,
                    "IncidentName": props.get("IncidentName"),
                    "IncidentTypeCategory": incident_type,
                    "IncidentTypeCategory_raw": incident_type_raw,
                    "DailyAcres": daily_acres,
                    "PercentContained": props.get("PercentContained"),
                    "FireDiscoveryDateTime": props.get("FireDiscoveryDateTime"),
                    "ModifiedOnDateTime": props.get("ModifiedOnDateTime"),
                    "POOState": state,
                    "POOState_raw": state_raw,
                    "POOCounty": county,
                    "raw": props,
                    # Mirror centroid (lon, lat) for the flat enrichment path.
                    "latitude": centroid[1] if centroid else None,
                    "longitude": centroid[0] if centroid else None,
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
                category="fire.incident.removed",
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
                "WFIGS incident fall-off detected",
                extra={"irwin_id": irwin_id, "state": state},
            )

        # Update observed table
        update_observed(self._db, LAYER_NAME, current_guids)
        delete_observed(self._db, LAYER_NAME, fallen_off)

        # Periodic cleanup of old entries
        cleanup_old_observed(self._db, LAYER_NAME)
        self.sweep_old_ids()

        logger.info(
            "WFIGS incidents poll completed",
            extra={
                "events_yielded": events_yielded,
                "current_observed": len(current_guids),
                "fallen_off": len(fallen_off),
            },
        )
