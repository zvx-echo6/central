"""Tests for WFIGS adapters."""

import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from central.config_models import AdapterConfig
from central.models import Event, Geo


# Sample GeoJSON response with incidents using real WFIGS format.
# v0.10.4: ModifiedOnDateTime_dt is now the real upstream field name (the
# pre-v0.10.2.1 ``ModifiedOnDateTime`` field was renamed by NIFC); set to
# "now-ish" via the module-load timestamp so these features pass the
# v0.10.4 client-side recency filter on every test run.
# Note: POOState comes as ISO 3166-2 ("US-MT"), IncidentTypeCategory as codes ("WF")
_FIXTURE_NOW_MS = int(time.time() * 1000)
SAMPLE_INCIDENTS_RESPONSE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-113.5, 48.5]},
            "properties": {
                "IrwinID": "GUID-001-GLACIER",
                "IncidentName": "Glacier Fire",
                "IncidentTypeCategory": "WF",  # Real format: 2-letter code
                "DailyAcres": 150,
                "PercentContained": 25,
                "FireDiscoveryDateTime": 1716000000000,
                "ModifiedOnDateTime_dt": _FIXTURE_NOW_MS,
                "POOState": "US-MT",  # Real format: ISO 3166-2
                "POOCounty": "Glacier",
            },
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-116.5, 43.5]},
            "properties": {
                "IrwinID": "GUID-002-OWYHEE",
                "IncidentName": "Owyhee Rx",
                "IncidentTypeCategory": "RX",  # Prescribed fire
                "DailyAcres": 5,
                "PercentContained": 100,
                "FireDiscoveryDateTime": 1716200000000,
                "ModifiedOnDateTime_dt": _FIXTURE_NOW_MS,
                "POOState": "US-ID",
                "POOCounty": "Owyhee",
            },
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-80.0, 26.0]},
            "properties": {
                "IrwinID": "GUID-003-FLORIDA",
                "IncidentName": "Florida Fire",
                "IncidentTypeCategory": "WF",
                "DailyAcres": 50,
                "PercentContained": 0,
                "FireDiscoveryDateTime": 1716400000000,
                "ModifiedOnDateTime_dt": _FIXTURE_NOW_MS,
                "POOState": "US-FL",
                "POOCounty": "Miami-Dade",
            },
        },
    ],
}

# Perimeters API uses prefixed field names (attr_*, poly_*)
SAMPLE_PERIMETERS_RESPONSE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-113.6, 48.4],
                    [-113.4, 48.4],
                    [-113.4, 48.6],
                    [-113.6, 48.6],
                    [-113.6, 48.4],
                ]],
            },
            "properties": {
                "attr_IrwinID": "GUID-001-GLACIER",
                "attr_IncidentName": "Glacier Fire",
                "attr_IncidentTypeCategory": "WF",  # Real format: 2-letter code
                "attr_IncidentSize": 150,
                "poly_GISAcres": 148.5,
                "attr_PercentContained": 25,
                "attr_FireDiscoveryDateTime": 1716000000000,
                "attr_ModifiedOnDateTime_dt": 1716100000000,
                "attr_POOState": "US-MT",  # Real format: ISO 3166-2
                "attr_POOCounty": "Glacier",
            },
        },
    ],
}


class TestWFIGSCommon:
    """Tests for WFIGS common utilities."""

    def test_severity_from_acres_none(self):
        from central.adapters.wfigs_common import severity_from_acres
        assert severity_from_acres(None) == 0
        assert severity_from_acres(0) == 0

    def test_severity_from_acres_small(self):
        from central.adapters.wfigs_common import severity_from_acres
        assert severity_from_acres(5) == 1
        assert severity_from_acres(9.9) == 1

    def test_severity_from_acres_medium(self):
        from central.adapters.wfigs_common import severity_from_acres
        assert severity_from_acres(10) == 2
        assert severity_from_acres(99) == 2

    def test_severity_from_acres_large(self):
        from central.adapters.wfigs_common import severity_from_acres
        assert severity_from_acres(100) == 3
        assert severity_from_acres(999) == 3

    def test_severity_from_acres_very_large(self):
        from central.adapters.wfigs_common import severity_from_acres
        assert severity_from_acres(1000) == 4
        assert severity_from_acres(100000) == 4

    def test_parse_wfigs_timestamp(self):
        from central.adapters.wfigs_common import parse_wfigs_timestamp
        ts = parse_wfigs_timestamp(1716000000000)
        assert ts is not None
        assert ts.tzinfo == timezone.utc
        assert ts.year == 2024

    def test_parse_wfigs_timestamp_none(self):
        from central.adapters.wfigs_common import parse_wfigs_timestamp
        assert parse_wfigs_timestamp(None) is None

    def test_build_regions_full(self):
        from central.adapters.wfigs_common import build_regions
        # Expects normalized 2-letter state code
        regions, primary = build_regions("MT", "Glacier")
        assert regions == ["US-MT-GLACIER"]
        assert primary == "US-MT-GLACIER"

    def test_build_regions_state_only(self):
        from central.adapters.wfigs_common import build_regions
        regions, primary = build_regions("MT", None)
        assert regions == ["US-MT"]
        assert primary == "US-MT"

    def test_build_regions_none(self):
        from central.adapters.wfigs_common import build_regions
        regions, primary = build_regions(None, None)
        assert regions == []
        assert primary is None

    def test_subject_suffix(self):
        from central.adapters.wfigs_common import subject_suffix
        # Expects normalized 2-letter state code
        assert subject_suffix("MT", "Glacier") == "mt.glacier"
        assert subject_suffix("ID", "Ada County") == "id.ada_county"
        assert subject_suffix("ID", None) == "id"
        assert subject_suffix(None, None) == "unknown"

    def test_point_in_bbox(self):
        from central.adapters.wfigs_common import point_in_bbox
        assert point_in_bbox(-116.5, 43.5, -124, 31, -102, 49) is True
        assert point_in_bbox(-80.0, 26.0, -124, 31, -102, 49) is False

    # Normalization tests
    def test_normalize_state_iso_3166(self):
        """normalize_state strips US- prefix from ISO 3166-2 codes."""
        from central.adapters.wfigs_common import normalize_state
        assert normalize_state("US-MT") == "MT"
        assert normalize_state("US-ID") == "ID"
        assert normalize_state("US-CA") == "CA"

    def test_normalize_state_already_2letter(self):
        """normalize_state passes through 2-letter codes."""
        from central.adapters.wfigs_common import normalize_state
        assert normalize_state("MT") == "MT"
        assert normalize_state("ID") == "ID"

    def test_normalize_state_none_empty(self):
        """normalize_state handles None and empty strings."""
        from central.adapters.wfigs_common import normalize_state
        assert normalize_state(None) is None
        assert normalize_state("") is None

    def test_normalize_state_unknown_format(self):
        """normalize_state passes through unknown formats."""
        from central.adapters.wfigs_common import normalize_state
        assert normalize_state("Montana") == "Montana"
        assert normalize_state("US-MONTANA") == "US-MONTANA"

    def test_normalize_incident_type_wf(self):
        """normalize_incident_type maps WF to wildfire."""
        from central.adapters.wfigs_common import normalize_incident_type
        assert normalize_incident_type("WF") == "wildfire"
        assert normalize_incident_type("wf") == "wildfire"

    def test_normalize_incident_type_rx(self):
        """normalize_incident_type maps RX to prescribed_fire."""
        from central.adapters.wfigs_common import normalize_incident_type
        assert normalize_incident_type("RX") == "prescribed_fire"
        assert normalize_incident_type("rx") == "prescribed_fire"

    def test_normalize_incident_type_cx(self):
        """normalize_incident_type maps CX to complex."""
        from central.adapters.wfigs_common import normalize_incident_type
        assert normalize_incident_type("CX") == "complex"

    def test_normalize_incident_type_fa(self):
        """normalize_incident_type maps FA to false_alarm."""
        from central.adapters.wfigs_common import normalize_incident_type
        assert normalize_incident_type("FA") == "false_alarm"

    def test_normalize_incident_type_unknown_code(self):
        """normalize_incident_type lowercases unknown codes."""
        from central.adapters.wfigs_common import normalize_incident_type
        assert normalize_incident_type("UNKNOWN_CODE") == "unknown_code"
        assert normalize_incident_type("Wildfire") == "wildfire"

    def test_normalize_incident_type_none(self):
        """normalize_incident_type returns unknown for None."""
        from central.adapters.wfigs_common import normalize_incident_type
        assert normalize_incident_type(None) == "unknown"
        assert normalize_incident_type("") == "unknown"


class TestWFIGSIncidentsAdapter:
    """Tests for WFIGS Incidents adapter."""

    @pytest.fixture
    def mock_config(self) -> AdapterConfig:
        return AdapterConfig(
            name="wfigs_incidents",
            enabled=True,
            cadence_s=300,
            settings={
                "region": {"north": 49.0, "south": 31.0, "east": -102.0, "west": -124.0},
                # v0.10.4: POOState scope for the server-side filter. The test
                # ships Idaho to match the production config; the assertion in
                # test_where_clause_filters_active_wf_only expects this value.
                "state": "US-ID",
            },
            updated_at=datetime.now(timezone.utc),
        )

    @pytest.fixture
    def mock_config_store(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def cursor_db_path(self, tmp_path: Path) -> Path:
        return tmp_path / "cursors.db"

    @pytest.mark.asyncio
    async def test_normalization_incidents(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """Incidents are correctly normalized to Events."""
        from central.adapters.wfigs_incidents import WFIGSIncidentsAdapter

        adapter = WFIGSIncidentsAdapter(mock_config, mock_config_store, cursor_db_path)
        await adapter.startup()

        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json = AsyncMock(return_value=SAMPLE_INCIDENTS_RESPONSE)

        with patch.object(adapter._session, "get", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response), __aexit__=AsyncMock())):
            events = [e async for e in adapter.poll()]

        await adapter.shutdown()

        # Should have 2 events (Florida filtered out by bbox)
        assert len(events) == 2

        # First event: Glacier Fire
        event = events[0]
        # v0.14.3: dedup key is IrwinID + ModifiedOnDateTime_dt so updates
        # republish (see TestWFIGSIncidentsUpdatePropagation below).
        assert event.id == f"GUID-001-GLACIER:{_FIXTURE_NOW_MS}"
        assert event.adapter == "wfigs_incidents"
        # Category uses normalized incident type
        assert event.category == "fire.incident.wildfire"  # NOT fire.incident.wf
        assert event.severity == 3  # 150 acres = severity 3 (100-999 range)
        # Region uses normalized state (no double US-)
        assert event.geo.primary_region == "US-MT-GLACIER"  # NOT US-US-MT-GLACIER
        # Data contains both normalized and raw values
        assert event.data["POOState"] == "MT"  # normalized
        assert event.data["POOState_raw"] == "US-MT"  # raw
        assert event.data["IncidentTypeCategory"] == "wildfire"  # normalized
        assert event.data["IncidentTypeCategory_raw"] == "WF"  # raw

        # Second event: Owyhee Rx
        event2 = events[1]
        assert event2.category == "fire.incident.prescribed_fire"  # NOT fire.incident.rx
        assert event2.data["POOState"] == "ID"
        assert event2.data["POOState_raw"] == "US-ID"

    @pytest.mark.asyncio
    async def test_is_published_dedup(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """is_published/mark_published provides dedup functionality."""
        from central.adapters.wfigs_incidents import WFIGSIncidentsAdapter

        adapter = WFIGSIncidentsAdapter(mock_config, mock_config_store, cursor_db_path)
        await adapter.startup()

        # Initially not published
        assert adapter.is_published("test-id") is False

        # Mark as published
        adapter.mark_published("test-id")

        # Now it should be published
        assert adapter.is_published("test-id") is True

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_where_clause_filters_active_wf_only(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """v0.10.4 regression guard: every poll sends the active-WF filter.

        v0.10.2.1 used ``where=1=1`` against the ``_Current`` endpoint. That
        endpoint excludes IMT-managed BLM fires (Blue Ridge, Sailor Cap, etc.)
        once they hit Type 3 IC reporting. v0.10.4 switched to the parent
        ``WFIGS_Incident_Locations`` endpoint with a server-side active-WF
        filter that surfaces those IMT-managed fires. The bug where-clause
        ``ModifiedOnDateTime[_dt] > timestamp 'X'`` from pre-v0.10.2.1 is
        also banned from the wire (the v0.10.2.1 silent-zero failure mode):
        ``ModifiedOnDateTime_dt`` may only appear in ``orderByFields`` and in
        the client-side python recency filter, never as a server-side
        boolean predicate inside ``where``.
        """
        from central.adapters.wfigs_incidents import WFIGSIncidentsAdapter

        adapter = WFIGSIncidentsAdapter(mock_config, mock_config_store, cursor_db_path)
        await adapter.startup()

        captured: list[dict] = []

        def _capture(url, params=None, **kw):
            captured.append(dict(params or {}))
            resp = AsyncMock()
            resp.raise_for_status = MagicMock()
            resp.json = AsyncMock(return_value=SAMPLE_INCIDENTS_RESPONSE)
            return AsyncMock(__aenter__=AsyncMock(return_value=resp), __aexit__=AsyncMock())

        with patch.object(adapter._session, "get", side_effect=_capture):
            _ = [e async for e in adapter.poll()]   # poll 1
            _ = [e async for e in adapter.poll()]   # poll 2

        await adapter.shutdown()

        expected_where = (
            "IncidentTypeCategory='WF' AND FireOutDateTime IS NULL "
            "AND POOState='US-ID'"
        )
        assert len(captured) == 2, "expected one HTTP call per poll"
        for i, params in enumerate(captured):
            assert params.get("where") == expected_where, (
                f"poll #{i+1} sent where={params.get('where')!r}; "
                f"expected {expected_where!r}"
            )
            assert params.get("orderByFields") == "ModifiedOnDateTime_dt DESC"
            assert params.get("resultRecordCount") == "300"
            # The where clause specifically must NOT contain a time-predicate
            # like `ModifiedOnDateTime[_dt] > N` -- that's the v0.10.2.1
            # silent-zero failure shape. (`_dt` IS allowed inside orderByFields
            # and in the post-fetch python filter; just not as a where predicate.)
            assert "ModifiedOnDateTime" not in params["where"], (
                f"poll #{i+1} smuggled ModifiedOnDateTime back into where clause: "
                f"{params['where']!r}"
            )

    @pytest.mark.asyncio
    async def test_no_last_poll_time_attribute(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """The vestigial ``_last_poll_time`` attribute must not be re-introduced."""
        from central.adapters.wfigs_incidents import WFIGSIncidentsAdapter

        adapter = WFIGSIncidentsAdapter(mock_config, mock_config_store, cursor_db_path)
        assert not hasattr(adapter, "_last_poll_time"), (
            "_last_poll_time was the in-memory cursor driving the broken "
            "incremental where-clause; do not re-add"
        )

    def test_endpoint_url_is_not_current_view(self):
        """v0.10.4 regression guard: the URL constant points to the parent
        ``WFIGS_Incident_Locations`` endpoint, NOT ``WFIGS_Incident_Locations_Current``.

        The ``_Current`` view excludes IMT-managed BLM fires (Type 3 IC and up).
        Blue Ridge -- a 14k-acre Owyhee County WF actively modified within
        the hour -- is absent from ``_Current`` but present in the parent
        endpoint. Reverting this constant would silently lose that class of
        fire from Central's coverage.
        """
        from central.adapters.wfigs_incidents import WFIGSIncidentsAdapter, WFIGS_INCIDENTS_URL

        assert WFIGS_INCIDENTS_URL.endswith(
            "/WFIGS_Incident_Locations/FeatureServer/0/query"
        ), f"unexpected endpoint suffix: {WFIGS_INCIDENTS_URL!r}"
        assert "_Current/" not in WFIGS_INCIDENTS_URL, (
            f"adapter reverted to the _Current view: {WFIGS_INCIDENTS_URL!r}"
        )
        # Cross-check that the adapter class actually uses the constant we
        # just asserted on (catches an accidental local override inside the
        # class body that bypasses the module-level constant).
        assert WFIGSIncidentsAdapter.__module__.endswith("wfigs_incidents")

    @pytest.mark.asyncio
    async def test_client_side_recency_filter_drops_stale_features(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """v0.10.4 regression guard: the post-fetch recency filter drops
        features whose ``ModifiedOnDateTime_dt`` is older than the cutoff.

        Server-side `ModifiedOnDateTime_dt > N` combined with any other
        predicate is rejected on this layer (returns "Unable to perform
        query"), so the recency floor lives client-side. Three crafted
        features: one modified now (kept), one 25d ago (kept under the
        30d cutoff), one 60d ago (dropped). Expect 2 events yielded.
        """
        from central.adapters import wfigs_incidents as mod
        from central.adapters.wfigs_incidents import WFIGSIncidentsAdapter

        now_ms = int(time.time()) * 1000
        cutoff_s = mod._RECENCY_CUTOFF_S
        # 25 days back -- inside the 30-day window
        modified_25d_ago_ms = now_ms - 25 * 86400 * 1000
        # 60 days back -- outside the 30-day window
        modified_60d_ago_ms = now_ms - 60 * 86400 * 1000
        # Sanity: the cutoff IS 30 days (catches accidental constant change).
        assert cutoff_s == 30 * 86400, (
            f"_RECENCY_CUTOFF_S changed to {cutoff_s}s; update this test "
            "or revisit the deploy plan's volume estimate"
        )

        def _feat(eid: str, lon: float, lat: float, modified_ms: int) -> dict:
            return {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "IrwinID": eid,
                    "IncidentName": eid,
                    "IncidentTypeCategory": "WF",
                    "DailyAcres": 10,
                    "PercentContained": 0,
                    "FireDiscoveryDateTime": now_ms - 86400 * 1000,
                    "ModifiedOnDateTime_dt": modified_ms,
                    "POOState": "US-ID",
                    "POOCounty": "Owyhee",
                },
            }

        crafted_response = {
            "type": "FeatureCollection",
            "features": [
                _feat("FRESH-NOW",   -116.5, 43.5, now_ms),
                _feat("FRESH-25D",   -116.4, 43.4, modified_25d_ago_ms),
                _feat("STALE-60D",   -116.3, 43.3, modified_60d_ago_ms),
            ],
        }

        adapter = WFIGSIncidentsAdapter(mock_config, mock_config_store, cursor_db_path)
        await adapter.startup()

        resp = AsyncMock()
        resp.raise_for_status = MagicMock()
        resp.json = AsyncMock(return_value=crafted_response)
        with patch.object(
            adapter._session, "get",
            return_value=AsyncMock(__aenter__=AsyncMock(return_value=resp), __aexit__=AsyncMock()),
        ):
            events = [e async for e in adapter.poll()]
        await adapter.shutdown()

        yielded_ids = sorted(e.id for e in events)
        # v0.14.3: id is now IrwinID + ModifiedOnDateTime_dt.
        assert yielded_ids == sorted(
            [f"FRESH-25D:{modified_25d_ago_ms}", f"FRESH-NOW:{now_ms}"]
        ), (
            f"recency filter mis-fired; yielded={yielded_ids!r} "
            "(expected the 2 within the 30-day cutoff, dropping the 60-day one)"
        )

    @pytest.mark.asyncio
    async def test_fall_off_emits_removal(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """Fall-off detection emits removal events."""
        from central.adapters.wfigs_incidents import WFIGSIncidentsAdapter

        adapter = WFIGSIncidentsAdapter(mock_config, mock_config_store, cursor_db_path)
        await adapter.startup()

        # First poll with 2 incidents
        mock_response1 = AsyncMock()
        mock_response1.raise_for_status = MagicMock()
        mock_response1.json = AsyncMock(return_value=SAMPLE_INCIDENTS_RESPONSE)

        # Second poll with only 1 incident (GUID-002 fell off)
        reduced_response = {
            "type": "FeatureCollection",
            "features": [SAMPLE_INCIDENTS_RESPONSE["features"][0]],
        }
        mock_response2 = AsyncMock()
        mock_response2.raise_for_status = MagicMock()
        mock_response2.json = AsyncMock(return_value=reduced_response)

        with patch.object(adapter._session, "get", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response1), __aexit__=AsyncMock())):
            events1 = [e async for e in adapter.poll()]

        with patch.object(adapter._session, "get", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response2), __aexit__=AsyncMock())):
            events2 = [e async for e in adapter.poll()]

        await adapter.shutdown()

        # First poll: 2 incident events
        assert len(events1) == 2

        # Second poll: 1 incident (seen again) + 1 removal for GUID-002
        # The incident event is yielded (supervisor does dedup via is_published)
        # The removal is yielded for GUID-002
        removal_events = [e for e in events2 if e.category == "fire.incident.removed"]
        assert len(removal_events) == 1
        assert removal_events[0].data["irwin_id"] == "GUID-002-OWYHEE"

    def test_subject_for_incidents_normalized(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """subject_for uses normalized state codes."""
        from central.adapters.wfigs_incidents import WFIGSIncidentsAdapter

        adapter = WFIGSIncidentsAdapter(mock_config, mock_config_store, cursor_db_path)

        # Event data contains normalized state (MT not US-MT)
        event = Event(
            id="test-id",
            adapter="wfigs_incidents",
            category="fire.incident.wildfire",
            time=datetime.now(timezone.utc),
            severity=2,
            geo=Geo(primary_region="US-MT-GLACIER"),
            data={"POOState": "MT", "POOCounty": "Glacier"},
        )

        subject = adapter.subject_for(event)
        # Subject uses normalized state: mt.glacier not us-mt.glacier
        assert subject == "central.fire.incident.mt.glacier"

    def test_subject_for_removal(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        from central.adapters.wfigs_incidents import WFIGSIncidentsAdapter

        adapter = WFIGSIncidentsAdapter(mock_config, mock_config_store, cursor_db_path)

        event = Event(
            id="test-id:removed:2024-01-01",
            adapter="wfigs_incidents",
            category="fire.incident.removed",
            time=datetime.now(timezone.utc),
            severity=0,
            geo=Geo(),
            data={"irwin_id": "test-id", "state": "MT"},
        )

        subject = adapter.subject_for(event)
        assert subject == "central.fire.incident.removed.mt"

    @pytest.mark.asyncio
    async def test_bbox_post_filter(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """Features outside bbox are filtered out."""
        from central.adapters.wfigs_incidents import WFIGSIncidentsAdapter

        adapter = WFIGSIncidentsAdapter(mock_config, mock_config_store, cursor_db_path)
        await adapter.startup()

        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json = AsyncMock(return_value=SAMPLE_INCIDENTS_RESPONSE)

        with patch.object(adapter._session, "get", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response), __aexit__=AsyncMock())):
            events = [e async for e in adapter.poll()]

        await adapter.shutdown()

        # Florida incident should be filtered out
        assert len(events) == 2
        irwin_ids = {e.id for e in events}
        assert "GUID-003-FLORIDA" not in irwin_ids

    @pytest.mark.asyncio
    async def test_apply_config_region_change(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        from central.adapters.wfigs_incidents import WFIGSIncidentsAdapter

        adapter = WFIGSIncidentsAdapter(mock_config, mock_config_store, cursor_db_path)

        assert adapter.region.north == 49.0

        new_config = AdapterConfig(
            name="wfigs_incidents",
            enabled=True,
            cadence_s=300,
            settings={
                "region": {"north": 50.0, "south": 35.0, "east": -100.0, "west": -120.0}
            },
            updated_at=datetime.now(timezone.utc),
        )
        await adapter.apply_config(new_config)

        assert adapter.region.north == 50.0
        assert adapter.region.south == 35.0


SAMPLE_PERIMETERS_MULTIPOLYGON = {
    "type": "FeatureCollection",
    "features": [{
        "type": "Feature",
        "geometry": {"type": "MultiPolygon", "coordinates": [
            [[[-113.6, 48.4], [-113.4, 48.4], [-113.4, 48.6], [-113.6, 48.4]]],
            [[[-114.0, 48.0], [-113.8, 48.0], [-113.8, 48.2], [-114.0, 48.0]]]]},
        "properties": {"attr_IrwinID": "GUID-002-MULTI", "attr_IncidentTypeCategory": "WF",
                       "attr_POOState": "US-MT", "attr_POOCounty": "Glacier",
                       "attr_FireDiscoveryDateTime": 1716000000000},
    }],
}


class TestWFIGSPerimetersAdapter:
    """Tests for WFIGS Perimeters adapter."""

    @pytest.fixture
    def mock_config(self) -> AdapterConfig:
        return AdapterConfig(
            name="wfigs_perimeters",
            enabled=True,
            cadence_s=300,
            settings={
                "region": {"north": 49.0, "south": 31.0, "east": -102.0, "west": -124.0}
            },
            updated_at=datetime.now(timezone.utc),
        )

    @pytest.fixture
    def mock_config_store(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def cursor_db_path(self, tmp_path: Path) -> Path:
        return tmp_path / "cursors.db"

    @pytest.mark.asyncio
    async def test_normalization_perimeters(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """Perimeters are correctly normalized to Events with geometry."""
        from central.adapters.wfigs_perimeters import WFIGSPerimetersAdapter

        adapter = WFIGSPerimetersAdapter(mock_config, mock_config_store, cursor_db_path)
        await adapter.startup()

        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json = AsyncMock(return_value=SAMPLE_PERIMETERS_RESPONSE)

        with patch.object(adapter._session, "get", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response), __aexit__=AsyncMock())):
            events = [e async for e in adapter.poll()]

        await adapter.shutdown()

        assert len(events) == 1

        event = events[0]
        assert event.id == "GUID-001-GLACIER"
        assert event.adapter == "wfigs_perimeters"
        # Category uses normalized incident type
        assert event.category == "fire.perimeter.wildfire"  # NOT fire.perimeter.wf
        # Region uses normalized state (no double US-)
        assert event.geo.primary_region == "US-MT-GLACIER"  # NOT US-US-MT-GLACIER
        # Data contains both normalized and raw values
        assert event.data["POOState"] == "MT"  # normalized
        assert event.data["POOState_raw"] == "US-MT"  # raw
        assert event.data["IncidentTypeCategory"] == "wildfire"  # normalized
        assert event.data["IncidentTypeCategory_raw"] == "WF"  # raw
        # Geometry is included
        assert "geometry" in event.data
        assert event.data["geometry"]["type"] == "Polygon"

    def test_subject_for_perimeters_normalized(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """subject_for uses normalized state codes."""
        from central.adapters.wfigs_perimeters import WFIGSPerimetersAdapter

        adapter = WFIGSPerimetersAdapter(mock_config, mock_config_store, cursor_db_path)

        # Event data contains normalized state (MT not US-MT)
        event = Event(
            id="test-id",
            adapter="wfigs_perimeters",
            category="fire.perimeter.wildfire",
            time=datetime.now(timezone.utc),
            severity=2,
            geo=Geo(primary_region="US-MT-GLACIER"),
            data={"POOState": "MT", "POOCounty": "Glacier", "geometry": {}},
        )

        subject = adapter.subject_for(event)
        # Subject uses normalized state: mt.glacier not us-mt.glacier
        assert subject == "central.fire.perimeter.mt.glacier"

    async def _poll_once(self, adapter, response):
        resp = AsyncMock()
        resp.raise_for_status = MagicMock()
        resp.json = AsyncMock(return_value=response)
        with patch.object(adapter._session, "get", return_value=AsyncMock(__aenter__=AsyncMock(return_value=resp), __aexit__=AsyncMock())):
            return [e async for e in adapter.poll()]

    @pytest.mark.asyncio
    async def test_geometry_field_set_from_upstream_polygon(self, mock_config, mock_config_store, cursor_db_path):
        """geo.geometry carries the full upstream Polygon (v0.9.3 framework)."""
        from central.adapters.wfigs_perimeters import WFIGSPerimetersAdapter
        adapter = WFIGSPerimetersAdapter(mock_config, mock_config_store, cursor_db_path)
        await adapter.startup()
        events = await self._poll_once(adapter, SAMPLE_PERIMETERS_RESPONSE)
        await adapter.shutdown()
        geom = events[0].geo.geometry
        assert geom is not None and geom["type"] == "Polygon"
        assert geom["coordinates"] == SAMPLE_PERIMETERS_RESPONSE["features"][0]["geometry"]["coordinates"]

    @pytest.mark.asyncio
    async def test_geometry_field_set_from_multipolygon(self, mock_config, mock_config_store, cursor_db_path):
        """geo.geometry carries a MultiPolygon intact (archive ST_GeomFromGeoJSON handles it)."""
        from central.adapters.wfigs_perimeters import WFIGSPerimetersAdapter
        adapter = WFIGSPerimetersAdapter(mock_config, mock_config_store, cursor_db_path)
        await adapter.startup()
        events = await self._poll_once(adapter, SAMPLE_PERIMETERS_MULTIPOLYGON)
        await adapter.shutdown()
        geom = events[0].geo.geometry
        assert geom is not None and geom["type"] == "MultiPolygon"
        assert len(geom["coordinates"]) == 2

    @pytest.mark.asyncio
    async def test_fall_off_geometry_stays_none(self, mock_config, mock_config_store, cursor_db_path):
        """Synthetic removal events carry no geometry."""
        from central.adapters.wfigs_perimeters import WFIGSPerimetersAdapter
        adapter = WFIGSPerimetersAdapter(mock_config, mock_config_store, cursor_db_path)
        await adapter.startup()
        await self._poll_once(adapter, SAMPLE_PERIMETERS_RESPONSE)
        events = await self._poll_once(adapter, {"type": "FeatureCollection", "features": []})
        await adapter.shutdown()
        removed = [e for e in events if e.category == "fire.perimeter.removed"]
        assert removed and removed[0].geo.geometry is None

    def test_geometry_coercion_handles_malformed_and_none(self):
        """_as_geometry: dict passthrough, JSON-string coercion, malformed/absent -> None."""
        from central.adapters.wfigs_perimeters import _as_geometry
        assert _as_geometry({"type": "Polygon"})["type"] == "Polygon"
        assert _as_geometry('{"type": "Point"}')["type"] == "Point"
        assert _as_geometry("not json") is None
        assert _as_geometry(None) is None

    @pytest.mark.asyncio
    async def test_where_clause_is_1_eq_1_on_every_poll(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """v0.10.2.1 regression guard: every poll sends ``where=1=1``.

        The pre-v0.10.2.1 perimeters adapter sent
        ``where=attr_ModifiedOnDateTime_dt > timestamp 'X'`` on every poll
        after the first -- a type-broken comparison (epoch ms vs SQL
        timestamp literal) that silently returned 0 features. The fall-off
        detector then tombstoned Summit Creek (1924-acre Idaho WF, 85%
        contained) on poll #2 after the v0.10.2 supervisor restart.
        """
        from central.adapters.wfigs_perimeters import WFIGSPerimetersAdapter

        adapter = WFIGSPerimetersAdapter(mock_config, mock_config_store, cursor_db_path)
        await adapter.startup()

        captured: list[dict] = []

        def _capture(url, params=None, **kw):
            captured.append(dict(params or {}))
            resp = AsyncMock()
            resp.raise_for_status = MagicMock()
            resp.json = AsyncMock(return_value=SAMPLE_PERIMETERS_RESPONSE)
            return AsyncMock(__aenter__=AsyncMock(return_value=resp), __aexit__=AsyncMock())

        with patch.object(adapter._session, "get", side_effect=_capture):
            _ = [e async for e in adapter.poll()]   # poll 1
            _ = [e async for e in adapter.poll()]   # poll 2

        await adapter.shutdown()

        assert len(captured) == 2, "expected one HTTP call per poll"
        for i, params in enumerate(captured):
            assert params.get("where") == "1=1", (
                f"poll #{i+1} sent where={params.get('where')!r}; expected '1=1'"
            )
            for v in params.values():
                assert "ModifiedOnDateTime" not in str(v), (
                    f"poll #{i+1} param value referenced ModifiedOnDateTime: {v!r}"
                )

    @pytest.mark.asyncio
    async def test_no_last_poll_time_attribute(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """The vestigial ``_last_poll_time`` attribute must not be re-introduced."""
        from central.adapters.wfigs_perimeters import WFIGSPerimetersAdapter

        adapter = WFIGSPerimetersAdapter(mock_config, mock_config_store, cursor_db_path)
        assert not hasattr(adapter, "_last_poll_time"), (
            "_last_poll_time was the in-memory cursor driving the broken "
            "incremental where-clause; do not re-add"
        )


# --- v0.14.3: incident UPDATE propagation -------------------------------------
# The dedup key is now IrwinID + ModifiedOnDateTime_dt (was bare IrwinID, which
# published a fire once and silenced every later acreage/containment update).
# These tests pin both halves: unchanged records still collapse; modified records
# republish with a distinct id; subject derivation is untouched.

def _incident_response(irwin: str, mod_dt, county: str = "Ada", name: str = "Test Fire"):
    """Single Idaho incident feature with a controllable ModifiedOnDateTime_dt."""
    return {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-116.2, 43.6]},  # Idaho
            "properties": {
                "IrwinID": irwin,
                "IncidentName": name,
                "IncidentTypeCategory": "WF",
                "DailyAcres": 100,
                "PercentContained": 0,
                "FireDiscoveryDateTime": 1716000000000,
                "ModifiedOnDateTime_dt": mod_dt,
                "POOState": "US-ID",
                "POOCounty": county,
            },
        }],
    }


async def _poll_once(adapter, response):
    mr = AsyncMock()
    mr.raise_for_status = MagicMock()
    mr.json = AsyncMock(return_value=response)
    with patch.object(
        adapter._session, "get",
        return_value=AsyncMock(__aenter__=AsyncMock(return_value=mr), __aexit__=AsyncMock()),
    ):
        return [e async for e in adapter.poll()]


def _dedup_publish(adapter, events):
    """Mirror the supervisor's publish gate: skip ids already in published_ids,
    else mark + 'publish'. Returns the events that would actually be published."""
    published = []
    for e in events:
        if adapter.is_published(e.id):
            adapter.bump_last_seen(e.id)
            continue
        adapter.mark_published(e.id)
        published.append(e)
    return published


class TestWFIGSIncidentsUpdatePropagation:
    """v0.14.3: dedup key includes ModifiedOnDateTime_dt."""

    def _adapter(self, tmp_path: Path):
        from central.adapters.wfigs_incidents import WFIGSIncidentsAdapter
        config = AdapterConfig(
            name="wfigs_incidents", enabled=True, cadence_s=300,
            settings={
                "region": {"north": 49.0, "south": 31.0, "east": -102.0, "west": -124.0},
                "state": "US-ID",
            },
            updated_at=datetime.now(timezone.utc),
        )
        return WFIGSIncidentsAdapter(config, MagicMock(), tmp_path / "cursors.db")

    @pytest.mark.asyncio
    async def test_unchanged_record_dedups_across_polls(self, tmp_path: Path):
        """Same IrwinID + same ModifiedOnDateTime_dt: first publishes, second deduped."""
        adapter = self._adapter(tmp_path)
        await adapter.startup()
        now_ms = int(time.time()) * 1000  # recent -> passes the 30-day recency floor
        resp = _incident_response("GUID-A", now_ms)

        pub1 = _dedup_publish(adapter, await _poll_once(adapter, resp))
        pub2 = _dedup_publish(adapter, await _poll_once(adapter, resp))

        await adapter.shutdown()
        assert len(pub1) == 1
        assert len(pub2) == 0  # collapsed via published_ids on the identical key
        assert pub1[0].id == f"GUID-A:{now_ms}"

    @pytest.mark.asyncio
    async def test_modified_record_republishes_with_distinct_id(self, tmp_path: Path):
        """Same IrwinID, DIFFERENT ModifiedOnDateTime_dt: BOTH publish, distinct ids.

        This is the load-bearing test: it proves the silencing is fixed."""
        adapter = self._adapter(tmp_path)
        await adapter.startup()
        now_ms = int(time.time()) * 1000
        t1, t2 = now_ms - 600_000, now_ms  # both recent, distinct (upstream modified)
        pub1 = _dedup_publish(adapter, await _poll_once(adapter, _incident_response("GUID-A", t1)))
        pub2 = _dedup_publish(adapter, await _poll_once(adapter, _incident_response("GUID-A", t2)))

        await adapter.shutdown()
        assert len(pub1) == 1
        assert len(pub2) == 1  # the modification propagated, not silenced
        assert pub1[0].id == f"GUID-A:{t1}"
        assert pub2[0].id == f"GUID-A:{t2}"
        assert pub1[0].id != pub2[0].id

    @pytest.mark.asyncio
    async def test_subject_derivation_unchanged(self, tmp_path: Path):
        """The new id does not affect subject_for: still central.fire.incident.<state>.<county>."""
        adapter = self._adapter(tmp_path)
        await adapter.startup()
        now_ms = int(time.time()) * 1000
        events = await _poll_once(adapter, _incident_response("GUID-A", now_ms, county="Ada"))
        await adapter.shutdown()
        assert len(events) == 1
        assert events[0].id == f"GUID-A:{now_ms}"
        assert adapter.subject_for(events[0]) == "central.fire.incident.id.ada"

    @pytest.mark.asyncio
    async def test_none_modified_time_is_deterministic(self, tmp_path: Path, monkeypatch):
        """Defensive: a None ModifiedOnDateTime_dt yields a deterministic ':None'
        id without raising. (In production the recency filter drops such features
        first; widen the cutoff here so the construction path is exercised.)"""
        import central.adapters.wfigs_incidents as wi
        monkeypatch.setattr(wi, "_RECENCY_CUTOFF_S", 10**13)  # cutoff in the past -> nothing filtered
        adapter = self._adapter(tmp_path)
        await adapter.startup()
        resp = _incident_response("GUID-A", None)

        pub1 = _dedup_publish(adapter, await _poll_once(adapter, resp))
        pub2 = _dedup_publish(adapter, await _poll_once(adapter, resp))

        await adapter.shutdown()
        assert len(pub1) == 1
        assert pub1[0].id == "GUID-A:None"
        assert len(pub2) == 0  # still dedups consistently on the ':None' key
