"""Tests for NOAA SWPC space weather adapters."""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from central.config_models import AdapterConfig
from central.models import Event


# Frozen fixtures captured from upstream feeds; real shapes.
SAMPLE_ALERTS = [
    {
        "product_id": "EF3A",
        "issue_datetime": "2026-05-19 05:14:59.780",
        "message": (
            "Space Weather Message Code: ALTEF3\r\nSerial Number: 3689\r\n"
            "Issue Time: 2026 May 19 0514 UTC\r\n\r\n"
            "ALERT: Electron 2MeV Integral Flux exceeded 1000pfu \n"
            "Threshold Reached: 2026 May 16 1740 UTC\n"
            "Station: GOES-19\n"
        ),
    },
    {
        "product_id": "K05A",
        "issue_datetime": "2026-05-15 14:30:00.000",
        "message": (
            "Space Weather Message Code: ALTK05\r\nSerial Number: 100\r\n"
            "Issue Time: 2026 May 15 1430 UTC\r\n\r\n"
            "ALERT: Geomagnetic K-index of 5\n"
        ),
    },
    {
        "product_id": "K07A",
        "issue_datetime": "2026-05-15 18:00:00.000",
        "message": "Space Weather Message Code: ALTK07\r\nSerial Number: 101\r\n",
    },
]

SAMPLE_KINDEX = [
    {"time_tag": "2026-05-12T00:00:00", "Kp": 0.67, "a_running": 3, "station_count": 8},
    {"time_tag": "2026-05-12T03:00:00", "Kp": 5.33, "a_running": 30, "station_count": 8},
    {"time_tag": "2026-05-12T06:00:00", "Kp": 8.0,  "a_running": 100, "station_count": 8},
]

SAMPLE_PROTONS = [
    {"time_tag": "2026-05-18T05:35:00Z", "satellite": 19, "flux": 7.09, "energy": ">=1 MeV"},
    {"time_tag": "2026-05-18T05:35:00Z", "satellite": 19, "flux": 0.21, "energy": ">=10 MeV"},
    {"time_tag": "2026-05-18T05:40:00Z", "satellite": 19, "flux": 7.10, "energy": ">=1 MeV"},
]


def _config(name: str, cadence: int) -> AdapterConfig:
    return AdapterConfig(
        name=name,
        enabled=True,
        cadence_s=cadence,
        settings={},
        updated_at=datetime.now(timezone.utc),
    )


class TestSWPCCommon:
    """Tests for swpc_common helpers."""

    def test_parse_swpc_timestamp_alerts(self):
        from central.adapters.swpc_common import parse_swpc_timestamp

        dt = parse_swpc_timestamp("2026-05-19 05:14:59.780", "alerts")
        assert dt == datetime(2026, 5, 19, 5, 14, 59, 780000, tzinfo=timezone.utc)

    def test_parse_swpc_timestamp_alerts_no_fraction(self):
        from central.adapters.swpc_common import parse_swpc_timestamp

        dt = parse_swpc_timestamp("2026-05-19 05:14:59", "alerts")
        assert dt == datetime(2026, 5, 19, 5, 14, 59, tzinfo=timezone.utc)

    def test_parse_swpc_timestamp_kindex(self):
        from central.adapters.swpc_common import parse_swpc_timestamp

        dt = parse_swpc_timestamp("2026-05-12T03:00:00", "kindex")
        assert dt == datetime(2026, 5, 12, 3, 0, 0, tzinfo=timezone.utc)

    def test_parse_swpc_timestamp_protons(self):
        from central.adapters.swpc_common import parse_swpc_timestamp

        dt = parse_swpc_timestamp("2026-05-18T05:35:00Z", "protons")
        assert dt == datetime(2026, 5, 18, 5, 35, 0, tzinfo=timezone.utc)

    def test_parse_swpc_timestamp_empty(self):
        from central.adapters.swpc_common import parse_swpc_timestamp

        assert parse_swpc_timestamp("", "alerts") is None
        assert parse_swpc_timestamp(None, "alerts") is None

    def test_severity_from_kp_boundaries(self):
        from central.adapters.swpc_common import severity_from_kp

        assert severity_from_kp(None) == 0
        assert severity_from_kp(0) == 0
        assert severity_from_kp(4.5) == 0
        assert severity_from_kp(4.9) == 0
        assert severity_from_kp(5.0) == 1
        assert severity_from_kp(5.99) == 1
        assert severity_from_kp(6.0) == 2
        assert severity_from_kp(6.99) == 2
        assert severity_from_kp(7.0) == 3
        assert severity_from_kp(7.99) == 3
        assert severity_from_kp(8.0) == 4
        assert severity_from_kp(9.0) == 4

    def test_severity_from_alert_product_id(self):
        from central.adapters.swpc_common import severity_from_alert_product_id

        assert severity_from_alert_product_id(None) == 0
        assert severity_from_alert_product_id("") == 0
        assert severity_from_alert_product_id("EF3A") == 0
        assert severity_from_alert_product_id("BHIS") == 0
        assert severity_from_alert_product_id("K04A") == 0
        assert severity_from_alert_product_id("K05A") == 1
        assert severity_from_alert_product_id("K05W") == 1
        assert severity_from_alert_product_id("K06A") == 2
        assert severity_from_alert_product_id("K07A") == 3
        assert severity_from_alert_product_id("K08A") == 4
        assert severity_from_alert_product_id("K09A") == 4


class TestSWPCAlertsAdapter:
    """Tests for SWPCAlertsAdapter."""

    @pytest.mark.asyncio
    async def test_alerts_normalization(self, tmp_path: Path):
        from central.adapters.swpc_alerts import SWPCAlertsAdapter

        adapter = SWPCAlertsAdapter(
            _config("swpc_alerts", 300), MagicMock(), tmp_path / "cursors.db"
        )
        adapter._fetch = AsyncMock(return_value=SAMPLE_ALERTS)

        await adapter.startup()
        events: list[Event] = [e async for e in adapter.poll()]
        await adapter.shutdown()

        assert len(events) == 3

        ef3a = events[0]
        assert ef3a.adapter == "swpc_alerts"
        assert ef3a.category == "space.alert"
        assert ef3a.id == "EF3A|2026-05-19 05:14:59.780"
        assert ef3a.time == datetime(2026, 5, 19, 5, 14, 59, 780000, tzinfo=timezone.utc)
        assert ef3a.severity == 0
        assert ef3a.data["product_id"] == "EF3A"
        assert ef3a.geo.centroid is None
        assert ef3a.geo.regions == []
        assert ef3a.geo.primary_region is None

        k05a = events[1]
        assert k05a.severity == 1
        k07a = events[2]
        assert k07a.severity == 3

    @pytest.mark.asyncio
    async def test_alerts_dedup(self, tmp_path: Path):
        from central.adapters.swpc_alerts import SWPCAlertsAdapter

        adapter = SWPCAlertsAdapter(
            _config("swpc_alerts", 300), MagicMock(), tmp_path / "cursors.db"
        )
        adapter._fetch = AsyncMock(return_value=SAMPLE_ALERTS)

        await adapter.startup()
        first_pass = [e async for e in adapter.poll()]
        second_pass = [e async for e in adapter.poll()]
        await adapter.shutdown()

        assert len(first_pass) == 3
        assert len(second_pass) == 0

    @pytest.mark.asyncio
    async def test_alerts_subject_for(self, tmp_path: Path):
        from central.adapters.swpc_alerts import SWPCAlertsAdapter
        from central.models import Geo

        adapter = SWPCAlertsAdapter(
            _config("swpc_alerts", 300), MagicMock(), tmp_path / "cursors.db"
        )
        event = Event(
            id="EF3A|2026-05-19 05:14:59.780",
            adapter="swpc_alerts",
            category="space.alert",
            time=datetime(2026, 5, 19, 5, 14, 59, tzinfo=timezone.utc),
            severity=0,
            geo=Geo(),
            data={"product_id": "EF3A"},
        )
        assert adapter.subject_for(event) == "central.space.alert.ef3a"

        event_k = Event(
            id="K05A|...",
            adapter="swpc_alerts",
            category="space.alert",
            time=datetime(2026, 5, 15, tzinfo=timezone.utc),
            severity=1,
            geo=Geo(),
            data={"product_id": "K05A"},
        )
        assert adapter.subject_for(event_k) == "central.space.alert.k05a"


class TestSWPCKindexAdapter:
    """Tests for SWPCKindexAdapter."""

    @pytest.mark.asyncio
    async def test_kindex_normalization(self, tmp_path: Path):
        from central.adapters.swpc_kindex import SWPCKindexAdapter

        adapter = SWPCKindexAdapter(
            _config("swpc_kindex", 600), MagicMock(), tmp_path / "cursors.db"
        )
        adapter._fetch = AsyncMock(return_value=SAMPLE_KINDEX)

        await adapter.startup()
        events = [e async for e in adapter.poll()]
        await adapter.shutdown()

        assert len(events) == 3
        quiet, g1, g4 = events
        assert quiet.category == "space.kindex"
        assert quiet.id == "2026-05-12T00:00:00"
        assert quiet.severity == 0
        assert quiet.data["Kp"] == 0.67
        assert g1.severity == 1
        assert g4.severity == 4
        assert g4.time == datetime(2026, 5, 12, 6, 0, 0, tzinfo=timezone.utc)

    @pytest.mark.asyncio
    async def test_kindex_dedup(self, tmp_path: Path):
        from central.adapters.swpc_kindex import SWPCKindexAdapter

        adapter = SWPCKindexAdapter(
            _config("swpc_kindex", 600), MagicMock(), tmp_path / "cursors.db"
        )
        adapter._fetch = AsyncMock(return_value=SAMPLE_KINDEX)

        await adapter.startup()
        first_pass = [e async for e in adapter.poll()]
        second_pass = [e async for e in adapter.poll()]
        await adapter.shutdown()

        assert len(first_pass) == 3
        assert len(second_pass) == 0

    @pytest.mark.asyncio
    async def test_kindex_subject_for(self, tmp_path: Path):
        from central.adapters.swpc_kindex import SWPCKindexAdapter
        from central.models import Geo

        adapter = SWPCKindexAdapter(
            _config("swpc_kindex", 600), MagicMock(), tmp_path / "cursors.db"
        )
        event = Event(
            id="2026-05-12T03:00:00",
            adapter="swpc_kindex",
            category="space.kindex",
            time=datetime(2026, 5, 12, 3, tzinfo=timezone.utc),
            severity=1,
            geo=Geo(),
            data={"Kp": 5.33},
        )
        assert adapter.subject_for(event) == "central.space.kindex"


class TestSWPCProtonsAdapter:
    """Tests for SWPCProtonsAdapter."""

    @pytest.mark.asyncio
    async def test_protons_normalization(self, tmp_path: Path):
        from central.adapters.swpc_protons import SWPCProtonsAdapter

        adapter = SWPCProtonsAdapter(
            _config("swpc_protons", 600), MagicMock(), tmp_path / "cursors.db"
        )
        adapter._fetch = AsyncMock(return_value=SAMPLE_PROTONS)

        await adapter.startup()
        events = [e async for e in adapter.poll()]
        await adapter.shutdown()

        assert len(events) == 3
        first = events[0]
        assert first.category == "space.proton_flux"
        assert first.id == "2026-05-18T05:35:00Z|>=1 MeV"
        assert first.severity == 0
        assert first.data["energy"] == ">=1 MeV"
        assert first.data["flux"] == 7.09
        assert first.time == datetime(2026, 5, 18, 5, 35, 0, tzinfo=timezone.utc)
        assert first.geo.centroid is None
        assert first.geo.regions == []

        # Same time_tag, different energy -> distinct event_id
        assert events[1].id == "2026-05-18T05:35:00Z|>=10 MeV"

    @pytest.mark.asyncio
    async def test_protons_dedup(self, tmp_path: Path):
        from central.adapters.swpc_protons import SWPCProtonsAdapter

        adapter = SWPCProtonsAdapter(
            _config("swpc_protons", 600), MagicMock(), tmp_path / "cursors.db"
        )
        adapter._fetch = AsyncMock(return_value=SAMPLE_PROTONS)

        await adapter.startup()
        first_pass = [e async for e in adapter.poll()]
        second_pass = [e async for e in adapter.poll()]
        await adapter.shutdown()

        assert len(first_pass) == 3
        assert len(second_pass) == 0

    @pytest.mark.asyncio
    async def test_protons_subject_for(self, tmp_path: Path):
        from central.adapters.swpc_protons import SWPCProtonsAdapter
        from central.models import Geo

        adapter = SWPCProtonsAdapter(
            _config("swpc_protons", 600), MagicMock(), tmp_path / "cursors.db"
        )
        event = Event(
            id="2026-05-18T05:35:00Z|>=10 MeV",
            adapter="swpc_protons",
            category="space.proton_flux",
            time=datetime(2026, 5, 18, 5, 35, 0, tzinfo=timezone.utc),
            severity=0,
            geo=Geo(),
            data={"energy": ">=10 MeV", "flux": 0.21},
        )
        assert adapter.subject_for(event) == "central.space.proton_flux"
