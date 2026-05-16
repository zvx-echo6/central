"""Smoke tests for Central models and CloudEvents wire format."""

from datetime import datetime, timezone

import pytest

from central.models import Event, Geo, subject_for_event
from central.config import NWSAdapterConfig, CloudEventsConfig, NATSConfig, PostgresConfig, Config
from central.cloudevents_wire import wrap_event


@pytest.fixture
def sample_geo() -> Geo:
    """Sample Geo object for testing."""
    return Geo(
        centroid=(-116.2, 43.6),
        bbox=(-116.5, 43.4, -115.9, 43.8),
        regions=["US-ID-Ada", "US-ID-Canyon"],
        primary_region="US-ID-Ada",
    )


@pytest.fixture
def sample_event(sample_geo: Geo) -> Event:
    """Sample Event object for testing."""
    return Event(
        id="urn:central:nws:alert:KBOI-202401151200-SVR",
        source="central/adapters/nws",
        category="wx.alert.severe_thunderstorm_warning",
        time=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        expires=datetime(2024, 1, 15, 13, 0, 0, tzinfo=timezone.utc),
        severity=3,
        geo=sample_geo,
        data={"headline": "Severe Thunderstorm Warning", "urgency": "Immediate"},
    )


@pytest.fixture
def sample_config() -> Config:
    """Sample Config object for testing."""
    return Config(
        adapters={
            "nws": NWSAdapterConfig(
                enabled=True,
                cadence_s=60,
                states=["ID", "MT"],
                contact_email="test@example.com",
            )
        },
        cloudevents=CloudEventsConfig(
            type_prefix="central",
            source="central.local",
            schema_version="1.0",
        ),
        nats=NATSConfig(url="nats://localhost:4222"),
        postgres=PostgresConfig(dsn="postgresql://user:pass@localhost/db"),
    )


class TestSubjectForEvent:
    """Tests for subject_for_event helper."""

    def test_county_subject(self, sample_event: Event) -> None:
        """County codes produce county subject."""
        subject = subject_for_event(sample_event)
        assert subject == "central.wx.alert.us.id.county.ada"

    def test_zone_subject(self, sample_geo: Geo) -> None:
        """Zone codes produce zone subject."""
        geo = Geo(
            centroid=sample_geo.centroid,
            bbox=sample_geo.bbox,
            regions=["US-ID-Z033"],
            primary_region="US-ID-Z033",
        )
        event = Event(
            id="test-zone",
            source="test",
            category="wx.alert.winter_storm_warning",
            time=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            geo=geo,
            data={},
        )
        subject = subject_for_event(event)
        assert subject == "central.wx.alert.us.id.zone.z033"

    def test_unknown_subject(self, sample_event: Event) -> None:
        """Missing primary_region produces unknown subject."""
        geo = Geo(regions=[], primary_region=None)
        event = Event(
            id="test-unknown",
            source="test",
            category="wx.alert.test",
            time=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            geo=geo,
            data={},
        )
        subject = subject_for_event(event)
        assert subject == "central.wx.alert.us.unknown"

    def test_custom_prefix(self, sample_event: Event) -> None:
        """Custom prefix is used in subject."""
        subject = subject_for_event(sample_event, prefix="myapp.events")
        assert subject == "myapp.events.alert.us.id.county.ada"


class TestCloudEventsWire:
    """Tests for CloudEvents wire format."""

    def test_required_fields_present(
        self, sample_event: Event, sample_config: Config
    ) -> None:
        """Required CloudEvents fields are present."""
        envelope, msg_id = wrap_event(sample_event, sample_config)

        assert msg_id == sample_event.id
        assert envelope["id"] == sample_event.id
        assert envelope["source"] == sample_config.cloudevents.source
        assert envelope["type"] == "central.wx.alert.severe_thunderstorm_warning.v1"
        assert envelope["specversion"] == "1.0"
        assert "time" in envelope
        assert envelope["datacontenttype"] == "application/json"
        assert "data" in envelope

    def test_extension_attributes_lowercase(
        self, sample_event: Event, sample_config: Config
    ) -> None:
        """Extension attributes are lowercase with no underscores."""
        envelope, _ = wrap_event(sample_event, sample_config)

        # Check that extension attributes exist and are lowercase
        assert envelope["centralschemaversion"] == "1.0"
        assert envelope["centralcategory"] == "wx.alert.severe_thunderstorm_warning"
        assert envelope["centralseverity"] == 3

        # Verify no uppercase or underscores in extension names
        for key in ["centralschemaversion", "centralcategory", "centralseverity"]:
            assert key.islower()
            assert "_" not in key

    def test_severity_none_omits_centralseverity(
        self, sample_geo: Geo, sample_config: Config
    ) -> None:
        """When severity is None, centralseverity is omitted entirely."""
        event = Event(
            id="test-no-severity",
            source="test",
            category="wx.alert.test",
            time=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            severity=None,  # Explicitly None
            geo=sample_geo,
            data={},
        )

        envelope, _ = wrap_event(event, sample_config)

        # centralseverity should not be present at all
        assert "centralseverity" not in envelope
        # Other extensions should still be present
        assert "centralschemaversion" in envelope
        assert "centralcategory" in envelope
