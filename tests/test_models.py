"""Smoke tests for Central models and CloudEvents wire format."""

from datetime import datetime, timezone

import pytest

from central.models import Event, Geo
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
        adapter="nws",
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



class TestCloudEventsWire:
    """Tests for CloudEvents wire format."""

    def test_required_fields_present(
        self, sample_event: Event, sample_config: Config
    ) -> None:
        """Required CloudEvents fields are present.

        v0.10.8: msg_id is now category-discriminated. The CloudEvents
        envelope ``id`` stays the bare ``event.id`` per CE spec; only the
        ``Nats-Msg-Id`` header (= the second return value of wrap_event)
        carries the ``"{event.id}:{event.category}"`` shape.
        """
        envelope, msg_id = wrap_event(sample_event, sample_config)

        assert msg_id == f"{sample_event.id}:{sample_event.category}"
        assert envelope["id"] == sample_event.id  # CE spec: bare event.id
        assert envelope["source"] == sample_config.cloudevents.source
        assert envelope["type"] == "central.wx.alert.severe_thunderstorm_warning.v1"
        assert envelope["specversion"] == "1.0"
        assert "time" in envelope
        assert envelope["datacontenttype"] == "application/json"
        assert "data" in envelope

    def test_msgid_disambiguates_incident_vs_perimeter_same_guid(
        self, sample_geo: Geo, sample_config: Config
    ) -> None:
        """v0.10.8 regression guard: Summit Creek dedup collision.

        On 2026-06-07 a wfigs poll yielded both an incident envelope and
        a perimeter envelope for the same IRWIN ``{6B1C6EB1-...}`` within
        JetStream's 2-min dedup window on CENTRAL_FIRE. Both publishes
        carried the bare IRWIN as ``Nats-Msg-Id`` (JetStream's dedup key
        is per-stream, NOT per-subject), so the second publish was
        silently dropped -- supervisor logs showed 'Published event' for
        both but only one landed in the stream. The fix adds an
        ``:{event.category}`` suffix to the msg_id so the two are
        distinct without changing ``envelope["id"]`` (CE spec) or the
        adapter-side event.id contract.
        """
        irwin = "{6B1C6EB1-30F7-4613-9C58-4801DC8FD822}"  # Summit Creek
        incident = Event(
            id=irwin,
            adapter="wfigs_incidents",
            category="fire.incident.wildfire",
            time=datetime(2026, 6, 7, 6, 25, 5, tzinfo=timezone.utc),
            severity=2,
            geo=sample_geo,
            data={},
        )
        perimeter = Event(
            id=irwin,
            adapter="wfigs_perimeters",
            category="fire.perimeter.wildfire",
            time=datetime(2026, 6, 7, 6, 25, 5, tzinfo=timezone.utc),
            severity=2,
            geo=sample_geo,
            data={},
        )

        _, incident_msgid = wrap_event(incident, sample_config)
        _, perimeter_msgid = wrap_event(perimeter, sample_config)

        assert incident_msgid != perimeter_msgid
        assert incident_msgid == f"{irwin}:fire.incident.wildfire"
        assert perimeter_msgid == f"{irwin}:fire.perimeter.wildfire"

    @pytest.mark.parametrize("category", [
        "fire.incident.wildfire",
        "fire.perimeter.wildfire",
        "wx.alert.severe_thunderstorm_warning",
        "quake.usgs_quake",
        "hydro.00060.usgs.13162225",
    ])
    def test_msgid_shape_is_id_colon_category(
        self, sample_geo: Geo, sample_config: Config, category: str
    ) -> None:
        """Every wrap_event call returns msg_id == ``f'{id}:{category}'``.

        Covers both multi-class adapters (where the discriminator is
        load-bearing) and single-class adapters (where it's a no-op for
        dedup but still consistent shape across the codebase).
        """
        event = Event(
            id=f"test:{category}:42",
            adapter="test",
            category=category,
            time=datetime(2026, 6, 7, 6, 25, 5, tzinfo=timezone.utc),
            severity=1,
            geo=sample_geo,
            data={},
        )
        _, msg_id = wrap_event(event, sample_config)
        assert msg_id == f"{event.id}:{category}"

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
            adapter="nws",
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
