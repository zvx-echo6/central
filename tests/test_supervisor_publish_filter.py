"""Supervisor publish-time monitoring-area filter (v0.10.2).

Covers the wire-up between ``subject_for`` and ``_publish_event`` in
``Supervisor._run_adapter_loop``: out-of-area drops, in-area publishes,
null-geom passes, invalid-geom fails open, and the refresh-loop reload.
"""
import asyncio
import logging
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from central import supervisor as sup_mod
from central.config_models import AdapterConfig, EnrichmentConfig
from central.models import Event, Geo
from central.monitoring_area import MonitoringArea

IDAHO = MonitoringArea(north=44.5, south=41.8, east=-111.0, west=-117.5)
SPOKANE = (-117.4, 47.6)
BOISE = (-114.0, 43.5)
NYC = (-74.0, 40.7)


def _ev(eid: str, geo: Geo) -> Event:
    return Event(
        id=eid, adapter="mock", category="mock.test",
        time=datetime.now(timezone.utc), geo=geo, data={},
    )


class _MockAdapter:
    requires_api_key = None
    enrichment_locations = ()
    bypass_bbox_filter = False  # v0.14.2: real adapters inherit this from
    # SourceAdapter; the mock declares it so _run_adapter_loop's direct attr
    # access works. Bypass tests use _BypassMockAdapter (overrides True).

    def __init__(self, config, *_args) -> None:
        self.config = config
        self.cadence_s = config.cadence_s
        self.events: list[Event] = []
        self.published: set[str] = set()
        self.done = asyncio.Event()
        self._called = False

    async def startup(self): ...
    async def shutdown(self): ...
    async def apply_config(self, c): ...

    async def poll(self):
        if not self._called:
            self._called = True
            for e in self.events:
                yield e
            self.done.set()

    def is_published(self, eid): return eid in self.published
    def mark_published(self, eid): self.published.add(eid)
    def bump_last_seen(self, eid): ...
    def sweep_old_ids(self): return 0
    def subject_for(self, e): return f"central.test.{e.id}"


@pytest.fixture
def sup_factory():
    """Build a supervisor with mocked NATS + ConfigStore; spy on _publish_event."""
    def _build(area: MonitoringArea | None):
        nc = AsyncMock(); js = AsyncMock(); js.publish = AsyncMock()
        nc.jetstream = MagicMock(return_value=js)
        store = MagicMock()
        store.list_streams = AsyncMock(return_value=[])
        store.get_stream = AsyncMock(return_value=None)
        store.set_adapter_last_error = AsyncMock()
        store.get_api_key = AsyncMock(return_value=None)
        store.get_monitoring_area = AsyncMock(return_value=area)
        store.get_monitoring_areas = AsyncMock(
            return_value=[area] if area is not None else []
        )
        config_source = MagicMock()
        config_source.get_enrichment_config = AsyncMock(return_value=EnrichmentConfig())
        sup = sup_mod.Supervisor(
            config_source=config_source, config_store=store,
            nats_url="nats://x:4222", cloudevents_config=None,
            enrichment_config=EnrichmentConfig(),
        )
        sup._nc = nc; sup._js = nc.jetstream()
        sup._monitoring_area = area
        sup._publish_event = AsyncMock()
        sup._publish_meta = AsyncMock()
        sup._adapters["mock"] = _MockAdapter
        return sup
    return _build


async def _drive(sup, events, adapter_cls=_MockAdapter):
    adapter = adapter_cls(MagicMock(cadence_s=3600))
    adapter.events = events
    config = AdapterConfig(
        name="mock", enabled=True, cadence_s=3600, settings={},
        paused_at=None, updated_at=datetime.now(timezone.utc),
    )
    state = sup_mod.AdapterState(name="mock", config=config, adapter=adapter)
    task = asyncio.create_task(sup._run_adapter_loop(state))
    try:
        await asyncio.wait_for(adapter.done.wait(), timeout=5.0)
        await asyncio.sleep(0)
    finally:
        # Cancel ONLY this loop -- never touch sup._shutdown_event so callers
        # can drive the same supervisor through multiple poll cycles.
        task.cancel()
        try: await task
        except (asyncio.CancelledError, Exception): pass
    return adapter


# --- verdict matrix -----------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("area,geo,should_publish,should_drop", [
    pytest.param(None, Geo(centroid=NYC), True, False, id="no-area-publishes-out-of-bbox"),
    pytest.param(IDAHO, Geo(centroid=BOISE), True, False, id="in-bounds-publishes"),
    pytest.param(IDAHO, Geo(centroid=NYC), False, True, id="out-of-bounds-NY-drops"),
    pytest.param(IDAHO, Geo(centroid=SPOKANE), False, True, id="out-of-bounds-Spokane-drops"),
    pytest.param(IDAHO, Geo(), True, False, id="null-geom-publishes"),
    pytest.param(IDAHO, Geo(geometry={"type": "Nonsense"}), True, False, id="invalid-geom-fail-open"),
])
async def test_verdict(sup_factory, area, geo, should_publish, should_drop):
    sup = sup_factory(area)
    a = await _drive(sup, [_ev("e1", geo)])
    if should_publish:
        assert sup._publish_event.await_count == 1
        assert a.published == {"e1"}
    else:
        sup._publish_event.assert_not_called()
        # CRITICAL forward-only: drops MUST NOT mark_published, else widening
        # the bbox can't re-deliver -- see [[feedback_dedup_forward_only]].
        assert a.published == set()
    assert (sup._dropped_publish == {"mock": 1}) is should_drop


@pytest.mark.asyncio
async def test_mixed_batch_partitions_correctly(sup_factory):
    sup = sup_factory(IDAHO)
    a = await _drive(sup, [
        _ev("in1", Geo(centroid=BOISE)),
        _ev("out1", Geo(centroid=NYC)),
        _ev("null1", Geo()),
        _ev("out2", Geo(centroid=SPOKANE)),
    ])
    assert sup._publish_event.await_count == 2
    assert a.published == {"in1", "null1"}
    assert sup._dropped_publish == {"mock": 2}


@pytest.mark.asyncio
async def test_widening_area_re_publishes_previously_dropped(sup_factory):
    """Forward-only invariant: drops are reversible -- never mark_published."""
    sup = sup_factory(IDAHO)
    spokane = _ev("spokane", Geo(centroid=SPOKANE))
    # The same supervisor drives two polls so the second sees the same id
    # AFTER the bbox widens. Need a fresh adapter each call because _drive's
    # MockAdapter signals done once per instance.
    a1 = await _drive(sup, [spokane])
    sup._publish_event.assert_not_called()
    assert a1.published == set()
    sup._monitoring_area = MonitoringArea(
        north=48.5, south=41.8, east=-111.0, west=-118.0
    )
    a2 = await _drive(sup, [spokane])
    assert sup._publish_event.await_count == 1
    assert a2.published == {"spokane"}


@pytest.mark.asyncio
async def test_refresh_loop_reloads_area_and_logs_summary(
    sup_factory, caplog, monkeypatch
):
    sup = sup_factory(None)
    sup._dropped_publish = {"mock": 7}
    monkeypatch.setattr(sup_mod, "MONITORING_AREA_REFRESH_S", 0.05)
    sup._config_store.get_monitoring_areas = AsyncMock(return_value=[IDAHO])
    with caplog.at_level(logging.INFO):
        task = asyncio.create_task(sup._refresh_monitoring_area_loop())
        await asyncio.sleep(0.15)
        sup._shutdown_event.set()
        try: await asyncio.wait_for(task, timeout=1.0)
        except (asyncio.TimeoutError, asyncio.CancelledError): task.cancel()
    assert sup._monitoring_area == IDAHO
    assert any(
        "publish bbox filter drop summary" in r.message for r in caplog.records
    )


# --- v0.14.2: global-by-design satellite telemetry bypasses the bbox filter ---

class _BypassMockAdapter(_MockAdapter):
    """Stand-in for a global-by-design adapter (sat_positions/sat_orbits)."""
    bypass_bbox_filter = True


class TestBypassBboxAdapters:
    @pytest.mark.asyncio
    async def test_sat_positions_publishes_when_out_of_bounds(self, sup_factory):
        """Centroid over NYC (outside Idaho) still publishes; not dropped."""
        sup = sup_factory(IDAHO)
        a = await _drive(
            sup, [_ev("iss", Geo(centroid=NYC))], adapter_cls=_BypassMockAdapter
        )
        assert sup._publish_event.await_count == 1
        assert a.published == {"iss"}
        assert sup._dropped_publish == {}
        assert sup._bypassed_publish == {"mock": 1}

    @pytest.mark.asyncio
    async def test_sat_orbits_publishes_when_linestring_outside_idaho(self, sup_factory):
        """An equatorial forward-track LineString never intersects Idaho, yet publishes."""
        sup = sup_factory(IDAHO)
        equator = Geo(geometry={
            "type": "LineString",
            "coordinates": [[0.0, 0.0], [10.0, 0.0], [20.0, 1.0]],
        })
        a = await _drive(
            sup, [_ev("orbit1", equator)], adapter_cls=_BypassMockAdapter
        )
        assert sup._publish_event.await_count == 1
        assert a.published == {"orbit1"}
        assert sup._dropped_publish == {}
        assert sup._bypassed_publish == {"mock": 1}

    @pytest.mark.asyncio
    async def test_tomtom_flow_still_drops_when_out_of_bounds(self, sup_factory):
        """Regression guard: a non-bypass adapter is still filtered (NYC drops)."""
        sup = sup_factory(IDAHO)
        a = await _drive(sup, [_ev("flow1", Geo(centroid=NYC))])  # _MockAdapter: bypass False
        sup._publish_event.assert_not_called()
        assert a.published == set()
        assert sup._dropped_publish == {"mock": 1}
        assert sup._bypassed_publish == {}
