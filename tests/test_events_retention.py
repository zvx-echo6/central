"""Archived-events retention sweep (v0.9.13).

The supervisor deletes events older than their stream's max_age_s (the per-stream
/streams retention). Mapping events.category -> stream is explicit because category
domains don't equal stream subject domains for the traffic family. Fail-safe:
streams with missing/<=0 max_age_s are skipped (never deleted), and any category
domain the map doesn't cover is surfaced as a warning.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock

from central.supervisor import (
    STREAM_CATEGORY_DOMAINS,
    EVENTS_RETENTION_INTERVAL_S,
    Supervisor,
    _all_mapped_domains,
)
from central.streams import STREAMS as STREAM_REGISTRY

# Every category first-token domain observed in the events table (the coverage
# guard: a new adapter domain must be added to STREAM_CATEGORY_DOMAINS or this fails).
KNOWN_EVENT_DOMAINS = {
    "wx", "fire", "quake", "space", "disaster", "hydro",
    "incident", "closure", "work_zone", "flow", "camera",
}


class TestRetentionMap:
    def test_map_covers_all_known_domains(self):
        assert KNOWN_EVENT_DOMAINS <= _all_mapped_domains()

    def test_map_keys_are_event_bearing_streams(self):
        event_streams = {s.name for s in STREAM_REGISTRY if s.event_bearing}
        assert set(STREAM_CATEGORY_DOMAINS) <= event_streams

    def test_central_meta_not_in_map(self):
        # CENTRAL_META is status-only (not event-bearing) -> no events rows to sweep.
        assert "CENTRAL_META" not in STREAM_CATEGORY_DOMAINS

    def test_no_domain_mapped_to_two_streams(self):
        seen, dupes = set(), set()
        for domains in STREAM_CATEGORY_DOMAINS.values():
            for d in domains:
                (dupes if d in seen else seen).add(d)
        assert not dupes

    def test_interval_is_hourly(self):
        assert EVENTS_RETENTION_INTERVAL_S == 3600


def _stream(name, max_age_s):
    s = MagicMock()
    s.name = name
    s.max_age_s = max_age_s
    return s


def _supervisor_with_store(store):
    sup = Supervisor.__new__(Supervisor)  # bypass __init__/connections
    sup._config_store = store
    return sup


@pytest.mark.asyncio
async def test_sweep_deletes_per_event_bearing_stream():
    store = AsyncMock()
    store.list_streams.return_value = [
        _stream(name, 604800) for name in STREAM_CATEGORY_DOMAINS
    ]
    store.delete_events_older_than.return_value = 5
    store.unmapped_event_domains.return_value = []
    sup = _supervisor_with_store(store)

    await sup._sweep_events_retention()

    assert store.delete_events_older_than.await_count == len(STREAM_CATEGORY_DOMAINS)
    # CENTRAL_TRAFFIC carries the multi-domain traffic family.
    calls = {c.args[0][0]: c.args for c in store.delete_events_older_than.await_args_list}
    assert "incident" in [d for c in store.delete_events_older_than.await_args_list for d in c.args[0]]


@pytest.mark.asyncio
async def test_sweep_skips_missing_and_nonpositive_max_age():
    store = AsyncMock()
    # WX present/valid; FIRE max_age_s=0 (skip); others absent (skip).
    store.list_streams.return_value = [_stream("CENTRAL_WX", 86400), _stream("CENTRAL_FIRE", 0)]
    store.delete_events_older_than.return_value = 0
    store.unmapped_event_domains.return_value = []
    sup = _supervisor_with_store(store)

    await sup._sweep_events_retention()

    swept_domains = [c.args[0] for c in store.delete_events_older_than.await_args_list]
    assert ["wx"] in swept_domains          # WX swept
    assert ["fire"] not in swept_domains    # FIRE (max_age 0) skipped
    assert store.delete_events_older_than.await_count == 1


@pytest.mark.asyncio
async def test_sweep_passes_max_age_to_delete():
    store = AsyncMock()
    store.list_streams.return_value = [_stream("CENTRAL_WX", 123456)]
    store.delete_events_older_than.return_value = 0
    store.unmapped_event_domains.return_value = []
    sup = _supervisor_with_store(store)

    await sup._sweep_events_retention()

    args = store.delete_events_older_than.await_args
    assert args.args == (["wx"], 123456)


@pytest.mark.asyncio
async def test_sweep_warns_on_unmapped_domains_but_does_not_raise():
    store = AsyncMock()
    store.list_streams.return_value = [_stream("CENTRAL_WX", 86400)]
    store.delete_events_older_than.return_value = 0
    store.unmapped_event_domains.return_value = ["mystery_domain"]
    sup = _supervisor_with_store(store)
    # Should complete without raising even though an unmapped domain exists.
    await sup._sweep_events_retention()
    store.unmapped_event_domains.assert_awaited_once()
