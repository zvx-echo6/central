"""Tests for v0.10.5 'Re-send recent events' (resend.py + routes).

Backend: preview_resend counts per stream; execute_resend republishes with
the suffix-style Nats-Msg-Id; per-message failures don't sink the batch;
audit-log meta-event is emitted on completion.

Frontend: the dashboard renders the new card; preview returns the
confirmation fragment with the count; POST validates CSRF and returns the
success fragment.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from central.gui import resend as resend_mod
from central.gui.resend import (
    TIME_WINDOWS,
    execute_resend,
    is_valid_window,
    preview_resend,
    window_label,
)


# --- helpers -----------------------------------------------------------------


def _mk_msg(subject: str, data: bytes = b'{"data":{"x":1}}',
            headers: dict | None = None, stream_seq: int = 0):
    msg = MagicMock()
    msg.subject = subject
    msg.data = data
    msg.headers = headers if headers is not None else {"Nats-Msg-Id": subject}
    # Mirror nats-py's Msg.metadata.sequence.stream so the v0.10.5.2 snapshot
    # filter has a concrete int to compare against (default 0 stays well
    # below the high default snapshot in _mk_js so no existing test gets
    # filtered out).
    msg.metadata.sequence.stream = stream_seq
    return msg


def _mk_js(per_stream_msgs: dict[str, list]) -> MagicMock:
    """JS mock whose pull_subscribe yields a per-stream message list.

    The fetch sequence returns the list once, then empty (terminates the
    iterator). publish is captured for assertions; unsubscribe is no-op.
    stream_info returns a snapshot that's intentionally far above any test
    message's stream_seq so the v0.10.5.2 boundary check is a no-op for
    every existing test -- only the dedicated regression tests below
    exercise the filter explicitly.
    """
    js = MagicMock()
    captured_publishes: list[tuple[str, bytes, dict]] = []

    async def _publish(subject, data, headers=None):
        captured_publishes.append((subject, data, dict(headers or {})))

    js.publish = AsyncMock(side_effect=_publish)
    js._captured = captured_publishes

    async def _stream_info(name):
        info = MagicMock()
        info.state.last_seq = 10**12
        return info

    js.stream_info = AsyncMock(side_effect=_stream_info)

    async def _pull_subscribe(filter_subj, durable=None, stream=None, config=None):
        sub = MagicMock()
        msgs = list(per_stream_msgs.get(stream, []))
        calls = {"n": 0}

        async def _fetch(batch=200, timeout=2.0):
            if calls["n"] == 0:
                calls["n"] += 1
                return msgs
            return []

        sub.fetch = AsyncMock(side_effect=_fetch)
        sub.unsubscribe = AsyncMock()
        return sub

    js.pull_subscribe = AsyncMock(side_effect=_pull_subscribe)
    return js


# --- pure-config tests -------------------------------------------------------


def test_time_windows_locked_set():
    """The dropdown is the operator-facing source of truth -- nothing else
    should accept arbitrary minute values."""
    assert is_valid_window(60) is True
    assert is_valid_window(5) is True
    assert is_valid_window(1440) is True
    assert is_valid_window(0) is False
    assert is_valid_window(-1) is False
    assert is_valid_window(7) is False           # off-list value
    assert is_valid_window(99999) is False


def test_window_label_round_trips_dropdown():
    for m, label in TIME_WINDOWS:
        assert window_label(m) == label
    # Off-list falls back to the bare minute count (operator never sees this).
    assert window_label(7) == "7 minutes"


# --- preview -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_preview_counts_per_stream():
    js = _mk_js({
        "CENTRAL_FIRE": [_mk_msg("central.fire.incident.id.cassia") for _ in range(3)],
        "CENTRAL_TRAFFIC": [_mk_msg("central.traffic.incident.id") for _ in range(7)],
    })
    out = await preview_resend(js, minutes=60)
    assert out["count"] == 10
    assert out["by_stream"]["CENTRAL_FIRE"] == 3
    assert out["by_stream"]["CENTRAL_TRAFFIC"] == 7
    # Every event-bearing stream gets a key (zero when empty).
    assert out["by_stream"]["CENTRAL_WX"] == 0
    # CENTRAL_META is intentionally excluded -- never appears in the dict.
    assert "CENTRAL_META" not in out["by_stream"]
    assert out["window_label"] == "1 hour"


@pytest.mark.asyncio
async def test_preview_rejects_invalid_window():
    js = _mk_js({})
    out = await preview_resend(js, minutes=7)
    assert out["count"] == 0
    assert out["by_stream"] == {}
    js.pull_subscribe.assert_not_called()


@pytest.mark.asyncio
async def test_preview_per_stream_error_does_not_sink_batch():
    """A NATS error on one stream marks its count as None but the rest count."""
    js = _mk_js({"CENTRAL_FIRE": [_mk_msg("central.fire.x") for _ in range(2)]})

    original = js.pull_subscribe.side_effect

    async def _maybe_fail(filter_subj, durable=None, stream=None, config=None):
        if stream == "CENTRAL_TRAFFIC":
            raise RuntimeError("simulated stream-level NATS error")
        return await original(filter_subj, durable=durable, stream=stream, config=config)

    js.pull_subscribe = AsyncMock(side_effect=_maybe_fail)
    out = await preview_resend(js, minutes=60)
    assert out["by_stream"]["CENTRAL_FIRE"] == 2
    assert out["by_stream"]["CENTRAL_TRAFFIC"] is None
    assert out["errors"] == 1
    # Total only counts streams that succeeded.
    assert out["count"] == 2


# --- execute -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_replays_with_suffix_msg_id():
    """Each republish keeps subject + data, gets new {orig}:resend:{ts} msg id."""
    msgs = [
        _mk_msg("central.fire.incident.id.cassia",
                data=b'{"data":{"name":"Summit Creek"}}',
                headers={"Nats-Msg-Id": "wfigs_incidents:cassia:1"}),
        _mk_msg("central.fire.incident.id.owyhee",
                data=b'{"data":{"name":"Blue Ridge"}}',
                headers={"Nats-Msg-Id": "wfigs_incidents:owyhee:2"}),
    ]
    js = _mk_js({"CENTRAL_FIRE": msgs})
    nc = MagicMock()
    nc.publish = AsyncMock()
    out = await execute_resend(js, nc, minutes=60, operator="matt")

    assert out["published"] == 2
    assert out["errors"] == 0
    pubs = js._captured
    # Subject + data preserved byte-for-byte
    assert pubs[0][0] == "central.fire.incident.id.cassia"
    assert pubs[0][1] == b'{"data":{"name":"Summit Creek"}}'
    # New msg id is {orig}:resend:{ts_ms} -- avoids JetStream dedup window.
    assert pubs[0][2]["Nats-Msg-Id"].startswith("wfigs_incidents:cassia:1:resend:")
    assert pubs[1][2]["Nats-Msg-Id"].startswith("wfigs_incidents:owyhee:2:resend:")


@pytest.mark.asyncio
async def test_execute_emits_audit_log_meta_event():
    js = _mk_js({"CENTRAL_FIRE": [_mk_msg("central.fire.x") for _ in range(3)]})
    nc = MagicMock()
    nc.publish = AsyncMock()
    await execute_resend(js, nc, minutes=60, operator="matt")
    nc.publish.assert_awaited_once()
    subject, payload = nc.publish.await_args.args
    assert subject == "central.meta.action.resend"
    meta = json.loads(payload.decode())
    assert meta["operator"] == "matt"
    assert meta["window_minutes"] == 60
    assert meta["count"] == 3
    assert meta["errors"] == 0
    assert "started_at" in meta and "finished_at" in meta


@pytest.mark.asyncio
async def test_execute_per_message_failure_does_not_sink_batch():
    """One bad publish counts as an error but the rest still ship."""
    msgs = [_mk_msg(f"central.fire.x.{i}",
                    headers={"Nats-Msg-Id": f"id-{i}"}) for i in range(3)]
    js = _mk_js({"CENTRAL_FIRE": msgs})

    calls = {"n": 0}

    async def _flaky_publish(subject, data, headers=None):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("simulated NATS publish error")

    js.publish = AsyncMock(side_effect=_flaky_publish)
    nc = MagicMock()
    nc.publish = AsyncMock()

    out = await execute_resend(js, nc, minutes=60, operator="matt")
    assert out["published"] == 2
    assert out["errors"] == 1
    assert out["by_stream"]["CENTRAL_FIRE"]["published"] == 2
    assert out["by_stream"]["CENTRAL_FIRE"]["errors"] == 1


@pytest.mark.asyncio
async def test_execute_handles_message_with_no_original_msg_id():
    """Older publishes might lack Nats-Msg-Id -- we still mint a unique id."""
    msg = _mk_msg("central.fire.x", headers={})
    js = _mk_js({"CENTRAL_FIRE": [msg]})
    nc = MagicMock()
    nc.publish = AsyncMock()
    await execute_resend(js, nc, minutes=60, operator="matt")
    new_id = js._captured[0][2]["Nats-Msg-Id"]
    assert new_id.startswith("resend:") and "CENTRAL_FIRE" in new_id


@pytest.mark.asyncio
async def test_execute_audit_log_failure_does_not_sink_result():
    """nc.publish failure is logged and swallowed; published count still returns."""
    js = _mk_js({"CENTRAL_FIRE": [_mk_msg("central.fire.x")]})
    nc = MagicMock()
    nc.publish = AsyncMock(side_effect=RuntimeError("audit publish failed"))
    out = await execute_resend(js, nc, minutes=60, operator="matt")
    assert out["published"] == 1
    assert out["errors"] == 0  # audit failure is logged-only, not counted


@pytest.mark.asyncio
async def test_execute_rejects_invalid_window():
    js = _mk_js({})
    nc = MagicMock()
    nc.publish = AsyncMock()
    out = await execute_resend(js, nc, minutes=7, operator="matt")
    assert out["published"] == 0
    js.pull_subscribe.assert_not_called()
    nc.publish.assert_not_called()


# --- ConsumerConfig regression guard (v0.10.5.1) -----------------------------


@pytest.mark.asyncio
async def test_pull_subscribe_inactive_threshold_within_nats_range():
    """v0.10.5.1 regression guard: ``inactive_threshold`` on the ephemeral
    consumer must be a number nats-py can serialise as a Go ``time.Duration``.

    v0.10.5 passed ``int(30e9)`` thinking it was nanoseconds. nats-py treats
    the value as float SECONDS and multiplies by 1e9 internally, so the
    server received 30e18 -- out of int64 range. NATS rejected the consumer
    with ``err_code=10025``; preview_resend caught the exception per-stream
    and returned 0 events across the board (silent verification failure).

    Assert the captured config has:
      - ``inactive_threshold`` in [1, 3600] seconds (operator sanity range)
      - the nanosecond-equivalent (value * 1e9) fits within int64
    The ``int64`` ceiling is 9_223_372_036_854_775_807 -- anything above that
    triggers the same JSON unmarshal error that broke v0.10.5.
    """
    captured_configs: list = []

    async def _capture_config(filter_subj, durable=None, stream=None, config=None):
        captured_configs.append(config)
        sub = MagicMock()
        sub.fetch = AsyncMock(return_value=[])
        sub.unsubscribe = AsyncMock()
        return sub

    js = MagicMock()
    js.pull_subscribe = AsyncMock(side_effect=_capture_config)
    # v0.10.5.2: preview_resend now snapshots last_seq before iterating; give
    # the bare mock a stream_info AsyncMock so we still reach pull_subscribe.
    async def _stream_info(name):
        info = MagicMock()
        info.state.last_seq = 10**12
        return info
    js.stream_info = AsyncMock(side_effect=_stream_info)
    await preview_resend(js, minutes=60)

    INT64_MAX_NS = 9_223_372_036_854_775_807
    assert captured_configs, "expected at least one pull_subscribe call"
    for cfg in captured_configs:
        threshold = cfg.inactive_threshold
        assert threshold is not None, "inactive_threshold must be set"
        assert 1 <= threshold <= 3600, (
            f"inactive_threshold={threshold!r}s outside [1, 3600] sanity range"
        )
        ns = threshold * 1_000_000_000
        assert ns < INT64_MAX_NS, (
            f"inactive_threshold={threshold!r} would produce ns={ns}, "
            f"overflowing int64 ({INT64_MAX_NS}). This is the exact v0.10.5 "
            f"bug -- a unit confusion that produced 30e18 and triggered "
            f"err_code=10025 from NATS."
        )


# --- BY_START_TIME feedback-loop guard (v0.10.5.2) ---------------------------


def _mk_batched_sub(batches: list[list]) -> MagicMock:
    """Pull-sub mock whose fetch returns one entry from ``batches`` per call,
    then empty -- lets a test stage messages that arrive AFTER the snapshot.
    """
    sub = MagicMock()
    calls = {"n": 0}

    async def _fetch(batch=200, timeout=2.0):
        if calls["n"] < len(batches):
            out = batches[calls["n"]]
            calls["n"] += 1
            return out
        return []

    sub.fetch = AsyncMock(side_effect=_fetch)
    sub.unsubscribe = AsyncMock()
    return sub


@pytest.mark.asyncio
async def test_iter_window_stops_at_snapshot_last_seq_boundary():
    """v0.10.5.2 regression guard: the feedback loop that took down central.

    v0.10.5 used DeliverPolicy.BY_START_TIME with no upper bound on the
    consumer. As soon as execute_resend republished a message, the new copy
    matched the time filter and the same consumer fetched it again,
    republishing in an unbounded loop until the per-stream cap tripped --
    potentially 450k spurious republishes across 9 streams, which is what
    timed out central-gui's POST and brought the host down.

    Fix: snapshot the stream's ``last_seq`` at the top of preview/execute
    and pass it as ``max_stream_seq`` to ``_iter_window``. Any message
    whose ``metadata.sequence.stream`` exceeds the snapshot was published
    AFTER we started -- either an unrelated adapter ingest or the very
    republish we just emitted -- and must not be touched.

    Simulation: batch 1 holds the 100 legit pre-snapshot messages
    (stream_seqs 100..199, snapshot=199). Batch 2 holds 100 post-snapshot
    messages (stream_seqs 200..299) -- the "echoes" that v0.10.5 would
    have looped on. Assert iter yields exactly 100 (all from batch 1) and
    NEVER yields a batch-2 message.
    """
    pre_snapshot = [
        _mk_msg(f"central.fire.x.{seq}", stream_seq=seq)
        for seq in range(100, 200)
    ]
    post_snapshot = [
        _mk_msg(f"central.fire.x.{seq}", stream_seq=seq)
        for seq in range(200, 300)
    ]
    sub = _mk_batched_sub([pre_snapshot, post_snapshot])

    js = MagicMock()
    js.pull_subscribe = AsyncMock(return_value=sub)

    yielded = []
    async for msg in resend_mod._iter_window(
        js, "CENTRAL_FIRE", "central.fire.>",
        cutoff=datetime(2026, 6, 7, 0, 0, 0, tzinfo=timezone.utc),
        max_stream_seq=199,
    ):
        yielded.append(msg.metadata.sequence.stream)

    assert len(yielded) == 100
    assert yielded == list(range(100, 200))
    # Cleanup still runs after early return.
    sub.unsubscribe.assert_awaited_once()


@pytest.mark.asyncio
async def test_iter_window_enforces_per_stream_cap_with_warning(caplog):
    """v0.10.5.2 regression guard: cap dropped from 50_000 to 5_000.

    A legit operator window should never exceed this. If it does, we want
    the operator to hear about it via a warning log -- silent truncation
    is exactly the kind of behavior that hid the v0.10.5 feedback loop
    from the operator until it took the host down.

    Simulation: snapshot is set high enough (10**6) that it never limits
    iteration; cap (5000) is the only stopper. Batches of 200 messages
    each, seqs 1..6200, so the cap trips well before the source runs out.
    """
    batches = [
        [_mk_msg(f"central.x.{seq}", stream_seq=seq)
         for seq in range(start, start + 200)]
        for start in range(1, 6201, 200)
    ]
    sub = _mk_batched_sub(batches)

    js = MagicMock()
    js.pull_subscribe = AsyncMock(return_value=sub)

    yielded = 0
    with caplog.at_level("WARNING", logger="central.gui.resend"):
        async for _ in resend_mod._iter_window(
            js, "CENTRAL_FIRE", "central.fire.>",
            cutoff=datetime(2026, 6, 7, 0, 0, 0, tzinfo=timezone.utc),
            max_stream_seq=10**6,
        ):
            yielded += 1

    assert yielded == resend_mod._MAX_MSGS_PER_STREAM == 5_000
    cap_warnings = [r for r in caplog.records
                    if r.levelname == "WARNING"
                    and "cap reached" in r.getMessage()]
    assert len(cap_warnings) == 1
    assert cap_warnings[0].stream == "CENTRAL_FIRE"
    sub.unsubscribe.assert_awaited_once()


# --- stream-set safety -------------------------------------------------------


def test_central_meta_excluded_from_replay_set():
    """CENTRAL_META is status-only; replaying it would broadcast stale audit
    records back through archive's consumers."""
    names = [s.name for s in resend_mod._event_bearing_streams()]
    assert "CENTRAL_META" not in names
    # Sanity: the 9 event-bearing streams are present.
    for expected in ("CENTRAL_FIRE", "CENTRAL_TRAFFIC", "CENTRAL_WX",
                     "CENTRAL_QUAKE", "CENTRAL_SPACE", "CENTRAL_DISASTER",
                     "CENTRAL_HYDRO", "CENTRAL_TRAFFIC_FLOW",
                     "CENTRAL_TRAFFIC_CAMERAS"):
        assert expected in names
