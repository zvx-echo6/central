"""v0.10.5 — operator-controlled re-publish of recent events.

The dashboard's "Re-send recent events" card lets an operator pick a time
window (5 minutes → 24 hours), preview the count of messages that would be
re-sent across every event-bearing JetStream stream, then confirm to
re-publish them.

Each replayed message keeps its original subject and raw byte payload
(CloudEvents envelope unchanged) but receives a new ``Nats-Msg-Id`` of the
form ``{original}:resend:{ts_epoch_ms}`` so JetStream's per-stream
deduplication window doesn't silently drop the replay. Consumers with
``deliver_policy=new`` see the messages as fresh; the archive UPSERTs on
``(id, time)`` so the events table doesn't grow.

The supervisor's publish-time monitoring-area bbox filter (v0.10.2) is NOT
applied here -- the operator is intentionally replaying messages that
already passed through it on their original publish.

Stream set is derived from ``central.streams.STREAMS`` -- only the
``event_bearing=True`` entries are touched; ``CENTRAL_META`` is excluded
deliberately so audit/status messages aren't re-broadcast.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import nats
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js import JetStreamContext
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy
from nats.js.errors import NotFoundError

from central.streams import STREAMS

logger = logging.getLogger(__name__)

# Pull-fetch tuning. The ephemeral consumer's inactive_threshold guarantees
# JetStream auto-cleans the temp consumer if anything kills our iterator.
# v0.10.5.1 fix: ``inactive_threshold`` is expected as float SECONDS by
# nats-py (which then multiplies by 1e9 internally to form the nanosecond
# value sent to the server). v0.10.5 passed ``int(30e9)`` thinking it was
# already in ns, which got re-multiplied to 30e18 -- out of int64 range,
# rejected by the server with err_code=10025. Use the documented float-
# seconds API and let the library handle the unit conversion.
_FETCH_BATCH = 200
_FETCH_TIMEOUT_S = 2.0
_INACTIVE_THRESHOLD_S = 30.0

# Hard cap per stream per operation. 24h * worst-case CENTRAL_TRAFFIC_FLOW
# volume is still well under this; bump if a legitimate operator action
# ever hits it.
_MAX_MSGS_PER_STREAM = 50_000

# Audit-log meta subject. CENTRAL_META filter (`central.meta.>`) already
# captures it; archive does NOT consume CENTRAL_META.
_AUDIT_SUBJECT = "central.meta.action.resend"

# Operator-facing time-window dropdown. Keys are minutes posted by the GUI;
# values are the labels shown to the operator. Adding a window: one tuple.
TIME_WINDOWS: list[tuple[int, str]] = [
    (5, "5 minutes"),
    (30, "30 minutes"),
    (60, "1 hour"),
    (180, "3 hours"),
    (360, "6 hours"),
    (720, "12 hours"),
    (1440, "24 hours"),
]


def _event_bearing_streams():
    """Replay set = STREAMS minus CENTRAL_META (status-only, never replayed)."""
    return [s for s in STREAMS if s.event_bearing]


def is_valid_window(minutes: int) -> bool:
    """Reject any minute value not in the locked dropdown set."""
    return any(m == minutes for m, _ in TIME_WINDOWS)


def window_label(minutes: int) -> str:
    """Map a minute value back to its operator-facing label."""
    for m, label in TIME_WINDOWS:
        if m == minutes:
            return label
    return f"{minutes} minutes"


async def _iter_window(
    js: JetStreamContext,
    stream_name: str,
    subject_filter: str,
    cutoff: datetime,
):
    """Yield each NATS message in ``stream_name`` since ``cutoff``.

    Uses an ephemeral pull-consumer (``durable=None``, ``ack_policy=NONE``,
    ``inactive_threshold=30s``) with ``DeliverPolicy.BY_START_TIME`` so the
    JetStream server filters server-side and we never paginate over the full
    stream history.
    """
    config = ConsumerConfig(
        deliver_policy=DeliverPolicy.BY_START_TIME,
        opt_start_time=cutoff.isoformat(),
        ack_policy=AckPolicy.NONE,
        inactive_threshold=_INACTIVE_THRESHOLD_S,
        filter_subject=subject_filter,
    )
    try:
        sub = await js.pull_subscribe(
            subject_filter,
            durable=None,
            stream=stream_name,
            config=config,
        )
    except NotFoundError:
        # Stream doesn't exist (fresh dev box) -- treat as empty.
        return

    yielded = 0
    try:
        while yielded < _MAX_MSGS_PER_STREAM:
            try:
                msgs = await sub.fetch(batch=_FETCH_BATCH, timeout=_FETCH_TIMEOUT_S)
            except (NatsTimeoutError, asyncio.TimeoutError, TimeoutError):
                break
            except Exception:
                logger.exception("resend: fetch error", extra={"stream": stream_name})
                break
            if not msgs:
                break
            for msg in msgs:
                yielded += 1
                yield msg
                if yielded >= _MAX_MSGS_PER_STREAM:
                    break
    finally:
        try:
            await sub.unsubscribe()
        except Exception:
            pass


async def preview_resend(js: JetStreamContext, minutes: int) -> dict[str, Any]:
    """Count messages per event-bearing stream within the last ``minutes``.

    Streams that error out are reported with ``None`` in ``by_stream`` and
    ``errors`` incremented; the preview never raises.
    """
    if minutes <= 0 or not is_valid_window(minutes):
        return {"count": 0, "by_stream": {}, "minutes": minutes,
                "window_label": window_label(minutes), "errors": 0}
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    by_stream: dict[str, int | None] = {}
    total = 0
    errors = 0
    for s in _event_bearing_streams():
        try:
            n = 0
            async for _ in _iter_window(js, s.name, s.subject_filter, cutoff):
                n += 1
            by_stream[s.name] = n
            total += n
        except Exception:
            logger.exception("resend preview failed", extra={"stream": s.name})
            by_stream[s.name] = None
            errors += 1
    return {
        "count": total,
        "by_stream": by_stream,
        "minutes": minutes,
        "window_label": window_label(minutes),
        "errors": errors,
    }


async def execute_resend(
    js: JetStreamContext,
    nc: nats.NATS | None,
    minutes: int,
    operator: str,
) -> dict[str, Any]:
    """Re-publish each message in the last ``minutes`` across event-bearing streams.

    Each republish gets a new ``Nats-Msg-Id = {original}:resend:{ts_ms}`` so
    JetStream's dedup window doesn't drop it. Emits a meta-event on
    ``central.meta.action.resend`` after the wave completes (success OR
    partial). Audit-log publish failures are logged but never sink the
    operator-visible result.
    """
    if minutes <= 0 or not is_valid_window(minutes):
        return {"published": 0, "errors": 0, "elapsed_s": 0.0, "by_stream": {},
                "window_label": window_label(minutes)}

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    started_at = datetime.now(timezone.utc).isoformat()
    started_mono = time.monotonic()
    ts_ms = int(time.time() * 1000)

    published = 0
    errors = 0
    by_stream: dict[str, dict[str, int]] = {}

    for s in _event_bearing_streams():
        n_ok = 0
        n_err = 0
        try:
            async for msg in _iter_window(js, s.name, s.subject_filter, cutoff):
                hdr = msg.headers or {}
                orig = hdr.get("Nats-Msg-Id") or hdr.get("nats-msg-id")
                if orig:
                    new_id = f"{orig}:resend:{ts_ms}"
                else:
                    # Older messages without a dedup header still get a unique
                    # resend id so JetStream doesn't drop them.
                    new_id = f"resend:{ts_ms}:{s.name}:{n_ok}"
                try:
                    await js.publish(
                        msg.subject, msg.data,
                        headers={"Nats-Msg-Id": new_id},
                    )
                    n_ok += 1
                except Exception:
                    n_err += 1
                    logger.exception(
                        "resend: republish failed",
                        extra={"subject": msg.subject, "stream": s.name},
                    )
        except Exception:
            logger.exception("resend: stream iteration failed",
                             extra={"stream": s.name})
            n_err += 1
        by_stream[s.name] = {"published": n_ok, "errors": n_err}
        published += n_ok
        errors += n_err

    elapsed = round(time.monotonic() - started_mono, 3)
    finished_at = datetime.now(timezone.utc).isoformat()

    meta = {
        "operator": operator,
        "window_minutes": minutes,
        "count": published,
        "errors": errors,
        "started_at": started_at,
        "finished_at": finished_at,
        "elapsed_s": elapsed,
        "by_stream": by_stream,
    }
    if nc is not None:
        try:
            await nc.publish(_AUDIT_SUBJECT, json.dumps(meta).encode())
        except Exception:
            logger.exception("resend: audit-log publish failed")
    else:
        logger.warning("resend: no NATS connection for audit-log meta-event")

    logger.info(
        "resend wave complete",
        extra={"operator": operator, "window_minutes": minutes,
               "published": published, "errors": errors, "elapsed_s": elapsed},
    )

    return {
        "published": published,
        "errors": errors,
        "elapsed_s": elapsed,
        "by_stream": by_stream,
        "window_label": window_label(minutes),
    }
