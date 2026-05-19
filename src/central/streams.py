"""Stream registry — single source of truth for NATS JetStream stream definitions.

Subject-filter mappings live in code (structural; change only when code changes).
Retention / max_bytes live in config.streams (operator-tunable; ops state).

Adding a stream: one StreamEntry below + one migration row that seeds
config.streams. supervisor STREAM_SUBJECTS / archive STREAMS / gui DASHBOARD_STREAMS
all derive automatically.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class StreamEntry:
    name: str
    subject_filter: str
    event_bearing: bool = True
    """Whether central-archive consumes events from this stream into the events table.
    False for status-only streams (CENTRAL_META) that the archive intentionally skips."""
    dashboard: bool = True
    """Whether the GUI dashboard surfaces this stream's stats card."""


STREAMS: list[StreamEntry] = [
    StreamEntry("CENTRAL_WX",       "central.wx.>"),
    StreamEntry("CENTRAL_FIRE",     "central.fire.>"),
    StreamEntry("CENTRAL_QUAKE",    "central.quake.>"),
    StreamEntry("CENTRAL_SPACE",    "central.space.>"),
    StreamEntry("CENTRAL_DISASTER", "central.disaster.>"),
    StreamEntry("CENTRAL_META",     "central.meta.>", event_bearing=False),
]
