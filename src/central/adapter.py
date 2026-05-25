"""Base adapter interface for event sources."""

import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel

if TYPE_CHECKING:
    from central.config_models import AdapterConfig

from central.models import Event

logger = logging.getLogger(__name__)


class SourceAdapter(ABC):
    """
    Abstract base class for event source adapters.
    
    Adapters yield Events. The supervisor handles scheduling,
    CloudEvents wrapping, publish, and metadata heartbeats.
    
    Class attributes that subclasses must define:
        name: Short identifier, e.g. "nws"
        display_name: Human-readable name for GUI
        description: Short description of the adapter
        settings_schema: Pydantic model class for adapter settings
        requires_api_key: Key alias if API key required, else None
        wizard_order: Order in setup wizard (None = not in wizard)
        default_cadence_s: Default polling interval in seconds
    """
    
    name: str
    display_name: str
    description: str
    settings_schema: type[BaseModel]
    requires_api_key: str | None = None
    api_key_field: str | None = None
    """Names the settings_schema field that holds an api_key alias reference, if any.
    The GUI renders this field as a select populated from config.api_keys;
    the wizard validates it against staged api_keys state."""
    wizard_order: int | None = None
    default_cadence_s: int

    enrichment_locations: list[tuple[str, str]] = []
    """Coordinate field paths the supervisor enriches, as (lat_field, lon_field)
    tuples into Event.data. Empty (the default) means publish as-is — no
    enrichment. Each tuple names top-level keys in Event.data holding a
    latitude and longitude; the supervisor extracts them, runs registered
    enrichers, and attaches results under Event.data["_enriched"]."""

    data_class: Literal["event", "telemetry"] = "event"
    """How the GUI classifies this source. "event" (default) = discrete events,
    shown on /events. "telemetry" = continuous/high-volume feeds (e.g. NWIS water
    gauges) that would drown discrete-event signal; shown on /telemetry instead.
    GUI-only — does not affect publishing or the events.json contract."""

    @abstractmethod
    async def poll(self) -> AsyncIterator[Event]:
        """
        Poll the source for new events.
        
        Yields Event objects for each new/updated event found.
        """
        ...
    
    @abstractmethod
    async def apply_config(self, new_config: "AdapterConfig") -> None:
        """
        Apply new configuration to the adapter.
        
        Called by supervisor when config changes via hot-reload.
        The adapter should extract relevant settings from
        new_config.settings and update its internal state.
        """
        ...
    
    @abstractmethod
    def subject_for(self, event: Event) -> str:
        """
        Compute the NATS subject for an event.
        
        Each adapter knows its own subject hierarchy. The supervisor
        calls this to determine where to publish each event.
        """
        ...
    
    async def startup(self) -> None:
        """Optional lifecycle hook called before first poll."""
        pass
    
    async def shutdown(self) -> None:
        """Optional lifecycle hook called on graceful shutdown."""
        pass

    dedup_sweep_days: int = 14
    """Retention window (days) for the ``published_ids`` dedup table; the
    inherited ``sweep_old_ids`` deletes rows older than this. Override per
    adapter (NWIS uses 30; most use the 14-day default)."""

    def is_published(self, event_id: str) -> bool:
        """True if ``event_id`` is already recorded. No-op-safe without ``_db``."""
        db = getattr(self, "_db", None)
        if db is None:
            return False
        cur = db.execute(
            "SELECT 1 FROM published_ids WHERE adapter = ? AND event_id = ?",
            (self.name, event_id),
        )
        return cur.fetchone() is not None

    def mark_published(self, event_id: str) -> None:
        """Record ``event_id`` as published; refresh ``last_seen`` on re-publish."""
        db = getattr(self, "_db", None)
        if db is None:
            return
        db.execute(
            """
            INSERT INTO published_ids (adapter, event_id, first_seen, last_seen)
            VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (adapter, event_id) DO UPDATE SET last_seen = CURRENT_TIMESTAMP
            """,
            (self.name, event_id),
        )
        db.commit()

    def sweep_old_ids(self) -> int:
        """Purge dedup rows older than ``dedup_sweep_days``; returns count deleted."""
        db = getattr(self, "_db", None)
        if db is None:
            return 0
        cur = db.execute(
            "DELETE FROM published_ids WHERE adapter = ? "
            "AND last_seen < datetime('now', ?)",
            (self.name, f"-{self.dedup_sweep_days} days"),
        )
        db.commit()
        count = cur.rowcount
        if count > 0:
            logger.info(
                "Swept old dedup entries",
                extra={"adapter": self.name, "count": count},
            )
        return count

    def bump_last_seen(self, event_id: str) -> None:
        """Refresh the dedup ``last_seen`` for an already-published event.

        The supervisor calls this on every dedup hit so the periodic
        ``sweep_old_ids`` purge doesn't expire long-lived events that upstream
        keeps re-listing (e.g. EONET open events, NWS active alerts). Adapters
        that maintain the standard ``published_ids`` table inherit this; it is a
        safe no-op for any adapter without a ``_db`` handle. Previously only
        four adapters defined it, so adapters that re-emit (eonet, etc.) raised
        ``AttributeError`` here and the supervisor surfaced it on /dashboard.
        """
        db = getattr(self, "_db", None)
        if db is None:
            return
        db.execute(
            "UPDATE published_ids SET last_seen = CURRENT_TIMESTAMP "
            "WHERE adapter = ? AND event_id = ?",
            (self.name, event_id),
        )
        db.commit()

    async def preview_for_settings(self, settings: BaseModel) -> list[dict] | None:
        """Optional. Override to surface a settings-driven preview on the edit page.

        Return list[dict] (framework renders as a generic table; columns come from
        the first dict's keys, in insertion order). Return None to skip preview.
        Raise to surface an error banner — framework catches at the route boundary.

        Contract:
        - Preview is a pure function of `settings`. Do NOT access
          self._config_store or cursor_db state — the framework may instantiate
          adapters with a stub config_store solely to call this method.
        - Network preview implementations must open their own short-lived
          aiohttp session (the adapter's polling session may not exist; the GUI
          process never calls startup()).
        - Return None when preview is not meaningful (e.g., required settings
          like region are unset). Return [] explicitly if the query ran and
          matched zero rows — the framework renders that distinctly from None.
        """
        return None
