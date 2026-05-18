"""Base adapter interface for event sources."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from central.config_models import AdapterConfig

from central.models import Event


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
    wizard_order: int | None = None
    default_cadence_s: int
    
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
