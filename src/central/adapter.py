"""Base adapter interface for event sources."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

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
        stream_name: Target JetStream stream, e.g. "CENTRAL_WX"
    """
    
    name: str  # short identifier, e.g. "nws"
    stream_name: str  # target JetStream stream, e.g. "CENTRAL_WX"
    
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
    
    @classmethod
    @abstractmethod
    def settings_schema(cls) -> dict[str, Any]:
        """
        Return the JSON-serializable schema for this adapter's settings.
        
        Used by the GUI to render adapter configuration forms.
        Returns a dict with keys like:
            {
                "contact_email": {"type": "str", "default": "", "description": "..."},
                "region": {"type": "RegionConfig", "default": None, "description": "..."},
            }
        
        Note: If a second nested type beyond RegionConfig appears,
        refactor this to use generic recursion for nested schemas.
        """
        ...
    
    async def startup(self) -> None:
        """Optional lifecycle hook called before first poll."""
        pass
    
    async def shutdown(self) -> None:
        """Optional lifecycle hook called on graceful shutdown."""
        pass
