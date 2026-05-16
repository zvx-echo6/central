"""Base adapter interface for event sources."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from central.config_models import AdapterConfig

from central.models import Event


class SourceAdapter(ABC):
    """
    Abstract base class for event source adapters.
    
    Adapters yield Events. The supervisor handles scheduling,
    CloudEvents wrapping, publish, and metadata heartbeats.
    """
    
    name: str  # short identifier, e.g. "nws"
    
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
    
    async def startup(self) -> None:
        """Optional lifecycle hook called before first poll."""
        pass
    
    async def shutdown(self) -> None:
        """Optional lifecycle hook called on graceful shutdown."""
        pass
