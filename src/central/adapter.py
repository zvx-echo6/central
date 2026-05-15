"""Base adapter interface for event sources."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from central.models import Event


class SourceAdapter(ABC):
    """
    Abstract base class for event source adapters.
    
    Adapters yield Events. The supervisor handles scheduling,
    CloudEvents wrapping, publish, and metadata heartbeats.
    """
    
    name: str  # short identifier, e.g. "nws"
    cadence_s: int  # seconds between poll() calls
    
    @abstractmethod
    async def poll(self) -> AsyncIterator[Event]:
        """
        Poll the source for new events.
        
        Yields Event objects for each new/updated event found.
        """
        ...
    
    async def startup(self) -> None:
        """Optional lifecycle hook called before first poll."""
        pass
    
    async def shutdown(self) -> None:
        """Optional lifecycle hook called on graceful shutdown."""
        pass
