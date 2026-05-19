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
    api_key_field: str | None = None
    """Names the settings_schema field that holds an api_key alias reference, if any.
    The GUI renders this field as a select populated from config.api_keys;
    the wizard validates it against staged api_keys state."""
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
