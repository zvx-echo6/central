"""Adapter discovery utilities."""

import importlib
import logging
import pkgutil

import central.adapters
from central.adapter import SourceAdapter

logger = logging.getLogger(__name__)


def discover_adapters() -> dict[str, type[SourceAdapter]]:
    """Auto-discover adapter classes from central.adapters package."""
    registry: dict[str, type[SourceAdapter]] = {}
    for module_info in pkgutil.iter_modules(central.adapters.__path__):
        try:
            module = importlib.import_module(f"central.adapters.{module_info.name}")
        except Exception as e:
            logger.error(
                "Failed to import adapter module",
                extra={"module": module_info.name, "error": str(e)},
            )
            continue
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                isinstance(attr, type)
                and issubclass(attr, SourceAdapter)
                and attr is not SourceAdapter
                and hasattr(attr, "name")
            ):
                registry[attr.name] = attr
    return registry
