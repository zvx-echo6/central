"""CloudEvents configuration constants.

These defaults are used when no TOML configuration file is available
(e.g., when using database-backed configuration source).

These values are baked into the code because CloudEvents envelope format
is part of the Central protocol contract, not operator-configurable.
"""

from central.config import CloudEventsConfig

# Default CloudEvents configuration - used when TOML is unavailable
DEFAULT_CLOUDEVENTS_CONFIG = CloudEventsConfig(
    type_prefix="central",
    source="central.echo6.co",
    schema_version="1.0",
)
