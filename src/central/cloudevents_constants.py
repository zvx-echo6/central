"""CloudEvents configuration constants.

These are the protocol-level constants for CloudEvents envelope format.
CloudEvents envelope format is part of the Central protocol contract
and is not operator-configurable.
"""

from central.config import CloudEventsConfig

# CloudEvents protocol constants
CLOUDEVENTS_CONFIG = CloudEventsConfig(
    type_prefix="central",
    source="central.echo6.co",
    schema_version="1.0",
)
