"""CloudEvents wire format helpers."""

from typing import Any, Union

from cloudevents.v1.http import CloudEvent

from central.config import Config, CloudEventsConfig
from central.cloudevents_constants import CLOUDEVENTS_CONFIG
from central.models import Event


def wrap_event(
    event: Event,
    config: Union[Config, CloudEventsConfig, None] = None,
) -> tuple[dict[str, Any], str]:
    """
    Wrap an Event into a CNCF CloudEvents v1.0 JSON envelope.

    Args:
        event: The event to wrap
        config: Either a full Config object, a CloudEventsConfig object,
                or None to use defaults.

    Returns:
        A tuple of (envelope_dict, msg_id) where msg_id is the
        CloudEvent id for use as Nats-Msg-Id header.
    """
    # Resolve CloudEventsConfig from various input types
    if config is None:
        ce_config = CLOUDEVENTS_CONFIG
    elif isinstance(config, CloudEventsConfig):
        ce_config = config
    else:
        # It's a full Config object
        ce_config = config.cloudevents

    # Build CE type: {prefix}.{category}.v1
    ce_type = f"{ce_config.type_prefix}.{event.category}.v1"

    # Serialize event data
    event_data = event.model_dump(mode="json")

    # Build extension attributes - lowercase, no underscores per CE spec
    extensions: dict[str, Any] = {
        "centralschemaversion": ce_config.schema_version,
        "centralcategory": event.category,
    }

    # Only include centralseverity if severity is present
    if event.severity is not None:
        extensions["centralseverity"] = event.severity

    # Create CloudEvent
    ce = CloudEvent(
        attributes={
            "id": event.id,
            "source": ce_config.source,
            "type": ce_type,
            "time": event.time.isoformat(),
            "datacontenttype": "application/json",
            **extensions,
        },
        data=event_data,
    )

    # Build envelope dict from CloudEvent
    envelope: dict[str, Any] = dict(ce.get_attributes())
    envelope["data"] = ce.data

    return envelope, event.id
