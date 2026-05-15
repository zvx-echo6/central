"""CloudEvents wire format helpers."""

from typing import Any

from cloudevents.v1.http import CloudEvent

from central.config import Config
from central.models import Event


def wrap_event(event: Event, config: Config) -> tuple[dict[str, Any], str]:
    """
    Wrap an Event into a CNCF CloudEvents v1.0 JSON envelope.
    
    Returns:
        A tuple of (envelope_dict, msg_id) where msg_id is the
        CloudEvent id for use as Nats-Msg-Id header.
    """
    # Build CE type: {prefix}.{category}.v1
    ce_type = f"{config.cloudevents.type_prefix}.{event.category}.v1"
    
    # Serialize event data
    event_data = event.model_dump(mode="json")
    
    # Build extension attributes - lowercase, no underscores per CE spec
    extensions: dict[str, Any] = {
        "hubschemaversion": config.cloudevents.schema_version,
        "hubcategory": event.category,
    }
    
    # Only include hubseverity if severity is present
    if event.severity is not None:
        extensions["hubseverity"] = event.severity
    
    # Create CloudEvent
    ce = CloudEvent(
        attributes={
            "id": event.id,
            "source": config.cloudevents.source,
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
