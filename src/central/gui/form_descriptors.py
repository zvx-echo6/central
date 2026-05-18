"""Form field descriptors for adapter settings.

If a second nested settings type beyond RegionConfig appears,
refactor this helper to recurse over nested models.
"""

from dataclasses import dataclass
from typing import Any, Union, get_args, get_origin

from pydantic import BaseModel
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from central.config_models import RegionConfig


@dataclass
class FieldDescriptor:
    """Describes a form field for rendering."""
    name: str
    label: str
    widget: str  # "text", "number", "checkbox", "csv", "region"
    current_value: Any
    default: Any
    description: str
    required: bool


def _type_to_widget(field_name: str, field_type: type) -> str:
    """Map a Python type to a widget type."""
    # Handle Optional/Union types
    origin = get_origin(field_type)
    args = get_args(field_type)

    # Check for Optional[X] (Union[X, None])
    if origin is Union or (origin is not None and type(None) in args):
        # Get the non-None type
        non_none_args = [a for a in args if a is not type(None)]
        if non_none_args:
            inner_type = non_none_args[0]
            # Recursively determine widget for the inner type
            return _type_to_widget(field_name, inner_type)

    # Direct type checks
    if field_type is str:
        return "text"
    if field_type is int:
        return "number"
    if field_type is bool:
        return "checkbox"
    if field_type is RegionConfig:
        return "region"

    # Check for list[str]
    if origin is list:
        if args and args[0] is str:
            return "csv"
        raise NotImplementedError(
            f"Field '{field_name}' has unsupported list type: list[{args[0].__name__ if args else '?'}]"
        )

    # Check if it's a BaseModel subclass (nested model other than RegionConfig)
    if isinstance(field_type, type) and issubclass(field_type, BaseModel):
        raise NotImplementedError(
            f"Field '{field_name}' has unsupported nested type: {field_type.__name__}. "
            f"If a second nested type beyond RegionConfig is needed, "
            f"refactor describe_fields to recurse over nested models."
        )

    raise NotImplementedError(
        f"Field '{field_name}' has unsupported type: {field_type}"
    )


def _name_to_label(name: str) -> str:
    """Convert field name to human-readable label."""
    return name.replace("_", " ").title()


def _is_undefined(value: Any) -> bool:
    """Check if a value is Pydantic's undefined sentinel."""
    return value is PydanticUndefined


def describe_fields(model_cls: type[BaseModel], current: dict) -> list[FieldDescriptor]:
    """Generate field descriptors for a Pydantic model.

    Args:
        model_cls: The Pydantic model class (e.g., NWSSettings)
        current: Current settings values from the database

    Returns:
        List of FieldDescriptor objects for rendering the form
    """
    descriptors = []

    for field_name, field_info in model_cls.model_fields.items():
        # Get the field type
        field_type = field_info.annotation

        # Determine widget
        widget = _type_to_widget(field_name, field_type)

        # Get current value, falling back to default
        if field_name in current:
            current_value = current[field_name]
        elif not _is_undefined(field_info.default):
            current_value = field_info.default
        else:
            current_value = None

        # Get default
        default = field_info.default if not _is_undefined(field_info.default) else None

        # Get description
        description = ""
        if field_info.description:
            description = field_info.description

        # Determine if required (no default and not Optional)
        required = _is_undefined(field_info.default) and field_info.is_required()

        descriptors.append(FieldDescriptor(
            name=field_name,
            label=_name_to_label(field_name),
            widget=widget,
            current_value=current_value,
            default=default,
            description=description,
            required=required,
        ))

    return descriptors
