"""Form field descriptors for adapter settings.

If a second nested settings type beyond RegionConfig appears,
refactor this helper to recurse over nested models.
"""

from dataclasses import dataclass, field
from typing import Any, Literal, Union, get_args, get_origin

from pydantic import BaseModel
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from central.config_models import RegionConfig


@dataclass
class FieldDescriptor:
    """Describes a form field for rendering."""
    name: str
    label: str
    widget: str  # "text", "number", "checkbox", "csv", "select", "checkboxes", "region"
    current_value: Any
    default: Any
    description: str
    required: bool
    options: list[str] | None = None  # For select/checkboxes widgets
    sub_fields: list["FieldDescriptor"] | None = None  # For model_list: per-column descriptors


def _is_literal(tp: type) -> bool:
    """Check if a type is a Literal type."""
    return get_origin(tp) is Literal


def _get_literal_values(tp: type) -> list[str]:
    """Extract the literal values from a Literal type."""
    return list(get_args(tp))


def _type_to_widget_and_options(field_name: str, field_type: type) -> tuple[str, list[str] | None]:
    """Map a Python type to a widget type and optional options list.

    Returns:
        Tuple of (widget_type, options_list_or_none)
    """
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
            return _type_to_widget_and_options(field_name, inner_type)

    # Check for Literal type (single select)
    if _is_literal(field_type):
        options = _get_literal_values(field_type)
        return "select", [str(o) for o in options]

    # Direct type checks
    if field_type is str:
        return "text", None
    if field_type is int:
        return "number", None
    if field_type is float:
        return "number", None
    if field_type is bool:
        return "checkbox", None
    if field_type is RegionConfig:
        return "region", None

    # Check for list types
    if origin is list:
        inner_type = args[0] if args else None

        # list[Literal[...]] -> checkboxes
        if inner_type is not None and _is_literal(inner_type):
            options = _get_literal_values(inner_type)
            return "checkboxes", [str(o) for o in options]

        # list[str] -> csv
        if inner_type is str:
            return "csv", None

        # list[<BaseModel>] -> repeatable per-row editor (sub-columns resolved
        # by describe_fields, which recurses into the row model).
        if isinstance(inner_type, type) and issubclass(inner_type, BaseModel):
            return "model_list", None

        raise NotImplementedError(
            f"Field '{field_name}' has unsupported list type: list[{inner_type.__name__ if inner_type else '?'}]"
        )

    # dict -> json textarea (generic; e.g. EnrichmentConfig.backend_settings).
    # The form renders the value as JSON; the POST handler parses it back.
    if field_type is dict or origin is dict:
        return "json", None

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


def _list_row_model(field_type: type) -> type[BaseModel] | None:
    """Row model M for a ``list[M]`` (or ``Optional[list[M]]``) field where M is
    a BaseModel subclass; else None."""
    origin = get_origin(field_type)
    args = get_args(field_type)
    if origin is Union or (origin is not None and type(None) in args):
        non_none = [a for a in args if a is not type(None)]
        return _list_row_model(non_none[0]) if non_none else None
    if origin is list:
        inner = args[0] if args else None
        if isinstance(inner, type) and issubclass(inner, BaseModel):
            return inner
    return None


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

        # Determine widget and options
        widget, options = _type_to_widget_and_options(field_name, field_type)

        # For a list-of-model field, recurse once to get column descriptors.
        sub_fields = None
        if widget == "model_list":
            row_model = _list_row_model(field_type)
            if row_model is not None:
                sub_fields = describe_fields(row_model, {})

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
            options=options,
            sub_fields=sub_fields,
        ))

    return descriptors
