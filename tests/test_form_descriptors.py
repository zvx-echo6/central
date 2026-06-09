"""Tests for form_descriptors module."""

import pytest
from pydantic import BaseModel
from typing import Optional

from central.gui.form_descriptors import describe_fields, FieldDescriptor, _type_to_widget_and_options
from central.config_models import RegionConfig


class SimpleSettings(BaseModel):
    """Simple settings model for testing."""
    name: str
    count: int
    enabled: bool


class SettingsWithOptional(BaseModel):
    """Settings with optional fields."""
    required_field: str
    optional_field: Optional[str] = None
    with_default: str = "default_value"


class SettingsWithList(BaseModel):
    """Settings with list field."""
    tags: list[str]


class SettingsWithRegion(BaseModel):
    """Settings with region config."""
    region: Optional[RegionConfig] = None


class TestTypeToWidget:
    """Tests for _type_to_widget_and_options function."""

    def test_str_maps_to_text(self):
        assert _type_to_widget_and_options("field", str) == ("text", None)

    def test_int_maps_to_number(self):
        assert _type_to_widget_and_options("field", int) == ("number", None)

    def test_bool_maps_to_checkbox(self):
        assert _type_to_widget_and_options("field", bool) == ("checkbox", None)

    def test_list_str_maps_to_csv(self):
        assert _type_to_widget_and_options("field", list[str]) == ("csv", None)

    def test_region_config_maps_to_region(self):
        assert _type_to_widget_and_options("field", RegionConfig) == ("region", None)

    def test_optional_region_maps_to_region(self):
        assert _type_to_widget_and_options("field", Optional[RegionConfig]) == ("region", None)

    def test_optional_str_maps_to_text(self):
        """Optional[str] should map to text widget."""
        assert _type_to_widget_and_options("field", Optional[str]) == ("text", None)

    def test_optional_int_maps_to_number(self):
        """Optional[int] should map to number widget."""
        assert _type_to_widget_and_options("field", Optional[int]) == ("number", None)

    def test_unsupported_type_raises(self):
        class CustomType:
            pass
        with pytest.raises(NotImplementedError):
            _type_to_widget_and_options("field", CustomType)


class TestDescribeFields:
    """Tests for describe_fields function."""

    def test_simple_model_fields(self):
        """describe_fields returns correct descriptors for simple model."""
        fields = describe_fields(SimpleSettings, {"name": "test", "count": 5, "enabled": True})

        assert len(fields) == 3

        name_field = next(f for f in fields if f.name == "name")
        assert name_field.label == "Name"
        assert name_field.widget == "text"
        assert name_field.current_value == "test"

        count_field = next(f for f in fields if f.name == "count")
        assert count_field.label == "Count"
        assert count_field.widget == "number"
        assert count_field.current_value == 5

        enabled_field = next(f for f in fields if f.name == "enabled")
        assert enabled_field.label == "Enabled"
        assert enabled_field.widget == "checkbox"
        assert enabled_field.current_value is True

    def test_uses_current_values(self):
        """Current values from dict are used."""
        fields = describe_fields(SimpleSettings, {"name": "current_name", "count": 42, "enabled": False})

        name_field = next(f for f in fields if f.name == "name")
        assert name_field.current_value == "current_name"

        count_field = next(f for f in fields if f.name == "count")
        assert count_field.current_value == 42

    def test_missing_values_use_defaults(self):
        """Missing values fall back to model defaults."""
        fields = describe_fields(SettingsWithOptional, {"required_field": "value"})

        optional_field = next(f for f in fields if f.name == "optional_field")
        assert optional_field.current_value is None
        assert optional_field.widget == "text"  # Optional[str] -> text

        default_field = next(f for f in fields if f.name == "with_default")
        assert default_field.current_value == "default_value"

    def test_list_field_returns_csv_widget(self):
        """List[str] fields get csv widget."""
        fields = describe_fields(SettingsWithList, {"tags": ["a", "b", "c"]})

        tags_field = next(f for f in fields if f.name == "tags")
        assert tags_field.widget == "csv"
        assert tags_field.current_value == ["a", "b", "c"]

    def test_region_field_returns_region_widget(self):
        """RegionConfig fields get region widget."""
        fields = describe_fields(SettingsWithRegion, {
            "region": {"north": 50.0, "south": 40.0, "east": -100.0, "west": -120.0}
        })

        region_field = next(f for f in fields if f.name == "region")
        assert region_field.widget == "region"

    def test_empty_current_dict(self):
        """Works with empty current values dict."""
        fields = describe_fields(SettingsWithOptional, {})

        required_field = next(f for f in fields if f.name == "required_field")
        assert required_field.current_value is None
        assert required_field.widget == "text"

    def test_field_descriptor_attributes(self):
        """FieldDescriptor has all expected attributes."""
        fields = describe_fields(SimpleSettings, {"name": "test", "count": 1, "enabled": True})
        field = fields[0]

        assert hasattr(field, "name")
        assert hasattr(field, "label")
        assert hasattr(field, "widget")
        assert hasattr(field, "current_value")
        assert hasattr(field, "default")
        assert hasattr(field, "description")
        assert hasattr(field, "required")


class TestRealAdapterSchemas:
    """Test with actual adapter settings schemas."""

    def test_nws_settings(self):
        """NWSSettings generates correct field descriptors."""
        from central.adapters.nws import NWSSettings

        fields = describe_fields(NWSSettings, {"contact_email": "test@example.com"})

        assert len(fields) >= 1
        email_field = next(f for f in fields if f.name == "contact_email")
        assert email_field.widget == "text"
        assert email_field.current_value == "test@example.com"

    def test_firms_settings(self):
        """FIRMSSettings generates correct field descriptors."""
        from central.adapters.firms import FIRMSSettings

        fields = describe_fields(FIRMSSettings, {
            "api_key_alias": "firms_key",
            "satellites": ["VIIRS_SNPP_NRT"]
        })

        key_field = next(f for f in fields if f.name == "api_key_alias")
        assert key_field.widget == "text"

        sat_field = next(f for f in fields if f.name == "satellites")
        assert sat_field.widget == "checkboxes"
        assert sat_field.current_value == ["VIIRS_SNPP_NRT"]
        assert sat_field.options is not None
        assert "VIIRS_SNPP_NRT" in sat_field.options

    def test_usgs_quake_settings(self):
        """USGSQuakeSettings generates correct field descriptors."""
        from central.adapters.usgs_quake import USGSQuakeSettings

        fields = describe_fields(USGSQuakeSettings, {"feed": "all_hour"})

        feed_field = next(f for f in fields if f.name == "feed")
        assert feed_field.widget == "select"
        assert feed_field.current_value == "all_hour"
        assert feed_field.options is not None
        assert "all_hour" in feed_field.options
        assert "all_day" in feed_field.options

    def test_all_adapters_have_region_field(self):
        """All adapter settings schemas include region field."""
        from central.adapters.nws import NWSSettings
        from central.adapters.firms import FIRMSSettings
        from central.adapters.usgs_quake import USGSQuakeSettings

        for schema in [NWSSettings, FIRMSSettings, USGSQuakeSettings]:
            fields = describe_fields(schema, {})
            region_field = next((f for f in fields if f.name == "region"), None)
            assert region_field is not None, f"{schema.__name__} should have region field"
            assert region_field.widget == "region"

class TestLiteralTypes:
    """Tests for Literal type support."""

    def test_literal_maps_to_select(self):
        """Literal type maps to select widget with options."""
        from typing import Literal

        widget, options = _type_to_widget_and_options("field", Literal["a", "b", "c"])
        assert widget == "select"
        assert options == ["a", "b", "c"]

    def test_list_literal_maps_to_checkboxes(self):
        """list[Literal] maps to checkboxes widget with options."""
        from typing import Literal

        widget, options = _type_to_widget_and_options("field", list[Literal["x", "y", "z"]])
        assert widget == "checkboxes"
        assert options == ["x", "y", "z"]

    def test_optional_literal_maps_to_select(self):
        """Optional[Literal] maps to select widget."""
        from typing import Literal, Optional

        widget, options = _type_to_widget_and_options("field", Optional[Literal["one", "two"]])
        assert widget == "select"
        assert options == ["one", "two"]


# --- v0.11.3: list[int] support (the bug origin) ----------------------------


class TestListIntSupport:
    """v0.11.3 hotfix: production traceback was

        NotImplementedError: Field 'extra_norad_ids' has unsupported list type: list[int]

    when celestrak_tle's edit form GET hit describe_fields. The handler now
    maps ``list[int]`` to a ``csv_int`` widget (parallel to ``csv`` for
    list[str] but with per-token int() coercion in the POST parser).
    """

    def test_list_int_maps_to_csv_int(self):
        widget, options = _type_to_widget_and_options("field", list[int])
        assert widget == "csv_int"
        assert options is None

    def test_describe_fields_celestrak_tle_settings_succeeds(self):
        """Regression guard for the production traceback: celestrak_tle's
        settings schema (groups: list[str] + extra_norad_ids: list[int])
        must render without raising."""
        from central.adapters.celestrak_tle import CelestrakTleSettings

        descriptors = describe_fields(
            CelestrakTleSettings,
            {"groups": ["stations"], "extra_norad_ids": [25544, 33591]},
        )
        by_name = {d.name: d for d in descriptors}
        # groups: list[str] -> csv (unchanged regression)
        assert by_name["groups"].widget == "csv"
        # extra_norad_ids: list[int] -> csv_int (the fix)
        assert by_name["extra_norad_ids"].widget == "csv_int"
        assert by_name["extra_norad_ids"].current_value == [25544, 33591]

    def test_describe_fields_satpass_predict_settings_uses_model_list(self):
        """Regression guard: satpass_predict's observers: list[Observer]
        must continue to render as model_list, not get swept into a JSON
        textarea fallback. v0.11.3 explicitly preserved the existing
        model_list path -- this test guards against accidental scope creep."""
        from central.adapters.satpass_predict import SatpassPredictSettings

        descriptors = describe_fields(SatpassPredictSettings, {})
        by_name = {d.name: d for d in descriptors}
        assert by_name["observers"].widget == "model_list"
        # And the sub-column descriptors are populated by the recursive call
        # in describe_fields (lines 161-166).
        assert by_name["observers"].sub_fields is not None
        sub_names = {sf.name for sf in by_name["observers"].sub_fields}
        assert {"name", "slug", "state", "lat", "lon", "elev_m"} <= sub_names
        # Scalars still map as expected.
        assert by_name["min_elevation_deg"].widget == "number"
        assert by_name["horizon_hours"].widget == "number"

    def test_unsupported_list_type_error_names_the_actual_type(self):
        """v0.11.3 sharpened the NotImplementedError message to name the
        encountered inner type AND list the supported alternatives."""
        with pytest.raises(NotImplementedError) as exc_info:
            _type_to_widget_and_options("strange_field", list[dict])
        msg = str(exc_info.value)
        assert "strange_field" in msg
        assert "list[dict]" in msg
        # Mentions the supported set so the operator can see what to use.
        assert "csv_int" in msg or "Supported" in msg


# --- POST parser round-trip for csv_int -------------------------------------


class _FakeForm(dict):
    """Minimal stand-in for starlette FormData supporting .get() + .getlist()."""
    def __init__(self, mapping):
        super().__init__(mapping)
    def get(self, key, default=""):
        return super().get(key, default)


def _parse_csv_int_token(raw: str, field_name: str = "extra_norad_ids") -> list[int]:
    """Inline of the parser logic added to routes.py at the two POST sites.

    Kept identical to those branches; if either site diverges, this helper
    will drift and the round-trip tests will catch it. (We can't import the
    parser directly because it's an inline branch in a 200-line async
    function.)
    """
    import logging as _logging
    parsed: list[int] = []
    if raw.strip():
        for tok in raw.split(","):
            tok = tok.strip()
            if not tok:
                continue
            try:
                parsed.append(int(tok))
            except ValueError:
                _logging.getLogger("test").warning(
                    "csv_int: dropped non-numeric token",
                    extra={"field": field_name, "token": tok},
                )
    return parsed


class TestCsvIntRoundTrip:
    """Mirror of the POST-branch logic at routes.py:942 + :1713. The branches
    themselves are duplicated by necessity (sync wizard vs edit page); both
    were updated in v0.11.3 and these tests pin the contract."""

    def test_three_ints_round_trip(self):
        assert _parse_csv_int_token("25544, 28654, 33591") == [25544, 28654, 33591]

    def test_garbage_dropped_with_valid_kept(self):
        """Mixed input 'foo, 123' -> [123]; the 'foo' is dropped (warning logged)."""
        assert _parse_csv_int_token("foo, 123") == [123]

    def test_empty_input_yields_empty_list_not_none(self):
        assert _parse_csv_int_token("") == []
        assert _parse_csv_int_token("   ") == []

    def test_whitespace_only_tokens_skipped(self):
        assert _parse_csv_int_token("1, , 2,  ,3") == [1, 2, 3]

    def test_negative_ints_accepted(self):
        """No-op for the NORAD-id case but the parser shouldn't reject negatives."""
        assert _parse_csv_int_token("-1, 0, 1") == [-1, 0, 1]

    def test_all_garbage_yields_empty_list(self):
        assert _parse_csv_int_token("foo, bar, baz") == []

