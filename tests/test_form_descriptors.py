"""Tests for form_descriptors module."""

import pytest
from pydantic import BaseModel
from typing import Optional

from central.gui.form_descriptors import describe_fields, FieldDescriptor, _type_to_widget
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
    """Tests for _type_to_widget function."""

    def test_str_maps_to_text(self):
        assert _type_to_widget("field", str) == "text"

    def test_int_maps_to_number(self):
        assert _type_to_widget("field", int) == "number"

    def test_bool_maps_to_checkbox(self):
        assert _type_to_widget("field", bool) == "checkbox"

    def test_list_str_maps_to_csv(self):
        assert _type_to_widget("field", list[str]) == "csv"

    def test_region_config_maps_to_region(self):
        assert _type_to_widget("field", RegionConfig) == "region"

    def test_optional_region_maps_to_region(self):
        assert _type_to_widget("field", Optional[RegionConfig]) == "region"

    def test_optional_str_maps_to_text(self):
        """Optional[str] should map to text widget."""
        assert _type_to_widget("field", Optional[str]) == "text"

    def test_optional_int_maps_to_number(self):
        """Optional[int] should map to number widget."""
        assert _type_to_widget("field", Optional[int]) == "number"

    def test_unsupported_type_raises(self):
        class CustomType:
            pass
        with pytest.raises(NotImplementedError):
            _type_to_widget("field", CustomType)


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
            "satellites": ["VIIRS_SNPP"]
        })

        key_field = next(f for f in fields if f.name == "api_key_alias")
        assert key_field.widget == "text"

        sat_field = next(f for f in fields if f.name == "satellites")
        assert sat_field.widget == "csv"
        assert sat_field.current_value == ["VIIRS_SNPP"]

    def test_usgs_quake_settings(self):
        """USGSQuakeSettings generates correct field descriptors."""
        from central.adapters.usgs_quake import USGSQuakeSettings

        fields = describe_fields(USGSQuakeSettings, {"feed": "all_hour"})

        feed_field = next(f for f in fields if f.name == "feed")
        assert feed_field.widget == "text"
        assert feed_field.current_value == "all_hour"

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
