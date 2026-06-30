"""Tests for requires_api_key enforcement."""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

from central.config_models import AdapterConfig


class TestConfigStoreSetAdapterLastError:
    """Tests for ConfigStore.set_adapter_last_error method."""

    @pytest.mark.asyncio
    async def test_set_adapter_last_error_updates_row(self):
        """set_adapter_last_error should update the last_error column."""
        from central.config_store import ConfigStore

        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=mock_conn)

        config_store = ConfigStore.__new__(ConfigStore)
        config_store._pool = mock_pool

        await config_store.set_adapter_last_error("firms", "missing api key: firms")

        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args[0]
        assert "UPDATE config.adapters SET last_error" in call_args[0]
        assert call_args[1] == "missing api key: firms"
        assert call_args[2] == "firms"

    @pytest.mark.asyncio
    async def test_clear_adapter_last_error(self):
        """set_adapter_last_error with None should clear the error."""
        from central.config_store import ConfigStore

        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=mock_conn)

        config_store = ConfigStore.__new__(ConfigStore)
        config_store._pool = mock_pool

        await config_store.set_adapter_last_error("firms", None)

        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args[0]
        assert call_args[1] is None
        assert call_args[2] == "firms"


class TestRoutesApiKeyMissing:
    """Tests for routes api_key_missing computation."""

    @pytest.mark.asyncio
    async def test_adapters_list_includes_api_key_missing_flag(self):
        """adapters_list should compute api_key_missing for each adapter."""
        from central.gui.routes import adapters_list

        mock_request = MagicMock()
        mock_request.state = MagicMock()
        mock_request.state.operator = {"username": "test"}
        mock_request.state.csrf_token = "test_token"

        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(return_value=[
            {"name": "firms", "kind": "firms", "enabled": False, "cadence_s": 300, "settings": {}, "paused_at": None, "updated_at": None, "last_error": None},
        ])
        mock_conn.fetchval = AsyncMock(return_value=None)  # No API key exists
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=mock_conn)

        # Mock adapter class with requires_api_key
        mock_firms_cls = MagicMock()
        mock_firms_cls.requires_api_key = "firms"
        mock_firms_cls.display_name = "FIRMS"

        with patch("central.gui.routes._get_templates") as mock_templates:
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                with patch("central.gui.routes._adapter_classes", return_value={"firms": mock_firms_cls}):
                    mock_template_response = MagicMock()
                    mock_templates.return_value.TemplateResponse = MagicMock(return_value=mock_template_response)

                    await adapters_list(mock_request)

                    # Check the context passed to template
                    call_kwargs = mock_templates.return_value.TemplateResponse.call_args[1]
                    adapters = call_kwargs["context"]["adapters"]

                    assert len(adapters) == 1
                    assert adapters[0]["api_key_missing"] is True
                    assert adapters[0]["requires_api_key_alias"] == "firms"


class TestAdapterClassRequiresApiKey:
    """Tests for adapter class requires_api_key attribute."""

    def test_firms_adapter_requires_api_key(self):
        """FIRMS adapter should declare requires_api_key."""
        from central.adapters.firms import FIRMSAdapter
        assert FIRMSAdapter.requires_api_key == "firms"

    def test_nws_adapter_no_requires_api_key(self):
        """NWS adapter should not require an API key."""
        from central.adapters.nws import NWSAdapter
        assert NWSAdapter.requires_api_key is None

    def test_usgs_quake_adapter_no_requires_api_key(self):
        """USGS Quake adapter should not require an API key."""
        from central.adapters.usgs_quake import USGSQuakeAdapter
        assert USGSQuakeAdapter.requires_api_key is None


class TestSupervisorApiKeyPrecondition:
    """Tests for supervisor API key precondition check in _start_adapter."""

    @pytest.mark.asyncio
    async def test_start_adapter_refuses_when_required_key_missing(self, tmp_path: Path):
        """Adapter with requires_api_key but missing key should not start."""
        from central.supervisor import Supervisor
        from central.adapters.firms import FIRMSAdapter

        # Create mock config store
        mock_config_store = MagicMock()
        mock_config_store.get_api_key = AsyncMock(return_value=None)  # Key missing
        mock_config_store.set_adapter_last_error = AsyncMock()

        # Create mock NATS
        mock_nats = MagicMock()
        mock_nats.publish = AsyncMock()

        # Build supervisor with FIRMS adapter
        supervisor = Supervisor.__new__(Supervisor)
        supervisor._config_store = mock_config_store
        supervisor._adapters = {"firms": FIRMSAdapter}
        supervisor._adapter_states = {}
        supervisor._nats = mock_nats
        supervisor._cursor_db_path = tmp_path / "cursors.db"
        supervisor._log = MagicMock()

        config = AdapterConfig(
            name="firms",
            enabled=True,
            cadence_s=300,
            settings={"api_key_alias": "firms", "satellites": ["VIIRS_SNPP_NRT"]},
            updated_at=datetime.now(timezone.utc),
        )

        await supervisor._start_adapter(config)

        # Should have checked for key
        mock_config_store.get_api_key.assert_called_once_with("firms")

        # Should have set error
        mock_config_store.set_adapter_last_error.assert_called_once()
        args = mock_config_store.set_adapter_last_error.call_args[0]
        assert args[0] == "firms"
        assert "missing api key" in args[1].lower()

        # Should NOT have created adapter state (adapter did not start)
        assert "firms" not in supervisor._adapter_states

        # Should NOT have published to NATS
        mock_nats.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_adapter_succeeds_after_key_added_and_clears_last_error(self, tmp_path: Path):
        """Adapter with requires_api_key and key present should start and clear last_error."""
        from central.supervisor import Supervisor
        from central.adapters.firms import FIRMSAdapter

        # Create mock config store with key present
        mock_config_store = MagicMock()
        mock_config_store.get_api_key = AsyncMock(return_value="encrypted-firms-key")
        mock_config_store.set_adapter_last_error = AsyncMock()

        # Create mock NATS
        mock_nats = MagicMock()
        mock_nats.publish = AsyncMock()

        # Build supervisor with FIRMS adapter
        supervisor = Supervisor.__new__(Supervisor)
        supervisor._config_store = mock_config_store
        supervisor._adapters = {"firms": FIRMSAdapter}
        supervisor._adapter_states = {}
        supervisor._nats = mock_nats
        supervisor._cursor_db_path = tmp_path / "cursors.db"
        supervisor._log = MagicMock()

        config = AdapterConfig(
            name="firms",
            enabled=True,
            cadence_s=300,
            settings={"api_key_alias": "firms", "satellites": ["VIIRS_SNPP_NRT"]},
            updated_at=datetime.now(timezone.utc),
        )

        # Mock the adapter instantiation to avoid actual HTTP calls
        with patch.object(FIRMSAdapter, "__init__", return_value=None):
            with patch.object(FIRMSAdapter, "startup", new_callable=AsyncMock):
                await supervisor._start_adapter(config)

        # Should have checked for key
        mock_config_store.get_api_key.assert_called_once_with("firms")

        # Should have cleared any stale error (called with None)
        mock_config_store.set_adapter_last_error.assert_called_once_with("firms", None)

        # Should have created adapter state
        assert "firms" in supervisor._adapter_states

    @pytest.mark.asyncio
    async def test_start_adapter_does_not_check_when_no_requires_api_key(self, tmp_path: Path):
        """Adapter without requires_api_key should skip the API key check."""
        from central.supervisor import Supervisor
        from central.adapters.nws import NWSAdapter

        # Create mock config store
        mock_config_store = MagicMock()
        mock_config_store.get_api_key = AsyncMock()
        mock_config_store.set_adapter_last_error = AsyncMock()

        # Create mock NATS
        mock_nats = MagicMock()
        mock_nats.publish = AsyncMock()

        # Build supervisor with NWS adapter (no requires_api_key)
        supervisor = Supervisor.__new__(Supervisor)
        supervisor._config_store = mock_config_store
        supervisor._adapters = {"nws": NWSAdapter}
        supervisor._adapter_states = {}
        supervisor._nats = mock_nats
        supervisor._cursor_db_path = tmp_path / "cursors.db"
        supervisor._log = MagicMock()

        config = AdapterConfig(
            name="nws",
            enabled=True,
            cadence_s=300,
            settings={"contact_email": "test@example.com"},
            updated_at=datetime.now(timezone.utc),
        )

        # Mock the adapter instantiation to avoid actual HTTP calls
        with patch.object(NWSAdapter, "__init__", return_value=None):
            with patch.object(NWSAdapter, "startup", new_callable=AsyncMock):
                await supervisor._start_adapter(config)

        # Should NOT have called get_api_key (no requires_api_key)
        mock_config_store.get_api_key.assert_not_called()

        # Should have cleared stale error (routine clear)
        mock_config_store.set_adapter_last_error.assert_called_once_with("nws", None)

        # Should have created adapter state
        assert "nws" in supervisor._adapter_states


class TestAdaptersEditSubmitErrorRerender:
    """Tests for adapters_edit_submit error re-render including api_key_missing."""

    @pytest.mark.asyncio
    async def test_adapters_edit_submit_error_rerender_includes_api_key_missing(self):
        """Error re-render on /adapters/firms should include api_key_missing in context."""
        from central.gui.routes import adapters_edit_submit
        from pydantic import BaseModel
        from typing import Literal

        mock_request = MagicMock()
        mock_request.state = MagicMock()
        mock_request.state.operator = {"username": "test"}
        mock_request.state.csrf_token = "test_token"

        # Mock form with invalid cadence (below minimum of 10)
        mock_form = MagicMock()
        def form_get(k, d=""):
            values = {
                "csrf_token": "test_token",
                "cadence_s": "5",  # Invalid - below minimum
                "api_key_alias": "firms",
                "satellites": "",
                "region_north": "",
                "region_south": "",
                "region_east": "",
                "region_west": "",
            }
            return values.get(k, d)
        mock_form.get = MagicMock(side_effect=form_get)
        mock_form.getlist = MagicMock(return_value=["VIIRS_SNPP_NRT"])
        mock_form.__contains__ = lambda self, k: k == "enabled"
        mock_request.form = AsyncMock(return_value=mock_form)

        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(side_effect=[
            # First call: adapter row
            {
                "name": "firms",
                "kind": "firms",
                "enabled": False,
                "cadence_s": 300,
                "settings": {"api_key_alias": "firms", "satellites": ["VIIRS_SNPP_NRT"]},
                "paused_at": None,
                "updated_at": datetime.now(timezone.utc),
                "last_error": None,
            },
            # Second call: system row for map tiles
            {"map_tile_url": "https://tile.example.com/{z}/{x}/{y}.png", "map_attribution": "Test"},
        ])
        mock_conn.fetchval = AsyncMock(return_value=None)  # No API key exists
        mock_conn.fetch = AsyncMock(return_value=[])  # No API keys
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=mock_conn)

        # Mock FIRMS adapter class
        class MockFIRMSSettings(BaseModel):
            api_key_alias: str = ""
            satellites: list[Literal["VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT"]] = []

        mock_firms_cls = MagicMock()
        mock_firms_cls.requires_api_key = "firms"
        mock_firms_cls.api_key_field = "api_key_alias"
        mock_firms_cls.display_name = "FIRMS"
        mock_firms_cls.description = "Fire detection"
        mock_firms_cls.settings_schema = MockFIRMSSettings

        with patch("central.gui.routes._get_templates") as mock_templates:
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                with patch("central.gui.routes._adapter_classes", return_value={"firms": mock_firms_cls}):
                    with patch("central.gui.routes.describe_fields", return_value=[]):
                        mock_template_response = MagicMock()
                        mock_template_response.status_code = 200
                        mock_templates.return_value.TemplateResponse = MagicMock(return_value=mock_template_response)

                        result = await adapters_edit_submit(mock_request, "firms")

                        # Verify TemplateResponse was called (error re-render)
                        assert mock_templates.return_value.TemplateResponse.called

                        # Check the context passed to template
                        call_kwargs = mock_templates.return_value.TemplateResponse.call_args[1]
                        context = call_kwargs["context"]

                        # Should have errors (invalid cadence)
                        assert context.get("errors") is not None
                        assert "cadence_s" in context["errors"]

                        # Should include api_key_missing
                        assert context["api_key_missing"] is True
                        assert context["requires_api_key_alias"] == "firms"
