"""Tests for requires_api_key enforcement."""

import pytest
from unittest.mock import MagicMock, AsyncMock, patch


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
            {"name": "firms", "enabled": False, "cadence_s": 300, "settings": {}, "paused_at": None, "updated_at": None, "last_error": None},
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
