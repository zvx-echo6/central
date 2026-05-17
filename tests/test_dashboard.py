"""Tests for dashboard routes."""

import os
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

# Set required env vars before importing central modules
os.environ.setdefault("CENTRAL_DB_DSN", "postgresql://test:test@localhost/test")
os.environ.setdefault("CENTRAL_CSRF_SECRET", "testsecret12345678901234567890ab")
os.environ.setdefault("CENTRAL_NATS_URL", "nats://localhost:4222")


class TestFormatBytes:
    """Test _format_bytes helper."""

    def test_format_bytes_bytes(self):
        """Bytes are shown as B."""
        from central.gui.routes import _format_bytes
        assert _format_bytes(100) == "100 B"

    def test_format_bytes_kilobytes(self):
        """KB formatting."""
        from central.gui.routes import _format_bytes
        assert _format_bytes(1024) == "1.0 KB"

    def test_format_bytes_megabytes(self):
        """MB formatting."""
        from central.gui.routes import _format_bytes
        assert _format_bytes(1048576) == "1.0 MB"

    def test_format_bytes_gigabytes(self):
        """GB formatting."""
        from central.gui.routes import _format_bytes
        assert _format_bytes(1073741824) == "1.0 GB"


class TestDashboardEventsSQL:
    """Test events query construction."""

    def test_events_query_has_24h_filter(self):
        """Events query filters by received > NOW() - 24h."""
        # We can't easily test the full route without mocking,
        # but we can verify the query logic by inspecting the source
        import inspect
        from central.gui.routes import dashboard_events
        source = inspect.getsource(dashboard_events)
        assert "24 hours" in source
        assert "received > NOW()" in source


class TestDashboardStreamsGracefulDegradation:
    """Test streams endpoint graceful degradation."""

    @pytest.mark.asyncio
    async def test_nats_unavailable_returns_error_message(self):
        """When NATS is unavailable, streams returns error message not 500."""
        from central.gui.routes import dashboard_streams

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock()

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_templates.TemplateResponse.return_value = mock_response

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.nats.get_js", return_value=None):
                result = await dashboard_streams(mock_request)

        # Should call template with error context
        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert context["error"] == "NATS unavailable"
        assert context["streams"] is None


class TestDashboardPollsGracefulDegradation:
    """Test polls endpoint graceful degradation."""

    @pytest.mark.asyncio
    async def test_nats_unavailable_shows_all_adapters_with_error(self):
        """When NATS is unavailable, polls shows adapters with error message."""
        from central.gui.routes import dashboard_polls

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock()

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [{"name": "nws"}, {"name": "firms"}]

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_templates.TemplateResponse.return_value = mock_response

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                with patch("central.gui.nats.get_js", return_value=None):
                    result = await dashboard_polls(mock_request)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert context["error"] == "NATS unavailable"
        assert len(context["adapters"]) == 2
        assert context["adapters"][0]["error"] == "NATS unavailable"


class TestDashboardStreamsIsolation:
    """Test stream failure isolation."""

    @pytest.mark.asyncio
    async def test_single_stream_failure_doesnt_crash_card(self):
        """A single stream failure shows error for that stream only."""
        from central.gui.routes import dashboard_streams

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock()

        async def mock_stream_info(name):
            if name == "CENTRAL_FIRE":
                raise Exception("Not found")
            state = MagicMock()
            state.messages = 100
            state.bytes = 1024
            info = MagicMock()
            info.state = state
            return info

        mock_js = AsyncMock()
        mock_js.stream_info.side_effect = mock_stream_info

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_templates.TemplateResponse.return_value = mock_response

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.nats.get_js", return_value=mock_js):
                result = await dashboard_streams(mock_request)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))

        streams = context["streams"]
        # Should have 4 streams
        assert len(streams) == 4

        # CENTRAL_FIRE should have error
        fire_stream = next(s for s in streams if s["name"] == "CENTRAL_FIRE")
        assert fire_stream.get("error") == "unavailable"

        # CENTRAL_WX should be fine
        wx_stream = next(s for s in streams if s["name"] == "CENTRAL_WX")
        assert wx_stream.get("error") is None
        assert wx_stream["messages"] == 100
