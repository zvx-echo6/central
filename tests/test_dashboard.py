"""Tests for dashboard routes."""

import json
import os
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

os.environ.setdefault("CENTRAL_DB_DSN", "postgresql://test:test@localhost/test")
os.environ.setdefault("CENTRAL_CSRF_SECRET", "testsecret12345678901234567890ab")
os.environ.setdefault("CENTRAL_NATS_URL", "nats://localhost:4222")


class TestFormatBytes:
    def test_format_bytes_bytes(self):
        from central.gui.routes import _format_bytes
        assert _format_bytes(100) == "100 B"

    def test_format_bytes_kilobytes(self):
        from central.gui.routes import _format_bytes
        assert _format_bytes(1024) == "1.0 KB"

    def test_format_bytes_megabytes(self):
        from central.gui.routes import _format_bytes
        assert _format_bytes(1048576) == "1.0 MB"

    def test_format_bytes_gigabytes(self):
        from central.gui.routes import _format_bytes
        assert _format_bytes(1073741824) == "1.0 GB"


class TestDashboardEventsSQL:
    def test_events_query_has_24h_filter(self):
        import inspect
        from central.gui.routes import dashboard_events
        source = inspect.getsource(dashboard_events)
        assert "24 hours" in source
        assert "received > NOW()" in source


class TestDashboardStreamsGracefulDegradation:
    @pytest.mark.asyncio
    async def test_nats_unavailable_returns_error_message(self):
        from central.gui.routes import dashboard_streams
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock()
        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_templates.TemplateResponse.return_value = mock_response
        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.nats.get_js", return_value=None):
                result = await dashboard_streams(mock_request)
        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert context["error"] == "NATS unavailable"
        assert context["streams"] is None


class TestDashboardPollsGracefulDegradation:
    @pytest.mark.asyncio
    async def test_nats_unavailable_shows_all_adapters_with_error(self):
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


class TestDashboardPollsGetLastMsg:
    @pytest.mark.asyncio
    async def test_polls_returns_timestamp_from_status_message(self):
        from central.gui.routes import dashboard_polls
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock()
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [{"name": "nws"}]
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_msg = MagicMock()
        mock_msg.data = json.dumps({"ok": True, "ts": "2026-05-17T12:34:56Z"}).encode()
        mock_js = AsyncMock()
        mock_js.get_last_msg = AsyncMock(return_value=mock_msg)
        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_templates.TemplateResponse.return_value = mock_response
        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                with patch("central.gui.nats.get_js", return_value=mock_js):
                    result = await dashboard_polls(mock_request)
        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert len(context["adapters"]) == 1
        adapter = context["adapters"][0]
        assert adapter["name"] == "nws"
        assert adapter["last_poll"] == "2026-05-17T12:34:56Z"
        assert adapter["status"] == "\u2713"
        assert adapter["error"] is None

    @pytest.mark.asyncio
    async def test_polls_handles_not_found_error_gracefully(self):
        from central.gui.routes import dashboard_polls
        from nats.js.errors import NotFoundError
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock()
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [{"name": "nws"}]
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_js = AsyncMock()
        mock_js.get_last_msg = AsyncMock(side_effect=NotFoundError())
        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_templates.TemplateResponse.return_value = mock_response
        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                with patch("central.gui.nats.get_js", return_value=mock_js):
                    result = await dashboard_polls(mock_request)
        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        adapter = context["adapters"][0]
        assert adapter["last_poll"] is None
        assert adapter["status"] is None
        assert adapter["error"] is None

    @pytest.mark.asyncio
    async def test_polls_shows_failure_status_when_ok_is_false(self):
        from central.gui.routes import dashboard_polls
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock()
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [{"name": "nws"}]
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_msg = MagicMock()
        mock_msg.data = json.dumps({"ok": False, "ts": "2026-05-17T12:34:56Z", "error": "Connection timeout"}).encode()
        mock_js = AsyncMock()
        mock_js.get_last_msg = AsyncMock(return_value=mock_msg)
        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_templates.TemplateResponse.return_value = mock_response
        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                with patch("central.gui.nats.get_js", return_value=mock_js):
                    result = await dashboard_polls(mock_request)
        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        adapter = context["adapters"][0]
        assert adapter["status"] == "\u2717"
        assert adapter["error"] == "Connection timeout"


class TestDashboardPollsNoSubscribe:
    def test_polls_does_not_use_pull_subscribe(self):
        import inspect
        from central.gui.routes import dashboard_polls
        source = inspect.getsource(dashboard_polls)
        assert "pull_subscribe" not in source
        assert "get_last_msg" in source
        assert "central.meta.adapter." in source


class TestDashboardStreamsIsolation:
    @pytest.mark.asyncio
    async def test_single_stream_failure_doesnt_crash_card(self):
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
        fire_stream = next(s for s in streams if s["name"] == "CENTRAL_FIRE")
        assert fire_stream.get("error") == "unavailable"
        wx_stream = next(s for s in streams if s["name"] == "CENTRAL_WX")
        assert wx_stream.get("error") is None
        assert wx_stream["messages"] == 100
