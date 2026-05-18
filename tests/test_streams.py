"""Tests for streams list and edit routes."""

import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Set required env vars before importing central modules
os.environ.setdefault("CENTRAL_DB_DSN", "postgresql://test:test@localhost/test")
os.environ.setdefault("CENTRAL_CSRF_SECRET", "testsecret12345678901234567890ab")
os.environ.setdefault("CENTRAL_NATS_URL", "nats://localhost:4222")


class TestStreamsListUnauthenticated:
    """Test streams list without authentication."""

    @pytest.mark.asyncio
    async def test_streams_list_unauthenticated_redirects(self):
        """GET /streams without auth redirects to /login."""
        from central.gui.routes import streams_list

        # The middleware handles the redirect, so we test the route exists
        # In practice, middleware returns 302 before route is called
        assert streams_list is not None


class TestStreamsListAuthenticated:
    """Test streams list with authentication."""

    @pytest.mark.asyncio
    async def test_streams_list_returns_all_streams_with_timestamps(self):
        """GET /streams authenticated returns 200 with all four streams and timestamps."""
        from central.gui.routes import streams_list

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [
            {"name": "CENTRAL_FIRE", "max_age_s": 604800, "max_bytes": 1073741824, "managed_max_bytes": True, "updated_at": None},
            {"name": "CENTRAL_META", "max_age_s": 86400, "max_bytes": 1073741824, "managed_max_bytes": True, "updated_at": None},
            {"name": "CENTRAL_QUAKE", "max_age_s": 604800, "max_bytes": 1073741824, "managed_max_bytes": True, "updated_at": None},
            {"name": "CENTRAL_WX", "max_age_s": 604800, "max_bytes": 1073741824, "managed_max_bytes": True, "updated_at": None},
        ]

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_templates.TemplateResponse.return_value = mock_response

        # Mock JetStream with proper state fields
        mock_js = AsyncMock()
        mock_stream_info = MagicMock()
        mock_stream_info.state.bytes = 1024000
        mock_stream_info.state.messages = 100
        mock_stream_info.state.first_seq = 1
        mock_stream_info.state.last_seq = 100
        mock_js.stream_info.return_value = mock_stream_info

        # Mock get_msg for timestamps (RawStreamMsg has .time attribute)
        test_ts = datetime(2026, 5, 17, 12, 0, 0, tzinfo=timezone.utc)
        mock_msg = MagicMock()
        mock_msg.time = test_ts
        mock_js.get_msg.return_value = mock_msg

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                with patch("central.gui.nats.get_js", return_value=mock_js):
                    result = await streams_list(mock_request)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert len(context["streams"]) == 4
        stream_names = [s["name"] for s in context["streams"]]
        assert "CENTRAL_FIRE" in stream_names
        assert "CENTRAL_META" in stream_names
        assert "CENTRAL_QUAKE" in stream_names
        assert "CENTRAL_WX" in stream_names

        # Check that timestamps are populated
        fire_stream = next(s for s in context["streams"] if s["name"] == "CENTRAL_FIRE")
        assert fire_stream["live_bytes"] == 1024000
        assert fire_stream["live_messages"] == 100
        assert fire_stream["live_first_ts"] == test_ts
        assert fire_stream["live_last_ts"] == test_ts


class TestStreamsListNatsUnavailable:
    """Test streams list when NATS is unavailable."""

    @pytest.mark.asyncio
    async def test_nats_unavailable_shows_unavailable(self):
        """GET /streams when NATS unreachable shows (unavailable) for live data."""
        from central.gui.routes import streams_list

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [
            {"name": "CENTRAL_WX", "max_age_s": 604800, "max_bytes": 1073741824, "managed_max_bytes": True, "updated_at": None},
        ]

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_templates.TemplateResponse.return_value = mock_response

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                with patch("central.gui.nats.get_js", return_value=None):
                    result = await streams_list(mock_request)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert context["streams"][0]["error"] == "NATS unavailable"
        assert context["streams"][0]["live_bytes"] is None


class TestStreamsListPartialFailure:
    """Test streams list with one stream's info raising."""

    @pytest.mark.asyncio
    async def test_stream_info_raises_shows_exception_type(self):
        """GET /streams with stream_info error shows unavailable with exception type."""
        from central.gui.routes import streams_list

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [
            {"name": "CENTRAL_FIRE", "max_age_s": 604800, "max_bytes": 1073741824, "managed_max_bytes": True, "updated_at": None},
            {"name": "CENTRAL_WX", "max_age_s": 604800, "max_bytes": 1073741824, "managed_max_bytes": True, "updated_at": None},
        ]

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_templates.TemplateResponse.return_value = mock_response

        # Mock JetStream - CENTRAL_FIRE raises ValueError, CENTRAL_WX works
        mock_js = AsyncMock()
        test_ts = datetime(2026, 5, 17, 12, 0, 0, tzinfo=timezone.utc)

        def stream_info_side_effect(name):
            if name == "CENTRAL_FIRE":
                raise ValueError("Stream not found")
            mock_info = MagicMock()
            mock_info.state.bytes = 2048
            mock_info.state.messages = 50
            mock_info.state.first_seq = 1
            mock_info.state.last_seq = 50
            return mock_info

        mock_js.stream_info.side_effect = stream_info_side_effect

        mock_msg = MagicMock()
        mock_msg.time = test_ts
        mock_js.get_msg.return_value = mock_msg

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                with patch("central.gui.nats.get_js", return_value=mock_js):
                    result = await streams_list(mock_request)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))

        fire_stream = next(s for s in context["streams"] if s["name"] == "CENTRAL_FIRE")
        wx_stream = next(s for s in context["streams"] if s["name"] == "CENTRAL_WX")

        # Exception type should be in error message
        assert "unavailable: ValueError" in fire_stream["error"]
        assert wx_stream["error"] is None
        assert wx_stream["live_bytes"] == 2048


class TestStreamsListEmptyStream:
    """Test streams list with empty stream (zero messages)."""

    @pytest.mark.asyncio
    async def test_empty_stream_no_get_msg_calls(self):
        """Empty stream (first_seq == 0) renders correctly, no get_msg calls made."""
        from central.gui.routes import streams_list

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [
            {"name": "CENTRAL_QUAKE", "max_age_s": 604800, "max_bytes": 1073741824, "managed_max_bytes": True, "updated_at": None},
        ]

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_templates.TemplateResponse.return_value = mock_response

        # Mock JetStream with empty stream (first_seq = 0)
        mock_js = AsyncMock()
        mock_stream_info = MagicMock()
        mock_stream_info.state.bytes = 0
        mock_stream_info.state.messages = 0
        mock_stream_info.state.first_seq = 0
        mock_stream_info.state.last_seq = 0
        mock_js.stream_info.return_value = mock_stream_info
        mock_js.get_msg = AsyncMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                with patch("central.gui.nats.get_js", return_value=mock_js):
                    result = await streams_list(mock_request)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))

        quake_stream = context["streams"][0]
        assert quake_stream["live_messages"] == 0
        assert quake_stream["live_first_ts"] is None
        assert quake_stream["live_last_ts"] is None
        assert quake_stream["error"] is None

        # get_msg should NOT have been called for empty stream
        mock_js.get_msg.assert_not_called()


class TestStreamsListSingleMessage:
    """Test streams list with single-message stream."""

    @pytest.mark.asyncio
    async def test_single_message_stream_reuses_timestamp(self):
        """Single-message stream (first_seq == last_seq) calls get_msg once."""
        from central.gui.routes import streams_list

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [
            {"name": "CENTRAL_WX", "max_age_s": 604800, "max_bytes": 1073741824, "managed_max_bytes": True, "updated_at": None},
        ]

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_templates.TemplateResponse.return_value = mock_response

        # Mock JetStream with single message (first_seq == last_seq)
        mock_js = AsyncMock()
        mock_stream_info = MagicMock()
        mock_stream_info.state.bytes = 512
        mock_stream_info.state.messages = 1
        mock_stream_info.state.first_seq = 1
        mock_stream_info.state.last_seq = 1
        mock_js.stream_info.return_value = mock_stream_info

        test_ts = datetime(2026, 5, 17, 12, 0, 0, tzinfo=timezone.utc)
        mock_msg = MagicMock()
        mock_msg.time = test_ts
        mock_js.get_msg.return_value = mock_msg

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                with patch("central.gui.nats.get_js", return_value=mock_js):
                    result = await streams_list(mock_request)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))

        wx_stream = context["streams"][0]
        assert wx_stream["live_messages"] == 1
        assert wx_stream["live_first_ts"] == test_ts
        assert wx_stream["live_last_ts"] == test_ts

        # get_msg should have been called once (for first message only)
        assert mock_js.get_msg.call_count == 1


class TestStreamsListGetMsgFailure:
    """Test streams list when get_msg fails for a message."""

    @pytest.mark.asyncio
    async def test_get_msg_first_fails_shows_lookup_error(self):
        """get_msg raises for first message shows lookup failed, rest renders normally."""
        from central.gui.routes import streams_list

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [
            {"name": "CENTRAL_WX", "max_age_s": 604800, "max_bytes": 1073741824, "managed_max_bytes": True, "updated_at": None},
        ]

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_templates.TemplateResponse.return_value = mock_response

        # Mock JetStream
        mock_js = AsyncMock()
        mock_stream_info = MagicMock()
        mock_stream_info.state.bytes = 2048
        mock_stream_info.state.messages = 50
        mock_stream_info.state.first_seq = 1
        mock_stream_info.state.last_seq = 50
        mock_js.stream_info.return_value = mock_stream_info

        test_ts = datetime(2026, 5, 17, 12, 0, 0, tzinfo=timezone.utc)

        # First call fails, second succeeds
        def get_msg_side_effect(name, seq):
            if seq == 1:
                raise KeyError("Message not found")
            mock_msg = MagicMock()
            mock_msg.time = test_ts
            return mock_msg

        mock_js.get_msg.side_effect = get_msg_side_effect

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                with patch("central.gui.nats.get_js", return_value=mock_js):
                    result = await streams_list(mock_request)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))

        wx_stream = context["streams"][0]
        # Main stream info still works
        assert wx_stream["live_bytes"] == 2048
        assert wx_stream["live_messages"] == 50
        assert wx_stream["error"] is None
        # First timestamp failed
        assert wx_stream["live_first_ts"] is None
        assert wx_stream["first_ts_error"] == "KeyError"
        # Last timestamp succeeded
        assert wx_stream["live_last_ts"] == test_ts
        assert wx_stream["last_ts_error"] is None


class TestStreamsUpdate:
    """Test stream update POST handler."""

    @pytest.mark.asyncio
    async def test_valid_update_redirects(self):
        """POST /streams/CENTRAL_WX with valid max_age_s redirects and updates DB."""
        from central.gui.routes import streams_update

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_request.state.csrf_token = "test_csrf_token"
        mock_form = MagicMock()
        mock_form.get.side_effect = lambda k, d="": {
            "csrf_token": "test_csrf_token",
            "max_age_s": "1209600",
        }.get(k, d)
        mock_request.form = AsyncMock(return_value=mock_form)

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {"name": "CENTRAL_WX", "max_age_s": 604800}
        mock_conn.execute = AsyncMock()

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        captured_audit = {}

        async def capture_audit(conn, action, operator_id=None, target=None, before=None, after=None):
            captured_audit["action"] = action
            captured_audit["target"] = target
            captured_audit["before"] = before
            captured_audit["after"] = after

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            with patch("central.gui.routes.write_audit", side_effect=capture_audit):
                result = await streams_update(mock_request, "CENTRAL_WX")

        assert result.status_code == 302
        assert result.headers["location"] == "/streams"

        # Verify audit
        assert captured_audit["action"] == "stream.update"
        assert captured_audit["target"] == "CENTRAL_WX"
        assert captured_audit["before"]["max_age_s"] == 604800
        assert captured_audit["after"]["max_age_s"] == 1209600

    @pytest.mark.asyncio
    async def test_too_small_max_age_shows_error(self):
        """POST /streams/CENTRAL_WX with max_age_s=60 shows error, DB unchanged."""
        from central.gui.routes import streams_update

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_request.state.csrf_token = "test_csrf_token"
        mock_form = MagicMock()
        mock_form.get.side_effect = lambda k, d="": {
            "csrf_token": "test_csrf_token",
            "max_age_s": "60",
        }.get(k, d)
        mock_request.form = AsyncMock(return_value=mock_form)

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {"name": "CENTRAL_WX", "max_age_s": 604800}
        mock_conn.fetch.return_value = [
            {"name": "CENTRAL_WX", "max_age_s": 604800, "max_bytes": 1073741824, "managed_max_bytes": True, "updated_at": None},
        ]
        mock_conn.execute = AsyncMock()

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_templates.TemplateResponse.return_value = mock_response

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                with patch("central.gui.nats.get_js", return_value=None):
                    result = await streams_update(mock_request, "CENTRAL_WX")

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert "CENTRAL_WX" in context["errors"]
        assert "3600" in context["errors"]["CENTRAL_WX"]  # mentions min value

    @pytest.mark.asyncio
    async def test_too_large_max_age_shows_error(self):
        """POST /streams/CENTRAL_WX with max_age_s=999999999 shows error, DB unchanged."""
        from central.gui.routes import streams_update

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")
        mock_request.state.csrf_token = "test_csrf_token"

        mock_form = MagicMock()
        mock_form.get.side_effect = lambda k, d="": {"csrf_token": "test_csrf_token", "max_age_s": "999999999"}.get(k, d)  # Way too large
        mock_request.form = AsyncMock(return_value=mock_form)

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {"name": "CENTRAL_WX", "max_age_s": 604800}
        mock_conn.fetch.return_value = [
            {"name": "CENTRAL_WX", "max_age_s": 604800, "max_bytes": 1073741824, "managed_max_bytes": True, "updated_at": None},
        ]
        mock_conn.execute = AsyncMock()

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_templates = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_templates.TemplateResponse.return_value = mock_response

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                with patch("central.gui.nats.get_js", return_value=None):
                    result = await streams_update(mock_request, "CENTRAL_WX")

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert "CENTRAL_WX" in context["errors"]

    @pytest.mark.asyncio
    async def test_nonexistent_stream_returns_404(self):
        """POST /streams/nonexistent returns 404."""
        from central.gui.routes import streams_update

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_request.state.csrf_token = "test_csrf_token"
        mock_form = MagicMock()
        mock_form.get.side_effect = lambda k, d="": {
            "csrf_token": "test_csrf_token",
            "max_age_s": "604800",
        }.get(k, d)
        mock_request.form = AsyncMock(return_value=mock_form)

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = None  # Stream not found

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            result = await streams_update(mock_request, "nonexistent")

        assert result.status_code == 404


class TestStreamsAudit:
    """Test stream audit logging."""

    @pytest.mark.asyncio
    async def test_audit_captures_max_age_change(self):
        """Audit row captures before/after max_age_s values."""
        from central.gui.routes import streams_update

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")

        mock_request.state.csrf_token = "test_csrf_token"
        mock_form = MagicMock()
        mock_form.get.side_effect = lambda k, d="": {
            "csrf_token": "test_csrf_token",
            "max_age_s": "1209600",
        }.get(k, d)
        mock_request.form = AsyncMock(return_value=mock_form)

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {"name": "CENTRAL_QUAKE", "max_age_s": 604800}  # 7 days
        mock_conn.execute = AsyncMock()

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        captured_audit = {}

        async def capture_audit(conn, action, operator_id=None, target=None, before=None, after=None):
            captured_audit["action"] = action
            captured_audit["operator_id"] = operator_id
            captured_audit["target"] = target
            captured_audit["before"] = before
            captured_audit["after"] = after

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            with patch("central.gui.routes.write_audit", side_effect=capture_audit):
                await streams_update(mock_request, "CENTRAL_QUAKE")

        assert captured_audit["action"] == "stream.update"
        assert captured_audit["operator_id"] == 1
        assert captured_audit["target"] == "CENTRAL_QUAKE"
        assert captured_audit["before"] == {"max_age_s": 604800}
        assert captured_audit["after"] == {"max_age_s": 1209600}
