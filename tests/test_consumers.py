"""Tests for consumers admin routes (GET /consumers, POST /consumers/{s}/{c}/delete)."""

import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Set required env vars before importing central modules
os.environ.setdefault("CENTRAL_DB_DSN", "postgresql://test:test@localhost/test")
os.environ.setdefault("CENTRAL_CSRF_SECRET", "testsecret12345678901234567890ab")
os.environ.setdefault("CENTRAL_NATS_URL", "nats://localhost:4222")


def _make_consumer_info(name: str, num_pending: int = 0, num_ack_pending: int = 0,
                        num_redelivered: int = 0, num_waiting: int = 0):
    ci = MagicMock()
    ci.name = name
    ci.num_pending = num_pending
    ci.num_ack_pending = num_ack_pending
    ci.num_redelivered = num_redelivered
    ci.num_waiting = num_waiting
    ci.created = datetime(2026, 5, 17, 12, 0, 0, tzinfo=timezone.utc)
    return ci


def _make_js_with_consumers(consumers_by_stream: dict):
    """Build a mock JetStreamContext whose consumers_info is a coroutine returning a list."""
    mock_js = MagicMock()
    mock_js.consumers_info = AsyncMock(
        side_effect=lambda stream, **kw: consumers_by_stream.get(stream, [])
    )
    mock_js.consumer_info = AsyncMock()
    mock_js.delete_consumer = AsyncMock()
    return mock_js


class TestConsumersListNatsUnavailable:
    """GET /consumers when NATS is down shows per-stream error."""

    @pytest.mark.asyncio
    async def test_nats_unavailable_shows_error_per_stream(self):
        from central.gui.routes import consumers_list

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")
        mock_request.state.csrf_token = "test_csrf"

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.nats.get_js", return_value=None):
                await consumers_list(mock_request)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        streams = context["streams"]
        # All streams should show the NATS unavailable error
        assert all(s["error"] == "NATS unavailable" for s in streams)
        # And no consumers listed
        assert all(s["consumers"] == [] for s in streams)


class TestConsumersListWithConsumers:
    """GET /consumers with live NATS returns consumers per stream."""

    @pytest.mark.asyncio
    async def test_consumers_listed_with_protected_flag(self):
        from central.gui.routes import consumers_list

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")
        mock_request.state.csrf_token = "test_csrf"

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock()

        consumers_by_stream = {
            "CENTRAL_WX": [
                _make_consumer_info("archive-CENTRAL_WX", num_pending=5, num_waiting=1),
                _make_consumer_info("meshai-wx", num_pending=1000, num_waiting=0),
            ],
        }
        mock_js = _make_js_with_consumers(consumers_by_stream)

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.nats.get_js", return_value=mock_js):
                await consumers_list(mock_request)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        streams = context["streams"]

        wx = next(s for s in streams if s["stream"] == "CENTRAL_WX")
        assert wx["error"] is None
        assert len(wx["consumers"]) == 2

        archive_c = next(c for c in wx["consumers"] if c["name"] == "archive-CENTRAL_WX")
        assert archive_c["protected"] is True
        assert archive_c["num_pending"] == 5

        meshai_c = next(c for c in wx["consumers"] if c["name"] == "meshai-wx")
        assert meshai_c["protected"] is False
        assert meshai_c["num_pending"] == 1000
        assert meshai_c["num_waiting"] == 0

        # Regression guard: consumer names must appear in the template context so
        # they are rendered into the HTML body (guards against the coroutine/iterator
        # bug where consumers_info was consumed as an async-iterable instead of awaited).
        consumer_names_in_context = {c["name"] for c in wx["consumers"]}
        assert "archive-CENTRAL_WX" in consumer_names_in_context
        assert "meshai-wx" in consumer_names_in_context

    @pytest.mark.asyncio
    async def test_stream_with_no_consumers_shows_empty(self):
        from central.gui.routes import consumers_list

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")
        mock_request.state.csrf_token = "test_csrf"

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock()

        mock_js = _make_js_with_consumers({})  # No consumers on any stream

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.nats.get_js", return_value=mock_js):
                await consumers_list(mock_request)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        streams = context["streams"]
        assert all(s["consumers"] == [] for s in streams)
        assert all(s["error"] is None for s in streams)

    @pytest.mark.asyncio
    async def test_one_stream_error_does_not_break_page(self):
        from central.gui.routes import consumers_list

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="testop")
        mock_request.state.csrf_token = "test_csrf"

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock()

        mock_js = MagicMock()

        async def consumers_info_raising(stream_name):
            if stream_name == "CENTRAL_FIRE":
                raise RuntimeError("stream not found")
            # other streams: empty list (coroutine returning a list, not an async generator)
            return []

        mock_js.consumers_info = consumers_info_raising

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.nats.get_js", return_value=mock_js):
                await consumers_list(mock_request)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        streams = context["streams"]

        fire = next(s for s in streams if s["stream"] == "CENTRAL_FIRE")
        assert "unavailable" in fire["error"]
        assert fire["consumers"] == []


class TestConsumersListHtmlRender:
    """Render consumers_list.html through the real Jinja2 environment.

    Stronger than the context-dict checks above: these prove the values
    actually reach the rendered HTML body. Guards two regressions:
      - the consumer NAME must appear in the rendered HTML (proves the
        ``await js.consumers_info(...)`` list reaches the template, not the
        coroutine/async-iterator bug)
      - Optional[int] count fields that are None must not render the literal
        string ``None`` (they are guarded to an em dash).
    """

    PROTECTED_LABEL = '<span class="muted" style="font-size: 0.85em;">central-owned</span>'

    def _render(self, streams):
        from central.gui import templates as templates_mod
        template = templates_mod.env.get_template("consumers_list.html")
        return template.render(
            operator=MagicMock(username="testop"),
            csrf_token="test_csrf",
            streams=streams,
        )

    def test_consumer_name_appears_in_html(self):
        streams = [
            {
                "stream": "CENTRAL_WX",
                "error": None,
                "consumers": [
                    {
                        "name": "meshai-wx",
                        "num_pending": 1000,
                        "num_ack_pending": 0,
                        "num_redelivered": 0,
                        "num_waiting": 0,
                        "created": datetime(2026, 5, 17, 12, 0, 0, tzinfo=timezone.utc),
                        "protected": False,
                    },
                ],
            },
        ]
        html = self._render(streams)
        assert "meshai-wx" in html
        # Non-protected consumer renders a delete form
        assert "/consumers/CENTRAL_WX/meshai-wx/delete" in html
        # ...and not the central-owned label span (which only the legend prose
        # mentions, so we match the exact span markup, not the bare phrase)
        assert self.PROTECTED_LABEL not in html

    def test_protected_consumer_renders_label_not_button(self):
        streams = [
            {
                "stream": "CENTRAL_WX",
                "error": None,
                "consumers": [
                    {
                        "name": "archive-CENTRAL_WX",
                        "num_pending": 5,
                        "num_ack_pending": 0,
                        "num_redelivered": 0,
                        "num_waiting": 1,
                        "created": datetime(2026, 5, 17, 12, 0, 0, tzinfo=timezone.utc),
                        "protected": True,
                    },
                ],
            },
        ]
        html = self._render(streams)
        assert "archive-CENTRAL_WX" in html
        assert self.PROTECTED_LABEL in html
        # No delete form for the protected consumer
        assert "/consumers/CENTRAL_WX/archive-CENTRAL_WX/delete" not in html

    def test_none_counts_render_dash_not_literal_none(self):
        streams = [
            {
                "stream": "CENTRAL_WX",
                "error": None,
                "consumers": [
                    {
                        "name": "meshai-wx",
                        "num_pending": None,
                        "num_ack_pending": None,
                        "num_redelivered": None,
                        "num_waiting": None,
                        "created": None,
                        "protected": False,
                    },
                ],
            },
        ]
        html = self._render(streams)
        assert "meshai-wx" in html
        # The literal "None" must never leak into a rendered table cell
        assert ">None<" not in html
        # The guarded fallback em dash is rendered instead
        assert "—" in html


class TestConsumersDeleteArchiveGuard:
    """POST /consumers/{stream}/archive-*/delete must be refused."""

    @pytest.mark.asyncio
    async def test_archive_consumer_refused_redirects(self):
        from central.gui.routes import consumers_delete

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1)
        mock_request.state.csrf_token = "tok"
        form_data = MagicMock()
        form_data.get.side_effect = lambda k, d="": {"csrf_token": "tok"}.get(k, d)
        mock_request.form = AsyncMock(return_value=form_data)

        with patch("central.gui.nats.get_js", return_value=MagicMock()):
            result = await consumers_delete(mock_request, "CENTRAL_WX", "archive-CENTRAL_WX")

        assert result.status_code == 302
        assert result.headers["location"] == "/consumers"


class TestConsumersDeleteSuccess:
    """POST /consumers/{stream}/{consumer}/delete happy path."""

    @pytest.mark.asyncio
    async def test_delete_non_protected_consumer_audits_and_redirects(self):
        from central.gui.routes import consumers_delete

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1)
        mock_request.state.csrf_token = "tok"
        form_data = MagicMock()
        form_data.get.side_effect = lambda k, d="": {"csrf_token": "tok"}.get(k, d)
        mock_request.form = AsyncMock(return_value=form_data)

        before_ci = _make_consumer_info("meshai-wx", num_pending=500)
        mock_js = MagicMock()
        mock_js.consumer_info = AsyncMock(return_value=before_ci)
        mock_js.delete_consumer = AsyncMock()

        mock_conn = AsyncMock()
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

        with patch("central.gui.nats.get_js", return_value=mock_js):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                with patch("central.gui.routes.write_audit", side_effect=capture_audit):
                    result = await consumers_delete(mock_request, "CENTRAL_WX", "meshai-wx")

        assert result.status_code == 302
        assert result.headers["location"] == "/consumers"

        mock_js.delete_consumer.assert_awaited_once_with("CENTRAL_WX", "meshai-wx")

        assert captured_audit["action"] == "consumer.delete"
        assert captured_audit["operator_id"] == 1
        assert captured_audit["target"] == "CENTRAL_WX/meshai-wx"
        assert captured_audit["before"]["name"] == "meshai-wx"
        assert captured_audit["after"] is None


class TestConsumersDeleteCsrfGuard:
    """POST /consumers/{stream}/{consumer}/delete CSRF mismatch raises."""

    @pytest.mark.asyncio
    async def test_csrf_mismatch_raises(self):
        from central.gui.routes import consumers_delete
        from central.gui.auth import CsrfValidationError

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1)
        mock_request.state.csrf_token = "real_token"
        form_data = MagicMock()
        form_data.get.side_effect = lambda k, d="": {"csrf_token": "wrong_token"}.get(k, d)
        mock_request.form = AsyncMock(return_value=form_data)

        with pytest.raises(CsrfValidationError):
            await consumers_delete(mock_request, "CENTRAL_WX", "meshai-wx")


class TestConsumersDeleteNatsUnavailable:
    """POST /consumers/{stream}/{consumer}/delete when NATS is down redirects."""

    @pytest.mark.asyncio
    async def test_nats_unavailable_redirects(self):
        from central.gui.routes import consumers_delete

        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1)
        mock_request.state.csrf_token = "tok"
        form_data = MagicMock()
        form_data.get.side_effect = lambda k, d="": {"csrf_token": "tok"}.get(k, d)
        mock_request.form = AsyncMock(return_value=form_data)

        with patch("central.gui.nats.get_js", return_value=None):
            result = await consumers_delete(mock_request, "CENTRAL_WX", "meshai-wx")

        assert result.status_code == 302
        assert result.headers["location"] == "/consumers"
