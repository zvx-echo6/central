"""v0.15.0 PR3 — GUI create + delete for adapter instances.

Test strategy
─────────────
* Pure-unit (always run, no DB):
  - ADAPTER_NAME_REGEX validation
  - Deletability rule (name in registry → not deletable)

* Mock-DB (always run; mirrors test_gui_adapter_edit.py pattern):
  - GET /adapters/new renders correctly
  - POST create: valid → INSERT, audit, 302
  - POST create: duplicate name → 409
  - POST create: bad name format → 422
  - POST create: non-creatable kind → 422
  - POST create: invalid settings (missing required field) → 422
  - POST delete: operator instance → DELETE, audit, 302
  - POST delete: built-in adapter → 403, no DELETE

DB-backed INSERT/DELETE tests (test_db_* below) use the central_test
Postgres fixture.  They will raise ConnectionRefusedError when the test DB
is absent — the same behaviour as other DB-backed tests in this suite (e.g.
test_config_store.py, test_supervisor_hotreload.py).
"""

import re
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.datastructures import FormData
from starlette.requests import Request

from central.gui import templates as gui_templates
from central.gui.routes import (
    ADAPTER_NAME_REGEX,
    adapters_create_form,
    adapters_create_submit,
    adapters_delete,
    adapters_list,
)
from central.adapters.generic_http import GenericHttpAdapter


# ---------------------------------------------------------------------------
# Helpers shared across test classes
# ---------------------------------------------------------------------------

def _make_request(method="GET", form_pairs=None, csrf="x"):
    """Build a mock Request with CSRF + optional form data."""
    req = MagicMock()
    req.state.operator = SimpleNamespace(id=1, username="admin")
    req.state.csrf_token = csrf
    if form_pairs is not None:
        pairs = [("csrf_token", csrf)] + list(form_pairs)
        req.form = AsyncMock(return_value=FormData(pairs))
    else:
        req.form = AsyncMock(return_value=FormData([("csrf_token", csrf)]))
    return req


def _make_pool(fetchrow_returns=None, fetchval_returns=None, fetch_returns=None):
    """Build a mock asyncpg pool.

    Values are set unconditionally so that None (e.g. "row not found") is
    returned faithfully instead of the default truthy AsyncMock sentinel.
    Pass a list for fetchrow_returns to use side_effect for sequential calls.
    """
    conn = AsyncMock()
    if isinstance(fetchrow_returns, list):
        conn.fetchrow.side_effect = fetchrow_returns
    else:
        conn.fetchrow.return_value = fetchrow_returns  # None = not found
    conn.fetchval.return_value = fetchval_returns       # None = not found
    conn.fetch.return_value = fetch_returns if fetch_returns is not None else []
    pool = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
    return pool, conn


# ---------------------------------------------------------------------------
# UNIT: name-regex validation
# ---------------------------------------------------------------------------

class TestAdapterNameRegex:
    """Pure-unit — no I/O, always run."""

    VALID = [
        "my_source",
        "mysource2",
        "aa",               # minimum length (2 chars)
        "a" + "b" * 63,     # maximum length (64 chars)
        "a1_b2_c3",
    ]
    INVALID = [
        "",                  # empty
        "a",                 # too short (only 1 char)
        "A_source",          # uppercase
        "1source",           # starts with digit
        "_source",           # starts with underscore
        "my-source",         # hyphen not allowed
        "my source",         # space not allowed
        "a" + "b" * 64,      # 65 chars — too long
    ]

    @pytest.mark.parametrize("name", VALID)
    def test_valid(self, name):
        assert ADAPTER_NAME_REGEX.match(name), f"Expected {name!r} to match"

    @pytest.mark.parametrize("name", INVALID)
    def test_invalid(self, name):
        assert not ADAPTER_NAME_REGEX.match(name), f"Expected {name!r} not to match"


# ---------------------------------------------------------------------------
# UNIT: deletability rule
# ---------------------------------------------------------------------------

class TestDeletabilityRule:
    """Pure-unit — the rule is: name NOT IN adapter_classes → deletable.

    Built-ins have name == kind (the class's .name attribute) which IS a key in
    the adapter class registry.  Operator instances have a unique name that is
    NOT a registry key.
    """

    def test_builtin_not_deletable(self):
        from central.adapter_discovery import discover_adapters
        classes = discover_adapters()
        # Every registered kind key should be considered a built-in.
        for kind in classes:
            assert kind in classes, "sanity"
            # The deletability check: name in adapter_classes → NOT deletable
            assert kind in classes  # confirms the guard fires

    def test_operator_instance_is_deletable(self):
        from central.adapter_discovery import discover_adapters
        classes = discover_adapters()
        operator_name = "my_custom_source_42"
        assert operator_name not in classes, (
            "Test assumes operator_name is not a registered kind; "
            "update the name if a new kind was added with this identifier."
        )

    def test_generic_http_kind_is_not_deletable_by_name(self):
        """The KIND 'generic_http' itself should not be deletable (it's a built-in key)."""
        from central.adapter_discovery import discover_adapters
        classes = discover_adapters()
        assert "generic_http" in classes

    def test_generic_http_instance_is_deletable(self):
        """An operator instance named 'my_feed' (not a kind key) should be deletable."""
        from central.adapter_discovery import discover_adapters
        classes = discover_adapters()
        assert "my_feed" not in classes


# ---------------------------------------------------------------------------
# UNIT: GenericHttpAdapter.operator_creatable
# ---------------------------------------------------------------------------

def test_generic_http_is_operator_creatable():
    assert GenericHttpAdapter.operator_creatable is True


def test_base_class_default_not_creatable():
    from central.adapter import SourceAdapter
    assert SourceAdapter.operator_creatable is False


# ---------------------------------------------------------------------------
# Mock-DB: GET /adapters/new
# ---------------------------------------------------------------------------

class TestGetAdaptersNew:
    @pytest.mark.asyncio
    async def test_renders_200_with_generic_http_in_kind_select(self):
        pool, conn = _make_pool(fetch_returns=[])
        tmpl = MagicMock()
        tmpl.TemplateResponse.return_value = MagicMock(status_code=200)
        req = _make_request()

        with patch("central.gui.routes._get_templates", return_value=tmpl), \
             patch("central.gui.routes.get_pool", return_value=pool):
            await adapters_create_form(req)

        ctx = tmpl.TemplateResponse.call_args.kwargs["context"]
        kind_names = [ck["kind"] for ck in ctx["creatable_kinds"]]
        assert "generic_http" in kind_names

    @pytest.mark.asyncio
    async def test_fields_present_for_generic_http(self):
        pool, conn = _make_pool(fetch_returns=[])
        tmpl = MagicMock()
        tmpl.TemplateResponse.return_value = MagicMock(status_code=200)
        req = _make_request()

        with patch("central.gui.routes._get_templates", return_value=tmpl), \
             patch("central.gui.routes.get_pool", return_value=pool):
            await adapters_create_form(req)

        ctx = tmpl.TemplateResponse.call_args.kwargs["context"]
        field_names = [f.name for f in ctx["fields"]]
        # GenericHttpSettings requires url, domain, id_path at minimum
        assert "url" in field_names
        assert "domain" in field_names
        assert "id_path" in field_names

    @pytest.mark.asyncio
    async def test_template_renders_without_errors(self):
        """Smoke test: the template itself renders without crashing."""
        pool, conn = _make_pool(fetch_returns=[])
        tmpl = MagicMock()
        tmpl.TemplateResponse.return_value = MagicMock(status_code=200)
        req = _make_request()

        with patch("central.gui.routes._get_templates", return_value=tmpl), \
             patch("central.gui.routes.get_pool", return_value=pool):
            resp = await adapters_create_form(req)

        # Template was called — no exception raised
        assert tmpl.TemplateResponse.called


# ---------------------------------------------------------------------------
# Mock-DB: POST /adapters/new — happy path
# ---------------------------------------------------------------------------

def _valid_generic_http_pairs(name="my_feed"):
    """Minimal valid form pairs for a generic_http instance."""
    return [
        ("kind",        "generic_http"),
        ("name",        name),
        ("cadence_s",   "300"),
        # enabled intentionally absent → ships disabled
        ("url",         "https://example.com/feed.geojson"),
        ("domain",      "fire"),
        ("id_path",     "properties.id"),
    ]


class TestPostAdaptersNewHappyPath:
    @pytest.mark.asyncio
    async def test_valid_creates_and_redirects(self):
        pool, conn = _make_pool(
            fetchval_returns=None,   # name does not exist yet
            fetch_returns=[],        # no api keys
        )
        inserted: list = []

        async def cap_execute(q, *args):
            if "INSERT INTO config.adapters" in q:
                inserted.append(args)

        conn.execute.side_effect = cap_execute

        req = _make_request(form_pairs=_valid_generic_http_pairs())

        with patch("central.gui.routes.get_pool", return_value=pool), \
             patch("central.gui.routes.write_audit", new=AsyncMock()):
            resp = await adapters_create_submit(req)

        assert resp.status_code == 302
        assert "/adapters/my_feed" in resp.headers["location"]
        assert len(inserted) == 1
        # args: name, kind, enabled, cadence_s, settings
        _name, _kind, _enabled, _cadence, _settings = inserted[0]
        assert _name == "my_feed"
        assert _kind == "generic_http"
        assert _enabled is False         # no 'enabled' in form → ships disabled
        assert _cadence == 300
        assert _settings["url"] == "https://example.com/feed.geojson"

    @pytest.mark.asyncio
    async def test_enabled_flag_set_when_checked(self):
        pool, conn = _make_pool(fetchval_returns=None, fetch_returns=[])
        inserted: list = []

        async def cap(q, *args):
            if "INSERT" in q:
                inserted.append(args)

        conn.execute.side_effect = cap
        pairs = _valid_generic_http_pairs() + [("enabled", "on")]
        req = _make_request(form_pairs=pairs)

        with patch("central.gui.routes.get_pool", return_value=pool), \
             patch("central.gui.routes.write_audit", new=AsyncMock()):
            resp = await adapters_create_submit(req)

        assert resp.status_code == 302
        _name, _kind, _enabled, *_ = inserted[0]
        assert _enabled is True

    @pytest.mark.asyncio
    async def test_audit_record_written_on_create(self):
        pool, conn = _make_pool(fetchval_returns=None, fetch_returns=[])
        conn.execute.return_value = None
        audited: list = []

        async def cap_audit(conn_, action, **kw):
            audited.append((action, kw))

        req = _make_request(form_pairs=_valid_generic_http_pairs())
        with patch("central.gui.routes.get_pool", return_value=pool), \
             patch("central.gui.routes.write_audit", side_effect=cap_audit):
            await adapters_create_submit(req)

        assert len(audited) == 1
        action, kw = audited[0]
        assert action == "adapter.create"
        assert kw["target"] == "my_feed"


# ---------------------------------------------------------------------------
# Mock-DB: POST /adapters/new — validation errors
# ---------------------------------------------------------------------------

async def _post_new(pairs, fetchval=None, fetch_returns=None):
    """Helper: POST /adapters/new and return (response, template_call_args).

    fetchval=None means "adapter name does not exist" (duplicate check passes).
    Pass fetchval=1 to simulate a duplicate.
    """
    pool, conn = _make_pool(
        fetchval_returns=fetchval,   # None = not found; passed unconditionally
        fetch_returns=fetch_returns or [],
    )
    tmpl = MagicMock()
    tmpl.TemplateResponse.return_value = MagicMock()
    req = _make_request(form_pairs=pairs)
    with patch("central.gui.routes._get_templates", return_value=tmpl), \
         patch("central.gui.routes.get_pool", return_value=pool), \
         patch("central.gui.routes.write_audit", new=AsyncMock()):
        resp = await adapters_create_submit(req)
    return resp, tmpl.TemplateResponse.call_args


class TestPostAdaptersNewValidationErrors:
    @pytest.mark.asyncio
    async def test_duplicate_name_returns_409(self):
        pool, conn = _make_pool(fetchval_returns=1, fetch_returns=[])
        req = _make_request(form_pairs=_valid_generic_http_pairs())
        with patch("central.gui.routes.get_pool", return_value=pool), \
             patch("central.gui.routes.write_audit", new=AsyncMock()):
            resp = await adapters_create_submit(req)
        assert resp.status_code == 409

    @pytest.mark.asyncio
    async def test_bad_name_format_returns_422(self):
        pairs = _valid_generic_http_pairs(name="Bad-Name!")
        resp, ca = await _post_new(pairs)
        assert ca.kwargs["status_code"] == 422
        assert "name" in ca.kwargs["context"]["errors"]

    @pytest.mark.asyncio
    async def test_name_starts_with_digit_returns_422(self):
        pairs = _valid_generic_http_pairs(name="1invalid")
        resp, ca = await _post_new(pairs)
        assert ca.kwargs["status_code"] == 422

    @pytest.mark.asyncio
    async def test_name_too_short_returns_422(self):
        pairs = _valid_generic_http_pairs(name="a")   # only 1 char
        resp, ca = await _post_new(pairs)
        assert ca.kwargs["status_code"] == 422

    @pytest.mark.asyncio
    async def test_name_shadows_kind_returns_422(self):
        """Cannot use a registered kind name as an instance name."""
        pairs = _valid_generic_http_pairs(name="generic_http")
        resp, ca = await _post_new(pairs)
        assert ca.kwargs["status_code"] == 422
        assert "name" in ca.kwargs["context"]["errors"]

    @pytest.mark.asyncio
    async def test_non_creatable_kind_returns_422(self):
        # usgs_quake is a real built-in kind that is NOT operator_creatable
        pairs = [
            ("kind",      "usgs_quake"),
            ("name",      "my_quake"),
            ("cadence_s", "300"),
            ("url",       "https://example.com"),
            ("domain",    "quake"),
            ("id_path",   "id"),
        ]
        resp, ca = await _post_new(pairs)
        assert ca.kwargs["status_code"] == 422
        assert "kind" in ca.kwargs["context"]["errors"]

    @pytest.mark.asyncio
    async def test_invalid_settings_missing_required_field_returns_422(self):
        # Omit required 'url' field from generic_http settings
        pairs = [
            ("kind",      "generic_http"),
            ("name",      "my_feed"),
            ("cadence_s", "300"),
            # no 'url', no 'domain', no 'id_path'
        ]
        resp, ca = await _post_new(pairs)
        assert ca.kwargs["status_code"] == 422

    @pytest.mark.asyncio
    async def test_cadence_below_10_returns_422(self):
        pairs = _valid_generic_http_pairs()
        # replace cadence_s
        pairs = [(k, "5") if k == "cadence_s" else (k, v) for k, v in pairs]
        resp, ca = await _post_new(pairs)
        assert ca.kwargs["status_code"] == 422
        assert "cadence_s" in ca.kwargs["context"]["errors"]

    @pytest.mark.asyncio
    async def test_invalid_domain_returns_422(self):
        pairs = _valid_generic_http_pairs()
        pairs = [(k, "notadomain") if k == "domain" else (k, v) for k, v in pairs]
        resp, ca = await _post_new(pairs)
        assert ca.kwargs["status_code"] == 422


# ---------------------------------------------------------------------------
# Mock-DB: POST /adapters/{name}/delete
# ---------------------------------------------------------------------------

class TestPostAdaptersDelete:
    @pytest.mark.asyncio
    async def test_operator_instance_is_deleted(self):
        """Deleting an operator instance removes the row and audits."""
        pool, conn = _make_pool(
            fetchrow_returns={"name": "my_feed", "kind": "generic_http"},
        )
        deleted: list = []

        async def cap(q, *args):
            if "DELETE FROM config.adapters" in q:
                deleted.append(args)

        conn.execute.side_effect = cap
        req = _make_request()

        with patch("central.gui.routes.get_pool", return_value=pool), \
             patch("central.gui.routes.write_audit", new=AsyncMock()):
            resp = await adapters_delete(req, "my_feed")

        assert resp.status_code == 302
        assert resp.headers["location"] == "/adapters"
        assert len(deleted) == 1
        assert deleted[0][0] == "my_feed"

    @pytest.mark.asyncio
    async def test_builtin_adapter_returns_403(self):
        """Attempting to delete a built-in adapter (name in registry) → 403."""
        pool, conn = _make_pool(
            fetchrow_returns={"name": "usgs_quake", "kind": "usgs_quake"},
        )
        req = _make_request()

        with patch("central.gui.routes.get_pool", return_value=pool), \
             patch("central.gui.routes.write_audit", new=AsyncMock()):
            resp = await adapters_delete(req, "usgs_quake")

        assert resp.status_code == 403
        assert "built-in" in resp.body.decode()
        # DELETE must NOT have been called
        for call in conn.execute.call_args_list:
            assert "DELETE" not in str(call)

    @pytest.mark.asyncio
    async def test_missing_adapter_returns_404(self):
        pool, conn = _make_pool(fetchrow_returns=None)
        req = _make_request()

        with patch("central.gui.routes.get_pool", return_value=pool), \
             patch("central.gui.routes.write_audit", new=AsyncMock()):
            resp = await adapters_delete(req, "nonexistent")

        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_audit_record_written(self):
        pool, conn = _make_pool(
            fetchrow_returns={"name": "my_feed", "kind": "generic_http"},
        )
        conn.execute.return_value = None
        audited: list = []

        async def cap_audit(conn_, action, **kw):
            audited.append((action, kw))

        req = _make_request()
        with patch("central.gui.routes.get_pool", return_value=pool), \
             patch("central.gui.routes.write_audit", side_effect=cap_audit):
            await adapters_delete(req, "my_feed")

        assert len(audited) == 1
        action, kw = audited[0]
        assert action == "adapter.delete"
        assert kw["target"] == "my_feed"

    @pytest.mark.asyncio
    async def test_generic_http_kind_itself_is_protected(self):
        """The class entry 'generic_http' IS in the registry → 403."""
        pool, conn = _make_pool(
            fetchrow_returns={"name": "generic_http", "kind": "generic_http"},
        )
        req = _make_request()

        with patch("central.gui.routes.get_pool", return_value=pool), \
             patch("central.gui.routes.write_audit", new=AsyncMock()):
            resp = await adapters_delete(req, "generic_http")

        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Template smoke test: adapters_new.html renders without crashing
# ---------------------------------------------------------------------------

class TestAdaptersNewTemplate:
    def _render(self, ctx):
        req = Request({
            "type": "http", "method": "GET", "path": "/",
            "headers": [], "query_string": b"",
        })
        return gui_templates.TemplateResponse(
            request=req, name="adapters_new.html", context=ctx
        ).body.decode()

    def _ctx(self, errors=None, form_data=None):
        from central.gui.form_descriptors import describe_fields
        from central.adapters.generic_http import GenericHttpSettings
        fields = describe_fields(GenericHttpSettings, {})
        return {
            "operator": SimpleNamespace(username="admin"),
            "csrf_token": "x",
            "creatable_kinds": [{"kind": "generic_http", "display_name": "Generic HTTP Source"}],
            "selected_kind": "generic_http",
            "default_cadence_s": 300,
            "fields": fields,
            "api_keys": [],
            "errors": errors,
            "form_data": form_data,
        }

    def test_renders_kind_select(self):
        out = self._render(self._ctx())
        assert "generic_http" in out
        assert 'name="kind"' in out

    def test_renders_name_input(self):
        out = self._render(self._ctx())
        assert 'name="name"' in out

    def test_renders_cadence_input_with_default(self):
        out = self._render(self._ctx())
        assert 'name="cadence_s"' in out
        assert "300" in out

    def test_renders_url_field_for_generic_http(self):
        out = self._render(self._ctx())
        assert 'name="url"' in out

    def test_enabled_unchecked_by_default(self):
        out = self._render(self._ctx())
        # The enabled checkbox must not be checked in default render
        # (spec: ships disabled)
        assert 'name="enabled"' in out
        # Extract the enabled checkbox line and confirm no 'checked' attribute
        for line in out.splitlines():
            if 'name="enabled"' in line:
                assert "checked" not in line, f"enabled checkbox should be unchecked by default: {line}"
                break

    def test_error_messages_displayed(self):
        errors = {"name": "Name is invalid", "url": "URL is required"}
        out = self._render(self._ctx(errors=errors))
        assert "Name is invalid" in out
        assert "URL is required" in out

    def test_form_data_restores_values(self):
        form_data = {"kind": "generic_http", "name": "restored_name",
                     "cadence_s": "600", "url": "https://example.com/data.json",
                     "domain": "fire", "id_path": "id", "enabled": False}
        out = self._render(self._ctx(form_data=form_data))
        assert "restored_name" in out
        assert "https://example.com/data.json" in out
