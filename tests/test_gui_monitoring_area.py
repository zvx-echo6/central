"""v0.14.0 monitoring-areas GUI routes: list / create / update / delete.

Server-rendered forms (matching the rest of the GUI), so these call the route
handlers directly with a mock pool + request and assert status + side effects.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from asyncpg.exceptions import UniqueViolationError
from fastapi.responses import RedirectResponse

from central.gui.routes import (
    monitoring_area_create,
    monitoring_area_delete,
    monitoring_area_list,
    monitoring_area_update,
)

_AREA_ROW = {"id": 1, "name": "treasure_valley",
             "north": 44.0, "south": 43.0, "east": -115.5, "west": -116.5}


class _Tmpl:
    """Stand-in for Jinja templates -- echoes status + context for assertions."""
    def TemplateResponse(self, **kw):
        return SimpleNamespace(
            status_code=kw.get("status_code", 200), context=kw["context"])


def _conn(*, fetch=None, fetchrow=None, execute_error=None):
    c = MagicMock()
    c.fetch = AsyncMock(return_value=fetch if fetch is not None else [])
    c.fetchrow = AsyncMock(return_value=fetchrow)
    c.execute = AsyncMock(side_effect=execute_error)
    return c


def _pool(conn):
    pool = MagicMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=False)
    pool.acquire = MagicMock(return_value=cm)
    return pool


def _req(form=None):
    r = MagicMock()
    r.state.csrf_token = "tok"
    r.state.operator = SimpleNamespace(id=1, username="admin")

    async def _form():
        return form or {}
    r.form = _form
    return r


def _form(**over):
    base = {"csrf_token": "tok", "name": "magic_valley",
            "north": "43.0", "south": "42.3", "east": "-113.4", "west": "-114.9"}
    base.update(over)
    return base


def _patches(conn):
    return (
        patch("central.gui.routes.get_pool", return_value=_pool(conn)),
        patch("central.gui.routes._get_templates", return_value=_Tmpl()),
        patch("central.gui.routes.write_audit", new=AsyncMock()),
    )


@pytest.mark.asyncio
class TestList:
    async def test_renders_areas(self):
        conn = _conn(fetch=[_AREA_ROW], fetchrow=None)
        p1, p2, p3 = _patches(conn)
        with p1, p2, p3:
            res = await monitoring_area_list(_req())
        assert res.status_code == 200
        assert res.context["areas"] == [_AREA_ROW]


@pytest.mark.asyncio
class TestCreate:
    async def test_valid_redirects_and_inserts(self):
        conn = _conn()
        p1, p2, p3 = _patches(conn)
        with p1, p2, p3:
            res = await monitoring_area_create(_req(_form()))
        assert isinstance(res, RedirectResponse) and res.status_code == 302
        assert "INSERT INTO config.monitoring_areas" in conn.execute.call_args[0][0]

    async def test_invalid_name_rerenders_no_insert(self):
        conn = _conn()
        p1, p2, p3 = _patches(conn)
        with p1, p2, p3:
            res = await monitoring_area_create(_req(_form(name="")))
        assert res.status_code == 200 and res.context["error"]
        conn.execute.assert_not_called()

    async def test_inverted_bounds_rerenders(self):
        conn = _conn()
        p1, p2, p3 = _patches(conn)
        with p1, p2, p3:
            res = await monitoring_area_create(_req(_form(north="42.0", south="43.0")))
        assert res.status_code == 200 and res.context["error"]
        conn.execute.assert_not_called()

    async def test_duplicate_name_rerenders(self):
        conn = _conn(execute_error=UniqueViolationError("dup"))
        p1, p2, p3 = _patches(conn)
        with p1, p2, p3:
            res = await monitoring_area_create(_req(_form()))
        assert res.status_code == 200 and "already exists" in res.context["error"]


@pytest.mark.asyncio
class TestUpdate:
    async def test_valid_redirects_and_updates(self):
        conn = _conn(fetchrow=_AREA_ROW)
        p1, p2, p3 = _patches(conn)
        with p1, p2, p3:
            res = await monitoring_area_update(_req(_form()), 1)
        assert isinstance(res, RedirectResponse) and res.status_code == 302
        assert "UPDATE config.monitoring_areas" in conn.execute.call_args[0][0]

    async def test_missing_id_returns_404(self):
        conn = _conn(fetchrow=None)
        p1, p2, p3 = _patches(conn)
        with p1, p2, p3:
            res = await monitoring_area_update(_req(_form()), 999)
        assert res.status_code == 404


@pytest.mark.asyncio
class TestDelete:
    async def test_redirects_and_deletes(self):
        conn = _conn(fetchrow=_AREA_ROW)
        p1, p2, p3 = _patches(conn)
        with p1, p2, p3:
            res = await monitoring_area_delete(_req({"csrf_token": "tok"}), 1)
        assert isinstance(res, RedirectResponse) and res.status_code == 302
        assert "DELETE FROM config.monitoring_areas" in conn.execute.call_args[0][0]
