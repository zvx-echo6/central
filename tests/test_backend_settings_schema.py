"""Tests for per-backend settings schemas (PR L.5).

Each GeocoderBackend declares a Pydantic settings_schema (extra='forbid').
build_enrichers validates backend_settings against it BEFORE constructing, so
a config/settings mismatch is a clean ValidationError, not a TypeError. The
supervisor's hot-reload keeps the previous backend running on ValidationError;
the GUI POST re-renders with field errors and does not write the DB row.

Regression guard for the 2026-05-20 incident: switching backend_class to
NoOpBackend while backend_settings still held {"base_url": ...} crashed
_rebuild_enrichers with `TypeError: NoOpBackend() takes no arguments`.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel, ValidationError

from central.config_models import EnrichmentConfig
from central.enrichment.cache import EnrichmentCache
from central.supervisor import _BACKEND_REGISTRY, build_enrichers


# --- schemas exist + extra='forbid' -----------------------------------------

def test_every_backend_declares_a_settings_schema():
    for name, cls in _BACKEND_REGISTRY.items():
        schema = getattr(cls, "settings_schema", None)
        assert schema is not None, f"{name} has no settings_schema"
        assert isinstance(schema, type) and issubclass(schema, BaseModel), name


def test_every_settings_schema_forbids_extra():
    for name, cls in _BACKEND_REGISTRY.items():
        with pytest.raises(ValidationError):
            cls.settings_schema.model_validate({"definitely_not_a_field": 1})


def test_navi_schema_accepts_valid_kwargs_and_preserves_defaults():
    from central.enrichment.backends.navi import NaviBackendSettings

    m = NaviBackendSettings()
    assert m.base_url == "http://localhost:8440"
    assert m.timeout_s == 10.0  # preserved from __init__, NOT 5.0
    assert m.headers is None
    assert m.warmup is True
    m2 = NaviBackendSettings(base_url="http://navi:8440", timeout_s=3.0, warmup=False)
    assert m2.base_url == "http://navi:8440" and m2.timeout_s == 3.0


def test_noop_schema_has_no_fields():
    from central.enrichment.backends.no_op import NoOpBackendSettings

    assert list(NoOpBackendSettings.model_fields) == []


# --- build_enrichers validation ----------------------------------------------

def _cache(tmp_path) -> EnrichmentCache:
    return EnrichmentCache(tmp_path / "c.db")


def test_build_enrichers_raises_validation_error_not_typeerror(tmp_path):
    """The exact 2026-05-20 bug: NoOpBackend + stale base_url. Must be a clean
    ValidationError, never a TypeError from the constructor."""
    cfg = EnrichmentConfig(backend_class="NoOpBackend", backend_settings={"base_url": "http://x"})
    with pytest.raises(ValidationError):
        build_enrichers(cfg, _cache(tmp_path))


def test_build_enrichers_navi_valid_settings(tmp_path):
    from central.enrichment.backends.navi import NaviBackend

    cfg = EnrichmentConfig(
        backend_class="NaviBackend",
        backend_settings={"base_url": "http://navi:8440", "warmup": False},
    )
    enrichers = build_enrichers(cfg, _cache(tmp_path))
    assert isinstance(enrichers[0]._backend, NaviBackend)


def test_build_enrichers_navi_unknown_setting_rejected(tmp_path):
    cfg = EnrichmentConfig(
        backend_class="NaviBackend",
        backend_settings={"base_url": "http://navi:8440", "bogus": 1},
    )
    with pytest.raises(ValidationError):
        build_enrichers(cfg, _cache(tmp_path))


# --- supervisor hot-reload keeps previous backend on ValidationError ---------

def _supervisor():
    from central import supervisor as sup_mod

    config_source = MagicMock()
    config_source.get_enrichment_config = AsyncMock(return_value=EnrichmentConfig())
    return sup_mod.Supervisor(
        config_source=config_source,
        config_store=MagicMock(),
        nats_url="nats://localhost:4222",
    )


@pytest.mark.asyncio
async def test_handle_enrichment_change_keeps_previous_on_validation_error():
    """Navi active, then a bad NOTIFY (NoOp + leftover base_url) arrives. The
    supervisor must keep NaviBackend running and not crash."""
    from central.enrichment.backends.navi import NaviBackend
    from central.enrichment.backends.no_op import NoOpBackend

    with patch("central.supervisor.EnrichmentCache") as cache_cls:
        cache_cls.return_value = MagicMock(invalidate=AsyncMock(return_value=0))
        sup = _supervisor()
        # Establish a valid NaviBackend active state.
        sup._rebuild_enrichers(EnrichmentConfig(
            backend_class="NaviBackend",
            backend_settings={"base_url": "http://navi:8440", "warmup": False},
        ))
        assert isinstance(sup._enrichers[0]._backend, NaviBackend)
        active_before = sup._active_enrichment_config

        # Bad config arrives via NOTIFY: NoOp + stale base_url -> ValidationError.
        sup._config_source.get_enrichment_config = AsyncMock(
            return_value=EnrichmentConfig(
                backend_class="NoOpBackend",
                backend_settings={"base_url": "http://navi:8440"},
            )
        )
        await sup._handle_enrichment_change()  # must NOT raise

        # Previous backend still active; config unchanged; cache NOT invalidated.
        assert isinstance(sup._enrichers[0]._backend, NaviBackend)
        assert sup._active_enrichment_config is active_before
        cache_cls.return_value.invalidate.assert_not_awaited()


@pytest.mark.asyncio
async def test_handle_enrichment_change_applies_valid_noop_with_empty_settings():
    """Switching to NoOp with empty backend_settings (the correct way) applies
    cleanly and invalidates the cache."""
    from central.enrichment.backends.no_op import NoOpBackend

    with patch("central.supervisor.EnrichmentCache") as cache_cls:
        cache_cls.return_value = MagicMock(invalidate=AsyncMock(return_value=3))
        sup = _supervisor()
        sup._rebuild_enrichers(EnrichmentConfig(
            backend_class="NaviBackend",
            backend_settings={"base_url": "http://navi:8440", "warmup": False},
        ))
        sup._config_source.get_enrichment_config = AsyncMock(
            return_value=EnrichmentConfig(backend_class="NoOpBackend", backend_settings={})
        )
        await sup._handle_enrichment_change()
        assert isinstance(sup._enrichers[0]._backend, NoOpBackend)
        cache_cls.return_value.invalidate.assert_awaited()


# --- GUI POST validates backend_settings + does not write on error ----------

def _mock_pool(conn):
    pool = MagicMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=None)
    pool.acquire = MagicMock(return_value=cm)
    return pool


@pytest.mark.asyncio
async def test_gui_post_rejects_bad_backend_settings_without_writing():
    """POST with a bad backend setting (timeout_s non-numeric) re-renders with a
    field error keyed bs_timeout_s and does NOT execute the DB upsert."""
    from central.gui.routes import enrichment_update

    request = MagicMock()
    request.state.operator = MagicMock(username="op")
    request.state.csrf_token = "tok"

    form = {
        "csrf_token": "tok",
        "enricher_class": "GeocoderEnricher",
        "backend_class": "NaviBackend",
        "cache_ttl_s": "86400",
        "bs_base_url": "http://navi:8440",
        "bs_timeout_s": "not-a-number",
    }
    request.form = AsyncMock(return_value=form)

    conn = MagicMock()
    conn.execute = AsyncMock()
    conn.fetchrow = AsyncMock(return_value={})
    templates = MagicMock()
    templates.TemplateResponse.return_value = MagicMock()

    with patch("central.gui.routes._get_templates", return_value=templates), \
         patch("central.gui.routes.get_pool", return_value=_mock_pool(conn)):
        await enrichment_update(request)

    conn.execute.assert_not_awaited()  # no DB write on validation failure
    ctx = templates.TemplateResponse.call_args.kwargs["context"]
    assert "bs_timeout_s" in ctx["errors"]
    assert ctx["backend_class"] == "NaviBackend"  # re-rendered against SUBMITTED backend


@pytest.mark.asyncio
async def test_gui_post_writes_on_valid_settings():
    from central.gui.routes import enrichment_update

    request = MagicMock()
    request.state.operator = MagicMock(username="op")
    request.state.csrf_token = "tok"
    form = {
        "csrf_token": "tok",
        "enricher_class": "GeocoderEnricher",
        "backend_class": "NaviBackend",
        "cache_ttl_s": "86400",
        "bs_base_url": "http://navi:8440",
        "bs_timeout_s": "10",
    }
    request.form = AsyncMock(return_value=form)
    conn = MagicMock()
    conn.execute = AsyncMock()
    templates = MagicMock()

    with patch("central.gui.routes._get_templates", return_value=templates), \
         patch("central.gui.routes.get_pool", return_value=_mock_pool(conn)):
        resp = await enrichment_update(request)

    conn.execute.assert_awaited()  # DB upsert ran
    # the backend_settings written are the validated/dumped dict
    args = conn.execute.call_args.args
    assert "INSERT INTO config.enrichment" in args[0]
    assert resp.status_code == 302
