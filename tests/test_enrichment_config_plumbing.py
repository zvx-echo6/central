"""Tests for operator-settable EnrichmentConfig plumbing (PR K.5).

Covers: ConfigStore DB read/upsert, supervisor startup read + hot-reload
rebuild, cache invalidation on backend change (but not on TTL-only change),
EnrichmentCache.invalidate, the generic json widget for backend_settings, and
the /enrichment GUI render. No real DB / NATS — pool, config_source, and the
EnrichmentCache class are mocked.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from central.config_models import EnrichmentConfig
from central.enrichment.cache import EnrichmentCache
from central.enrichment.backends.navi import NaviBackend
from central.enrichment.backends.no_op import NoOpBackend
from central.gui.form_descriptors import describe_fields


# --- mock pool/conn helpers -------------------------------------------------

def _mock_pool(conn: MagicMock) -> MagicMock:
    pool = MagicMock()
    acquire_cm = MagicMock()
    acquire_cm.__aenter__ = AsyncMock(return_value=conn)
    acquire_cm.__aexit__ = AsyncMock(return_value=None)
    pool.acquire = MagicMock(return_value=acquire_cm)
    return pool


# --- ConfigStore --------------------------------------------------------------

@pytest.mark.asyncio
async def test_config_store_reads_enrichment_row():
    from central.config_store import ConfigStore

    conn = MagicMock()
    conn.fetchrow = AsyncMock(return_value={
        "enricher_class": "GeocoderEnricher",
        "backend_class": "NaviBackend",
        "backend_settings": {"base_url": "http://example.test:8440"},
        "cache_ttl_s": 3600,
    })
    store = ConfigStore(_mock_pool(conn))
    cfg = await store.get_enrichment_config()
    assert isinstance(cfg, EnrichmentConfig)
    assert cfg.backend_class == "NaviBackend"
    assert cfg.backend_settings == {"base_url": "http://example.test:8440"}
    assert cfg.cache_ttl_s == 3600


@pytest.mark.asyncio
async def test_config_store_falls_back_to_defaults_when_row_absent():
    from central.config_store import ConfigStore

    conn = MagicMock()
    conn.fetchrow = AsyncMock(return_value=None)
    store = ConfigStore(_mock_pool(conn))
    cfg = await store.get_enrichment_config()
    assert cfg == EnrichmentConfig()  # framework defaults
    assert cfg.backend_class == "NoOpBackend"


@pytest.mark.asyncio
async def test_config_store_upsert_passes_dict_settings():
    from central.config_store import ConfigStore

    conn = MagicMock()
    conn.execute = AsyncMock()
    store = ConfigStore(_mock_pool(conn))
    cfg = EnrichmentConfig(backend_class="NaviBackend", backend_settings={"base_url": "x"})
    await store.upsert_enrichment_config(cfg)
    args = conn.execute.call_args.args
    assert "INSERT INTO config.enrichment" in args[0]
    # backend_settings passed as a dict (pool codec encodes to jsonb), not a str.
    assert {"base_url": "x"} in args


# --- EnrichmentCache.invalidate ----------------------------------------------

@pytest.mark.asyncio
async def test_cache_invalidate_all(tmp_path):
    cache = EnrichmentCache(tmp_path / "c.db", ttl_s=3600)
    await cache.set("geocoder", 1.0, 2.0, {"name": "x"})
    await cache.set("geocoder", 3.0, 4.0, {"name": "y"})
    deleted = await cache.invalidate()
    assert deleted == 2
    assert await cache.get("geocoder", 1.0, 2.0) is None


@pytest.mark.asyncio
async def test_cache_invalidate_scoped_to_enricher(tmp_path):
    cache = EnrichmentCache(tmp_path / "c.db", ttl_s=3600)
    await cache.set("geocoder", 1.0, 2.0, {"name": "x"})
    await cache.set("other", 1.0, 2.0, {"name": "z"})
    deleted = await cache.invalidate("geocoder")
    assert deleted == 1
    assert await cache.get("geocoder", 1.0, 2.0) is None
    assert await cache.get("other", 1.0, 2.0) == {"name": "z"}


# --- Supervisor startup read + hot-reload ------------------------------------

def _supervisor_with(enrichment_cfg: EnrichmentConfig):
    """Build a Supervisor with mocked deps and a mocked EnrichmentCache class
    (so no real /var/lib cache file is touched)."""
    from central import supervisor as sup_mod

    config_source = MagicMock()
    config_source.get_enrichment_config = AsyncMock(return_value=enrichment_cfg)
    config_store = MagicMock()
    sup = sup_mod.Supervisor(
        config_source=config_source,
        config_store=config_store,
        nats_url="nats://localhost:4222",
    )
    return sup


@pytest.mark.asyncio
async def test_supervisor_builds_navi_from_config():
    """Given a config naming NaviBackend, the supervisor's enricher set wraps a
    NaviBackend — proves the registry resolution end-to-end."""
    with patch("central.supervisor.EnrichmentCache") as cache_cls:
        cache_cls.return_value = MagicMock(invalidate=AsyncMock(return_value=0))
        sup = _supervisor_with(
            EnrichmentConfig(backend_class="NaviBackend",
                             backend_settings={"base_url": "http://x:8440", "warmup": False})
        )
        cfg = await sup._config_source.get_enrichment_config()
        sup._rebuild_enrichers(cfg)
        assert isinstance(sup._enrichers[0]._backend, NaviBackend)


@pytest.mark.asyncio
async def test_hot_reload_rebuilds_and_invalidates_on_backend_change():
    from central import supervisor as sup_mod

    with patch("central.supervisor.EnrichmentCache") as cache_cls:
        invalidate = AsyncMock(return_value=5)
        cache_cls.return_value = MagicMock(invalidate=invalidate)
        # Start at NoOp.
        sup = _supervisor_with(EnrichmentConfig())
        sup._rebuild_enrichers(EnrichmentConfig())
        assert isinstance(sup._enrichers[0]._backend, NoOpBackend)
        # Config flips to Navi.
        sup._config_source.get_enrichment_config = AsyncMock(
            return_value=EnrichmentConfig(
                backend_class="NaviBackend",
                backend_settings={"base_url": "http://x:8440", "warmup": False},
            )
        )
        await sup._handle_enrichment_change()
        assert isinstance(sup._enrichers[0]._backend, NaviBackend)
        invalidate.assert_awaited()  # backend changed -> cache wiped


@pytest.mark.asyncio
async def test_hot_reload_does_not_invalidate_on_ttl_only_change():
    with patch("central.supervisor.EnrichmentCache") as cache_cls:
        invalidate = AsyncMock(return_value=0)
        cache_cls.return_value = MagicMock(invalidate=invalidate)
        sup = _supervisor_with(EnrichmentConfig())
        sup._rebuild_enrichers(EnrichmentConfig())
        # Same backend, only TTL changes.
        sup._config_source.get_enrichment_config = AsyncMock(
            return_value=EnrichmentConfig(cache_ttl_s=3600)
        )
        await sup._handle_enrichment_change()
        invalidate.assert_not_awaited()


# --- generic json widget + GUI render ----------------------------------------

def test_describe_fields_renders_dict_as_json_widget():
    fields = {f.name: f.widget for f in describe_fields(EnrichmentConfig, {})}
    assert fields["backend_settings"] == "json"
    assert fields["enricher_class"] == "text"
    assert fields["cache_ttl_s"] == "number"


@pytest.mark.asyncio
async def test_enrichment_form_renders():
    from central.gui.routes import enrichment_form

    request = MagicMock()
    request.state.operator = MagicMock(username="op")
    request.state.csrf_token = "tok"

    conn = MagicMock()
    conn.fetchrow = AsyncMock(return_value={
        "enricher_class": "GeocoderEnricher",
        "backend_class": "NoOpBackend",
        "backend_settings": {},
        "cache_ttl_s": 86400,
    })
    templates = MagicMock()
    templates.TemplateResponse.return_value = MagicMock()

    with patch("central.gui.routes._get_templates", return_value=templates), \
         patch("central.gui.routes.get_pool", return_value=_mock_pool(conn)):
        await enrichment_form(request)

    ctx = templates.TemplateResponse.call_args.kwargs["context"]
    field_widgets = {f.name: f.widget for f in ctx["fields"]}
    assert field_widgets["backend_settings"] == "json"
    assert ctx["csrf_token"] == "tok"
