-- Migration: 024_add_config_enrichment
-- Adds config.enrichment — the single-row, operator-settable enrichment config
-- the supervisor reads at startup and hot-reloads via LISTEN/NOTIFY.
--
-- Single-row pattern mirrors config.system (id BOOLEAN PK CHECK (id = true)).
-- Seeds framework DEFAULTS ONLY: GeocoderEnricher + NoOpBackend, empty
-- backend_settings, 24h cache TTL. NO deployment-specific values (no URLs,
-- IPs, or auth) — operators set base_url / auth via the /enrichment GUI page
-- after this merges.
--
-- The seed mirrors central.config_models.EnrichmentConfig() defaults.
-- Regenerate via:
--     sudo -u central .venv/bin/python -c \
--         "from central.config_models import EnrichmentConfig; print(EnrichmentConfig().model_dump_json())"
--
-- Idempotent per docs/migrations.md (CREATE TABLE IF NOT EXISTS, INSERT ...
-- ON CONFLICT DO NOTHING, DROP TRIGGER IF EXISTS before CREATE TRIGGER).

CREATE TABLE IF NOT EXISTS config.enrichment (
    id                BOOLEAN PRIMARY KEY DEFAULT true CHECK (id = true),
    enricher_class    TEXT NOT NULL DEFAULT 'GeocoderEnricher',
    backend_class     TEXT NOT NULL DEFAULT 'NoOpBackend',
    backend_settings  JSONB NOT NULL DEFAULT '{}'::jsonb,
    cache_ttl_s       INTEGER NOT NULL DEFAULT 86400,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Reuse the existing updated_at trigger function (migration 002).
DROP TRIGGER IF EXISTS enrichment_set_updated_at ON config.enrichment;
CREATE TRIGGER enrichment_set_updated_at
    BEFORE UPDATE ON config.enrichment
    FOR EACH ROW
    EXECUTE FUNCTION config.set_updated_at();

-- Reuse the existing NOTIFY function (migration 001) so the supervisor's
-- LISTEN/NOTIFY hot-reload picks up enrichment changes. The function's ELSE
-- branch emits 'enrichment:' (empty key — single-row table has no natural key).
DROP TRIGGER IF EXISTS enrichment_notify ON config.enrichment;
CREATE TRIGGER enrichment_notify
    AFTER INSERT OR UPDATE OR DELETE ON config.enrichment
    FOR EACH ROW EXECUTE FUNCTION config.notify_config_change();

-- Seed the single framework-default row (NoOp; no deployment-specific values).
INSERT INTO config.enrichment (id) VALUES (true) ON CONFLICT DO NOTHING;
