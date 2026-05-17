-- Migration 007: Add config.system table for global settings
-- Idempotent per docs/migrations.md

CREATE TABLE IF NOT EXISTS config.system (
    id BOOLEAN PRIMARY KEY DEFAULT true CHECK (id = true),
    setup_complete BOOLEAN NOT NULL DEFAULT false,
    session_lifetime_days INTEGER NOT NULL DEFAULT 90,
    map_tile_url TEXT NOT NULL DEFAULT 'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
    map_attribution TEXT NOT NULL DEFAULT '© OpenStreetMap contributors',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Reuse existing set_updated_at trigger function
DROP TRIGGER IF EXISTS system_set_updated_at ON config.system;
CREATE TRIGGER system_set_updated_at
    BEFORE UPDATE ON config.system
    FOR EACH ROW
    EXECUTE FUNCTION config.set_updated_at();

-- Seed single row
INSERT INTO config.system (id) VALUES (true) ON CONFLICT DO NOTHING;
