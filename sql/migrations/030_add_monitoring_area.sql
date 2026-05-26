-- Migration 030: Add system-level monitoring-area bbox to config.system (v0.9.12)
-- Backs the archive-level safety-net filter: events whose geometry lies entirely
-- outside this bbox are dropped at INSERT time; null-geom events are always kept.
-- Default bounds = Idaho. Idempotent per docs/migrations.md.

ALTER TABLE config.system
    ADD COLUMN IF NOT EXISTS monitor_north DOUBLE PRECISION NOT NULL DEFAULT 44.5,
    ADD COLUMN IF NOT EXISTS monitor_south DOUBLE PRECISION NOT NULL DEFAULT 41.8,
    ADD COLUMN IF NOT EXISTS monitor_east  DOUBLE PRECISION NOT NULL DEFAULT -111.0,
    ADD COLUMN IF NOT EXISTS monitor_west  DOUBLE PRECISION NOT NULL DEFAULT -117.5;
