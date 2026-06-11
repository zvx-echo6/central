-- Migration 042: generalize the single system bbox into a named multi-area list (v0.14.0)
--
-- Until now the system monitoring area was a single rectangle stored as four
-- columns on the config.system singleton (monitor_north/south/east/west, added
-- by migration 030, default widened to full Idaho by 034). Matt needs to watch
-- several non-contiguous regions (Treasure Valley, Magic Valley, Mountain Home,
-- ...) without inflating one box to cover everything between them. This migration
-- introduces config.monitoring_areas: a list of named bboxes with set-union
-- semantics -- an event is kept by the archive (and at publish time) if its
-- geometry intersects ANY area in the list. An empty list keeps everything,
-- matching the pre-feature "no area configured" default.
--
-- The existing single bbox is preserved verbatim as the row named 'default',
-- carrying Matt's current production bounds (N=49.0 S=41.8 E=-111.0 W=-117.5).
--
-- The old config.system.monitor_* columns are intentionally LEFT IN PLACE for
-- v0.14.0 (consumers stop reading them; the GUI tile_url/attribution still live
-- on config.system). They are dropped in a follow-up (v0.14.1) once the new
-- table is proven on prod. Idempotent per docs/migrations.md.

CREATE TABLE IF NOT EXISTS config.monitoring_areas (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    north      DOUBLE PRECISION NOT NULL,
    south      DOUBLE PRECISION NOT NULL,
    east       DOUBLE PRECISION NOT NULL,
    west       DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT monitoring_areas_lat_order CHECK (north > south),
    CONSTRAINT monitoring_areas_lon_order CHECK (east  > west)
);

CREATE INDEX IF NOT EXISTS monitoring_areas_name_idx ON config.monitoring_areas(name);

-- Seed the 'default' area from the existing single bbox so current bounds are
-- preserved on upgrade. Skipped cleanly if the row already exists (re-run) or if
-- any monitor_* column is NULL (no usable bbox -> nothing to migrate, list stays
-- empty = keep everything).
INSERT INTO config.monitoring_areas (name, north, south, east, west)
SELECT 'default', monitor_north, monitor_south, monitor_east, monitor_west
FROM config.system
WHERE id = true
  AND monitor_north IS NOT NULL
  AND monitor_south IS NOT NULL
  AND monitor_east  IS NOT NULL
  AND monitor_west  IS NOT NULL
ON CONFLICT (name) DO NOTHING;
