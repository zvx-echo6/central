-- Migration: 022_add_eonet_adapter
-- Adds the NASA EONET adapter row to config.adapters.
-- Ships disabled; operator enables via GUI.
--
-- The settings JSON below is the literal output of EONETSettings().model_dump_json()
-- at migration-author time. Regenerate via:
--     sudo -u central .venv/bin/python -c \
--         "from central.adapters.eonet import EONETSettings; print(EONETSettings().model_dump_json())"
-- Do NOT hand-edit the category_allowlist here — _DEFAULT_CATEGORIES in
-- src/central/adapters/eonet.py is the single source of truth.
--
-- Idempotent: uses ON CONFLICT DO NOTHING.

INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'eonet',
    false,
    1800,
    '{"category_allowlist":["drought","dustHaze","earthquakes","floods","landslides","manmade","seaLakeIce","severeStorms","snow","tempExtremes","volcanoes","waterColor","wildfires"],"region":null}'::jsonb
)
ON CONFLICT (name) DO NOTHING;
