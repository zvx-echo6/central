-- Migration: 023_add_nwis_adapter_and_hydro_stream
-- Adds the CENTRAL_HYDRO JetStream stream row AND the NWIS adapter row.
-- Folded into a single migration because the adapter publishes onto
-- central.hydro.> — both rows ship together.
--
-- Stream retention mirrors CENTRAL_DISASTER (7 days, 1 GiB).
-- Adapter ships disabled; operator enables via GUI after setting a bbox.
--
-- The settings JSON below is the literal output of NWISSettings().model_dump_json()
-- at migration-author time. Regenerate via:
--     sudo -u central .venv/bin/python -c \
--         "from central.adapters.nwis import NWISSettings; print(NWISSettings().model_dump_json())"
-- Do NOT hand-edit the parameter_codes here — _DEFAULT_PARAMETER_CODES in
-- src/central/adapters/nwis.py is the single source of truth.
--
-- Idempotent: both inserts use ON CONFLICT DO NOTHING.

INSERT INTO config.streams (name, max_age_s, max_bytes)
VALUES ('CENTRAL_HYDRO', 604800, 1073741824)
ON CONFLICT (name) DO NOTHING;

INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'nwis',
    false,
    900,
    '{"parameter_codes":["00060","00065","00010"],"region":null}'::jsonb
)
ON CONFLICT (name) DO NOTHING;
