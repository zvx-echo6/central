-- Migration: 025_add_wzdx_adapter_and_traffic_stream
-- Adds the CENTRAL_TRAFFIC JetStream stream row AND the WZDx adapter row.
-- Folded into one migration because the adapter publishes onto
-- central.traffic.> -- both rows ship together (mirrors 023 nwis/hydro).
--
-- Stream retention mirrors CENTRAL_DISASTER / CENTRAL_HYDRO (7 days, 1 GiB).
-- Adapter ships disabled; operator enables via GUI. settings {"states": null}
-- = poll every eligible feed; an allowlist of 2-letter codes narrows it.
--
-- Additive-only: both inserts are idempotent via ON CONFLICT DO NOTHING.

INSERT INTO config.streams (name, max_age_s, max_bytes)
VALUES ('CENTRAL_TRAFFIC', 604800, 1073741824)
ON CONFLICT (name) DO NOTHING;

INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'wzdx',
    false,
    600,
    '{"states": null}'::jsonb
)
ON CONFLICT (name) DO NOTHING;
