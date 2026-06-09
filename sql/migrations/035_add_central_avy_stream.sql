-- Migration 035: seed CENTRAL_AVY JetStream stream config row (v0.10.10)
-- Backs the central.avy.> subject space populated by the avalanche_org adapter.
-- 7-day retention, 1 GiB max_bytes — mirrors CENTRAL_FIRE / CENTRAL_QUAKE /
-- CENTRAL_SPACE defaults. Operator can re-tune via the /streams GUI page.
-- Idempotent: uses ON CONFLICT DO NOTHING.

INSERT INTO config.streams (name, max_age_s, max_bytes)
VALUES ('CENTRAL_AVY', 604800, 1073741824)
ON CONFLICT (name) DO NOTHING;
