-- Migration 036: register avalanche_org adapter row in config.adapters (v0.10.10.1)
--
-- v0.10.10 (PR #98) shipped the avalanche_org adapter code, GUI templates,
-- stream registry entry, and migration 035 (CENTRAL_AVY config.streams seed).
-- It also (incorrectly) assumed the supervisor would schedule a new adapter
-- the moment its code landed. It won't: supervisor.list_enabled_adapters()
-- reads from config.adapters and skips any adapter without a row, so the
-- v0.10.10 deploy left avalanche_org code-shipped-but-inactive.
--
-- This migration closes that gap. Mirrors the pattern of migration 031
-- (itd_511) -- INSERT a row with default settings, idempotent via
-- ON CONFLICT (name) DO NOTHING.
--
-- Unlike itd_511 (which shipped disabled because it needed an API key
-- staged first), avalanche_org has no auth requirement and is enabled
-- immediately: the upstream API is public and the off-season severity
-- gate means the adapter yields zero events during summer regardless,
-- so 'enabled at install' is the right default.
--
-- Settings JSON matches AvalancheOrgSettings: center_ids defaults to
-- SNFAC + PAC (Idaho-region coverage). Operator can extend via GUI to
-- any avalanche.org-recognised center (NWAC, CAIC, etc.).
--
-- Idempotent: re-running is a no-op for existing rows.

INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'avalanche_org',
    true,
    1800,
    '{"center_ids": ["SNFAC", "PAC"]}'::jsonb
)
ON CONFLICT (name) DO NOTHING;
