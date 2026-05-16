-- Migration: 002_add_updated_at_trigger_and_index
-- Adds auto-update trigger for updated_at column on adapters table
-- Adds partial index for efficient enabled adapter queries

-- Auto-update trigger for updated_at
CREATE OR REPLACE FUNCTION config.set_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER adapters_set_updated_at
    BEFORE UPDATE ON config.adapters
    FOR EACH ROW EXECUTE FUNCTION config.set_updated_at();

-- Partial index for enabled adapters (common query pattern)
CREATE INDEX adapters_enabled_idx
    ON config.adapters (enabled)
    WHERE enabled = true;
