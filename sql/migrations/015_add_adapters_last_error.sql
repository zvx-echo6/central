-- Migration: 015_add_adapters_last_error
-- Adds last_error column for adapter-side error reporting.
-- Populated by supervisor when an adapter fails to start or apply config.

ALTER TABLE config.adapters
ADD COLUMN last_error TEXT;
