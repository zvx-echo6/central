-- Migration 011: Add adapter column to events, drop source column
-- Replaces module-path-based source with stable adapter identifier

-- Add adapter column (idempotent)
ALTER TABLE public.events ADD COLUMN IF NOT EXISTS adapter TEXT;

-- Backfill from existing source values
UPDATE public.events
SET adapter = REPLACE(source, 'central/adapters/', '')
WHERE adapter IS NULL AND source IS NOT NULL;

-- Make NOT NULL after backfill (idempotent check)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'events'
          AND column_name = 'adapter'
          AND is_nullable = 'YES'
    ) THEN
        ALTER TABLE public.events ALTER COLUMN adapter SET NOT NULL;
    END IF;
END $$;

-- Add FK constraint (idempotent check)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'events_adapter_fkey'
          AND table_name = 'events'
    ) THEN
        ALTER TABLE public.events
        ADD CONSTRAINT events_adapter_fkey
        FOREIGN KEY (adapter) REFERENCES config.adapters(name)
        ON DELETE RESTRICT;
    END IF;
END $$;

-- Add index for dashboard queries (idempotent)
CREATE INDEX IF NOT EXISTS events_adapter_received_idx
ON public.events (adapter, received DESC);

-- Drop deprecated source column (idempotent)
ALTER TABLE public.events DROP COLUMN IF EXISTS source;
