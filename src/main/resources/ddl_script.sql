-- A) Ensure gw2_prices exists
CREATE TABLE IF NOT EXISTS public.gw2_prices (
    item_id    INTEGER PRIMARY KEY,
    buy        INTEGER NOT NULL,
    sell       INTEGER NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS gw2_prices_updated_at_idx
    ON public.gw2_prices (updated_at DESC);

-- B) Bring calculations to latest shape
ALTER TABLE public.calculations
    ADD COLUMN IF NOT EXISTS notes            TEXT,
    ADD COLUMN IF NOT EXISTS inserted_at      TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_at       TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS source_table_key VARCHAR(255);

-- Unique rule per (category,key)
CREATE UNIQUE INDEX IF NOT EXISTS calculations_unique
    ON public.calculations (category, key);

-- Ensure taxes CHECK constraint exists
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relname = 'calculations'
      AND c.conname = 'calculations_tax_chk'
  ) THEN
    ALTER TABLE public.calculations
      ADD CONSTRAINT calculations_tax_chk CHECK (taxes >= 0 AND taxes <= 100);
  END IF;
END$$;

-- (Re)create updated_at touch trigger
CREATE OR REPLACE FUNCTION public.tg_calculations_touch_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  NEW.updated_at := NOW();
  RETURN NEW;
END
$fn$;

DROP TRIGGER IF EXISTS trg_calculations_touch ON public.calculations;
CREATE TRIGGER trg_calculations_touch
BEFORE UPDATE ON public.calculations
FOR EACH ROW
EXECUTE PROCEDURE public.tg_calculations_touch_updated_at();

-- C) Seed a few rules (safe to re-run)
INSERT INTO public.calculations (category, key, operation, taxes, source_table_key, notes)
VALUES
  ('bag','branded-geodermite','SUM',15,NULL,'default bag rule'),
  ('INTERNAL','conversions/spirit-shard','SUM',0,NULL,'internal no tax'),
  ('INTERNAL','conversions/karma','SUM',0,NULL,'internal no tax')
ON CONFLICT (category, key) DO UPDATE
SET operation = EXCLUDED.operation,
    taxes     = EXCLUDED.taxes,
    source_table_key = EXCLUDED.source_table_key;

-- D) Optional: seed one price to verify overlay end-to-end
INSERT INTO public.gw2_prices(item_id, buy, sell)
VALUES (84084, 164107, 173225)
ON CONFLICT (item_id) DO UPDATE
SET buy = EXCLUDED.buy, sell = EXCLUDED.sell, updated_at = NOW();

-- 1) Add the missing timestamp column
ALTER TABLE public.gw2_prices
  ADD COLUMN IF NOT EXISTS ts timestamptz;

-- 2) Backfill NULLs (if any) and enforce constraints
UPDATE public.gw2_prices SET ts = now() WHERE ts IS NULL;
ALTER TABLE public.gw2_prices ALTER COLUMN ts SET DEFAULT now();
ALTER TABLE public.gw2_prices ALTER COLUMN ts SET NOT NULL;

-- 3) Ensure the correct composite index exists (drop old, recreate proper)
DROP INDEX IF EXISTS gw2_prices_item_ts_idx;
CREATE INDEX gw2_prices_item_ts_idx
  ON public.gw2_prices (item_id, ts DESC);