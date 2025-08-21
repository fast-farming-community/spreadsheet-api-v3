-- gw2_prices
CREATE TABLE IF NOT EXISTS public.gw2_prices (
  item_id integer NOT NULL,
  buy integer NOT NULL,
  sell integer NOT NULL,
  vendor_value integer,
  updated_at timestamp without time zone NOT NULL DEFAULT now(),
  ts timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT gw2_prices_pkey PRIMARY KEY (item_id)
);

-- detail_tables overlays
CREATE TABLE IF NOT EXISTS public.detail_tables_overlay (
  detail_feature_id BIGINT NOT NULL,
  key VARCHAR(255) NOT NULL,
  tier VARCHAR(16) NOT NULL,
  -- '5m','10m','15m','60m'
  rows JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (detail_feature_id, key, tier)
);

-- tables overlays
CREATE TABLE IF NOT EXISTS public.tables_overlay (
  key VARCHAR(255) NOT NULL,
  tier VARCHAR(16) NOT NULL,
  -- '5m','10m','15m','60m'
  rows JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (key, tier)
);

-- gw2_prices_tiers
CREATE TABLE IF NOT EXISTS public.gw2_prices_tiers (
  item_id INTEGER PRIMARY KEY,
  buy_5m INTEGER,
  sell_5m INTEGER,
  ts_5m TIMESTAMPTZ,
  buy_10m INTEGER,
  sell_10m INTEGER,
  ts_10m TIMESTAMPTZ,
  buy_15m INTEGER,
  sell_15m INTEGER,
  ts_15m TIMESTAMPTZ,
  buy_60m INTEGER,
  sell_60m INTEGER,
  ts_60m TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS tables_range_uniq ON public.tables("range");

CREATE UNIQUE INDEX IF NOT EXISTS detail_tables_range_uniq ON public.detail_tables("range");

CREATE INDEX IF NOT EXISTS gw2_prices_vendor_idx ON public.gw2_prices (vendor_value);

CREATE INDEX IF NOT EXISTS gw2_prices_tiers_item_id_idx ON public.gw2_prices_tiers(item_id);

CREATE INDEX IF NOT EXISTS gw2_prices_tiers_ts_5m_idx ON public.gw2_prices_tiers(ts_5m);

CREATE INDEX IF NOT EXISTS gw2_prices_tiers_ts_10m_idx ON public.gw2_prices_tiers(ts_10m);

CREATE INDEX IF NOT EXISTS gw2_prices_tiers_ts_15m_idx ON public.gw2_prices_tiers(ts_15m);

CREATE INDEX IF NOT EXISTS gw2_prices_tiers_ts_60m_idx ON public.gw2_prices_tiers(ts_60m);