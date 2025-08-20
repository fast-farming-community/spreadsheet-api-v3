-- static sheet snapshots (no dynamic price columns here)
CREATE TABLE IF NOT EXISTS detail_tables (
  id             bigserial PRIMARY KEY,
  key            text NOT NULL UNIQUE,
  name           text NOT NULL,
  range          text NOT NULL,
  rows           jsonb NOT NULL,             -- JSON array of objects (static-only)
  inserted_at    timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  content_sha256 text
);

-- fast price cache (we'll fill this later with GW2 prices)
CREATE TABLE IF NOT EXISTS prices_current (
  item_id    integer PRIMARY KEY,
  buy_unit   integer,
  sell_unit  integer,
  updated_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS prices_current_updated_idx ON prices_current(updated_at DESC);
