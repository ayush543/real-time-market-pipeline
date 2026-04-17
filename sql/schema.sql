CREATE TABLE IF NOT EXISTS raw_market_data (
    id SERIAL PRIMARY KEY,
    asset_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    price_usd NUMERIC NOT NULL,
    market_cap NUMERIC,
    total_volume NUMERIC,
    price_change_pct_24h NUMERIC,
    source_last_updated TIMESTAMP,
    event_ts TIMESTAMP NOT NULL,
    is_anomaly BOOLEAN DEFAULT FALSE,
    raw_payload JSONB NOT NULL
);
