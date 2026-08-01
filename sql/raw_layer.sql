-- ============================================================
-- Stock Market ETL Pipeline & Analytics Platform
-- Raw Layer Schema
-- ============================================================
-- Purpose: Stores stock data as close as possible to its
-- original form from yfinance, before any cleaning or
-- transformation. This is the landing zone for daily extraction.
-- ============================================================

CREATE TABLE raw_stock_prices (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    price_date DATE NOT NULL,
    open NUMERIC(12,4),
    high NUMERIC(12,4),
    low NUMERIC(12,4),
    close NUMERIC(12,4),
    volume BIGINT,
    dividends NUMERIC(10,4) DEFAULT 0,
    stock_splits NUMERIC(10,4) DEFAULT 0,
    ingested_at TIMESTAMPTZ DEFAULT now(),

    -- Prevents duplicate rows if the Airflow DAG re-runs
    -- for a date that was already ingested.
    UNIQUE (symbol, price_date)
);

-- Helpful index for querying by symbol across date ranges
CREATE INDEX idx_raw_stock_prices_symbol_date
    ON raw_stock_prices (symbol, price_date);

-- ============================================================
-- Notes
-- ============================================================
-- price_date is stored as DATE (not TIMESTAMPTZ) because daily
-- OHLCV data represents a full trading day, not a single moment.
-- Any timezone-aware timestamp coming from yfinance should be
-- normalized to UTC and truncated to a date before insertion.
--
-- ingested_at tracks when the row was actually loaded, useful
-- for debugging failed/re-run DAG executions.
-- ============================================================
