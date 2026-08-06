-- ============================================================
-- Stock Market ETL Pipeline & Analytics Platform
-- Warehouse Schema (Star Schema)
-- ============================================================
-- Purpose: Cleaned, structured data populated by PySpark after
-- transforming rows from raw_stock_prices. Optimized for
-- analytical queries and the dashboard layer.
--
-- Safe to re-run: uses IF NOT EXISTS so it won't error out if
-- the tables/indexes already exist.
-- ============================================================

-- ------------------------------------------------------------
-- Dimension: Stocks
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_stock (
    stock_id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) UNIQUE NOT NULL,
    company_name VARCHAR(100),
    sector VARCHAR(50)
);

-- ------------------------------------------------------------
-- Dimension: Dates
-- ------------------------------------------------------------
-- Precomputing date attributes avoids repeated EXTRACT()/date
-- math in every analytical query (e.g. quarterly comparisons,
-- day-of-week performance).
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    day INT,
    month INT,
    year INT,
    quarter INT,
    day_of_week VARCHAR(10),
    is_trading_day BOOLEAN DEFAULT TRUE
);

-- ------------------------------------------------------------
-- Fact: Stock Prices + Computed Indicators
-- ------------------------------------------------------------
-- Indicators (moving averages, volatility, daily return) are
-- computed by PySpark during the transform step, not derived
-- here. Keeps a clean separation between raw ingestion and
-- transformation, matching the pipeline architecture.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_stock_prices (
    fact_id BIGSERIAL PRIMARY KEY,
    stock_id INT REFERENCES dim_stock(stock_id),
    date_id INT REFERENCES dim_date(date_id),
    open NUMERIC(12,4),
    high NUMERIC(12,4),
    low NUMERIC(12,4),
    close NUMERIC(12,4),
    volume BIGINT,
    daily_return NUMERIC(8,4),
    moving_avg_5d NUMERIC(12,4),
    moving_avg_20d NUMERIC(12,4),
    volatility_20d NUMERIC(12,4),

    UNIQUE (stock_id, date_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_stock_prices_stock_date
    ON fact_stock_prices (stock_id, date_id);

-- ------------------------------------------------------------
-- Optional: Corporate Actions (dividends / stock splits)
-- ------------------------------------------------------------
-- Kept separate from the fact table since these events are
-- sparse (mostly zero on any given day) and not part of the
-- core daily price analysis.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_corporate_actions (
    action_id BIGSERIAL PRIMARY KEY,
    stock_id INT REFERENCES dim_stock(stock_id),
    date_id INT REFERENCES dim_date(date_id),
    dividends NUMERIC(10,4) DEFAULT 0,
    stock_splits NUMERIC(10,4) DEFAULT 0,

    UNIQUE (stock_id, date_id)
);



-- ============================================================
--docker exec -it stock_postgres psql -U stock_admin -d stock_data -f /raw_layer.sql
--docker cp sql/raw_layer.sql stock_postgres:/raw_layer.sql
--docker cp sql/warehouse_schema.sql stock_postgres:/warehouse_schema.sql
-- ============================================================
