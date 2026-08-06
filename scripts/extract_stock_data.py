"""
Stock Market ETL Pipeline & Analytics Platform
Extraction Script

Pulls daily OHLCV data for a list of stock symbols from yfinance
and loads it into the raw_stock_prices table in PostgreSQL.

This script is designed to be safe to re-run: if a (symbol, date)
row already exists, it gets updated instead of causing a duplicate
key error (upsert behavior via ON CONFLICT).

Usage:
    python extract_stock_data.py
"""
import logging
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv
import os

import pandas as pd
import psycopg2
import psycopg2.extras
import yfinance as yf

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

SYMBOLS = ["NVDA", "AMD", "INTC"]
PERIOD = "1mo"  # how much history to pull each run

load_dotenv(dotenv_path=os.getenv("DOTENV_PATH", ".env.local"), override=True)
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------
# Extraction
# ------------------------------------------------------------

def fetch_symbol_data(symbol: str, period: str = PERIOD) -> pd.DataFrame:
    """Fetch OHLCV history for a single symbol from yfinance."""
    logger.info(f"Fetching data for {symbol}...")
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period)
    except Exception as e:
        logger.error(f"Failed to fetch data for {symbol}: {e}")
        return pd.DataFrame()

    if df.empty:
        logger.warning(f"No data returned for {symbol}")
        return df

    df = df.reset_index()
    df["symbol"] = symbol

    # Normalize the date: yfinance returns a tz-aware Date column
    # (market timezone). Convert to UTC, then take the date part only,
    # since daily OHLCV represents a full trading day, not a moment.
    df["price_date"] = pd.to_datetime(df["Date"]).dt.tz_convert("UTC").dt.date

    df = df.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
            "Dividends": "dividends",
            "Stock Splits": "stock_splits",
        }
    )

    return df[
        [
            "symbol",
            "price_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "dividends",
            "stock_splits",
        ]
    ]


def fetch_all(symbols: list[str]) -> pd.DataFrame:
    """Fetch data for all symbols and combine into one DataFrame."""
    frames = [fetch_symbol_data(s) for s in symbols]
    frames = [f for f in frames if not f.empty]

    if not frames:
        logger.error("No data fetched for any symbol. Aborting.")
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


# ------------------------------------------------------------
# Loading
# ------------------------------------------------------------

UPSERT_SQL = """
    INSERT INTO raw_stock_prices (
        symbol, price_date, open, high, low, close,
        volume, dividends, stock_splits
    )
    VALUES %s
    ON CONFLICT (symbol, price_date)
    DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        dividends = EXCLUDED.dividends,
        stock_splits = EXCLUDED.stock_splits,
        ingested_at = now();
"""


def load_to_postgres(df: pd.DataFrame, db_config: dict) -> None:
    """Load a DataFrame of stock rows into raw_stock_prices via upsert."""
    if df.empty:
        logger.warning("Nothing to load — DataFrame is empty.")
        return

    records = list(
        df[
            [
                "symbol",
                "price_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "dividends",
                "stock_splits",
            ]
        ].itertuples(index=False, name=None)
    )

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, UPSERT_SQL, records)
        conn.commit()
        logger.info(f"Loaded/updated {len(records)} rows into raw_stock_prices.")
    except Exception as e:
        logger.error(f"Database load failed: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


# ------------------------------------------------------------
# Entry point
# ------------------------------------------------------------

def main():
    logger.info(f"Starting extraction run at {datetime.now(timezone.utc).isoformat()}")
    logger.info(f"Symbols: {', '.join(SYMBOLS)}")

    df = fetch_all(SYMBOLS)
    if df.empty:
        logger.error("Extraction produced no data. Exiting with error code.")
        sys.exit(1)

    load_to_postgres(df, DB_CONFIG)
    logger.info("Extraction run complete.")


if __name__ == "__main__":
    main()
