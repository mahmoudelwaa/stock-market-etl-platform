"""
Backfill script for dim_date table.

Dynamic range: reads the earliest and latest date actually present in
raw_stock_prices, adds a buffer (BUFFER_DAYS) before/after, and generates
one row per calendar day in that range.

is_trading_day is computed using the real NASDAQ trading calendar via
pandas_market_calendars (accounts for New Year, MLK Day, Good Friday,
Memorial Day, Juneteenth, Independence Day, Labor Day, Thanksgiving,
Christmas, etc.) instead of a naive weekday/weekend check.

Requirements:
    pip install pandas_market_calendars psycopg2-binary

DB connection settings are read from environment variables (see
get_db_config below). Override the defaults there if your env var names
differ.
"""

import os
import sys
from datetime import date, timedelta

import pandas_market_calendars as mcal
import psycopg2
from psycopg2.extras import execute_values

# Days of buffer to add before the earliest and after the latest date
# found in raw_stock_prices.
BUFFER_DAYS = 30

# Source table/column used to determine the date range dynamically.
# Update these if your column/table names differ.
RAW_TABLE = "raw_stock_prices"
RAW_DATE_COLUMN = "price_date"


def get_db_config():
    """Read DB connection settings from environment variables."""
    return {
        "host": os.getenv("DB_HOST", "postgres"),
        "port": os.getenv("DB_PORT", "5432"),
        "dbname": os.getenv("DB_NAME", "stock_admin"),
        "user": os.getenv("DB_USER", "stock_admin"),
        "password": os.getenv("DB_PASSWORD", ""),
    }


def get_date_range(db_config):
    """Get MIN/MAX date from raw_stock_prices and apply BUFFER_DAYS margin."""
    conn = psycopg2.connect(**db_config)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT MIN({RAW_DATE_COLUMN}), MAX({RAW_DATE_COLUMN}) FROM {RAW_TABLE}"
            )
            min_date, max_date = cur.fetchone()
    finally:
        conn.close()

    if min_date is None or max_date is None:
        raise ValueError(
            f"{RAW_TABLE} is empty or has no data in column {RAW_DATE_COLUMN}. "
            "Data must be extracted first before backfilling dim_date."
        )

    start = min_date - timedelta(days=BUFFER_DAYS)
    end = max_date + timedelta(days=BUFFER_DAYS)
    return start.isoformat(), end.isoformat()


def build_date_dim(start: str, end: str):
    """Build the list of date rows, with is_trading_day from the real NASDAQ calendar."""
    nasdaq = mcal.get_calendar("NASDAQ")
    schedule = nasdaq.schedule(start_date=start, end_date=end)
    trading_days = set(schedule.index.date)

    all_days = []
    current = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    while current <= end_d:
        all_days.append(
            {
                # date_id is a deterministic YYYYMMDD integer (not an
                # incrementing counter), so the same date always maps to
                # the same id regardless of the backfill range used.
                "date_id": int(current.strftime("%Y%m%d")),
                "full_date": current,
                "day": current.day,
                "month": current.month,
                "year": current.year,
                "quarter": (current.month - 1) // 3 + 1,
                "day_of_week": current.strftime("%A"),
                "is_trading_day": current in trading_days,
            }
        )
        current = current + timedelta(days=1)

    return all_days


def insert_rows(rows, db_config):
    conn = psycopg2.connect(**db_config)
    try:
        with conn.cursor() as cur:
            query = """
                INSERT INTO dim_date
                    (date_id, full_date, day, month, year, quarter, day_of_week, is_trading_day)
                VALUES %s
                ON CONFLICT (date_id) DO NOTHING
            """
            values = [
                (
                    r["date_id"],
                    r["full_date"],
                    r["day"],
                    r["month"],
                    r["year"],
                    r["quarter"],
                    r["day_of_week"],
                    r["is_trading_day"],
                )
                for r in rows
            ]
            execute_values(cur, query, values, page_size=1000)
        conn.commit()
        print(f"Inserted {len(rows)} rows into dim_date successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}", file=sys.stderr)
        raise
    finally:
        conn.close()


def main():
    db_config = get_db_config()
    print(f"Connecting to {db_config['host']}:{db_config['port']}/{db_config['dbname']} ...")

    print(f"Getting actual date range from {RAW_TABLE} ...")
    start, end = get_date_range(db_config)
    print(f"Range (with {BUFFER_DAYS}-day buffer): {start} -> {end}")

    print("Generating dates ...")
    rows = build_date_dim(start, end)
    print("Computing trading days from the NASDAQ calendar ...")
    trading_count = sum(1 for r in rows if r["is_trading_day"])
    print(f"Total days: {len(rows)} | trading days: {trading_count}")

    insert_rows(rows, db_config)


if __name__ == "__main__":
    main()