"""
Phase 5: Transform raw stock data into the analytical warehouse using PySpark.

Pipeline steps:
    1. Read raw OHLCV rows from `raw_stock_prices` via a Spark JDBC connection.
    2. Compute technical indicators using window functions:
       - daily_return   (% change vs previous trading day's close)
       - moving_avg_5d  (simple moving average, last 5 trading days)
       - moving_avg_20d (simple moving average, last 20 trading days)
       - volatility_20d (stddev of daily_return, last 20 trading days)
    3. Resolve foreign keys against `dim_stock` (symbol -> stock_id) and
       `dim_date` (price_date -> date_id).
    4. Upsert the final rows into `fact_stock_prices` (idempotent: safe
       to re-run for any date range, existing rows get overwritten with
       fresh values via ON CONFLICT DO UPDATE).

Environment variables required (same convention as extract_stock_data.py):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

Notes:
    - Indicators are computed as ROW-based windows (last N rows per symbol,
      ordered by price_date), not calendar-day windows. This is correct
      here because raw_stock_prices only ever contains actual trading
      days (no weekend/holiday gaps to account for).
    - daily_return is expressed as a PERCENTAGE (e.g. 2.35, not 0.0235).
    - The first row(s) of each symbol's history will have NULL
      daily_return / volatility_20d until enough prior rows exist to
      compute them. This is expected and not an error.
"""

import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([DB_NAME, DB_USER, DB_PASSWORD]):
    sys.exit(
        "Missing required DB environment variables "
        "(DB_NAME, DB_USER, DB_PASSWORD). Check .env / container env."
    )

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
JDBC_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# Indicator window sizes (in trading days / rows)
MA_SHORT_WINDOW = 5
MA_LONG_WINDOW = 20
VOLATILITY_WINDOW = 20

POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.3"


def get_spark_session() -> SparkSession:
    """Create a Spark session with the Postgres JDBC driver available."""
    return (
        SparkSession.builder.appName("StockDataTransform")
        .config("spark.jars.packages", POSTGRES_JDBC_PACKAGE)
        .getOrCreate()
    )


def read_raw_prices(spark: SparkSession):
    """Read raw OHLCV data from raw_stock_prices via JDBC."""
    return spark.read.jdbc(
        url=JDBC_URL, table="raw_stock_prices", properties=JDBC_PROPERTIES
    ).select("symbol", "price_date", "open", "high", "low", "close", "volume")


def compute_indicators(df):
    """Add daily_return, moving_avg_5d, moving_avg_20d, volatility_20d."""
    order_spec = Window.partitionBy("symbol").orderBy("price_date")

    ma_short_spec = order_spec.rowsBetween(-(MA_SHORT_WINDOW - 1), 0)
    ma_long_spec = order_spec.rowsBetween(-(MA_LONG_WINDOW - 1), 0)
    vol_spec = order_spec.rowsBetween(-(VOLATILITY_WINDOW - 1), 0)

    df = df.withColumn("prev_close", F.lag("close").over(order_spec))

    df = df.withColumn(
        "daily_return",
        F.when(
            F.col("prev_close").isNotNull(),
            (F.col("close") - F.col("prev_close")) / F.col("prev_close") * 100,
        ),
    )

    df = df.withColumn("moving_avg_5d", F.avg("close").over(ma_short_spec))
    df = df.withColumn("moving_avg_20d", F.avg("close").over(ma_long_spec))
    df = df.withColumn("volatility_20d", F.stddev("daily_return").over(vol_spec))

    return df.drop("prev_close")


def resolve_foreign_keys(spark: SparkSession, df):
    """Join computed indicators with dim_stock and dim_date to get FKs."""
    dim_stock = spark.read.jdbc(
        url=JDBC_URL, table="dim_stock", properties=JDBC_PROPERTIES
    ).select("stock_id", "symbol")

    dim_date = spark.read.jdbc(
        url=JDBC_URL, table="dim_date", properties=JDBC_PROPERTIES
    ).select("date_id", "full_date")

    df = df.join(dim_stock, on="symbol", how="inner")
    df = df.join(dim_date, df["price_date"] == dim_date["full_date"], how="inner")

    return df.select(
        "stock_id",
        "date_id",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "daily_return",
        "moving_avg_5d",
        "moving_avg_20d",
        "volatility_20d",
    )


def upsert_to_postgres(rows: list[tuple]) -> None:
    """Upsert final rows into fact_stock_prices via psycopg2 (not Spark JDBC write,
    to get proper ON CONFLICT handling)."""
    if not rows:
        print("No rows to upsert.")
        return

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    try:
        with conn.cursor() as cur:
            query = """
                INSERT INTO fact_stock_prices (
                    stock_id, date_id, open, high, low, close, volume,
                    daily_return, moving_avg_5d, moving_avg_20d, volatility_20d
                )
                VALUES %s
                ON CONFLICT (stock_id, date_id) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    daily_return = EXCLUDED.daily_return,
                    moving_avg_5d = EXCLUDED.moving_avg_5d,
                    moving_avg_20d = EXCLUDED.moving_avg_20d,
                    volatility_20d = EXCLUDED.volatility_20d;
            """
            execute_values(cur, query, rows)
        conn.commit()
        print(f"Upserted {len(rows)} rows into fact_stock_prices.")
    finally:
        conn.close()


def main():
    spark = get_spark_session()
    try:
        raw_df = read_raw_prices(spark)
        enriched_df = compute_indicators(raw_df)
        final_df = resolve_foreign_keys(spark, enriched_df)

        rows = [tuple(r) for r in final_df.collect()]
        upsert_to_postgres(rows)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()