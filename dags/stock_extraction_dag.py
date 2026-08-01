"""
Stock Market ETL Pipeline — Daily Extraction DAG

Runs extract_stock_data.main() once a day to pull the latest OHLCV data
for NVDA, AMD, and INTC from yfinance and upsert it into raw_stock_prices.

The extraction script lives in /opt/airflow/scripts (mounted separately
from /opt/airflow/dags), so it's added to sys.path before importing.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# scripts/ is mounted at /opt/airflow/scripts in docker-compose.yml
SCRIPTS_PATH = "/opt/airflow/scripts"
if SCRIPTS_PATH not in sys.path:
    sys.path.insert(0, SCRIPTS_PATH)

from extract_stock_data import main as run_extraction  # noqa: E402


default_args = {
    "owner": "mahmoud",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_extraction_daily",
    description="Daily extraction of NVDA/AMD/INTC OHLCV data into raw_stock_prices",
    default_args=default_args,
    schedule="0 6 * * *",  # 06:00 UTC daily — after US markets close
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["stock-pipeline", "extraction"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_stock_data",
        python_callable=run_extraction,
    )