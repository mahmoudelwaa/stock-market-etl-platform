"""
Stock Market ETL Pipeline — Daily Transformation DAG

Runs transform_stock_data.main() to compute technical indicators
(daily_return, moving_avg_5d, moving_avg_20d, volatility_20d) via
PySpark and upsert the results into fact_stock_prices.

This DAG has no fixed schedule of its own — it's triggered
automatically by stock_extraction_daily's trigger_transformation
task right after a successful extraction run. This avoids relying
on a fixed time offset between the two DAGs, which would break if
extraction ever ran late.

The transform script lives in /opt/airflow/scripts (mounted separately
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

from transform_stock_data import main as run_transformation  # noqa: E402


default_args = {
    "owner": "mahmoud",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_transformation_daily",
    description="PySpark transformation of raw_stock_prices into fact_stock_prices",
    default_args=default_args,
    schedule=None,  # triggered by stock_extraction_daily, not time-based
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["stock-pipeline", "transformation"],
) as dag:

    transform_task = PythonOperator(
        task_id="transform_stock_data",
        python_callable=run_transformation,
    )