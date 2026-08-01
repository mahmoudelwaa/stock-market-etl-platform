# Stock Market ETL Pipeline & Analytics Platform

An end-to-end data pipeline that automatically extracts stock market data (NVIDIA + competitors) on a daily schedule, processes it, and visualizes it through an analytics dashboard.

## Overview
This project combines Data Engineering and Data Analytics in a single system — from automated extraction and orchestration to transformation and visualization.

## Tech Stack
- **Orchestration:** Apache Airflow
- **Storage:** PostgreSQL
- **Processing:** PySpark
- **Dashboard:** Streamlit / Power BI
- **Containerization:** Docker & Docker Compose

## Status
🚧 Under active development

## Architecture
```
Airflow DAG (daily automated run)
    ↓
Extract stock data from API (Alpha Vantage / yfinance)
    ↓
Store raw data in PostgreSQL
    ↓
PySpark cleans data & computes indicators (moving average, volatility...)
    ↓
Update the Data Warehouse (Star Schema)
    ↓
Dashboard updates automatically
```