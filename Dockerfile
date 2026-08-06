## Dockerfile for Airflow with Java support
## this Dockerfile extends the official Apache Airflow image to include Java support.
## pyspark requires Java to be installed, so this Dockerfile installs the default JDK.

FROM apache/airflow:2.9.3

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

RUN pip install --no-cache-dir \
    psycopg2-binary \
    pandas \
    yfinance \
    pyspark==3.5.1 \
    python-dotenv \
    pandas_market_calendars