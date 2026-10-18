FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    DBT_PROFILES_DIR=/app/dbt_divvy_streaming

# Microsoft ODBC driver: dbt talks to SQL Server through it (via pyodbc).
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gnupg ca-certificates \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
    | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list \
    -o /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 unixodbc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY dbt_divvy_streaming/requirements.txt dbt_divvy_streaming/requirements.txt
RUN pip install --no-cache-dir -r dbt_divvy_streaming/requirements.txt

COPY dbt_divvy_streaming/ dbt_divvy_streaming/

WORKDIR /app/dbt_divvy_streaming

# Stopgap scheduler until Airflow: rebuild the models every interval.
CMD ["sh", "-c", "while true; do dbt run --target docker; sleep ${DBT_INTERVAL_SECONDS:-60}; done"]