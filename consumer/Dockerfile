FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

# Spark needs a JVM; procps provides the `ps` command Spark scripts use.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY consumer/requirements.txt consumer/requirements.txt
RUN pip install --no-cache-dir -r consumer/requirements.txt

COPY consumer/ consumer/

CMD ["python", "consumer/spark_consumer.py"]