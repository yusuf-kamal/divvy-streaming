import json
import shutil

import pytest

pytest.importorskip("pyspark")

import spark_consumer  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.functions import col  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    if shutil.which("java") is None:
        pytest.skip("Java is not installed")
    session = (
        SparkSession.builder.master("local[1]")
        .appName("divvy-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def station_message(**overrides):
    record = {
        "network_id": "divvy",
        "station_id": "abc123",
        "name": "Calumet Ave & 21st St",
        "latitude": 41.854,
        "longitude": -87.619,
        "free_bikes": 11,
        "empty_slots": 1,
        "source_timestamp": "2026-10-13T04:32:29.844222+00:00Z",
        "ingested_at": "2026-10-13T04:33:07.445179+00:00",
    }
    record.update(overrides)
    return json.dumps(record)


def raw_frame(spark, message):
    # A Kafka message arrives with its payload in a "value" column.
    return spark.createDataFrame([(message,)], ["value"])


def test_load_config_overrides_connections_from_environment(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    monkeypatch.setenv("SQLSERVER_HOST", "sqlserver")
    monkeypatch.setenv("SQLSERVER_PORT", "1433")

    cfg = spark_consumer.load_config()

    assert cfg["kafka"]["bootstrap_servers"] == "kafka:29092"
    assert cfg["sqlserver"]["host"] == "sqlserver"
    assert cfg["sqlserver"]["port"] == "1433"


def test_parse_stations_strips_the_trailing_z(spark):
    parsed = spark_consumer.parse_stations(raw_frame(spark, station_message()))
    row = parsed.select(
        col("source_timestamp").cast("string").alias("source"),
        col("ingested_at").cast("string").alias("ingested"),
    ).collect()[0]

    # Without the fix the API's "+00:00Z" suffix made this NULL.
    assert row.source == "2026-10-13 04:32:29.844222"
    assert row.ingested == "2026-10-13 04:33:07.445179"


def test_parse_stations_casts_types(spark):
    parsed = spark_consumer.parse_stations(raw_frame(spark, station_message()))
    types = {f.name: f.dataType.simpleString() for f in parsed.schema.fields}

    assert types["latitude"] == "double"
    assert types["free_bikes"] == "int"
    assert types["source_timestamp"] == "timestamp"


def test_parse_stations_keeps_rows_with_missing_optional_fields(spark):
    message = json.dumps({
        "network_id": "divvy",
        "station_id": "x",
        "ingested_at": "2026-10-13T04:33:07+00:00",
    })
    rows = spark_consumer.parse_stations(raw_frame(spark, message)).collect()

    assert len(rows) == 1
    assert rows[0].station_id == "x"
    assert rows[0].free_bikes is None
    assert rows[0].source_timestamp is None
