"""Divvy station consumer.

Reads station messages from Kafka with Spark Structured Streaming, parses the
JSON into typed columns, and appends each micro-batch to SQL Server (bronze).

The same code runs locally (Windows authentication) and in Docker (SQL login).
Environment variables override config.yaml; see load_config() and
make_batch_writer().
"""

import logging
import os
import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_replace, to_timestamp
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
# The checkpoint must live on persistent storage (a Docker volume in compose).
CHECKPOINT_DIR = Path(os.environ.get(
    "CHECKPOINT_DIR", PROJECT_ROOT / "checkpoints" / "divvy_bronze"))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("divvy-consumer")

# Make Spark's Python workers use the same interpreter as this one.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

# Must match the record built by the producer's to_record().
STATION_SCHEMA = StructType([
    StructField("network_id", StringType()),
    StructField("station_id", StringType()),
    StructField("name", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("free_bikes", IntegerType()),
    StructField("empty_slots", IntegerType()),
    StructField("source_timestamp", StringType()),
    StructField("ingested_at", StringType()),
])

PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11",
])


def load_config():
    """Load config.yaml, letting environment variables override connections."""
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    env = os.environ
    cfg["kafka"]["bootstrap_servers"] = env.get(
        "KAFKA_BOOTSTRAP_SERVERS", cfg["kafka"]["bootstrap_servers"])
    cfg["sqlserver"]["host"] = env.get(
        "SQLSERVER_HOST", cfg["sqlserver"]["host"])
    cfg["sqlserver"]["port"] = env.get(
        "SQLSERVER_PORT", cfg["sqlserver"]["port"])
    return cfg


def create_spark_session(cfg):
    """Create a Spark session with the Kafka and SQL Server connectors."""
    java_opts = ["-Duser.timezone=UTC"]
    if os.name == "nt":
        # Windows only. java.library.path REPLACES the default search path, so
        # keep Hadoop's bin folder (hadoop.dll) next to the folder holding the
        # SQL Server auth DLL (needed only for Windows authentication).
        dll_dir = (PROJECT_ROOT / cfg["sqlserver"]["auth_dll_dir"]).as_posix()
        hadoop_bin = (Path(os.environ["HADOOP_HOME"]) / "bin").as_posix()
        java_opts.append(
            f"-Djava.library.path={os.pathsep.join([dll_dir, hadoop_bin])}")

    return (
        SparkSession.builder
        .appName("divvy-stations-consumer")
        .master("local[*]")
        .config("spark.jars.packages", PACKAGES)
        # Keep everything in UTC (Spark session and the JVM used by JDBC).
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", " ".join(java_opts))
        .getOrCreate()
    )


def read_stream(spark, cfg):
    """Return a streaming DataFrame of raw Kafka messages."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"])
        .option("subscribe", cfg["kafka"]["topic"])
        # Only used when no checkpoint exists yet (first run).
        .option("startingOffsets", os.environ.get("STARTING_OFFSETS", "earliest"))
        .load()
    )


def parse_stations(raw_df):
    """Turn raw Kafka messages (bytes) into typed station columns."""
    return (
        raw_df
        .select(from_json(col("value").cast("string"), STATION_SCHEMA).alias("data"))
        .select("data.*")
        # The API sends both an offset and a trailing Z ("+00:00Z"), which Spark
        # cannot parse, so strip the Z before converting.
        .withColumn("source_timestamp",
                    to_timestamp(regexp_replace("source_timestamp", "Z$", "")))
        .withColumn("ingested_at", to_timestamp("ingested_at"))
    )


def make_batch_writer(cfg):
    """Build the foreachBatch function that appends one batch to SQL Server.

    Uses SQL authentication when SQLSERVER_USER is set (Docker), otherwise
    Windows integrated authentication (local development).
    """
    sql = cfg["sqlserver"]
    user = os.environ.get("SQLSERVER_USER")
    password = os.environ.get("SQLSERVER_PASSWORD")

    url = (f"jdbc:sqlserver://{sql['host']}:{sql['port']};"
           f"databaseName={sql['database']};"
           "encrypt=true;trustServerCertificate=true")
    options = {
        "dbtable": sql["table"],
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "batchsize": "5000",
    }
    if user:
        options["user"] = user
        options["password"] = password
    else:
        url += ";integratedSecurity=true;authenticationScheme=NativeAuthentication"

    def write_batch(batch_df, batch_id):
        # Batch 0 is usually empty; skip it instead of opening a connection.
        if batch_df.isEmpty():
            return
        (
            batch_df.write
            .format("jdbc")
            .option("url", url)
            .options(**options)
            .mode("append")
            .save()
        )
        log.info("Batch %s written to %s", batch_id, sql["table"])

    return write_batch


def main():
    cfg = load_config()
    spark = create_spark_session(cfg)
    spark.sparkContext.setLogLevel("WARN")

    stations = parse_stations(read_stream(spark, cfg))

    query = (
        stations.writeStream
        .foreachBatch(make_batch_writer(cfg))
        .option("checkpointLocation", str(CHECKPOINT_DIR))
        .trigger(processingTime="30 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
