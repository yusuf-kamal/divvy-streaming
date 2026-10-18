"""Divvy station producer.

Polls the CityBikes API for the Divvy (Chicago) network and publishes one JSON
message per station to a Kafka topic. Settings are read from config/config.yaml.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from confluent_kafka import Producer

# Resolved from this file's location so the script works from any working directory.
CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "config.yaml"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("divvy-producer")


def load_config():
    """Load the YAML configuration file into a dict.

    The Kafka address can be overridden with KAFKA_BOOTSTRAP_SERVERS, which is
    how the Docker container reaches the broker (kafka:29092).
    """
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg["kafka"]["bootstrap_servers"] = os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS", cfg["kafka"]["bootstrap_servers"])
    return cfg


def fetch_network(url, timeout):
    """Fetch the network object (including its stations) from the API.

    Errors are intentionally not caught here; publish_snapshot() handles them.
    """
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.json()["network"]


def to_record(network, station, ingested_at):
    """Flatten a single station into the record published to Kafka.

    Raises KeyError if the station has no id, since that means corrupt data.
    All other fields are optional and default to None.
    """
    return {
        "network_id": network["id"],
        "station_id": station["id"],
        "name": station.get("name"),
        "latitude": station.get("latitude"),
        "longitude": station.get("longitude"),
        "free_bikes": station.get("free_bikes"),
        "empty_slots": station.get("empty_slots"),
        # Time of the station's last update at the source.
        "source_timestamp": station.get("timestamp"),
        # Time we pulled the data; lets us track pipeline freshness and history.
        "ingested_at": ingested_at,
    }


def make_delivery_report(stats):
    """Return a Kafka delivery callback that counts delivered/failed messages."""
    def delivery_report(err, msg):
        if err is not None:
            stats["failed"] += 1
            log.error("Delivery failed: %s", err)
        else:
            stats["delivered"] += 1
    return delivery_report


def publish_snapshot(cfg, producer):
    """Pull one full snapshot from the API and publish it to Kafka.

    A failure here is logged and never raised, so one bad cycle (API down,
    network drop) does not kill the producer.
    """
    stats = {"delivered": 0, "failed": 0}
    on_delivery = make_delivery_report(stats)

    try:
        network = fetch_network(
            cfg["api"]["url"], cfg["api"]["timeout_seconds"])
        # Single UTC timestamp so all stations of one pull match.
        ingested_at = datetime.now(timezone.utc).isoformat()
        stations = network.get("stations", [])

        for station in stations:
            record = to_record(network, station, ingested_at)
            producer.produce(
                cfg["kafka"]["topic"],
                # Same key -> same partition, preserving per-station ordering.
                key=station["id"],
                value=json.dumps(record),
                callback=on_delivery,
            )
            # Serve delivery callbacks so the local queue never fills up.
            producer.poll(0)

        pending = producer.flush(timeout=30)
    except Exception:
        log.exception("Cycle failed")
        return

    if pending or stats["failed"]:
        log.error("Not all messages were delivered: %d pending, %d failed",
                  pending, stats["failed"])
    else:
        log.info("Sent %d stations", stats["delivered"])


def main():
    """Poll the API forever and publish station records to Kafka."""
    cfg = load_config()
    producer = Producer(
        {"bootstrap.servers": cfg["kafka"]["bootstrap_servers"]})

    log.info("Producer started, topic=%s", cfg["kafka"]["topic"])
    try:
        while True:
            publish_snapshot(cfg, producer)
            # Outside the try block in publish_snapshot, so a failing API is
            # not retried in a tight loop.
            time.sleep(cfg["api"]["poll_interval_seconds"])
    except KeyboardInterrupt:
        log.info("Stopped by user")
        producer.flush(timeout=10)


if __name__ == "__main__":
    main()
