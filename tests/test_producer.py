import json

import pytest
import requests

import producer

NETWORK = {"id": "divvy"}
CFG = {
    "api": {"url": "http://example.test", "timeout_seconds": 1},
    "kafka": {"topic": "divvy_stations"},
}


def make_station(**overrides):
    station = {
        "id": "abc123",
        "name": "Calumet Ave & 21st St",
        "latitude": 41.854,
        "longitude": -87.619,
        "free_bikes": 11,
        "empty_slots": 1,
        "timestamp": "2026-10-13T04:32:29.844222+00:00Z",
    }
    station.update(overrides)
    return station


class FakeProducer:
    """Stands in for confluent_kafka.Producer and records what was sent."""

    def __init__(self, fail_delivery=False):
        self.messages = []
        self.fail_delivery = fail_delivery

    def produce(self, topic, key, value, callback):
        self.messages.append({"topic": topic, "key": key, "value": value})
        callback("boom" if self.fail_delivery else None, None)

    def poll(self, timeout):
        return 0

    def flush(self, timeout=None):
        return 0


def test_to_record_flattens_a_station():
    record = producer.to_record(
        NETWORK, make_station(), "2026-10-13T04:33:07+00:00")
    assert record == {
        "network_id": "divvy",
        "station_id": "abc123",
        "name": "Calumet Ave & 21st St",
        "latitude": 41.854,
        "longitude": -87.619,
        "free_bikes": 11,
        "empty_slots": 1,
        "source_timestamp": "2026-10-13T04:32:29.844222+00:00Z",
        "ingested_at": "2026-10-13T04:33:07+00:00",
    }


def test_to_record_defaults_missing_optional_fields_to_none():
    record = producer.to_record(NETWORK, {"id": "abc123"}, "t")
    assert record["name"] is None
    assert record["free_bikes"] is None
    assert record["source_timestamp"] is None


def test_to_record_rejects_a_station_without_id():
    with pytest.raises(KeyError):
        producer.to_record(NETWORK, {"name": "no id"}, "t")


def test_load_config_reads_kafka_address_from_environment(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    assert producer.load_config(
    )["kafka"]["bootstrap_servers"] == "kafka:29092"


def test_load_config_defaults_to_the_yaml_value(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    assert producer.load_config(
    )["kafka"]["bootstrap_servers"] == "localhost:9092"


def test_delivery_report_counts_successes_and_failures():
    stats = {"delivered": 0, "failed": 0}
    callback = producer.make_delivery_report(stats)
    callback(None, None)
    callback("error", None)
    assert stats == {"delivered": 1, "failed": 1}


def test_publish_snapshot_sends_one_message_per_station(monkeypatch):
    network = {"id": "divvy", "stations": [
        make_station(id="a"), make_station(id="b")]}
    monkeypatch.setattr(producer, "fetch_network",
                        lambda url, timeout: network)
    fake = FakeProducer()

    producer.publish_snapshot(CFG, fake)

    assert [m["key"] for m in fake.messages] == ["a", "b"]
    assert {m["topic"] for m in fake.messages} == {"divvy_stations"}
    first = json.loads(fake.messages[0]["value"])
    assert first["station_id"] == "a"
    assert first["network_id"] == "divvy"
    # All stations of one pull share the same ingestion timestamp.
    assert len({json.loads(m["value"])["ingested_at"]
               for m in fake.messages}) == 1


def test_publish_snapshot_survives_an_api_failure(monkeypatch):
    def broken(url, timeout):
        raise requests.ConnectionError("API down")

    monkeypatch.setattr(producer, "fetch_network", broken)
    fake = FakeProducer()

    producer.publish_snapshot(CFG, fake)  # must not raise

    assert fake.messages == []


def test_publish_snapshot_logs_failed_deliveries(monkeypatch, caplog):
    network = {"id": "divvy", "stations": [make_station()]}
    monkeypatch.setattr(producer, "fetch_network",
                        lambda url, timeout: network)

    with caplog.at_level("ERROR", logger="divvy-producer"):
        producer.publish_snapshot(CFG, FakeProducer(fail_delivery=True))

    assert "Not all messages were delivered" in caplog.text
