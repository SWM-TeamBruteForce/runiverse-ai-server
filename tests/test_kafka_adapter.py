"""Kafka 어댑터: 접속 설정, 발행 확인, 수동 커밋. 실제 브로커 없이 confluent 객체를 흉내 낸다."""

import json

import pytest

from runiverse_ai_server.adapters.kafka import (
    KafkaPublisher,
    KafkaRequestSource,
    consumer_config,
    producer_config,
)
from runiverse_ai_server.config import KafkaSettings
from runiverse_ai_server.ports import KafkaRecord, PublishError


@pytest.fixture
def settings(monkeypatch, tmp_path):
    ca = tmp_path / "kafka-ca.crt"
    ca.write_text("dummy")
    for key, value in {"KAFKA_ENV": "dev", "KAFKA_BOOTSTRAP_SERVERS": "<DB_SERVER_IP>:9092",
                       "KAFKA_PASSWORD": "secret", "KAFKA_CA_PATH": str(ca)}.items():
        monkeypatch.setenv(key, value)
    return KafkaSettings()


def test_consumer_config_matches_backend_example_and_commits_manually(settings):
    config = consumer_config(settings)

    assert {k: config[k] for k in ["bootstrap.servers", "security.protocol", "sasl.mechanism", "sasl.username",
                                   "sasl.password", "group.id"]} == {
        "bootstrap.servers": "<DB_SERVER_IP>:9092", "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512", "sasl.username": "dev.runiverse-ai", "sasl.password": "secret",
        "group.id": "dev.runiverse-ai.record-card-music"}
    assert config["ssl.ca.location"].endswith("kafka-ca.crt")
    assert config["enable.auto.commit"] is False  # 202 이후에만 커밋
    assert config["auto.offset.reset"] == "earliest"


def test_producer_waits_for_all_replicas(settings):
    config = producer_config(settings)

    assert (config["acks"], config["enable.idempotence"]) == ("all", True)
    assert "group.id" not in config


# ── 발행 ──────────────────────────────────────────────


class FakeProducer:
    def __init__(self, error=None, remaining=0):
        self.produced = []
        self.error = error
        self.remaining = remaining

    def produce(self, topic, value, key, on_delivery):
        self.produced.append((topic, key, value))
        self._callback = on_delivery

    def flush(self, timeout):
        if not self.remaining:
            self._callback(self.error, None)
        return self.remaining


def test_publish_sends_json_with_card_key():
    producer = FakeProducer()
    message = {"inference_id": "x", "record_card_id": 101, "status": "FAILED", "invoke": {"message": "한글"}}

    KafkaPublisher(producer, "dev.record-card.music.generated").publish(message)

    [(topic, key, value)] = producer.produced
    assert (topic, key) == ("dev.record-card.music.generated", b"101")
    assert json.loads(value.decode("utf-8")) == message
    assert "한글".encode() in value  # \uXXXX로 바꾸지 않는다


def test_publish_without_card_id_has_no_key():
    producer = FakeProducer()

    KafkaPublisher(producer, "t").publish({"record_card_id": None})

    assert producer.produced[0][1] is None


def test_publish_delivery_error_raises():
    with pytest.raises(PublishError):
        KafkaPublisher(FakeProducer(error="broker down"), "t").publish({"record_card_id": 1})


def test_publish_timeout_raises():
    with pytest.raises(PublishError, match="확인"):
        KafkaPublisher(FakeProducer(remaining=1), "t").publish({"record_card_id": 1})


# ── 소비·커밋 ──────────────────────────────────────────


class FakeMessage:
    def __init__(self, offset=5, error=None):
        self._offset = offset
        self._error = error

    def error(self):
        return self._error

    def topic(self):
        return "dev.record-card.music.requested"

    def partition(self):
        return 2

    def offset(self):
        return self._offset

    def value(self):
        return b'{"user_id": 1}'


class FakeConsumer:
    def __init__(self, messages):
        self.messages = list(messages)
        self.subscribed = None
        self.commits = []

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, timeout):
        return self.messages.pop(0) if self.messages else None

    def commit(self, offsets, asynchronous):
        self.commits.append(([(tp.topic, tp.partition, tp.offset) for tp in offsets], asynchronous))


def test_source_subscribes_and_returns_record():
    consumer = FakeConsumer([FakeMessage()])
    source = KafkaRequestSource(consumer, "dev.record-card.music.requested")

    record = source.poll(1.0)

    assert consumer.subscribed == ["dev.record-card.music.requested"]
    assert record == KafkaRecord(topic="dev.record-card.music.requested", partition=2, offset=5,
                                 value=b'{"user_id": 1}')


def test_source_returns_none_when_idle():
    assert KafkaRequestSource(FakeConsumer([]), "t").poll(1.0) is None


def test_source_skips_broker_event_messages():
    consumer = FakeConsumer([FakeMessage(error="partition EOF")])

    assert KafkaRequestSource(consumer, "t").poll(1.0) is None


def test_commit_is_next_offset_and_synchronous():
    consumer = FakeConsumer([])
    record = KafkaRecord(topic="dev.record-card.music.requested", partition=2, offset=5, value=b"")

    KafkaRequestSource(consumer, "t").commit(record)

    assert consumer.commits == [([("dev.record-card.music.requested", 2, 6)], False)]


def test_source_close_leaves_group():
    consumer = FakeConsumer([])
    consumer.close = lambda: setattr(consumer, "closed", True)

    KafkaRequestSource(consumer, "t").close()

    assert consumer.closed


def test_publisher_close_flushes_pending():
    producer = FakeProducer()
    flushed = []
    producer.flush = lambda timeout: flushed.append(timeout) or 0

    KafkaPublisher(producer, "t").close()

    assert flushed == [30]
