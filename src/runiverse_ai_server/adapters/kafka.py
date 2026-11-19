"""Kafka 어댑터 (confluent-kafka). 접속 설정은 백엔드 예시와 같고, 커밋은 수동이다."""

from __future__ import annotations

import json
from typing import Any

from ..config import KafkaSettings
from ..log import log_event
from ..messages import kafka_headers, kafka_key
from ..ports import KafkaRecord, PublishError

PUBLISH_TIMEOUT_SECONDS = 30


def _common(settings: KafkaSettings) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": settings.kafka_username,
        "sasl.password": settings.kafka_password.get_secret_value(),
        "ssl.ca.location": str(settings.kafka_ca_path),
    }


def consumer_config(settings: KafkaSettings) -> dict[str, Any]:
    return {
        **_common(settings),
        "group.id": settings.kafka_group_id,
        "enable.auto.commit": False,  # 202(또는 실패 발행) 이후 commit()으로만 커밋
        "auto.offset.reset": "earliest",  # 커밋 기록이 없을 때 쌓인 요청부터 읽는다
    }


def producer_config(settings: KafkaSettings) -> dict[str, Any]:
    return {**_common(settings), "acks": "all", "enable.idempotence": True}


class KafkaPublisher:
    """완료 메시지 발행. 브로커 저장 확인까지 기다린다."""

    def __init__(self, producer: Any, topic: str, timeout: float = PUBLISH_TIMEOUT_SECONDS):
        self.producer = producer
        self.topic = topic
        self.timeout = timeout

    def publish(self, message: dict[str, Any]) -> None:
        errors = []
        self.producer.produce(self.topic, value=json.dumps(message, ensure_ascii=False).encode("utf-8"),
                              key=kafka_key(message), headers=kafka_headers(message),
                              on_delivery=lambda err, _msg: errors.append(err))
        if self.producer.flush(self.timeout) > 0:
            raise PublishError(f"{self.timeout:.0f}초 안에 발행 확인을 받지 못했습니다")
        if errors and errors[0] is not None:
            raise PublishError(str(errors[0]))

    def close(self) -> None:
        remaining = self.producer.flush(self.timeout)
        if remaining:
            log_event("kafka", "producer_unflushed", level="ERROR", remaining=remaining)


class KafkaRequestSource:
    """요청 토픽 소비. 처리 끝난 메시지만 commit()으로 커밋한다."""

    def __init__(self, consumer: Any, topic: str):
        self.consumer = consumer
        self.consumer.subscribe([topic])

    def poll(self, timeout: float) -> KafkaRecord | None:
        msg = self.consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():  # 브로커 이벤트·오류 (인증 실패 등). 본문이 없다
            log_event("kafka", "consumer_error", level="ERROR", error=str(msg.error()))
            return None
        return KafkaRecord(topic=msg.topic(), partition=msg.partition(), offset=msg.offset(), value=msg.value())

    def commit(self, record: KafkaRecord) -> None:
        from confluent_kafka import TopicPartition

        # 커밋 값 = 다음에 읽을 위치
        self.consumer.commit(offsets=[TopicPartition(record.topic, record.partition, record.offset + 1)],
                             asynchronous=False)

    def close(self) -> None:
        """그룹에서 바로 빠진다 (다음 실행이 곧바로 파티션을 배정받음)."""
        self.consumer.close()
