"""dev Kafka 읽기 전용 점검: 접속·토픽·파티션별 offset·우리 그룹이 읽을 메시지 수.

메시지를 읽거나 쓰지 않고, 그룹에 참여하지도 않는다 (커밋 위치만 조회).
사용 (ai-server 루트, 비밀번호는 미리 환경 변수로):
    KAFKA_ENV=dev KAFKA_BOOTSTRAP_SERVERS=<DB_SERVER_IP>:9092 PYTHONUTF8=1 .venv/Scripts/python scripts/kafka_check.py
"""

from __future__ import annotations

import sys

TIMEOUT = 15


def pending_count(low: int, high: int, committed: int | None) -> int:
    """earliest 기준으로 우리 그룹이 앞으로 읽을 메시지 수."""
    start = low if committed is None or committed < low else committed
    return max(high - start, 0)


def main() -> int:
    from confluent_kafka import Consumer, KafkaException, TopicPartition
    from confluent_kafka.admin import AdminClient

    from runiverse_ai_server.adapters.kafka import consumer_config
    from runiverse_ai_server.config import ConfigError, KafkaSettings, load_settings

    try:
        settings = load_settings(KafkaSettings)
    except ConfigError as exc:
        print(f"❌ {exc}")
        return 2

    config = consumer_config(settings)
    print(f"브로커 {settings.kafka_bootstrap_servers} · 사용자 {settings.kafka_username} · 그룹 {settings.kafka_group_id}")

    admin_config = {k: v for k, v in config.items()
                    if k not in ("group.id", "enable.auto.commit", "auto.offset.reset")}
    try:
        metadata = AdminClient(admin_config).list_topics(timeout=TIMEOUT)
    except KafkaException as exc:
        print(f"❌ 접속 실패: {exc}")
        return 1
    print(f"✅ 접속 성공 (브로커 {len(metadata.brokers)}대)")

    consumer = Consumer(config)
    exit_code = 0
    try:
        for topic in (settings.request_topic, settings.completion_topic):
            info = metadata.topics.get(topic)
            if info is None or info.error is not None:
                print(f"\n❌ {topic}: 보이지 않음 ({info.error if info else '없음'}) → 토픽 없음 또는 권한(Describe) 없음")
                exit_code = 1
                continue
            partitions = sorted(info.partitions)
            print(f"\n📂 {topic} (파티션 {len(partitions)}개)")

            committed: dict[int, int | None] = {p: None for p in partitions}
            if topic == settings.request_topic:
                try:
                    for tp in consumer.committed([TopicPartition(topic, p) for p in partitions], timeout=TIMEOUT):
                        committed[tp.partition] = tp.offset if tp.offset >= 0 else None
                except KafkaException as exc:
                    print(f"  ⚠️ 그룹 커밋 위치 조회 실패: {exc} → 그룹 권한(Read) 확인 필요")
                    exit_code = 1

            total = 0
            for p in partitions:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=TIMEOUT)
                line = f"  파티션 {p}: offset {low}~{high} (보관 {high - low}건)"
                if topic == settings.request_topic:
                    pending = pending_count(low, high, committed[p])
                    total += pending
                    line += f" · 커밋 {committed[p] if committed[p] is not None else '없음'} · 읽을 메시지 {pending}건"
                print(line)
            if topic == settings.request_topic:
                print(f"  → 서버를 켜면 처리할 요청: {total}건")
    except KafkaException as exc:
        print(f"❌ 조회 실패: {exc}")
        exit_code = 1
    finally:
        consumer.close()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
