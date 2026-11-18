"""요청 토픽 메시지의 **구조만** 출력한다 (키 이름·값 타입. 값은 출력하지 않음 → 회고·캡션 노출 없음).

- 그룹에 참여하지 않고(assign) 커밋하지 않는다 → 서버가 읽을 위치에 영향 없음
- 사용 (WSL, 터널·비밀번호·KAFKA_ENV 준비 후, ai-server 루트):
    ~/.venvs/ai-server/bin/python scripts/kafka_shape_check.py            # 보관된 메시지 전부
    ~/.venvs/ai-server/bin/python scripts/kafka_shape_check.py --offset 3 # 한 건만
"""

from __future__ import annotations

import argparse
import json
from typing import Any

TIMEOUT = 15


def shape(value: Any, path: str = "", out: list[str] | None = None) -> list[str]:
    """값 대신 타입을 적은 키 경로 목록. 리스트는 첫 원소 구조와 길이만."""
    out = [] if out is None else out
    if isinstance(value, dict):
        if not value:
            out.append(f"{path or '(최상위)'}: 빈 객체")
        for key, child in value.items():
            shape(child, f"{path}.{key}" if path else key, out)
    elif isinstance(value, list):
        out.append(f"{path}: 리스트 {len(value)}개")
        if value:
            shape(value[0], f"{path}[0]", out)
    else:
        out.append(f"{path}: {_type(value)}")
    return out


def _type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "정수(양수)" if value > 0 else "정수(0 이하)"
    if isinstance(value, float):
        return "실수"
    if isinstance(value, str):
        if value.strip().lstrip("-").isdigit():
            return "문자열(숫자 모양)"
        return f"문자열({len(value)}자)"
    return type(value).__name__


def describe(body: bytes | None) -> list[str]:
    if body is None:
        return ["본문 없음 (null)"]
    try:
        data = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return [f"JSON 아님 ({len(body)}바이트)"]
    return shape(data) or ["(최상위): 값 하나"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="요청 토픽 메시지 구조(키·타입) 확인")
    parser.add_argument("--offset", type=int, help="이 offset 한 건만")
    args = parser.parse_args(argv)

    from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

    from runiverse_ai_server.adapters.kafka import consumer_config
    from runiverse_ai_server.config import ConfigError, KafkaSettings, load_settings

    try:
        settings = load_settings(KafkaSettings)
    except ConfigError as exc:
        print(f"❌ {exc}")
        return 2

    topic = settings.request_topic
    consumer = Consumer(consumer_config(settings))
    try:
        partitions = sorted(consumer.list_topics(topic, timeout=TIMEOUT).topics[topic].partitions)
        ends = {}
        assignment = []
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=TIMEOUT)
            if high > low:
                ends[p] = high
                start = args.offset if args.offset is not None else OFFSET_BEGINNING
                assignment.append(TopicPartition(topic, p, start))
        if not assignment:
            print(f"{topic}: 보관된 메시지 없음")
            return 0
        consumer.assign(assignment)  # subscribe가 아니므로 그룹 참여·커밋 없음

        print(f"📂 {topic}")
        done: set[int] = set()
        while len(done) < len(ends):
            msg = consumer.poll(TIMEOUT)
            if msg is None:
                print("  (응답 없음 → 종료)")
                break
            if msg.error():
                print(f"  ⚠️ {msg.error()}")
                continue
            print(f"\n  📨 offset {msg.offset()} (파티션 {msg.partition()}) · key {'있음' if msg.key() else '없음'}")
            for line in describe(msg.value()):
                print(f"    {line}")
            if args.offset is not None or msg.offset() + 1 >= ends[msg.partition()]:
                done.add(msg.partition())
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
