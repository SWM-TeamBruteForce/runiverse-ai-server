"""검증 ②: dev 요청 토픽 소비·커밋·재시작 후 재소비. 소비 뒤 처리는 Fake (S3·SageMaker·DB·완료 발행 없음).

    status                 그룹 상태·소비자·커밋 위치
    reset                  그룹 커밋 위치를 토픽 맨 앞(보관된 첫 offset)으로 되돌림
    consume [--no-commit]  메시지를 받아 RequestFlow(Fake)로 처리. 새 메시지가 없으면 종료

- 그룹에 붙어 있는 소비자가 있으면 reset·consume은 멈춘다 (메시지를 나눠 갖지 않도록)
- 메시지 본문은 출력하지 않는다 (offset·처리 결과·record_card_id만)
- 사용 (WSL, 터널·비밀번호 준비 후, ai-server 루트):
    ~/.venvs/ai-server/bin/python scripts/kafka_consume_check.py status
"""

from __future__ import annotations

import argparse
import contextlib
import io
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tests"))  # 점검용 Fake 재사용

TIMEOUT = 15


def _settings():
    from runiverse_ai_server.config import ConfigError, KafkaSettings, load_settings

    try:
        return load_settings(KafkaSettings)
    except ConfigError as exc:
        raise SystemExit(f"❌ {exc}")


def step(message: str) -> None:
    print(f"  … {message}", flush=True)


def _members(admin_config: dict, group: str) -> tuple[str, list[str] | None]:
    """(상태, 소비자 목록). 조회하지 못하면 목록은 None."""
    from concurrent.futures import TimeoutError as FutureTimeout

    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient

    step("그룹 소비자 조회")
    admin = AdminClient(admin_config)  # 결과를 받을 때까지 살아 있어야 한다 (먼저 정리되면 응답을 못 받음)
    futures = admin.describe_consumer_groups([group], request_timeout=TIMEOUT)
    try:
        desc = futures[group].result(timeout=TIMEOUT + 5)
    except FutureTimeout:
        return f"응답 없음({TIMEOUT + 5}초)", None
    except KafkaException as exc:
        return f"조회 실패({exc.args[0]})", None
    return str(desc.state).split(".")[-1], [f"{m.client_id}@{m.host}" for m in desc.members]


def _offsets(consumer, topic: str) -> list[tuple[int, int, int, int | None]]:
    """(partition, low, high, committed)"""
    from confluent_kafka import TopicPartition

    step("커밋 위치·offset 조회")
    meta = consumer.list_topics(topic, timeout=TIMEOUT).topics[topic]
    parts = sorted(meta.partitions)
    committed = {tp.partition: (tp.offset if tp.offset >= 0 else None)
                 for tp in consumer.committed([TopicPartition(topic, p) for p in parts], timeout=TIMEOUT)}
    rows = []
    for p in parts:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=TIMEOUT)
        rows.append((p, low, high, committed[p]))
    return rows


def _print_status(state: str, members: list[str] | None, rows) -> None:
    who = "확인 불가" if members is None else f"{len(members)}명" + (f" ({', '.join(members)})" if members else "")
    print(f"그룹 상태 {state} · 소비자 {who}")
    for p, low, high, committed in rows:
        print(f"  파티션 {p}: offset {low}~{high} · 커밋 {committed if committed is not None else '없음'}")


class ReportingFlow:
    """RequestFlow(Fake 연결)를 감싸 결과만 한 줄로 출력한다. 로그(JSON)는 숨긴다."""

    def __init__(self, flow, invoker, publisher):
        self.flow, self.invoker, self.publisher = flow, invoker, publisher

    def handle(self, record) -> None:
        calls, published = len(self.invoker.calls), len(self.publisher.messages)
        with contextlib.redirect_stdout(io.StringIO()):
            self.flow.handle(record)
        if len(self.invoker.calls) > calls:
            row = self.flow.repo.get(self.invoker.calls[-1][1])
            result = f"정상 요청 → (Fake) 업로드·호출 · record_card_id={row.record_card_id}"
        elif len(self.publisher.messages) > published:
            msg = self.publisher.messages[-1]
            result = (f"{msg['invoke']['error_code']} → (Fake) 실패 발행 · record_card_id={msg['record_card_id']}"
                      f" · 사유: {msg['invoke']['message']}")
        else:
            result = "건너뜀 (같은 offset 재수신)"
        print(f"  📨 offset {record.offset} (파티션 {record.partition}): {result}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="dev Kafka 소비·커밋 점검 (Fake 처리)")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    sub.add_parser("reset")
    consume = sub.add_parser("consume")
    consume.add_argument("--no-commit", action="store_true", help="처리 후 커밋하지 않음 (재소비 확인용)")
    consume.add_argument("--idle", type=float, default=15, help="새 메시지가 없을 때 종료까지 초")
    for p in sub.choices.values():
        p.add_argument("--skip-member-check", action="store_true",
                       help="그룹 소비자 조회를 건너뜀 (조회가 안 될 때만. 다른 소비자가 없다는 걸 직접 확인한 뒤)")
    args = parser.parse_args(argv)

    from confluent_kafka import Consumer, TopicPartition

    from runiverse_ai_server.adapters.kafka import KafkaRequestSource, consumer_config

    settings = _settings()
    config = consumer_config(settings)
    admin_config = {k: v for k, v in config.items()
                    if k not in ("group.id", "enable.auto.commit", "auto.offset.reset")}
    group, topic = settings.kafka_group_id, settings.request_topic
    print(f"그룹 {group} · 토픽 {topic}", flush=True)

    state, members = ("건너뜀", None) if args.skip_member_check else _members(admin_config, group)
    if args.command != "status":
        if members:
            print(f"❌ 그룹에 소비자 {len(members)}명이 붙어 있습니다 ({', '.join(members)}) → 중단")
            return 1
        if members is None and not args.skip_member_check:
            print(f"❌ 그룹 소비자를 확인하지 못했습니다: {state} → 중단 (--skip-member-check로 건너뛸 수 있음)")
            return 1

    if args.command in ("status", "reset"):
        consumer = Consumer(config)  # subscribe 하지 않음 → 그룹에 참여하지 않는다
        try:
            if args.command == "reset":
                rows = _offsets(consumer, topic)
                consumer.commit(offsets=[TopicPartition(topic, p, low) for p, low, _, _ in rows], asynchronous=False)
                print("✅ 커밋 위치를 맨 앞으로 되돌림")
            _print_status(state, members, _offsets(consumer, topic))
        finally:
            consumer.close()
        return 0

    from fakes import FakeInvoker, FakeObjectStore, FakePublisher, FakeRepository
    from runiverse_ai_server.request_flow import RequestFlow
    from runiverse_ai_server.workers import consume_requests

    invoker, publisher = FakeInvoker(), FakePublisher()
    flow = RequestFlow(store=FakeObjectStore(), invoker=invoker, publisher=publisher, repo=FakeRepository(),
                       input_prefix="async/inputs/local", endpoint_name="fake-endpoint")
    consumer = Consumer(config)
    last_activity = [time.monotonic()]
    deadline = time.monotonic() + 120

    def should_stop() -> bool:
        now = time.monotonic()
        if not consumer.assignment():  # 그룹 참여(파티션 배정) 전
            last_activity[0] = now
        return now - last_activity[0] > args.idle or now > deadline

    class Touch:  # 메시지를 받을 때마다 대기 시간을 다시 센다
        def __init__(self, inner):
            self.inner = inner

        def handle(self, record):
            self.inner.handle(record)
            last_activity[0] = time.monotonic()

    mode = "커밋 안 함" if args.no_commit else "처리 후 커밋"
    print(f"소비 시작 ({mode}, 그룹 참여 후 새 메시지 없으면 {args.idle:.0f}초 뒤 종료)", flush=True)
    try:
        handled = consume_requests(KafkaRequestSource(consumer, topic),
                                   Touch(ReportingFlow(flow, invoker, publisher)),
                                   should_stop=should_stop, commit=not args.no_commit)
    finally:
        consumer.close()  # 그룹에서 바로 빠진다 → 다음 실행이 곧바로 배정받음
    print(f"✅ 처리 {handled}건 ({mode})")

    status_consumer = Consumer(config)
    after = ("건너뜀", None) if args.skip_member_check else _members(admin_config, group)
    try:
        _print_status(*after, _offsets(status_consumer, topic))
    finally:
        status_consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
