"""워커 루프. server.py가 스레드로 돌린다.

요청 워커: poll → RequestFlow.handle → commit. handle이 예외를 내면 커밋하지 않고 예외를 올린다
→ 프로세스가 멈추고, 재시작하면 마지막 커밋 위치부터 다시 받는다 (같은 offset 재수신은 DB로 처리).
"""

from __future__ import annotations

from typing import Callable, Protocol

from .log import log_event
from .ports import KafkaRecord, Notification

POLL_TIMEOUT_SECONDS = 1.0
SQS_WAIT_SECONDS = 20  # long polling


class RequestSource(Protocol):
    def poll(self, timeout: float) -> KafkaRecord | None: ...

    def commit(self, record: KafkaRecord) -> None: ...


class RequestHandler(Protocol):
    def handle(self, record: KafkaRecord) -> None: ...


def consume_requests(source: RequestSource, flow: RequestHandler, *, should_stop: Callable[[], bool],
                     commit: bool = True, poll_timeout: float = POLL_TIMEOUT_SECONDS) -> int:
    """처리한 메시지 수. commit=False는 점검용 (재소비 확인)."""
    handled = 0
    while not should_stop():
        record = source.poll(poll_timeout)
        if record is None:
            continue
        flow.handle(record)
        if commit:
            source.commit(record)
        handled += 1
    return handled


class NotificationSource(Protocol):
    def receive(self, wait_seconds: int) -> list[Notification]: ...


class NotificationHandler(Protocol):
    def handle(self, notification: Notification) -> None: ...


def consume_notifications(queue: NotificationSource, flow: NotificationHandler, *,
                          should_stop: Callable[[], bool], wait_seconds: int = SQS_WAIT_SECONDS) -> int:
    """처리한 알림 수. 한 건이 실패해도 멈추지 않는다 (SQS 메시지를 지우지 않았으니 다시 온다 → 한도 넘으면 DLQ)."""
    handled = 0
    while not should_stop():
        for notification in queue.receive(wait_seconds):
            try:
                flow.handle(notification)
            except Exception as exc:  # noqa: BLE001 — 알림 1건 실패로 워커를 멈추지 않는다
                log_event("completion", "completion_failed", inference_id=getattr(notification, "inference_id", None),
                          level="ERROR", error_type=type(exc).__name__, error=str(exc))
                continue
            handled += 1
    return handled
