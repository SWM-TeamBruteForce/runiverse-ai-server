"""워커 루프. server.py가 스레드로 돌린다.

완료 워커: 발행 전 요청의 결과 파일을 주기적으로 확인 (CompletionFlow.pending → check).

요청 워커: poll → RequestFlow.handle → commit. handle이 예외를 내면 커밋하지 않고 예외를 올린다
→ 프로세스가 멈추고, 재시작하면 마지막 커밋 위치부터 다시 받는다 (같은 offset 재수신은 DB로 처리).
"""

from __future__ import annotations

from typing import Callable, Protocol

from .log import log_event
from .ports import InferenceRecord, KafkaRecord

POLL_TIMEOUT_SECONDS = 1.0
COMPLETION_POLL_SECONDS = 10  # 결과 파일 확인 간격


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


class CompletionChecker(Protocol):
    def pending(self) -> list[InferenceRecord]: ...

    def check(self, row: InferenceRecord) -> bool: ...


def poll_completions(flow: CompletionChecker, *, should_stop: Callable[[], bool], wait: Callable[[float], object],
                     interval: float = COMPLETION_POLL_SECONDS) -> int:
    """확인 횟수(라운드). 한 건·한 라운드가 실패해도 멈추지 않는다 (published_at이 비어 있으니 다음 라운드에 다시)."""
    rounds = 0
    while not should_stop():
        try:
            rows = flow.pending()
        except Exception as exc:  # noqa: BLE001 — DB 일시 장애 등. 다음 라운드에 다시
            log_event("completion", "pending_failed", level="ERROR", error_type=type(exc).__name__, error=str(exc))
            rows = []
        for row in rows:  # 한 라운드는 최대 PENDING_LIMIT건 → 끝까지 돌고 멈춤 여부를 본다
            try:
                flow.check(row)
            except Exception as exc:  # noqa: BLE001 — 1건 실패로 워커를 멈추지 않는다
                log_event("completion", "completion_failed", inference_id=getattr(row, "inference_id", None),
                          level="ERROR", error_type=type(exc).__name__, error=str(exc))
        rounds += 1
        wait(interval)
    return rounds
