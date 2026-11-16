"""워커 스레드 2개(요청·완료)를 실행하고 멈춘다.

- 요청 워커 오류 → 서버 전체 중지 (종료 코드 1). 재시작하면 Kafka 커밋 위치부터 다시 받는다
- 완료 워커는 알림 1건 실패로 멈추지 않는다 (workers.consume_notifications)
- stop(): 처리 중인 1건을 끝낸 뒤 멈춘다 → Kafka 소비자 닫기, 발행 대기분 flush
"""

from __future__ import annotations

import threading
from typing import Any

from .log import log_event
from .workers import POLL_TIMEOUT_SECONDS, SQS_WAIT_SECONDS, consume_notifications, consume_requests

STAGE = "server"


class Server:
    def __init__(self, request_source: Any, request_flow: Any, notification_queue: Any, completion_flow: Any,
                 publisher: Any, *, poll_timeout: float = POLL_TIMEOUT_SECONDS,
                 wait_seconds: int = SQS_WAIT_SECONDS):
        self.request_source = request_source
        self.request_flow = request_flow
        self.notification_queue = notification_queue
        self.completion_flow = completion_flow
        self.publisher = publisher
        self.poll_timeout = poll_timeout
        self.wait_seconds = wait_seconds
        self._stop = threading.Event()
        self._failed = threading.Event()
        self._threads: list[threading.Thread] = []

    @property
    def running(self) -> bool:
        return any(t.is_alive() for t in self._threads) and not self._stop.is_set()

    def start(self) -> None:
        self._threads = [
            threading.Thread(target=self._guard, args=("request_worker", self._run_requests), name="request"),
            threading.Thread(target=self._guard, args=("completion_worker", self._run_completions),
                             name="completion"),
        ]
        for thread in self._threads:
            thread.start()
        log_event(STAGE, "started")

    def stop(self) -> None:
        if not self._stop.is_set():
            log_event(STAGE, "stopping")
        self._stop.set()

    def wait(self, timeout: float | None = None) -> int:
        """스레드가 끝날 때까지 기다리고 정리한다. 종료 코드: 정상 0, 워커 오류 1."""
        for thread in self._threads:
            thread.join(timeout)
        self._close()
        code = 1 if self._failed.is_set() else 0
        log_event(STAGE, "stopped", exit_code=code)
        return code

    def _run_requests(self) -> None:
        consume_requests(self.request_source, self.request_flow, should_stop=self._stop.is_set,
                         poll_timeout=self.poll_timeout)

    def _run_completions(self) -> None:
        consume_notifications(self.notification_queue, self.completion_flow, should_stop=self._stop.is_set,
                              wait_seconds=self.wait_seconds)

    def _guard(self, name: str, target) -> None:
        try:
            target()
        except Exception as exc:  # noqa: BLE001 — 어느 워커든 죽으면 서버 전체를 멈춘다
            log_event(STAGE, "worker_failed", level="ERROR", worker=name, error_type=type(exc).__name__,
                      error=str(exc))
            self._failed.set()
            self.stop()

    def _close(self) -> None:
        for part, name in ((self.request_source, "request_source"), (self.publisher, "publisher")):
            try:
                part.close()
            except Exception as exc:  # noqa: BLE001 — 정리 실패는 기록만
                log_event(STAGE, "close_failed", level="ERROR", part=name, error=str(exc))
