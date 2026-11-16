"""흐름 ②: SageMaker 완료 알림 1건 → 결과/실패 파일 읽기 → DB 저장 → Kafka 발행 → SQS 삭제 (설계 4.2).

발행에 실패하면 예외를 올리고 SQS 메시지를 지우지 않는다 → 가시성 타임아웃 후 다시 받는다 (추론 재실행 아님).
"""

from __future__ import annotations

import json

from . import messages
from .log import log_event
from .ports import InferenceRepository, Notification, NotificationQueue, ObjectStore, Publisher

STAGE = "completion"


def _json_or_none(body: str | None) -> str | None:
    """jsonb에 넣을 수 있는 원문만. JSON이 아니면 None (내용은 error_message에 남는다)."""
    if body is None:
        return None
    try:
        json.loads(body)
    except json.JSONDecodeError:
        return None
    return body


class CompletionFlow:
    def __init__(self, *, store: ObjectStore, publisher: Publisher, repo: InferenceRepository,
                 queue: NotificationQueue):
        self.store = store
        self.publisher = publisher
        self.repo = repo
        self.queue = queue

    def handle(self, notification: Notification) -> None:
        row = self.repo.get(notification.inference_id)
        if row is None:  # 다른 환경의 알림 등
            log_event(STAGE, "unknown_inference_id", inference_id=notification.inference_id, level="ERROR")
            self.queue.delete(notification)
            return

        ids = {"inference_id": row.inference_id, "record_card_id": row.record_card_id}
        if row.published_at is not None:
            log_event(STAGE, "already_published", **ids)
            self.queue.delete(notification)
            return
        if row.output_location is None:  # 202 기록 전에 알림이 먼저 왔다
            log_event(STAGE, "not_invoked_yet", status=row.status, level="WARNING", **ids)
            return

        if notification.succeeded:
            body = self.store.read_text(row.output_location)
            try:
                if body is None:
                    raise messages.ResultFormatError("성공 결과 파일이 없습니다")
                message = messages.succeeded(row.inference_id, row.record_card_id, body)
            except messages.ResultFormatError as exc:
                log_event(STAGE, "unusable_output", level="ERROR", reason=str(exc), **ids)
                message = messages.unusable_output(row.inference_id, row.record_card_id, str(exc))
        else:
            body = self.store.read_text(row.failure_location) if row.failure_location else None
            message = messages.inference_failed(row.inference_id, row.record_card_id, body)

        error = (message["result"] or {}).get("error")
        self.repo.mark_completed(row.inference_id, message["status"], _json_or_none(body),
                                 error and error["code"], error and error["message"])
        log_event(STAGE, "completed", status=message["status"], error_code=error and error["code"], **ids)

        self.publisher.publish(message)
        self.repo.mark_published(row.inference_id)
        log_event(STAGE, "published", status=message["status"], **ids)
        self.queue.delete(notification)
