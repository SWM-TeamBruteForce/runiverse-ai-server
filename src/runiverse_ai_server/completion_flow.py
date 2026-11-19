"""흐름 ②: 발행 전 요청 1건의 S3 결과/실패 파일 확인 → DB 저장 → Kafka 발행 (설계 4.2).

2026-11-16: SNS·SQS 대신 202 응답의 결과 위치를 주기적으로 확인한다.
- OutputLocation 있음 → 성공 / FailureLocation 있음 → 실패 / 둘 다 없음 → 기다림
- 제한 시간(SageMaker가 요청을 포기하는 시점)까지 둘 다 없음 → INFERENCE_FAILED (응답 없음)
- 발행 실패 → 예외. published_at이 비어 있으니 다음 확인 때 다시 발행한다 (추론 재실행 아님)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Callable

from . import messages
from .log import log_event
from .ports import InferenceRecord, InferenceRepository, ObjectStore, Publisher

STAGE = "completion"
PENDING_LIMIT = 100


def _json_or_none(body: str | None) -> str | None:
    """JSON 컬럼에 넣을 수 있는 원문만. JSON이 아니면 None (내용은 error_message에 남는다)."""
    if body is None:
        return None
    try:
        json.loads(body)
    except json.JSONDecodeError:
        return None
    return body


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class CompletionFlow:
    def __init__(self, *, store: ObjectStore, publisher: Publisher, repo: InferenceRepository,
                 response_deadline_seconds: int, clock: Callable[[], datetime] = _utc_now):
        self.store = store
        self.publisher = publisher
        self.repo = repo
        self.response_deadline_seconds = response_deadline_seconds
        self.clock = clock

    def pending(self) -> list[InferenceRecord]:
        return self.repo.list_pending(limit=PENDING_LIMIT)

    def _photo_ids(self, row: InferenceRecord) -> list:
        """실패 메시지에 넣을 요청의 사진 ID (DB input_json에서)."""
        return messages.photo_image_ids(self.repo.get_input_json(row.inference_id))

    def check(self, row: InferenceRecord) -> bool:
        """발행했으면 True, 아직 결과가 없으면 False."""
        if row.published_at is not None:
            return False
        ids = {"inference_id": row.inference_id, "record_card_id": row.record_card_id}

        output = self.store.read_text(row.output_location) if row.output_location else None
        failure = None
        if output is None and row.failure_location:
            failure = self.store.read_text(row.failure_location)

        if output is not None:
            body = output
            try:
                message = messages.succeeded(row.inference_id, row.record_card_id, output)
            except messages.ResultFormatError as exc:
                log_event(STAGE, "unusable_output", level="ERROR", reason=str(exc), **ids)
                message = messages.unusable_output(row.inference_id, row.record_card_id, str(exc),
                                                   photo_ids=self._photo_ids(row))
        elif failure is not None:
            body = failure
            message = messages.inference_failed(row.inference_id, row.record_card_id, failure,
                                                photo_ids=self._photo_ids(row))
        else:
            waited = int((self.clock() - row.invoked_at).total_seconds()) if row.invoked_at else 0
            if waited <= self.response_deadline_seconds:
                return False
            log_event(STAGE, "no_response", level="ERROR", status=row.status, waited_seconds=waited, **ids)
            body = None
            message = messages.no_response(row.inference_id, row.record_card_id, waited,
                                           photo_ids=self._photo_ids(row))

        error = (message["result"] or {}).get("error")
        self.repo.mark_completed(row.inference_id, message["status"], _json_or_none(body),
                                 error and error["code"], error and error["message"])
        log_event(STAGE, "completed", status=message["status"], error_code=error and error["code"], **ids)

        self.publisher.publish(message)
        self.repo.mark_published(row.inference_id)
        log_event(STAGE, "published", status=message["status"], **ids)
        return True
