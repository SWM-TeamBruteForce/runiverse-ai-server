"""흐름 ①: Kafka 요청 1건 → S3 업로드 → InvokeEndpointAsync (설계 4.1).

handle()이 정상 반환하면 호출한 쪽이 오프셋을 커밋한다. 예외(DB·발행 실패)면 커밋하지 않는다.
"""

from __future__ import annotations

import uuid
from typing import Callable

from . import messages
from .log import log_event
from .ports import (
    AsyncInvoker,
    InferenceRecord,
    InferenceRepository,
    InvokeError,
    KafkaRecord,
    ObjectStore,
    Publisher,
    UploadError,
)

STAGE = "request"


class RequestFlow:
    def __init__(self, *, store: ObjectStore, invoker: AsyncInvoker, publisher: Publisher,
                 repo: InferenceRepository, input_prefix: str, endpoint_name: str,
                 new_id: Callable[[], str] = lambda: str(uuid.uuid4())):
        self.store = store
        self.invoker = invoker
        self.publisher = publisher
        self.repo = repo
        self.input_prefix = input_prefix
        self.endpoint_name = endpoint_name
        self.new_id = new_id

    def handle(self, record: KafkaRecord) -> None:
        kafka = {"kafka_topic": record.topic, "kafka_partition": record.partition, "kafka_offset": record.offset}
        parsed = messages.parse_request(record.value)
        if isinstance(parsed, messages.InvalidMessage):
            log_event(STAGE, "invalid_message", record_card_id=parsed.record_card_id, level="WARNING",
                      reason=parsed.reason, **kafka)
            self.publisher.publish(messages.invalid_message(parsed))
            return

        row = self.repo.find_by_offset(record.topic, record.partition, record.offset)
        if row is None:
            inference_id = self.new_id()
            row = InferenceRecord(
                inference_id=inference_id, user_id=parsed.user_id, record_card_id=parsed.record_card_id,
                status="RECEIVED", endpoint_name=self.endpoint_name,
                input_s3_key=messages.input_key(self.input_prefix, parsed.record_card_id, inference_id), **kafka,
            )
            self.repo.insert_received(row, parsed.body)
            log_event(STAGE, "received", inference_id=row.inference_id, record_card_id=row.record_card_id, **kafka)
        elif not self._resume(row):
            return

        self._upload_and_invoke(row, parsed.body)

    def _resume(self, row: InferenceRecord) -> bool:
        """같은 offset 재수신. True면 업로드부터 이어서 처리한다."""
        ids = {"inference_id": row.inference_id, "record_card_id": row.record_card_id}
        if row.status in ("RECEIVED", "UPLOADED"):
            log_event(STAGE, "redelivered_resume", status=row.status, **ids)
            return True
        if row.status == "FAILED" and row.failed_stage == "INVOKE" and row.published_at is None:
            log_event(STAGE, "redelivered_republish", **ids)
            self._publish(row.inference_id, messages.invoke_failed(
                row.inference_id, row.record_card_id, row.invoke_http_status, row.invoke_error_code,
                row.invoke_message))
            return False
        # INVOKING: 호출됐는지 모른다 → 다시 부르지 않고 알림 또는 무응답 점검(설계 4.3)에 맡긴다
        log_event(STAGE, "redelivered_skip", status=row.status, **ids)
        return False

    def _upload_and_invoke(self, row: InferenceRecord, body: bytes) -> None:
        ids = {"inference_id": row.inference_id, "record_card_id": row.record_card_id}
        try:
            input_uri = self.store.put(row.input_s3_key, body)
            self.repo.mark_uploaded(row.inference_id)
            log_event(STAGE, "uploaded", input_s3_key=row.input_s3_key, **ids)
            # 호출 직후 죽으면 202를 기록하지 못한다 → 먼저 남겨서 재시작 후 다시 호출하지 않게 한다 (많아야 1번)
            self.repo.mark_invoking(row.inference_id)
            invocation = self.invoker.invoke(input_uri, row.inference_id)
        except (UploadError, InvokeError) as error:
            http_status = getattr(error, "http_status", None)
            log_event(STAGE, "invoke_failed", level="ERROR", http_status=http_status,
                      error_code=error.error_code, error_message=error.message, **ids)
            self.repo.mark_invoke_failed(row.inference_id, error)
            self._publish(row.inference_id, messages.invoke_failed(
                row.inference_id, row.record_card_id, http_status, error.error_code, error.message))
            return

        self.repo.mark_invoked(row.inference_id, invocation)
        log_event(STAGE, "invoked", output_location=invocation.output_location,
                  failure_location=invocation.failure_location, **ids)

    def _publish(self, inference_id: str, message: messages.CompletionMessage) -> None:
        self.publisher.publish(message)
        self.repo.mark_published(inference_id)
        log_event(STAGE, "published", inference_id=inference_id, record_card_id=message["record_card_id"],
                  status=message["status"], failed_stage=message["failed_stage"])
