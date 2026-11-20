"""포트별 메모리 Fake. AWS·Kafka·DB 없이 두 흐름을 테스트한다."""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone

from runiverse_ai_server.ports import (
    DuplicateOffsetError,
    Invocation,
    InferenceRecord,
    InvokeError,
    PublishError,
    UploadError,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class FakeObjectStore:
    def __init__(self, bucket: str = "bucket"):
        self.bucket = bucket
        self.objects: dict[str, bytes] = {}
        self.fail_put: UploadError | None = None

    def put(self, key: str, body: bytes) -> str:
        if self.fail_put:
            raise self.fail_put
        self.objects[f"s3://{self.bucket}/{key}"] = body
        return f"s3://{self.bucket}/{key}"

    def read_text(self, uri: str) -> str | None:
        body = self.objects.get(uri)
        return None if body is None else body.decode("utf-8")


class FakeInvoker:
    def __init__(self):
        self.calls: list[tuple[str, str]] = []
        self.error: InvokeError | None = None
        self.on_call = lambda inference_id: None

    def invoke(self, input_uri: str, inference_id: str) -> Invocation:
        self.calls.append((input_uri, inference_id))
        self.on_call(inference_id)
        if self.error:
            raise self.error
        return Invocation(output_location=f"s3://bucket/async/outputs/test/{inference_id}.out",
                          failure_location=f"s3://bucket/async/failures/test/{inference_id}-error.out")


class FakePublisher:
    def __init__(self):
        self.messages: list[dict] = []
        self.fail = False

    def publish(self, message: dict) -> None:
        if self.fail:
            raise PublishError("broker down")
        self.messages.append(message)


class FakeRepository:
    """상태 전이 규칙은 MySQL 구현과 같아야 한다 (test_repository_contract.py)."""

    def __init__(self):
        self.rows: dict[str, InferenceRecord] = {}
        self.input_json: dict[str, bytes] = {}

    def _update(self, inference_id: str, **changes) -> None:
        self.rows[inference_id] = dataclasses.replace(self.rows[inference_id], **changes)

    def find_by_offset(self, topic: str, partition: int, offset: int) -> InferenceRecord | None:
        for row in self.rows.values():
            if (row.kafka_topic, row.kafka_partition, row.kafka_offset) == (topic, partition, offset):
                return row
        return None

    def get(self, inference_id: str) -> InferenceRecord | None:
        return self.rows.get(inference_id)

    def get_input_json(self, inference_id: str) -> str | None:
        body = self.input_json.get(inference_id)
        return None if body is None else body.decode("utf-8")

    def list_pending(self, limit: int) -> list[InferenceRecord]:
        rows = [r for r in self.rows.values() if r.published_at is None and (
            r.status in ("INVOKING", "INVOKED", "SUCCEEDED") or (r.status == "FAILED" and r.failed_stage == "INFERENCE"))]
        return sorted(rows, key=lambda r: r.invoked_at)[:limit]

    def insert_received(self, record: InferenceRecord, input_json: bytes) -> None:
        if self.find_by_offset(record.kafka_topic, record.kafka_partition, record.kafka_offset):
            raise DuplicateOffsetError(record.kafka_offset)
        self.rows[record.inference_id] = record
        self.input_json[record.inference_id] = input_json

    def mark_uploaded(self, inference_id: str) -> None:
        self._update(inference_id, status="UPLOADED")

    def mark_invoking(self, inference_id: str) -> None:
        self._update(inference_id, status="INVOKING", invoked_at=_now())

    def mark_invoked(self, inference_id: str, invocation: Invocation) -> None:
        if self.rows[inference_id].status == "INVOKING":  # 완료가 먼저 기록됐으면 되돌리지 않는다
            self._update(inference_id, status="INVOKED", output_location=invocation.output_location,
                         failure_location=invocation.failure_location, invoke_http_status=202)

    def mark_invoke_failed(self, inference_id: str, error: UploadError | InvokeError) -> None:
        self._update(inference_id, status="FAILED", failed_stage="INVOKE",
                     invoke_http_status=getattr(error, "http_status", None),
                     invoke_error_code=error.error_code, invoke_message=error.message)

    def mark_completed(self, inference_id: str, status: str, result_json: str | None,
                       error_code: str | None, error_message: str | None) -> None:
        self._update(inference_id, status=status, failed_stage=None if status == "SUCCEEDED" else "INFERENCE",
                     result_json=result_json, error_code=error_code, error_message=error_message)

    def mark_published(self, inference_id: str) -> None:
        if self.rows[inference_id].published_at is None:
            self._update(inference_id, published_at=_now())
