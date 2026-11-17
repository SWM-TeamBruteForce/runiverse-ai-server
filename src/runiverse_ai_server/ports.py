"""외부 시스템 경계(Protocol). 흐름은 이것만 의존한다 → 테스트는 tests/fakes.py로 돈다."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol


@dataclass(frozen=True)
class KafkaRecord:
    topic: str
    partition: int
    offset: int
    value: bytes


@dataclass(frozen=True)
class Invocation:
    """202 응답 헤더의 결과 위치."""

    output_location: str
    failure_location: str | None


@dataclass(frozen=True)
class InferenceRecord:
    """inference_request 행 (input_json 제외)."""

    inference_id: str
    user_id: str  # 백엔드 UUID 문자열
    record_card_id: str
    status: str  # RECEIVED·UPLOADED·INVOKING·INVOKED·SUCCEEDED·FAILED
    endpoint_name: str
    input_s3_key: str
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    failed_stage: str | None = None  # INVOKE·INFERENCE
    output_location: str | None = None
    failure_location: str | None = None
    invoke_http_status: int | None = None
    invoke_error_code: str | None = None
    invoke_message: str | None = None
    result_json: str | None = None  # 컨테이너 결과 파일 원문 (JSON일 때만)
    error_code: str | None = None
    error_message: str | None = None
    invoked_at: datetime | None = None  # INVOKING 기록 시각 (UTC). 응답 없음 판단 기준
    published_at: datetime | None = None


class UploadError(Exception):
    """S3 입력 업로드 실패. SageMaker 응답이 없으므로 http_status는 없다."""

    def __init__(self, error_code: str, message: str):
        super().__init__(f"{error_code}: {message}")
        self.error_code = error_code
        self.message = message


class InvokeError(Exception):
    """InvokeEndpointAsync 실패. 거부면 http_status(400·500·503), 응답이 없으면 None."""

    def __init__(self, http_status: int | None, error_code: str, message: str):
        super().__init__(f"{http_status} {error_code}: {message}")
        self.http_status = http_status
        self.error_code = error_code
        self.message = message


class DuplicateOffsetError(Exception):
    """같은 (topic, partition, offset) 행이 이미 있다."""


class PublishError(Exception):
    """완료 메시지 발행 확인 실패."""


class ObjectStore(Protocol):
    def put(self, key: str, body: bytes) -> str:
        """업로드 후 s3:// URI. 실패하면 UploadError."""

    def read_text(self, uri: str) -> str | None:
        """객체 내용. 없으면 None."""


class AsyncInvoker(Protocol):
    def invoke(self, input_uri: str, inference_id: str) -> Invocation:
        """202면 Invocation, 아니면 InvokeError. 재시도하지 않는다."""


class Publisher(Protocol):
    def publish(self, message: dict[str, Any]) -> None:
        """브로커 저장 확인까지 기다린다. 실패하면 PublishError."""


class InferenceRepository(Protocol):
    def find_by_offset(self, topic: str, partition: int, offset: int) -> InferenceRecord | None: ...

    def get(self, inference_id: str) -> InferenceRecord | None: ...

    def list_pending(self, limit: int) -> list[InferenceRecord]:
        """완료 발행 전인 행: INVOKING·INVOKED, 그리고 완료 저장 후 발행 못 한 SUCCEEDED·FAILED(INFERENCE).
        요청 흐름이 맡는 RECEIVED·UPLOADED·FAILED(INVOKE)는 제외. 오래된 호출부터."""

    def insert_received(self, record: InferenceRecord, input_json: bytes) -> None:
        """같은 offset이 있으면 DuplicateOffsetError."""

    def mark_uploaded(self, inference_id: str) -> None: ...

    def mark_invoking(self, inference_id: str) -> None:
        """호출 직전. invoked_at도 이때 남긴다 (무응답 점검 기준). 이 상태로 재수신하면 다시 호출하지 않는다."""

    def mark_invoked(self, inference_id: str, invocation: Invocation) -> None:
        """INVOKING일 때만 INVOKED로. 완료가 먼저 기록됐으면 되돌리지 않는다."""

    def mark_invoke_failed(self, inference_id: str, error: UploadError | InvokeError) -> None: ...

    def mark_completed(self, inference_id: str, status: str, result_json: str | None,
                       error_code: str | None, error_message: str | None) -> None: ...

    def mark_published(self, inference_id: str) -> None:
        """이미 있으면 처음 시각을 유지한다."""
