"""InferenceRepository의 PostgreSQL 구현. 문장 하나 = 트랜잭션 하나 (autocommit).

연결은 스레드끼리 나눠 쓰지 않는다 → 워커마다 connect()로 하나씩 만든다.
"""

from __future__ import annotations

import uuid

import psycopg

from ..config import DbSettings
from ..ports import DuplicateOffsetError, InferenceRecord, Invocation, InvokeError, UploadError

_COLUMNS = (
    "inference_id::text, user_id, record_card_id, status, endpoint_name, input_s3_key, "
    "kafka_topic, kafka_partition, kafka_offset, failed_stage, output_location, failure_location, "
    "invoke_http_status, invoke_error_code, invoke_message, result_json::text, error_code, error_message, "
    "published_at"
)
_FIELDS = (
    "inference_id", "user_id", "record_card_id", "status", "endpoint_name", "input_s3_key",
    "kafka_topic", "kafka_partition", "kafka_offset", "failed_stage", "output_location", "failure_location",
    "invoke_http_status", "invoke_error_code", "invoke_message", "result_json", "error_code", "error_message",
    "published_at",
)


def connect(settings: DbSettings) -> psycopg.Connection:
    return psycopg.connect(host=settings.db_host, port=settings.db_port, dbname=settings.db_name,
                           user=settings.db_user, password=settings.db_password.get_secret_value(),
                           autocommit=True, connect_timeout=10)


class PostgresRepository:
    def __init__(self, conn: psycopg.Connection):
        self.conn = conn

    def _one(self, where: str, params: list) -> InferenceRecord | None:
        row = self.conn.execute(f"SELECT {_COLUMNS} FROM inference_request WHERE {where}", params).fetchone()
        return None if row is None else InferenceRecord(**dict(zip(_FIELDS, row)))

    def _update(self, inference_id: str, assignments: str, params: list, condition: str = "") -> None:
        self.conn.execute(f"UPDATE inference_request SET {assignments} WHERE inference_id = %s {condition}",
                          [*params, inference_id])

    def find_by_offset(self, topic: str, partition: int, offset: int) -> InferenceRecord | None:
        return self._one("kafka_topic = %s AND kafka_partition = %s AND kafka_offset = %s",
                         [topic, partition, offset])

    def get(self, inference_id: str) -> InferenceRecord | None:
        try:
            uuid.UUID(inference_id)
        except ValueError:  # 다른 시스템의 ID 등. uuid 캐스팅 오류 대신 없음으로 본다
            return None
        return self._one("inference_id = %s", [inference_id])

    def insert_received(self, record: InferenceRecord, input_json: bytes) -> None:
        try:
            self.conn.execute(
                "INSERT INTO inference_request (inference_id, user_id, record_card_id, status, endpoint_name, "
                "input_json, input_s3_key, kafka_topic, kafka_partition, kafka_offset) "
                "VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)",
                [record.inference_id, record.user_id, record.record_card_id, record.status, record.endpoint_name,
                 input_json.decode("utf-8"), record.input_s3_key, record.kafka_topic, record.kafka_partition,
                 record.kafka_offset],
            )
        except psycopg.errors.UniqueViolation as exc:
            raise DuplicateOffsetError(record.kafka_offset) from exc

    def mark_uploaded(self, inference_id: str) -> None:
        self._update(inference_id, "status = 'UPLOADED'", [])

    def mark_invoking(self, inference_id: str) -> None:
        self._update(inference_id, "status = 'INVOKING', invoked_at = now()", [])

    def mark_invoked(self, inference_id: str, invocation: Invocation) -> None:
        self._update(inference_id,
                     "status = 'INVOKED', output_location = %s, failure_location = %s, invoke_http_status = 202",
                     [invocation.output_location, invocation.failure_location],
                     "AND status = 'INVOKING'")  # 완료가 먼저 기록됐으면 되돌리지 않는다

    def mark_invoke_failed(self, inference_id: str, error: UploadError | InvokeError) -> None:
        self._update(inference_id,
                     "status = 'FAILED', failed_stage = 'INVOKE', invoke_http_status = %s, "
                     "invoke_error_code = %s, invoke_message = %s, completed_at = now()",
                     [getattr(error, "http_status", None), error.error_code, error.message])

    def mark_completed(self, inference_id: str, status: str, result_json: str | None,
                       error_code: str | None, error_message: str | None) -> None:
        self._update(inference_id,
                     "status = %s, failed_stage = %s, result_json = %s::jsonb, error_code = %s, "
                     "error_message = %s, completed_at = now()",
                     [status, None if status == "SUCCEEDED" else "INFERENCE", result_json, error_code, error_message])

    def mark_published(self, inference_id: str) -> None:
        self._update(inference_id, "published_at = now()", [], "AND published_at IS NULL")
