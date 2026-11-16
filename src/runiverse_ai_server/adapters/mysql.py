"""InferenceRepository의 MySQL 8.4 구현 (PyMySQL). 문장 하나 = 트랜잭션 하나 (autocommit).

- 연결은 스레드끼리 나눠 쓰지 않는다 → 워커마다 connect()로 하나씩 만든다
- 시각은 UTC: 연결마다 time_zone = '+00:00', 읽을 때 UTC tzinfo를 붙인다
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import pymysql

from ..config import DbSettings
from ..ports import DuplicateOffsetError, InferenceRecord, Invocation, InvokeError, UploadError

_DUPLICATE_ENTRY = 1062
_FIELDS = (
    "inference_id", "user_id", "record_card_id", "status", "endpoint_name", "input_s3_key",
    "kafka_topic", "kafka_partition", "kafka_offset", "failed_stage", "output_location", "failure_location",
    "invoke_http_status", "invoke_error_code", "invoke_message", "result_json", "error_code", "error_message",
    "invoked_at", "published_at",
)
_COLUMNS = ", ".join(_FIELDS)


def connect_params(*, host: str, port: int, user: str, password: str, database: str) -> dict[str, Any]:
    return {"host": host, "port": port, "user": user, "password": password, "database": database,
            "charset": "utf8mb4", "autocommit": True, "connect_timeout": 10,
            "init_command": "SET time_zone = '+00:00'"}


def connect(settings: DbSettings) -> pymysql.connections.Connection:
    return pymysql.connect(**connect_params(host=settings.db_host, port=settings.db_port, user=settings.db_user,
                                            password=settings.db_password.get_secret_value(),
                                            database=settings.db_name))


def _utc(value: datetime | None) -> datetime | None:
    return None if value is None else value.replace(tzinfo=timezone.utc)


class MySQLRepository:
    def __init__(self, conn: Any):
        self.conn = conn

    def _execute(self, sql: str, params: list) -> Any:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()

    @staticmethod
    def _record(row: tuple) -> InferenceRecord:
        values = dict(zip(_FIELDS, row))
        values["invoked_at"] = _utc(values["invoked_at"])
        values["published_at"] = _utc(values["published_at"])
        return InferenceRecord(**values)

    def _one(self, where: str, params: list) -> InferenceRecord | None:
        row = self._execute(f"SELECT {_COLUMNS} FROM inference_request WHERE {where}", params)
        return None if row is None else self._record(row)

    def _update(self, inference_id: str, assignments: str, params: list, condition: str = "") -> None:
        self._execute(f"UPDATE inference_request SET {assignments} WHERE inference_id = %s {condition}",
                      [*params, inference_id])

    def find_by_offset(self, topic: str, partition: int, offset: int) -> InferenceRecord | None:
        return self._one("kafka_topic = %s AND kafka_partition = %s AND kafka_offset = %s",
                         [topic, partition, offset])

    def get(self, inference_id: str) -> InferenceRecord | None:
        try:
            uuid.UUID(inference_id)
        except ValueError:  # 다른 시스템의 ID 등 → 없음
            return None
        return self._one("inference_id = %s", [inference_id])

    def list_pending(self, limit: int) -> list[InferenceRecord]:
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT {_COLUMNS} FROM inference_request WHERE published_at IS NULL AND ("
                "status IN ('INVOKING', 'INVOKED', 'SUCCEEDED') OR (status = 'FAILED' AND failed_stage = 'INFERENCE')) "
                "ORDER BY invoked_at LIMIT %s", [limit])
            return [self._record(row) for row in cur.fetchall()]

    def insert_received(self, record: InferenceRecord, input_json: bytes) -> None:
        try:
            self._execute(
                "INSERT INTO inference_request (inference_id, user_id, record_card_id, status, endpoint_name, "
                "input_json, input_s3_key, kafka_topic, kafka_partition, kafka_offset) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                [record.inference_id, record.user_id, record.record_card_id, record.status, record.endpoint_name,
                 input_json.decode("utf-8"), record.input_s3_key, record.kafka_topic, record.kafka_partition,
                 record.kafka_offset],
            )
        except pymysql.err.IntegrityError as exc:
            if exc.args and exc.args[0] == _DUPLICATE_ENTRY:
                raise DuplicateOffsetError(record.kafka_offset) from exc
            raise

    def mark_uploaded(self, inference_id: str) -> None:
        self._update(inference_id, "status = 'UPLOADED'", [])

    def mark_invoking(self, inference_id: str) -> None:
        self._update(inference_id, "status = 'INVOKING', invoked_at = CURRENT_TIMESTAMP(6)", [])

    def mark_invoked(self, inference_id: str, invocation: Invocation) -> None:
        self._update(inference_id,
                     "status = 'INVOKED', output_location = %s, failure_location = %s, invoke_http_status = 202",
                     [invocation.output_location, invocation.failure_location],
                     "AND status = 'INVOKING'")  # 완료가 먼저 기록됐으면 되돌리지 않는다

    def mark_invoke_failed(self, inference_id: str, error: UploadError | InvokeError) -> None:
        self._update(inference_id,
                     "status = 'FAILED', failed_stage = 'INVOKE', invoke_http_status = %s, "
                     "invoke_error_code = %s, invoke_message = %s, completed_at = CURRENT_TIMESTAMP(6)",
                     [getattr(error, "http_status", None), error.error_code, error.message])

    def mark_completed(self, inference_id: str, status: str, result_json: str | None,
                       error_code: str | None, error_message: str | None) -> None:
        self._update(inference_id,
                     "status = %s, failed_stage = %s, result_json = %s, error_code = %s, "
                     "error_message = %s, completed_at = CURRENT_TIMESTAMP(6)",
                     [status, None if status == "SUCCEEDED" else "INFERENCE", result_json, error_code, error_message])

    def mark_published(self, inference_id: str) -> None:
        self._update(inference_id, "published_at = CURRENT_TIMESTAMP(6)", [], "AND published_at IS NULL")
