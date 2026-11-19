"""순수 함수: 요청 파싱, S3 입력 키, 완료 메시지 조립. 형식 정본은 Notion「AI 기능 아키텍처 및 data 입출력」3장·4.1.

- 요청 본문은 해석·수정하지 않는다. user_id·record_card_id 존재(UUID 문자열)만 본다
- 결과는 정해진 필드만 옮긴다. 값을 바꾸거나 새로 계산하지 않는다
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from typing import Any

INVALID_MESSAGE = "INVALID_MESSAGE"
INFERENCE_FAILED = "INFERENCE_FAILED"
NO_FAILURE_FILE = "실패 결과 파일이 없습니다"

CompletionMessage = dict[str, Any]


@dataclass(frozen=True)
class ParsedRequest:
    user_id: str
    record_card_id: str
    body: bytes  # 받은 바이트 그대로. S3에 이 값을 올린다


@dataclass(frozen=True)
class InvalidMessage:
    record_card_id: str | None
    reason: str  # 무엇이 잘못됐는지만. 본문 내용은 넣지 않는다


class ResultFormatError(ValueError):
    """성공 결과 파일이 계약(4.2.1) 형식이 아니다."""


# 백엔드 ID는 UUID 문자열 (2026-11-17). S3 경로·DB CHAR(36)에 들어가므로 모양만 확인하고 값은 그대로 쓴다
_UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")


def _uuid_text(value: Any) -> str | None:
    if isinstance(value, str) and _UUID.fullmatch(value):
        return value
    return None


def parse_request(body: bytes) -> ParsedRequest | InvalidMessage:
    try:
        data = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return InvalidMessage(record_card_id=None, reason="본문이 JSON이 아닙니다")
    if not isinstance(data, dict):
        return InvalidMessage(record_card_id=None, reason="본문이 JSON 객체가 아닙니다")

    record_card_id = _uuid_text(data.get("record_card_id"))
    user_id = _uuid_text(data.get("user_id"))
    missing = [name for name, value in (("user_id", user_id), ("record_card_id", record_card_id)) if value is None]
    if missing:
        return InvalidMessage(record_card_id=record_card_id,
                              reason=f"{'·'.join(missing)}가 없거나 UUID 문자열이 아닙니다")
    return ParsedRequest(user_id=user_id, record_card_id=record_card_id, body=body)


def input_key(input_prefix: str, record_card_id: str, inference_id: str) -> str:
    return f"{input_prefix}/{record_card_id}/{inference_id}.json"


def _message(inference_id: str | None, record_card_id: str | None, status: str, failed_stage: str | None,
             invoke: dict[str, Any], result: dict[str, Any] | None) -> CompletionMessage:
    return {
        "inference_id": inference_id,
        "record_card_id": record_card_id,
        "status": status,
        "failed_stage": failed_stage,
        "invoke": invoke,
        "result": result,
    }


def photo_image_ids(body: bytes | str | None) -> list[Any]:
    """요청의 photos[].record_card_image_id (받은 값 그대로). 읽을 수 없으면 []. 본문 다른 내용은 보지 않는다."""
    try:
        data = json.loads(body)
    except (TypeError, UnicodeDecodeError, json.JSONDecodeError):
        return []
    photos = data.get("photos") if isinstance(data, dict) else None
    if not isinstance(photos, list):
        return []
    return [photo.get("record_card_image_id") for photo in photos if isinstance(photo, dict)]


def _failure_result(photo_ids: list[Any], code: str, message: str | None) -> dict[str, Any]:
    """실패도 성공 양식에 null을 채우고 error를 붙인다 (2026-11-19 백엔드 요청)."""
    return {
        "base_music": {"s3_key": None, "duration_seconds": None},
        "photo_music": [{"record_card_image_id": photo_id, "s3_key": None, "duration_seconds": None}
                        for photo_id in photo_ids],
        "error": {"code": code, "message": message},
    }


def _accepted() -> dict[str, Any]:
    return {"http_status": 202, "error_code": None, "message": None}


def _music(item: Any, fields: tuple[str, ...]) -> dict[str, Any]:
    if not isinstance(item, dict) or any(name not in item for name in fields):
        raise ResultFormatError(f"{'·'.join(fields)} 중 없는 값이 있습니다")
    return {name: item[name] for name in fields}


def succeeded(inference_id: str, record_card_id: str, output_body: str) -> CompletionMessage:
    try:
        output = json.loads(output_body)
    except json.JSONDecodeError:
        raise ResultFormatError("성공 결과가 JSON이 아닙니다") from None
    if not isinstance(output, dict) or not isinstance(output.get("photo_music"), list):
        raise ResultFormatError("성공 결과에 photo_music 목록이 없습니다")

    result = {
        "base_music": _music(output.get("base_music"), ("s3_key", "duration_seconds")),
        "photo_music": [_music(item, ("record_card_image_id", "s3_key", "duration_seconds"))
                        for item in output["photo_music"]],
    }
    return _message(inference_id, record_card_id, "SUCCEEDED", None, _accepted(), result)


def _inference_failure(inference_id: str, record_card_id: str, photo_ids: list[Any],
                       code: str, message: str | None) -> CompletionMessage:
    return _message(inference_id, record_card_id, "FAILED", "INFERENCE", _accepted(),
                    _failure_result(photo_ids, code, message))


def inference_failed(inference_id: str, record_card_id: str, failure_body: str | None, *,
                     photo_ids: list[Any]) -> CompletionMessage:
    """failure_body: FailureLocation 파일 내용. 파일이 없으면 None. photo_ids: photo_image_ids(요청 본문)."""
    if failure_body is None:
        return _inference_failure(inference_id, record_card_id, photo_ids, INFERENCE_FAILED, NO_FAILURE_FILE)
    code, message = INFERENCE_FAILED, failure_body
    try:
        parsed = json.loads(failure_body)
    except json.JSONDecodeError:
        parsed = None
    container_error = parsed.get("error") if isinstance(parsed, dict) else None
    if isinstance(container_error, dict) and container_error.get("code"):
        code, message = container_error["code"], container_error.get("message")
    return _inference_failure(inference_id, record_card_id, photo_ids, code, message)


def unusable_output(inference_id: str, record_card_id: str, reason: str, *, photo_ids: list[Any]) -> CompletionMessage:
    """성공 알림인데 결과 파일이 없거나 계약 형식이 아니다 → 백엔드가 기다리지 않도록 실패로 알린다."""
    return _inference_failure(inference_id, record_card_id, photo_ids, INFERENCE_FAILED, reason)


def no_response(inference_id: str, record_card_id: str, waited_seconds: int, *,
                photo_ids: list[Any]) -> CompletionMessage:
    """제한 시간 안에 결과·실패 파일이 생기지 않았다 (컨테이너 응답 없음)."""
    return _inference_failure(inference_id, record_card_id, photo_ids, INFERENCE_FAILED,
                              f"응답 없음: 호출 후 {waited_seconds}초 동안 결과·실패 파일이 없습니다")


def invoke_failed(inference_id: str, record_card_id: str, http_status: int | None,
                  error_code: str, message: str, *, photo_ids: list[Any]) -> CompletionMessage:
    """호출 거부(400·500·503) 또는 SageMaker 응답 없음(http_status=None: 업로드 실패·네트워크 오류)."""
    invoke = {"http_status": http_status, "error_code": error_code, "message": message}
    return _message(inference_id, record_card_id, "FAILED", "INVOKE", invoke,
                    _failure_result(photo_ids, error_code, message))


# 요청 메시지 오류용 ID 이름공간. 바꾸면 같은 메시지 재발행 때 ID가 달라진다
_INVALID_MESSAGE_NAMESPACE = uuid.UUID("6f1d3c52-8a4e-4b7f-9c2d-5e0a1b7f3d64")


def invalid_message_id(topic: str, partition: int, offset: int) -> str:
    """요청 메시지 오류는 DB 행이 없다 → Kafka 위치로 ID를 정해 같은 메시지 재수신 때도 같은 값을 쓴다."""
    return str(uuid.uuid5(_INVALID_MESSAGE_NAMESPACE, f"{topic}:{partition}:{offset}"))


def invalid_message(invalid: InvalidMessage, inference_id: str, *, photo_ids: list[Any]) -> CompletionMessage:
    invoke = {"http_status": None, "error_code": INVALID_MESSAGE, "message": invalid.reason}
    return _message(inference_id, invalid.record_card_id, "FAILED", "INVOKE", invoke,
                    _failure_result(photo_ids, INVALID_MESSAGE, invalid.reason))


def kafka_key(message: CompletionMessage) -> bytes | None:
    """완료 메시지 키 = str(record_card_id). 없으면 키 없이 발행."""
    record_card_id = message["record_card_id"]
    return None if record_card_id is None else str(record_card_id).encode("utf-8")


def kafka_headers(message: CompletionMessage) -> list[tuple[str, bytes]]:
    """백엔드 중복 처리용 헤더 inference_id (본문 값과 같다)."""
    return [("inference_id", str(message["inference_id"]).encode("utf-8"))]
