"""순수 함수: 요청 파싱, S3 입력 키, 완료 메시지 조립. 형식 정본은 Notion「AI 기능 아키텍처 및 data 입출력」3장·4.1.

- 요청 본문은 해석·수정하지 않는다. user_id·record_card_id 존재(양의 정수)만 본다
- 결과는 정해진 필드만 옮긴다. 값을 바꾸거나 새로 계산하지 않는다
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

INVALID_MESSAGE = "INVALID_MESSAGE"
INFERENCE_FAILED = "INFERENCE_FAILED"
NO_FAILURE_FILE = "실패 결과 파일이 없습니다"

CompletionMessage = dict[str, Any]


@dataclass(frozen=True)
class ParsedRequest:
    user_id: int
    record_card_id: int
    body: bytes  # 받은 바이트 그대로. S3에 이 값을 올린다


@dataclass(frozen=True)
class InvalidMessage:
    record_card_id: int | None
    reason: str  # 무엇이 잘못됐는지만. 본문 내용은 넣지 않는다


class ResultFormatError(ValueError):
    """성공 결과 파일이 계약(4.2.1) 형식이 아니다."""


def _positive_int(value: Any) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return None


def parse_request(body: bytes) -> ParsedRequest | InvalidMessage:
    try:
        data = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return InvalidMessage(record_card_id=None, reason="본문이 JSON이 아닙니다")
    if not isinstance(data, dict):
        return InvalidMessage(record_card_id=None, reason="본문이 JSON 객체가 아닙니다")

    record_card_id = _positive_int(data.get("record_card_id"))
    user_id = _positive_int(data.get("user_id"))
    missing = [name for name, value in (("user_id", user_id), ("record_card_id", record_card_id)) if value is None]
    if missing:
        return InvalidMessage(record_card_id=record_card_id,
                              reason=f"{'·'.join(missing)}가 없거나 양의 정수가 아닙니다")
    return ParsedRequest(user_id=user_id, record_card_id=record_card_id, body=body)


def input_key(input_prefix: str, record_card_id: int, inference_id: str) -> str:
    return f"{input_prefix}/{record_card_id}/{inference_id}.json"


def _message(inference_id: str | None, record_card_id: int | None, status: str, failed_stage: str | None,
             invoke: dict[str, Any], result: dict[str, Any] | None) -> CompletionMessage:
    return {
        "inference_id": inference_id,
        "record_card_id": record_card_id,
        "status": status,
        "failed_stage": failed_stage,
        "invoke": invoke,
        "result": result,
    }


def _accepted() -> dict[str, Any]:
    return {"http_status": 202, "error_code": None, "message": None}


def _music(item: Any, fields: tuple[str, ...]) -> dict[str, Any]:
    if not isinstance(item, dict) or any(name not in item for name in fields):
        raise ResultFormatError(f"{'·'.join(fields)} 중 없는 값이 있습니다")
    return {name: item[name] for name in fields}


def succeeded(inference_id: str, record_card_id: int, output_body: str) -> CompletionMessage:
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


def inference_failed(inference_id: str, record_card_id: int, failure_body: str | None) -> CompletionMessage:
    """failure_body: FailureLocation 파일 내용. 파일이 없으면 None."""
    if failure_body is None:
        error = {"code": INFERENCE_FAILED, "message": NO_FAILURE_FILE}
    else:
        error = {"code": INFERENCE_FAILED, "message": failure_body}
        try:
            parsed = json.loads(failure_body)
        except json.JSONDecodeError:
            parsed = None
        container_error = parsed.get("error") if isinstance(parsed, dict) else None
        if isinstance(container_error, dict) and container_error.get("code"):
            error = {"code": container_error["code"], "message": container_error.get("message")}
    return _message(inference_id, record_card_id, "FAILED", "INFERENCE", _accepted(), {"error": error})


def unusable_output(inference_id: str, record_card_id: int, reason: str) -> CompletionMessage:
    """성공 알림인데 결과 파일이 없거나 계약 형식이 아니다 → 백엔드가 기다리지 않도록 실패로 알린다."""
    error = {"code": INFERENCE_FAILED, "message": reason}
    return _message(inference_id, record_card_id, "FAILED", "INFERENCE", _accepted(), {"error": error})


def invoke_failed(inference_id: str, record_card_id: int, http_status: int | None,
                  error_code: str, message: str) -> CompletionMessage:
    """호출 거부(400·500·503) 또는 SageMaker 응답 없음(http_status=None: 업로드 실패·네트워크 오류)."""
    invoke = {"http_status": http_status, "error_code": error_code, "message": message}
    return _message(inference_id, record_card_id, "FAILED", "INVOKE", invoke, None)


def invalid_message(invalid: InvalidMessage) -> CompletionMessage:
    invoke = {"http_status": None, "error_code": INVALID_MESSAGE, "message": invalid.reason}
    return _message(None, invalid.record_card_id, "FAILED", "INVOKE", invoke, None)


def kafka_key(message: CompletionMessage) -> bytes | None:
    """완료 메시지 키 = str(record_card_id). 없으면 키 없이 발행."""
    record_card_id = message["record_card_id"]
    return None if record_card_id is None else str(record_card_id).encode("utf-8")
