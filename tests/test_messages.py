"""messages.py: 요청 파싱, S3 키, 완료 메시지 조립 (Notion 4.1 정본)."""

import json

import pytest

from runiverse_ai_server.messages import (
    INFERENCE_FAILED,
    INVALID_MESSAGE,
    InvalidMessage,
    ParsedRequest,
    ResultFormatError,
    inference_failed,
    input_key,
    invalid_message,
    invoke_failed,
    kafka_key,
    no_response,
    parse_request,
    succeeded,
    unusable_output,
)

IID = "3f2a9c1e-0000-4000-8000-000000000001"


def raw(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


# ── 요청 파싱 ──────────────────────────────────────────────


def test_parse_keeps_original_bytes():
    body = b'{"user_id": 1, "record_card_id": 101, "memo": "\xed\x9e\x98\xeb\x93\xa4\xec\x97\x88\xeb\x8b\xa4", "unknown": 1}'

    parsed = parse_request(body)

    assert parsed == ParsedRequest(user_id=1, record_card_id=101, body=body)


@pytest.mark.parametrize("body", [b"not json", b"", b"\xff\xfe", b"[1, 2]", b'"text"', b"null"])
def test_parse_rejects_non_object(body):
    parsed = parse_request(body)

    assert isinstance(parsed, InvalidMessage)
    assert parsed.record_card_id is None


def test_parse_missing_user_id_keeps_card_id():
    parsed = parse_request(raw({"record_card_id": 101}))

    assert isinstance(parsed, InvalidMessage)
    assert parsed.record_card_id == 101
    assert "user_id" in parsed.reason


def test_parse_missing_card_id():
    parsed = parse_request(raw({"user_id": 1}))

    assert isinstance(parsed, InvalidMessage)
    assert parsed.record_card_id is None
    assert "record_card_id" in parsed.reason


@pytest.mark.parametrize("bad", [None, "101", 1.5, True, -1, 0])
def test_parse_rejects_non_positive_int_card_id(bad):
    parsed = parse_request(raw({"user_id": 1, "record_card_id": bad}))

    assert isinstance(parsed, InvalidMessage)
    assert parsed.record_card_id is None  # 정수 ID로 읽히지 않으면 담지 않는다


@pytest.mark.parametrize("bad", [None, "1", 2.0, False, 0])
def test_parse_rejects_non_positive_int_user_id(bad):
    parsed = parse_request(raw({"user_id": bad, "record_card_id": 101}))

    assert isinstance(parsed, InvalidMessage)
    assert parsed.record_card_id == 101


def test_invalid_reason_has_no_body_content():
    parsed = parse_request(raw({"record_card_id": 101, "memo": "비밀 회고"}))

    assert "비밀 회고" not in parsed.reason


# ── S3 키 ──────────────────────────────────────────────


def test_input_key():
    assert input_key("async/inputs/local", 101, IID) == f"async/inputs/local/101/{IID}.json"


# ── 완료 메시지: 성공 ──────────────────────────────────────

CONTAINER_SUCCESS = {
    "user_id": 1,
    "record_card_id": 101,
    "status": "SUCCEEDED",
    "review_mood": {"emotion_direction": "성취", "emotion_cue": "회고 원문", "music_cue_en": "x"},
    "base_music": {"s3_key": "audio/test/1/RACE/101/base.wav", "duration_seconds": 30.04},
    "photo_music": [
        {"record_card_image_id": 201, "photo_index": 0, "image_key": "cards/101/photos/201.jpg",
         "s3_key": "audio/test/1/RACE/101/201.wav", "duration_seconds": 10.0},
    ],
    "error": None,
    "manifest": {"prompt_mapping_version": "v1"},
}


def test_succeeded_extracts_only_contract_fields():
    msg = succeeded(IID, 101, json.dumps(CONTAINER_SUCCESS))

    assert msg == {
        "inference_id": IID,
        "record_card_id": 101,
        "status": "SUCCEEDED",
        "failed_stage": None,
        "invoke": {"http_status": 202, "error_code": None, "message": None},
        "result": {
            "base_music": {"s3_key": "audio/test/1/RACE/101/base.wav", "duration_seconds": 30.04},
            "photo_music": [
                {"record_card_image_id": 201, "s3_key": "audio/test/1/RACE/101/201.wav", "duration_seconds": 10.0},
            ],
        },
    }


def test_succeeded_without_photos():
    output = {**CONTAINER_SUCCESS, "photo_music": []}

    assert succeeded(IID, 101, json.dumps(output))["result"]["photo_music"] == []


@pytest.mark.parametrize("output", [
    "not json",
    json.dumps([]),
    json.dumps({**CONTAINER_SUCCESS, "base_music": None}),
    json.dumps({k: v for k, v in CONTAINER_SUCCESS.items() if k != "photo_music"}),
    json.dumps({**CONTAINER_SUCCESS, "photo_music": [{"s3_key": "a", "duration_seconds": 1}]}),
])
def test_succeeded_rejects_unexpected_output(output):
    with pytest.raises(ResultFormatError):
        succeeded(IID, 101, output)


# ── 완료 메시지: 컨테이너 실패 ─────────────────────────────


def _failed_inference(code, message):
    return {
        "inference_id": IID,
        "record_card_id": 101,
        "status": "FAILED",
        "failed_stage": "INFERENCE",
        "invoke": {"http_status": 202, "error_code": None, "message": None},
        "result": {"error": {"code": code, "message": message}},
    }


def test_inference_failed_extracts_code_and_message_only():
    body = json.dumps({"user_id": 1, "record_card_id": 101, "status": "FAILED",
                       "error": {"code": "MUSIC_GENERATION_OOM", "message": "사진 0(CudaOutOfMemory)",
                                 "detail": [{"loc": "x"}]}})

    assert inference_failed(IID, 101, body) == _failed_inference("MUSIC_GENERATION_OOM", "사진 0(CudaOutOfMemory)")


@pytest.mark.parametrize("body", ["Internal Server Error", json.dumps({"error": None}), json.dumps(["x"]),
                                  json.dumps({"error": {"message": "코드 없음"}})])
def test_inference_failed_unreadable_file_uses_whole_content(body):
    assert inference_failed(IID, 101, body) == _failed_inference(INFERENCE_FAILED, body)


def test_unusable_output_is_inference_failed():
    msg = unusable_output(IID, 101, "성공 결과가 JSON이 아닙니다")

    assert msg == _failed_inference(INFERENCE_FAILED, "성공 결과가 JSON이 아닙니다")


def test_no_response_is_inference_failed_with_waited_time():
    msg = no_response(IID, 101, waited_seconds=22500)

    assert msg == _failed_inference(INFERENCE_FAILED, "응답 없음: 호출 후 22500초 동안 결과·실패 파일이 없습니다")


def test_inference_failed_without_file():
    msg = inference_failed(IID, 101, None)

    assert msg["result"]["error"]["code"] == INFERENCE_FAILED
    assert msg["result"]["error"]["message"]


# ── 완료 메시지: 호출 단계 실패 ────────────────────────────


@pytest.mark.parametrize("http_status", [400, 500, 503, None])
def test_invoke_failed(http_status):
    msg = invoke_failed(IID, 101, http_status, "ValidationError", "Endpoint not found")

    assert msg == {
        "inference_id": IID,
        "record_card_id": 101,
        "status": "FAILED",
        "failed_stage": "INVOKE",
        "invoke": {"http_status": http_status, "error_code": "ValidationError", "message": "Endpoint not found"},
        "result": None,
    }


@pytest.mark.parametrize("record_card_id", [101, None])
def test_invalid_message(record_card_id):
    msg = invalid_message(InvalidMessage(record_card_id=record_card_id, reason="user_id가 없습니다"))

    assert msg == {
        "inference_id": None,
        "record_card_id": record_card_id,
        "status": "FAILED",
        "failed_stage": "INVOKE",
        "invoke": {"http_status": None, "error_code": INVALID_MESSAGE, "message": "user_id가 없습니다"},
        "result": None,
    }


# ── Kafka 키 ──────────────────────────────────────────────


def test_kafka_key_is_card_id_string():
    assert kafka_key(invoke_failed(IID, 101, None, "x", "y")) == b"101"


def test_kafka_key_none_without_card_id():
    assert kafka_key(invalid_message(InvalidMessage(record_card_id=None, reason="JSON이 아닙니다"))) is None
