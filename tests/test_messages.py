"""messages.py: 요청 파싱, S3 키, 완료 메시지 조립 (Notion 4.1 정본)."""

import json
import uuid

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
    invalid_message_id,
    invoke_failed,
    kafka_key,
    no_response,
    parse_request,
    photo_image_ids,
    succeeded,
    unusable_output,
)

IID = "3f2a9c1e-0000-4000-8000-000000000001"
USER = "7b1e4c2a-1111-4aaa-8bbb-000000000001"  # 백엔드 ID는 UUID 문자열 (2026-11-17)
CARD = "0c9d8e7f-2222-4ccc-9ddd-000000000101"
PHOTO = "5a6b7c8d-3333-4eee-afff-000000000201"
PHOTO2 = "5a6b7c8d-3333-4eee-afff-000000000202"
PHOTO_IDS = [PHOTO, PHOTO2]


def raw(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


# ── 요청 파싱 ──────────────────────────────────────────────


def test_parse_keeps_original_bytes():
    body = raw({"user_id": USER, "record_card_id": CARD, "memo": "힘들었다", "unknown": 1})

    parsed = parse_request(body)

    assert parsed == ParsedRequest(user_id=USER, record_card_id=CARD, body=body)


def test_parse_keeps_id_as_sent_without_normalizing():
    upper = CARD.upper()  # 백엔드가 보낸 값 그대로 돌려줘야 백엔드가 찾을 수 있다

    parsed = parse_request(raw({"user_id": USER, "record_card_id": upper}))

    assert parsed.record_card_id == upper


@pytest.mark.parametrize("body", [b"not json", b"", b"\xff\xfe", b"[1, 2]", b'"text"', b"null"])
def test_parse_rejects_non_object(body):
    parsed = parse_request(body)

    assert isinstance(parsed, InvalidMessage)
    assert parsed.record_card_id is None


def test_parse_missing_user_id_keeps_card_id():
    parsed = parse_request(raw({"record_card_id": CARD}))

    assert isinstance(parsed, InvalidMessage)
    assert parsed.record_card_id == CARD
    assert "user_id" in parsed.reason


def test_parse_missing_card_id():
    parsed = parse_request(raw({"user_id": USER}))

    assert isinstance(parsed, InvalidMessage)
    assert parsed.record_card_id is None
    assert "record_card_id" in parsed.reason


# S3 경로·DB CHAR(36)에 들어가므로 UUID 모양(8-4-4-4-12 16진수)만 받는다
BAD_IDS = [None, 101, "101", "", " ", True, 1.5, CARD + "0", CARD[:-1], CARD.replace("-", ""),
           f" {CARD}", "../../" + CARD[6:], "g" + CARD[1:], [CARD]]


@pytest.mark.parametrize("bad", BAD_IDS)
def test_parse_rejects_non_uuid_card_id(bad):
    parsed = parse_request(raw({"user_id": USER, "record_card_id": bad}))

    assert isinstance(parsed, InvalidMessage)
    assert parsed.record_card_id is None  # UUID로 읽히지 않으면 담지 않는다


@pytest.mark.parametrize("bad", BAD_IDS)
def test_parse_rejects_non_uuid_user_id(bad):
    parsed = parse_request(raw({"user_id": bad, "record_card_id": CARD}))

    assert isinstance(parsed, InvalidMessage)
    assert parsed.record_card_id == CARD
    assert "UUID" in parsed.reason


def test_invalid_reason_has_no_body_content():
    parsed = parse_request(raw({"record_card_id": CARD, "memo": "비밀 회고"}))

    assert "비밀 회고" not in parsed.reason


# ── S3 키 ──────────────────────────────────────────────


def test_input_key():
    assert input_key("async/inputs/local", CARD, IID) == f"async/inputs/local/{CARD}/{IID}.json"


# ── 완료 메시지: 성공 ──────────────────────────────────────

CONTAINER_SUCCESS = {
    "user_id": USER,
    "record_card_id": CARD,
    "status": "SUCCEEDED",
    "review_mood": {"emotion_direction": "성취", "emotion_cue": "회고 원문", "music_cue_en": "x"},
    "base_music": {"s3_key": "audio/test/1/RACE/101/base.wav", "duration_seconds": 30.04},
    "photo_music": [
        {"record_card_image_id": PHOTO, "photo_index": 0, "image_key": "cards/101/photos/201.jpg",
         "s3_key": "audio/test/1/RACE/101/201.wav", "duration_seconds": 10.0},
    ],
    "error": None,
    "manifest": {"prompt_mapping_version": "v1"},
}


def test_succeeded_extracts_only_contract_fields():
    msg = succeeded(IID, CARD, json.dumps(CONTAINER_SUCCESS))

    assert msg == {
        "inference_id": IID,
        "record_card_id": CARD,
        "status": "SUCCEEDED",
        "failed_stage": None,
        "invoke": {"http_status": 202, "error_code": None, "message": None},
        "result": {
            "base_music": {"s3_key": "audio/test/1/RACE/101/base.wav", "duration_seconds": 30.04},
            "photo_music": [
                {"record_card_image_id": PHOTO, "s3_key": "audio/test/1/RACE/101/201.wav", "duration_seconds": 10.0},
            ],
        },
    }


def test_succeeded_without_photos():
    output = {**CONTAINER_SUCCESS, "photo_music": []}

    assert succeeded(IID, CARD, json.dumps(output))["result"]["photo_music"] == []


@pytest.mark.parametrize("output", [
    "not json",
    json.dumps([]),
    json.dumps({**CONTAINER_SUCCESS, "base_music": None}),
    json.dumps({k: v for k, v in CONTAINER_SUCCESS.items() if k != "photo_music"}),
    json.dumps({**CONTAINER_SUCCESS, "photo_music": [{"s3_key": "a", "duration_seconds": 1}]}),
])
def test_succeeded_rejects_unexpected_output(output):
    with pytest.raises(ResultFormatError):
        succeeded(IID, CARD, output)


# ── 완료 메시지: 컨테이너 실패 ─────────────────────────────


def _failure_result(code, message, photo_ids=PHOTO_IDS):
    """실패도 성공 양식 + error (2026-11-19 백엔드 요청). 음악 값은 null, 사진 ID는 요청 값 그대로."""
    return {
        "base_music": {"s3_key": None, "duration_seconds": None},
        "photo_music": [{"record_card_image_id": pid, "s3_key": None, "duration_seconds": None}
                        for pid in photo_ids],
        "error": {"code": code, "message": message},
    }


def _failed_inference(code, message):
    return {
        "inference_id": IID,
        "record_card_id": CARD,
        "status": "FAILED",
        "failed_stage": "INFERENCE",
        "invoke": {"http_status": 202, "error_code": None, "message": None},
        "result": _failure_result(code, message),
    }


def test_inference_failed_extracts_code_and_message_only():
    body = json.dumps({"user_id": USER, "record_card_id": CARD, "status": "FAILED",
                       "error": {"code": "MUSIC_GENERATION_OOM", "message": "사진 0(CudaOutOfMemory)",
                                 "detail": [{"loc": "x"}]}})

    assert inference_failed(IID, CARD, body, photo_ids=PHOTO_IDS) == \
        _failed_inference("MUSIC_GENERATION_OOM", "사진 0(CudaOutOfMemory)")


@pytest.mark.parametrize("body", ["Internal Server Error", json.dumps({"error": None}), json.dumps(["x"]),
                                  json.dumps({"error": {"message": "코드 없음"}})])
def test_inference_failed_unreadable_file_uses_whole_content(body):
    assert inference_failed(IID, CARD, body, photo_ids=PHOTO_IDS) == _failed_inference(INFERENCE_FAILED, body)


# dev 엔드포인트에서 받은 실제 실패 파일 (2026-11-18 검증 ④). 버킷 이름만 바꿈
REAL_INVALID_REQUEST_FILE = json.dumps({
    "user_id": USER, "record_card_id": CARD, "status": "FAILED",
    "error": {"code": "INVALID_REQUEST", "message": "요청 형식이 잘못되었습니다.",
              "detail": [{"loc": ["generation_id"], "msg": "Extra inputs are not permitted"}]},
}, ensure_ascii=False, indent=2)
REAL_TIMEOUT_FILE = ("Timed out uploading object (bucket: bucket-test, "
                     "key: async/failures/test/433545b9-53b3-4ca4-8c45-dbc646c8d008-error.out).")


def test_real_container_400_file_is_container_json_as_is():
    # SageMaker가 감싸지 않고 컨테이너 응답 본문을 그대로 남긴다
    msg = inference_failed(IID, CARD, REAL_INVALID_REQUEST_FILE, photo_ids=PHOTO_IDS)

    assert msg == _failed_inference("INVALID_REQUEST", "요청 형식이 잘못되었습니다.")


def test_real_invocation_timeout_file_is_plain_text():
    # InvocationTimeoutSeconds 초과 시 SageMaker가 JSON이 아닌 문장 한 줄을 남긴다
    msg = inference_failed(IID, CARD, REAL_TIMEOUT_FILE, photo_ids=PHOTO_IDS)

    assert msg == _failed_inference(INFERENCE_FAILED, REAL_TIMEOUT_FILE)


def test_unusable_output_is_inference_failed():
    msg = unusable_output(IID, CARD, "성공 결과가 JSON이 아닙니다", photo_ids=PHOTO_IDS)

    assert msg == _failed_inference(INFERENCE_FAILED, "성공 결과가 JSON이 아닙니다")


def test_no_response_is_inference_failed_with_waited_time():
    msg = no_response(IID, CARD, waited_seconds=22500, photo_ids=PHOTO_IDS)

    assert msg == _failed_inference(INFERENCE_FAILED, "응답 없음: 호출 후 22500초 동안 결과·실패 파일이 없습니다")


def test_inference_failed_without_file():
    msg = inference_failed(IID, CARD, None, photo_ids=PHOTO_IDS)

    assert msg["result"]["error"]["code"] == INFERENCE_FAILED
    assert msg["result"]["error"]["message"]


# ── 완료 메시지: 호출 단계 실패 ────────────────────────────


@pytest.mark.parametrize("http_status", [400, 500, 503, None])
def test_invoke_failed(http_status):
    msg = invoke_failed(IID, CARD, http_status, "ValidationError", "Endpoint not found", photo_ids=PHOTO_IDS)

    assert msg == {
        "inference_id": IID,
        "record_card_id": CARD,
        "status": "FAILED",
        "failed_stage": "INVOKE",
        "invoke": {"http_status": http_status, "error_code": "ValidationError", "message": "Endpoint not found"},
        "result": _failure_result("ValidationError", "Endpoint not found"),
    }


@pytest.mark.parametrize("record_card_id", [CARD, None])
def test_invalid_message(record_card_id):
    msg = invalid_message(InvalidMessage(record_card_id=record_card_id, reason="user_id가 없습니다"), IID,
                          photo_ids=PHOTO_IDS)

    assert msg == {
        "inference_id": IID,
        "record_card_id": record_card_id,
        "status": "FAILED",
        "failed_stage": "INVOKE",
        "invoke": {"http_status": None, "error_code": INVALID_MESSAGE, "message": "user_id가 없습니다"},
        "result": _failure_result(INVALID_MESSAGE, "user_id가 없습니다"),
    }


def test_photo_image_ids_from_request_as_sent():
    body = raw({"photos": [{"record_card_image_id": PHOTO, "image_key": "k", "caption": "c"},
                           {"record_card_image_id": 12}, {"caption": "id 없음"}, "not object"]})

    assert photo_image_ids(body) == [PHOTO, 12, None]  # 값은 바꾸지 않는다. 객체가 아닌 항목만 뺀다


@pytest.mark.parametrize("body", [b"not json", raw(["x"]), raw({}), raw({"photos": None}), raw({"photos": "x"}),
                                  None])
def test_photo_image_ids_unreadable_is_empty(body):
    assert photo_image_ids(body) == []


# ── Kafka 키 ──────────────────────────────────────────────


def test_kafka_key_is_card_id_string():
    assert kafka_key(invoke_failed(IID, CARD, None, "x", "y", photo_ids=[])) == CARD.encode()


def test_kafka_key_none_without_card_id():
    assert kafka_key(invalid_message(InvalidMessage(record_card_id=None, reason="JSON이 아닙니다"), IID, photo_ids=[])) is None


def test_invalid_message_id_is_uuid_fixed_per_kafka_message():
    first = invalid_message_id("test.record-card.music.requested", 0, 7)

    assert str(uuid.UUID(first)) == first
    assert invalid_message_id("test.record-card.music.requested", 0, 7) == first  # 같은 메시지 재수신 → 같은 값
    assert invalid_message_id("test.record-card.music.requested", 0, 8) != first
    assert invalid_message_id("test.record-card.music.requested", 1, 7) != first
