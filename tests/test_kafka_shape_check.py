"""구조 점검 스크립트: 키·타입만 출력하고 값은 내보내지 않는다."""

import json

from kafka_shape_check import describe

BODY = json.dumps({"userId": "7", "payload": {"record_card_id": 10, "memo": "회고 원문 비밀"},
                   "photos": [{"caption": "사진 캡션"}]}, ensure_ascii=False).encode()


def test_lists_key_paths_with_types():
    assert describe(BODY) == [
        "userId: 문자열(숫자 모양)",
        "payload.record_card_id: 정수(양수)",
        "payload.memo: 문자열(8자)",
        "photos: 리스트 1개",
        "photos[0].caption: 문자열(5자)",
    ]


def test_never_outputs_values():
    text = "\n".join(describe(BODY))
    assert "회고" not in text and "캡션" not in text and "7" not in text


def test_non_json_reports_size_only():
    assert describe(b"not json") == ["JSON 아님 (8바이트)"]
