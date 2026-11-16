"""로그는 단계마다 JSON 한 줄. inference_id·record_card_id·stage를 키로 둔다."""

import json

from runiverse_ai_server.log import log_event


def _lines(capsys) -> list[dict]:
    return [json.loads(line) for line in capsys.readouterr().out.splitlines()]


def test_writes_one_json_line_with_keys(capsys):
    log_event("upload", "succeeded", inference_id="abc", record_card_id=101, seconds=0.12)

    (line,) = _lines(capsys)
    assert line["stage"] == "upload"
    assert line["event"] == "succeeded"
    assert line["inference_id"] == "abc"
    assert line["record_card_id"] == 101
    assert line["seconds"] == 0.12
    assert line["level"] == "INFO"
    assert "ts" in line


def test_keeps_id_keys_even_when_unknown(capsys):
    log_event("parse", "failed", level="ERROR", error_code="INVALID_MESSAGE")

    (line,) = _lines(capsys)
    assert line["inference_id"] is None
    assert line["record_card_id"] is None
    assert line["level"] == "ERROR"


def test_keeps_korean_readable(capsys):
    log_event("parse", "failed", message="record_card_id 없음")

    assert "record_card_id 없음" in capsys.readouterr().out
