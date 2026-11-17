"""SageMaker 스모크 스크립트의 AWS 없이 도는 부분. 실제 호출은 수동 실행."""

import json
from pathlib import Path

import pytest

from smoke_invoke import SAMPLES_DIR, input_key, load_sample, parse_s3_uri, wait_for_result

CONTAINER_KEYS = {"user_id", "record_card_id", "memo", "activity_type", "running_summary", "photos"}


def test_input_key_follows_s3_layout():
    assert input_key("async/inputs/local", "c-1", "abc") == "async/inputs/local/c-1/abc.json"


def test_parse_s3_uri():
    assert parse_s3_uri("s3://bucket/async/outputs/test/x.out") == ("bucket", "async/outputs/test/x.out")


def test_parse_s3_uri_rejects_other_scheme():
    with pytest.raises(ValueError):
        parse_s3_uri("https://bucket/x")


def test_load_sample_returns_raw_bytes_and_card_id():
    body, record_card_id = load_sample("no_photo")

    assert body == (SAMPLES_DIR / "no_photo.json").read_bytes()  # 수정 없이 그대로 올린다
    assert record_card_id == "00000000-0000-4000-8000-000000900001"  # 백엔드처럼 UUID 문자열


def test_load_sample_rejects_integer_card_id(tmp_path, monkeypatch):
    (tmp_path / "old.json").write_text('{"user_id": "00000000-0000-4000-8000-000000000001", "record_card_id": 101}')
    monkeypatch.setattr("smoke_invoke.SAMPLES_DIR", tmp_path)

    with pytest.raises(ValueError, match="record_card_id"):
        load_sample("old")


def test_load_sample_rejects_missing_card_id(tmp_path, monkeypatch):
    (tmp_path / "broken.json").write_text('{"user_id": 0}')
    monkeypatch.setattr("smoke_invoke.SAMPLES_DIR", tmp_path)

    with pytest.raises(ValueError, match="record_card_id"):
        load_sample("broken")


@pytest.mark.parametrize("name", ["no_photo", "with_photo"])
def test_valid_samples_match_container_keys(name):
    data = json.loads((SAMPLES_DIR / f"{name}.json").read_text(encoding="utf-8"))

    assert set(data) == CONTAINER_KEYS


def test_invalid_sample_has_extra_key_for_400():
    data = json.loads((SAMPLES_DIR / "invalid_unknown_field.json").read_text(encoding="utf-8"))

    assert set(data) - CONTAINER_KEYS == {"generation_id"}


class FakeS3:
    """head_object가 N번째 확인부터 객체가 있다고 답한다."""

    def __init__(self, objects: dict[str, bytes], appear_after: int = 0):
        self.objects = objects
        self.appear_after = appear_after
        self.checks = 0

    def head_object(self, Bucket, Key):
        from botocore.exceptions import ClientError

        self.checks += 1
        if self.checks > self.appear_after and f"s3://{Bucket}/{Key}" in self.objects:
            return {}
        raise ClientError({"Error": {"Code": "404"}}, "HeadObject")

    def get_object(self, Bucket, Key):
        import io

        return {"Body": io.BytesIO(self.objects[f"s3://{Bucket}/{Key}"])}


def test_wait_returns_failure_file_raw():
    s3 = FakeS3({"s3://b/failures/x": b"not json"})

    status, body = wait_for_result(s3, "s3://b/outputs/x", "s3://b/failures/x", timeout=60, sleep=lambda _: None)

    assert (status, body) == ("failure", "not json")


def test_wait_polls_until_output_appears():
    s3 = FakeS3({"s3://b/outputs/x": b'{"status": "SUCCEEDED"}'}, appear_after=4)

    status, body = wait_for_result(s3, "s3://b/outputs/x", "s3://b/failures/x", timeout=60, sleep=lambda _: None)

    assert status == "success"
    assert json.loads(body) == {"status": "SUCCEEDED"}


def test_wait_times_out():
    s3 = FakeS3({})
    now = iter(range(0, 1000, 30))

    with pytest.raises(TimeoutError):
        wait_for_result(s3, "s3://b/outputs/x", "s3://b/failures/x", timeout=60,
                        sleep=lambda _: None, clock=lambda: next(now))


@pytest.mark.parametrize("name", ["no_photo", "with_photo"])
def test_valid_samples_pass_request_parsing(name):
    from runiverse_ai_server.messages import ParsedRequest, parse_request

    assert isinstance(parse_request((SAMPLES_DIR / f"{name}.json").read_bytes()), ParsedRequest)
