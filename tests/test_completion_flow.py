"""흐름 ②: 발행 전 요청의 S3 결과/실패 파일 확인 → 저장 → Kafka 발행 (설계 4.2, 2026-11-16 SQS 대신 S3 확인)."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from fakes import FakeInvoker, FakeObjectStore, FakePublisher, FakeRepository
from test_messages import CARD, USER
from runiverse_ai_server.completion_flow import CompletionFlow
from runiverse_ai_server.messages import INFERENCE_FAILED
from runiverse_ai_server.ports import KafkaRecord, PublishError
from runiverse_ai_server.request_flow import RequestFlow

IID = "22222222-2222-4222-8222-222222222222"
BODY = json.dumps({"user_id": USER, "record_card_id": CARD, "memo": None, "activity_type": "RACE",
                   "running_summary": {}, "photos": []}).encode()
OUTPUT = f"s3://bucket/async/outputs/test/{IID}.out"
FAILURE = f"s3://bucket/async/failures/test/{IID}-error.out"
SUCCESS_JSON = json.dumps({"user_id": USER, "record_card_id": CARD, "status": "SUCCEEDED", "review_mood": None,
                           "base_music": {"s3_key": "audio/test/1/RACE/101/base.wav", "duration_seconds": 30.0},
                           "photo_music": [], "error": None, "manifest": {}})
DEADLINE = 600


@pytest.fixture
def deps():
    return {"store": FakeObjectStore(), "publisher": FakePublisher(), "repo": FakeRepository()}


class Clock:
    def __init__(self):
        self.now = datetime.now(timezone.utc)

    def __call__(self):
        return self.now


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def flow(deps, clock):
    return CompletionFlow(**deps, response_deadline_seconds=DEADLINE, clock=clock)


@pytest.fixture
def invoked(deps):
    """요청 흐름으로 INVOKED 행을 만든다."""
    RequestFlow(store=deps["store"], invoker=FakeInvoker(), publisher=deps["publisher"], repo=deps["repo"],
                input_prefix="async/inputs/local", endpoint_name="ep", new_id=lambda: IID) \
        .handle(KafkaRecord(topic="t", partition=0, offset=1, value=BODY))
    return deps["repo"].get(IID)


def check(flow, deps):
    return flow.check(deps["repo"].get(IID))


def test_pending_lists_invoked_rows(flow, invoked):
    assert [row.inference_id for row in flow.pending()] == [IID]


def test_no_file_yet_waits(flow, deps, invoked):
    assert check(flow, deps) is False

    assert deps["publisher"].messages == []
    assert deps["repo"].get(IID).status == "INVOKED"


def test_output_file_publishes_extracted_result(flow, deps, invoked):
    deps["store"].objects[OUTPUT] = SUCCESS_JSON.encode()

    assert check(flow, deps) is True

    [msg] = deps["publisher"].messages
    assert (msg["inference_id"], msg["record_card_id"], msg["status"]) == (IID, CARD, "SUCCEEDED")
    assert msg["result"]["base_music"]["duration_seconds"] == 30.0
    row = deps["repo"].get(IID)
    assert (row.status, row.result_json, row.published_at is not None) == ("SUCCEEDED", SUCCESS_JSON, True)
    assert flow.pending() == []


def test_failure_file_publishes_container_error(flow, deps, invoked):
    failure = json.dumps({"status": "FAILED", "error": {"code": "INVALID_REQUEST", "message": "모르는 필드"}})
    deps["store"].objects[FAILURE] = failure.encode()

    check(flow, deps)

    [msg] = deps["publisher"].messages
    assert (msg["failed_stage"], msg["result"]["error"]["code"]) == ("INFERENCE", "INVALID_REQUEST")
    row = deps["repo"].get(IID)
    assert (row.status, row.error_code, row.error_message, row.result_json) == \
        ("FAILED", "INVALID_REQUEST", "모르는 필드", failure)


def test_non_json_failure_file_stored_as_message(flow, deps, invoked):
    deps["store"].objects[FAILURE] = b"Received server error (500)"

    check(flow, deps)

    row = deps["repo"].get(IID)
    assert (row.error_code, row.error_message, row.result_json) == \
        (INFERENCE_FAILED, "Received server error (500)", None)  # JSON이 아닌 내용은 error_message에만


@pytest.mark.parametrize("output", [b"not json", b'{"photo_music": []}'])
def test_unusable_output_publishes_inference_failed(flow, deps, invoked, output):
    deps["store"].objects[OUTPUT] = output

    check(flow, deps)

    [msg] = deps["publisher"].messages
    assert (msg["status"], msg["failed_stage"], msg["result"]["error"]["code"]) == \
        ("FAILED", "INFERENCE", INFERENCE_FAILED)


def test_no_file_after_deadline_publishes_no_response(flow, deps, invoked, clock):
    clock.now = deps["repo"].get(IID).invoked_at + timedelta(seconds=DEADLINE + 1)

    assert check(flow, deps) is True

    [msg] = deps["publisher"].messages
    assert (msg["failed_stage"], msg["result"]["error"]["code"]) == ("INFERENCE", INFERENCE_FAILED)
    assert "응답 없음" in msg["result"]["error"]["message"]
    assert deps["repo"].get(IID).status == "FAILED"


def test_invoking_without_location_waits_then_times_out(flow, deps, invoked, clock):
    deps["repo"]._update(IID, status="INVOKING", output_location=None, failure_location=None)

    assert check(flow, deps) is False  # 결과 위치를 모른다 → 제한 시간까지 기다린다

    clock.now = deps["repo"].get(IID).invoked_at + timedelta(seconds=DEADLINE + 1)
    assert check(flow, deps) is True
    assert deps["publisher"].messages[0]["result"]["error"]["code"] == INFERENCE_FAILED


def test_publish_failure_raises_and_retries_next_check(flow, deps, invoked):
    deps["store"].objects[OUTPUT] = SUCCESS_JSON.encode()
    deps["publisher"].fail = True
    with pytest.raises(PublishError):
        check(flow, deps)
    assert deps["repo"].get(IID).published_at is None
    assert [row.inference_id for row in flow.pending()] == [IID]  # 다음 확인 때 다시

    deps["publisher"].fail = False
    check(flow, deps)

    assert len(deps["publisher"].messages) == 1
    assert flow.pending() == []


def test_already_published_does_nothing(flow, deps, invoked):
    deps["store"].objects[OUTPUT] = SUCCESS_JSON.encode()
    check(flow, deps)
    deps["publisher"].messages.clear()

    assert check(flow, deps) is False
    assert deps["publisher"].messages == []
