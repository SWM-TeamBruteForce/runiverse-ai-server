"""흐름 ②: SQS 알림 → 결과/실패 파일 읽기 → 저장 → Kafka 발행 (설계 4.2)."""

import json

import pytest

from fakes import FakeInvoker, FakeObjectStore, FakePublisher, FakeQueue, FakeRepository
from runiverse_ai_server.completion_flow import CompletionFlow
from runiverse_ai_server.messages import INFERENCE_FAILED
from runiverse_ai_server.ports import KafkaRecord, Notification, PublishError
from runiverse_ai_server.request_flow import RequestFlow

IID = "22222222-2222-4222-8222-222222222222"
BODY = json.dumps({"user_id": 1, "record_card_id": 101, "memo": None, "activity_type": "RACE",
                   "running_summary": {}, "photos": []}).encode()
OUTPUT = f"s3://bucket/async/outputs/test/{IID}.out"
FAILURE = f"s3://bucket/async/failures/test/{IID}-error.out"
SUCCESS_JSON = json.dumps({"user_id": 1, "record_card_id": 101, "status": "SUCCEEDED", "review_mood": None,
                           "base_music": {"s3_key": "audio/test/1/RACE/101/base.wav", "duration_seconds": 30.0},
                           "photo_music": [], "error": None, "manifest": {}})


@pytest.fixture
def deps():
    return {"store": FakeObjectStore(), "publisher": FakePublisher(), "repo": FakeRepository(), "queue": FakeQueue()}


@pytest.fixture
def flow(deps):
    return CompletionFlow(**deps)


@pytest.fixture
def invoked(deps):
    """요청 흐름으로 INVOKED 행을 만든다."""
    RequestFlow(store=deps["store"], invoker=FakeInvoker(), publisher=deps["publisher"], repo=deps["repo"],
                input_prefix="async/inputs/local", endpoint_name="ep", new_id=lambda: IID) \
        .handle(KafkaRecord(topic="t", partition=0, offset=1, value=BODY))
    return deps["repo"].get(IID)


def note(succeeded=True, inference_id=IID):
    return Notification(inference_id=inference_id, succeeded=succeeded, handle="receipt-1")


def test_success_publishes_extracted_result_then_deletes(flow, deps, invoked):
    deps["store"].objects[OUTPUT] = SUCCESS_JSON.encode()

    flow.handle(note())

    [msg] = deps["publisher"].messages
    assert (msg["inference_id"], msg["record_card_id"], msg["status"]) == (IID, 101, "SUCCEEDED")
    assert msg["result"]["base_music"]["duration_seconds"] == 30.0
    row = deps["repo"].get(IID)
    assert (row.status, row.result_json, row.published_at is not None) == ("SUCCEEDED", SUCCESS_JSON, True)
    assert deps["queue"].deleted == [note()]


def test_container_failure_publishes_error(flow, deps, invoked):
    failure = json.dumps({"status": "FAILED", "error": {"code": "INVALID_REQUEST", "message": "모르는 필드"}})
    deps["store"].objects[FAILURE] = failure.encode()

    flow.handle(note(succeeded=False))

    [msg] = deps["publisher"].messages
    assert (msg["failed_stage"], msg["result"]["error"]["code"]) == ("INFERENCE", "INVALID_REQUEST")
    row = deps["repo"].get(IID)
    assert (row.status, row.error_code, row.error_message, row.result_json) == \
        ("FAILED", "INVALID_REQUEST", "모르는 필드", failure)


def test_non_json_failure_file_stored_as_message(flow, deps, invoked):
    deps["store"].objects[FAILURE] = b"Received server error (500)"

    flow.handle(note(succeeded=False))

    row = deps["repo"].get(IID)
    assert (row.error_code, row.error_message, row.result_json) == \
        (INFERENCE_FAILED, "Received server error (500)", None)  # jsonb에 못 넣는 내용은 error_message에만


def test_missing_failure_file(flow, deps, invoked):
    flow.handle(note(succeeded=False))

    [msg] = deps["publisher"].messages
    assert msg["result"]["error"]["code"] == INFERENCE_FAILED


@pytest.mark.parametrize("output", [None, b"not json", b'{"photo_music": []}'])
def test_success_notification_with_unusable_output_publishes_inference_failed(flow, deps, invoked, output):
    if output is not None:
        deps["store"].objects[OUTPUT] = output

    flow.handle(note())

    [msg] = deps["publisher"].messages
    assert (msg["status"], msg["failed_stage"], msg["result"]["error"]["code"]) == \
        ("FAILED", "INFERENCE", INFERENCE_FAILED)
    assert deps["repo"].get(IID).status == "FAILED"


def test_publish_failure_keeps_sqs_message(flow, deps, invoked):
    deps["store"].objects[OUTPUT] = SUCCESS_JSON.encode()
    deps["publisher"].fail = True

    with pytest.raises(PublishError):
        flow.handle(note())

    assert deps["queue"].deleted == []
    assert deps["repo"].get(IID).published_at is None


def test_redelivered_notification_after_publish_only_deletes(flow, deps, invoked):
    deps["store"].objects[OUTPUT] = SUCCESS_JSON.encode()
    flow.handle(note())
    deps["publisher"].messages.clear()

    flow.handle(note())

    assert deps["publisher"].messages == []
    assert deps["queue"].deleted == [note(), note()]


def test_redelivered_notification_after_failed_publish_republishes(flow, deps, invoked):
    deps["store"].objects[OUTPUT] = SUCCESS_JSON.encode()
    deps["publisher"].fail = True
    with pytest.raises(PublishError):
        flow.handle(note())
    deps["publisher"].fail = False

    flow.handle(note())

    assert len(deps["publisher"].messages) == 1
    assert deps["repo"].get(IID).published_at is not None


def test_unknown_inference_id_is_deleted(flow, deps):
    flow.handle(note(inference_id="unknown"))

    assert deps["publisher"].messages == []
    assert deps["queue"].deleted == [note(inference_id="unknown")]


def test_notification_before_invoke_recorded_is_left_in_queue(flow, deps, invoked):
    deps["repo"]._update(IID, status="UPLOADED", output_location=None, failure_location=None)

    flow.handle(note())  # 결과 위치를 아직 모른다 → 가시성 타임아웃 후 다시 받는다

    assert deps["publisher"].messages == []
    assert deps["queue"].deleted == []
