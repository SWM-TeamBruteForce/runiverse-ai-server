"""흐름 ①: Kafka 요청 → S3 업로드 → InvokeEndpointAsync (설계 4.1)."""

import dataclasses
import json

import pytest

from fakes import FakeInvoker, FakeObjectStore, FakePublisher, FakeRepository
from test_messages import CARD, PHOTO, USER
from runiverse_ai_server.messages import INVALID_MESSAGE, invalid_message_id
from runiverse_ai_server.ports import InvokeError, KafkaRecord, PublishError, UploadError
from runiverse_ai_server.request_flow import RequestFlow

TOPIC = "dev.record-card.music.requested"
IID = "11111111-1111-4111-8111-111111111111"
BODY = json.dumps({"user_id": USER, "record_card_id": CARD, "memo": "회고 원문", "activity_type": "RACE",
                   "running_summary": {}, "photos": []}, ensure_ascii=False).encode("utf-8")


@pytest.fixture
def deps():
    return {"store": FakeObjectStore(), "invoker": FakeInvoker(), "publisher": FakePublisher(),
            "repo": FakeRepository()}


@pytest.fixture
def flow(deps):
    return RequestFlow(**deps, input_prefix="async/inputs/local", endpoint_name="ep", new_id=lambda: IID)


def record(value=BODY, offset=7):
    return KafkaRecord(topic=TOPIC, partition=0, offset=offset, value=value)


def test_success_uploads_raw_body_and_invokes(flow, deps):
    flow.handle(record())

    key = f"async/inputs/local/{CARD}/{IID}.json"
    assert deps["store"].objects == {f"s3://bucket/{key}": BODY}
    assert deps["invoker"].calls == [(f"s3://bucket/{key}", IID)]
    row = deps["repo"].get(IID)
    assert (row.status, row.input_s3_key, row.invoke_http_status) == ("INVOKED", key, 202)
    assert row.output_location.endswith(f"{IID}.out")
    assert (row.user_id, row.record_card_id, row.endpoint_name) == (USER, CARD, "ep")
    assert (row.kafka_topic, row.kafka_partition, row.kafka_offset) == (TOPIC, 0, 7)
    assert deps["repo"].input_json[IID] == BODY
    assert deps["publisher"].messages == []  # 202는 완료가 아니다


def test_invalid_message_publishes_failure_without_db_or_invoke(flow, deps):
    flow.handle(record(json.dumps({"record_card_id": CARD}).encode()))

    [msg] = deps["publisher"].messages
    assert (msg["inference_id"], msg["record_card_id"], msg["invoke"]["error_code"]) == \
        (invalid_message_id(TOPIC, 0, 7), CARD, INVALID_MESSAGE)
    assert msg["inference_id"] != IID  # 정상 요청용 ID 생성기를 쓰지 않는다
    assert deps["repo"].rows == {}
    assert deps["invoker"].calls == []


def test_invalid_message_result_lists_request_photo_ids(flow, deps):
    flow.handle(record(json.dumps({"record_card_id": CARD, "photos": [{"record_card_image_id": PHOTO}]}).encode()))

    [msg] = deps["publisher"].messages
    assert msg["result"]["photo_music"] == [{"record_card_image_id": PHOTO, "s3_key": None, "duration_seconds": None}]
    assert msg["result"]["error"]["code"] == INVALID_MESSAGE


def test_invalid_message_redelivery_keeps_same_inference_id(flow, deps):
    flow.handle(record(b"not json"))
    flow.handle(record(b"not json"))

    first, second = deps["publisher"].messages
    assert first["inference_id"] == second["inference_id"] is not None


@pytest.mark.parametrize("error", [InvokeError(400, "ValidationError", "Endpoint ep not found"),
                                   InvokeError(None, "ReadTimeoutError", "timeout")])
def test_invoke_failure_is_saved_then_published(flow, deps, error):
    deps["invoker"].error = error

    flow.handle(record())

    [msg] = deps["publisher"].messages
    assert (msg["failed_stage"], msg["invoke"]["http_status"], msg["invoke"]["error_code"]) == \
        ("INVOKE", error.http_status, error.error_code)
    row = deps["repo"].get(IID)
    assert (row.status, row.failed_stage, row.invoke_http_status) == ("FAILED", "INVOKE", error.http_status)
    assert row.published_at is not None


def test_upload_failure_publishes_without_http_status(flow, deps):
    deps["store"].fail_put = UploadError("AccessDenied", "no s3:PutObject")

    flow.handle(record())

    [msg] = deps["publisher"].messages
    assert msg["invoke"] == {"http_status": None, "error_code": "AccessDenied", "message": "no s3:PutObject"}
    assert deps["invoker"].calls == []
    assert deps["repo"].get(IID).published_at is not None


def test_publish_failure_propagates_and_leaves_unpublished(flow, deps):
    deps["invoker"].error = InvokeError(503, "ServiceUnavailable", "busy")
    deps["publisher"].fail = True

    with pytest.raises(PublishError):  # 커밋하지 않도록 호출한 쪽에 알린다
        flow.handle(record())

    assert deps["repo"].get(IID).published_at is None


def test_each_new_offset_gets_new_inference_id(deps):
    ids = iter(["a", "b"])
    flow = RequestFlow(**deps, input_prefix="async/inputs/local", endpoint_name="ep", new_id=lambda: next(ids))

    flow.handle(record(offset=1))
    flow.handle(record(offset=2))  # 같은 내용 재요청도 요청건대로 처리

    assert [iid for _, iid in deps["invoker"].calls] == ["a", "b"]


# ── 같은 offset 재수신 (설계 4.1 표) ────────────────────────


def _seed(deps, flow, **changes):
    deps["invoker"].error = InvokeError(500, "InternalFailure", "x") if changes.pop("fail", False) else None
    flow.handle(record())
    deps["repo"]._update(IID, **changes)
    deps["invoker"].calls.clear()
    deps["invoker"].error = None
    deps["publisher"].messages.clear()


@pytest.mark.parametrize("status", ["RECEIVED", "UPLOADED"])
def test_redelivery_before_invoke_resumes_with_same_id(flow, deps, status):
    _seed(deps, flow, status=status)

    flow.handle(record())

    assert deps["invoker"].calls == [(f"s3://bucket/async/inputs/local/{CARD}/{IID}.json", IID)]
    assert len(deps["repo"].rows) == 1
    assert deps["repo"].get(IID).status == "INVOKED"


def test_invoking_is_recorded_before_call(flow, deps):
    seen = []
    deps["invoker"].on_call = lambda iid: seen.append(deps["repo"].get(iid).status)

    flow.handle(record())

    assert seen == ["INVOKING"]  # 호출 직후 죽어도 다시 호출하지 않도록 먼저 남긴다
    assert deps["repo"].get(IID).status == "INVOKED"


@pytest.mark.parametrize("status", ["INVOKING", "INVOKED", "SUCCEEDED"])
def test_redelivery_after_invoke_does_nothing(flow, deps, status):
    _seed(deps, flow, status=status)

    flow.handle(record())

    assert deps["invoker"].calls == []
    assert deps["publisher"].messages == []


def test_redelivery_of_unpublished_invoke_failure_republishes(flow, deps):
    _seed(deps, flow, fail=True, published_at=None)

    flow.handle(record())

    [msg] = deps["publisher"].messages
    assert (msg["inference_id"], msg["failed_stage"], msg["invoke"]["http_status"]) == (IID, "INVOKE", 500)
    assert deps["invoker"].calls == []
    assert deps["repo"].get(IID).published_at is not None


def test_redelivery_of_published_failure_does_nothing(flow, deps):
    _seed(deps, flow, fail=True)

    flow.handle(record())

    assert deps["publisher"].messages == []
    assert deps["invoker"].calls == []
