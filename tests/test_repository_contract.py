"""InferenceRepository 계약: Fake와 MySQL이 똑같이 동작해야 한다."""

import json

import pytest

from conftest import MIGRATIONS
from fakes import FakeRepository
from runiverse_ai_server.ports import DuplicateOffsetError, InferenceRecord, Invocation, InvokeError, UploadError

IID = "33333333-3333-4333-8333-333333333333"
OTHER = "44444444-4444-4444-8444-444444444444"
BODY = json.dumps({"user_id": 1, "record_card_id": 101, "memo": "회고 원문"}, ensure_ascii=False).encode()


@pytest.fixture(params=["fake", "mysql"])
def repo(request):
    if request.param == "fake":
        return FakeRepository()
    from runiverse_ai_server.adapters.mysql import MySQLRepository
    from runiverse_ai_server.migrate import apply_migrations

    conn = request.getfixturevalue("db_conn")
    apply_migrations(conn, MIGRATIONS)
    return MySQLRepository(conn)


def new_row(inference_id=IID, offset=7):
    return InferenceRecord(inference_id=inference_id, user_id=1, record_card_id=101, status="RECEIVED",
                           endpoint_name="ep", input_s3_key=f"async/inputs/local/101/{inference_id}.json",
                           kafka_topic="dev.record-card.music.requested", kafka_partition=0, kafka_offset=offset)


@pytest.fixture
def received(repo):
    repo.insert_received(new_row(), BODY)
    return repo


def invoked(repo):
    repo.mark_uploaded(IID)
    repo.mark_invoking(IID)
    repo.mark_invoked(IID, Invocation("s3://b/out/x.out", "s3://b/fail/x-error.out"))


def test_insert_and_get(received):
    assert received.get(IID) == new_row()


def test_find_by_offset(received):
    assert received.find_by_offset("dev.record-card.music.requested", 0, 7) == new_row()
    assert received.find_by_offset("dev.record-card.music.requested", 0, 8) is None
    assert received.find_by_offset("dev.record-card.music.requested", 1, 7) is None


def test_duplicate_offset_rejected(received):
    with pytest.raises(DuplicateOffsetError):
        received.insert_received(new_row(inference_id=OTHER), BODY)


@pytest.mark.parametrize("inference_id", [OTHER, "not-a-uuid"])
def test_get_unknown(received, inference_id):
    assert received.get(inference_id) is None


def test_transitions_to_invoked(received):
    invoked(received)

    row = received.get(IID)
    assert (row.status, row.output_location, row.failure_location, row.invoke_http_status) == \
        ("INVOKED", "s3://b/out/x.out", "s3://b/fail/x-error.out", 202)


def test_invoked_does_not_overwrite_completion(received):
    received.mark_uploaded(IID)
    received.mark_invoking(IID)
    received.mark_completed(IID, "SUCCEEDED", "{}", None, None)

    received.mark_invoked(IID, Invocation("s3://b/out/x.out", None))

    assert received.get(IID).status == "SUCCEEDED"


@pytest.mark.parametrize("error, http_status", [(InvokeError(503, "ServiceUnavailable", "busy"), 503),
                                                (UploadError("AccessDenied", "denied"), None)])
def test_invoke_failed(received, error, http_status):
    received.mark_invoke_failed(IID, error)

    row = received.get(IID)
    assert (row.status, row.failed_stage, row.invoke_http_status, row.invoke_error_code, row.invoke_message) == \
        ("FAILED", "INVOKE", http_status, error.error_code, error.message)


def test_completed_success(received):
    invoked(received)
    result = json.dumps({"status": "SUCCEEDED", "base_music": {"s3_key": "k", "duration_seconds": 30.04}})

    received.mark_completed(IID, "SUCCEEDED", result, None, None)

    row = received.get(IID)
    assert (row.status, row.failed_stage, row.error_code) == ("SUCCEEDED", None, None)
    assert json.loads(row.result_json) == json.loads(result)  # JSON 컬럼은 키 순서·공백을 정리한다


def test_completed_failure_without_json(received):
    invoked(received)

    received.mark_completed(IID, "FAILED", None, "INFERENCE_FAILED", "Received server error (500)")

    row = received.get(IID)
    assert (row.status, row.failed_stage, row.result_json, row.error_code, row.error_message) == \
        ("FAILED", "INFERENCE", None, "INFERENCE_FAILED", "Received server error (500)")


def test_published_keeps_first_time(received):
    received.mark_published(IID)
    first = received.get(IID).published_at

    received.mark_published(IID)

    assert first is not None
    assert received.get(IID).published_at == first


def test_invoking_records_invoked_at_utc(received):
    from datetime import timezone

    received.mark_uploaded(IID)
    received.mark_invoking(IID)

    assert received.get(IID).invoked_at.tzinfo == timezone.utc


def test_list_pending_returns_only_rows_awaiting_completion(repo):
    ids = {name: f"{n:08d}-0000-4000-8000-000000000000" for n, name in enumerate(
        ["received", "invoking", "invoked", "succeeded_unpublished", "succeeded_published",
         "invoke_failed_unpublished", "inference_failed_unpublished"])}
    for offset, (name, iid) in enumerate(ids.items()):
        repo.insert_received(new_row(inference_id=iid, offset=offset), BODY)
        if name == "received":
            continue
        repo.mark_uploaded(iid)
        if name == "invoke_failed_unpublished":
            repo.mark_invoke_failed(iid, InvokeError(400, "ValidationError", "x"))
            continue
        repo.mark_invoking(iid)
        if name == "invoking":
            continue
        repo.mark_invoked(iid, Invocation("s3://b/o", "s3://b/f"))
        if name.startswith("succeeded"):
            repo.mark_completed(iid, "SUCCEEDED", "{}", None, None)
        if name == "inference_failed_unpublished":
            repo.mark_completed(iid, "FAILED", None, "INFERENCE_FAILED", "x")
        if name == "succeeded_published":
            repo.mark_published(iid)

    pending = {row.inference_id for row in repo.list_pending(limit=100)}

    # 요청 흐름이 맡는 행(RECEIVED, INVOKE 실패)과 발행 끝난 행은 제외
    assert pending == {ids["invoking"], ids["invoked"], ids["succeeded_unpublished"],
                       ids["inference_failed_unpublished"]}


def test_list_pending_respects_limit(repo):
    for n in range(3):
        iid = f"{n:08d}-1111-4111-8111-111111111111"
        repo.insert_received(new_row(inference_id=iid, offset=100 + n), BODY)
        repo.mark_uploaded(iid)
        repo.mark_invoking(iid)

    assert len(repo.list_pending(limit=2)) == 2
