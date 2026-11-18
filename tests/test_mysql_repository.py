"""MySQL에만 있는 것: input_json 보관, 시각 컬럼(UTC), 한글."""

import json
from datetime import timezone

from conftest import MIGRATIONS, query
from runiverse_ai_server.adapters.mysql import MySQLRepository
from runiverse_ai_server.migrate import apply_migrations
from runiverse_ai_server.ports import Invocation
from test_repository_contract import BODY, IID, new_row


def make(db_conn) -> MySQLRepository:
    apply_migrations(db_conn, MIGRATIONS)
    return MySQLRepository(db_conn)


def test_input_json_stored_with_korean(db_conn):
    make(db_conn).insert_received(new_row(), BODY)

    [(stored,)] = query(db_conn, "SELECT input_json FROM inference_request WHERE inference_id = %s", [IID])
    assert json.loads(stored) == json.loads(BODY)
    assert json.loads(stored)["memo"] == "회고 원문"


def test_timestamps_in_order(db_conn):
    repo = make(db_conn)
    repo.insert_received(new_row(), BODY)
    repo.mark_uploaded(IID)
    repo.mark_invoking(IID)
    repo.mark_invoked(IID, Invocation("s3://b/o", None))
    repo.mark_completed(IID, "SUCCEEDED", "{}", None, None)
    repo.mark_published(IID)

    [(created, invoked, completed, published)] = query(
        db_conn, "SELECT created_at, invoked_at, completed_at, published_at FROM inference_request")
    assert created <= invoked <= completed <= published


def test_published_at_is_utc_aware(db_conn):
    repo = make(db_conn)
    repo.insert_received(new_row(), BODY)
    repo.mark_published(IID)

    assert repo.get(IID).published_at.tzinfo == timezone.utc


def test_long_failure_message_fits(db_conn):
    repo = make(db_conn)
    repo.insert_received(new_row(), BODY)
    message = "x" * 200_000  # 실패 파일 내용 전체가 message로 들어갈 수 있다

    repo.mark_completed(IID, "FAILED", None, "INFERENCE_FAILED", message)

    assert repo.get(IID).error_message == message


def test_reconnects_after_connection_lost(db_conn, db_connect):
    apply_migrations(db_conn, MIGRATIONS)
    repo = MySQLRepository(db_connect(), connect=db_connect)
    repo.insert_received(new_row(), BODY)

    repo.conn.close()  # 서버가 유휴 연결을 끊은 경우 (wait_timeout)

    assert repo.get(IID).inference_id == IID
    repo.conn.close()


def test_without_connect_function_lost_connection_raises(db_conn):
    import pymysql
    import pytest

    repo = make(db_conn)
    db_conn.close()

    with pytest.raises(pymysql.err.Error):
        repo.get(IID)


def test_uuid_ids_stored_as_sent(db_conn):
    import dataclasses

    from test_messages import CARD, USER

    repo = make(db_conn)
    row = dataclasses.replace(new_row(), user_id=USER.upper(), record_card_id=CARD.upper())
    repo.insert_received(row, BODY)

    stored = repo.get(IID)
    assert (stored.user_id, stored.record_card_id) == (USER.upper(), CARD.upper())  # 대소문자 그대로 돌려준다
