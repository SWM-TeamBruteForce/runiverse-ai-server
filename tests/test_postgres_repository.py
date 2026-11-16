"""Postgres에만 있는 것: input_json 보관, 시각 컬럼."""

import json

from conftest import MIGRATIONS
from runiverse_ai_server.adapters.postgres import PostgresRepository
from runiverse_ai_server.migrate import apply_migrations
from runiverse_ai_server.ports import Invocation
from test_repository_contract import BODY, IID, new_row


def make(pg_conn) -> PostgresRepository:
    apply_migrations(pg_conn, MIGRATIONS)
    return PostgresRepository(pg_conn)


def test_input_json_stored_with_korean(pg_conn):
    make(pg_conn).insert_received(new_row(), BODY)

    [(stored,)] = pg_conn.execute("SELECT input_json FROM inference_request WHERE inference_id = %s", [IID])
    assert stored == json.loads(BODY)
    assert stored["memo"] == "회고 원문"


def test_timestamps(pg_conn):
    repo = make(pg_conn)
    repo.insert_received(new_row(), BODY)
    repo.mark_uploaded(IID)
    repo.mark_invoking(IID)
    repo.mark_invoked(IID, Invocation("s3://b/o", None))
    repo.mark_completed(IID, "SUCCEEDED", "{}", None, None)
    repo.mark_published(IID)

    [(created, invoked, completed, published)] = pg_conn.execute(
        "SELECT created_at, invoked_at, completed_at, published_at FROM inference_request")
    assert created <= invoked <= completed <= published
