"""Postgres 테스트는 TEST_DATABASE_URL이 있을 때만 돈다.

테이블을 지우고 다시 만들므로 **전용 테스트 DB**만 가리킨다 (DB_* 설정과 별개).
    TEST_DATABASE_URL=postgresql://runiverse_ai:<로컬 비밀번호>@127.0.0.1:55432/runiverse_ai_pytest
"""

import os
from pathlib import Path

import pytest

MIGRATIONS = Path(__file__).parent.parent / "migrations"


@pytest.fixture
def pg_conn():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL 없음 → Postgres 테스트 건너뜀")
    import psycopg

    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS inference_request, schema_migrations")
        yield conn
