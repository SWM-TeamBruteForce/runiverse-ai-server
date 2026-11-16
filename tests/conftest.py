"""MySQL 테스트는 TEST_DATABASE_URL이 있을 때만 돈다.

테이블을 지우고 다시 만들므로 **전용 테스트 DB**만 가리킨다 (DB_* 설정과 별개).
    TEST_DATABASE_URL=mysql://runiverse_ai:<로컬 비밀번호>@127.0.0.1:53306/runiverse_ai_pytest
"""

import os
from pathlib import Path
from urllib.parse import unquote, urlparse

import pytest

MIGRATIONS = Path(__file__).parent.parent / "migrations"


@pytest.fixture
def db_conn():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL 없음 → MySQL 테스트 건너뜀")
    from runiverse_ai_server.adapters.mysql import connect_params

    import pymysql

    parsed = urlparse(url)
    conn = pymysql.connect(**connect_params(host=parsed.hostname, port=parsed.port or 3306,
                                            user=unquote(parsed.username), password=unquote(parsed.password),
                                            database=parsed.path.lstrip("/")))
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS inference_request, schema_migrations, t_ok, t_bad")
    yield conn
    conn.close()


def query(conn, sql, params=()):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()
