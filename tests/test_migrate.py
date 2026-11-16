"""migrations/NNN_*.sql 순서대로 한 번씩만 적용."""

import pytest

from conftest import MIGRATIONS, query
from runiverse_ai_server.migrate import apply_migrations, migration_files, split_statements


def test_files_sorted_by_number(tmp_path):
    for name in ["010_b.sql", "002_a.sql", "notes.md", "001_init.sql"]:
        (tmp_path / name).write_text("SELECT 1;")

    assert [p.name for p in migration_files(tmp_path)] == ["001_init.sql", "002_a.sql", "010_b.sql"]


def test_rejects_duplicate_number(tmp_path):
    (tmp_path / "001_a.sql").write_text("SELECT 1;")
    (tmp_path / "001_b.sql").write_text("SELECT 1;")

    with pytest.raises(ValueError, match="001"):
        migration_files(tmp_path)


def test_split_statements_ignores_comments_and_blank():
    sql = "-- 설명\nCREATE TABLE a (id int);  -- 끝 주석\n\nCREATE INDEX i ON a (id);\n"

    assert split_statements(sql) == ["CREATE TABLE a (id int)", "CREATE INDEX i ON a (id)"]


def test_applies_once(db_conn):
    assert apply_migrations(db_conn, MIGRATIONS) == ["001_init.sql"]
    assert apply_migrations(db_conn, MIGRATIONS) == []

    columns = {r[0] for r in query(db_conn, "SELECT column_name FROM information_schema.columns "
                                            "WHERE table_schema = DATABASE() AND table_name = 'inference_request'")}
    assert {"inference_id", "status", "input_json", "result_json", "kafka_offset", "published_at"} <= columns


def test_failed_migration_is_not_recorded(db_conn, tmp_path):
    (tmp_path / "001_ok.sql").write_text("CREATE TABLE t_ok (id int);")
    (tmp_path / "002_bad.sql").write_text("CREATE TABLE t_bad (id int);\nSELECT nope;")

    with pytest.raises(Exception):
        apply_migrations(db_conn, tmp_path)

    applied = [r[0] for r in query(db_conn, "SELECT name FROM schema_migrations ORDER BY name")]
    assert applied == ["001_ok.sql"]  # 실패한 파일은 기록하지 않는다
    # MySQL은 DDL을 되돌리지 못한다 → 앞 문장(t_bad)은 남는다. 그래서 파일 하나에 DDL은 되도록 하나씩
