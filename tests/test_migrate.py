"""migrations/NNN_*.sql 순서대로 한 번씩만 적용."""

import pytest

from conftest import MIGRATIONS
from runiverse_ai_server.migrate import apply_migrations, migration_files


def test_files_sorted_by_number(tmp_path):
    for name in ["010_b.sql", "002_a.sql", "notes.md", "001_init.sql"]:
        (tmp_path / name).write_text("SELECT 1;")

    assert [p.name for p in migration_files(tmp_path)] == ["001_init.sql", "002_a.sql", "010_b.sql"]


def test_rejects_duplicate_number(tmp_path):
    (tmp_path / "001_a.sql").write_text("SELECT 1;")
    (tmp_path / "001_b.sql").write_text("SELECT 1;")

    with pytest.raises(ValueError, match="001"):
        migration_files(tmp_path)


def test_applies_once(pg_conn):
    assert apply_migrations(pg_conn, MIGRATIONS) == ["001_init.sql"]
    assert apply_migrations(pg_conn, MIGRATIONS) == []

    columns = {r[0] for r in pg_conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'inference_request'")}
    assert {"inference_id", "status", "input_json", "result_json", "kafka_offset", "published_at"} <= columns


def test_failed_migration_is_not_recorded(pg_conn, tmp_path):
    (tmp_path / "001_ok.sql").write_text("CREATE TABLE t_ok (id int);")
    (tmp_path / "002_bad.sql").write_text("CREATE TABLE t_bad (id int); SELECT nope;")

    with pytest.raises(Exception):
        apply_migrations(pg_conn, tmp_path)

    applied = [r[0] for r in pg_conn.execute("SELECT name FROM schema_migrations ORDER BY name")]
    tables = {r[0] for r in pg_conn.execute("SELECT tablename FROM pg_tables WHERE tablename LIKE 't_%'")}
    pg_conn.execute("DROP TABLE IF EXISTS t_ok, t_bad")
    assert applied == ["001_ok.sql"]
    assert tables == {"t_ok"}  # 실패한 파일은 통째로 되돌린다
