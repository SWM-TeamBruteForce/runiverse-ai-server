"""migrations/NNN_*.sql을 번호 순서로 한 번씩만 적용한다. 파일 하나 = 트랜잭션 하나.

    DB_HOST=... DB_NAME=... DB_USER=... DB_PASSWORD=... python -m runiverse_ai_server.migrate
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import psycopg

DEFAULT_DIR = Path("migrations")  # 실행 디렉터리 기준
_NAME = re.compile(r"^(\d{3})_[a-z0-9_]+\.sql$")


def migration_files(directory: Path) -> list[Path]:
    files = sorted(p for p in directory.iterdir() if _NAME.match(p.name))
    numbers = [_NAME.match(p.name).group(1) for p in files]
    duplicated = sorted({n for n in numbers if numbers.count(n) > 1})
    if duplicated:
        raise ValueError(f"마이그레이션 번호 중복: {', '.join(duplicated)}")
    return files


def apply_migrations(conn: psycopg.Connection, directory: Path = DEFAULT_DIR) -> list[str]:
    """새로 적용한 파일 이름 목록."""
    conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations ("
                 "name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())")
    done = {row[0] for row in conn.execute("SELECT name FROM schema_migrations")}
    applied = []
    for path in migration_files(directory):
        if path.name in done:
            continue
        with conn.transaction():
            conn.execute(path.read_text(encoding="utf-8"))
            conn.execute("INSERT INTO schema_migrations (name) VALUES (%s)", [path.name])
        applied.append(path.name)
    return applied


def main() -> int:
    from .adapters.postgres import connect
    from .config import DbSettings, load_settings

    with connect(load_settings(DbSettings)) as conn:
        applied = apply_migrations(conn)
    print("적용: " + (", ".join(applied) if applied else "없음 (최신)"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
