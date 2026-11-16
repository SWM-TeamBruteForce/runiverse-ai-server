"""migrations/NNN_*.sql을 번호 순서로 한 번씩만 적용한다 (MySQL).

- 파일의 문장을 모두 실행한 뒤에만 schema_migrations에 기록한다
- ⚠️ MySQL은 DDL을 되돌리지 못한다 → 중간에 실패하면 앞 문장은 남는다. 파일 하나에 DDL은 되도록 하나씩
- 문장 구분은 줄 끝의 `;` (문자열 안에 `;`를 쓰지 않는다)

    DB_HOST=... DB_NAME=... DB_USER=... DB_PASSWORD=... python -m runiverse_ai_server.migrate
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

DEFAULT_DIR = Path("migrations")  # 실행 디렉터리 기준
_NAME = re.compile(r"^(\d{3})_[a-z0-9_]+\.sql$")


def migration_files(directory: Path) -> list[Path]:
    files = sorted(p for p in directory.iterdir() if _NAME.match(p.name))
    numbers = [_NAME.match(p.name).group(1) for p in files]
    duplicated = sorted({n for n in numbers if numbers.count(n) > 1})
    if duplicated:
        raise ValueError(f"마이그레이션 번호 중복: {', '.join(duplicated)}")
    return files


def split_statements(sql: str) -> list[str]:
    lines = [re.sub(r"\s*--.*$", "", line) for line in sql.splitlines()]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def apply_migrations(conn: Any, directory: Path = DEFAULT_DIR) -> list[str]:
    """새로 적용한 파일 이름 목록."""
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS schema_migrations ("
                    "name VARCHAR(255) PRIMARY KEY, applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6))")
        cur.execute("SELECT name FROM schema_migrations")
        done = {row[0] for row in cur.fetchall()}
        applied = []
        for path in migration_files(directory):
            if path.name in done:
                continue
            for statement in split_statements(path.read_text(encoding="utf-8")):
                cur.execute(statement)
            cur.execute("INSERT INTO schema_migrations (name) VALUES (%s)", [path.name])
            applied.append(path.name)
    return applied


def main() -> int:
    from .adapters.mysql import connect
    from .config import DbSettings, load_settings

    conn = connect(load_settings(DbSettings))
    try:
        applied = apply_migrations(conn)
    finally:
        conn.close()
    print("적용: " + (", ".join(applied) if applied else "없음 (최신)"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
