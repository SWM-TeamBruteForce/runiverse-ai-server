"""단계별 JSON 한 줄 로그. 회고 원문·캡션·입력 JSON 본문은 넘기지 않는다 (호출하는 쪽 책임)."""

import json
import sys
from datetime import datetime, timezone
from typing import Any


def log_event(
    stage: str,
    event: str,
    *,
    inference_id: str | None = None,
    record_card_id: str | None = None,
    level: str = "INFO",
    **fields: Any,
) -> None:
    line = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "level": level,
        "stage": stage,
        "event": event,
        "inference_id": inference_id,
        "record_card_id": record_card_id,
        **fields,
    }
    # 스레드 2개가 같이 쓰므로 한 번의 write로 줄 단위를 지킨다
    sys.stdout.write(json.dumps(line, ensure_ascii=False, default=str) + "\n")
    sys.stdout.flush()
