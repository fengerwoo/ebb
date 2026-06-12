"""结构化 JSON 日志，走 stdout，docker logs 即可观测。"""

from __future__ import annotations

import json
import sys
import threading
from datetime import datetime, timezone

_lock = threading.Lock()


def log(event: str, level: str = "info", **fields) -> None:
    record = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "level": level,
        "event": event,
        **fields,
    }
    line = json.dumps(record, ensure_ascii=False, default=str)
    with _lock:
        print(line, file=sys.stdout, flush=True)


def log_error(event: str, exc: BaseException | None = None, **fields) -> None:
    if exc is not None:
        fields.setdefault("error", f"{type(exc).__name__}: {exc}")
    log(event, level="error", **fields)
