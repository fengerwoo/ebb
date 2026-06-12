"""serve 进程的内存注册表：正在执行的任务与上轮结果，供 /admin/jobs 与 ebb ps 读取。"""

from __future__ import annotations

import threading
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Registry:
    """线程安全。键为 (job, kind)，kind ∈ {export, compact, purge}。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[tuple[str, str], dict[str, Any]] = {}

    def _entry(self, job: str, kind: str) -> dict[str, Any]:
        return self._entries.setdefault(
            (job, kind),
            {"job": job, "kind": kind, "state": "idle", "progress": None, "last_result": None},
        )

    def start(self, job: str, kind: str) -> None:
        with self._lock:
            e = self._entry(job, kind)
            e["state"] = "running"
            e["started_at"] = _now()
            e["progress"] = {}

    def progress(self, job: str, kind: str, data: dict) -> None:
        with self._lock:
            e = self._entry(job, kind)
            e["progress"] = data

    def finish(self, job: str, kind: str, result: Any = None, error: str | None = None) -> None:
        with self._lock:
            e = self._entry(job, kind)
            e["state"] = "idle"
            e["progress"] = None
            e["finished_at"] = _now()
            if error is not None:
                e["last_result"] = {"status": "error", "error": error}
            elif is_dataclass(result):
                e["last_result"] = asdict(result)
            elif result is not None:
                e["last_result"] = result

    def set_next_run(self, job: str, kind: str, when: datetime | None) -> None:
        with self._lock:
            e = self._entry(job, kind)
            e["next_run_at"] = when.isoformat(timespec="seconds") if when else None

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(e) for e in self._entries.values()]
