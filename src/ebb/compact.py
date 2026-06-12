"""每日合并：把一个分区内的全部小文件合并为单个 data 文件。

流程：读全部源文件 → 写临时对象 tmp-data-… → 校验行数 → 改名为 data-… →
删除源文件。任何一步崩溃都可安全重跑：
- tmp- 文件不参与水位解析与查询通配（inc-*/data-*），是不可见的中间产物；
- 改名后、删源前崩溃：分区里 data 与 inc 并存（行重复），重跑时合并按
  游标列去重后覆盖目标并删掉源文件，恢复一致（目标名由源文件 id 区间
  决定，幂等）。
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable

from . import engine, naming
from .config import Config, JobConfig
from .logs import log
from .s3util import S3Store


@dataclass
class CompactResult:
    job: str
    dt: str
    status: str  # ok / skip
    source_files: int = 0
    rows: int = 0
    bytes: int = 0
    target_key: str = ""
    duration_seconds: float = 0.0
    removed_keys: list[str] = field(default_factory=list)


def list_partition_files(store: S3Store, prefix: str, dt: str) -> list[naming.DataFile]:
    keys = store.list_keys(naming.partition_dir(prefix, dt))
    return naming.parse_keys(prefix, keys)


def run_compact(
    config: Config,
    job: JobConfig,
    dt: str,
    *,
    on_progress: Callable[[dict], None] | None = None,
) -> CompactResult:
    started = time.monotonic()
    storage = config.storage_of(job)
    store = S3Store(storage)

    files = list_partition_files(store, job.prefix, dt)
    result = CompactResult(job=job.name, dt=dt, status="skip", source_files=len(files))
    inc_files = [f for f in files if f.kind == "inc"]
    if not files or (not inc_files and len(files) == 1):
        # 没有文件，或已经只剩单个 data 文件：无事可做
        result.duration_seconds = round(time.monotonic() - started, 3)
        log("compact", job=job.name, dt=dt, status="skip", source_files=len(files))
        return result

    # 单个 inc 文件无需合并，直接改名为 data（纯服务端 CopyObject，零数据传输）
    if len(files) == 1 and inc_files:
        f = files[0]
        target = naming.data_key(job.prefix, dt, f.from_id, f.to_id)
        store.rename(f.key, target)
        result.status = "ok"
        result.bytes = store.head_size(target)
        result.target_key = target
        result.removed_keys = [f.key]
        result.duration_seconds = round(time.monotonic() - started, 3)
        log(
            "compact", job=job.name, dt=dt, status="renamed",
            source_files=1, bytes=result.bytes, target=target,
            duration_seconds=result.duration_seconds,
        )
        return result

    from_id = min(f.from_id for f in files)
    to_id = max(f.to_id for f in files)
    target = naming.data_key(job.prefix, dt, from_id, to_id)
    tmp = naming.tmp_key(job.prefix, dt, from_id, to_id)
    if on_progress:
        on_progress({"dt": dt, "source_files": len(files), "stage": "merging"})

    conn = engine.connect(storage=storage)
    try:
        cursor = f'"{job.cursor_column}"'
        urls = ", ".join(f"'{engine.s3_url(storage, f.key)}'" for f in files)
        # 按游标列去重：源文件正常情况下不重叠，去重只在「上次合并删源前
        # 崩溃」的重跑场景下生效
        expected = conn.execute(
            f"SELECT count(DISTINCT {cursor}) FROM read_parquet([{urls}])"
        ).fetchone()[0]
        conn.execute(
            f"COPY (SELECT * FROM read_parquet([{urls}]) "
            f"QUALIFY row_number() OVER (PARTITION BY {cursor}) = 1 "
            f"ORDER BY {cursor}) "
            f"TO '{engine.s3_url(storage, tmp)}' (FORMAT parquet, COMPRESSION zstd)"
        )
        actual = conn.execute(
            f"SELECT count(*) FROM read_parquet('{engine.s3_url(storage, tmp)}')"
        ).fetchone()[0]
    finally:
        conn.close()

    if actual != expected:
        store.delete_key(tmp)
        raise RuntimeError(
            f"合并校验失败 job={job.name} dt={dt}: 源 {expected} 行, 合并后 {actual} 行"
        )

    store.rename(tmp, target)
    result.removed_keys = [f.key for f in files if f.key != target]
    store.delete_keys(result.removed_keys)

    result.status = "ok"
    result.rows = int(actual)
    result.bytes = store.head_size(target)
    result.target_key = target
    result.duration_seconds = round(time.monotonic() - started, 3)
    log(
        "compact",
        job=job.name,
        dt=dt,
        status="ok",
        source_files=len(files),
        rows=result.rows,
        bytes=result.bytes,
        target=target,
        duration_seconds=result.duration_seconds,
    )
    return result


def pending_compact_dates(store: S3Store, prefix: str, today_local: str) -> list[str]:
    """所有「早于今天且仍存在 inc 文件」的分区日期（用于补漏合并）。"""
    files = naming.parse_keys(prefix, store.list_keys(prefix))
    dates = sorted({f.dt for f in files if f.kind == "inc" and f.dt < today_local})
    return dates
