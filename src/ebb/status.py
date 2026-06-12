"""ebb status：水位、落后量与追平估算。

不依赖 serve 进程：直接由 S3 文件名重建水位，由 MySQL 取最大 id，
再用最近增量文件的（行数, 修改时间）估算导出速率与追平时间。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from . import mysqlutil, naming
from .config import Config, JobConfig
from .s3util import S3Store


@dataclass
class JobStatus:
    job: str
    watermark: int
    max_id: int
    lag_rows: int
    file_count: int
    last_export_at: datetime | None
    rate_rows_per_second: float | None  # 最近一小时增量速率
    eta_seconds: float | None  # 按速率估算的追平时间


def job_status(config: Config, job: JobConfig) -> JobStatus:
    storage = config.storage_of(job)
    source = config.source_of(job)
    store = S3Store(storage)

    objs = store.list_objects(job.prefix)
    files: list[tuple[naming.DataFile, datetime]] = []
    for obj in objs:
        parsed = naming.parse_key(job.prefix, obj["Key"])
        if parsed:
            files.append((parsed, obj["LastModified"]))

    watermark = naming.watermark_of([f for f, _ in files])
    with mysqlutil.connect(source) as conn:
        top = mysqlutil.max_id(conn, job.table, job.cursor_column)
        lag = (
            mysqlutil.count_above(conn, job.table, job.cursor_column, watermark)
            if top > watermark
            else 0
        )

    inc_files = [(f, ts) for f, ts in files if f.kind == "inc"]
    last_export = max((ts for _, ts in inc_files), default=None)

    rate = None
    eta = None
    cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
    recent = [(f, ts) for f, ts in inc_files if ts >= cutoff]
    if len(recent) >= 2:
        rows = sum(f.to_id - f.from_id + 1 for f, _ in recent)
        span = (
            max(ts for _, ts in recent) - min(ts for _, ts in recent)
        ).total_seconds()
        if span > 0:
            rate = rows / span
            if rate > 0:
                eta = lag / rate

    return JobStatus(
        job=job.name,
        watermark=watermark,
        max_id=top,
        lag_rows=lag,
        file_count=len(files),
        last_export_at=last_export,
        rate_rows_per_second=rate,
        eta_seconds=eta,
    )
