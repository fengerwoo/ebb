"""存量回填：按天切片，把历史数据导入对象存储。

与增量导出共用同一套「拉取 → 推导 dt → 写 Parquet」逻辑，区别：
- 用时间窗口（某天 00:00 ~ 次日 00:00，job 时区）圈定数据而不是 id 游标；
- 直接写 data-{from}-{to}.parquet（天级文件，无需再合并）；
- 已存在任何数据文件的分区默认跳过（认为已被增量或先前回填覆盖），
  避免与增量导出的数据重复。

注意：回填会抬高水位（水位 = 全部文件 to_id 最大值），因此应当先回填
历史、再开启增量；或保证回填区间早于增量已覆盖的区间。
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Callable

from . import engine, naming
from .config import Config, JobConfig
from .logs import log
from .s3util import S3Store
from .timeutil import duckdb_dt_expr, mysql_time_predicate


@dataclass
class BackfillDayResult:
    dt: str
    status: str  # ok / empty / skip
    rows: int = 0
    bytes: int = 0
    files: list[str] = field(default_factory=list)


@dataclass
class BackfillResult:
    job: str
    days: list[BackfillDayResult] = field(default_factory=list)
    rows: int = 0
    bytes: int = 0
    duration_seconds: float = 0.0


def _day_sql(job: JobConfig, day: date) -> str:
    start = datetime(day.year, day.month, day.day, tzinfo=job.tzinfo)
    end = start + timedelta(days=1)
    pred_lo = mysql_time_predicate(job, ">=", start)
    pred_hi = mysql_time_predicate(job, "<", end)
    return (
        f"SELECT * FROM `{job.table}` WHERE {pred_lo} AND {pred_hi} "
        f"ORDER BY `{job.cursor_column}`"
    )


def backfill_day(config: Config, job: JobConfig, day: date) -> BackfillDayResult:
    dt_str = day.isoformat()
    storage = config.storage_of(job)
    source = config.source_of(job)
    store = S3Store(storage)

    existing = store.list_keys(naming.partition_dir(job.prefix, dt_str))
    if naming.parse_keys(job.prefix, existing):
        log("backfill", job=job.name, dt=dt_str, status="skip", reason="分区已有数据文件")
        return BackfillDayResult(dt=dt_str, status="skip")

    result = BackfillDayResult(dt=dt_str, status="empty")
    conn = engine.connect(storage=storage, source=source, timezone=job.timezone)
    try:
        dt_expr = duckdb_dt_expr(job)
        conn.execute(
            f"CREATE TEMP TABLE batch AS "
            f"SELECT *, {dt_expr} AS __dt "
            f"FROM {engine.mysql_passthrough(_day_sql(job, day))}"
        )
        cursor = f'"{job.cursor_column}"'
        groups = conn.execute(
            f"SELECT __dt, min({cursor}), max({cursor}), count(*) "
            f"FROM batch GROUP BY __dt ORDER BY min({cursor})"
        ).fetchall()
        for dt_val, from_id, to_id, rows in groups:
            key = naming.data_key(job.prefix, dt_val, int(from_id), int(to_id))
            conn.execute(
                f"COPY (SELECT * EXCLUDE (__dt) FROM batch WHERE __dt = '{dt_val}' "
                f"ORDER BY {cursor}) "
                f"TO '{engine.s3_url(storage, key)}' (FORMAT parquet, COMPRESSION zstd)"
            )
            result.files.append(key)
            result.rows += int(rows)
            result.bytes += store.head_size(key)
        if result.rows > 0:
            result.status = "ok"
    finally:
        conn.close()

    log(
        "backfill",
        job=job.name,
        dt=dt_str,
        status=result.status,
        rows=result.rows,
        bytes=result.bytes,
        files=len(result.files),
    )
    return result


def run_backfill(
    config: Config,
    job: JobConfig,
    from_day: date,
    to_day: date,
    *,
    on_progress: Callable[[dict], None] | None = None,
) -> BackfillResult:
    if from_day > to_day:
        raise ValueError("--from 必须不晚于 --to")
    started = time.monotonic()
    result = BackfillResult(job=job.name)
    day = from_day
    total_days = (to_day - from_day).days + 1
    while day <= to_day:
        day_result = backfill_day(config, job, day)
        result.days.append(day_result)
        result.rows += day_result.rows
        result.bytes += day_result.bytes
        if on_progress:
            on_progress(
                {
                    "days_done": len(result.days),
                    "days_total": total_days,
                    "rows": result.rows,
                    "current_day": day.isoformat(),
                }
            )
        day = day + timedelta(days=1)
    result.duration_seconds = round(time.monotonic() - started, 3)
    return result
