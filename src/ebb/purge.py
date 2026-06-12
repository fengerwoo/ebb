"""校验后删除（purge）：线上只保留最近一段时间的数据。

可删边界由「水位 + 保留期」实时推导，不持久化任何进度，中断后下次重新
推导继续：

1. bound = MAX(id)  WHERE 时间早于 now - 保留期 AND id <= 水位；
2. 校验（可配置关闭）：对线上仍存在的 [MIN(id), bound] 区间，对比 MySQL
   与 Parquet 的行数 + id 和，完全一致才删；
3. 分批 DELETE ... LIMIT n，批间 sleep，每批独立提交。
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from . import engine, mysqlutil
from .config import Config, JobConfig
from .export import get_watermark
from .logs import log
from .s3util import S3Store
from .timeutil import mysql_time_predicate


@dataclass
class PurgeResult:
    job: str
    status: str  # ok / empty / dry-run / verify-failed
    watermark: int = 0
    bound_id: int = 0
    eligible_rows: int = 0
    deleted_rows: int = 0
    batches: int = 0
    duration_seconds: float = 0.0
    detail: str = ""


def _deletable_bound(conn, job: JobConfig, watermark: int) -> int | None:
    cutoff = datetime.now(tz=job.tzinfo).timestamp() - job.retention.online_retain_seconds
    pred = mysql_time_predicate(
        job, "<", datetime.fromtimestamp(cutoff, tz=job.tzinfo)
    )
    v = mysqlutil.fetch_value(
        conn,
        f"SELECT MAX(`{job.cursor_column}`) FROM `{job.table}` "
        f"WHERE {pred} AND `{job.cursor_column}` <= {int(watermark)}",
    )
    return int(v) if v is not None else None


def _verify_archived(config: Config, job: JobConfig, from_id: int, to_id: int) -> tuple[bool, str]:
    """对比 [from_id, to_id] 闭区间内 MySQL 与 Parquet 的行数及 id 和。"""
    source = config.source_of(job)
    storage = config.storage_of(job)
    with mysqlutil.connect(source) as conn:
        my_count, my_sum = mysqlutil.count_and_sum_range(
            conn, job.table, job.cursor_column, from_id, to_id
        )
    duck = engine.connect(storage=storage)
    try:
        cursor = f'"{job.cursor_column}"'
        row = duck.execute(
            f"SELECT count(*), COALESCE(sum({cursor}), 0) "
            f"FROM {engine.read_parquet_expr(storage, job.prefix)} "
            f"WHERE {cursor} BETWEEN {int(from_id)} AND {int(to_id)}"
        ).fetchone()
        pq_count, pq_sum = int(row[0]), int(row[1])
    finally:
        duck.close()
    if (my_count, my_sum) != (pq_count, pq_sum):
        return False, (
            f"MySQL rows={my_count} sum={my_sum}, Parquet rows={pq_count} sum={pq_sum}"
        )
    return True, f"rows={my_count}"


def run_purge(
    config: Config,
    job: JobConfig,
    *,
    dry_run: bool = False,
    on_progress: Callable[[dict], None] | None = None,
) -> PurgeResult:
    started = time.monotonic()
    source = config.source_of(job)
    storage = config.storage_of(job)
    store = S3Store(storage)

    watermark = get_watermark(store, job.prefix)
    result = PurgeResult(job=job.name, status="empty", watermark=watermark)
    if watermark <= 0:
        result.duration_seconds = round(time.monotonic() - started, 3)
        return result

    with mysqlutil.connect(source) as conn:
        bound = _deletable_bound(conn, job, watermark)
        if bound is None:
            result.duration_seconds = round(time.monotonic() - started, 3)
            log("purge", job=job.name, status="empty", watermark=watermark)
            return result
        result.bound_id = bound
        low = mysqlutil.min_id(conn, job.table, job.cursor_column)
        eligible, _ = mysqlutil.count_and_sum_range(
            conn, job.table, job.cursor_column, low, bound
        )
        result.eligible_rows = eligible

    if dry_run:
        result.status = "dry-run"
        result.duration_seconds = round(time.monotonic() - started, 3)
        return result

    if job.retention.verify_before_delete:
        ok, detail = _verify_archived(config, job, low, bound)
        result.detail = detail
        if not ok:
            result.status = "verify-failed"
            result.duration_seconds = round(time.monotonic() - started, 3)
            log("purge", job=job.name, status="verify-failed", detail=detail, level="error")
            return result

    with mysqlutil.connect(source) as conn:
        while True:
            affected = mysqlutil.delete_batch(
                conn, job.table, job.cursor_column, bound, job.batch.delete_rows
            )
            if affected == 0:
                break
            result.deleted_rows += affected
            result.batches += 1
            if on_progress:
                on_progress(
                    {
                        "eligible_rows": result.eligible_rows,
                        "deleted_rows": result.deleted_rows,
                        "batches": result.batches,
                        "bound_id": bound,
                    }
                )
            if job.batch.delete_sleep_ms > 0:
                time.sleep(job.batch.delete_sleep_ms / 1000)

    result.status = "ok"
    result.duration_seconds = round(time.monotonic() - started, 3)
    log(
        "purge",
        job=job.name,
        status="ok",
        bound_id=bound,
        deleted_rows=result.deleted_rows,
        batches=result.batches,
        duration_seconds=result.duration_seconds,
    )
    return result
