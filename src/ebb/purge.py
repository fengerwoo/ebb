"""校验后删除（purge）：线上只保留最近一段时间的数据。

可删边界由「水位 + 保留期」实时推导，不持久化任何进度，中断后下次重新
推导继续：

1. bound = MAX(id)  WHERE 时间早于 now - 保留期 AND id <= 水位；
2. 校验（可配置关闭）：本轮待删集合（已过期且 id <= bound）逐 id 反连接
   Parquet，缺失为 0 才删。语义是「待删 ⊆ 归档」的包含关系而非区间全等：
   带时间条件的删除会留下「洞上方的倒挂幸存行」，Parquet 比线上多出已删行
   是常态；只有「线上待删、Parquet 缺失」才说明漏归档，必须拦截；
3. 分批 DELETE ... WHERE id <= bound AND 时间早于截止线 ORDER BY id LIMIT n，
   批间 sleep，每批独立提交。删除必须带时间条件：bound 只是过期行的最大 id，
   时间列与 id 倒挂时区间内会混有未过期的行，只按 id 删会把它们提前下线。
   本轮被跳过的未过期行待过期后由后续轮次删除。
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


def _expired_predicate(job: JobConfig) -> str:
    """「时间早于 now - 保留期」的 MySQL 谓词。bound 推导、可删行统计、
    分批删除三处共用同一个谓词，保证看到一致的候选集合。"""
    cutoff = datetime.now(tz=job.tzinfo).timestamp() - job.retention.online_retain_seconds
    return mysql_time_predicate(job, "<", datetime.fromtimestamp(cutoff, tz=job.tzinfo))


def _deletable_bound(conn, job: JobConfig, watermark: int, expired_pred: str) -> int | None:
    v = mysqlutil.fetch_value(
        conn,
        f"SELECT MAX(`{job.cursor_column}`) FROM `{job.table}` "
        f"WHERE {expired_pred} AND `{job.cursor_column}` <= {int(watermark)}",
    )
    return int(v) if v is not None else None


def _verify_archived(
    config: Config, job: JobConfig, cand_min: int, bound: int, expired_pred: str
) -> tuple[bool, str]:
    """校验「本轮待删集合 ⊆ Parquet」：候选行（已过期且 id <= bound）逐 id
    反连接归档文件，缺失为 0 才放行。

    候选集由 MySQL 谓词评估、只按 id 对账，Parquet 侧不重算时间条件
    （timestamp 列的墙钟渲染依赖会话时区，id 才是两侧一致的可靠键）。
    Parquet 侧的重复行（compact 改名与删源之间的窗口）对反连接无影响；
    verify 与 delete 之间不会有新候选出现（新行 id 必大于 bound），并发
    删除只会让候选集变小，两个方向都安全。"""
    source = config.source_of(job)
    storage = config.storage_of(job)
    cur = job.cursor_column
    candidate_sql = (
        f"SELECT `{cur}` FROM `{job.table}` "
        f"WHERE `{cur}` <= {int(bound)} AND {expired_pred}"
    )
    cursor = f'"{cur}"'
    duck = engine.connect(storage=storage, source=source)
    try:
        row = duck.execute(
            f"SELECT count(*), min(id) FROM "
            f"(SELECT {cursor} AS id FROM {engine.mysql_passthrough(candidate_sql)}) m "
            f"ANTI JOIN "
            f"(SELECT {cursor} AS id FROM {engine.read_parquet_expr(storage, job.prefix)} "
            f"WHERE {cursor} BETWEEN {int(cand_min)} AND {int(bound)}) p "
            f"USING (id)"
        ).fetchone()
        missing, sample = int(row[0]), row[1]
    finally:
        duck.close()
    if missing:
        return False, f"{missing} 行待删数据未在 Parquet 中（如 id={int(sample)}）"
    return True, ""


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

    expired_pred = _expired_predicate(job)
    with mysqlutil.connect(source) as conn:
        bound = _deletable_bound(conn, job, watermark, expired_pred)
        if bound is None:
            result.duration_seconds = round(time.monotonic() - started, 3)
            log("purge", job=job.name, status="empty", watermark=watermark)
            return result
        result.bound_id = bound
        # 可删行数与最小候选 id：条件与删除完全一致（不含倒挂的未过期行），
        # 最小候选 id 用于裁剪校验时的 Parquet 扫描范围
        row = mysqlutil.fetch_row(
            conn,
            f"SELECT COUNT(*), MIN(`{job.cursor_column}`) FROM `{job.table}` "
            f"WHERE `{job.cursor_column}` <= {int(bound)} AND {expired_pred}",
        )
        eligible = int(row[0] or 0)
        cand_min = int(row[1]) if row[1] is not None else None
        result.eligible_rows = eligible

    if eligible == 0 or cand_min is None:
        # bound 推导与候选统计之间候选被并发删空：本轮无事可做
        result.duration_seconds = round(time.monotonic() - started, 3)
        log("purge", job=job.name, status="empty", watermark=watermark)
        return result

    if on_progress:
        on_progress(
            {
                "stage": "plan",
                "watermark": watermark,
                "bound_id": bound,
                "eligible_rows": eligible,
            }
        )

    if dry_run:
        result.status = "dry-run"
        result.duration_seconds = round(time.monotonic() - started, 3)
        return result

    if job.retention.verify_before_delete:
        if on_progress:
            on_progress({"stage": "verify", "from_id": cand_min, "to_id": bound})
        ok, detail = _verify_archived(config, job, cand_min, bound, expired_pred)
        result.detail = detail or f"verified={eligible}"
        if not ok:
            result.status = "verify-failed"
            result.duration_seconds = round(time.monotonic() - started, 3)
            log("purge", job=job.name, status="verify-failed", detail=detail, level="error")
            return result

    delete_started = time.monotonic()
    with mysqlutil.connect(source) as conn:
        while True:
            affected = mysqlutil.delete_batch(
                conn,
                job.table,
                job.cursor_column,
                bound,
                job.batch.delete_rows,
                extra_where=expired_pred,
            )
            if affected == 0:
                break
            result.deleted_rows += affected
            result.batches += 1
            if on_progress:
                on_progress(
                    {
                        "stage": "delete",
                        "eligible_rows": result.eligible_rows,
                        "deleted_rows": result.deleted_rows,
                        "batches": result.batches,
                        "bound_id": bound,
                        "elapsed_seconds": time.monotonic() - delete_started,
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
