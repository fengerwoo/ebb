"""增量导出：MySQL → Parquet → 对象存储。

水位 = 文件名，先上传、后（隐式）提交：上传成功的瞬间，下一轮 LIST 自然看到新水位。

正确性关键点：
1. 批内按「分区日期连续段」切文件——按 id 排序后，dt 发生变化即切段，
   再按 id 顺序逐段上传。任何时刻崩溃，已上传文件覆盖的 id 必然是
   「从旧水位起的连续区间」，重启后从新水位继续，不丢不重；
2. 同一水位重跑产生完全相同的文件名，S3 PUT 原子覆盖，天然幂等；
3. safety_lag_seconds：只导出时间早于 now - lag 的行，规避自增 id
   提交乱序导致的游标漏读。
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable

from . import engine, mysqlutil, naming
from .config import Config, JobConfig
from .logs import log
from .s3util import S3Store
from .timeutil import duckdb_dt_expr, mysql_time_predicate

ProgressFn = Callable[[dict], None]


@dataclass
class ExportResult:
    job: str
    status: str  # ok / empty / dry-run
    watermark_before: int = 0
    watermark_after: int = 0
    rows: int = 0
    bytes: int = 0
    files: list[str] = field(default_factory=list)
    lag_rows: int = 0
    duration_seconds: float = 0.0


def get_watermark(store: S3Store, prefix: str) -> int:
    keys = store.list_keys(prefix)
    return naming.watermark_of(naming.parse_keys(prefix, keys))


def _batch_sql(job: JobConfig, watermark: int, now: datetime) -> str:
    """下推到 MySQL 的增量查询。"""
    where = [f"`{job.cursor_column}` > {int(watermark)}"]
    if job.batch.safety_lag_seconds > 0:
        cutoff = datetime.fromtimestamp(
            now.timestamp() - job.batch.safety_lag_seconds, tz=job.tzinfo
        )
        where.append(mysql_time_predicate(job, "<", cutoff))
    return (
        f"SELECT * FROM `{job.table}` WHERE {' AND '.join(where)} "
        f"ORDER BY `{job.cursor_column}` LIMIT {job.batch.export_rows}"
    )


def run_export(
    config: Config,
    job: JobConfig,
    *,
    dry_run: bool = False,
    on_progress: ProgressFn | None = None,
) -> ExportResult:
    started = time.monotonic()
    source = config.source_of(job)
    storage = config.storage_of(job)
    store = S3Store(storage)

    watermark = get_watermark(store, job.prefix)
    result = ExportResult(job=job.name, status="ok", watermark_before=watermark)
    now = datetime.now(tz=job.tzinfo)
    batch_sql = _batch_sql(job, watermark, now)

    if dry_run:
        with mysqlutil.connect(source) as conn:
            row = mysqlutil.fetch_row(
                conn,
                f"SELECT COUNT(*), MIN(`{job.cursor_column}`), MAX(`{job.cursor_column}`) "
                f"FROM ({batch_sql}) AS t",
            )
            result.status = "dry-run"
            result.rows = int(row[0])
            result.watermark_after = int(row[2]) if row[2] is not None else watermark
            result.lag_rows = mysqlutil.count_above(
                conn, job.table, job.cursor_column, watermark
            )
        result.duration_seconds = round(time.monotonic() - started, 3)
        return result

    conn = engine.connect(storage=storage, source=source, timezone=job.timezone)
    try:
        dt_expr = duckdb_dt_expr(job)
        conn.execute(
            f"CREATE TEMP TABLE batch AS "
            f"SELECT *, {dt_expr} AS __dt FROM {engine.mysql_passthrough(batch_sql)} "
            f'ORDER BY "{job.cursor_column}"'
        )
        total = conn.execute("SELECT count(*) FROM batch").fetchone()[0]
        if total == 0:
            result.status = "empty"
            result.watermark_after = watermark
            result.duration_seconds = round(time.monotonic() - started, 3)
            log("export", job=job.name, status="empty", watermark=watermark)
            return result

        # 按 id 顺序切「同一 dt 的连续段」，逐段上传，保证已上传 id 区间始终连续
        cursor = f'"{job.cursor_column}"'
        segments = conn.execute(
            f"""
            WITH marked AS (
                SELECT __dt, {cursor} AS cid,
                       CASE WHEN __dt = lag(__dt) OVER (ORDER BY {cursor})
                            THEN 0 ELSE 1 END AS boundary
                FROM batch
            ),
            seg AS (
                SELECT __dt, cid,
                       sum(boundary) OVER (ORDER BY cid) AS seg_id
                FROM marked
            )
            SELECT __dt, min(cid), max(cid), count(*)
            FROM seg GROUP BY seg_id, __dt ORDER BY min(cid)
            """
        ).fetchall()

        uploaded_rows = 0
        for dt, from_id, to_id, seg_rows in segments:
            key = naming.inc_key(job.prefix, dt, int(from_id), int(to_id))
            url = engine.s3_url(storage, key)
            conn.execute(
                f"COPY (SELECT * EXCLUDE (__dt) FROM batch "
                f"WHERE {cursor} BETWEEN {int(from_id)} AND {int(to_id)} "
                f"ORDER BY {cursor}) "
                f"TO '{url}' (FORMAT parquet, COMPRESSION zstd)"
            )
            result.files.append(key)
            result.bytes += store.head_size(key)
            uploaded_rows += int(seg_rows)
            result.watermark_after = int(to_id)
            if on_progress:
                on_progress(
                    {
                        "rows_exported": uploaded_rows,
                        "rows_total": int(total),
                        "watermark": int(to_id),
                    }
                )

        result.rows = int(total)
    finally:
        conn.close()

    with mysqlutil.connect(source) as myconn:
        result.lag_rows = mysqlutil.count_above(
            myconn, job.table, job.cursor_column, result.watermark_after
        )
    result.duration_seconds = round(time.monotonic() - started, 3)
    log(
        "export",
        job=job.name,
        status=result.status,
        rows=result.rows,
        bytes=result.bytes,
        files=len(result.files),
        watermark=result.watermark_after,
        lag_rows=result.lag_rows,
        duration_seconds=result.duration_seconds,
    )
    return result
