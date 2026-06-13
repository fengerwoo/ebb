"""增量导出：MySQL → Parquet → 对象存储。

水位 = 文件名，先上传、后（隐式）提交：上传成功的瞬间，下一轮 LIST 自然看到新水位。

正确性关键点：
1. 批内按「分区日期连续段」切文件——按 id 排序后，dt 发生变化即切段，
   再按 id 顺序逐段上传。任何时刻崩溃，已上传文件覆盖的 id 必然是
   「从旧水位起的连续区间」，重启后从新水位继续，不丢不重；
2. 同一水位重跑产生完全相同的文件名，S3 PUT 原子覆盖，天然幂等；
3. safety_lag_seconds：先取「时间早于 now - lag 的行」里从水位起一批的
   MAX(id) 作为本批 id 上界，再按纯 id 区间导出。用 id 截断而不是按时间
   过滤行：时间列与 id 在截止线附近倒挂时，按时间过滤会把「小 id、新时间」
   的行永久跳过（水位被更大的 id 抬高后不再回看），按 id 截断则区间内的行
   无论时间值如何都会被导出，不漏；
4. trx_guard：时间滞后只在「时间列 ≈ 提交时间且事务时长 < lag」的假设下
   成立，守卫用「自增计数器 + 活跃写事务观察窗口」给出机制级的安全 id
   上界（见 _trx_guard_cap），检测到跨窗口写事务时本轮停写而不是冒险。
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
from .timeutil import duckdb_dt_expr, internal_columns, mysql_select_clause, mysql_time_predicate

ProgressFn = Callable[[dict], None]


@dataclass
class ExportResult:
    job: str
    status: str  # ok / empty / stalled / dry-run
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


def _trx_guard_cap(conn, job: JobConfig, database: str) -> int | None:
    """活跃事务守卫：返回本轮安全 id 上界（含）；存在跨观察窗口的写事务时
    返回 None（本轮停写，等它结束后下轮重试，不丢数据）。

    原理：自增计数器单调递增 ⇒ T1 读到的「下一个自增值」A1 之下的 id 已经
    全部分配完毕，其中未提交的必属于「T1 时已修改过行的活跃事务」。等待
    一个观察窗口（safety_lag_seconds，上限 30s）后这批事务全部结束，则
    id < A1 已尘埃落定（提交或永久作废），A1-1 即安全上界；仍有事务横跨
    窗口则它持有的 id 无从得知，本轮放弃。时间滞后只能在「时间列 ≈ 提交
    时间」的假设下缓解漏读，守卫不依赖时间列语义，旧时间值晚提交同样挡住。

    两个实现要点：
    - 读取顺序必须先 A1 后事务集合，反过来会漏掉两次读取之间开始写的事务；
    - InnoDB 的 information_schema 表是缓冲视图（至多每 0.1s 刷新），快照
      可能落后真实状态：读 A1 之后必须等过一个缓存周期再取事务集合，否则
      「A1 之前刚开始写」的事务可能尚未出现在视图里（不安全方向）。等待后
      快照只会偏旧（多看到刚结束的事务），方向保守，最多误停一轮。
    """
    a1 = mysqlutil.autoinc_next(conn, database, job.table, job.cursor_column)
    if a1 is None:
        raise RuntimeError(
            f"trx_guard 需要游标列为 AUTO_INCREMENT（表 {job.table} 无自增计数器）；"
            f"请确认表结构，或显式设置 batch.trx_guard=false 关闭守卫"
        )
    time.sleep(0.2)  # 越过 innodb_trx 的 0.1s 缓存周期，保证快照不早于 A1 时刻
    try:
        trx1 = mysqlutil.active_write_trx_ids(conn)
    except Exception as exc:
        raise RuntimeError(
            "trx_guard 读取 information_schema.innodb_trx 失败（通常缺少 PROCESS 权限）；"
            "请授权或显式设置 batch.trx_guard=false 关闭守卫"
        ) from exc
    if trx1:
        time.sleep(min(job.batch.safety_lag_seconds, 30))
        if trx1 & mysqlutil.active_write_trx_ids(conn):
            return None
    return a1 - 1


def _safe_bound(
    conn, job: JobConfig, watermark: int, now: datetime, id_cap: int | None = None
) -> int | None:
    """本批的安全 id 上界：时间早于 now - lag 的行里，从水位起按 id 序
    取一批的 MAX(id)。

    上界行已提交且时间值早于安全线，按 safety lag 的假设其更小 id 的行
    均已可见；批查询用纯 id 区间（不带时间条件），区间内时间倒挂的行也
    会被导出。窗口内没有可导出的行时返回 None。id_cap 是 trx_guard 给出的
    上界，时间窗口的选取不越过它。
    """
    cutoff = datetime.fromtimestamp(
        now.timestamp() - job.batch.safety_lag_seconds, tz=job.tzinfo
    )
    cur = job.cursor_column
    where = [
        f"`{cur}` > {int(watermark)}",
        mysql_time_predicate(job, "<", cutoff),
    ]
    if id_cap is not None:
        where.append(f"`{cur}` <= {int(id_cap)}")
    v = mysqlutil.fetch_value(
        conn,
        f"SELECT MAX(`{cur}`) FROM ("
        f"SELECT `{cur}` FROM `{job.table}` "
        f"WHERE {' AND '.join(where)} "
        f"ORDER BY `{cur}` LIMIT {job.batch.export_rows}"
        f") AS t",
    )
    return int(v) if v is not None else None


def _batch_sql(job: JobConfig, watermark: int, bound: int | None) -> str:
    """下推到 MySQL 的增量查询。bound 为 None 表示不设 id 上界（lag=0）。

    保留 LIMIT：理论上 (水位, bound] 内可能混入超出批大小的时间倒挂行，
    截断后剩余部分仍是连续区间，下一轮继续，内存有界。
    """
    where = [f"`{job.cursor_column}` > {int(watermark)}"]
    if bound is not None:
        where.append(f"`{job.cursor_column}` <= {int(bound)}")
    return (
        f"SELECT {mysql_select_clause(job)} FROM `{job.table}` "
        f"WHERE {' AND '.join(where)} "
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

    bound: int | None = None  # None = 不设 id 上界
    if job.batch.trx_guard or job.batch.safety_lag_seconds > 0:
        with mysqlutil.connect(source) as conn:
            if job.batch.trx_guard:
                cap = _trx_guard_cap(conn, job, source.parts["database"])
                if cap is None:
                    # 有写事务横跨观察窗口：其持有的 id 可能小于任何可见 id，
                    # 本轮停写等它结束；下一轮重新判定，不丢数据
                    result.status = "stalled"
                    result.watermark_after = watermark
                    result.duration_seconds = round(time.monotonic() - started, 3)
                    log("export", job=job.name, status="stalled", watermark=watermark)
                    return result
                bound = cap
            if job.batch.safety_lag_seconds > 0:
                lag_bound = _safe_bound(conn, job, watermark, now, bound)
                # 时间窗口内没有可导出的行：上界设为水位，构造空区间，
                # 走下面统一的 empty / dry-run 路径
                bound = lag_bound if lag_bound is not None else watermark
    batch_sql = _batch_sql(job, watermark, bound)

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
        exclude = ", ".join(f'"{c}"' for c in internal_columns(job))
        for dt, from_id, to_id, seg_rows in segments:
            key = naming.inc_key(job.prefix, dt, int(from_id), int(to_id))
            url = engine.s3_url(storage, key)
            conn.execute(
                f"COPY (SELECT * EXCLUDE ({exclude}) FROM batch "
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
