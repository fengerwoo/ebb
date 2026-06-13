"""存量回填：按天切片，把历史数据导入对象存储。

与增量导出共用同一套「拉取 → 推导 dt → 写 Parquet」逻辑，区别：
- 用时间窗口（某天 00:00 ~ 次日 00:00，job 时区）圈定数据而不是 id 游标；
- 直接写 data-{from}-{to}.parquet（天级文件，无需再合并）；
- 已被完整覆盖的分区跳过，避免与增量导出的数据重复。

回填会抬高水位（水位 = 全部文件 to_id 最大值），水位之下的未归档行会被
增量导出永久跳过。为保证回填不制造这种空洞，本轮的 id 宇宙冻结在
id_cap = max(当前水位, 区间内 MAX(id))，并设两道守卫：

1. 前置守卫：区间外仍有 (水位, id_cap] 的未归档行（id 大于水位即未归档，
   水位语义保证）时拒绝执行——执行下去水位会越过它们。可 --force 绕过；
2. 分区检查：已有文件的分区若当日窗口内还有 (水位, id_cap] 的行，说明
   只是部分覆盖（如增量推进到当天中途），报错而不是静默跳过，不可绕过。

日切 SQL 同样以 id_cap 为上界：运行期间新写入的行（含时间倒挂的旧时间值）
一律留给增量或下一次回填，最终水位不会越过任何未归档行。
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Callable

from . import engine, mysqlutil, naming
from .config import Config, JobConfig
from .export import get_watermark
from .logs import log
from .s3util import S3Store
from .timeutil import (
    duckdb_dt_expr,
    internal_columns,
    local_date_of,
    mysql_min_time_expr,
    mysql_select_clause,
    mysql_time_predicate,
)


class BackfillRefused(RuntimeError):
    """危险的回填被拒绝：继续执行会制造归档空洞。"""


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
    # 跨天 id 交错的相邻 (前一天, 后一天) 列表：非空表示回填中断后必须重跑
    # 同一区间，否则较晚一天里 id 小于已写水位的行会被增量永久跳过。
    interleave: list[tuple[str, str]] = field(default_factory=list)


def earliest_day(config: Config, job: JobConfig) -> date | None:
    """线上数据最早一天（job 时区）；表为空返回 None。CLI 缺省 --from 时使用。"""
    source = config.source_of(job)
    with mysqlutil.connect(source) as conn:
        v = mysqlutil.fetch_value(
            conn, f"SELECT {mysql_min_time_expr(job)} FROM `{job.table}`"
        )
    return local_date_of(job, v) if v is not None else None


def _range_predicate(job: JobConfig, from_day: date, to_day: date) -> str:
    """[from_day 00:00, to_day+1 00:00)（job 时区）的 MySQL 时间谓词。"""
    start = datetime(from_day.year, from_day.month, from_day.day, tzinfo=job.tzinfo)
    end = datetime(to_day.year, to_day.month, to_day.day, tzinfo=job.tzinfo) + timedelta(days=1)
    pred_lo = mysql_time_predicate(job, ">=", start)
    pred_hi = mysql_time_predicate(job, "<", end)
    return f"{pred_lo} AND {pred_hi}"


def count_rows_between(config: Config, job: JobConfig, from_day: date, to_day: date) -> int:
    """统计回填区间内的线上总行数，用于行级进度与剩余时间估算。"""
    source = config.source_of(job)
    with mysqlutil.connect(source) as conn:
        v = mysqlutil.fetch_value(
            conn,
            f"SELECT COUNT(*) FROM `{job.table}` "
            f"WHERE {_range_predicate(job, from_day, to_day)}",
        )
    return int(v or 0)


def _day_sql(job: JobConfig, day: date, id_cap: int | None) -> str:
    where = [_range_predicate(job, day, day)]
    if id_cap is not None:
        # 冻结 id 宇宙：运行期间新写入的行留给增量，最终水位不越过 id_cap
        where.append(f"`{job.cursor_column}` <= {int(id_cap)}")
    return (
        f"SELECT {mysql_select_clause(job)} FROM `{job.table}` "
        f"WHERE {' AND '.join(where)} "
        f"ORDER BY `{job.cursor_column}`"
    )


def _unarchived_in_day(conn, job: JobConfig, day: date, watermark: int, id_cap: int | None) -> int:
    """当日窗口内「id 大于水位（即未归档）且不超过 id_cap」的行数。"""
    cur = job.cursor_column
    where = [_range_predicate(job, day, day), f"`{cur}` > {int(watermark)}"]
    if id_cap is not None:
        where.append(f"`{cur}` <= {int(id_cap)}")
    v = mysqlutil.fetch_value(
        conn, f"SELECT COUNT(*) FROM `{job.table}` WHERE {' AND '.join(where)}"
    )
    return int(v or 0)


def backfill_day(
    config: Config,
    job: JobConfig,
    day: date,
    *,
    watermark: int = 0,
    id_cap: int | None = None,
) -> BackfillDayResult:
    dt_str = day.isoformat()
    storage = config.storage_of(job)
    source = config.source_of(job)
    store = S3Store(storage)

    existing = store.list_keys(naming.partition_dir(job.prefix, dt_str))
    if naming.parse_keys(job.prefix, existing):
        # 已有文件最多覆盖到水位（水位 = 全部文件 to_id 最大值）；当日窗口内
        # 若还有水位之上的行，说明分区只是部分覆盖，静默跳过会留下永久空洞
        with mysqlutil.connect(source) as myconn:
            pending = _unarchived_in_day(myconn, job, day, watermark, id_cap)
        if pending:
            raise BackfillRefused(
                f"分区 dt={dt_str} 已有数据文件，但当日窗口内仍有 {pending} 行 "
                f"id 大于水位（{watermark}）的未归档数据：分区只是部分覆盖"
                f"（例如增量只推进到当天中途），跳过会留下空洞。"
                f"请等增量追平该分区后重试，或删除该分区的文件后重新回填这一天"
            )
        log("backfill", job=job.name, dt=dt_str, status="skip", reason="分区已完整覆盖")
        return BackfillDayResult(dt=dt_str, status="skip")

    result = BackfillDayResult(dt=dt_str, status="empty")
    conn = engine.connect(storage=storage, source=source, timezone=job.timezone)
    try:
        dt_expr = duckdb_dt_expr(job)
        conn.execute(
            f"CREATE TEMP TABLE batch AS "
            f"SELECT *, {dt_expr} AS __dt "
            f"FROM {engine.mysql_passthrough(_day_sql(job, day, id_cap))}"
        )
        cursor = f'"{job.cursor_column}"'
        groups = conn.execute(
            f"SELECT __dt, min({cursor}), max({cursor}), count(*) "
            f"FROM batch GROUP BY __dt ORDER BY min({cursor})"
        ).fetchall()
        exclude = ", ".join(f'"{c}"' for c in internal_columns(job))
        for dt_val, from_id, to_id, rows in groups:
            key = naming.data_key(job.prefix, dt_val, int(from_id), int(to_id))
            conn.execute(
                f"COPY (SELECT * EXCLUDE ({exclude}) FROM batch WHERE __dt = '{dt_val}' "
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


def _preflight(conn, job: JobConfig, watermark: int, from_day: date, to_day: date) -> tuple[int, int]:
    """计算本轮 id 上界与「区间外的水位下空洞」行数。

    id_cap = max(水位, 区间内 MAX(id))，是本轮结束后水位可能到达的最大值。
    区间外若存在 (水位, id_cap] 的行，回填会把水位抬过这些未归档行，
    增量导出（只看 id > 水位）从此永久跳过它们。"""
    cur = job.cursor_column
    range_pred = _range_predicate(job, from_day, to_day)
    v = mysqlutil.fetch_value(
        conn, f"SELECT MAX(`{cur}`) FROM `{job.table}` WHERE {range_pred}"
    )
    id_cap = max(watermark, int(v) if v is not None else 0)
    gap = 0
    if id_cap > watermark:
        gap = int(
            mysqlutil.fetch_value(
                conn,
                f"SELECT COUNT(*) FROM `{job.table}` "
                f"WHERE `{cur}` > {int(watermark)} AND `{cur}` <= {int(id_cap)} "
                f"AND NOT ({range_pred})",
            )
            or 0
        )
    return id_cap, gap


def _cross_day_id_overlap(
    conn, job: JobConfig, from_day: date, to_day: date, id_cap: int
) -> list[tuple[str, str]]:
    """检测相邻日期的 id 区间是否重叠（跨天 id 交错），用于回填前的警告。

    追加表正常情况下 id 随时间单调，逐日 id 区间互不相交。时间列与 id 倒挂
    （如午夜前后数秒级时钟回拨）会让靠近边界的行落到隔壁天，相邻两天 id 区间
    相交。回填逐天写文件、水位取已写文件 to_id 的最大值；若在相交的两天之间
    中断、又改用增量而非重跑同一区间，较晚一天里 id 小于已写水位的行会被增量
    （只看 id > 水位）永久跳过。

    逐天取 (MIN id, MAX id)（限定 id <= id_cap，与实际写入范围一致，空天跳过），
    相邻非空天的后者 MIN <= 前者 MAX 即为重叠。返回重叠的相邻 (前一天, 后一天)
    日期串列表。这是纯诊断查询，不改变任何写入行为。
    """
    cur = job.cursor_column
    spans: list[tuple[str, int, int]] = []
    day = from_day
    while day <= to_day:
        row = mysqlutil.fetch_row(
            conn,
            f"SELECT MIN(`{cur}`), MAX(`{cur}`) FROM `{job.table}` "
            f"WHERE {_range_predicate(job, day, day)} AND `{cur}` <= {int(id_cap)}",
        )
        if row and row[0] is not None:
            spans.append((day.isoformat(), int(row[0]), int(row[1])))
        day = day + timedelta(days=1)
    overlaps: list[tuple[str, str]] = []
    for (prev_dt, _, prev_max), (next_dt, next_min, _) in zip(spans, spans[1:]):
        if next_min <= prev_max:
            overlaps.append((prev_dt, next_dt))
    return overlaps


def run_backfill(
    config: Config,
    job: JobConfig,
    from_day: date,
    to_day: date,
    *,
    force: bool = False,
    on_progress: Callable[[dict], None] | None = None,
    on_warning: Callable[[str], None] | None = None,
) -> BackfillResult:
    if from_day > to_day:
        raise ValueError("--from must not be later than --to")
    started = time.monotonic()
    watermark = get_watermark(S3Store(config.storage_of(job)), job.prefix)
    with mysqlutil.connect(config.source_of(job)) as conn:
        id_cap, gap = _preflight(conn, job, watermark, from_day, to_day)
        overlaps = _cross_day_id_overlap(conn, job, from_day, to_day, id_cap)
    if gap and not force:
        raise BackfillRefused(
            f"回填区间外仍有 {gap} 行未归档数据会落在回填后的水位之下"
            f"（id ∈ ({watermark}, {id_cap}] 且时间不在 {from_day} ~ {to_day} 内）："
            f"继续执行会让增量导出永久跳过这些行。请扩大回填区间覆盖它们所在的日期，"
            f"或确认无须归档后用 --force 强制执行"
        )
    result = BackfillResult(job=job.name)
    result.interleave = overlaps
    if overlaps:
        pairs = ", ".join(f"{a}/{b}" for a, b in overlaps)
        msg = (
            f"检测到跨天 id 交错（相邻日期 id 区间重叠：{pairs}）。回填按日期顺序"
            f"逐天写文件并抬高水位，若中途中断且不重跑相同 --from/--to 区间，较晚"
            f"一天里 id 小于已写水位的行会被增量导出永久跳过。中断后务必重跑同一区间"
        )
        log("backfill", job=job.name, status="interleave_warning", pairs=pairs)
        if on_warning:
            on_warning(msg)
    day = from_day
    total_days = (to_day - from_day).days + 1
    total_rows = count_rows_between(config, job, from_day, to_day) if on_progress else 0
    processed_rows = 0  # 已写入 + 已跳过（skip 天按线上行数计），用于进度与 ETA
    while day <= to_day:
        day_result = backfill_day(config, job, day, watermark=watermark, id_cap=id_cap)
        result.days.append(day_result)
        result.rows += day_result.rows
        result.bytes += day_result.bytes
        if on_progress:
            if day_result.status == "skip":
                processed_rows += count_rows_between(config, job, day, day)
            else:
                processed_rows += day_result.rows
            on_progress(
                {
                    "days_done": len(result.days),
                    "days_total": total_days,
                    "rows": result.rows,
                    "processed_rows": processed_rows,
                    "total_rows": total_rows,
                    "current_day": day.isoformat(),
                    "current_status": day_result.status,
                    "elapsed_seconds": time.monotonic() - started,
                }
            )
        day = day + timedelta(days=1)
    result.duration_seconds = round(time.monotonic() - started, 3)
    return result
