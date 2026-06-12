"""时间列的两套转换：

1. MySQL 侧 WHERE 谓词：把「job 时区下的一个时刻」转成对时间列可索引的比较条件
   （purge 截止线、backfill 日切片、导出安全滞后）；
2. DuckDB 侧 dt 推导：从拉回来的时间列值推导分区日期字符串。

时间列类型语义：
- unix_s / unix_ms / unix_us：整数 epoch，绝对时刻，用 job 时区转日期；
- datetime：无时区墙钟，按 job 时区解释，日期直接取墙钟日期；
- timestamp：MySQL 侧按 UTC 瞬间比较（FROM_UNIXTIME，与会话时区无关）；
  拉取值是会话时区渲染的墙钟，分区日期直接取该墙钟日期。
"""

from __future__ import annotations

from datetime import date, datetime

from .config import JobConfig

_FACTOR = {"unix_s": 1, "unix_ms": 1_000, "unix_us": 1_000_000}


def mysql_min_time_expr(job: JobConfig) -> str:
    """取时间列最小值的 SQL 表达式（推导线上最早一天用）。

    timestamp 列转成 UTC epoch 秒返回，规避会话时区对渲染值的影响。
    """
    col = f"`{job.time_column}`"
    if job.time_column_type == "timestamp":
        return f"UNIX_TIMESTAMP(MIN({col}))"
    return f"MIN({col})"


def local_date_of(job: JobConfig, value) -> date:
    """把 mysql_min_time_expr 的查询结果转成 job 时区下的日期。"""
    t = job.time_column_type
    if t in _FACTOR:
        return datetime.fromtimestamp(int(value) / _FACTOR[t], tz=job.tzinfo).date()
    if t == "timestamp":  # 已被 UNIX_TIMESTAMP 转为 epoch 秒
        return datetime.fromtimestamp(float(value), tz=job.tzinfo).date()
    # datetime：无时区墙钟，按 job 时区解释，日期即墙钟日期
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def _ensure_aware(job: JobConfig, local: datetime) -> datetime:
    if local.tzinfo is None:
        return local.replace(tzinfo=job.tzinfo)
    return local.astimezone(job.tzinfo)


def mysql_time_predicate(job: JobConfig, op: str, local: datetime) -> str:
    """生成 `time_col {op} <literal>` 形式的 MySQL 谓词（可走索引）。"""
    assert op in ("<", ">=", "<=", ">")
    aware = _ensure_aware(job, local)
    col = f"`{job.time_column}`"
    t = job.time_column_type
    if t in _FACTOR:
        value = int(aware.timestamp() * _FACTOR[t])
        return f"{col} {op} {value}"
    if t == "datetime":
        return f"{col} {op} '{aware.strftime('%Y-%m-%d %H:%M:%S')}'"
    # timestamp：FROM_UNIXTIME 与列值都按会话时区渲染，比较结果等价于 UTC 比较
    return f"{col} {op} FROM_UNIXTIME({int(aware.timestamp())})"


def duckdb_dt_expr(job: JobConfig) -> str:
    """生成 DuckDB 中从时间列推导 dt（YYYY-MM-DD 字符串）的表达式。

    连接需要先 SET TimeZone = job.timezone（to_timestamp 返回 TIMESTAMPTZ，
    转 TIMESTAMP 时按会话时区本地化）。
    """
    col = f'"{job.time_column}"'
    t = job.time_column_type
    if t in _FACTOR:
        return (
            f"strftime(CAST(to_timestamp(CAST({col} AS DOUBLE) / {_FACTOR[t]}) "
            f"AS TIMESTAMP), '%Y-%m-%d')"
        )
    # datetime / timestamp：拉取值即墙钟，直接取日期
    return f"strftime(CAST({col} AS TIMESTAMP), '%Y-%m-%d')"
