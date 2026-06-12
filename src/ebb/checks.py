"""ebb check：上线前体检。

逐项检查：MySQL 连通与表结构（游标列/时间列存在、游标列是整数）、
对象存储读写删、DuckDB 扩展可用、mysql_query 下推可用。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from . import engine, mysqlutil
from .config import Config, JobConfig

INT_TYPES = ("int", "bigint", "mediumint", "smallint", "tinyint")


@dataclass
class CheckItem:
    name: str
    ok: bool
    detail: str = ""


@dataclass
class CheckReport:
    job: str
    items: list[CheckItem] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(i.ok for i in self.items)

    def add(self, name: str, ok: bool, detail: str = "") -> None:
        self.items.append(CheckItem(name=name, ok=ok, detail=detail))


def check_job(config: Config, job: JobConfig) -> CheckReport:
    report = CheckReport(job=job.name)
    source = config.source_of(job)
    storage = config.storage_of(job)

    # MySQL 连接与表结构
    columns: dict[str, str] = {}
    try:
        with mysqlutil.connect(source) as conn:
            columns = mysqlutil.table_columns(conn, job.table)
        report.add("mysql.connect", True, f"table {job.table} has {len(columns)} columns")
    except Exception as exc:
        report.add("mysql.connect", False, f"{type(exc).__name__}: {exc}")

    if columns:
        cur_type = columns.get(job.cursor_column)
        if cur_type is None:
            report.add("mysql.cursor_column", False, f"column not found: {job.cursor_column}")
        elif not any(cur_type.startswith(t) for t in INT_TYPES):
            report.add(
                "mysql.cursor_column", False, f"cursor column must be an integer type, got: {cur_type}"
            )
        else:
            report.add("mysql.cursor_column", True, cur_type)

        time_type = columns.get(job.time_column)
        if time_type is None:
            report.add("mysql.time_column", False, f"column not found: {job.time_column}")
        else:
            expects_int = job.time_column_type.startswith("unix_")
            is_int = any(time_type.startswith(t) for t in INT_TYPES)
            if expects_int != is_int:
                report.add(
                    "mysql.time_column",
                    False,
                    f"time_column_type={job.time_column_type} does not match actual type {time_type}",
                )
            else:
                report.add("mysql.time_column", True, time_type)

    # 对象存储读写删
    try:
        from .s3util import S3Store

        store = S3Store(storage)
        probe = f"{job.prefix}/.ebb-check"
        store.put_probe(probe)
        data = store.get_bytes(probe)
        store.delete_key(probe)
        report.add("storage.rw", data == b"ebb", f"bucket={storage.bucket}")
    except Exception as exc:
        report.add("storage.rw", False, f"{type(exc).__name__}: {exc}")

    # DuckDB 扩展与下推
    try:
        conn = engine.connect(storage=storage, source=source, timezone=job.timezone)
        try:
            one = conn.execute(
                f"SELECT * FROM {engine.mysql_passthrough('SELECT 1')}"
            ).fetchone()
            report.add("duckdb.extensions", one is not None and one[0] == 1)
        finally:
            conn.close()
    except Exception as exc:
        report.add("duckdb.extensions", False, f"{type(exc).__name__}: {exc}")

    return report
