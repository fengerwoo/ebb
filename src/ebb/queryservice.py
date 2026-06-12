"""SQL 查询执行：供 HTTP /query 与 ebb query 共用。

安全约束（比认证更重要）：
- 每次查询一个全新 DuckDB 连接，只挂 httpfs + S3 secret，不挂 MySQL；
- 仅允许单条 SELECT / WITH 语句（用 DuckDB 解析器判定语句类型）;
- 强制超时（后台线程 interrupt）与返回行数上限；
- 为每个 job 注册视图，视图名默认取表名（冲突时退化为 job 名），
  查询方可直接 SELECT ... FROM <表名>。
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass

import duckdb

from . import engine
from .config import Config

_SELECT_RE = re.compile(r"^\s*(select|with|from)\b", re.IGNORECASE)


class QueryRejected(Exception):
    pass


class QueryTimeout(Exception):
    pass


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[list]
    row_count: int
    truncated: bool


def view_names(config: Config) -> dict[str, str]:
    """视图名 -> job 名。表名唯一时用表名，冲突时用 job 名。"""
    by_table: dict[str, list[str]] = {}
    for job in config.jobs:
        by_table.setdefault(job.table, []).append(job.name)
    mapping: dict[str, str] = {}
    for table, jobs in by_table.items():
        if len(jobs) == 1:
            mapping[table] = jobs[0]
        else:
            for name in jobs:
                mapping[name] = name
    return mapping


def _validate(sql: str) -> None:
    if not _SELECT_RE.match(sql):
        raise QueryRejected("仅允许 SELECT / WITH 查询")
    statements = duckdb.extract_statements(sql)
    if len(statements) != 1:
        raise QueryRejected("仅允许单条语句")
    st = statements[0].type
    if st != duckdb.StatementType.SELECT:
        raise QueryRejected(f"仅允许 SELECT / WITH 查询（实际: {st.name}）")


def run_query(
    config: Config,
    sql: str,
    *,
    max_rows: int | None = None,
    timeout_seconds: int | None = None,
) -> QueryResult:
    _validate(sql)
    max_rows = max_rows or config.query_api.max_rows
    timeout_seconds = timeout_seconds or config.query_api.timeout_seconds

    # 任意一个 job 的 storage 都可能被查询，secret 取第一个 storage；
    # 多 storage 时为每个 bucket 建独立 SCOPE secret。
    conn = duckdb.connect()
    try:
        storages = list(config.storages.values())
        if storages:
            engine._load_extension(conn, "httpfs")
            engine._load_extension(conn, "icu")
            quote = engine._quote
            for i, st in enumerate(storages):
                parts = [
                    "TYPE s3",
                    f"KEY_ID {quote(st.access_key_id)}",
                    f"SECRET {quote(st.secret_access_key)}",
                    f"URL_STYLE {quote(st.url_style)}",
                    f"USE_SSL {'true' if st.use_ssl else 'false'}",
                    f"SCOPE {quote('s3://' + st.bucket)}",
                ]
                if st.region:
                    parts.append(f"REGION {quote(st.region)}")
                if st.duckdb_endpoint:
                    parts.append(f"ENDPOINT {quote(st.duckdb_endpoint)}")
                conn.execute(f"CREATE OR REPLACE SECRET ebb_s3_{i} ({', '.join(parts)})")

        jobs_by_name = {j.name: j for j in config.jobs}
        for view, job_name in view_names(config).items():
            job = jobs_by_name[job_name]
            storage = config.storage_of(job)
            try:
                conn.execute(
                    f'CREATE VIEW "{view}" AS SELECT * FROM '
                    f"{engine.read_parquet_expr(storage, job.prefix)}"
                )
            except duckdb.Error:
                # 该 job 还没有任何归档文件（glob 绑定失败）：跳过其视图，
                # 不影响其他 job 的查询
                continue

        holder: dict = {}

        def _run() -> None:
            try:
                cur = conn.execute(sql)
                rows = cur.fetchmany(max_rows + 1)
                holder["columns"] = [d[0] for d in cur.description]
                holder["rows"] = rows
            except Exception as exc:  # noqa: BLE001 传回主线程
                holder["error"] = exc

        worker = threading.Thread(target=_run, daemon=True)
        worker.start()
        worker.join(timeout_seconds)
        if worker.is_alive():
            conn.interrupt()
            worker.join(5)
            raise QueryTimeout(f"查询超时（>{timeout_seconds}s）")
        if "error" in holder:
            raise holder["error"]

        rows = holder["rows"]
        truncated = len(rows) > max_rows
        if truncated:
            rows = rows[:max_rows]
        return QueryResult(
            columns=holder["columns"],
            rows=[list(r) for r in rows],
            row_count=len(rows),
            truncated=truncated,
        )
    finally:
        conn.close()
