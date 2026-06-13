"""SQL 查询执行：供 HTTP /query 与 ebb query 共用。

安全约束（比认证更重要）：
- 每次查询一个全新 DuckDB 连接，只挂 httpfs + S3 secret，不挂 MySQL；
- 仅允许单条 SELECT / WITH 语句（用 DuckDB 解析器判定语句类型）;
- S3 secret 按 job 建、SCOPE 收窄到 bucket/prefix：查询方即使手写
  read_parquet('s3://...') 也只能读各 job 归档前缀下的对象，同 bucket
  的其他路径没有凭据可用；
- 禁用本地文件系统与 http(s) 直读并锁定配置：SELECT 也能调用
  read_text/read_csv 等表函数读服务器本地文件（泄露配置里的数据库/存储
  凭据）或向任意 http 地址发请求（SSRF/数据外带），必须在建好 secret
  与视图之后 SET disabled_filesystems + lock_configuration；
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

    conn = duckdb.connect()
    # 超时 interrupt 后工作线程仍未退出时置 True：此时不能 close（连接
    # 仍被另一线程使用，并发关闭可能让整个进程崩溃），留给 GC 随线程回收
    detached = False
    try:
        if config.storages:
            engine._load_extension(conn, "httpfs")
            engine._load_extension(conn, "icu")
        quote = engine._quote
        # 每个 job 一个 secret，SCOPE 收窄到 bucket/prefix（DuckDB 按最长
        # 匹配选 secret）：API key 只解锁各 job 的归档前缀，不解锁整个 bucket。
        # SCOPE 必须带尾部 /：DuckDB 的 scope 是字符串前缀匹配，不带 / 时
        # prefix=logs 会连带匹配兄弟前缀 logs2/... 造成越权读其他 job 的对象。
        for i, job in enumerate(config.jobs):
            st = config.storage_of(job)
            parts = [
                "TYPE s3",
                f"KEY_ID {quote(st.access_key_id)}",
                f"SECRET {quote(st.secret_access_key)}",
                f"URL_STYLE {quote(st.url_style)}",
                f"USE_SSL {'true' if st.use_ssl else 'false'}",
                f"SCOPE {quote(f's3://{st.bucket}/{job.prefix}/')}",
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

        # 沙箱收口：扩展加载、secret、视图都已就绪，禁用本地文件系统
        # （read_text('/etc/...') 等也是 SELECT，语句类型校验拦不住）与
        # http(s) 直读（SSRF/外带；S3FileSystem 独立注册，s3:// 不受影响），
        # 再锁定配置防止查询把限制改回去。顺序不能动：LOAD 需要读本地磁盘。
        conn.execute("SET disabled_filesystems = 'LocalFileSystem,HTTPFileSystem'")
        conn.execute("SET lock_configuration = true")

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
            if worker.is_alive():
                detached = True
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
        if not detached:
            conn.close()
