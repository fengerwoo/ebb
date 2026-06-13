"""DuckDB 引擎：搬数据的重活全在这里完成。

每次操作建一个独立的内存连接（连接本身极轻），按需：
- LOAD httpfs + 注册 S3 secret（读写对象存储）；
- LOAD mysql + ATTACH 数据源（通过 mysql_query 把原生 SQL 下推到 MySQL）；
- SET TimeZone（int 时间戳 → 分区日期的本地化）。
"""

from __future__ import annotations

import os
import tempfile

import duckdb

from .config import SourceConfig, StorageConfig

MYSQL_ALIAS = "src"


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _load_extension(conn: duckdb.DuckDBPyConnection, name: str) -> None:
    try:
        conn.execute(f"LOAD {name}")
    except duckdb.Error:
        conn.execute(f"INSTALL {name}")
        conn.execute(f"LOAD {name}")


def connect(
    storage: StorageConfig | None = None,
    source: SourceConfig | None = None,
    timezone: str | None = None,
) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    # 内存连接默认不落盘，单批数据（尤其 backfill 的一整天）超过内存上限
    # 会直接 OOM；指定 temp_directory 后超限自动溢写磁盘，变慢但不变错
    conn.execute(
        f"SET temp_directory = {_quote(os.path.join(tempfile.gettempdir(), 'ebb-duckdb'))}"
    )
    _load_extension(conn, "icu")
    if timezone:
        conn.execute(f"SET TimeZone = {_quote(timezone)}")
    if storage is not None:
        _load_extension(conn, "httpfs")
        parts = [
            "TYPE s3",
            f"KEY_ID {_quote(storage.access_key_id)}",
            f"SECRET {_quote(storage.secret_access_key)}",
            f"URL_STYLE {_quote(storage.url_style)}",
            f"USE_SSL {'true' if storage.use_ssl else 'false'}",
        ]
        if storage.region:
            parts.append(f"REGION {_quote(storage.region)}")
        if storage.duckdb_endpoint:
            parts.append(f"ENDPOINT {_quote(storage.duckdb_endpoint)}")
        conn.execute(f"CREATE OR REPLACE SECRET ebb_s3 ({', '.join(parts)})")
    if source is not None:
        _load_extension(conn, "mysql")
        p = source.parts
        attach = (
            f"host={p['host']} port={p['port']} user={p['user']} "
            f"passwd={p['password']} db={p['database']}"
        )
        conn.execute(f"ATTACH {_quote(attach)} AS {MYSQL_ALIAS} (TYPE mysql, READ_ONLY)")
    return conn


def mysql_passthrough(sql: str) -> str:
    """生成把原生 SQL 下推到 MySQL 执行的 DuckDB 表函数调用。"""
    return f"mysql_query({_quote(MYSQL_ALIAS)}, {_quote(sql)})"


def s3_url(storage: StorageConfig, key: str) -> str:
    return f"s3://{storage.bucket}/{key}"


def s3_glob(storage: StorageConfig, prefix: str) -> str:
    """覆盖全部分区与文件的通配符（inc + data 一视同仁；合并中间产物
    用 .tmp 后缀，不会被命中）。"""
    return f"s3://{storage.bucket}/{prefix}/*/*.parquet"


def read_parquet_expr(storage: StorageConfig, prefix: str) -> str:
    return (
        f"read_parquet({_quote(s3_glob(storage, prefix))}, "
        f"hive_partitioning = true, union_by_name = true)"
    )
