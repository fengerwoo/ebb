"""MySQL 控制面操作（pymysql）：计数、最大 id、分批删除。

数据面的批量读取走 DuckDB mysql 扩展，这里只做轻量查询与删除。
"""

from __future__ import annotations

from contextlib import contextmanager

import pymysql

from .config import SourceConfig


@contextmanager
def connect(source: SourceConfig):
    parts = source.parts
    conn = pymysql.connect(
        host=parts["host"],
        port=parts["port"],
        user=parts["user"],
        password=parts["password"],
        database=parts["database"],
        charset="utf8mb4",
        autocommit=False,
    )
    try:
        yield conn
    finally:
        conn.close()


def fetch_value(conn, sql: str):
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    conn.commit()  # 释放快照
    return row[0] if row else None


def fetch_row(conn, sql: str):
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    conn.commit()
    return row


def max_id(conn, table: str, cursor_column: str) -> int:
    v = fetch_value(conn, f"SELECT MAX(`{cursor_column}`) FROM `{table}`")
    return int(v) if v is not None else 0


def min_id(conn, table: str, cursor_column: str) -> int | None:
    v = fetch_value(conn, f"SELECT MIN(`{cursor_column}`) FROM `{table}`")
    return int(v) if v is not None else None


def count_above(conn, table: str, cursor_column: str, above_id: int) -> int:
    v = fetch_value(
        conn, f"SELECT COUNT(*) FROM `{table}` WHERE `{cursor_column}` > {int(above_id)}"
    )
    return int(v or 0)


def count_and_sum_range(
    conn, table: str, cursor_column: str, from_id: int, to_id: int
) -> tuple[int, int]:
    """[from_id, to_id] 闭区间内的行数与 id 和（用于删除前校验）。"""
    row = fetch_row(
        conn,
        f"SELECT COUNT(*), COALESCE(SUM(`{cursor_column}`), 0) FROM `{table}` "
        f"WHERE `{cursor_column}` BETWEEN {int(from_id)} AND {int(to_id)}",
    )
    return int(row[0]), int(row[1])


def table_columns(conn, table: str) -> dict[str, str]:
    """列名 -> 类型（小写）。表不存在时抛异常。"""
    with conn.cursor() as cur:
        cur.execute(f"SHOW COLUMNS FROM `{table}`")
        rows = cur.fetchall()
    conn.commit()
    return {r[0]: str(r[1]).lower() for r in rows}


def delete_batch(conn, table: str, cursor_column: str, bound_id: int, limit: int) -> int:
    """删除 id <= bound_id 的一批行，返回实际删除行数。每批独立提交。"""
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM `{table}` WHERE `{cursor_column}` <= {int(bound_id)} "
            f"LIMIT {int(limit)}"
        )
        affected = cur.rowcount
    conn.commit()
    return affected
