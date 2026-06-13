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


def count_above(conn, table: str, cursor_column: str, above_id: int) -> int:
    v = fetch_value(
        conn, f"SELECT COUNT(*) FROM `{table}` WHERE `{cursor_column}` > {int(above_id)}"
    )
    return int(v or 0)


def autoinc_next(conn, database: str, table: str, column: str) -> int | None:
    """游标列的下一个自增值（已分配 id 的上确界 + 1）；该列不是
    AUTO_INCREMENT 时返回 None。

    MySQL 8 默认缓存 information_schema 统计（最长一天），先把本会话的
    缓存时效清零才能拿到实时值。从未插入过行的表计数器尚未初始化
    （information_schema 中为 NULL），此时返回 1——低估上界永远安全
    （本轮少导，下一轮补上），高估才会漏数据。"""
    with conn.cursor() as cur:
        cur.execute(f"SHOW COLUMNS FROM `{table}` WHERE Field = %s", (column,))
        col = cur.fetchone()
        # SHOW COLUMNS 列序：0=Field ... 5=Extra
        if col is None or "auto_increment" not in str(col[5]).lower():
            conn.commit()
            return None
        cur.execute("SET SESSION information_schema_stats_expiry = 0")
        cur.execute(
            "SELECT AUTO_INCREMENT FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
            (database, table),
        )
        row = cur.fetchone()
    conn.commit()
    return int(row[0]) if row and row[0] is not None else 1


def active_write_trx_ids(conn) -> set[str]:
    """可能持有「已分配但未提交的小 id」的活跃事务集合（需要 PROCESS 权限）。

    纳入两类事务：
    - 已写过行的（trx_rows_modified > 0）：明确持有未提交的自增 id；
    - 锁等待中的（trx_state = 'LOCK WAIT'）：INSERT 先分配自增 id、再做
      B-tree 插入，插入若阻塞在锁等待上，事务此刻已持有小于当前自增计数器
      的 id，而 trx_rows_modified 仍是 0——只看 trx_rows_modified 会放行它，
      它晚于本轮批量读提交就在水位之下永久漏归档。锁等待事务很罕见，纳入
      最多让本轮多停一次，方向保守。

    只读长事务（如 mysqldump --single-transaction）既不持有未提交的自增 id、
    也不会进入 LOCK WAIT，不会阻塞导出。"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT trx_id FROM information_schema.innodb_trx "
            "WHERE trx_rows_modified > 0 OR trx_state = 'LOCK WAIT'"
        )
        rows = cur.fetchall()
    conn.commit()
    return {str(r[0]) for r in rows}


def single_column_unique_columns(conn, table: str) -> set[str]:
    """拥有单列唯一索引（含主键）的列名集合。

    水位推进假设游标列唯一：同 id 多行会在 `id > 水位` 处被部分跳过，
    check 阶段必须能发现这种误配。
    """
    with conn.cursor() as cur:
        cur.execute(f"SHOW INDEX FROM `{table}`")
        rows = cur.fetchall()
    conn.commit()
    # SHOW INDEX 列序：1=Non_unique, 2=Key_name, 3=Seq_in_index, 4=Column_name
    by_key: dict[str, list] = {}
    for r in rows:
        by_key.setdefault(str(r[2]), []).append(r)
    cols: set[str] = set()
    for parts in by_key.values():
        if len(parts) == 1 and int(parts[0][1]) == 0 and parts[0][4] is not None:
            cols.add(str(parts[0][4]))
    return cols


def table_columns(conn, table: str) -> dict[str, str]:
    """列名 -> 类型（小写）。表不存在时抛异常。"""
    with conn.cursor() as cur:
        cur.execute(f"SHOW COLUMNS FROM `{table}`")
        rows = cur.fetchall()
    conn.commit()
    return {r[0]: str(r[1]).lower() for r in rows}


def nullable_columns(conn, table: str) -> set[str]:
    """允许 NULL 的列名集合（SHOW COLUMNS 的 Null 字段为 'YES'）。

    时间列可空是隐患：NULL 时间值会推导出 dt=None 的分区（不参与水位解析、
    却被查询通配符命中），且 purge 的时间谓词对 NULL 恒为假——这些行永远
    不会被清理、永久留在线上。check 阶段据此要求时间列 NOT NULL。"""
    with conn.cursor() as cur:
        cur.execute(f"SHOW COLUMNS FROM `{table}`")
        rows = cur.fetchall()
    conn.commit()
    # SHOW COLUMNS 列序：0=Field, 1=Type, 2=Null（'YES'/'NO'）
    return {str(r[0]) for r in rows if str(r[2]).upper() == "YES"}


def delete_batch(
    conn,
    table: str,
    cursor_column: str,
    bound_id: int,
    limit: int,
    extra_where: str | None = None,
) -> int:
    """按 id 升序删除 id <= bound_id（且满足 extra_where）的一批行，
    返回实际删除行数。每批独立提交。

    extra_where 用于带上时间条件：bound_id 只是「过期行的最大 id」，
    区间内可能混有时间倒挂的未过期行，必须靠时间条件保住。
    """
    where = f"`{cursor_column}` <= {int(bound_id)}"
    if extra_where:
        where += f" AND ({extra_where})"
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM `{table}` WHERE {where} "
            f"ORDER BY `{cursor_column}` LIMIT {int(limit)}"
        )
        affected = cur.rowcount
    conn.commit()
    return affected
