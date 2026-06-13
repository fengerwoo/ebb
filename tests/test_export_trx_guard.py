"""活跃事务守卫：小 id 事务晚提交不丢数据。

场景刻意使用「旧时间值 + 未提交」——这是 safety_lag（时间假设）挡不住、
只有机制级守卫能挡住的向量：事务 A 先拿到小 id 不提交，事务 B 的大 id
先提交且时间值早于任何滞后窗口，按旧逻辑水位会越过 A，A 提交后永久漏归档。
"""

import threading
import time

import pymysql

from ebb import mysqlutil
from ebb.export import run_export

from conftest import MYSQL_PORT, create_log_table, days_ago, insert_rows, make_config


def _raw_conn():
    return pymysql.connect(
        host="127.0.0.1", port=MYSQL_PORT, user="root",
        password="ebbtest", database="ebbtest", autocommit=False,
    )


def test_late_commit_low_id_not_lost(mysql_conn, uniq):
    table = f"logs_{uniq}"
    prefix = f"trx/{uniq}"
    config = make_config(table, prefix)  # lag=0：观察窗口为 0，纯靠事务集合判定
    job = config.jobs[0]
    create_log_table(mysql_conn, table)

    ts = int(days_ago(1).timestamp())  # 旧时间值：时间滞后挡不住
    conn_a = _raw_conn()
    try:
        with conn_a.cursor() as cur:  # 事务 A：拿到 id=1，不提交
            cur.execute(
                f"INSERT INTO `{table}` (created_at, user_id, content) "
                f"VALUES ({ts}, 1, 'late')"
            )
        insert_rows(mysql_conn, table, [days_ago(1)])  # 事务 B：id=2，已提交

        r1 = run_export(config, job)
        # 守卫检测到跨观察窗口的写事务：本轮停写，id=2 不能先于 id=1 归档
        assert r1.status == "stalled"
        assert r1.watermark_after == 0

        conn_a.commit()
    finally:
        conn_a.close()

    r2 = run_export(config, job)
    assert r2.status == "ok"
    assert r2.rows == 2
    assert r2.watermark_after == 2


def test_guard_quiet_table_exports_normally(mysql_conn, uniq):
    """没有活跃写事务时守卫零等待、不影响正常导出。"""
    table = f"logs_{uniq}"
    prefix = f"trxq/{uniq}"
    config = make_config(table, prefix)
    job = config.jobs[0]
    create_log_table(mysql_conn, table)
    insert_rows(mysql_conn, table, [days_ago(1)] * 5)

    r = run_export(config, job)
    assert r.status == "ok"
    assert r.rows == 5
    assert r.watermark_after == 5


def test_lock_wait_zero_rows_trx_caught(mysql_conn, uniq):
    """INSERT/UPDATE 先分配自增 id、再做 B-tree 插入；阻塞在锁等待时
    trx_rows_modified=0 但已持有小 id。旧逻辑（只看 modified>0）会漏掉它，
    新逻辑用 trx_state='LOCK WAIT' 兜住——否则它晚提交会落在水位之下漏归档。"""
    table = f"logs_{uniq}"
    create_log_table(mysql_conn, table)
    ts = int(days_ago(1).timestamp())
    with mysql_conn.cursor() as cur:  # 先放一行供争抢行锁（autocommit，立即提交）
        cur.execute(
            f"INSERT INTO `{table}` (created_at, user_id, content) VALUES ({ts}, 1, 'seed')"
        )

    conn_a = _raw_conn()  # 事务 A：锁住 id=1 不放（modified>0）
    conn_b = _raw_conn()  # 事务 B：争抢同一行 → 持续 LOCK WAIT（modified=0）
    with conn_a.cursor() as cur:
        cur.execute(f"UPDATE `{table}` SET content='A' WHERE id=1")

    def _block():
        with conn_b.cursor() as cur:
            cur.execute(f"UPDATE `{table}` SET content='B' WHERE id=1")  # 阻塞至 A 回滚

    blocked = threading.Thread(target=_block, daemon=True)
    try:
        blocked.start()
        deadline = time.time() + 10  # 等 B 进入 LOCK WAIT 且 modified 仍为 0
        while time.time() < deadline:
            with mysql_conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM information_schema.innodb_trx "
                    "WHERE trx_state='LOCK WAIT' AND trx_rows_modified=0"
                )
                if cur.fetchone()[0] >= 1:
                    break
            time.sleep(0.1)

        new_ids = mysqlutil.active_write_trx_ids(mysql_conn)  # 新逻辑
        with mysql_conn.cursor() as cur:  # 旧逻辑：只看 modified>0
            cur.execute(
                "SELECT trx_id FROM information_schema.innodb_trx WHERE trx_rows_modified > 0"
            )
            old_ids = {str(r[0]) for r in cur.fetchall()}
        # 新逻辑比旧逻辑多抓到锁等待中的 0-modified 事务，这正是被堵上的缝隙
        assert new_ids - old_ids, "锁等待中的 0-modified 事务应被新逻辑捕获、被旧逻辑漏掉"
    finally:
        conn_a.rollback()  # 释放锁，B 解除阻塞
        conn_a.close()
        blocked.join(timeout=5)
        conn_b.rollback()
        conn_b.close()
