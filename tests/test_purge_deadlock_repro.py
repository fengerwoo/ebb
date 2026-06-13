"""复现：倒挂行在第一轮 purge 幸存后，后续轮次 verify 永久失败（purge 死锁）。"""

from ebb.export import run_export
from ebb.purge import run_purge

from conftest import create_log_table, days_ago, hours_ago, insert_rows, make_config


def test_purge_second_round_after_inverted_survivor(mysql_conn, uniq):
    table = f"logs_{uniq}"
    prefix = f"t/{uniq}"
    config = make_config(table, prefix, retain_seconds=86400)
    job = config.jobs[0]
    create_log_table(mysql_conn, table)

    insert_rows(mysql_conn, table, [days_ago(2)] * 10)  # id 1-10 过期
    insert_rows(mysql_conn, table, [hours_ago(1)])      # id 11 未过期（倒挂）
    insert_rows(mysql_conn, table, [days_ago(2)])       # id 12 过期
    run_export(config, job)

    r1 = run_purge(config, job)
    assert r1.status == "ok"
    assert r1.deleted_rows == 11  # 留下 id 11

    # 时间继续流逝：新写入的 id 13 也过期了（活跃表的常态），并被增量导出
    insert_rows(mysql_conn, table, [days_ago(2)])  # id 13 过期
    run_export(config, job)

    r2 = run_purge(config, job)
    # 设计意图：本轮应把 id 13 删掉（id 11 仍未过期，继续保留）
    assert r2.status == "ok", f"第二轮 purge 未通过校验: status={r2.status} detail={r2.detail}"
    assert r2.deleted_rows == 1

    # 即使 id 11 之后过期，也应能被删掉
    with mysql_conn.cursor() as cur:
        cur.execute(
            f"UPDATE `{table}` SET created_at = %s WHERE id = 11",
            (int(days_ago(2).timestamp()),),
        )
    r3 = run_purge(config, job)
    assert r3.status == "ok", f"第三轮 purge 未通过校验: status={r3.status} detail={r3.detail}"
