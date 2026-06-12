"""serve 常驻模式的集成测试：调度执行 + 管理端点。"""

import threading
import time

import httpx
import pytest

from ebb.scheduler import serve

from conftest import create_log_table, hours_ago, insert_rows, make_config

ADMIN_PORT = 38082
ADMIN_PORT_PURGE = 38083


@pytest.fixture()
def served(mysql_conn, uniq):
    table = f"logs_{uniq}"
    prefix = f"t/{uniq}"
    config = make_config(table, prefix)
    config.jobs[0].schedule.interval_seconds = 2
    config.admin.listen = f"127.0.0.1:{ADMIN_PORT}"
    create_log_table(mysql_conn, table)
    insert_rows(mysql_conn, table, [hours_ago(2)] * 40)

    stop = threading.Event()
    t = threading.Thread(target=serve, args=(config,), kwargs={"stop_event": stop})
    t.start()
    yield table, prefix, config
    stop.set()
    t.join(timeout=30)
    assert not t.is_alive()


def _wait_admin(predicate, timeout=30, port=ADMIN_PORT):
    url = f"http://127.0.0.1:{port}/admin/jobs"
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = httpx.get(url, timeout=2).json()["jobs"]
            if predicate(last):
                return last
        except Exception:
            pass
        time.sleep(0.5)
    raise AssertionError(f"管理端点未达到预期状态: {last}")


def test_serve_exports_on_schedule_and_reports(served, mysql_conn):
    table, prefix, config = served

    # 启动后第一轮立即执行；等导出完成并出现在注册表
    entries = _wait_admin(
        lambda jobs: any(
            e["kind"] == "export"
            and e["state"] == "idle"
            and (e.get("last_result") or {}).get("rows") == 40
            for e in jobs
        )
    )
    export_entry = next(e for e in entries if e["kind"] == "export")
    assert export_entry["last_result"]["status"] == "ok"
    assert export_entry["last_result"]["watermark_after"] == 40
    assert export_entry.get("next_run_at")

    # 第二轮空轮
    _wait_admin(
        lambda jobs: any(
            e["kind"] == "export"
            and (e.get("last_result") or {}).get("status") == "empty"
            for e in jobs
        )
    )

    # 新数据进来后被下一轮追上
    insert_rows(mysql_conn, table, [hours_ago(1)] * 15)
    _wait_admin(
        lambda jobs: any(
            (e.get("last_result") or {}).get("watermark_after") == 55 for e in jobs
        )
    )


def test_serve_purge_on_interval(mysql_conn, uniq):
    """purge_interval_seconds：不等每日 compact，按间隔独立删除过期数据。"""
    table = f"logs_{uniq}"
    prefix = f"t/{uniq}"
    config = make_config(table, prefix, retain_seconds=3600)
    config.jobs[0].schedule.interval_seconds = 2
    config.jobs[0].schedule.purge_interval_seconds = 2
    config.admin.listen = f"127.0.0.1:{ADMIN_PORT_PURGE}"
    create_log_table(mysql_conn, table)
    insert_rows(mysql_conn, table, [hours_ago(2)] * 30)  # 全部早于保留期

    stop = threading.Event()
    t = threading.Thread(target=serve, args=(config,), kwargs={"stop_event": stop})
    t.start()
    try:
        # 导出先归档，随后的 purge 轮把 30 行全删掉
        _wait_admin(
            lambda jobs: any(
                e["kind"] == "purge"
                and (e.get("last_result") or {}).get("deleted_rows") == 30
                for e in jobs
            ),
            port=ADMIN_PORT_PURGE,
        )
    finally:
        stop.set()
        t.join(timeout=30)
    assert not t.is_alive()
    with mysql_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM `{table}`")
        assert cur.fetchone()[0] == 0
