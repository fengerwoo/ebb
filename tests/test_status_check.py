"""check 与 status 的集成测试。"""

from ebb.checks import check_job
from ebb.status import job_status

from conftest import create_log_table, hours_ago, insert_rows, make_config


def _setup(mysql_conn, uniq, **kw):
    table = f"logs_{uniq}"
    prefix = f"t/{uniq}"
    config = make_config(table, prefix, **kw)
    create_log_table(mysql_conn, table, time_type=kw.get("time_column_type", "unix_s"))
    return table, prefix, config, config.jobs[0]


def test_check_all_green(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq)
    report = check_job(config, job)
    assert report.ok, [f"{i.name}: {i.detail}" for i in report.items if not i.ok]
    names = {i.name for i in report.items}
    assert names == {"mysql.connect", "mysql.cursor_column", "mysql.time_column",
                     "storage.rw", "duckdb.extensions"}


def test_check_missing_table(mysql_conn, uniq):
    config = make_config(f"nonexistent_{uniq}", f"t/{uniq}")
    report = check_job(config, config.jobs[0])
    assert not report.ok


def test_check_missing_time_column(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq)
    config2 = make_config(table, prefix, time_column="no_such_col")
    report = check_job(config2, config2.jobs[0])
    assert any(i.name == "mysql.time_column" and not i.ok for i in report.items)


def test_check_type_mismatch(mysql_conn, uniq):
    """datetime 列配成 unix_s 应当报错。"""
    table, prefix, config, job = _setup(mysql_conn, uniq, time_column_type="datetime")
    bad = make_config(table, prefix, time_column_type="unix_s")
    report = check_job(bad, bad.jobs[0])
    assert any(i.name == "mysql.time_column" and not i.ok for i in report.items)


def test_check_bad_storage(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq)
    config.storages["minio"].secret_access_key = "wrong"
    report = check_job(config, config.jobs[0])
    assert any(i.name == "storage.rw" and not i.ok for i in report.items)


def test_status_lag_and_watermark(mysql_conn, uniq):
    from ebb.export import run_export

    table, prefix, config, job = _setup(mysql_conn, uniq, export_rows=30)
    insert_rows(mysql_conn, table, [hours_ago(2)] * 50)

    s0 = job_status(config, job)
    assert s0.watermark == 0 and s0.max_id == 50 and s0.lag_rows == 50
    assert s0.file_count == 0

    run_export(config, job)  # 导出 30 行
    s1 = job_status(config, job)
    assert s1.watermark == 30
    assert s1.lag_rows == 20
    assert s1.file_count == 1
    assert s1.last_export_at is not None
