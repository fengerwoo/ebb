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
                     "mysql.trx_guard", "mysql.reserved_columns",
                     "storage.rw", "duckdb.extensions"}


def test_check_reserved_column_conflict(mysql_conn, uniq):
    """业务表占用 dt / __dt / __ebb_ts 这类内部名时 check 必须报错。"""
    table = f"logs_{uniq}"
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"CREATE TABLE `{table}` ("
            f"id BIGINT AUTO_INCREMENT PRIMARY KEY, "
            f"created_at BIGINT NOT NULL, dt VARCHAR(10))"
        )
    config = make_config(table, f"t/{uniq}")
    report = check_job(config, config.jobs[0])
    assert any(i.name == "mysql.reserved_columns" and not i.ok for i in report.items)


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


def test_check_nullable_time_column(mysql_conn, uniq):
    """时间列可空必须报错：NULL 时间值生成 dt=None 垃圾分区，且 purge 时间谓词
    对 NULL 恒假——这些行永远不会被清理、永久留在线上。"""
    table = f"logs_{uniq}"
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"""
            CREATE TABLE `{table}` (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                created_at BIGINT NULL,
                user_id INT NOT NULL
            )
            """
        )
    config = make_config(table, f"t/{uniq}")  # 默认 unix_s，类型匹配，只差可空
    report = check_job(config, config.jobs[0])
    assert any(
        i.name == "mysql.time_column" and not i.ok and "NOT NULL" in i.detail
        for i in report.items
    )


def test_check_cursor_without_unique_index(mysql_conn, uniq):
    """游标列没有单列唯一索引（主键/UNIQUE）时 check 必须报错：
    游标不唯一会破坏水位语义（同 id 行被部分跳过）。"""
    table = f"logs_{uniq}"
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"""
            CREATE TABLE `{table}` (
                id BIGINT NOT NULL,
                created_at BIGINT NOT NULL,
                KEY idx_id (id)
            )
            """
        )
    config = make_config(table, f"t/{uniq}")
    report = check_job(config, config.jobs[0])
    assert any(
        i.name == "mysql.cursor_column" and not i.ok and "unique" in i.detail
        for i in report.items
    )


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
