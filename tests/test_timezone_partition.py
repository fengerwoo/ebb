"""timestamp 列的分区日期必须按 job 时区推导，不受 MySQL 会话时区影响。

测试 MySQL 的会话时区为 +08:00（conftest.TZ），job 时区设为 UTC：
同一瞬间在两个时区落在不同日期时，dt 必须取 job 时区（UTC）的日期。
修复前 dt 直接取会话时区渲染的墙钟日期，会写错一天。
"""

from datetime import datetime, timedelta, timezone

from ebb import naming
from ebb.backfill import run_backfill
from ebb.export import run_export
from ebb.s3util import S3Store

from conftest import TZ, create_log_table, insert_rows, make_config


def _cross_boundary_instant() -> datetime:
    """挑一个「+08 与 UTC 日期不同」的过去时刻：昨天(+08) 04:00，
    即 UTC 的前天 20:00。"""
    base = datetime.now(tz=TZ) - timedelta(days=1)
    return base.replace(hour=4, minute=0, second=0, microsecond=0)


def test_export_timestamp_dt_follows_job_timezone(mysql_conn, uniq):
    table = f"logs_{uniq}"
    prefix = f"tz/{uniq}"
    config = make_config(table, prefix, time_column_type="timestamp", timezone="UTC")
    job = config.jobs[0]
    create_log_table(mysql_conn, table, time_type="timestamp")

    instant = _cross_boundary_instant()
    expected_dt = instant.astimezone(timezone.utc).date().isoformat()
    assert expected_dt != instant.date().isoformat()  # 两个时区确实跨日

    insert_rows(mysql_conn, table, [instant] * 5, time_type="timestamp")
    result = run_export(config, job)
    assert result.rows == 5

    files = naming.parse_keys(prefix, S3Store(config.storage_of(job)).list_keys(prefix))
    assert {f.dt for f in files} == {expected_dt}


def test_backfill_timestamp_dt_follows_job_timezone(mysql_conn, uniq):
    table = f"logs_{uniq}"
    prefix = f"tzb/{uniq}"
    config = make_config(table, prefix, time_column_type="timestamp", timezone="UTC")
    job = config.jobs[0]
    create_log_table(mysql_conn, table, time_type="timestamp")

    instant = _cross_boundary_instant()
    expected_day = instant.astimezone(timezone.utc).date()
    insert_rows(mysql_conn, table, [instant] * 3, time_type="timestamp")

    result = run_backfill(config, job, expected_day, expected_day)
    assert result.rows == 3

    files = naming.parse_keys(prefix, S3Store(config.storage_of(job)).list_keys(prefix))
    assert {f.dt for f in files} == {expected_day.isoformat()}
