"""存量回填的集成测试。"""

from datetime import timedelta

from ebb import naming
from ebb.backfill import run_backfill
from ebb.export import get_watermark, run_export
from ebb.s3util import S3Store

from conftest import create_log_table, days_ago, insert_rows, make_config


def _setup(mysql_conn, uniq, **kw):
    table = f"logs_{uniq}"
    prefix = f"t/{uniq}"
    config = make_config(table, prefix, **kw)
    create_log_table(mysql_conn, table)
    return table, prefix, config, config.jobs[0]


def test_backfill_by_day(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq)
    d3, d2, d1 = days_ago(3), days_ago(2), days_ago(1)
    insert_rows(mysql_conn, table, [d3] * 10 + [d2] * 20 + [d1] * 5)

    result = run_backfill(config, job, d3.date(), d1.date())
    assert result.rows == 35
    statuses = {r.dt: r.status for r in result.days}
    assert statuses[d3.date().isoformat()] == "ok"
    assert statuses[d2.date().isoformat()] == "ok"

    store = S3Store(config.storage_of(job))
    files = naming.parse_keys(prefix, store.list_keys(prefix))
    assert all(f.kind == "data" for f in files)
    assert len(files) == 3
    assert get_watermark(store, prefix) == 35


def test_backfill_skips_covered_partitions(mysql_conn, uniq):
    """已有数据文件的分区跳过，避免与增量导出重复。"""
    table, prefix, config, job = _setup(mysql_conn, uniq)
    d1 = days_ago(1)
    insert_rows(mysql_conn, table, [d1] * 10)
    run_export(config, job)  # 增量已覆盖 d1

    result = run_backfill(config, job, d1.date(), d1.date())
    assert result.rows == 0
    assert result.days[0].status == "skip"


def test_backfill_empty_days(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq)
    d5 = days_ago(5)
    result = run_backfill(config, job, d5.date(), (d5 + timedelta(days=1)).date())
    assert result.rows == 0
    assert all(r.status == "empty" for r in result.days)


def test_backfill_then_incremental(mysql_conn, uniq):
    """先回填历史，增量从回填水位继续。"""
    table, prefix, config, job = _setup(mysql_conn, uniq)
    d2, d1 = days_ago(2), days_ago(1)
    insert_rows(mysql_conn, table, [d2] * 10 + [d1] * 10)
    run_backfill(config, job, d2.date(), d1.date())

    insert_rows(mysql_conn, table, [days_ago(0.05)] * 7)  # 新增今天的数据
    result = run_export(config, job)
    assert result.watermark_before == 20
    assert result.rows == 7
    assert result.watermark_after == 27
