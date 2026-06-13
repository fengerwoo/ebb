"""存量回填的集成测试。"""

from datetime import timedelta

import pytest

from ebb import naming
from ebb.backfill import BackfillRefused, earliest_day, run_backfill
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


def test_backfill_refuses_gap_outside_range(mysql_conn, uniq):
    """增量推进到一半（水位停在更早一天的中途）时，只回填后面的日期会把
    水位抬过区间外的未归档行，必须拒绝；--force 可显式绕过。"""
    table, prefix, config, job = _setup(mysql_conn, uniq, export_rows=5)
    d2, d1 = days_ago(2), days_ago(1)
    insert_rows(mysql_conn, table, [d2] * 10 + [d1] * 10)  # d2: id 1-10, d1: id 11-20
    run_export(config, job)  # 水位 5，d2 只归档了一半

    with pytest.raises(BackfillRefused, match="未归档"):
        run_backfill(config, job, d1.date(), d1.date())

    # force 绕过守卫后照常执行（空洞由调用者自担，purge 校验仍会兜底拦删除）
    result = run_backfill(config, job, d1.date(), d1.date(), force=True)
    assert result.rows == 10


def test_backfill_refuses_partially_covered_partition(mysql_conn, uniq):
    """分区已有文件但只覆盖了当天一部分（增量推进到当天中途）：
    报错而不是静默跳过，否则后续日期会把水位抬过缺口。"""
    table, prefix, config, job = _setup(mysql_conn, uniq, export_rows=5)
    d1 = days_ago(1)
    insert_rows(mysql_conn, table, [d1] * 10)
    run_export(config, job)  # 水位 5，dt=d1 分区只有 id 1-5

    with pytest.raises(BackfillRefused, match="部分覆盖"):
        run_backfill(config, job, d1.date(), d1.date())


def test_backfill_skips_purged_history(mysql_conn, uniq):
    """已归档且线上已被 purge 清空的历史分区：跳过而非误报。
    （Parquet 比线上多行是 purge 后的常态，完整覆盖的判断只看水位之上。）"""
    from ebb.purge import run_purge

    table, prefix, config, job = _setup(mysql_conn, uniq, retain_seconds=3600)
    d2 = days_ago(2)
    insert_rows(mysql_conn, table, [d2] * 10)
    run_export(config, job)
    purge_result = run_purge(config, job)
    assert purge_result.deleted_rows == 10  # 线上已清空

    result = run_backfill(config, job, d2.date(), d2.date())
    assert result.days[0].status == "skip"


def test_backfill_warns_on_cross_day_interleave(mysql_conn, uniq):
    """跨天 id 交错（午夜倒挂使相邻两天 id 区间重叠）时给出警告，提示中断后
    必须重跑同一区间；正常写入流程不受影响。"""
    table, prefix, config, job = _setup(mysql_conn, uniq)
    # 前一天 12:00 与后一天 12:00 的行按 id 顺序交错插入，使两天 id 区间相交：
    # d_early 拿到 id 1、3，d_late 拿到 id 2、4 → d_early.max(3) >= d_late.min(2)
    d_early = days_ago(2).replace(hour=12, minute=0, second=0, microsecond=0)
    d_late = days_ago(1).replace(hour=12, minute=0, second=0, microsecond=0)
    insert_rows(mysql_conn, table, [d_early, d_late, d_early, d_late])

    warnings: list[str] = []
    result = run_backfill(
        config, job, d_early.date(), d_late.date(), on_warning=warnings.append
    )
    assert (d_early.date().isoformat(), d_late.date().isoformat()) in result.interleave
    assert warnings and "交错" in warnings[0]
    assert result.rows == 4  # 正常流程照常写入


def test_backfill_no_interleave_warning_when_monotonic(mysql_conn, uniq):
    """按日期顺序插入（id 与日期单调一致）时不应误报交错。"""
    table, prefix, config, job = _setup(mysql_conn, uniq)
    d2, d1 = days_ago(2), days_ago(1)
    insert_rows(mysql_conn, table, [d2] * 5 + [d1] * 5)
    warnings: list[str] = []
    result = run_backfill(config, job, d2.date(), d1.date(), on_warning=warnings.append)
    assert result.interleave == []
    assert warnings == []


def test_earliest_day(mysql_conn, uniq):
    """CLI 缺省 --from 的依据：线上最早数据所在天（job 时区）。"""
    table, prefix, config, job = _setup(mysql_conn, uniq)
    assert earliest_day(config, job) is None  # 空表

    d3, d1 = days_ago(3), days_ago(1)
    insert_rows(mysql_conn, table, [d1] * 5 + [d3] * 5)  # 乱序插入也取最早
    assert earliest_day(config, job) == d3.date()
