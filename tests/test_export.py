"""增量导出的集成测试（真实 MySQL + MinIO）。"""

from datetime import datetime, timedelta

from ebb.export import get_watermark, run_export
from ebb.s3util import S3Store

from conftest import TZ, create_log_table, hours_ago, insert_rows, make_config


def _setup(mysql_conn, uniq, **kw):
    table = f"logs_{uniq}"
    prefix = f"t/{uniq}"
    config = make_config(table, prefix, **kw)
    create_log_table(mysql_conn, table, time_type=kw.get("time_column_type", "unix_s"))
    return table, prefix, config, config.jobs[0]


def test_export_basic(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq)
    insert_rows(mysql_conn, table, [hours_ago(2)] * 100)

    result = run_export(config, job)
    assert result.status == "ok"
    assert result.rows == 100
    assert result.watermark_after == 100
    assert result.lag_rows == 0
    assert result.bytes > 0
    assert len(result.files) == 1
    assert "/inc-1-100.parquet" in result.files[0]

    store = S3Store(config.storage_of(job))
    assert get_watermark(store, prefix) == 100


def test_export_incremental_continues_from_watermark(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq)
    insert_rows(mysql_conn, table, [hours_ago(3)] * 50)
    r1 = run_export(config, job)
    assert r1.rows == 50

    insert_rows(mysql_conn, table, [hours_ago(1)] * 30)
    r2 = run_export(config, job)
    assert r2.rows == 30
    assert r2.watermark_before == 50
    assert r2.watermark_after == 80
    assert any("/inc-51-80.parquet" in f for f in r2.files)


def test_export_empty_round(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq)
    result = run_export(config, job)
    assert result.status == "empty"
    assert result.rows == 0


def test_export_respects_batch_limit(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq, export_rows=40)
    insert_rows(mysql_conn, table, [hours_ago(2)] * 100)

    r1 = run_export(config, job)
    assert r1.rows == 40 and r1.watermark_after == 40
    r2 = run_export(config, job)
    assert r2.rows == 40 and r2.watermark_after == 80
    assert r2.lag_rows == 20
    r3 = run_export(config, job)
    assert r3.rows == 20 and r3.watermark_after == 100


def test_export_splits_by_partition_date(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq)
    yesterday = datetime.now(tz=TZ).replace(hour=1) - timedelta(days=1)
    today = datetime.now(tz=TZ).replace(hour=1)
    insert_rows(mysql_conn, table, [yesterday] * 10 + [today] * 5)

    result = run_export(config, job)
    assert result.rows == 15
    assert len(result.files) == 2
    assert f"dt={yesterday.date().isoformat()}" in result.files[0]
    assert "/inc-1-10.parquet" in result.files[0]
    assert f"dt={today.date().isoformat()}" in result.files[1]
    assert "/inc-11-15.parquet" in result.files[1]


def test_export_interleaved_dates_keep_contiguous_ranges(mysql_conn, uniq):
    """时间与 id 不单调时，按连续段切文件，任何前缀覆盖的 id 区间连续。"""
    table, prefix, config, job = _setup(mysql_conn, uniq)
    d1 = datetime.now(tz=TZ).replace(hour=1) - timedelta(days=2)
    d2 = datetime.now(tz=TZ).replace(hour=1) - timedelta(days=1)
    # id 1-3: d1, id 4-5: d2, id 6: d1（乱序写入）
    insert_rows(mysql_conn, table, [d1, d1, d1, d2, d2, d1])

    result = run_export(config, job)
    assert result.rows == 6
    names = [f.rsplit("/", 1)[-1] for f in result.files]
    assert names == ["inc-1-3.parquet", "inc-4-5.parquet", "inc-6-6.parquet"]


def test_export_idempotent_rerun_same_files(mysql_conn, uniq):
    """同一水位重跑导出，文件名与内容完全一致（覆盖上传，无重复）。"""
    table, prefix, config, job = _setup(mysql_conn, uniq)
    insert_rows(mysql_conn, table, [hours_ago(2)] * 20)
    r1 = run_export(config, job)

    store = S3Store(config.storage_of(job))
    # 模拟「上传成功但水位未被看见」的重跑：删掉文件再跑一遍
    store.delete_keys(r1.files)
    r2 = run_export(config, job)
    assert r2.files == r1.files
    assert r2.rows == r1.rows


def test_export_safety_lag_excludes_fresh_rows(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq, safety_lag_seconds=3600)
    insert_rows(mysql_conn, table, [hours_ago(2)] * 10)  # 旧数据
    insert_rows(mysql_conn, table, [hours_ago(0)] * 5)  # 刚写入

    result = run_export(config, job)
    assert result.rows == 10
    assert result.watermark_after == 10


def test_export_safety_lag_includes_inverted_rows(mysql_conn, uniq):
    """时间列与 id 在安全线附近倒挂时不得漏行：id 截断把「小 id、新时间」
    的行一并导出，而不是按时间过滤后被水位永久跳过。"""
    table, prefix, config, job = _setup(mysql_conn, uniq, safety_lag_seconds=3600)
    insert_rows(mysql_conn, table, [hours_ago(2)] * 10)  # id 1-10 旧
    insert_rows(mysql_conn, table, [hours_ago(0)])  # id 11 新（与 id 12 倒挂）
    insert_rows(mysql_conn, table, [hours_ago(2)])  # id 12 旧

    result = run_export(config, job)
    assert result.rows == 12  # id 11 必须包含在内
    assert result.watermark_after == 12
    assert result.lag_rows == 0


def test_export_safety_lag_no_safe_rows_is_empty(mysql_conn, uniq):
    """安全窗口内没有任何行时走 empty 路径，水位不动。"""
    table, prefix, config, job = _setup(mysql_conn, uniq, safety_lag_seconds=3600)
    insert_rows(mysql_conn, table, [hours_ago(0)] * 5)  # 全部是新行

    result = run_export(config, job)
    assert result.status == "empty"
    assert result.watermark_after == 0
    store = S3Store(config.storage_of(job))
    assert store.list_keys(prefix) == []


def test_export_dry_run_writes_nothing(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq)
    insert_rows(mysql_conn, table, [hours_ago(2)] * 30)

    result = run_export(config, job, dry_run=True)
    assert result.status == "dry-run"
    assert result.rows == 30
    assert result.lag_rows == 30
    store = S3Store(config.storage_of(job))
    assert store.list_keys(prefix) == []


def test_export_datetime_column(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq, time_column_type="datetime")
    t = hours_ago(2)
    insert_rows(mysql_conn, table, [t] * 8, time_type="datetime")

    result = run_export(config, job)
    assert result.rows == 8
    assert f"dt={t.date().isoformat()}" in result.files[0]


def test_export_unix_ms_column(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq, time_column_type="unix_ms")
    t = hours_ago(2)
    insert_rows(mysql_conn, table, [t] * 8, time_type="unix_ms")

    result = run_export(config, job)
    assert result.rows == 8
    assert f"dt={t.date().isoformat()}" in result.files[0]


def test_export_timestamp_column(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq, time_column_type="timestamp")
    t = hours_ago(2)
    insert_rows(mysql_conn, table, [t] * 8, time_type="timestamp")

    result = run_export(config, job)
    assert result.rows == 8
    assert f"dt={t.date().isoformat()}" in result.files[0]
