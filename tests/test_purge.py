"""校验后删除的集成测试。"""

import duckdb

from ebb import naming
from ebb.export import run_export
from ebb.purge import run_purge
from ebb.s3util import S3Store

from conftest import create_log_table, days_ago, hours_ago, insert_rows, make_config


def _setup(mysql_conn, uniq, **kw):
    table = f"logs_{uniq}"
    prefix = f"t/{uniq}"
    config = make_config(table, prefix, **kw)
    create_log_table(mysql_conn, table)
    return table, prefix, config, config.jobs[0]


def _count(conn, table) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM `{table}`")
        return cur.fetchone()[0]


def test_purge_deletes_only_archived_and_expired(mysql_conn, uniq):
    # 保留 1 天：3 天前的 30 行可删，1 小时前的 10 行保留
    table, prefix, config, job = _setup(mysql_conn, uniq, retain_seconds=86400)
    insert_rows(mysql_conn, table, [days_ago(3)] * 30 + [hours_ago(1)] * 10)
    run_export(config, job)

    result = run_purge(config, job)
    assert result.status == "ok"
    assert result.bound_id == 30
    assert result.deleted_rows == 30
    assert result.batches == 1  # delete_rows=100 一批删完
    assert _count(mysql_conn, table) == 10


def test_purge_respects_watermark(mysql_conn, uniq):
    """未归档的数据即使过期也不删。"""
    table, prefix, config, job = _setup(mysql_conn, uniq, retain_seconds=3600)
    insert_rows(mysql_conn, table, [days_ago(3)] * 20)
    # 不导出 → 水位 0 → 不可删
    result = run_purge(config, job)
    assert result.status == "empty"
    assert _count(mysql_conn, table) == 20


def test_purge_in_batches(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq, retain_seconds=3600)
    insert_rows(mysql_conn, table, [days_ago(2)] * 250)
    run_export(config, job)

    result = run_purge(config, job)  # delete_rows=100
    assert result.status == "ok"
    assert result.deleted_rows == 250
    assert result.batches == 3
    assert _count(mysql_conn, table) == 0


def test_purge_keeps_inverted_hot_rows(mysql_conn, uniq):
    """bound 区间内时间倒挂的未过期行不能被删：删除带时间条件，
    只按 id <= bound 会把这类行提前下线。"""
    table, prefix, config, job = _setup(mysql_conn, uniq, retain_seconds=86400)
    insert_rows(mysql_conn, table, [days_ago(2)] * 10)  # id 1-10 过期
    insert_rows(mysql_conn, table, [hours_ago(1)])  # id 11 未过期（倒挂）
    insert_rows(mysql_conn, table, [days_ago(2)])  # id 12 过期
    run_export(config, job)

    result = run_purge(config, job)
    assert result.status == "ok"
    assert result.bound_id == 12
    assert result.eligible_rows == 11  # 不含倒挂的 id 11
    assert result.deleted_rows == 11
    assert _count(mysql_conn, table) == 1
    with mysql_conn.cursor() as cur:
        cur.execute(f"SELECT id FROM `{table}`")
        assert cur.fetchone()[0] == 11  # 留下的正是未过期那行


def test_purge_dry_run(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq, retain_seconds=3600)
    insert_rows(mysql_conn, table, [days_ago(2)] * 15)
    run_export(config, job)

    result = run_purge(config, job, dry_run=True)
    assert result.status == "dry-run"
    assert result.bound_id == 15
    assert result.eligible_rows == 15
    assert _count(mysql_conn, table) == 15


def test_purge_verify_failure_blocks_delete(mysql_conn, uniq):
    """归档侧数据缺失时（行数对不上），校验失败、一行不删。"""
    table, prefix, config, job = _setup(mysql_conn, uniq, retain_seconds=3600)
    insert_rows(mysql_conn, table, [days_ago(2)] * 50)
    exported = run_export(config, job)

    # 用「只含 1 行但文件名仍声称覆盖到 id=50」的假文件替换真实归档：
    # 水位仍是 50，但 [1,50] 区间在归档侧只有 1 行 → 校验必须失败
    store = S3Store(config.storage_of(job))
    st = config.storage_of(job)
    day = days_ago(2).date().isoformat()
    fake = naming.inc_key(prefix, day, 1, 50)
    conn = duckdb.connect()
    conn.execute("LOAD httpfs")
    conn.execute(
        f"CREATE SECRET s (TYPE s3, KEY_ID '{st.access_key_id}', "
        f"SECRET '{st.secret_access_key}', ENDPOINT '{st.duckdb_endpoint}', "
        f"URL_STYLE 'path', USE_SSL false)"
    )
    conn.execute(
        f"COPY (SELECT 50 AS id, 0 AS created_at, 0 AS user_id, 'x' AS content) "
        f"TO 's3://{st.bucket}/{fake}' (FORMAT parquet)"
    )
    conn.close()
    store.delete_keys([k for k in exported.files if k != fake])

    result = run_purge(config, job)
    assert result.status == "verify-failed"
    assert "未在 Parquet 中" in result.detail
    assert _count(mysql_conn, table) == 50  # 一行未删


def test_purge_verify_tolerates_compact_window_duplicates(mysql_conn, uniq):
    """compact 改名后、删源前的瞬间，分区内 data 与 inc 并存（行重复）。
    包含语义的校验只关心「待删行是否在 Parquet」，重复不应误报。"""
    table, prefix, config, job = _setup(mysql_conn, uniq, retain_seconds=3600)
    insert_rows(mysql_conn, table, [days_ago(2)] * 20)
    exported = run_export(config, job)

    # 模拟 compact 卡在中间态：inc 文件复制出一份同区间的 data 文件，二者并存
    store = S3Store(config.storage_of(job))
    src = exported.files[0]
    f = naming.parse_key(prefix, src)
    dup = naming.data_key(prefix, f.dt, f.from_id, f.to_id)
    store.client.copy_object(
        Bucket=store.bucket, Key=dup, CopySource={"Bucket": store.bucket, "Key": src}
    )

    result = run_purge(config, job)
    assert result.status == "ok"
    assert result.deleted_rows == 20
    assert _count(mysql_conn, table) == 0


def test_purge_skip_verify_when_disabled(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq, retain_seconds=3600)
    config.jobs[0].retention.verify_before_delete = False
    insert_rows(mysql_conn, table, [days_ago(2)] * 10)
    run_export(config, job)

    result = run_purge(config, job)
    assert result.status == "ok"
    assert result.deleted_rows == 10


def test_purge_resumable_after_interruption(mysql_conn, uniq):
    """删一半中断后重新推导边界继续删（无持久化进度）。"""
    from ebb import mysqlutil

    table, prefix, config, job = _setup(mysql_conn, uniq, retain_seconds=3600)
    insert_rows(mysql_conn, table, [days_ago(2)] * 100)
    run_export(config, job)

    # 手工模拟删了一半后中断
    with mysqlutil.connect(config.source_of(job)) as conn:
        mysqlutil.delete_batch(conn, table, "id", 100, 50)
    assert _count(mysql_conn, table) == 50

    result = run_purge(config, job)
    assert result.status == "ok"
    assert result.deleted_rows == 50
    assert _count(mysql_conn, table) == 0


def test_purge_nothing_expired(mysql_conn, uniq):
    """数据都在保留期内：不删。"""
    table, prefix, config, job = _setup(mysql_conn, uniq, retain_seconds=86400 * 7)
    insert_rows(mysql_conn, table, [hours_ago(2)] * 10)
    run_export(config, job)

    result = run_purge(config, job)
    assert result.status == "empty"
    assert _count(mysql_conn, table) == 10


import pytest


@pytest.mark.parametrize("ttype", ["datetime", "timestamp", "unix_ms"])
def test_purge_with_other_time_types(mysql_conn, uniq, ttype):
    """datetime（墙钟字面量）/ timestamp（FROM_UNIXTIME）/ unix_ms 的删除边界。"""
    table = f"logs_{uniq}_{ttype}"
    prefix = f"t/{uniq}-{ttype}"
    config = make_config(table, prefix, retain_seconds=86400, time_column_type=ttype)
    job = config.jobs[0]
    create_log_table(mysql_conn, table, time_type=ttype)
    insert_rows(mysql_conn, table, [days_ago(3)] * 12, time_type=ttype)
    insert_rows(mysql_conn, table, [hours_ago(1)] * 6, time_type=ttype)
    run_export(config, job)

    result = run_purge(config, job)
    assert result.status == "ok"
    assert result.deleted_rows == 12
    assert _count(mysql_conn, table) == 6
