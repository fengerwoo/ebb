"""每日合并的集成测试。"""

from datetime import datetime

import duckdb

from ebb import naming
from ebb.compact import pending_compact_dates, run_compact
from ebb.export import run_export
from ebb.s3util import S3Store

from conftest import TZ, create_log_table, days_ago, insert_rows, make_config


def _setup(mysql_conn, uniq, **kw):
    table = f"logs_{uniq}"
    prefix = f"t/{uniq}"
    config = make_config(table, prefix, **kw)
    create_log_table(mysql_conn, table)
    return table, prefix, config, config.jobs[0]


def _read_remote_ids(config, job, key) -> list:
    st = config.storage_of(job)
    conn = duckdb.connect()
    conn.execute("LOAD httpfs")
    conn.execute(
        f"CREATE SECRET s (TYPE s3, KEY_ID '{st.access_key_id}', "
        f"SECRET '{st.secret_access_key}', ENDPOINT '{st.duckdb_endpoint}', "
        f"URL_STYLE 'path', USE_SSL false)"
    )
    rows = conn.execute(
        f"SELECT id FROM read_parquet('s3://{st.bucket}/{key}') ORDER BY id"
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def test_compact_merges_inc_files(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq, export_rows=10)
    day = days_ago(1).date().isoformat()
    insert_rows(mysql_conn, table, [days_ago(1)] * 25)
    for _ in range(3):  # 3 轮导出 → 3 个 inc 文件
        run_export(config, job)

    store = S3Store(config.storage_of(job))
    files = naming.parse_keys(prefix, store.list_keys(prefix))
    assert len([f for f in files if f.kind == "inc"]) == 3

    result = run_compact(config, job, day)
    assert result.status == "ok"
    assert result.rows == 25
    assert result.source_files == 3
    assert result.target_key == naming.data_key(prefix, day, 1, 25)

    files_after = naming.parse_keys(prefix, store.list_keys(prefix))
    assert [f.kind for f in files_after] == ["data"]
    assert _read_remote_ids(config, job, result.target_key) == list(range(1, 26))


def test_compact_skip_when_nothing_to_do(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq)
    day = days_ago(1).date().isoformat()
    assert run_compact(config, job, day).status == "skip"

    # 合并后只剩单个 data 文件，再跑一次也无需合并
    insert_rows(mysql_conn, table, [days_ago(1)] * 5)
    run_export(config, job)
    run_compact(config, job, day)
    assert run_compact(config, job, day).status == "skip"


def test_compact_data_plus_inc_dedup(mysql_conn, uniq):
    """模拟「改名完成但删源前崩溃」：data 与 inc 行完全重复，重跑合并去重收敛。"""
    table, prefix, config, job = _setup(mysql_conn, uniq)
    day = days_ago(1).date().isoformat()
    insert_rows(mysql_conn, table, [days_ago(1)] * 10)
    run_export(config, job)

    store = S3Store(config.storage_of(job))
    inc_key = naming.inc_key(prefix, day, 1, 10)
    data_key = naming.data_key(prefix, day, 1, 10)
    # 把 inc 复制一份成 data，构造行全重复的并存状态
    store.client.copy_object(
        Bucket=store.bucket,
        Key=data_key,
        CopySource={"Bucket": store.bucket, "Key": inc_key},
    )

    result = run_compact(config, job, day)
    assert result.status == "ok"
    assert result.rows == 10  # 去重后
    assert _read_remote_ids(config, job, result.target_key) == list(range(1, 11))
    files_after = naming.parse_keys(prefix, store.list_keys(prefix))
    assert len(files_after) == 1 and files_after[0].kind == "data"


def test_compact_no_tmp_leftover(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq, export_rows=5)
    day = days_ago(1).date().isoformat()
    insert_rows(mysql_conn, table, [days_ago(1)] * 10)
    run_export(config, job)
    run_export(config, job)
    run_compact(config, job, day)

    store = S3Store(config.storage_of(job))
    assert not [k for k in store.list_keys(prefix) if "/tmp-" in k]


def test_pending_compact_dates(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq)
    d1 = days_ago(2)
    d2 = days_ago(1)
    insert_rows(mysql_conn, table, [d1] * 3 + [d2] * 3)
    run_export(config, job)

    store = S3Store(config.storage_of(job))
    today = datetime.now(tz=TZ).date().isoformat()
    dates = pending_compact_dates(store, prefix, today)
    assert dates == [d1.date().isoformat(), d2.date().isoformat()]

    run_compact(config, job, dates[0])
    assert pending_compact_dates(store, prefix, today) == [d2.date().isoformat()]
