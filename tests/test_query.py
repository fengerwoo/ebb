"""查询服务（queryservice + HTTP API）的集成测试。"""

import pytest
from fastapi.testclient import TestClient

from ebb.api import build_admin_app, build_query_app
from ebb.export import run_export
from ebb.queryservice import QueryRejected, run_query, view_names
from ebb.registry import Registry

from conftest import create_log_table, days_ago, hours_ago, insert_rows, make_config


def _setup(mysql_conn, uniq, **kw):
    table = f"logs_{uniq}"
    prefix = f"t/{uniq}"
    config = make_config(table, prefix, **kw)
    create_log_table(mysql_conn, table)
    return table, prefix, config, config.jobs[0]


def test_view_named_after_table(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq)
    assert view_names(config) == {table: job.name}


def test_query_via_view_with_partition_pruning(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq)
    d2, d1 = days_ago(2), days_ago(1)
    insert_rows(mysql_conn, table, [d2] * 10 + [d1] * 20)
    run_export(config, job)

    r = run_query(config, f"SELECT count(*) AS n FROM {table}")
    assert r.rows[0][0] == 30

    r2 = run_query(
        config,
        f"SELECT count(*) FROM {table} WHERE dt = '{d1.date().isoformat()}'",
    )
    assert r2.rows[0][0] == 20

    r3 = run_query(
        config,
        f"SELECT dt, count(*) FROM {table} GROUP BY dt ORDER BY dt",
    )
    assert len(r3.rows) == 2


def test_query_rejects_non_select(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq)
    for sql in [
        "DROP TABLE x",
        "INSTALL mysql",
        "CREATE TABLE t (a int)",
        "SELECT 1; SELECT 2",
        "COPY (SELECT 1) TO '/tmp/x.csv'",
        "ATTACH 'x.db'",
        "SET threads = 1",
    ]:
        with pytest.raises(QueryRejected):
            run_query(config, sql)


def test_query_local_filesystem_disabled(mysql_conn, uniq):
    """SELECT 形式的本地文件读取（read_text/read_csv 等）必须被沙箱拦截，
    否则持有 API Key 即可读取服务器上的配置文件与凭据。"""
    table, prefix, config, job = _setup(mysql_conn, uniq)
    for sql in [
        "SELECT * FROM read_text('/etc/hosts')",
        "SELECT * FROM read_csv_auto('/etc/hosts', header=false)",
        "SELECT * FROM glob('/*')",
    ]:
        with pytest.raises(Exception, match="(?i)disabled|permission"):
            run_query(config, sql)


def test_query_cannot_unlock_configuration(mysql_conn, uniq):
    """配置已锁定：查询线程不能通过任何途径恢复本地文件系统。"""
    table, prefix, config, job = _setup(mysql_conn, uniq)
    # SET 语句本身先被语句类型校验拒绝
    with pytest.raises(QueryRejected):
        run_query(config, "SET disabled_filesystems = ''")


def test_query_with_cte_allowed(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq)
    insert_rows(mysql_conn, table, [hours_ago(2)] * 5)
    run_export(config, job)
    r = run_query(config, f"WITH t AS (SELECT * FROM {table}) SELECT count(*) FROM t")
    assert r.rows[0][0] == 5


def test_query_row_limit_truncation(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq)
    insert_rows(mysql_conn, table, [hours_ago(2)] * 20)
    run_export(config, job)
    r = run_query(config, f"SELECT * FROM {table}", max_rows=7)
    assert r.row_count == 7
    assert r.truncated is True


def test_http_query_api(mysql_conn, uniq):
    table, prefix, config, job = _setup(mysql_conn, uniq, api_keys=["sk-test-1"])
    insert_rows(mysql_conn, table, [hours_ago(2)] * 12)
    run_export(config, job)

    client = TestClient(build_query_app(config))

    # 无认证 / 错误 key
    assert client.post("/query", json={"sql": "SELECT 1"}).status_code == 401
    assert (
        client.post(
            "/query",
            json={"sql": "SELECT 1"},
            headers={"Authorization": "Bearer wrong"},
        ).status_code
        == 401
    )

    # 正常查询
    resp = client.post(
        "/query",
        json={"sql": f"SELECT count(*) AS n FROM {table}"},
        headers={"Authorization": "Bearer sk-test-1"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["rows"][0][0] == 12
    assert body["columns"] == ["n"]

    # 非 SELECT 拒绝
    resp = client.post(
        "/query",
        json={"sql": "DROP TABLE x"},
        headers={"Authorization": "Bearer sk-test-1"},
    )
    assert resp.status_code == 400

    # SQL 错误 422
    resp = client.post(
        "/query",
        json={"sql": "SELECT * FROM not_a_table"},
        headers={"Authorization": "Bearer sk-test-1"},
    )
    assert resp.status_code == 422

    # 请求级行数上限只能比全局更小
    resp = client.post(
        "/query",
        json={"sql": f"SELECT * FROM {table}", "max_rows": 5},
        headers={"Authorization": "Bearer sk-test-1"},
    )
    assert resp.json()["row_count"] == 5
    assert resp.json()["truncated"] is True


def test_admin_endpoint():
    registry = Registry()
    registry.start("j1", "export")
    registry.progress("j1", "export", {"rows_exported": 10})
    client = TestClient(build_admin_app(registry))
    data = client.get("/admin/jobs").json()
    assert data["jobs"][0]["job"] == "j1"
    assert data["jobs"][0]["state"] == "running"
    assert data["jobs"][0]["progress"] == {"rows_exported": 10}

    registry.finish("j1", "export", result={"status": "ok", "rows": 10})
    data = client.get("/admin/jobs").json()
    assert data["jobs"][0]["state"] == "idle"
    assert data["jobs"][0]["last_result"]["rows"] == 10


def test_query_cannot_read_other_prefixes_in_bucket(mysql_conn, uniq):
    """secret SCOPE 收窄到 job prefix：持有 API key 也读不到同 bucket
    其他前缀下的对象（例如其他系统存的私密文件）。"""
    from ebb import engine

    table, prefix, config, job = _setup(mysql_conn, uniq)
    insert_rows(mysql_conn, table, [hours_ago(2)] * 3)
    run_export(config, job)

    # 用数据面的全 bucket 凭据在 job prefix 之外种一个真实 parquet 对象
    storage = config.storage_of(job)
    outside = f"private-{uniq}/secret.parquet"
    duck = engine.connect(storage=storage)
    try:
        duck.execute(
            f"COPY (SELECT 42 AS answer) TO '{engine.s3_url(storage, outside)}' "
            f"(FORMAT parquet)"
        )
    finally:
        duck.close()

    # 视图内查询正常
    assert run_query(config, f"SELECT count(*) FROM {table}").rows[0][0] == 3
    # prefix 内手写 read_parquet 也允许（数据本就属于该 job）
    inside = run_query(
        config,
        f"SELECT count(*) FROM read_parquet('s3://{storage.bucket}/{prefix}/*/*.parquet')",
    )
    assert inside.rows[0][0] == 3
    # prefix 之外没有凭据可用，必须失败
    with pytest.raises(Exception):
        run_query(
            config,
            f"SELECT * FROM read_parquet('s3://{storage.bucket}/{outside}')",
        )


def test_query_cannot_read_sibling_prefix(mysql_conn, uniq):
    """SCOPE 带尾部 /：名字以 job prefix 开头但不在其下的兄弟前缀（如 t/<uniq>
    之于 t/<uniq>-leak）不能被越权读取。不带尾 / 时 scope 是字符串前缀匹配，
    会把兄弟前缀一并解锁。"""
    from ebb import engine

    table, prefix, config, job = _setup(mysql_conn, uniq)
    insert_rows(mysql_conn, table, [hours_ago(2)] * 3)
    run_export(config, job)

    # 兄弟前缀：字符串以 job prefix 开头，但不是 prefix/ 下的对象
    storage = config.storage_of(job)
    sibling = f"{prefix}-leak/secret.parquet"
    duck = engine.connect(storage=storage)  # 数据面全 bucket 凭据写入
    try:
        duck.execute(
            f"COPY (SELECT 42 AS answer) TO '{engine.s3_url(storage, sibling)}' "
            f"(FORMAT parquet)"
        )
    finally:
        duck.close()

    # 视图查询不受影响
    assert run_query(config, f"SELECT count(*) FROM {table}").rows[0][0] == 3
    # 兄弟前缀没有匹配的 secret，必须失败（未加尾 / 时这里会越权读到 42）
    with pytest.raises(Exception):
        run_query(
            config,
            f"SELECT * FROM read_parquet('s3://{storage.bucket}/{sibling}')",
        )


def test_query_http_fetch_disabled(mysql_conn, uniq):
    """http(s) 直读被禁（SSRF/外带面）；s3:// 不受影响由其他用例保证。"""
    table, prefix, config, job = _setup(mysql_conn, uniq)
    with pytest.raises(Exception, match="(?i)disabled|not allowed"):
        run_query(
            config,
            "SELECT * FROM read_csv_auto('http://127.0.0.1:9/x.csv')",
        )


def test_query_with_empty_job_does_not_break_others(mysql_conn, uniq):
    """某个 job 还没有任何归档文件时，不影响其他 job 的查询。"""
    table, prefix, config, job = _setup(mysql_conn, uniq)
    insert_rows(mysql_conn, table, [hours_ago(2)] * 5)
    run_export(config, job)

    raw = config.model_dump()
    empty_job = {**raw["jobs"][0], "name": "empty-job", "table": f"empty_{uniq}",
                 "prefix": f"t/{uniq}-empty"}
    raw["jobs"].append(empty_job)
    from ebb.config import Config

    config2 = Config.model_validate(raw)
    r = run_query(config2, f"SELECT count(*) FROM {table}")
    assert r.rows[0][0] == 5
