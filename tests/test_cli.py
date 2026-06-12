"""CLI 的集成测试（click runner + 真实容器）。"""

import yaml
from click.testing import CliRunner

from ebb.cli import main

from conftest import (
    BUCKET,
    MINIO_ACCESS,
    MINIO_PORT,
    MINIO_SECRET,
    MYSQL_DSN,
    create_log_table,
    days_ago,
    hours_ago,
    insert_rows,
)


def _write_config(tmp_path, table: str, prefix: str) -> str:
    data = {
        "sources": {"testdb": {"dsn": MYSQL_DSN}},
        "storages": {
            "minio": {
                "type": "s3",
                "endpoint": f"http://127.0.0.1:{MINIO_PORT}",
                "bucket": BUCKET,
                "access_key_id": MINIO_ACCESS,
                "secret_access_key": MINIO_SECRET,
                "url_style": "path",
                "use_ssl": False,
            }
        },
        "jobs": [
            {
                "name": "demo",
                "source": "testdb",
                "table": table,
                "time_column": "created_at",
                "time_column_type": "unix_s",
                "timezone": "Asia/Shanghai",
                "storage": "minio",
                "prefix": prefix,
                "batch": {"delete_rows": 100, "delete_sleep_ms": 0,
                          "safety_lag_seconds": 0},
                "retention": {"online_retain_seconds": 3600},
            }
        ],
    }
    path = tmp_path / "config.yml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return str(path)


def test_cli_missing_config():
    result = CliRunner().invoke(main, ["-c", "/nonexistent/config.yml", "status"])
    assert result.exit_code != 0
    assert "配置文件不存在" in result.output


def test_cli_full_flow(tmp_path, mysql_conn, uniq):
    table = f"logs_{uniq}"
    prefix = f"t/{uniq}"
    create_log_table(mysql_conn, table)
    cfg = _write_config(tmp_path, table, prefix)
    runner = CliRunner()

    # check
    r = runner.invoke(main, ["-c", cfg, "check"])
    assert r.exit_code == 0, r.output

    # 数据：2 天前 20 行（过期可删），1 小时前 10 行
    insert_rows(mysql_conn, table, [days_ago(2)] * 20 + [hours_ago(1)] * 10)

    # dry-run 不写
    r = runner.invoke(main, ["-c", cfg, "run", "--job", "demo", "--once", "--dry-run"])
    assert r.exit_code == 0, r.output
    assert "30" in r.output

    # 真跑
    r = runner.invoke(main, ["-c", cfg, "run", "--job", "demo", "--once"])
    assert r.exit_code == 0, r.output
    assert "导出完成" in r.output

    # status
    r = runner.invoke(main, ["-c", cfg, "status"])
    assert r.exit_code == 0, r.output
    assert "demo" in r.output

    # compact 昨天的分区（2 天前的数据单文件，skip 也算通过）
    day = days_ago(2).date().isoformat()
    r = runner.invoke(main, ["-c", cfg, "compact", "--job", "demo", "--date", day])
    assert r.exit_code == 0, r.output

    # purge dry-run + 真删
    r = runner.invoke(main, ["-c", cfg, "purge", "--job", "demo", "--dry-run"])
    assert r.exit_code == 0, r.output
    assert "20" in r.output
    r = runner.invoke(main, ["-c", cfg, "purge", "--job", "demo"])
    assert r.exit_code == 0, r.output
    assert "删除完成" in r.output

    # query
    r = runner.invoke(
        main, ["-c", cfg, "query", f"SELECT count(*) AS n FROM {table}"]
    )
    assert r.exit_code == 0, r.output
    assert "30" in r.output

    # backfill（区间内分区已覆盖 → skip，不报错）
    r = runner.invoke(
        main, ["-c", cfg, "backfill", "--job", "demo", "--from", day, "--to", day]
    )
    assert r.exit_code == 0, r.output

    # ps：serve 未运行
    r = runner.invoke(main, ["-c", cfg, "ps"])
    assert r.exit_code != 0
    assert "serve" in r.output
