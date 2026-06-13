"""集成测试环境：docker compose 起 MySQL 8 + MinIO。

容器跑起来后保留复用（重复运行测试更快）；每个测试用独立的表名/前缀，
互不干扰。需要本机 docker 可用。
"""

from __future__ import annotations

import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pymysql
import pytest

from ebb.config import Config

MYSQL_PORT = 33061
MINIO_PORT = 39001
MYSQL_DSN = f"mysql://root:ebbtest@127.0.0.1:{MYSQL_PORT}/ebbtest"
MINIO_ACCESS = "ebbtest"
MINIO_SECRET = "ebbtest123"
BUCKET = "ebb-test"
TZ = timezone(timedelta(hours=8))  # 与测试 MySQL 的会话时区一致

COMPOSE_FILE = Path(__file__).parent / "docker-compose.test.yml"


def _compose_up() -> None:
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d", "--wait"],
        check=True,
        capture_output=True,
        timeout=300,
    )


def _wait_mysql() -> None:
    deadline = time.time() + 120
    while True:
        try:
            conn = pymysql.connect(
                host="127.0.0.1", port=MYSQL_PORT, user="root",
                password="ebbtest", database="ebbtest",
            )
            conn.close()
            return
        except Exception:
            if time.time() > deadline:
                raise
            time.sleep(1)


@pytest.fixture(scope="session", autouse=True)
def test_env():
    _compose_up()
    _wait_mysql()
    # 建测试 bucket
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://127.0.0.1:{MINIO_PORT}",
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
    )
    try:
        s3.create_bucket(Bucket=BUCKET)
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    yield


@pytest.fixture()
def mysql_conn():
    conn = pymysql.connect(
        host="127.0.0.1", port=MYSQL_PORT, user="root",
        password="ebbtest", database="ebbtest", autocommit=True,
    )
    yield conn
    conn.close()


@pytest.fixture()
def uniq() -> str:
    return uuid.uuid4().hex[:8]


def make_config(
    table: str,
    prefix: str,
    *,
    time_column_type: str = "unix_s",
    time_column: str = "created_at",
    retain_seconds: int = 86400,
    export_rows: int = 500_000,
    safety_lag_seconds: int = 0,
    timezone: str = "Asia/Shanghai",
    api_keys: list[str] | None = None,
) -> Config:
    return Config.model_validate(
        {
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
                    "name": f"job-{table}",
                    "source": "testdb",
                    "table": table,
                    "cursor_column": "id",
                    "time_column": time_column,
                    "time_column_type": time_column_type,
                    "timezone": timezone,
                    "storage": "minio",
                    "prefix": prefix,
                    "schedule": {"interval_seconds": 300, "compact_at": "03:00"},
                    "batch": {
                        "export_rows": export_rows,
                        "delete_rows": 100,
                        "delete_sleep_ms": 0,
                        "safety_lag_seconds": safety_lag_seconds,
                    },
                    "retention": {
                        "online_retain_seconds": retain_seconds,
                        "verify_before_delete": True,
                    },
                }
            ],
            "query_api": {
                "enabled": bool(api_keys),
                "listen": "127.0.0.1:0",
                "api_keys": api_keys or [],
            },
        }
    )


def create_log_table(conn, table: str, *, time_type: str = "unix_s") -> None:
    col = {
        "unix_s": "BIGINT NOT NULL",
        "unix_ms": "BIGINT NOT NULL",
        "unix_us": "BIGINT NOT NULL",
        "datetime": "DATETIME NOT NULL",
        "timestamp": "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
    }[time_type]
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"""
            CREATE TABLE `{table}` (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                created_at {col},
                user_id INT NOT NULL,
                content TEXT,
                KEY idx_created (created_at)
            )
            """
        )


def insert_rows(
    conn,
    table: str,
    times: list[datetime],
    *,
    time_type: str = "unix_s",
) -> None:
    """按给定时刻列表插入行（id 自增，与时间顺序一致）。"""

    def conv(t: datetime):
        aware = t if t.tzinfo else t.replace(tzinfo=TZ)
        if time_type == "unix_s":
            return int(aware.timestamp())
        if time_type == "unix_ms":
            return int(aware.timestamp() * 1_000)
        if time_type == "unix_us":
            return int(aware.timestamp() * 1_000_000)
        return aware.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")

    with conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO `{table}` (created_at, user_id, content) VALUES (%s, %s, %s)",
            [(conv(t), i % 7, f"line-{i}") for i, t in enumerate(times)],
        )


def hours_ago(n: float) -> datetime:
    return datetime.now(tz=TZ) - timedelta(hours=n)


def days_ago(n: float) -> datetime:
    return datetime.now(tz=TZ) - timedelta(days=n)
