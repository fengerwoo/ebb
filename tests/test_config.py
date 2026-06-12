import pytest
from pydantic import ValidationError

from ebb.config import Config, SourceConfig, StorageConfig

BASE = {
    "sources": {"db1": {"dsn": "mysql://u:p@h:3306/d"}},
    "storages": {
        "s1": {
            "type": "s3",
            "endpoint": "oss-cn-hangzhou.aliyuncs.com",
            "bucket": "b",
            "access_key_id": "ak",
            "secret_access_key": "sk",
        }
    },
    "jobs": [
        {
            "name": "j1",
            "source": "db1",
            "table": "logs",
            "time_column": "created_at",
            "time_column_type": "unix_s",
            "timezone": "Asia/Shanghai",
            "storage": "s1",
            "prefix": "archive/logs",
        }
    ],
}


def test_valid_config():
    c = Config.model_validate(BASE)
    job = c.job("j1")
    assert job.cursor_column == "id"
    assert job.schedule.interval_seconds == 300
    assert c.storage_of(job).url_style == "vhost"


def test_dsn_parts():
    s = SourceConfig(dsn="mysql://user:p%40ss@db.example.com:3307/mydb")
    assert s.parts == {
        "host": "db.example.com",
        "port": 3307,
        "user": "user",
        "password": "p@ss",
        "database": "mydb",
    }


def test_bad_dsn():
    with pytest.raises(ValidationError):
        SourceConfig(dsn="postgres://u@h/d")


def test_unknown_source_ref():
    bad = {**BASE, "jobs": [{**BASE["jobs"][0], "source": "nope"}]}
    with pytest.raises(ValidationError, match="source"):
        Config.model_validate(bad)


def test_duplicate_job_name():
    bad = {**BASE, "jobs": [BASE["jobs"][0], BASE["jobs"][0]]}
    with pytest.raises(ValidationError, match="重复"):
        Config.model_validate(bad)


def test_bad_identifier_rejected():
    bad_job = {**BASE["jobs"][0], "table": "logs; DROP TABLE x"}
    with pytest.raises(ValidationError):
        Config.model_validate({**BASE, "jobs": [bad_job]})


def test_bad_timezone():
    bad_job = {**BASE["jobs"][0], "timezone": "Mars/Olympus"}
    with pytest.raises(Exception):
        Config.model_validate({**BASE, "jobs": [bad_job]})


def test_query_api_requires_keys():
    bad = {**BASE, "query_api": {"enabled": True, "api_keys": []}}
    with pytest.raises(ValidationError, match="api_keys"):
        Config.model_validate(bad)


def test_endpoint_url_variants():
    st = StorageConfig(
        endpoint="cos.ap-guangzhou.myqcloud.com", bucket="b",
        access_key_id="a", secret_access_key="s",
    )
    assert st.endpoint_url == "https://cos.ap-guangzhou.myqcloud.com"
    assert st.duckdb_endpoint == "cos.ap-guangzhou.myqcloud.com"

    st2 = StorageConfig(
        endpoint="http://127.0.0.1:9000", bucket="b",
        access_key_id="a", secret_access_key="s", use_ssl=False,
    )
    assert st2.endpoint_url == "http://127.0.0.1:9000"
    assert st2.duckdb_endpoint == "127.0.0.1:9000"

    aws = StorageConfig(bucket="b", access_key_id="a", secret_access_key="s")
    assert aws.endpoint_url is None
    assert aws.duckdb_endpoint == ""


def test_prefix_normalized():
    c = Config.model_validate(
        {**BASE, "jobs": [{**BASE["jobs"][0], "prefix": "/x/y/"}]}
    )
    assert c.jobs[0].prefix == "x/y"
