from datetime import datetime, timezone

from ebb.config import Config
from ebb.timeutil import duckdb_dt_expr, mysql_time_predicate

from conftest import make_config


def _job(time_column_type: str):
    return make_config("t", "p", time_column_type=time_column_type).jobs[0]


UTC_8 = datetime(2026, 6, 11, 12, 0, 0, tzinfo=timezone.utc)  # 北京时间 20:00


def test_predicate_unix_s():
    assert mysql_time_predicate(_job("unix_s"), "<", UTC_8) == (
        f"`created_at` < {int(UTC_8.timestamp())}"
    )


def test_predicate_unix_ms_us():
    e = int(UTC_8.timestamp())
    assert str(e * 1000) in mysql_time_predicate(_job("unix_ms"), "<", UTC_8)
    assert str(e * 1000000) in mysql_time_predicate(_job("unix_us"), "<", UTC_8)


def test_predicate_datetime_uses_job_tz_wall_clock():
    # job 时区 Asia/Shanghai：UTC 12:00 → 墙钟 20:00
    pred = mysql_time_predicate(_job("datetime"), ">=", UTC_8)
    assert pred == "`created_at` >= '2026-06-11 20:00:00'"


def test_predicate_timestamp_uses_from_unixtime():
    pred = mysql_time_predicate(_job("timestamp"), "<", UTC_8)
    assert pred == f"`created_at` < FROM_UNIXTIME({int(UTC_8.timestamp())})"


def test_dt_expr_smoke():
    import duckdb
    from zoneinfo import ZoneInfo

    conn = duckdb.connect()
    conn.execute("SET TimeZone = 'Asia/Shanghai'")
    # 北京时间 2026-06-11 23:30：临近午夜，UTC 日期已是 06-11 前一天的反例敏感点
    epoch = int(datetime(2026, 6, 11, 23, 30, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp())
    expr = duckdb_dt_expr(_job("unix_s")).replace('"created_at"', str(epoch))
    assert conn.execute(f"SELECT {expr}").fetchone()[0] == "2026-06-11"

    expr_ms = duckdb_dt_expr(_job("unix_ms")).replace('"created_at"', str(epoch * 1000))
    assert conn.execute(f"SELECT {expr_ms}").fetchone()[0] == "2026-06-11"

    expr_dt = duckdb_dt_expr(_job("datetime")).replace(
        '"created_at"', "TIMESTAMP '2026-06-11 23:30:00'"
    )
    assert conn.execute(f"SELECT {expr_dt}").fetchone()[0] == "2026-06-11"
    conn.close()
