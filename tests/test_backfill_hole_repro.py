"""复现：先增量再回填会静默制造归档空洞。

序列：
1. 线上两天数据：day1 = id 1-100，day2 = id 101-200；
2. 增量导出一轮（export_rows=50）：归档 id 1-50，水位停在 day1 中段（50）；
3. 回填 [day1, day2]：day1 因分区已有 inc 文件被整天 skip，day2 写
   data-101-200，全局水位被抬到 200；
4. 再跑增量：只查 id > 200，id 51-100 从此永远不会被归档。

修复后允许两种正确行为：回填检测到危险顺序拒绝执行（抛错），或正确补齐
缺口；唯一不允许的是静默留洞。所以断言的是最终不变量：
回填流程结束后，线上 id <= 水位 的行必须全部存在于 Parquet。
"""

from datetime import datetime, timedelta

from ebb import engine
from ebb.backfill import run_backfill
from ebb.export import get_watermark, run_export
from ebb.s3util import S3Store

from conftest import TZ, create_log_table, insert_rows, make_config


def test_backfill_after_export_must_not_leave_hole(mysql_conn, uniq):
    table = f"logs_{uniq}"
    prefix = f"hole/{uniq}"
    create_log_table(mysql_conn, table)

    today = datetime.now(tz=TZ).date()
    day1 = today - timedelta(days=2)
    day2 = today - timedelta(days=1)

    def at(day, i: int) -> datetime:
        # 当天 08:00 起每秒一行，保证全部落在同一分区日期内
        return datetime(day.year, day.month, day.day, 8, 0, tzinfo=TZ) + timedelta(seconds=i)

    insert_rows(mysql_conn, table, [at(day1, i) for i in range(100)])  # id 1-100
    insert_rows(mysql_conn, table, [at(day2, i) for i in range(100)])  # id 101-200

    config = make_config(table, prefix, export_rows=50)
    job = config.jobs[0]
    storage = config.storage_of(job)
    store = S3Store(storage)

    # 第 1 步：增量先行一轮，水位停在 day1 中段
    r1 = run_export(config, job)
    assert r1.watermark_after == 50

    # 第 2 步：回填两天（危险顺序）。修复后若选择拒绝执行，抛错即为正确行为
    try:
        run_backfill(config, job, day1, day2)
    except Exception:
        return

    # 第 3 步：回填执行成功的话，再给增量一次机会，然后验证最终不变量
    run_export(config, job)
    watermark = get_watermark(store, job.prefix)

    duck = engine.connect(storage=storage)
    try:
        archived = duck.execute(
            f"SELECT count(DISTINCT id) FROM {engine.read_parquet_expr(storage, job.prefix)} "
            f"WHERE id <= {watermark}"
        ).fetchone()[0]
    finally:
        duck.close()
    with mysql_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM `{table}` WHERE id <= %s", (watermark,))
        online = cur.fetchone()[0]

    assert archived == online, (
        f"归档空洞：线上 id <= 水位({watermark}) 共 {online} 行，"
        f"Parquet 仅 {archived} 行，缺 {online - archived} 行"
    )
