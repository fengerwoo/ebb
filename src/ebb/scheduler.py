"""ebb serve：调度器常驻 + 管理端点 + 可选查询 API。

- 每个 job 一个增量导出定时任务（interval_seconds），启动时立刻先跑一轮；
- 每日 compact_at（job 时区）触发合并：合并所有「早于今天且仍有 inc 文件」
  的分区（含补漏），随后执行 purge；若配置了 purge_interval_seconds，
  purge 改为按该间隔独立调度，每日任务只做合并；
- APScheduler max_instances=1 + coalesce=True：上一轮没结束就跳过本轮；
- SIGTERM/SIGINT 优雅退出：等当前批次做完。
"""

from __future__ import annotations

import threading
from datetime import datetime

import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from . import compact, export, purge
from .api import build_admin_app, build_query_app
from .config import Config, JobConfig
from .logs import log, log_error
from .registry import Registry
from .s3util import S3Store


def _run_export_job(config: Config, job: JobConfig, registry: Registry) -> None:
    registry.start(job.name, "export")
    try:
        result = export.run_export(
            config,
            job,
            on_progress=lambda p: registry.progress(job.name, "export", p),
        )
        registry.finish(job.name, "export", result=result)
    except Exception as exc:  # noqa: BLE001 单轮失败不影响调度
        log_error("export", exc=exc, job=job.name)
        registry.finish(job.name, "export", error=f"{type(exc).__name__}: {exc}")


def _run_purge_job(config: Config, job: JobConfig, registry: Registry) -> None:
    registry.start(job.name, "purge")
    try:
        result = purge.run_purge(
            config,
            job,
            on_progress=lambda p: registry.progress(job.name, "purge", p),
        )
        registry.finish(job.name, "purge", result=result)
    except Exception as exc:  # noqa: BLE001
        log_error("purge", exc=exc, job=job.name)
        registry.finish(job.name, "purge", error=f"{type(exc).__name__}: {exc}")


def _run_daily_job(config: Config, job: JobConfig, registry: Registry) -> None:
    """每日合并（含补漏）+ 清理。合并失败则跳过清理（清理依赖归档完整）。"""
    registry.start(job.name, "compact")
    compact_ok = True
    try:
        store = S3Store(config.storage_of(job))
        today_local = datetime.now(tz=job.tzinfo).date().isoformat()
        for dt in compact.pending_compact_dates(store, job.prefix, today_local):
            compact.run_compact(
                config,
                job,
                dt,
                on_progress=lambda p: registry.progress(job.name, "compact", p),
            )
        registry.finish(job.name, "compact", result={"status": "ok"})
    except Exception as exc:  # noqa: BLE001
        compact_ok = False
        log_error("compact", exc=exc, job=job.name)
        registry.finish(job.name, "compact", error=f"{type(exc).__name__}: {exc}")

    if not compact_ok:
        return
    if job.schedule.purge_interval_seconds:
        return  # purge 由独立的间隔任务负责
    _run_purge_job(config, job, registry)


class _UvicornThread(threading.Thread):
    def __init__(self, app, host: str, port: int):
        super().__init__(daemon=True)
        config = uvicorn.Config(app, host=host, port=port, log_level="warning")
        self.server = uvicorn.Server(config)

    def run(self) -> None:
        self.server.run()

    def stop(self) -> None:
        self.server.should_exit = True


def serve(config: Config, stop_event: threading.Event | None = None) -> None:
    """stop_event 为 None 时（生产模式）注册 SIGTERM/SIGINT 优雅退出；
    测试可注入自己的 stop_event 在线程里运行。"""
    registry = Registry()
    scheduler = BackgroundScheduler()

    scheduled_jobs = [j for j in config.jobs if j.schedule.enabled]
    for job in config.jobs:
        if not job.schedule.enabled:
            log("serve", status="schedule_disabled", job=job.name)
            continue
        scheduler.add_job(
            _run_export_job,
            IntervalTrigger(seconds=job.schedule.interval_seconds),
            args=(config, job, registry),
            id=f"export:{job.name}",
            max_instances=1,
            coalesce=True,
            next_run_time=datetime.now(),  # 启动立即先跑一轮
        )
        hour, minute = job.schedule.compact_hour_minute
        scheduler.add_job(
            _run_daily_job,
            CronTrigger(hour=hour, minute=minute, timezone=job.tzinfo),
            args=(config, job, registry),
            id=f"daily:{job.name}",
            max_instances=1,
            coalesce=True,
        )
        if job.schedule.purge_interval_seconds:
            # 独立 purge 周期（保留期短于一天的场景）。与凌晨 compact 可能并发：
            # 合并改名与删源之间的瞬间会让校验多算一次行数，校验失败即跳过本轮，
            # 下一轮重新推导边界继续，无正确性风险。
            scheduler.add_job(
                _run_purge_job,
                IntervalTrigger(seconds=job.schedule.purge_interval_seconds),
                args=(config, job, registry),
                id=f"purge:{job.name}",
                max_instances=1,
                coalesce=True,
            )

    def _refresh_next_runs() -> None:
        for job in scheduled_jobs:
            kinds = [("export", f"export:{job.name}"), ("compact", f"daily:{job.name}")]
            if job.schedule.purge_interval_seconds:
                kinds.append(("purge", f"purge:{job.name}"))
            for kind, job_id in kinds:
                aps_job = scheduler.get_job(job_id)
                registry.set_next_run(
                    job.name, kind, aps_job.next_run_time if aps_job else None
                )

    scheduler.add_job(_refresh_next_runs, IntervalTrigger(seconds=5), id="refresh-next-runs")

    servers: list[_UvicornThread] = []
    admin_host, admin_port = config.admin.host_port
    servers.append(_UvicornThread(build_admin_app(registry), admin_host, admin_port))
    if config.query_api.enabled:
        host, port = config.query_api.host_port
        servers.append(_UvicornThread(build_query_app(config), host, port))

    scheduler.start()
    _refresh_next_runs()
    for s in servers:
        s.start()
    log(
        "serve",
        status="started",
        jobs=[j.name for j in scheduled_jobs],
        admin=config.admin.listen,
        query_api=config.query_api.listen if config.query_api.enabled else None,
    )

    if stop_event is None:
        stop_event = threading.Event()

        def _graceful(signum, frame):  # noqa: ARG001
            log("serve", status="stopping", signal=signum)
            stop_event.set()

        import signal

        signal.signal(signal.SIGTERM, _graceful)
        signal.signal(signal.SIGINT, _graceful)

    stop_event.wait()
    scheduler.shutdown(wait=True)  # 等当前批次做完
    for s in servers:
        s.stop()
    log("serve", status="stopped")
