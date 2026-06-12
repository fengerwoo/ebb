"""ebb 命令行入口。"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime

import click

from .config import Config, load_config, resolve_config_path


def _load(ctx: click.Context) -> Config:
    path = ctx.obj.get("config_path")
    try:
        return load_config(path)
    except FileNotFoundError:
        raise click.ClickException(
            f"配置文件不存在: {resolve_config_path(path)}（用 -c 或 EBB_CONFIG 指定）"
        )


def _select_jobs(config: Config, job_name: str | None):
    if job_name is None:
        return config.jobs
    try:
        return [config.job(job_name)]
    except KeyError as exc:
        raise click.ClickException(str(exc))


def _fmt_int(v) -> str:
    return f"{v:,}" if v is not None else "-"


def _fmt_eta(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    if seconds < 60:
        return f"{seconds:.0f} 秒"
    if seconds < 3600:
        return f"{seconds / 60:.1f} 分钟"
    return f"{seconds / 3600:.1f} 小时"


@click.group()
@click.option("-c", "--config", "config_path", default=None, help="配置文件路径")
@click.pass_context
def main(ctx: click.Context, config_path: str | None) -> None:
    """ebb：MySQL 追加型表 → 对象存储 Parquet 归档，DuckDB 随时可查。"""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config_path


@main.command()
@click.option("--job", "job_name", default=None, help="只检查指定 job")
@click.pass_context
def check(ctx: click.Context, job_name: str | None) -> None:
    """体检：MySQL 连接、表结构、对象存储读写、DuckDB 扩展。"""
    from .checks import check_job

    config = _load(ctx)
    all_ok = True
    for job in _select_jobs(config, job_name):
        report = check_job(config, job)
        all_ok = all_ok and report.ok
        click.echo(f"\njob: {job.name}")
        for item in report.items:
            mark = click.style("✓", fg="green") if item.ok else click.style("✗", fg="red")
            click.echo(f"  {mark} {item.name:<22} {item.detail}")
    sys.exit(0 if all_ok else 1)


@main.command()
@click.option("--job", "job_name", default=None, help="只看指定 job")
@click.pass_context
def status(ctx: click.Context, job_name: str | None) -> None:
    """水位、线上最大 id、落后行数与追平估算（不依赖 serve）。"""
    from .status import job_status

    config = _load(ctx)
    header = f"{'JOB':<20} {'水位':>14} {'线上最大ID':>14} {'落后行数':>12} {'文件数':>8}  {'速率(行/秒)':>12}  追平预计"
    click.echo(header)
    for job in _select_jobs(config, job_name):
        s = job_status(config, job)
        rate = f"{s.rate_rows_per_second:.0f}" if s.rate_rows_per_second else "-"
        click.echo(
            f"{s.job:<20} {_fmt_int(s.watermark):>14} {_fmt_int(s.max_id):>14} "
            f"{_fmt_int(s.lag_rows):>12} {s.file_count:>8}  {rate:>12}  {_fmt_eta(s.eta_seconds)}"
        )


@main.command()
@click.pass_context
def ps(ctx: click.Context) -> None:
    """实时执行状态（读取 serve 进程的管理端点）。"""
    import httpx

    config = _load(ctx)
    host, port = config.admin.host_port
    url = f"http://{host or '127.0.0.1'}:{port}/admin/jobs"
    try:
        data = httpx.get(url, timeout=5).json()
    except Exception:
        raise click.ClickException("serve 进程未运行（管理端点不可达）")
    entries = data.get("jobs", [])
    if not entries:
        click.echo("serve 在运行，但还没有任何任务执行记录")
        return
    click.echo(f"{'JOB':<20} {'KIND':<8} {'STATE':<8} {'PROGRESS / LAST':<50} NEXT")
    for e in sorted(entries, key=lambda x: (x["job"], x["kind"])):
        if e["state"] == "running":
            info = json.dumps(e.get("progress") or {}, ensure_ascii=False)
        else:
            last = e.get("last_result") or {}
            info = json.dumps(
                {k: last[k] for k in ("status", "rows", "deleted_rows", "error") if k in last},
                ensure_ascii=False,
            )
        click.echo(
            f"{e['job']:<20} {e['kind']:<8} {e['state']:<8} {info:<50} {e.get('next_run_at') or '-'}"
        )


@main.command()
@click.option("--job", "job_name", required=True)
@click.option("--once", is_flag=True, default=True, help="跑一轮（当前仅支持单轮）")
@click.option("--dry-run", is_flag=True, default=False, help="只算不写")
@click.pass_context
def run(ctx: click.Context, job_name: str, once: bool, dry_run: bool) -> None:  # noqa: ARG001
    """手动跑一轮增量导出。"""
    from .export import run_export

    config = _load(ctx)
    job = _select_jobs(config, job_name)[0]
    result = run_export(config, job, dry_run=dry_run)
    if dry_run:
        click.echo(
            f"[dry-run] 本轮将导出 {_fmt_int(result.rows)} 行 "
            f"(水位 {_fmt_int(result.watermark_before)} → {_fmt_int(result.watermark_after)})，"
            f"当前落后 {_fmt_int(result.lag_rows)} 行"
        )
    else:
        click.echo(
            f"导出完成: {result.status}, {_fmt_int(result.rows)} 行, {result.bytes} 字节, "
            f"{len(result.files)} 个文件, 水位 {_fmt_int(result.watermark_after)}, "
            f"落后 {_fmt_int(result.lag_rows)} 行, 耗时 {result.duration_seconds}s"
        )


@main.command()
@click.option("--job", "job_name", required=True)
@click.option("--from", "from_str", required=True, help="起始日期 YYYY-MM-DD")
@click.option("--to", "to_str", required=True, help="结束日期 YYYY-MM-DD（含）")
@click.pass_context
def backfill(ctx: click.Context, job_name: str, from_str: str, to_str: str) -> None:
    """存量回填，按天切片。"""
    from .backfill import run_backfill

    config = _load(ctx)
    job = _select_jobs(config, job_name)[0]
    result = run_backfill(
        config,
        job,
        date.fromisoformat(from_str),
        date.fromisoformat(to_str),
        on_progress=lambda p: click.echo(
            f"  [{p['days_done']}/{p['days_total']}] {p['current_day']} 累计 {_fmt_int(p['rows'])} 行"
        ),
    )
    click.echo(
        f"回填完成: {_fmt_int(result.rows)} 行, {result.bytes} 字节, "
        f"{len(result.days)} 天, 耗时 {result.duration_seconds}s"
    )


@main.command("compact")
@click.option("--job", "job_name", required=True)
@click.option("--date", "date_str", required=True, help="要合并的分区日期 YYYY-MM-DD")
@click.pass_context
def compact_cmd(ctx: click.Context, job_name: str, date_str: str) -> None:
    """手动触发某天合并。"""
    from .compact import run_compact

    config = _load(ctx)
    job = _select_jobs(config, job_name)[0]
    date.fromisoformat(date_str)  # 校验格式
    result = run_compact(config, job, date_str)
    if result.status == "skip":
        click.echo(f"无需合并（{result.source_files} 个文件）")
    else:
        click.echo(
            f"合并完成: {result.source_files} 个文件 → {result.target_key} "
            f"({_fmt_int(result.rows)} 行, {result.bytes} 字节)"
        )


@main.command("purge")
@click.option("--job", "job_name", required=True)
@click.option("--dry-run", is_flag=True, default=False, help="只预览将删除的区间")
@click.pass_context
def purge_cmd(ctx: click.Context, job_name: str, dry_run: bool) -> None:
    """校验后分批删除线上已归档的过期数据。"""
    from .purge import run_purge

    config = _load(ctx)
    job = _select_jobs(config, job_name)[0]
    result = run_purge(config, job, dry_run=dry_run)
    if result.status == "empty":
        click.echo("没有可删除的数据")
    elif result.status == "dry-run":
        click.echo(
            f"[dry-run] 可删 id <= {_fmt_int(result.bound_id)}，"
            f"共 {_fmt_int(result.eligible_rows)} 行（水位 {_fmt_int(result.watermark)}）"
        )
    elif result.status == "verify-failed":
        click.echo(f"校验失败，未删除: {result.detail}", err=True)
        sys.exit(1)
    else:
        click.echo(
            f"删除完成: {_fmt_int(result.deleted_rows)} 行 / {result.batches} 批, "
            f"耗时 {result.duration_seconds}s"
        )


@main.command()
@click.pass_context
def serve(ctx: click.Context) -> None:
    """常驻模式：调度器 + 管理端点 + 可选查询 API（Docker 入口）。"""
    from .scheduler import serve as serve_loop

    serve_loop(_load(ctx))


@main.command()
@click.argument("sql")
@click.option("--max-rows", default=1000, show_default=True, help="最多显示行数")
@click.pass_context
def query(ctx: click.Context, sql: str, max_rows: int) -> None:
    """本地直查对象存储上的归档数据（每个 job 的表名即视图名）。"""
    from .queryservice import run_query

    config = _load(ctx)
    result = run_query(config, sql, max_rows=max_rows, timeout_seconds=300)
    widths = [
        max(len(str(c)), *(len(str(r[i])) for r in result.rows)) if result.rows else len(str(c))
        for i, c in enumerate(result.columns)
    ]
    click.echo(" | ".join(str(c).ljust(w) for c, w in zip(result.columns, widths)))
    click.echo("-+-".join("-" * w for w in widths))
    for row in result.rows:
        click.echo(" | ".join(str(v).ljust(w) for v, w in zip(row, widths)))
    suffix = "（已截断）" if result.truncated else ""
    click.echo(f"\n{result.row_count} 行{suffix}")


if __name__ == "__main__":
    main()
