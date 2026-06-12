"""ebb 命令行入口。"""

from __future__ import annotations

import json
import sys
import time
from datetime import date, datetime, timedelta

import click

from .config import Config, load_config, resolve_config_path


def _load(ctx: click.Context) -> Config:
    path = ctx.obj.get("config_path")
    try:
        return load_config(path)
    except FileNotFoundError:
        raise click.ClickException(
            f"config file not found: {resolve_config_path(path)} (use -c or EBB_CONFIG)"
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
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds / 60:.1f} min"
    return f"{seconds / 3600:.1f} h"


class _Throttle:
    """进度打印节流：避免小批量高频刷屏。"""

    def __init__(self, interval_seconds: float = 1.0) -> None:
        self._interval = interval_seconds
        self._last = 0.0

    def ready(self) -> bool:
        now = time.monotonic()
        if now - self._last >= self._interval:
            self._last = now
            return True
        return False


def _rows_progress(done: int, total: int, elapsed_seconds: float) -> str:
    """`done/total rows (pct%), rate rows/s, ETA xx` 形式的进度片段。"""
    pct = f"{done / total * 100:.1f}%" if total > 0 else "-"
    rate = done / elapsed_seconds if elapsed_seconds > 0 and done > 0 else None
    if total > 0 and done >= total:
        eta = 0.0
    elif rate and total > done:
        eta = (total - done) / rate
    else:
        eta = None
    rate_s = f"{rate:,.0f} rows/s" if rate else "-"
    return (
        f"{_fmt_int(done)}/{_fmt_int(total)} rows ({pct}), "
        f"{rate_s}, ETA {_fmt_eta(eta)}"
    )


@click.group()
@click.option("-c", "--config", "config_path", default=None, help="config file path")
@click.pass_context
def main(ctx: click.Context, config_path: str | None) -> None:
    """ebb: archive append-only MySQL tables to object storage as Parquet, query anytime with DuckDB."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config_path


@main.command()
@click.option("--job", "job_name", default=None, help="check the given job only")
@click.pass_context
def check(ctx: click.Context, job_name: str | None) -> None:
    """Preflight checks: MySQL connection, table schema, storage read/write, DuckDB extensions."""
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
@click.option("--job", "job_name", default=None, help="show the given job only")
@click.pass_context
def status(ctx: click.Context, job_name: str | None) -> None:
    """Watermark, online max id, lag and catch-up estimate (does not require serve)."""
    from .status import job_status

    config = _load(ctx)
    header = (
        f"{'JOB':<20} {'WATERMARK':>14} {'MAX_ID':>14} {'LAG_ROWS':>12} "
        f"{'FILES':>8}  {'RATE(rows/s)':>12}  ETA"
    )
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
    """Live execution state (reads the serve process admin endpoint)."""
    import httpx

    config = _load(ctx)
    host, port = config.admin.host_port
    url = f"http://{host or '127.0.0.1'}:{port}/admin/jobs"
    try:
        data = httpx.get(url, timeout=5).json()
    except Exception:
        raise click.ClickException("serve is not running (admin endpoint unreachable)")
    entries = data.get("jobs", [])
    if not entries:
        click.echo("serve is running, but no job executions have been recorded yet")
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
@click.option("--once", is_flag=True, default=True, help="run one round (only single round supported for now)")
@click.option("--dry-run", is_flag=True, default=False, help="compute only, write nothing")
@click.pass_context
def run(ctx: click.Context, job_name: str, once: bool, dry_run: bool) -> None:  # noqa: ARG001
    """Run one round of incremental export manually."""
    from .export import run_export

    config = _load(ctx)
    job = _select_jobs(config, job_name)[0]
    result = run_export(config, job, dry_run=dry_run)
    if dry_run:
        click.echo(
            f"[dry-run] would export {_fmt_int(result.rows)} rows "
            f"(watermark {_fmt_int(result.watermark_before)} -> {_fmt_int(result.watermark_after)}), "
            f"current lag {_fmt_int(result.lag_rows)} rows"
        )
    else:
        click.echo(
            f"export done: {result.status}, {_fmt_int(result.rows)} rows, {result.bytes} bytes, "
            f"{len(result.files)} files, watermark {_fmt_int(result.watermark_after)}, "
            f"lag {_fmt_int(result.lag_rows)} rows, took {result.duration_seconds}s"
        )


@main.command()
@click.option("--job", "job_name", required=True)
@click.option("--from", "from_str", default=None, help="start date YYYY-MM-DD; defaults to the earliest online day")
@click.option("--to", "to_str", default=None, help="end date YYYY-MM-DD (inclusive); defaults to yesterday (job timezone)")
@click.pass_context
def backfill(ctx: click.Context, job_name: str, from_str: str | None, to_str: str | None) -> None:
    """Backfill history day by day. Default range is earliest online day ~ yesterday; today is left to incremental export."""
    from .backfill import earliest_day, run_backfill

    config = _load(ctx)
    job = _select_jobs(config, job_name)[0]
    if from_str is None:
        from_day = earliest_day(config, job)
        if from_day is None:
            click.echo("table is empty, nothing to backfill")
            return
    else:
        from_day = date.fromisoformat(from_str)
    to_day = (
        date.fromisoformat(to_str)
        if to_str is not None
        else datetime.now(tz=job.tzinfo).date() - timedelta(days=1)
    )
    if from_day > to_day:
        click.echo(f"empty backfill range ({from_day} > {to_day}), nothing to do")
        return
    click.echo(f"backfill range: {from_day} ~ {to_day} (inclusive)")

    def on_progress(p: dict) -> None:
        mark = " (skip)" if p.get("current_status") == "skip" else ""
        click.echo(
            f"  [{p['days_done']}/{p['days_total']}] {p['current_day']}{mark}  "
            + _rows_progress(p["processed_rows"], p["total_rows"], p["elapsed_seconds"])
        )

    result = run_backfill(config, job, from_day, to_day, on_progress=on_progress)
    click.echo(
        f"backfill done: {_fmt_int(result.rows)} rows, {result.bytes} bytes, "
        f"{len(result.days)} days, took {result.duration_seconds}s"
    )


@main.command("compact")
@click.option("--job", "job_name", required=True)
@click.option("--date", "date_str", required=True, help="partition date to compact, YYYY-MM-DD")
@click.pass_context
def compact_cmd(ctx: click.Context, job_name: str, date_str: str) -> None:
    """Compact one day's partition manually."""
    from .compact import run_compact

    config = _load(ctx)
    job = _select_jobs(config, job_name)[0]
    date.fromisoformat(date_str)  # 校验格式
    result = run_compact(config, job, date_str)
    if result.status == "skip":
        click.echo(f"nothing to compact ({result.source_files} files)")
    else:
        click.echo(
            f"compact done: {result.source_files} files -> {result.target_key} "
            f"({_fmt_int(result.rows)} rows, {result.bytes} bytes)"
        )


@main.command("purge")
@click.option("--job", "job_name", required=True)
@click.option("--dry-run", is_flag=True, default=False, help="preview the deletable range only")
@click.pass_context
def purge_cmd(ctx: click.Context, job_name: str, dry_run: bool) -> None:
    """Verify archived data, then delete expired online rows in batches."""
    from .purge import run_purge

    config = _load(ctx)
    job = _select_jobs(config, job_name)[0]
    throttle = _Throttle()

    def on_progress(p: dict) -> None:
        stage = p.get("stage")
        if stage == "plan":
            click.echo(
                f"eligible: {_fmt_int(p['eligible_rows'])} rows "
                f"(id <= {_fmt_int(p['bound_id'])}, watermark {_fmt_int(p['watermark'])})"
            )
        elif stage == "verify":
            click.echo(
                f"verifying archived data for id [{_fmt_int(p['from_id'])}, {_fmt_int(p['to_id'])}] "
                f"(row count + id sum) ..."
            )
        elif stage == "delete":
            if p["deleted_rows"] >= p["eligible_rows"] or throttle.ready():
                click.echo(
                    f"  deleted {p['batches']} batches, "
                    + _rows_progress(p["deleted_rows"], p["eligible_rows"], p["elapsed_seconds"])
                )

    result = run_purge(config, job, dry_run=dry_run, on_progress=None if dry_run else on_progress)
    if result.status == "empty":
        click.echo("nothing to purge")
    elif result.status == "dry-run":
        click.echo(
            f"[dry-run] deletable id <= {_fmt_int(result.bound_id)}, "
            f"{_fmt_int(result.eligible_rows)} rows total (watermark {_fmt_int(result.watermark)})"
        )
    elif result.status == "verify-failed":
        click.echo(f"verify failed, nothing deleted: {result.detail}", err=True)
        sys.exit(1)
    else:
        click.echo(
            f"purge done: {_fmt_int(result.deleted_rows)} rows / {result.batches} batches, "
            f"took {result.duration_seconds}s"
        )


@main.command()
@click.pass_context
def serve(ctx: click.Context) -> None:
    """Long-running mode: scheduler + admin endpoint + optional query API (Docker entrypoint)."""
    from .scheduler import serve as serve_loop

    serve_loop(_load(ctx))


@main.command()
@click.argument("sql")
@click.option("--max-rows", default=1000, show_default=True, help="max rows to display")
@click.pass_context
def query(ctx: click.Context, sql: str, max_rows: int) -> None:
    """Query archived data on object storage directly (each job's table name is a view name)."""
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
    suffix = " (truncated)" if result.truncated else ""
    click.echo(f"\n{result.row_count} rows{suffix}")


if __name__ == "__main__":
    main()
