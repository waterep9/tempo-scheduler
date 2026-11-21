"""触发时间计算：cron / interval / once 的统一下次触发点。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..clock import aware, now
from ..cron import CronExpr, CronError
from ..models import Job

SCHEDULE_TYPES = ("cron", "interval", "once")
CATCH_UP_POLICIES = ("skip", "all")


class ScheduleError(ValueError):
    pass


def resolve_timezone(name: str | None) -> ZoneInfo:
    try:
        return ZoneInfo(name or "UTC")
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ScheduleError(f"未知时区：{name!r}") from exc


def next_fire_after(job: Job, moment: datetime) -> datetime | None:
    """给定上一次触发点，计算下一次触发点（带时区）。"""
    if job.schedule_type == "cron":
        if not job.cron_expr:
            raise ScheduleError("cron 任务缺少 cron_expr")
        return CronExpr.parse(job.cron_expr).next_after(moment, resolve_timezone(job.timezone))

    if job.schedule_type == "interval":
        seconds = job.interval_seconds or 0
        if seconds <= 0:
            raise ScheduleError("interval 任务需要 interval_seconds > 0")
        return moment + timedelta(seconds=seconds)

    if job.schedule_type == "once":
        return None

    raise ScheduleError(f"未知的调度类型：{job.schedule_type!r}")


def first_fire(job: Job, start: datetime | None = None) -> datetime | None:
    """任务创建/恢复时的首次触发点。"""
    start = start or now()
    if job.schedule_type == "once":
        run_at = aware(job.run_at) if job.run_at else start
        # 一次性任务若已过期，立即触发一次而不是永久沉默
        return run_at if run_at > start else start
    return next_fire_after(job, start)


def validate_schedule(
    schedule_type: str,
    *,
    cron_expr: str | None,
    interval_seconds: int | None,
    run_at: datetime | None,
    timezone_name: str,
) -> None:
    if schedule_type not in SCHEDULE_TYPES:
        raise ScheduleError(f"schedule_type 必须是 {SCHEDULE_TYPES} 之一")
    resolve_timezone(timezone_name)
    if schedule_type == "cron":
        if not cron_expr:
            raise ScheduleError("cron 任务必须提供 cron_expr")
        try:
            CronExpr.parse(cron_expr)
        except CronError as exc:
            raise ScheduleError(str(exc)) from exc
    elif schedule_type == "interval":
        if not interval_seconds or interval_seconds < 1:
            raise ScheduleError("interval 任务必须提供 >=1 的 interval_seconds")
    elif schedule_type == "once":
        if run_at is None:
            raise ScheduleError("once 任务必须提供 run_at")
        if run_at.tzinfo is None:
            raise ScheduleError("run_at 必须带时区，例如 2026-12-01T09:00:00+08:00")
    if timezone_name:
        try:
            ZoneInfo(timezone_name)
        except (ZoneInfoNotFoundError, ValueError) as exc:  # pragma: no cover
            raise ScheduleError(f"未知时区：{timezone_name}") from exc


def now_utc() -> datetime:
    return now().astimezone(timezone.utc)
