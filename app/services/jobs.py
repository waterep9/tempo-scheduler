"""任务定义（Job）的生命周期管理。"""
from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..clock import naive, now
from ..db import insert_ignore
from ..events import audit, emit
from ..models import Job, Task
from .triggers import first_fire


def apply_schedule(job: Job, start: datetime | None = None) -> None:
    nxt = first_fire(job, start or now())
    job.next_fire_at = naive(nxt) if nxt else None


def create_job(session: Session, data: dict, actor: str) -> Job:
    stamp = naive(now())
    job = Job(
        name=data["name"],
        description=data.get("description", ""),
        schedule_type=data.get("schedule_type", "cron"),
        cron_expr=data.get("cron_expr"),
        interval_seconds=data.get("interval_seconds"),
        run_at=naive(data["run_at"]) if data.get("run_at") else None,
        timezone=data.get("timezone", "Asia/Shanghai"),
        executor=data.get("executor", "shell"),
        command=data.get("command"),
        http_url=data.get("http_url"),
        http_method=data.get("http_method", "POST"),
        http_headers=data.get("http_headers") or {},
        payload=data.get("payload") or {},
        timeout_seconds=data.get("timeout_seconds") or 60,
        shard_count=data.get("shard_count") or 1,
        max_concurrency=data.get("max_concurrency") or 1,
        max_attempts=data.get("max_attempts") or 3,
        backoff_base_seconds=data.get("backoff_base_seconds") or 5.0,
        backoff_cap_seconds=data.get("backoff_cap_seconds") or 300.0,
        catch_up=data.get("catch_up") or "skip",
        priority=data.get("priority") or 0,
        enabled=bool(data.get("enabled", True)),
        created_by=actor,
        created_at=stamp,
        updated_at=stamp,
        version=1,
    )
    if job.enabled:
        apply_schedule(job)
    session.add(job)
    session.flush()
    emit(session, "JOB_CREATED", job_id=job.id, name=job.name, actor=actor)
    audit(session, actor, "JOB_CREATE", target_type="job", target_id=job.id, name=job.name)
    session.commit()
    return job


def update_job(session: Session, job: Job, data: dict, actor: str, expected_version: int | None) -> Job:
    if expected_version is not None and expected_version != job.version:
        raise VersionConflict(job.version)

    schedule_changed = False
    for field in (
        "name",
        "description",
        "schedule_type",
        "cron_expr",
        "interval_seconds",
        "timezone",
        "executor",
        "command",
        "http_url",
        "http_method",
        "http_headers",
        "payload",
        "timeout_seconds",
        "shard_count",
        "max_concurrency",
        "max_attempts",
        "backoff_base_seconds",
        "backoff_cap_seconds",
        "catch_up",
        "priority",
    ):
        if field in data and data[field] is not None:
            if field in ("schedule_type", "cron_expr", "interval_seconds", "timezone"):
                schedule_changed = True
            setattr(job, field, data[field])

    if "run_at" in data and data["run_at"] is not None:
        job.run_at = naive(data["run_at"])
        schedule_changed = True
    if "enabled" in data and data["enabled"] is not None:
        job.enabled = bool(data["enabled"])
        schedule_changed = True

    if schedule_changed:
        if job.enabled:
            apply_schedule(job)
        else:
            job.next_fire_at = None

    job.updated_at = naive(now())
    job.version += 1
    emit(session, "JOB_UPDATED", job_id=job.id, actor=actor, version=job.version)
    audit(session, actor, "JOB_UPDATE", target_type="job", target_id=job.id, version=job.version)
    session.commit()
    return job


def set_enabled(session: Session, job: Job, enabled: bool, actor: str) -> Job:
    job.enabled = enabled
    if enabled:
        apply_schedule(job)
    else:
        job.next_fire_at = None
    job.updated_at = naive(now())
    job.version += 1
    emit(session, "JOB_RESUMED" if enabled else "JOB_PAUSED", job_id=job.id, actor=actor)
    audit(session, actor, "JOB_RESUME" if enabled else "JOB_PAUSE", target_type="job", target_id=job.id)
    session.commit()
    return job


def delete_job(session: Session, job: Job, actor: str) -> None:
    running = session.scalar(
        select(Task.id).where(Task.job_id == job.id, Task.status == "RUNNING").limit(1)
    )
    if running:
        raise JobBusy("仍有执行中的任务，请先暂停或取消")
    session.delete(job)
    emit(session, "JOB_DELETED", job_id=job.id, name=job.name, actor=actor)
    audit(session, actor, "JOB_DELETE", target_type="job", target_id=job.id, name=job.name)
    session.commit()


def trigger_now(
    session: Session,
    job: Job,
    actor: str,
    *,
    payload: dict | None = None,
    shard_count: int | None = None,
) -> list[Task]:
    """手工触发：立即物化一批任务实例，优先级抬高以便插队执行。"""
    stamp = now()
    shards = max(1, min(200, shard_count or job.shard_count or 1))
    run_tag = uuid4().hex[:12]
    created: list[str] = []

    for shard in range(shards):
        key = f"{job.id}:manual:{run_tag}:{shard}"
        inserted = insert_ignore(
            session,
            Task,
            {
                "job_id": job.id,
                "fire_time": naive(stamp),
                "shard_index": shard,
                "shard_total": shards,
                "idempotency_key": key,
                "payload": payload or {},
                "status": "PENDING",
                "priority": (job.priority or 0) + 1000,
                "attempt": 0,
                "max_attempts": job.max_attempts or 1,
                "scheduled_at": naive(stamp),
                "next_retry_at": naive(stamp),
                "created_at": naive(stamp),
                "updated_at": naive(stamp),
            },
        )
        if inserted:
            created.append(key)

    emit(
        session,
        "JOB_MANUAL_TRIGGERED",
        job_id=job.id,
        actor=actor,
        shards=shards,
        created=len(created),
    )
    audit(
        session,
        actor,
        "JOB_TRIGGER",
        target_type="job",
        target_id=job.id,
        shards=shards,
        created=len(created),
    )
    session.commit()

    if not created:
        return []
    return session.scalars(
        select(Task).where(Task.idempotency_key.in_(created)).order_by(Task.shard_index)
    ).all()


class VersionConflict(Exception):
    def __init__(self, current: int) -> None:
        self.current = current
        super().__init__(f"版本冲突，当前版本 {current}")


class JobBusy(Exception):
    pass
