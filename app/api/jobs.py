"""任务定义 API。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..clock import aware
from ..models import Job, Task
from ..schemas import JobCreate, JobOut, JobUpdate, Page, TaskOut, TriggerRequest
from ..security import Principal
from ..services import jobs as job_service
from ..services.triggers import ScheduleError, validate_schedule
from .deps import ReadAccess, WriteAccess, get_db

router = APIRouter(prefix="/api/v1/jobs", tags=["jobs"])


def _get_job(db: Session, job_id: str) -> Job:
    job = db.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    return job


def _validate_merged(job: Job, provided: dict) -> None:
    try:
        validate_schedule(
            provided.get("schedule_type", job.schedule_type),
            cron_expr=provided.get("cron_expr", job.cron_expr),
            interval_seconds=provided.get("interval_seconds", job.interval_seconds),
            run_at=(
                provided["run_at"]
                if "run_at" in provided
                else (aware(job.run_at) if job.run_at else None)
            ),
            timezone_name=provided.get("timezone", job.timezone),
        )
    except ScheduleError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    executor = provided.get("executor", job.executor)
    if executor == "shell" and not (provided.get("command", job.command) or "").strip():
        raise HTTPException(status_code=422, detail="shell 执行器必须提供 command")
    if executor == "http" and not (provided.get("http_url", job.http_url) or "").strip():
        raise HTTPException(status_code=422, detail="http 执行器必须提供 http_url")


@router.get("", response_model=Page)
def list_jobs(
    q: str | None = Query(default=None, description="按名称/描述模糊搜索"),
    enabled: bool | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    db: Session = Depends(get_db),
    _: Principal = Depends(ReadAccess),
):
    stmt = select(Job)
    if q:
        pattern = f"%{q.strip()}%"
        stmt = stmt.where((Job.name.ilike(pattern)) | (Job.description.ilike(pattern)))
    if enabled is not None:
        stmt = stmt.where(Job.enabled.is_(enabled))

    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = db.scalars(
        stmt.order_by(Job.created_at.desc()).offset((page - 1) * page_size).limit(page_size)
    ).all()
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [JobOut.model_validate(row).model_dump() for row in rows],
    }


@router.post("", response_model=JobOut, status_code=201)
def create_job(
    payload: JobCreate,
    db: Session = Depends(get_db),
    principal: Principal = Depends(WriteAccess),
):
    try:
        job = job_service.create_job(db, payload.model_dump(), principal.name)
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="任务名已存在") from exc
    return JobOut.model_validate(job)


@router.get("/{job_id}", response_model=JobOut)
def get_job(
    job_id: str,
    db: Session = Depends(get_db),
    _: Principal = Depends(ReadAccess),
):
    return JobOut.model_validate(_get_job(db, job_id))


@router.patch("/{job_id}", response_model=JobOut)
def update_job(
    job_id: str,
    payload: JobUpdate,
    db: Session = Depends(get_db),
    principal: Principal = Depends(WriteAccess),
):
    job = _get_job(db, job_id)
    provided = payload.model_dump(exclude_unset=True)
    expected = provided.pop("version", None)
    _validate_merged(job, provided)
    try:
        job = job_service.update_job(db, job, provided, principal.name, expected)
    except job_service.VersionConflict as exc:
        raise HTTPException(
            status_code=409,
            detail=f"版本冲突：请求版本已过期，当前版本 {exc.current}",
        ) from exc
    return JobOut.model_validate(job)


@router.post("/{job_id}/pause", response_model=JobOut)
def pause_job(
    job_id: str,
    db: Session = Depends(get_db),
    principal: Principal = Depends(WriteAccess),
):
    job = job_service.set_enabled(db, _get_job(db, job_id), False, principal.name)
    return JobOut.model_validate(job)


@router.post("/{job_id}/resume", response_model=JobOut)
def resume_job(
    job_id: str,
    db: Session = Depends(get_db),
    principal: Principal = Depends(WriteAccess),
):
    job = job_service.set_enabled(db, _get_job(db, job_id), True, principal.name)
    return JobOut.model_validate(job)


@router.post("/{job_id}/run", response_model=list[TaskOut], status_code=202)
def trigger_job(
    job_id: str,
    payload: TriggerRequest | None = None,
    db: Session = Depends(get_db),
    principal: Principal = Depends(WriteAccess),
):
    job = _get_job(db, job_id)
    body = payload or TriggerRequest()
    tasks = job_service.trigger_now(
        db, job, principal.name, payload=body.payload, shard_count=body.shard_count
    )
    return [TaskOut.model_validate(task) for task in tasks]


@router.get("/{job_id}/runs", response_model=Page)
def job_runs(
    job_id: str,
    status_filter: str | None = Query(default=None, alias="status"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    db: Session = Depends(get_db),
    _: Principal = Depends(ReadAccess),
):
    _get_job(db, job_id)
    stmt = select(Task).where(Task.job_id == job_id)
    if status_filter:
        stmt = stmt.where(Task.status == status_filter.upper())
    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = db.scalars(
        stmt.order_by(Task.created_at.desc()).offset((page - 1) * page_size).limit(page_size)
    ).all()
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [TaskOut.model_validate(row).model_dump() for row in rows],
    }


@router.delete("/{job_id}", status_code=204)
def delete_job(
    job_id: str,
    db: Session = Depends(get_db),
    principal: Principal = Depends(WriteAccess),
):
    job = _get_job(db, job_id)
    try:
        job_service.delete_job(db, job, principal.name)
    except job_service.JobBusy as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return None
