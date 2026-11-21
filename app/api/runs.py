"""任务实例（运行记录）API。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..models import Task
from ..schemas import Page, TaskDetail, TaskOut
from ..security import Principal
from ..services import runner
from .deps import ReadAccess, WriteAccess, get_db

router = APIRouter(prefix="/api/v1/runs", tags=["runs"])

VALID_STATUS = {"PENDING", "RUNNING", "SUCCEEDED", "FAILED", "DEAD", "CANCELLED"}


@router.get("", response_model=Page)
def list_runs(
    job_id: str | None = None,
    status_filter: str | None = Query(default=None, alias="status"),
    worker_id: str | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    db: Session = Depends(get_db),
    _: Principal = Depends(ReadAccess),
):
    stmt = select(Task)
    if job_id:
        stmt = stmt.where(Task.job_id == job_id)
    if status_filter:
        status_value = status_filter.upper()
        if status_value not in VALID_STATUS:
            raise HTTPException(status_code=422, detail=f"非法状态：{status_filter}")
        stmt = stmt.where(Task.status == status_value)
    if worker_id:
        stmt = stmt.where(Task.worker_id == worker_id)

    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = db.scalars(
        stmt.order_by(Task.created_at.desc(), Task.shard_index.asc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    ).all()
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [TaskOut.model_validate(row).model_dump() for row in rows],
    }


@router.get("/{task_id}", response_model=TaskDetail)
def get_run(
    task_id: str,
    db: Session = Depends(get_db),
    _: Principal = Depends(ReadAccess),
):
    task = db.get(Task, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="运行记录不存在")
    return TaskDetail.model_validate(task)


@router.post("/{task_id}/replay", response_model=TaskOut)
def replay_run(
    task_id: str,
    db: Session = Depends(get_db),
    principal: Principal = Depends(WriteAccess),
):
    task = runner.replay_task(db, task_id, principal.name)
    if task is None:
        raise HTTPException(status_code=409, detail="仅 DEAD/FAILED/CANCELLED 的任务可以重放")
    return TaskOut.model_validate(task)


@router.post("/{task_id}/cancel", response_model=TaskOut)
def cancel_run(
    task_id: str,
    db: Session = Depends(get_db),
    principal: Principal = Depends(WriteAccess),
):
    task = runner.cancel_task(db, task_id, principal.name)
    if task is None:
        raise HTTPException(status_code=409, detail="任务不存在或已处于终态")
    return TaskOut.model_validate(task)
