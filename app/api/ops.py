"""运维与观测 API：概览统计、执行器、事件流、审计、选主状态、密钥管理。"""
from __future__ import annotations

from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..clock import iso, naive, now
from ..models import ApiKey, AuditLog, Event, Job, SchedulerLease, Task, Worker
from ..schemas import ApiKeyCreate, ApiKeyOut, AuditOut, EventOut, Page, WorkerOut
from ..security import Principal, new_token
from ..services.planner import Planner
from .deps import AdminAccess, ReadAccess, WriteAccess, get_db

router = APIRouter(prefix="/api/v1", tags=["ops"])


@router.get("/stats")
def overview(
    db: Session = Depends(get_db),
    _: Principal = Depends(ReadAccess),
):
    since = naive(now() - timedelta(hours=24))
    status_rows = db.execute(select(Task.status, func.count()).group_by(Task.status)).all()
    by_status = {status: count for status, count in status_rows}

    jobs_total = db.scalar(select(func.count()).select_from(Job)) or 0
    jobs_enabled = db.scalar(select(func.count()).select_from(Job).where(Job.enabled.is_(True))) or 0
    workers_online = (
        db.scalar(select(func.count()).select_from(Worker).where(Worker.status == "ONLINE")) or 0
    )
    capacity = (
        db.scalar(
            select(func.coalesce(func.sum(Worker.capacity), 0)).where(Worker.status == "ONLINE")
        )
        or 0
    )
    succeeded_24h = (
        db.scalar(
            select(func.count())
            .select_from(Task)
            .where(Task.status == "SUCCEEDED", Task.finished_at >= since)
        )
        or 0
    )
    failed_24h = (
        db.scalar(
            select(func.count())
            .select_from(Task)
            .where(Task.status.in_(("DEAD", "FAILED")), Task.finished_at >= since)
        )
        or 0
    )
    finished_24h = succeeded_24h + failed_24h
    success_rate = round(succeeded_24h / finished_24h * 100, 2) if finished_24h else None

    leader = db.get(SchedulerLease, 1)
    return {
        "jobs": {"total": jobs_total, "enabled": jobs_enabled},
        "tasks": {
            "pending": by_status.get("PENDING", 0),
            "running": by_status.get("RUNNING", 0),
            "succeeded": by_status.get("SUCCEEDED", 0),
            "dead": by_status.get("DEAD", 0),
            "cancelled": by_status.get("CANCELLED", 0),
        },
        "last_24h": {
            "succeeded": succeeded_24h,
            "failed": failed_24h,
            "success_rate": success_rate,
        },
        "workers": {"online": workers_online, "capacity": int(capacity)},
        "leader": {
            "holder": leader.holder if leader else None,
            "fencing_token": leader.fencing_token if leader else None,
            "expires_at": iso(leader.expires_at) if leader else None,
        },
        "server_time": iso(now()),
    }


@router.get("/workers", response_model=list[WorkerOut])
def list_workers(
    db: Session = Depends(get_db),
    _: Principal = Depends(ReadAccess),
):
    rows = db.scalars(select(Worker).order_by(Worker.status.asc(), Worker.started_at.asc())).all()
    return [WorkerOut.model_validate(row) for row in rows]


@router.get("/events", response_model=Page)
def list_events(
    after_id: int = Query(default=0, ge=0, description="游标：只返回 id 更大的事件"),
    type_filter: str | None = Query(default=None, alias="type"),
    limit: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
    _: Principal = Depends(ReadAccess),
):
    stmt = select(Event)
    if after_id:
        stmt = stmt.where(Event.id > after_id)
    if type_filter:
        stmt = stmt.where(Event.type == type_filter)
    rows = db.scalars(stmt.order_by(Event.id.desc()).limit(limit)).all()
    return {
        "total": len(rows),
        "page": 1,
        "page_size": limit,
        "items": [EventOut.model_validate(row).model_dump() for row in rows],
    }


@router.get("/audit", response_model=Page)
def list_audit(
    limit: int = Query(default=50, ge=1, le=500),
    actor: str | None = None,
    db: Session = Depends(get_db),
    _: Principal = Depends(ReadAccess),
):
    stmt = select(AuditLog)
    if actor:
        stmt = stmt.where(AuditLog.actor == actor)
    rows = db.scalars(stmt.order_by(AuditLog.id.desc()).limit(limit)).all()
    return {
        "total": len(rows),
        "page": 1,
        "page_size": limit,
        "items": [AuditOut.model_validate(row).model_dump() for row in rows],
    }


@router.post("/admin/tick")
def force_tick(
    db: Session = Depends(get_db),
    principal: Principal = Depends(WriteAccess),
):
    """手工推进一轮调度（排障/demo 用）。"""
    planner = Planner()
    result = planner.tick()
    result["triggered_by"] = principal.name
    return result


@router.get("/admin/leader")
def leader_state(
    db: Session = Depends(get_db),
    _: Principal = Depends(ReadAccess),
):
    row = db.get(SchedulerLease, 1)
    if row is None:
        return {"holder": None, "fencing_token": 0, "expires_at": None}
    return {
        "holder": row.holder,
        "fencing_token": row.fencing_token,
        "expires_at": iso(row.expires_at),
        "acquired_at": iso(row.acquired_at),
    }


# ---------------------------------------------------------------- API Keys
@router.get("/api-keys", response_model=list[ApiKeyOut])
def list_keys(
    db: Session = Depends(get_db),
    _: Principal = Depends(AdminAccess),
):
    rows = db.scalars(select(ApiKey).order_by(ApiKey.created_at.desc())).all()
    return [
        ApiKeyOut(
            id=row.id,
            name=row.name,
            prefix=row.prefix,
            role=row.role,
            created_at=row.created_at,
            last_used_at=row.last_used_at,
            revoked_at=row.revoked_at,
        )
        for row in rows
    ]


@router.post("/api-keys", status_code=201)
def create_key(
    payload: ApiKeyCreate,
    db: Session = Depends(get_db),
    principal: Principal = Depends(AdminAccess),
):
    raw, digest, prefix = new_token()
    record = ApiKey(
        name=payload.name,
        prefix=prefix,
        key_hash=digest,
        role=payload.role,
        created_at=naive(now()),
    )
    db.add(record)
    db.commit()
    return {
        "id": record.id,
        "name": record.name,
        "role": record.role,
        "token": raw,
        "notice": "明文只在本次响应中出现，请立即保存",
    }


@router.delete("/api-keys/{key_id}", status_code=204)
def revoke_key(
    key_id: str,
    db: Session = Depends(get_db),
    _: Principal = Depends(AdminAccess),
):
    record = db.get(ApiKey, key_id)
    if record is None:
        raise HTTPException(status_code=404, detail="密钥不存在")
    record.revoked_at = naive(now())
    db.commit()
    return None
