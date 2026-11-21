"""探针与指标。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Response
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from ..clock import iso, naive, now
from ..metrics import METRICS
from ..models import Job, Task, Worker
from .deps import get_db

router = APIRouter(tags=["system"])

VERSION = "1.0.0"


@router.get("/healthz")
def healthz(db: Session = Depends(get_db)):
    db.execute(text("SELECT 1"))
    return {"status": "ok", "version": VERSION, "time": iso(now())}


@router.get("/readyz")
def readyz(db: Session = Depends(get_db)):
    """就绪探针：数据库可写才算就绪。"""
    db.execute(text("SELECT 1"))
    return {"status": "ready"}


@router.get("/metrics")
def metrics(db: Session = Depends(get_db)):
    stamp = naive(now())
    METRICS.set_gauge(
        "tempo_queue_depth",
        db.scalar(select(func.count()).select_from(Task).where(Task.status == "PENDING")) or 0,
    )
    METRICS.set_gauge(
        "tempo_running_tasks",
        db.scalar(select(func.count()).select_from(Task).where(Task.status == "RUNNING")) or 0,
    )
    METRICS.set_gauge(
        "tempo_dead_tasks",
        db.scalar(select(func.count()).select_from(Task).where(Task.status == "DEAD")) or 0,
    )
    METRICS.set_gauge(
        "tempo_online_workers",
        db.scalar(select(func.count()).select_from(Worker).where(Worker.status == "ONLINE")) or 0,
    )
    METRICS.set_gauge(
        "tempo_jobs_enabled",
        db.scalar(select(func.count()).select_from(Job).where(Job.enabled.is_(True))) or 0,
    )
    METRICS.set_gauge("tempo_scrape_timestamp", stamp.timestamp())
    return Response(content=METRICS.render(), media_type="text/plain; version=0.0.4; charset=utf-8")
